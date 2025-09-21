package v2

import (
	"fmt"
	"math"
	"sync"
	"sync/atomic"

	hlc "github.com/cockroachdb/cockroachdb-parser/pkg/util/hlc"
	"github.com/daviszhen/plan/pkg/util"
	"github.com/tidwall/btree"
)

type TxnId hlc.Timestamp

func (id TxnId) Less(other TxnId) bool {
	a := hlc.Timestamp(id)
	b := hlc.Timestamp(other)
	return a.Less(b)
}

func (id TxnId) LessEq(other TxnId) bool {
	a := hlc.Timestamp(id)
	b := hlc.Timestamp(other)
	return a.LessEq(b)
}

func (id TxnId) IsValid() bool {
	return id != TxnIdInvalid && id != TxnIdMax
}

func txnIdLess(a TxnId, b TxnId) bool {
	return a.Less(b)
}

var (
	TxnIdInvalid TxnId = TxnId(hlc.Timestamp{})
	TxnIdMax     TxnId = TxnId(hlc.Timestamp{
		WallTime: math.MaxInt64,
		Logical:  math.MaxInt32,
	})
)

type TxnState uint32

const (
	TxnStateActive TxnState = iota
	TxnStateCommitted
	TxnStateAborted
)

type TxnIsolationLevel uint32

const (
	TxnIsolationLevelReadCommitted     TxnIsolationLevel = 0
	TxnIsolationLevelSnapshotIsolation TxnIsolationLevel = 1
)

// Snapshot is a snapshot of the database at txn start
type Snapshot struct {
	// minimal txnid that is still active
	// all txn ids <= _minTxnId are committed (visible) or aborted (invisible)
	_minTxnId TxnId
	// first txnid that has not been assigned
	// all txn ids >= _maxTxnId are invisible
	_maxTxnId TxnId
	// all txn ids are active when txn start, s.t. [_minTxnId, _maxTxnId)
	_activeTxnIds *btree.BTreeG[TxnId]
}

func (snapshot *Snapshot) String() string {
	return fmt.Sprintf("[minTxnId: %v maxTxnId: %v activeTxns: %v]",
		snapshot._minTxnId,
		snapshot._maxTxnId,
		snapshot._activeTxnIds)
}

func (snapshot *Snapshot) IsActiveInSnapshot(txnId TxnId) bool {
	return true
}

type Txn struct {
	_name string
	_id   TxnId //also the start ts

	_commitTs atomic.Pointer[TxnId]
	_abortTs  atomic.Pointer[TxnId]

	_minActiveId    TxnId //lowest active id at txn start
	_state          atomic.Uint32
	_snapshot       Snapshot
	_isolationLevel TxnIsolationLevel

	_undoBuffer UndoBuffer
}

func txnLess(a *Txn, b *Txn) bool {
	return a.Id().Less(b.Id())
}

func NewTxn(name string,
	id TxnId,
	minActiveId TxnId,
	snapshot *Snapshot,
	isolationLevel TxnIsolationLevel) *Txn {
	ret := &Txn{
		_name:           name,
		_id:             id,
		_minActiveId:    minActiveId,
		_snapshot:       *snapshot,
		_isolationLevel: isolationLevel,
	}
	return ret
}

func (txn *Txn) String() string {
	return fmt.Sprintf("[%s id: %v state: %v minActiveId: %v commitTs: %v abortTs: %v snapshot: %v isolationLevel: %v]",
		txn._name,
		txn._id,
		txn._state.Load(),
		txn._minActiveId,
		txn._commitTs.Load(),
		txn._abortTs.Load(),
		txn._snapshot,
		txn._isolationLevel)
}

func (txn *Txn) Id() TxnId {
	return txn._id
}

func (txn *Txn) CommitTs() TxnId {
	if txn._commitTs.Load() == nil {
		return TxnId{}
	}
	return *txn._commitTs.Load()
}

func (txn *Txn) AbortTs() TxnId {
	if txn._abortTs.Load() == nil {
		return TxnId{}
	}
	return *txn._abortTs.Load()
}

func (txn *Txn) IsCommitted() bool {
	return txn._state.Load() == uint32(TxnStateCommitted)
}

func (txn *Txn) IsAborted() bool {
	return txn._state.Load() == uint32(TxnStateAborted)
}

func (txn *Txn) Commit(commitTs TxnId) {
	//step 1: prepare redo log & undo log
	//step 2: write wal
	//step 3: flush wal
	//step 4: update ts
}

func (txn *Txn) Rollback(abortTs TxnId) {
	//step 1: cleanup
	//step 2: update ts
}

type TxnMgr struct {
	_clock                 *hlc.Clock
	_activeTxns            *btree.BTreeG[*Txn]
	_recentlyCommittedTxns *btree.BTreeG[*Txn]
	//waiting gc
	_oldTxns *btree.BTreeG[*Txn]
	_lock    sync.Locker
}

func NewTxnMgr(clock *hlc.Clock) *TxnMgr {
	txnMgr := &TxnMgr{
		_clock:                 clock,
		_lock:                  util.NewReentryLock(),
		_activeTxns:            btree.NewBTreeG[*Txn](txnLess),
		_recentlyCommittedTxns: btree.NewBTreeG[*Txn](txnLess),
		_oldTxns:               btree.NewBTreeG[*Txn](txnLess),
	}
	return txnMgr
}

func (mgr *TxnMgr) NewTxn(name string, isolationLevel TxnIsolationLevel) *Txn {
	mgr._lock.Lock()
	defer mgr._lock.Unlock()
	id := TxnId(mgr._clock.Now())
	//calculate minActiveId
	minActiveId := mgr.genMinActiveIdUnLock()

	//calculate snapshot for SI
	var snapshot *Snapshot
	if isolationLevel == TxnIsolationLevelSnapshotIsolation {
		snapshot = mgr.genSnapshotUnLock()
	}
	txn := NewTxn(name,
		id,
		minActiveId,
		snapshot,
		isolationLevel)
	mgr._activeTxns.Set(txn)
	return txn
}

func (mgr *TxnMgr) genMinActiveIdUnLock() TxnId {
	minActiveId := TxnIdMax
	mgr._activeTxns.Scan(
		func(item *Txn) bool {
			if item.Id().IsValid() {
				if item.Id().Less(minActiveId) {
					minActiveId = item.Id()
				}
			} else {
				panic("active txn has invalid id 1")
			}
			return true
		})

	//if no valid txn, set minActiveId to TxnIdInvalid
	if minActiveId == TxnIdMax {
		minActiveId = TxnIdInvalid
	}
	return minActiveId
}

func (mgr *TxnMgr) genActiveTxnsUnLock(
	upbound TxnId,
	activeTxnIds *btree.BTreeG[TxnId],
) TxnId {
	minActiveId := TxnIdMax
	mgr._activeTxns.Scan(
		func(item *Txn) bool {
			if item.Id().IsValid() {
				if item.Id().Less(minActiveId) {
					minActiveId = item.Id()
				}
				if item.Id().Less(upbound) {
					activeTxnIds.Set(item.Id())
				}
			} else {
				panic("active txn has invalid id 2")
			}
			return true
		})

	//if no valid txn, set minActiveId to TxnIdInvalid
	if minActiveId == TxnIdMax {
		minActiveId = TxnIdInvalid
	}
	return minActiveId
}

func (mgr *TxnMgr) GenSnapshot() *Snapshot {
	mgr._lock.Lock()
	defer mgr._lock.Unlock()
	return mgr.genSnapshotUnLock()
}

func (mgr *TxnMgr) genSnapshotUnLock() *Snapshot {
	//max txn id that has not been assigned
	maxTxnId := TxnId(mgr._clock.Now())
	snapshot := &Snapshot{
		_maxTxnId:     maxTxnId,
		_activeTxnIds: btree.NewBTreeG[TxnId](txnIdLess),
	}
	snapshot._minTxnId = mgr.genActiveTxnsUnLock(
		maxTxnId,
		snapshot._activeTxnIds)
	return snapshot
}

func (mgr *TxnMgr) Commit(txn *Txn) {
	mgr._lock.Lock()
	defer mgr._lock.Unlock()
	fmt.Println("commit txn", txn._name)
}

func (mgr *TxnMgr) Rollback(txn *Txn) {
	mgr._lock.Lock()
	defer mgr._lock.Unlock()
	fmt.Println("rollback txn", txn._name)
}

func (mgr *TxnMgr) Close() {
	mgr._lock.Lock()
	defer mgr._lock.Unlock()
	mgr._activeTxns.Scan(func(item *Txn) bool {
		item.Rollback(TxnId(mgr._clock.Now()))
		return true
	})
}

func (mgr *TxnMgr) TxnState(txnId TxnId) TxnState {
	mgr._lock.Lock()
	defer mgr._lock.Unlock()
	if txnId == TxnIdInvalid {
		return TxnStateAborted
	}
	tempTxn := &Txn{
		_id: txnId,
	}
	rawTxn, ok := mgr._activeTxns.Get(tempTxn)
	if !ok {
		rawTxn, ok = mgr._recentlyCommittedTxns.Get(tempTxn)
		if !ok {
			rawTxn, ok = mgr._oldTxns.Get(tempTxn)
			if !ok {
				//not found, it must be committed or doest not exist
				return TxnStateCommitted
			}
			return TxnState(rawTxn._state.Load())
		} else {
			return TxnState(rawTxn._state.Load())
		}
	} else {
		return TxnState(rawTxn._state.Load())
	}
}


