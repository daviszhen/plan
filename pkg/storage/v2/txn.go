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
	_minTxnId     TxnId                // all txn id <= _minTxnId are visible
	_maxTxnId     TxnId                // all txn id >= _maxTxnId are invisible
	_activeTxnIds *btree.BTreeG[TxnId] // all txn id are active [_minTxnId, _maxTxnId) when txn start
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
	_storage    *TxnStorage
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
	ret._storage = NewTxnStorage(ret)
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
			if item.Id().IsValid() &&
				item.Id().Less(minActiveId) {
				minActiveId = item.Id()
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
	minActiveId := mgr.genMinActiveIdUnLock()
	maxTxnId := TxnId(mgr._clock.Now())
	snapshot := &Snapshot{
		_minTxnId:     minActiveId,
		_maxTxnId:     maxTxnId,
		_activeTxnIds: btree.NewBTreeG[TxnId](txnIdLess),
	}
	//collect active txnid
	mgr._activeTxns.Scan(
		func(item *Txn) bool {
			if item.Id().IsValid() {
				snapshot._activeTxnIds.Set(item.Id())
			}
			return true
		})
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

func (mgr *TxnMgr) CheckRowVisiable(txn *Txn, rowInfo *ChunkInfo) bool {
	insertTxnId := *rowInfo._inserted.Load()
	deleteTxnId := TxnIdInvalid
	if rowInfo._deleted.Load() != nil {
		deleteTxnId = *rowInfo._deleted.Load()
	}

	insertTxnStatus := mgr.TxnState(insertTxnId)
	//case 1: insert txn id is aborted.
	if insertTxnStatus == TxnStateAborted {
		//插入的事务已经回滚，看不见
		return false
	} else if insertTxnStatus == TxnStateActive {
		//case 2: insert txn id is active.
		if insertTxnId == txn.Id() {
			//自己事务插入的。
			if deleteTxnId == TxnIdInvalid {
				//没有删除，可见
				return true
			} else {
				//有删除，不可见
				return false
			}
		} else { //其它事务插入的，看不见
			return false
		}
	} else if insertTxnStatus == TxnStateCommitted {
		//case 3: insert txn id is committed.
		insertTxnInSS := txn._snapshot.IsActiveInSnapshot(insertTxnId)
		deleteTxnStatus := TxnStateAborted
		if deleteTxnId != TxnIdInvalid {
			deleteTxnStatus = mgr.TxnState(deleteTxnId)
		}
		if insertTxnInSS {
			//插入的事务在快照active事务中，不可见
			return false
		} else if deleteTxnId == TxnIdInvalid || deleteTxnStatus == TxnStateAborted {
			//没有删除，或者删除的事务已经回滚，可见
			return true
		} else if deleteTxnStatus == TxnStateActive {
			if deleteTxnId == txn.Id() {
				//删除的事务是自己，不可见
				return false
			} else {
				//删除的事务是其它事务，可见
				return true
			}
		} else if deleteTxnStatus == TxnStateCommitted {
			deleteTxnInSS := txn._snapshot.IsActiveInSnapshot(deleteTxnId)
			if deleteTxnInSS {
				//删除的事务在快照active事务中，可见
				return true
			} else {
				//删除的事务不在快照active事务中，不可见
				return false
			}
		}
	}
	panic("unreachable")
}

func (mgr *TxnMgr) CheckUpdateVisiable(txn *Txn, updateInfo *UpdateInfo) bool {
	rowInfo := &ChunkInfo{}
	rowInfo._inserted.Store(updateInfo._version.Load())
	rowInfo._deleted.Store(nil)
	return mgr.CheckRowVisiable(txn, rowInfo)
}
