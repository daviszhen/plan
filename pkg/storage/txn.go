package storage

import (
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/daviszhen/plan/pkg/chunk"
	"github.com/daviszhen/plan/pkg/util"
)

const (
	TxnIdStart   TxnType = 4611686018427388000
	MaxTxnId     TxnType = math.MaxUint64
	NotDeletedId TxnType = math.MaxUint64
)

type TxnMgr struct {
	_curStartTs            TxnType
	_curTxnId              TxnType
	_lowestActiveId        TxnType
	_lowestActiveStart     TxnType
	_activeTxns            []*Txn
	_recentlyCommittedTxns []*Txn
	_oldTxns               []*Txn
	_threadIsCheckpointing bool
	_lock                  sync.Mutex
}

func NewTxnMgr() *TxnMgr {
	return &TxnMgr{
		_curStartTs:        2,
		_curTxnId:          TxnIdStart,
		_lowestActiveId:    TxnIdStart,
		_lowestActiveStart: MaxTxnId,
	}
}

func (txnMgr *TxnMgr) NewTxn() (*Txn, error) {
	txnMgr._lock.Lock()
	defer txnMgr._lock.Unlock()
	if txnMgr._curStartTs >= TxnIdStart {
		return nil, fmt.Errorf("invalid txn id")
	}
	startTime := txnMgr._curStartTs
	txnMgr._curStartTs++
	txnId := txnMgr._curTxnId
	txnMgr._curTxnId++
	if len(txnMgr._activeTxns) == 0 {
		txnMgr._lowestActiveId = txnId
		txnMgr._lowestActiveStart = startTime
	}
	txn := &Txn{
		_txnMgr:     txnMgr,
		_startTime:  startTime,
		_id:         txnId,
		_commitId:   0,
		_undoBuffer: UndoBuffer{},
		_storage:    nil,
	}
	txn._storage = NewLocalStorage(txn)
	txnMgr._activeTxns = append(txnMgr._activeTxns, txn)
	return txn, nil
}

func (txnMgr *TxnMgr) Commit(txn *Txn) error {
	txnMgr._lock.Lock()
	defer txnMgr._lock.Unlock()
	commitId := txnMgr._curStartTs
	txnMgr._curStartTs++
	err := txn.Commit(commitId, false)
	if err != nil {
		txn._commitId = 0
		txn.Rollback()
	}
	txnMgr.removeUnsafe(txn)
	return err
}

func (txnMgr *TxnMgr) Rollback(txn *Txn) {
	txnMgr._lock.Lock()
	defer txnMgr._lock.Unlock()
	txn.Rollback()
	txnMgr.removeUnsafe(txn)
}

func (txnMgr *TxnMgr) removeUnsafe(txn *Txn) {
	lStartTime := TxnIdStart
	lTxnId := MaxTxnId
	for _, act := range txnMgr._activeTxns {
		if act._id == txn._id {
			continue
		} else {
			lStartTime = min(lStartTime, act._startTime)
			lTxnId = min(lTxnId, act._id)
		}
	}
	txnMgr._lowestActiveStart = lStartTime
	txnMgr._lowestActiveId = lTxnId
	util.RemoveIf(txnMgr._activeTxns, func(t *Txn) bool {
		return t._id == txn._id
	})
	if txn._commitId != 0 {
		txnMgr._recentlyCommittedTxns = append(txnMgr._recentlyCommittedTxns, txn)
	} else {
		txnMgr._oldTxns = append(txnMgr._oldTxns, txn)
	}

	idx := 0
	for ; idx < len(txnMgr._recentlyCommittedTxns); idx++ {
		t := txnMgr._recentlyCommittedTxns[idx]
		if t._commitId < lStartTime {
			t.Cleanup()
			txnMgr._oldTxns = append(txnMgr._oldTxns, t)
		} else {
			break
		}
	}
	if idx > 0 {
		txnMgr._recentlyCommittedTxns = txnMgr._recentlyCommittedTxns[:idx]
	}

	if len(txnMgr._activeTxns) == 0 {
		idx = len(txnMgr._oldTxns)
	} else {
		idx = 0
	}
}

type Txn struct {
	_txnMgr     *TxnMgr
	_startTime  TxnType
	_id         TxnType
	_commitId   TxnType
	_undoBuffer UndoBuffer
	_storage    *LocalStorage
}

func (txn *Txn) Commit(commitId TxnType, ckp bool) error {
	txn._commitId = commitId
	err := txn._storage.Commit(txn)
	if err != nil {
		return err
	}
	err = txn._undoBuffer.Commit(nil, commitId)
	if err != nil {
		return err
	}
	return nil
}

func (txn *Txn) Rollback() {
	txn._storage.Rollback()
	txn._undoBuffer.Rollback()
}

func (txn *Txn) Cleanup() {
	txn._undoBuffer.Cleanup()
}

func (txn *Txn) Changed() bool {
	return txn._undoBuffer.Changed() || txn._storage.Changed()
}

func (txn *Txn) PushDelete(
	table *DataTable,
	info *ChunkInfo,
	rows []RowType,
	count IdxType,
	baseRow IdxType,
) {

}

func (txn *Txn) PushAppend(
	table *DataTable,
	rowStart IdxType,
	rowCount IdxType,
) {
	ptr := txn._undoBuffer.CreateEntry(INSERT_TUPLE, IdxType(appendInfoSize))
	infos := util.PointerToSlice[AppendInfo](ptr, int(appendInfoSize))
	infos[0]._table = table
	infos[0]._startRow = rowStart
	infos[0]._count = rowCount
}

func (txn *Txn) CreateUpdateInfo(
	typeSize IdxType,
	entries IdxType,
) *UpdateInfo {
	return nil
}

type ChunkInfoType int

const (
	CONSTANT_INFO ChunkInfoType = iota
	VECTOR_INFO
	EMPTY_INFO
)

type ChunkInfo struct {
	_type  ChunkInfoType
	_start IdxType
	//constant info
	_insertId atomic.Uint64
	_deleteId atomic.Uint64
	//vector info
	_sameInsertedId atomic.Bool
	_inserted       [STANDARD_VECTOR_SIZE]atomic.Uint64
	_deleted        [STANDARD_VECTOR_SIZE]atomic.Uint64
	_anyDeleted     atomic.Bool
}

func NewConstantInfo(start IdxType) *ChunkInfo {
	ret := &ChunkInfo{
		_type: CONSTANT_INFO,
	}
	ret._deleteId.Store(uint64(NotDeletedId))
	return ret
}

func NewVectorInfo(start IdxType) *ChunkInfo {
	ret := &ChunkInfo{
		_type:  VECTOR_INFO,
		_start: start,
	}
	ret._sameInsertedId.Store(true)
	for i := 0; i < STANDARD_VECTOR_SIZE; i++ {
		ret._deleted[i].Store(uint64(NotDeletedId))
	}
	return ret
}

func (info *ChunkInfo) Append(start IdxType, end IdxType, commitId TxnType) {
	if start == 0 {
		info._insertId.Store(uint64(commitId))
	} else if info._insertId.Load() != uint64(commitId) {
		info._sameInsertedId.Store(false)
		info._insertId.Store(uint64(NotDeletedId))
	}
	for i := start; i < end; i++ {
		info._inserted[i].Store(uint64(commitId))
	}
}

func (info *ChunkInfo) CommitAppend(
	commitId TxnType,
	start IdxType, end IdxType) {
	switch info._type {
	case CONSTANT_INFO:
		util.AssertFunc(start == 0 && end == STANDARD_VECTOR_SIZE)
		info._insertId.Store(uint64(commitId))
	case VECTOR_INFO:
		if info._sameInsertedId.Load() {
			info._insertId.Store(uint64(commitId))
		}
		for i := start; i < end; i++ {
			info._inserted[i].Store(uint64(commitId))
		}
	case EMPTY_INFO:
	}
}

func (info *ChunkInfo) CommitDelete(
	commitId TxnType,
	rows []RowType,
	count IdxType) {
	if info._type == VECTOR_INFO {
		for i := IdxType(0); i < count; i++ {
			info._deleted[rows[i]].Store(uint64(commitId))
		}
	}
}

func (info *ChunkInfo) GetSelVector(
	txn *Txn,
	sel *chunk.SelectVector,
	maxCount IdxType) IdxType {
	switch info._type {
	case CONSTANT_INFO:
		return info.TemplatedGetSelVectorWithConstant(
			txn._startTime,
			txn._id,
			sel,
			maxCount,
			TxnVersionOp{})
	case VECTOR_INFO:
		return info.TemplatedGetSelVectorWithVector(
			txn._startTime,
			txn._id,
			sel,
			maxCount,
			TxnVersionOp{})
	default:
		panic("unexpected info type")
	}
}

func (info *ChunkInfo) TemplatedGetSelVectorWithConstant(
	startTime TxnType,
	txnId TxnType,
	sel *chunk.SelectVector,
	maxCount IdxType,
	op VersionOp,
) IdxType {
	if op.UseInsertedVersion(startTime, txnId, TxnType(info._insertId.Load())) &&
		op.UseDeletedVersion(startTime, txnId, TxnType(info._deleteId.Load())) {
		return maxCount
	}
	return 0
}

func (info *ChunkInfo) TemplatedGetSelVectorWithVector(
	startTime TxnType,
	txnId TxnType,
	sel *chunk.SelectVector,
	maxCount IdxType,
	op VersionOp,
) IdxType {
	count := IdxType(0)
	if info._sameInsertedId.Load() && !info._anyDeleted.Load() {
		if op.UseInsertedVersion(startTime, txnId, TxnType(info._insertId.Load())) {
			return maxCount
		} else {
			return 0
		}
	} else if info._sameInsertedId.Load() {
		if !op.UseInsertedVersion(startTime, txnId, TxnType(info._insertId.Load())) {
			return 0
		}
		for i := IdxType(0); i < maxCount; i++ {
			if op.UseDeletedVersion(startTime, txnId, TxnType(info._deleted[i].Load())) {
				sel.SetIndex(int(count), int(i))
				count++
			}
		}
	} else if !info._anyDeleted.Load() {
		for i := IdxType(0); i < maxCount; i++ {
			if op.UseInsertedVersion(startTime, txnId, TxnType(info._inserted[i].Load())) {
				sel.SetIndex(int(count), int(i))
				count++
			}
		}
	} else {
		for i := IdxType(0); i < maxCount; i++ {
			if op.UseInsertedVersion(startTime, txnId, TxnType(info._inserted[i].Load())) &&
				op.UseDeletedVersion(startTime, txnId, TxnType(info._deleted[i].Load())) {
				sel.SetIndex(int(count), int(i))
				count++
			}
		}
	}
	return count
}

type VersionOp interface {
	UseInsertedVersion(startTime, txnId, id TxnType) bool
	UseDeletedVersion(startTime, txnId, id TxnType) bool
}

var _ VersionOp = &TxnVersionOp{}
var _ VersionOp = &CommittedVersionOp{}

type TxnVersionOp struct {
}

func (op TxnVersionOp) UseInsertedVersion(
	startTime, txnId, id TxnType) bool {
	return id < startTime || id == txnId
}

func (op TxnVersionOp) UseDeletedVersion(
	startTime, txnId, id TxnType) bool {
	return !op.UseInsertedVersion(startTime, txnId, id)
}

type CommittedVersionOp struct {
}

func (op CommittedVersionOp) UseInsertedVersion(
	startTime, txnId, id TxnType) bool {
	return true
}

func (op CommittedVersionOp) UseDeletedVersion(
	minStartTime, minTxnId, id TxnType) bool {
	return id >= minStartTime && id < TxnIdStart || id >= minTxnId
}

type UndoFlags uint32

const (
	EMPTY_ENTRY  UndoFlags = 0
	INSERT_TUPLE UndoFlags = 1
	DELETE_TUPLE UndoFlags = 2
	UPDATE_TUPLE UndoFlags = 3
)

type UndoBuffer struct {
	//records the base address of the undo buffer
	_logs []unsafe.Pointer
}

func (undo *UndoBuffer) Changed() bool {
	return len(undo._logs) != 0
}

// CreateEntry returns the pointer address after the header
func (undo *UndoBuffer) CreateEntry(
	typ UndoFlags,
	len IdxType,
) unsafe.Pointer {
	len = util.AlignValue(len, 8)
	allocLen := len + UNDO_ENTRY_HEADER_SIZE
	data := util.CMalloc(int(allocLen))
	undo._logs = append(undo._logs, data)
	util.Store[UndoFlags](typ, data)
	util.Store[uint32](uint32(len), util.PointerAdd(data, 4))
	return util.PointerAdd(data, UNDO_ENTRY_HEADER_SIZE)
}

func (undo *UndoBuffer) Commit(log *WriteAheadLog, commitId TxnType) error {
	state := &CommitState{
		_log:      log,
		_commitId: commitId,
	}
	for _, ulog := range undo._logs {
		typ := util.Load[UndoFlags](ulog)
		err := state.CommitEntry(typ, util.PointerAdd(ulog, UNDO_ENTRY_HEADER_SIZE))
		if err != nil {
			return err
		}
	}
	return nil
}

func (undo *UndoBuffer) Rollback() {
	state := &RollbackState{}
	for i := 0; i < len(undo._logs); i++ {
		j := len(undo._logs) - i - 1
		ulog := undo._logs[j]
		typ := util.Load[UndoFlags](ulog)
		state.RollbackEntry(typ, util.PointerAdd(ulog, UNDO_ENTRY_HEADER_SIZE))
	}
}

func (undo *UndoBuffer) Cleanup() {
	state := &CleanupState{}
	for _, ulog := range undo._logs {
		typ := util.Load[UndoFlags](ulog)
		state.CleanupEntry(typ, util.PointerAdd(ulog, UNDO_ENTRY_HEADER_SIZE))
	}
}

func (undo *UndoBuffer) Close() {
	for _, log := range undo._logs {
		util.CFree(log)
	}
	undo._logs = nil
}

type CommitState struct {
	_log      *WriteAheadLog
	_commitId TxnType
}

func (commit *CommitState) CommitEntry(
	typ UndoFlags,
	data unsafe.Pointer,
) error {
	switch typ {
	case INSERT_TUPLE:
		infos := util.PointerToSlice[AppendInfo](data, int(appendInfoSize))
		info := infos[0]
		info._table.CommitAppend(
			commit._commitId,
			info._startRow,
			info._count)
	case DELETE_TUPLE:
		infos := util.PointerToSlice[DeleteInfo](data, int(deleteInfoSize))
		info := infos[0]
		info._vinfo.CommitDelete(
			commit._commitId,
			info._rows[:],
			info._count)
	case UPDATE_TUPLE:
		infos := util.PointerToSlice[UpdateInfo](data, int(updateInfoSize))
		info := &infos[0]
		info._versionNumber.Store(uint64(commit._commitId))
	}
	return nil
}

type RollbackState struct {
}

func (rollback *RollbackState) RollbackEntry(
	typ UndoFlags,
	data unsafe.Pointer,
) {

}

type CleanupState struct {
}

func (cleanup *CleanupState) CleanupEntry(
	typ UndoFlags,
	data unsafe.Pointer,
) {

}

var (
	appendInfoSize = unsafe.Sizeof(AppendInfo{})
	deleteInfoSize = unsafe.Sizeof(DeleteInfo{})
	updateInfoSize = unsafe.Sizeof(UpdateInfo{})
)

type AppendInfo struct {
	_table    *DataTable
	_startRow IdxType
	_count    IdxType
}

type DeleteInfo struct {
	_table   *DataTable
	_vinfo   *ChunkInfo
	_count   IdxType
	_baseRow IdxType
	_rows    [1]RowType
}

type UpdateInfo struct {
	_segment       *UpdateSegment
	_columnIndex   IdxType
	_versionNumber atomic.Uint64
	_vectorIndex   IdxType
	_N             int
	_max           int
	_tuples        []int
	_tupleData     unsafe.Pointer
	_prev          *UpdateInfo
	_next          *UpdateInfo
}
