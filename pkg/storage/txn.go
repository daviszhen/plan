package storage

import (
	"fmt"
	"math"
	"slices"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/daviszhen/plan/pkg/chunk"
	"github.com/daviszhen/plan/pkg/common"
	"github.com/daviszhen/plan/pkg/util"
)

const (
	TxnIdStart   TxnType = 4611686018427388000
	MaxTxnId     TxnType = math.MaxUint64
	NotDeletedId TxnType = math.MaxUint64
)

var GTxnMgr *TxnMgr
var currentQueryNumber atomic.Uint64

func GetNewQueryNumber() uint64 {
	old := currentQueryNumber.Load()
	currentQueryNumber.Add(1)
	return old
}

func ActiveQueryNumber() uint64 {
	return currentQueryNumber.Load()
}

func BeginQuery(txn *Txn) {
	txn.SetActiveQuery(GetNewQueryNumber())
}

func init() {

}

type TxnMgr struct {
	_curStartTs            TxnType
	_curTxnId              TxnType
	_lowestActiveId        TxnType
	_lowestActiveStart     TxnType
	_activeTxns            []*Txn
	_recentlyCommittedTxns []*Txn
	//waiting gc
	_oldTxns               []*Txn
	_threadIsCheckpointing bool
	_lock                  sync.Locker
}

func NewTxnMgr() *TxnMgr {
	return &TxnMgr{
		_curStartTs:        2,
		_curTxnId:          TxnIdStart,
		_lowestActiveId:    TxnIdStart,
		_lowestActiveStart: MaxTxnId,
		_lock:              util.NewReentryLock(),
	}
}

func (txnMgr *TxnMgr) NewTxn(name string) (*Txn, error) {
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
		_name:       name,
		_startTime:  startTime,
		_id:         txnId,
		_commitId:   0,
		_undoBuffer: UndoBuffer{},
		_storage:    nil,
	}
	txn._activeQuery.Store(MAXIMUM_QUERY_ID)
	txn._storage = NewLocalStorage(txn)
	txnMgr._activeTxns = append(txnMgr._activeTxns, txn)
	return txn, nil
}

func (txnMgr *TxnMgr) NewTxn2(name string, id, start TxnType) (*Txn, error) {
	txnMgr._lock.Lock()
	defer txnMgr._lock.Unlock()
	if txnMgr._curStartTs >= TxnIdStart {
		return nil, fmt.Errorf("invalid txn id")
	}
	txn := &Txn{
		_txnMgr:     txnMgr,
		_name:       name,
		_startTime:  start,
		_id:         id,
		_commitId:   0,
		_undoBuffer: UndoBuffer{},
		_storage:    nil,
	}
	txn._activeQuery.Store(MAXIMUM_QUERY_ID)
	txn._storage = NewLocalStorage(txn)
	txnMgr._activeTxns = append(txnMgr._activeTxns, txn)
	return txn, nil
}

func (txnMgr *TxnMgr) Checkpoint(txn *Txn, force bool) error {
	txnMgr._lock.Lock()
	defer txnMgr._lock.Unlock()
	if txnMgr._threadIsCheckpointing {
		//another txn is checkpointing
		return nil
	}
	ckpLock := NewCheckpointLock(txnMgr)
	defer ckpLock.Unlock()
	txnMgr._lock.Unlock()

	txnMgr._lock.Lock()
	if txn.Changed() {
		return fmt.Errorf("can not checkpoint: txn has local changes")
	}

	if !force {
		if !txnMgr.CanCheckpoint(txn) {
			return fmt.Errorf("can not checkpoint: there are other txns. with 'FORCE = true' to abort other txns")
		}
	} else {
		if !txnMgr.CanCheckpoint(txn) {
			for i := 0; i < len(txnMgr._activeTxns); i++ {
				act := txnMgr._activeTxns[i]
				act.Rollback()
				txnMgr.removeUnsafe(act)
				i--
			}
			util.AssertFunc(txnMgr.CanCheckpoint(nil))
		}
	}
	return GStorageMgr.CreateCheckpoint(false, false)
}

func (txnMgr *TxnMgr) Commit(txn *Txn) error {
	txnMgr._lock.Lock()
	defer txnMgr._lock.Unlock()

	ckpLock := NewCheckpointLock(txnMgr)
	defer ckpLock.Unlock()
	ckp := false
	if txnMgr._threadIsCheckpointing {
		ckp = false
	} else {
		ckp = txnMgr.CanCheckpoint(txn)
	}
	if ckp {
		if txn.AutomaticCheckpoint() {
			ckpLock.Lock()
			txnMgr._lock.Unlock()

			txnMgr._lock.Lock()
			ckp = txnMgr.CanCheckpoint(txn)
			if !ckp {
				ckpLock.Unlock()
			}
		} else {
			ckp = false
		}
	}

	commitId := txnMgr._curStartTs
	txnMgr._curStartTs++
	err := txn.Commit(commitId, ckp)
	if err != nil {
		ckp = false
		txn._commitId = 0
		txn.Rollback()
	}
	if !ckp {
		ckpLock.Unlock()
	}
	txnMgr.removeUnsafe(txn)
	if ckp && GStorageMgr != nil {
		err = GStorageMgr.CreateCheckpoint(false, true)
	}
	return err
}

func (txnMgr *TxnMgr) CanCheckpoint(txn *Txn) bool {
	if len(txnMgr._recentlyCommittedTxns) != 0 ||
		len(txnMgr._oldTxns) != 0 {
		return false
	}
	for _, tt := range txnMgr._activeTxns {
		if tt != txn {
			return false
		}
	}
	return true
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
	lActiveQuery := MAXIMUM_QUERY_ID
	for _, act := range txnMgr._activeTxns {
		if act._id == txn._id {
			continue
		} else {
			lStartTime = min(lStartTime, act._startTime)
			lTxnId = min(lTxnId, act._id)
			lActiveQuery = min(lActiveQuery, act._activeQuery.Load())
		}
	}
	txnMgr._lowestActiveStart = lStartTime
	txnMgr._lowestActiveId = lTxnId

	lStoredQuery := lStartTime
	txnMgr._activeTxns = util.RemoveIf(txnMgr._activeTxns, func(t *Txn) bool {
		return t._id == txn._id
	})
	currentQuery := ActiveQueryNumber()
	if txn._commitId != 0 {
		//commited.
		txnMgr._recentlyCommittedTxns = append(txnMgr._recentlyCommittedTxns, txn)
	} else {
		//aborted. waiting GC
		txn._highestActiveQuery.Store(currentQuery)
		txnMgr._oldTxns = append(txnMgr._oldTxns, txn)
	}

	//find txns can be put to oldTxns queue
	idx := 0
	for ; idx < len(txnMgr._recentlyCommittedTxns); idx++ {
		t := txnMgr._recentlyCommittedTxns[idx]
		lStoredQuery = min(lStoredQuery, t._startTime)
		if t._commitId < lStartTime {
			//
			t.Cleanup()
			t._highestActiveQuery.Store(currentQuery)
			txnMgr._oldTxns = append(txnMgr._oldTxns, t)
		} else {
			break
		}
	}
	if idx > 0 {
		txnMgr._recentlyCommittedTxns = txnMgr._recentlyCommittedTxns[idx:]
	}

	//find txns can be GC
	if len(txnMgr._activeTxns) == 0 {
		//all txns can be GC
		idx = len(txnMgr._oldTxns)
	} else {
		idx = 0
	}

	for ; idx < len(txnMgr._oldTxns); idx++ {
		util.AssertFunc(txnMgr._oldTxns[idx]._highestActiveQuery.Load() > 0)
		if txnMgr._oldTxns[idx]._highestActiveQuery.Load() >=
			lActiveQuery {
			//query running may use this txn's data
			break
		}
	}

	if idx > 0 {
		//GC
		txnMgr._oldTxns = txnMgr._oldTxns[idx:]
	}
}

// id, start
func (txnMgr *TxnMgr) Lowest() (TxnType, TxnType) {
	txnMgr._lock.Lock()
	defer txnMgr._lock.Unlock()
	return txnMgr._lowestActiveId, txnMgr._lowestActiveStart
}

type Txn struct {
	_name       string
	_txnMgr     *TxnMgr
	_startTime  TxnType
	_id         TxnType
	_commitId   TxnType
	_undoBuffer UndoBuffer
	_storage    *LocalStorage
	//current active query.
	_activeQuery atomic.Uint64
	//when the txn finished. for GC
	_highestActiveQuery atomic.Uint64
}

func (txn *Txn) String() string {
	return fmt.Sprintf("[%s %d : %d %d]", txn._name, txn._id, txn._startTime, txn._commitId)
}

func (txn *Txn) Commit(commitId TxnType, ckp bool) error {
	txn._commitId = commitId

	var log *WriteAheadLog
	var sCommitState *StorageCommitState
	if GStorageMgr != nil {
		log = GStorageMgr._wal
		sCommitState = GStorageMgr.GenStorageCommitState(txn, ckp)
	}

	err := txn._storage.Commit(txn)
	if err != nil {
		return err
	}
	action := util.Check(util.FAULTS_SCOPE_TXN, "return_err_after_storage_commit")
	if action != nil {
		err = action.Action(action.Args)
		if err != nil {
			return err
		}
	}
	err = txn._undoBuffer.Commit(log, commitId)
	if err != nil {
		return err
	}

	//TODO: wrap it
	if sCommitState != nil {
		err = sCommitState.FlushCommit()
	}
	return err
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
	ptr := txn._undoBuffer.CreateEntry(DELETE_TUPLE, IdxType(deleteInfoSize))
	infos := util.PointerToSlice[DeleteInfo](ptr, int(deleteInfoSize))
	infos[0]._vinfo = info
	infos[0]._table = table
	infos[0]._count = count
	infos[0]._baseRow = baseRow
	copy(infos[0]._rows[:count], rows[:count])
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
	ptr := txn._undoBuffer.CreateEntry(UPDATE_TUPLE,
		IdxType(updateInfoSize)+(IdxType(common.Int32Size)+typeSize)*STANDARD_VECTOR_SIZE)
	infos := util.PointerToSlice[UpdateInfo](ptr, int(updateInfoSize))
	infos[0]._max = STANDARD_VECTOR_SIZE
	infos[0]._tuples = util.PointerToSlice[int](util.PointerAdd(ptr, int(updateInfoSize)), STANDARD_VECTOR_SIZE)
	infos[0]._tupleData = util.PointerAdd(ptr,
		int(updateInfoSize)+common.Int32Size*infos[0]._max)
	infos[0]._versionNumber.Store(uint64(txn._id))
	return &infos[0]
}

func (txn *Txn) PushCatalogEntry(ent *CatalogEntry) {
	ptr := txn._undoBuffer.CreateEntry(CATALOG_ENTRY,
		IdxType(catalogInfoSize))
	infos := util.PointerToSlice[CatalogInfo](ptr, int(catalogInfoSize))
	infos[0]._ent = ent
}

func (txn *Txn) SetActiveQuery(queryNo uint64) {
	txn._activeQuery.Store(queryNo)
}

func (txn *Txn) AutomaticCheckpoint() bool {
	estSize := txn._storage.EstimatedSize() + txn._undoBuffer.EstimatedSize()
	return GStorageMgr.AutomaticCheckpoint(estSize)
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

func (info *ChunkInfo) GetSelVector2(
	startTime TxnType,
	id TxnType,
	sel *chunk.SelectVector,
	maxCount IdxType) IdxType {
	switch info._type {
	case CONSTANT_INFO:
		return info.TemplatedGetSelVectorWithConstant(
			startTime,
			id,
			sel,
			maxCount,
			TxnVersionOp{})
	case VECTOR_INFO:
		return info.TemplatedGetSelVectorWithVector(
			startTime,
			id,
			sel,
			maxCount,
			TxnVersionOp{})
	default:
		panic("unexpected info type")
	}
}

func (info *ChunkInfo) GetSelVector(
	txn *Txn,
	sel *chunk.SelectVector,
	maxCount IdxType) IdxType {
	return info.GetSelVector2(txn._startTime, txn._id, sel, maxCount)
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

func (info *ChunkInfo) Delete(
	txnId TxnType,
	rows []RowType,
	count IdxType) IdxType {
	info._anyDeleted.Store(true)
	deleteTuples := IdxType(0)
	for i := IdxType(0); i < count; i++ {
		if info._deleted[rows[i]].Load() == uint64(txnId) {
			continue
		}

		if info._deleted[rows[i]].Load() != uint64(NotDeletedId) {
			panic(fmt.Sprintf("conflicts on txn %d deletion row. already deleted by other txn %d",
				txnId,
				info._deleted[rows[i]].Load()))
		}
		info._deleted[rows[i]].Store(uint64(txnId))
		rows[deleteTuples] = rows[i]
		deleteTuples++
	}
	return deleteTuples
}

func (info *ChunkInfo) Serialize(serial util.Serialize) error {
	if info._type == CONSTANT_INFO {
		isDel := info._insertId.Load() >= TRANSACTION_ID_START ||
			info._deleteId.Load() < TRANSACTION_ID_START
		if !isDel {
			return util.Write[ChunkInfoType](EMPTY_INFO, serial)
		}
		err := util.Write[ChunkInfoType](info._type, serial)
		if err != nil {
			return err
		}
		err = util.Write[IdxType](info._start, serial)
		if err != nil {
			return err
		}
		return nil
	} else {
		sel := chunk.NewSelectVector(STANDARD_VECTOR_SIZE)
		startTime := TxnType(TRANSACTION_ID_START - 1)
		txnId := TxnType(math.MaxUint64)
		count := info.GetSelVector2(
			startTime,
			txnId,
			sel,
			IdxType(STANDARD_VECTOR_SIZE))
		if count == STANDARD_VECTOR_SIZE {
			return util.Write[ChunkInfoType](EMPTY_INFO, serial)
		} else if count == 0 {
			err := util.Write[ChunkInfoType](CONSTANT_INFO, serial)
			if err != nil {
				return err
			}
			err = util.Write[IdxType](info._start, serial)
			if err != nil {
				return err
			}
			return err
		}
		err := util.Write[ChunkInfoType](info._type, serial)
		if err != nil {
			return err
		}
		err = util.Write[IdxType](info._start, serial)
		if err != nil {
			return err
		}
		var deletedTuples [STANDARD_VECTOR_SIZE]byte
		for i := 0; i < STANDARD_VECTOR_SIZE; i++ {
			deletedTuples[i] = 1
		}
		for i := IdxType(0); i < count; i++ {
			deletedTuples[sel.GetIndex(int(i))] = 0
		}

		return serial.WriteData(
			deletedTuples[:],
			STANDARD_VECTOR_SIZE,
		)
	}
}

func (info *ChunkInfo) Deserialize(src util.Deserialize) error {
	infoTyp := ChunkInfoType(0)
	err := util.Read[ChunkInfoType](&infoTyp, src)
	if err != nil {
		return err
	}
	switch infoTyp {
	case EMPTY_INFO:
		info._type = EMPTY_INFO
		return nil
	case CONSTANT_INFO:
		start := IdxType(0)
		err = util.Read[IdxType](&start, src)
		if err != nil {
			return err
		}
		info._type = CONSTANT_INFO
		info._start = start
		info._insertId.Store(0)
		info._deleteId.Store(0)
		return nil
	case VECTOR_INFO:
		start := IdxType(0)
		err = util.Read[IdxType](&start, src)
		if err != nil {
			return err
		}
		info._type = VECTOR_INFO
		info._start = start
		info._anyDeleted.Store(true)
		var deletedTuples [STANDARD_VECTOR_SIZE]byte
		err = src.ReadData(deletedTuples[:], STANDARD_VECTOR_SIZE)
		if err != nil {
			return err
		}
		for i := IdxType(0); i < STANDARD_VECTOR_SIZE; i++ {
			if deletedTuples[i] != 0 {
				info._deleted[i].Store(0)
			}
		}
	default:
		panic("unexpected info type")
	}
	return nil
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
	EMPTY_ENTRY   UndoFlags = 0
	INSERT_TUPLE  UndoFlags = 1
	DELETE_TUPLE  UndoFlags = 2
	UPDATE_TUPLE  UndoFlags = 3
	CATALOG_ENTRY UndoFlags = 4
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

func (undo *UndoBuffer) EstimatedSize() uint64 {
	sz := uint64(0)
	for _, ulog := range undo._logs {
		typ := util.Load[UndoFlags](ulog)
		switch typ {
		case EMPTY_ENTRY:
		case INSERT_TUPLE:
			sz += uint64(appendInfoSize)
		case DELETE_TUPLE:
			sz += uint64(deleteInfoSize)
		case UPDATE_TUPLE:
			sz += uint64(updateInfoSize)
		case CATALOG_ENTRY:
			sz += uint64(catalogInfoSize)
		}
	}
	return sz
}

type CommitState struct {
	_log              *WriteAheadLog
	_commitId         TxnType
	_currentTableInfo *DataTableInfo
	_rowIds           [STANDARD_VECTOR_SIZE]IdxType
	_deleteChunk      *chunk.Chunk
	_updateChunk      *chunk.Chunk
}

func (commit *CommitState) CommitEntry(
	typ UndoFlags,
	data unsafe.Pointer,
) error {
	switch typ {
	case CATALOG_ENTRY:
		infos := util.PointerToSlice[CatalogInfo](data, int(catalogInfoSize))
		info := infos[0]
		util.AssertFunc(info._ent._parent != nil)
		catalog := info._ent._catalog
		util.AssertFunc(catalog != nil)
		fun := func() error {
			catalog._writeLock.Lock()
			defer catalog._writeLock.Unlock()
			//update parent ent?
			info._ent._set.UpdateTimestamp(
				info._ent._parent,
				commit._commitId)
			if info._ent._name != info._ent._parent._name {
				info._ent._set.UpdateTimestamp(
					info._ent,
					commit._commitId)
			}
			if commit._log != nil {
				err := commit.WriteCatalogEntry(
					info._ent,
				)
				if err != nil {
					return err
				}
			}
			return nil
		}
		err := fun()
		if err != nil {
			return err
		}
	case INSERT_TUPLE:
		infos := util.PointerToSlice[AppendInfo](data, int(appendInfoSize))
		info := infos[0]
		if commit._log != nil {
			err := info._table.WriteToLog(
				commit._log,
				info._startRow,
				info._count)
			if err != nil {
				return err
			}
		}
		info._table.CommitAppend(
			commit._commitId,
			info._startRow,
			info._count)
	case DELETE_TUPLE:
		infos := util.PointerToSlice[DeleteInfo](data, int(deleteInfoSize))
		info := infos[0]
		if commit._log != nil {
			err := commit.WriteDelete(&info)
			if err != nil {
				return err
			}
		}
		info._vinfo.CommitDelete(
			commit._commitId,
			info._rows[:],
			info._count)
	case UPDATE_TUPLE:
		infos := util.PointerToSlice[UpdateInfo](data, int(updateInfoSize))
		info := &infos[0]
		if commit._log != nil {
			err := commit.WriteUpdate(info)
			if err != nil {
				return err
			}
		}
		info._versionNumber.Store(uint64(commit._commitId))
	}
	return nil
}

func (commit *CommitState) WriteDelete(info *DeleteInfo) error {
	err := commit.SwitchTable(info._table._info, DELETE_TUPLE)
	if err != nil {
		return err
	}
	if commit._deleteChunk == nil {
		typs := []common.LType{common.BigintType()}
		commit._deleteChunk = &chunk.Chunk{}
		commit._deleteChunk.Init(typs, STANDARD_VECTOR_SIZE)
	}
	rowsSlice := chunk.GetSliceInPhyFormatFlat[RowType](commit._deleteChunk.Data[0])
	for i := IdxType(0); i < info._count; i++ {
		rowsSlice[i] = RowType(info._baseRow) + info._rows[i]
	}
	commit._deleteChunk.SetCard(int(info._count))
	return commit._log.WriteDelete(commit._deleteChunk)
}

func (commit *CommitState) SwitchTable(info *DataTableInfo, tuple UndoFlags) error {
	if commit._currentTableInfo != info {
		err := commit._log.WriteSetTable(info._schema, info._table)
		if err != nil {
			return err
		}
		commit._currentTableInfo = info
	}
	return nil
}

func (commit *CommitState) WriteUpdate(info *UpdateInfo) error {
	colData := info._segment._colData
	tableInfo := colData._info
	err := commit.SwitchTable(tableInfo, UPDATE_TUPLE)
	if err != nil {
		return err
	}

	updateTyps := make([]common.LType, 0)
	if colData._typ.Id == common.LTID_VALIDITY {
		updateTyps = append(updateTyps, common.BooleanType())
	} else {
		updateTyps = append(updateTyps, colData._typ)
	}
	//row type
	updateTyps = append(updateTyps, common.BigintType())

	updateChunk := &chunk.Chunk{}
	updateChunk.Init(updateTyps, STANDARD_VECTOR_SIZE)
	commit._updateChunk = updateChunk

	//fetch updated values from the base segment
	info._segment.FetchCommitted(info._vectorIndex, updateChunk.Data[0])

	//save row ids
	rowSlice := chunk.GetSliceInPhyFormatFlat[RowType](updateChunk.Data[1])
	start := colData._start + info._vectorIndex*STANDARD_VECTOR_SIZE
	for i := 0; i < info._N; i++ {
		rowSlice[info._tuples[i]] =
			RowType(start + IdxType(info._tuples[i]))
	}

	if colData._typ.Id == common.LTID_VALIDITY {
		//init the validity
		validSlice := chunk.GetSliceInPhyFormatFlat[bool](updateChunk.Data[0])
		for i := 0; i < info._N; i++ {
			validSlice[info._tuples[i]] = false
		}
	}

	sel := chunk.NewSelectVector3(info._tuples)
	commit._updateChunk.SliceItself(sel, info._N)

	colIdx := make([]IdxType, 0)
	cur := colData
	for cur._parent != nil {
		colIdx = append(colIdx, cur._columnIndex)
		cur = cur._parent
	}
	colIdx = append(colIdx, info._columnIndex)
	slices.Reverse(colIdx)

	err = commit._log.WriteUpdate(updateChunk, colIdx)
	return err
}

func (commit *CommitState) WriteCatalogEntry(
	ent *CatalogEntry) error {
	parent := ent._parent
	switch parent._typ {
	case CatalogTypeTable:
		return commit._log.WriteCreateTable(parent)
	case CatalogTypeSchema:
		if ent._typ == CatalogTypeSchema {
			//skip alter schema
			return nil
		}
		return commit._log.WriteCreateSchema(parent)
	}
	return nil
}

type RollbackState struct {
}

func (rollback *RollbackState) RollbackEntry(
	typ UndoFlags,
	data unsafe.Pointer,
) {
	switch typ {
	case CATALOG_ENTRY:
		infos := util.PointerToSlice[CatalogInfo](data, int(catalogInfoSize))
		info := infos[0]
		util.AssertFunc(info._ent._set != nil)
		info._ent._set.Undo(info._ent)
	case INSERT_TUPLE:
		infos := util.PointerToSlice[AppendInfo](data, int(appendInfoSize))
		info := infos[0]
		//delete the appended data in base table
		info._table.RevertAppend(info._startRow, info._count)
	case DELETE_TUPLE:
		infos := util.PointerToSlice[DeleteInfo](data, int(deleteInfoSize))
		info := infos[0]
		info._vinfo.CommitDelete(NotDeletedId, info._rows[:], info._count)
	case UPDATE_TUPLE:
		infos := util.PointerToSlice[UpdateInfo](data, int(updateInfoSize))
		info := &infos[0]
		info._segment.RollbackUpdate(info)
	case EMPTY_ENTRY:
	default:
		panic("usp")
	}
}

type CleanupState struct {
}

func (cleanup *CleanupState) CleanupEntry(
	typ UndoFlags,
	data unsafe.Pointer,
) {

}

var (
	appendInfoSize  = unsafe.Sizeof(AppendInfo{})
	deleteInfoSize  = unsafe.Sizeof(DeleteInfo{})
	updateInfoSize  = unsafe.Sizeof(UpdateInfo{})
	catalogInfoSize = unsafe.Sizeof(CatalogInfo{})
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
	_rows    [STANDARD_VECTOR_SIZE]RowType
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

type CatalogInfo struct {
	_ent *CatalogEntry
}
