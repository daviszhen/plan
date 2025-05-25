package storage

import (
	"errors"
	"fmt"
	"os"
	"sync"

	treemap "github.com/liyue201/gostl/ds/map"

	"github.com/daviszhen/plan/pkg/chunk"
	"github.com/daviszhen/plan/pkg/common"
	"github.com/daviszhen/plan/pkg/util"
)

const (
	defaultDbPath = "/tmp/default"
)

var GCatalog *Catalog
var GStorageMgr *StorageMgr

func init() {
	GTxnMgr = NewTxnMgr()
	GBufferMgr = NewBufferManager(".")
	GCatalog = NewCatalog()
	if err := GCatalog.Init(); err != nil {
		panic(err)
	}
	GStorageMgr = NewStorageMgr(defaultDbPath, false)
	if err := GStorageMgr.LoadDatabase(); err != nil {
		panic(err)
	}
}

type LocalTableStorage struct {
	_table      *DataTable
	_rowGroups  *RowGroupCollection
	_deleteRows IdxType
	_indexes    *TableIndexList
}

func NewLocalTableStorage(table *DataTable) *LocalTableStorage {
	ret := &LocalTableStorage{
		_table:   table,
		_indexes: &TableIndexList{},
	}
	ret._rowGroups = NewRowGroupCollection(
		table._info,
		GStorageMgr._blockMgr,
		table.GetTypes(),
		IdxType(MAX_ROW_ID),
		0,
	)
	ret._rowGroups.InitializeEmpty()

	table._info._indexes.Scan(func(index *Index) bool {
		if index._constraintType != IndexConstraintTypeNone {

			newIndex := NewIndex(
				IndexTypeBPlus,
				index._blockMgr,
				index._columnIds,
				index._logicalTypes,
				index._constraintType,
				nil,
			)
			ret._indexes.AddIndex(newIndex)
		}
		return false
	})

	return ret
}

func (storage *LocalTableStorage) WriteNewRowGroup() {
	if storage._deleteRows != 0 {
		return
	}
}

func (storage *LocalTableStorage) FlushBlocks() {

}

func (storage *LocalTableStorage) Rollback() {

}

func (storage *LocalTableStorage) InitScan(state *CollectionScanState) {
	if storage._rowGroups._totalRows.Load() == 0 {
		return
	}
	storage._rowGroups.InitScan(state, state.GetColumnIds())
}

func (storage *LocalTableStorage) AppendToIndexes(
	txn *Txn,
	appendState *TableAppendState,
	appendCount IdxType,
	appendToTable bool) error {
	if appendToTable {
		storage._table.InitAppend(txn, appendState, appendCount)
	}
	var err error
	if appendToTable {
		storage._rowGroups.Scan(txn, func(data *chunk.Chunk) bool {
			err = storage._table.AppendToIndexes(
				data,
				uint64(appendState._currentRow))
			if err != nil {
				return false
			}
			storage._table.Append(data, appendState)
			return true
		})
	} else {
		err = storage.AppendToIndexes2(
			txn,
			storage._rowGroups,
			storage._table._info._indexes,
			storage._table.GetTypes(),
			uint64(appendState._currentRow),
		)
	}

	if err != nil {
		//revert append
		currentRow := appendState._rowStart
		storage._rowGroups.Scan(txn, func(data *chunk.Chunk) bool {
			err2 := storage._table.RemoveFromIndexes(appendState, data, currentRow)
			if err2 != nil {
				err = errors.Join(err, err2)
				return false
			}

			currentRow += RowType(data.Card())
			if currentRow >= appendState._currentRow {
				return false
			}
			return true
		})
		if appendToTable {
			storage._table.RevertAppendInternal(
				IdxType(appendState._rowStart),
				appendCount,
			)
		}
	}
	return err
}

func (storage *LocalTableStorage) AppendToIndexes2(
	txn *Txn,
	src *RowGroupCollection,
	indexList *TableIndexList,
	tableTypes []common.LType,
	startRow uint64,
) error {
	var err error
	cols := indexList.GetRequiredColumns()
	mockData := &chunk.Chunk{}
	mockData.Init(tableTypes, STANDARD_VECTOR_SIZE)

	src.Scan2(txn, cols, func(data *chunk.Chunk) bool {
		for i, colIdx := range cols {
			mockData.Data[colIdx].Reference(data.Data[i])
		}
		mockData.SetCard(data.Card())
		err = AppendToIndexes(indexList, mockData, startRow)
		if err != nil {
			return false
		}
		startRow += uint64(data.Card())
		return true
	})
	return err
}

func (storage *LocalTableStorage) EstimatedSize() uint64 {
	appendRows := storage._rowGroups._totalRows.Load() - uint64(storage._deleteRows)
	if appendRows == 0 {
		return 0
	}
	sz := uint64(0)
	for _, typ := range storage._rowGroups._types {
		sz += uint64(typ.GetInternalType().Size())
	}
	return appendRows * sz
}

type LocalStorage struct {
	_txn              *Txn
	_tableStorageLock sync.Mutex
	_tableStorage     *treemap.Map[*DataTable, *LocalTableStorage]
}

func NewLocalStorage(txn *Txn) *LocalStorage {
	cmp := func(a, b *DataTable) int {
		return a._info.comp(b._info)
	}
	temp := treemap.New[*DataTable, *LocalTableStorage](cmp)
	return &LocalStorage{
		_txn:          txn,
		_tableStorage: temp,
	}
}

func (storage *LocalStorage) Commit(txn *Txn) error {
	for iter := storage._tableStorage.Begin(); iter.IsValid(); iter.Next() {
		table := iter.Key()
		tableStorage := iter.Value()
		err := storage.Flush(table, tableStorage)
		if err != nil {
			return err
		}
	}
	storage._tableStorage.Clear()
	return nil
}

func (storage *LocalStorage) Rollback() {
	for iter := storage._tableStorage.Begin(); iter.IsValid(); iter.Next() {
		tableStorage := iter.Value()
		tableStorage.Rollback()
	}
	storage._tableStorage.Clear()
}

func (storage *LocalStorage) Changed() bool {
	storage._tableStorageLock.Lock()
	defer storage._tableStorageLock.Unlock()
	return storage._tableStorage.Size() != 0
}

func (storage *LocalStorage) InitializeAppend(
	state *LocalAppendState,
	table *DataTable,
) {
	state._storage = storage.getOrCreateStorage(table)
	state._storage._rowGroups.InitAppend(
		storage._txn,
		&state._appendState,
		0,
	)
}

func (storage *LocalStorage) Append(
	state *LocalAppendState,
	data *chunk.Chunk,
) {
	ls := state._storage

	baseId := uint64(MAX_ROW_ID) +
		ls._rowGroups._totalRows.Load() +
		uint64(state._appendState._totalAppendCount)
	err := AppendToIndexes(ls._indexes, data, baseId)
	if err != nil {
		panic(err)
	}

	nrg := ls._rowGroups.Append(data, &state._appendState)
	if nrg {
		ls.WriteNewRowGroup()
	}
}

func (storage *LocalStorage) FinalizeAppend(
	state *LocalAppendState,
) {
	state._storage._rowGroups.FinalizeAppend(state._appendState._txn, &state._appendState)
}

func (storage *LocalStorage) getOrCreateStorage(table *DataTable) *LocalTableStorage {
	storage._tableStorageLock.Lock()
	defer storage._tableStorageLock.Unlock()
	get, err := storage._tableStorage.Get(table)
	if err != nil {
		lts := NewLocalTableStorage(table)
		storage._tableStorage.Insert(table, lts)
		return lts
	}
	return get
}

func (storage *LocalStorage) getStorage(table *DataTable) *LocalTableStorage {
	storage._tableStorageLock.Lock()
	defer storage._tableStorageLock.Unlock()
	get, err := storage._tableStorage.Get(table)
	if err != nil {
		return nil
	}
	return get
}

func (storage *LocalStorage) Flush(
	table *DataTable,
	ltStorage *LocalTableStorage) error {
	if IdxType(ltStorage._rowGroups._totalRows.Load()) <= ltStorage._deleteRows {
		return nil
	}
	appendCount := IdxType(ltStorage._rowGroups._totalRows.Load()) - ltStorage._deleteRows
	var appendState TableAppendState
	release := table.AppendLock(&appendState)
	defer release()
	if (appendState._rowStart == 0 ||
		ltStorage._rowGroups._totalRows.Load() >= MERGE_THRESHOLD) &&
		ltStorage._deleteRows == 0 {
		ltStorage.FlushBlocks()
		if !table._info._indexes.Empty() {
			err := ltStorage.AppendToIndexes(
				storage._txn,
				&appendState,
				appendCount,
				false,
			)
			if err != nil {
				return err
			}
		}
		table.MergeStorage(ltStorage._rowGroups)
	} else {
		ltStorage.Rollback()
		err := ltStorage.AppendToIndexes(
			storage._txn,
			&appendState,
			appendCount,
			true)
		if err != nil {
			return err
		}
	}
	storage._txn.PushAppend(table, IdxType(appendState._rowStart), appendCount)
	return nil
}

func (storage *LocalStorage) InitScan(table *DataTable, state *CollectionScanState) {
	lstorage := storage.getStorage(table)
	if lstorage == nil {
		return
	}
	lstorage.InitScan(state)
}

func (storage *LocalStorage) Scan(state *CollectionScanState, ids []IdxType, result *chunk.Chunk) {
	state.Scan(storage._txn, result)
}

func (storage *LocalStorage) Delete(
	table *DataTable,
	rowIds *chunk.Vector,
	count IdxType) IdxType {
	lts := storage.getStorage(table)
	ids := chunk.GetSliceInPhyFormatFlat[RowType](rowIds)
	deleteCount := lts._rowGroups.Delete(storage._txn, table, ids, count)
	lts._deleteRows += deleteCount
	return deleteCount
}

func (storage *LocalStorage) Update(table *DataTable, rowIds *chunk.Vector, colIds []IdxType, updates *chunk.Chunk) {
	lts := storage.getStorage(table)
	ids := chunk.GetSliceInPhyFormatFlat[RowType](rowIds)
	lts._rowGroups.Update(storage._txn, ids, colIds, updates)
}

func (storage *LocalStorage) EstimatedSize() uint64 {
	storage._tableStorageLock.Lock()
	defer storage._tableStorageLock.Unlock()
	sz := uint64(0)
	storage._tableStorage.Traversal(func(key *DataTable, value *LocalTableStorage) bool {
		sz += value.EstimatedSize()
		return true
	})
	return sz
}

type TableAppendState struct {
	_rowGroupAppendState RowGroupAppendState
	_appendLock          *sync.Mutex
	_rowStart            RowType
	_currentRow          RowType
	_totalAppendCount    IdxType
	_startRowGroup       *RowGroup
	_txn                 *Txn
	_remaining           IdxType
}

type LocalAppendState struct {
	_appendState TableAppendState
	_storage     *LocalTableStorage
}

type StorageMgr struct {
	_path     string
	_readOnly bool
	_wal      *WriteAheadLog
	_blockMgr BlockMgr
}

func NewStorageMgr(path string, readOnly bool) *StorageMgr {
	return &StorageMgr{
		_path:     path,
		_readOnly: readOnly,
	}
}

func (storage *StorageMgr) LoadDatabase() error {
	var err error
	walPath := storage._path + ".wal"
	truncateWal := false
	if !util.FileIsValid(storage._path) {
		if storage._readOnly {
			return fmt.Errorf("database not found")
		}
		if util.FileIsValid(walPath) {
			if err = os.Remove(walPath); err != nil {
				return err
			}
		}
		//init blockMgr
		//create new database
		fBlockMgr := NewFileBlockMgr(
			GBufferMgr,
			storage._path,
			storage._readOnly)
		err = fBlockMgr.CreateNewDatabase()
		if err != nil {
			return err
		}
		storage._blockMgr = fBlockMgr

	} else {
		//load existing database
		fBlockMgr := NewFileBlockMgr(
			GBufferMgr,
			storage._path,
			storage._readOnly)
		err = fBlockMgr.LoadExistingDatabase()
		if err != nil {
			return err
		}
		storage._blockMgr = fBlockMgr

		//checkpoint reader
		ckp := NewFileCheckpointReader(storage)
		err = ckp.LoadFromStorage()
		if err != nil {
			return err
		}
		storage._blockMgr.ClearMetaBlockHandles()
		if util.FileIsValid(walPath) {
			truncateWal, err = Replay(walPath)
			if err != nil {
				return err
			}
		}
	}
	//init wal
	if !storage._readOnly {
		storage._wal, err = NewWriteAheadLog(walPath)
		if err != nil {
			return err
		}
		if truncateWal {
			err = storage._wal.Truncate(0)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (storage *StorageMgr) Close() error {
	return nil
}

func (storage *StorageMgr) AutomaticCheckpoint(estWalBytes uint64) bool {
	if storage == nil || storage._wal == nil {
		return false
	}
	initSize := storage._wal.GetWalSize()
	if uint64(initSize)+estWalBytes > (5 * 1024 * 1024) {
		return true
	}
	return false
}

func (storage *StorageMgr) GenStorageCommitState(txn *Txn, ckp bool) *StorageCommitState {
	return NewStorageCommitState(storage, ckp)
}

func (storage *StorageMgr) IsCheckpointClean(id BlockID) bool {
	return storage._blockMgr.IsRootBlock(id)
}

func (storage *StorageMgr) CreateCheckpoint(
	delWal bool,
	forceCkp bool,
) error {
	if storage._readOnly || storage._wal == nil {
		return nil
	}
	if storage._wal.GetWalSize() > 0 || forceCkp {
		ckp := NewCheckpointWriter(storage)
		err := ckp.CreateCheckpoint()
		if err != nil {
			return err
		}
	}
	if delWal {
		err := storage._wal.Delete()
		if err != nil {
			return err
		}
		storage._wal = nil
	}
	return nil
}

func (storage *StorageMgr) Size() DatabaseSize {
	return DatabaseSize{}
}

type StorageCommitState struct {
	_initWalSize IdxType
	_initWritten IdxType
	_log         *WriteAheadLog
	_ckp         bool
}

func NewStorageCommitState(
	storage *StorageMgr,
	ckp bool,
) *StorageCommitState {
	ret := &StorageCommitState{
		_log: storage._wal,
		_ckp: ckp,
	}
	if ret._log != nil {
		initSize := ret._log.GetWalSize()
		ret._initWalSize = IdxType(initSize)
		if ckp {
			ret._log._skipWriting = true
		}
	}

	return ret
}

func (state *StorageCommitState) FlushCommit() error {
	if state._log != nil {
		err := state._log.Flush()
		if err != nil {
			return err
		}
		state._log._skipWriting = false
	}
	state._log = nil
	return nil
}

func (state *StorageCommitState) Close() error {
	if state._log != nil {
		state._log._skipWriting = false
		err := state._log.Truncate(int64(state._initWalSize))
		if err != nil {
			return err
		}
	}
	return nil
}

type DatabaseSize struct {
}
