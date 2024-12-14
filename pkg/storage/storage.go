package storage

import (
	"sync"

	"github.com/liyue201/gostl/ds/map"

	"github.com/daviszhen/plan/pkg/chunk"
)

type LocalTableStorage struct {
	_table      *DataTable
	_rowGroups  *RowGroupCollection
	_deleteRows IdxType
}

func NewLocalTableStorage(table *DataTable) *LocalTableStorage {
	ret := &LocalTableStorage{
		_table: table,
	}
	ret._rowGroups = NewRowGroupCollection(
		table._info,
		NewBlockManager(GBufferMgr),
		table.GetTypes(),
		IdxType(MAX_ROW_ID),
		0,
	)
	ret._rowGroups.InitializeEmpty()
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
		storage.Flush(table, tableStorage)
	}
	return nil
}

func (storage *LocalStorage) Rollback() {

}

func (storage *LocalStorage) Changed() bool {
	return false
}

func (storage *LocalStorage) InitializeAppend(
	state *LocalAppendState,
	table *DataTable,
) {
	state._storage = storage.getOrCreateStorage(table)
	state._storage._rowGroups.InitializeAppend(
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

func (storage *LocalStorage) Flush(
	table *DataTable,
	ltStorage *LocalTableStorage) {
	if IdxType(ltStorage._rowGroups._totalRows.Load()) <= ltStorage._deleteRows {
		return
	}
	appendCount := IdxType(ltStorage._rowGroups._totalRows.Load()) - ltStorage._deleteRows
	var appendState TableAppendState
	release := table.AppendLock(&appendState)
	defer release()
	if (appendState._rowStart == 0 ||
		ltStorage._rowGroups._totalRows.Load() >= MERGE_THRESHOLD) &&
		ltStorage._deleteRows == 0 {
		ltStorage.FlushBlocks()
		table.MergeStorage(ltStorage._rowGroups)
	} else {
		ltStorage.Rollback()
	}
	storage._txn.PushAppend(table, IdxType(appendState._rowStart), appendCount)
}

type TableAppendState struct {
	_rowGroupAppendState RowGroupAppendState
	_appendLock          sync.Mutex
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
