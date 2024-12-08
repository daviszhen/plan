package storage

import (
	"sync"

	"github.com/liyue201/gostl/ds/map"

	"github.com/daviszhen/plan/pkg/chunk"
)

type LocalTableStorage struct {
	_table     *DataTable
	_rowGroups *RowGroupCollection
}

func (storage *LocalTableStorage) WriteNewRowGroup() {

}

type LocalStorage struct {
	_txn              *Txn
	_tableStorageLock sync.Mutex
	_tableStorage     treemap.Map[*DataTable, *LocalTableStorage]
}

func (storage *LocalStorage) Commit(txn *Txn) error {

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

func (storage *LocalStorage) getOrCreateStorage(table *DataTable) *LocalTableStorage {
	storage._tableStorageLock.Lock()
	defer storage._tableStorageLock.Unlock()
	get, err := storage._tableStorage.Get(table)
	if err != nil {
		lts := &LocalTableStorage{
			_table: table,
		}
		storage._tableStorage.Insert(table, lts)
		return lts
	}
	return get
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
