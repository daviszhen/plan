package storage

import (
	"sync"
	"sync/atomic"

	"github.com/daviszhen/plan/pkg/chunk"
	"github.com/daviszhen/plan/pkg/common"
)

type ColumnDefinition struct {
	_name string
	_type common.LType
}

type DataTableInfo struct {
	_schema string
	_name   string
	_card   atomic.Uint64
}

func (info *DataTableInfo) comp(o *DataTableInfo) int {
	if info._schema < o._schema {
		return -1
	} else if info._schema > o._schema {
		return 1
	} else {
		if info._name < o._name {
			return -1
		} else if info._name > o._name {
			return 1
		} else {
			return 0
		}
	}
}

type DataTable struct {
	_info       *DataTableInfo
	_colDefs    []*ColumnDefinition
	_appendLock sync.Mutex
	_rowGroups  *RowGroupCollection
}

func NewDataTable(
	schema, table string,
	colDefs []*ColumnDefinition) *DataTable {
	info := &DataTableInfo{
		_schema: schema,
		_name:   table,
	}

	dTable := &DataTable{
		_info:    info,
		_colDefs: colDefs,
	}

	types := make([]common.LType, 0)
	for _, colDef := range colDefs {
		types = append(types, colDef._type)
	}
	dTable._rowGroups = NewRowGroupCollection(
		info,
		NewBlockManager(GBufferMgr),
		types,
		0,
		0,
	)
	dTable._rowGroups.InitializeEmpty()

	return dTable
}

func (table *DataTable) GetTypes() []common.LType {
	types := make([]common.LType, 0)
	for _, colDef := range table._colDefs {
		types = append(types, colDef._type)
	}
	return types
}

func (table *DataTable) InitializeLocalAppend(
	txn *Txn,
	state *LocalAppendState) {
	txn._storage.InitializeAppend(state, table)
}

func (table *DataTable) LocalAppend(
	txn *Txn,
	state *LocalAppendState,
	data *chunk.Chunk,
	unsafe bool,
) {
	if data.Card() == 0 {
		return
	}
	txn._storage.Append(state, data)
}

func (table *DataTable) AppendLock(state *TableAppendState) func() {
	state._appendLock.Lock()
	state._rowStart = RowType(table._rowGroups._totalRows.Load())
	state._currentRow = state._rowStart
	return func() {
		state._appendLock.Unlock()
	}
}

func (table *DataTable) MergeStorage(data *RowGroupCollection) {
	table._rowGroups.MergeStorage(data)
}

func (table *DataTable) CommitAppend(
	commitTd TxnType,
	rowStart IdxType,
	count IdxType) {
	table._appendLock.Lock()
	defer table._appendLock.Unlock()
	table._rowGroups.CommitAppend(commitTd, rowStart, count)
	table._info._card.Add(uint64(count))
}
