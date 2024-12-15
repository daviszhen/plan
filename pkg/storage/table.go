package storage

import (
	"sync"
	"sync/atomic"

	"github.com/daviszhen/plan/pkg/chunk"
	"github.com/daviszhen/plan/pkg/common"
	"github.com/daviszhen/plan/pkg/util"
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

func (table *DataTable) FinalizeLocalAppend(txn *Txn, state *LocalAppendState) {
	txn._storage.FinalizeAppend(state)
}

func (table *DataTable) InitializeScan(
	txn *Txn,
	state *TableScanState,
	columnIds []IdxType,
) {
	state.Init(columnIds)
	table._rowGroups.InitScan(&state._tableState, columnIds)
	txn._storage.InitScan(table, &state._localState)
}

func (table *DataTable) Scan(
	txn *Txn,
	result *chunk.Chunk,
	state *TableScanState,
) {
	if state._tableState.Scan(txn, result) {
		util.AssertFunc(result.Card() > 0)
		return
	}
	txn._storage.Scan(&state._localState, state.GetColumnIds(), result)
}

type SegmentScanState struct {
}

type ColumnScanState struct {
	_current        *ColumnSegment
	_segmentTree    *ColumnSegmentTree
	_rowIdx         IdxType
	_internalIdx    IdxType
	_scanState      *SegmentScanState
	_childStates    []ColumnScanState
	_initialized    bool
	_segmentChecked bool
	_version        IdxType
	_previousStates []*SegmentScanState
	_lastOffset     IdxType
}

func (state *ColumnScanState) Init(typ common.LType) {
	if typ.Id == common.LTID_VALIDITY {
		return
	}
	state._childStates = make([]ColumnScanState, 1)
}

func (state *ColumnScanState) NextInternal(count IdxType) {
	if state._current == nil {
		return
	}
	state._rowIdx += count
	for state._rowIdx >=
		state._current._start+IdxType(state._current._count.Load()) {
		state._current = state._segmentTree.GetNextSegment(state._current)
		state._initialized = false
		state._segmentChecked = false
		if state._current == nil {
			break
		}
	}
}

func (state *ColumnScanState) Next(count IdxType) {
	state.NextInternal(count)
	for _, childState := range state._childStates {
		childState.Next(count)
	}
}

type CollectionScanState struct {
	_rowGroup       *RowGroup
	_vectorIdx      IdxType
	_maxRowGroupRow IdxType
	_columnScans    []*ColumnScanState
	_rowGroups      *RowGroupSegmentTree
	_maxRow         IdxType
	_batchIdx       IdxType
	_parent         *TableScanState
}

func (state *CollectionScanState) Init(types []common.LType) {
	colIds := state.GetColumnIds()
	state._columnScans = make([]*ColumnScanState, len(colIds))
	for i := 0; i < len(colIds); i++ {
		if colIds[i] == IdxType(-1) {
			continue
		}
		state._columnScans[i].Init(types[colIds[i]])
	}
}

func (state *CollectionScanState) GetColumnIds() []IdxType {
	return state._parent.GetColumnIds()
}

func (state *CollectionScanState) Scan(
	txn *Txn,
	result *chunk.Chunk) bool {
	for state._rowGroup != nil {
		state._rowGroup.Scan(txn, state, result)
		if result.Card() > 0 {
			return true
		} else if state._maxRow <=
			state._rowGroup._start+IdxType(state._rowGroup._count.Load()) {
			state._rowGroup = nil
			return false
		} else {
			fun := func() {
				for {
					release := state._rowGroups.Lock()
					state._rowGroup =
						state._rowGroups.GetNextSegment(state._rowGroup)
					release()
					if state._rowGroup != nil {
						if state._rowGroup._start >= state._maxRow {
							state._rowGroup = nil
							break
						}
						scanRg := state._rowGroup.InitScan(state)
						if scanRg {
							break
						}
					} else {
						break
					}
				}
			}
			fun()
		}
	}
	return false
}

type TableScanState struct {
	_tableState CollectionScanState
	_localState CollectionScanState
	_columnIds  []IdxType
}

func (state *TableScanState) Init(ids []IdxType) {
	state._columnIds = ids
}

func (state *TableScanState) GetColumnIds() []IdxType {
	return state._columnIds
}
