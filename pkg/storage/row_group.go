package storage

import (
	"sync"
	"sync/atomic"

	treeset "github.com/liyue201/gostl/ds/set"

	"github.com/daviszhen/plan/pkg/chunk"
	"github.com/daviszhen/plan/pkg/common"
	"github.com/daviszhen/plan/pkg/util"
)

const (
	STANDARD_VECTOR_SIZE     = 2048
	STANDARD_ROW_GROUPS_SIZE = 122880
	ROW_GROUP_SIZE           = STANDARD_ROW_GROUPS_SIZE
	ROW_GROUP_VECTOR_COUNT   = ROW_GROUP_SIZE / STANDARD_VECTOR_SIZE
)

type RowGroup struct {
	SegmentBase[RowGroup]
	_collect      *RowGroupCollection
	_versionInfo  *VersionNode
	_columns      []*ColumnData
	_rowGroupLock sync.Mutex
}

func (rg *RowGroup) InitEmpty(types []common.LType) {
	util.AssertFunc(len(rg._columns) == 0)
	for i, lt := range types {
		cd := NewColumnData(
			rg._collect._blockMgr,
			rg._collect._info,
			i,
			rg._start,
			lt,
		)
		rg._columns = append(rg._columns, cd)
	}
}

func (rg *RowGroup) ColumnCount() int {
	return len(rg._columns)
}

func (rg *RowGroup) GetColumn(i int) *ColumnData {
	return rg._columns[i]
}

func (rg *RowGroup) InitAppend(state *RowGroupAppendState) {
	state._rowGroup = rg
	state._offsetInRowGroup = IdxType(rg._count.Load())
	state._states = make([]*ColumnAppendState, len(rg._columns))
	for i := 0; i < len(state._states); i++ {
		col := rg.GetColumn(i)
		col.InitAppend(state._states[i])
	}
}

func (rg *RowGroup) AppendVersionInfo(txn *Txn, count IdxType) {
	rowGroupStart := IdxType(rg._count.Load())
	rowGroupEnd := rowGroupStart + count
	if rowGroupEnd > ROW_GROUP_SIZE {
		rowGroupEnd = ROW_GROUP_SIZE
	}
	rg._rowGroupLock.Lock()
	defer rg._rowGroupLock.Unlock()
	if rg._versionInfo == nil {
		rg._versionInfo = &VersionNode{}
	}
	startVectorIdx := rowGroupStart / STANDARD_VECTOR_SIZE
	endVectorIdx := (rowGroupEnd - 1) / STANDARD_VECTOR_SIZE
	for idx := startVectorIdx; idx <= endVectorIdx; idx++ {
		start := IdxType(0)
		end := IdxType(0)
		if idx == startVectorIdx {
			start = rowGroupStart - startVectorIdx*STANDARD_VECTOR_SIZE
		}
		if idx == endVectorIdx {
			end = rowGroupEnd - endVectorIdx*STANDARD_VECTOR_SIZE
		} else {
			end = STANDARD_VECTOR_SIZE
		}
		if start == 0 && end == STANDARD_VECTOR_SIZE {
			//full vector
			info := &ChunkInfo{
				_type:  CONSTANT_INFO,
				_start: rg._start + idx*STANDARD_VECTOR_SIZE,
			}
			info._insertId.Store(uint64(txn._id))
			info._deleteId.Store(uint64(NotDeletedId))
			rg._versionInfo._info[idx] = info
		} else {
			var info *ChunkInfo
			if rg._versionInfo._info[idx] == nil {
				//first time to vector
				info = &ChunkInfo{
					_type:  VECTOR_INFO,
					_start: rg._start + idx*STANDARD_VECTOR_SIZE,
				}
				rg._versionInfo._info[idx] = info
			} else {
				info = rg._versionInfo._info[idx]
			}
			info.Append(start, end, txn._id)
		}
	}
	rg._count.Store(uint64(rowGroupEnd))
}

func (rg *RowGroup) Append(
	state *RowGroupAppendState,
	data *chunk.Chunk,
	cnt IdxType) {
	for i := 0; i < rg.ColumnCount(); i++ {
		col := rg.GetColumn(i)
		col.Append(state._states[i], data.Data[i], cnt)
	}
	state._offsetInRowGroup += cnt
}

func NewRowGroup(collect *RowGroupCollection, start IdxType, count IdxType) *RowGroup {
	rg := &RowGroup{
		SegmentBase: SegmentBase[RowGroup]{
			_start: start,
		},
		_collect: collect,
	}
	rg._count.Store(uint64(count))

	return rg
}

type RowGroupCollection struct {
	_blockMgr  *BlockManager
	_totalRows atomic.Uint32
	_info      *DataTableInfo
	_types     []common.LType
	_rowStart  IdxType
	_rowGroups *RowGroupSegmentTree
}

func (collect *RowGroupCollection) InitializeAppend(
	txn *Txn,
	state *TableAppendState,
	appendCount IdxType) {
	state._rowStart = RowType(collect._totalRows.Load())
	state._currentRow = state._rowStart
	state._totalAppendCount = 0
	lock := collect._rowGroups.Lock()
	defer lock()
	if collect._rowGroups.IsEmpty() {
		collect.AppendRowGroup(collect._rowStart)
	}
	state._startRowGroup = collect._rowGroups.GetLastSegment()
	state._startRowGroup.InitAppend(&state._rowGroupAppendState)
	state._remaining = appendCount
	state._txn = txn
	if state._remaining > 0 {
		state._startRowGroup.AppendVersionInfo(txn, state._remaining)
		collect._totalRows.Add(uint32(state._remaining))
	}
}

func (collect *RowGroupCollection) Append(data *chunk.Chunk, state *TableAppendState) bool {
	newRg := false
	appendCount := IdxType(data.Card())
	remaining := IdxType(data.Card())
	state._totalAppendCount += appendCount
	for {
		curRg := state._rowGroupAppendState._rowGroup
		cnt := min(remaining,
			ROW_GROUP_SIZE-state._rowGroupAppendState._offsetInRowGroup)
		if cnt > 0 {
			curRg.Append(&state._rowGroupAppendState, data, cnt)
			//TODO:
		}
		remaining -= cnt
		if state._remaining > 0 {
			state._remaining -= cnt
		}
		if remaining > 0 {

		} else {
			break
		}
	}
	return newRg
}

func (collect *RowGroupCollection) AppendRowGroup(start IdxType) {
	rg := NewRowGroup(collect, start, 0)
	rg.InitEmpty(collect._types)
	collect._rowGroups.AppendSegment(rg)
}

type VersionNode struct {
	_info [ROW_GROUP_VECTOR_COUNT]*ChunkInfo
}

type RowGroupAppendState struct {
	_parent           *TableAppendState
	_rowGroup         *RowGroup
	_states           []*ColumnAppendState
	_offsetInRowGroup IdxType
}

type SegmentNode[T any] struct {
	_rowStart IdxType
	_node     *T
}

type RowGroupSegmentTree struct {
	_lock  sync.Mutex
	_nodes treeset.Set[SegmentNode[RowGroup]]
}

func (tree *RowGroupSegmentTree) Lock() func() {
	tree._lock.Lock()
	return func() {
		tree._lock.Unlock()
	}
}

func (tree *RowGroupSegmentTree) IsEmpty() bool {
	return tree._nodes.Size() == 0
}

func (tree *RowGroupSegmentTree) AppendSegment(rg *RowGroup) {
	if tree._nodes.Size() != 0 {
		last := tree._nodes.Last()
		last.Value()._node._next.Store(rg)
	}
	rg._index = IdxType(tree._nodes.Size())
	node := SegmentNode[RowGroup]{
		_rowStart: rg._start,
		_node:     rg,
	}
	tree._nodes.Insert(node)
}

func (tree *RowGroupSegmentTree) GetLastSegment() *RowGroup {
	if tree._nodes.Size() == 0 {
		return nil
	}
	return tree._nodes.Last().Value()._node
}
