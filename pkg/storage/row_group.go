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
	MERGE_THRESHOLD          = ROW_GROUP_SIZE
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
	for i, typ := range types {
		cd := NewColumnData(
			rg._collect._blockMgr,
			rg._collect._info,
			i,
			rg._start,
			typ,
			nil,
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
		state._states[i] = &ColumnAppendState{}
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

func (rg *RowGroup) MoveToCollection(collect *RowGroupCollection, newStart IdxType) {
	rg._collect = collect
	rg._start = newStart
	for _, column := range rg._columns {
		column.SetStart(newStart)
	}
	if rg._versionInfo != nil {
		rg._versionInfo.SetStart(newStart)
	}
}

func (rg *RowGroup) CommitAppend(
	commitId TxnType,
	rgStart IdxType,
	cnt IdxType) {
	rgEnd := rgStart + cnt
	rg._rowGroupLock.Lock()
	defer rg._rowGroupLock.Unlock()
	startIdx := rgStart / STANDARD_VECTOR_SIZE
	endIdx := (rgEnd - 1) / STANDARD_VECTOR_SIZE
	for idx := startIdx; idx <= endIdx; idx++ {
		start := IdxType(0)
		end := IdxType(0)
		if idx == startIdx {
			start = rgStart - startIdx*STANDARD_VECTOR_SIZE
		}
		if idx == endIdx {
			end = rgEnd - endIdx*STANDARD_VECTOR_SIZE
		} else {
			end = STANDARD_VECTOR_SIZE
		}
		info := rg._versionInfo._info[idx]
		info.CommitAppend(commitId, start, end)
	}
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
	_totalRows atomic.Uint64
	_info      *DataTableInfo
	_types     []common.LType
	_rowStart  IdxType
	_rowGroups *RowGroupSegmentTree
}

func NewRowGroupCollection(
	info *DataTableInfo,
	blockMgr *BlockManager,
	types []common.LType,
	rowStart IdxType,
	totalRows IdxType,
) *RowGroupCollection {
	ret := &RowGroupCollection{
		_blockMgr: blockMgr,
		_info:     info,
		_types:    types,
		_rowStart: rowStart,
	}
	ret._totalRows.Store(uint64(totalRows))
	rgst := NewRowGroupSegmentTree(ret)
	ret._rowGroups = rgst
	return ret
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
		collect._totalRows.Add(uint64(state._remaining))
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

func (collect *RowGroupCollection) FinalizeAppend(txn *Txn, state *TableAppendState) {
	remaining := state._totalAppendCount
	rowGroup := state._startRowGroup
	for remaining > 0 {
		appendCnt := min(remaining,
			IdxType(ROW_GROUP_SIZE-rowGroup._count.Load()))
		rowGroup.AppendVersionInfo(txn, appendCnt)
		remaining -= appendCnt
		rowGroup = collect._rowGroups.GetNextSegment(rowGroup)
	}
	collect._totalRows.Add(uint64(state._totalAppendCount))
	state._totalAppendCount = 0
	state._startRowGroup = nil
}

func (collect *RowGroupCollection) MergeStorage(data *RowGroupCollection) {
	idx := collect._rowStart + IdxType(collect._totalRows.Load())
	segments := data._rowGroups.MoveSegments()
	for _, entry := range segments {
		entry.MoveToCollection(collect, idx)
		idx += IdxType(entry._count.Load())
		collect._rowGroups.AppendSegment(entry)
	}
	collect._totalRows.Add(data._totalRows.Load())
}

func (collect *RowGroupCollection) CommitAppend(
	commitId TxnType, rowStart IdxType, count IdxType) {
	rg := collect._rowGroups.GetSegment(rowStart)
	curRow := rowStart
	remaining := count
	for {
		startInRg := curRow - rg._start
		appendCnt := min(IdxType(rg._count.Load())-startInRg, remaining)
		rg.CommitAppend(commitId, startInRg, appendCnt)
		curRow += appendCnt
		remaining -= appendCnt
		if remaining == 0 {
			break
		}
		rg = collect._rowGroups.GetNextSegment(rg)
	}
}

func (collect *RowGroupCollection) InitializeEmpty() {

}

type VersionNode struct {
	_info [ROW_GROUP_VECTOR_COUNT]*ChunkInfo
}

func (node *VersionNode) SetStart(start IdxType) {
	curStart := start
	for i := 0; i < ROW_GROUP_VECTOR_COUNT; i++ {
		if node._info[i] != nil {
			node._info[i]._start = curStart
		}
		curStart += STANDARD_VECTOR_SIZE
	}
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
	_lock            sync.Mutex
	_nodes           *treeset.Set[SegmentNode[RowGroup]]
	_collect         *RowGroupCollection
	_currentRowGroup IdxType
	_maxRowGroup     IdxType
}

func NewRowGroupSegmentTree(
	collect *RowGroupCollection) *RowGroupSegmentTree {
	cmp := func(a, b SegmentNode[RowGroup]) int {
		ret := a._rowStart - b._rowStart
		if ret < 0 {
			return -1
		}
		if ret > 0 {
			return 1
		}
		return 0
	}
	tree := &RowGroupSegmentTree{
		_collect: collect,
	}
	tree._nodes = treeset.New[SegmentNode[RowGroup]](cmp)
	return tree
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

func (tree *RowGroupSegmentTree) GetNextSegment(group *RowGroup) *RowGroup {
	release := tree.Lock()
	defer release()
	return tree.GetSegmentByIndex(group._index + 1)
}

func (tree *RowGroupSegmentTree) GetSegmentByIndex(idx IdxType) *RowGroup {
	util.AssertFunc(idx >= 0)
	temp := SegmentNode[RowGroup]{
		_rowStart: idx,
	}
	iter := tree._nodes.Find(temp)
	if iter.IsValid() {
		return iter.Value()._node
	} else {
		return nil
	}
}

func (tree *RowGroupSegmentTree) MoveSegments() []*RowGroup {
	ret := make([]*RowGroup, 0)
	for iter := tree._nodes.Begin(); iter.IsValid(); iter.Next() {
		ret = append(ret, iter.Value()._node)
	}
	return ret
}

func (tree *RowGroupSegmentTree) GetSegment(start IdxType) *RowGroup {
	return tree.GetSegmentByIndex(start)
}
