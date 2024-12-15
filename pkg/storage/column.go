package storage

import (
	"sync"

	treeset "github.com/liyue201/gostl/ds/set"

	"github.com/daviszhen/plan/pkg/chunk"
	"github.com/daviszhen/plan/pkg/common"
	"github.com/daviszhen/plan/pkg/util"
)

type ColumnDataType int

const (
	ColumnDataTypeStandard ColumnDataType = 0
	ColumnDataTypeValidity ColumnDataType = 1
)

type ColumnData struct {
	_cdType      ColumnDataType
	_start       IdxType
	_count       IdxType
	_blockMgr    *BlockManager
	_info        *DataTableInfo
	_columnIndex IdxType
	_typ         common.LType
	_parent      *ColumnData
	_data        *ColumnSegmentTree
	_updateLock  sync.Mutex
	_updates     *UpdateSegment
	_version     IdxType
	_validity    *ColumnData
}

func NewColumnData(
	mgr *BlockManager,
	info *DataTableInfo,
	colIdx int,
	start IdxType,
	typ common.LType,
	parent *ColumnData,
) *ColumnData {
	ret := &ColumnData{
		_cdType:      ColumnDataTypeStandard,
		_blockMgr:    mgr,
		_info:        info,
		_columnIndex: IdxType(colIdx),
		_start:       start,
		_typ:         typ,
		_version:     0,
		_parent:      parent,
		_data:        NewColumnSegmentTree(),
	}
	ret._validity = &ColumnData{
		_cdType:      ColumnDataTypeValidity,
		_blockMgr:    mgr,
		_info:        info,
		_columnIndex: 0,
		_start:       start,
		_typ:         common.MakeLType(common.LTID_VALIDITY),
		_parent:      ret,
		_data:        NewColumnSegmentTree(),
	}
	return ret
}

func (column *ColumnData) InitAppend(state *ColumnAppendState) {
	release := column._data.Lock()
	defer release()
	if column._data.IsEmpty() {
		column.AppendSegment(column._start)
	}
	segment := column._data.GetLastSegment()
	state._current = segment
	state._current.InitAppend(state)
	if column._cdType == ColumnDataTypeStandard {
		childAppend := &ColumnAppendState{}
		column._validity.InitAppend(childAppend)
		state._childAppends = append(state._childAppends, childAppend)
	}
}

func (column *ColumnData) AppendSegment(start IdxType) {
	segSize := BLOCK_SIZE
	if start == IdxType(MAX_ROW_ID) {
		segSize = uint64(STANDARD_VECTOR_SIZE * column._typ.GetInternalType().Size())
	}
	seg := NewColumnSegment(column._typ, start, IdxType(segSize))
	column._data.AppendSegment(seg)
}

func (column *ColumnData) Append(
	state *ColumnAppendState,
	vector *chunk.Vector,
	cnt IdxType) {
	var vdata chunk.UnifiedFormat
	vector.ToUnifiedFormat(int(cnt), &vdata)
	column.AppendData(state, &vdata, cnt)
}

func (column *ColumnData) AppendData(
	state *ColumnAppendState,
	vdata *chunk.UnifiedFormat,
	cnt IdxType) {
	offset := IdxType(0)
	column._count += cnt
	for {
		copied := state._current.Append(state, vdata, offset, cnt)
		if copied == cnt {
			break
		}
		fun := func() {
			release := column._data.Lock()
			defer release()
			column.AppendSegment(state._current._start +
				IdxType(state._current._count.Load()))
			state._current = column._data.GetLastSegment()
			state._current.InitAppend(state)
		}
		fun()
		offset += copied
		cnt -= copied
	}
	if column._cdType == ColumnDataTypeStandard {
		column._validity.AppendData(state._childAppends[0], vdata, cnt)
	}
}

func (column *ColumnData) SetStart(newStart IdxType) {
	column._start = newStart
	offset := IdxType(0)
	for iter := column._data._nodes.Begin(); iter.IsValid(); iter.Next() {
		iter.Value()._node._start = column._start + offset
		offset += IdxType(iter.Value()._node._count.Load())
	}
	column._data.Reinitialize()
}

func (column *ColumnData) InitScan(state *ColumnScanState) {
	state._current = column._data.GetRootSegment()
	state._segmentTree = column._data
	state._rowIdx = 0
	if state._current != nil {
		state._rowIdx = state._current._start
	}
	state._internalIdx = state._rowIdx
	state._initialized = false
	state._version = column._version
	state._scanState = nil
	state._lastOffset = 0
}

func (column *ColumnData) Skip(
	state *ColumnScanState,
	count IdxType) {
	state.Next(count)
}

func (column *ColumnData) Scan(
	txn *Txn,
	vecIdx IdxType,
	state *ColumnScanState,
	result *chunk.Vector) IdxType {
	return column.ScanVector(
		txn,
		vecIdx,
		state,
		result,
		false,
		true)
}

func (column *ColumnData) ScanVector(
	txn *Txn,
	vecIdx IdxType,
	state *ColumnScanState,
	result *chunk.Vector,
	scanCommitted bool,
	allowUpdates bool,
) IdxType {
	scanCount := column.ScanVector2(state, result, STANDARD_VECTOR_SIZE)
	column._updateLock.Lock()
	defer column._updateLock.Unlock()
	if column._updates != nil {
		result.Flatten(int(scanCount))
		if scanCommitted {
			panic("usp")
		} else {
			panic("usp")
		}
	}
	return scanCount
}

func (column *ColumnData) ScanVector2(
	state *ColumnScanState,
	result *chunk.Vector,
	remaining IdxType,
) IdxType {
	state._previousStates = nil
	if state._version != column._version {
		column.InitScanWithOffset(state, state._rowIdx)
		state._current.InitScan(state)
		state._initialized = true
	}
	return -1
}

func (column *ColumnData) InitScanWithOffset(
	state *ColumnScanState,
	rowIdx IdxType) {
	state._current = column._data.GetSegment(rowIdx)
}

type ColumnSegmentTree struct {
	_lock  sync.Mutex
	_nodes []SegmentNode[ColumnSegment]
}

func NewColumnSegmentTree() *ColumnSegmentTree {
	ret := &ColumnSegmentTree{}
	return ret
}

func (tree *ColumnSegmentTree) Lock() func() {
	tree._lock.Lock()
	return func() {
		tree._lock.Unlock()
	}
}

func (tree *ColumnSegmentTree) IsEmpty() bool {
	return len(tree._nodes) == 0
}

func (tree *ColumnSegmentTree) AppendSegment(seg *ColumnSegment) {
	if tree._nodes.Size() != 0 {
		last := tree._nodes.Last()
		last.Value()._node._next.Store(seg)
	}
	seg._index = IdxType(tree._nodes.Size())
	node := SegmentNode[ColumnSegment]{
		_rowStart: seg._start,
		_node:     seg,
	}
	tree._nodes.Insert(node)
}

func (tree *ColumnSegmentTree) GetLastSegment() *ColumnSegment {
	return tree._nodes.Last().Value()._node
}

func (tree *ColumnSegmentTree) Reinitialize() {
	if tree._nodes.Size() == 0 {
		return
	}
	offset := tree._nodes.Begin().Value()._node._start
	for iter := tree._nodes.Begin(); iter.IsValid(); iter.Next() {
		node := iter.Value()
		node._rowStart = offset
		offset += IdxType(node._node._count.Load())
	}
}

func (tree *ColumnSegmentTree) GetRootSegment() *ColumnSegment {
	return tree._nodes.First().Value()._node
}

func (tree *ColumnSegmentTree) GetNextSegment(current *ColumnSegment) *ColumnSegment {
	return tree.GetSegmentByIndex(current._index + 1)
}

func (tree *ColumnSegmentTree) GetSegmentByIndex(idx IdxType) *ColumnSegment {
	util.AssertFunc(idx >= 0)
	temp := SegmentNode[ColumnSegment]{
		_rowStart: idx,
	}
	iter := tree._nodes.Find(temp)
	if iter.IsValid() {
		return iter.Value()._node
	} else {
		return nil
	}
}

func (tree *ColumnSegmentTree) GetSegment(rowNumber IdxType) *ColumnSegment {
	idx := tree.GetSegmentIdx(rowNumber)
	iter := tree._nodes.Find(SegmentNode[ColumnSegment]{
		_rowStart: idx,
	})
	return iter.Value()._node
}

func (tree *ColumnSegmentTree) GetSegmentIdx(rowNumber IdxType) IdxType {
	for iter := tree._nodes.Begin(); iter.IsValid(); iter.Next() {
		node := iter.Value()
		if node._rowStart <= rowNumber && rowNumber < node._rowStart+IdxType(node._node._count.Load()) {
			return node._rowStart
		}
	}
	panic("usp")
}

type UpdateSegment struct {
}

type ColumnSegment struct {
	SegmentBase[ColumnSegment]
	_type        common.LType
	_typeSize    IdxType
	_function    *CompressFunction
	_block       *BlockHandle
	_blockId     BlockID
	_offset      IdxType
	_segmentSize IdxType
}

func NewColumnSegment(typ common.LType, start IdxType, size IdxType) *ColumnSegment {
	fun := GetUncompressedCompressFunction(typ.GetInternalType())
	var block *BlockHandle
	if size < IdxType(BLOCK_SIZE) {
		block = GBufferMgr.RegisterSmallMemory(uint64(size))
	} else {
		GBufferMgr.Allocate(uint64(size), false, &block)
	}
	return &ColumnSegment{
		SegmentBase: SegmentBase[ColumnSegment]{
			_start: start,
		},
		_type:        typ,
		_typeSize:    IdxType(typ.GetInternalType().Size()),
		_function:    fun,
		_block:       block,
		_blockId:     -1,
		_offset:      0,
		_segmentSize: size,
	}
}

func (segment *ColumnSegment) InitAppend(state *ColumnAppendState) {
	state._appendState = segment._function._initAppend(segment)
}

func (segment *ColumnSegment) Append(
	state *ColumnAppendState,
	appendData *chunk.UnifiedFormat,
	offset IdxType,
	cnt IdxType,
) IdxType {
	return segment._function._append(
		state._appendState,
		segment,
		appendData,
		offset,
		cnt,
	)
}

func (segment *ColumnSegment) InitScan(state *ColumnScanState) {

}

type ColumnAppendState struct {
	_current      *ColumnSegment
	_childAppends []*ColumnAppendState
	_appendState  *CompressAppendState
}
