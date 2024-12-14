package storage

import (
	"sync"

	treeset "github.com/liyue201/gostl/ds/set"

	"github.com/daviszhen/plan/pkg/chunk"
	"github.com/daviszhen/plan/pkg/common"
)

type ColumnDataType int

const (
	ColumnDataTypeStandard ColumnDataType = 0
	ColumnDataTypeValidity
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

type ColumnSegmentTree struct {
	_lock  sync.Mutex
	_nodes *treeset.Set[SegmentNode[ColumnSegment]]
}

func NewColumnSegmentTree() *ColumnSegmentTree {
	cmp := func(a, b SegmentNode[ColumnSegment]) int {
		ret := a._rowStart - b._rowStart
		if ret < 0 {
			return -1
		}
		if ret > 0 {
			return 1
		}
		return 0
	}
	ret := &ColumnSegmentTree{
		_nodes: treeset.New[SegmentNode[ColumnSegment]](cmp),
	}
	return ret
}

func (tree *ColumnSegmentTree) Lock() func() {
	tree._lock.Lock()
	return func() {
		tree._lock.Unlock()
	}
}

func (tree *ColumnSegmentTree) IsEmpty() bool {
	return tree._nodes.Size() == 0
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

type ColumnAppendState struct {
	_current      *ColumnSegment
	_childAppends []*ColumnAppendState
	_appendState  *CompressAppendState
}
