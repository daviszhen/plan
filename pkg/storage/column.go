package storage

import (
	"sync"

	treeset "github.com/liyue201/gostl/ds/set"

	"github.com/daviszhen/plan/pkg/chunk"
	"github.com/daviszhen/plan/pkg/common"
)

type ColumnData struct {
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
	//FIXME:
}

func NewColumnData(mgr *BlockManager, info *DataTableInfo, i int, start IdxType, lt common.LType) *ColumnData {
	return nil
}

type ColumnSegmentTree struct {
	_lock  sync.Mutex
	_nodes treeset.Set[SegmentNode[ColumnSegment]]
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

func (segment *ColumnSegment) InitAppend(state *ColumnAppendState) {
	state._appendState = segment._function._initAppend(segment)
}

func NewColumnSegment(typ common.LType, start IdxType, size IdxType) *ColumnSegment {
	fun := GetUncompressedCompressFunction(typ.GetInternalType())
	var block *BlockHandle
	if size < IdxType(BLOCK_SIZE) {
		block = GBufferMgr.RegisterMemory(uint64(size), false)
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

type ColumnAppendState struct {
	_current     *ColumnSegment
	_appendState *CompressAppendState
}
