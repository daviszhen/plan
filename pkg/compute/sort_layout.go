package compute

import (
	"unsafe"

	"github.com/daviszhen/plan/pkg/common"
	"github.com/daviszhen/plan/pkg/util"
)

type SortLayout struct {
	_columnCount      int
	_orderTypes       []OrderType
	_orderByNullTypes []OrderByNullType
	_logicalTypes     []common.LType
	_allConstant      bool
	_constantSize     []bool
	//column size + null byte
	_columnSizes   []int
	_prefixLengths []int
	_hasNull       []bool
	//bytes count that need to be compared
	_comparisonSize int
	//equal to _comparisonSize + sizeof(int32)
	_entrySize        int
	_blobLayout       *RowLayout
	_sortingToBlobCol map[int]int
}

func NewSortLayout(orders []*Expr) *SortLayout {
	ret := &SortLayout{
		_columnCount:      len(orders),
		_allConstant:      true,
		_sortingToBlobCol: make(map[int]int),
	}

	blobLayoutTypes := make([]common.LType, 0)
	for i := 0; i < ret._columnCount; i++ {
		order := orders[i]
		realOrder := order.Children[0]
		if order.Desc {
			ret._orderTypes = append(ret._orderTypes, OT_DESC)
		} else {
			ret._orderTypes = append(ret._orderTypes, OT_ASC)
		}

		ret._orderByNullTypes = append(ret._orderByNullTypes, OBNT_NULLS_FIRST)
		ret._logicalTypes = append(ret._logicalTypes, realOrder.DataTyp)

		interTyp := realOrder.DataTyp.GetInternalType()
		ret._constantSize = append(ret._constantSize, interTyp.IsConstant())

		ret._hasNull = append(ret._hasNull, true)

		colSize := 0
		if ret._hasNull[len(ret._hasNull)-1] {
			//?
			colSize = 1
		}

		ret._prefixLengths = append(ret._prefixLengths, 0)
		if !interTyp.IsConstant() && interTyp != common.VARCHAR {
			panic("usp")
		} else if interTyp == common.VARCHAR {
			sizeBefore := colSize
			colSize = 12
			ret._prefixLengths[len(ret._prefixLengths)-1] = colSize - sizeBefore
		} else {
			colSize += interTyp.Size()
		}

		ret._comparisonSize += colSize
		ret._columnSizes = append(ret._columnSizes, colSize)
	}
	ret._entrySize = ret._comparisonSize + common.Int32Size

	//check all constant
	for i := 0; i < ret._columnCount; i++ {
		ret._allConstant = ret._allConstant && ret._constantSize[i]
		if !ret._constantSize[i] {
			ret._sortingToBlobCol[i] = len(blobLayoutTypes)
			blobLayoutTypes = append(blobLayoutTypes, ret._logicalTypes[i])
		}
	}
	//init blob layout
	ret._blobLayout = NewRowLayout(blobLayoutTypes, nil)
	return ret
}

type RowLayout struct {
	_types             []common.LType
	_aggregates        []*AggrObject
	_flagWidth         int
	_dataWidth         int
	_aggrWidth         int
	_rowWidth          int
	_offsets           []int
	_allConstant       bool
	_heapPointerOffset int
}

func NewRowLayout(types []common.LType, aggrObjs []*AggrObject) *RowLayout {
	ret := &RowLayout{
		_types:       common.CopyLTypes(types...),
		_allConstant: true,
	}

	alignWith := func() {
		ret._rowWidth = util.AlignValue8(ret._rowWidth)
	}

	ret._flagWidth = util.EntryCount(len(types))
	ret._rowWidth = ret._flagWidth
	alignWith()

	for _, lType := range types {
		ret._allConstant = ret._allConstant &&
			lType.GetInternalType().IsConstant()
	}

	//swizzling
	if !ret._allConstant {
		ret._heapPointerOffset = ret._rowWidth
		ret._rowWidth += common.Int64Size
		alignWith()
	}

	for _, lType := range types {
		ret._offsets = append(ret._offsets, ret._rowWidth)
		interTyp := lType.GetInternalType()
		if interTyp.IsConstant() || interTyp == common.VARCHAR {
			ret._rowWidth += interTyp.Size()
			alignWith()
		} else {
			ret._rowWidth += common.Int64Size
			alignWith()
		}
	}

	ret._dataWidth = ret._rowWidth - ret._flagWidth
	ret._aggregates = aggrObjs
	for _, obj := range aggrObjs {
		ret._offsets = append(ret._offsets, ret._rowWidth)
		ret._rowWidth += obj._payloadSize
		alignWith()
	}
	ret._aggrWidth = ret._rowWidth - ret._dataWidth - ret._flagWidth

	return ret
}

func (lay *RowLayout) rowWidth() int {
	return lay._rowWidth
}

func (lay *RowLayout) CoumnCount() int {
	return len(lay._types)
}

func (lay *RowLayout) GetOffsets() []int {
	return lay._offsets
}

func (lay *RowLayout) GetTypes() []common.LType {
	return lay._types
}

func (lay *RowLayout) AllConstant() bool {
	return lay._allConstant
}

func (lay *RowLayout) GetHeapOffset() int {
	return lay._heapPointerOffset
}

type RowDataBlock struct {
	_ptr       unsafe.Pointer
	_capacity  int
	_entrySize int
	_count     int
	//write offset for var len entry
	_byteOffset int
}

func (block *RowDataBlock) Close() {
	util.CFree(block._ptr)
	block._ptr = unsafe.Pointer(nil)
	block._count = 0
}

func (block *RowDataBlock) Copy() *RowDataBlock {
	ret := &RowDataBlock{_entrySize: block._entrySize}
	ret._ptr = block._ptr
	ret._capacity = block._capacity
	ret._count = block._count
	ret._byteOffset = block._byteOffset
	return ret
}

func NewRowDataBlock(capacity int, entrySize int) *RowDataBlock {
	ret := &RowDataBlock{
		_capacity:  capacity,
		_entrySize: entrySize,
	}
	sz := max(BLOCK_SIZE, capacity*entrySize)
	ret._ptr = util.CMalloc(sz)
	return ret
}
