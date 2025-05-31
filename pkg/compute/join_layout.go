package compute

import (
	"github.com/daviszhen/plan/pkg/common"
	"github.com/daviszhen/plan/pkg/util"
)

/*
TupleDataLayout
format:

					bitmap | heap_size_offset? | data columns | aggr fields |
	                |------|----------------------------------|-------------|
	                  |                |                               |    |
	                  |                |                               |    |

bitmapWidth-----------|                |                               |    |
dataWidth------------------------------|                               |    |
aggrWidth--------------------------------------------------------------|    |
rowWidth--------------------------------------------------------------------|

offsets: start of data columns and aggr fields
*/
type TupleDataLayout struct {
	//types of the data columns
	_types               []common.LType
	_childrenOutputTypes []common.LType

	//Width of bitmap header
	_bitmapWidth int

	//Width of data part
	_dataWidth int

	//Width of agg state part
	_aggWidth int

	//Width of entire row
	_rowWidth int

	//offsets to the columns and agg data in each row
	_offsets []int

	//all columns in this layout are constant size
	_allConst bool

	//offset to the heap size of each row
	_heapSizeOffset int

	_aggregates []*AggrObject
}

func NewTupleDataLayout(types []common.LType, aggrObjs []*AggrObject, childrenOutputTypes []common.LType, align bool, needHeapOffset bool) *TupleDataLayout {
	layout := &TupleDataLayout{
		_types:               common.CopyLTypes(types...),
		_childrenOutputTypes: common.CopyLTypes(childrenOutputTypes...),
	}

	alignWidth := func() {
		if align {
			layout._rowWidth = util.AlignValue8(layout._rowWidth)
		}
	}

	layout._bitmapWidth = util.EntryCount(len(layout._types) + len(layout._childrenOutputTypes))
	layout._rowWidth = layout._bitmapWidth
	alignWidth()

	//all columns + children output columns are constant size
	for _, lType := range append(layout._types, layout._childrenOutputTypes...) {
		layout._allConst = layout._allConst &&
			lType.GetInternalType().IsConstant()
	}

	if needHeapOffset && !layout._allConst {
		layout._heapSizeOffset = layout._rowWidth
		layout._rowWidth += common.Int64Size
		alignWidth()
	}

	//data columns + children output columns
	for _, lType := range append(layout._types, layout._childrenOutputTypes...) {
		layout._offsets = append(layout._offsets, layout._rowWidth)
		if lType.GetInternalType().IsConstant() ||
			lType.GetInternalType().IsVarchar() {
			layout._rowWidth += lType.GetInternalType().Size()
		} else {
			//for variable length types, pointer to the actual data
			layout._rowWidth += common.Int64Size
		}
		alignWidth()
	}

	layout._dataWidth = layout._rowWidth - layout._bitmapWidth

	layout._aggregates = aggrObjs
	for _, aggrObj := range aggrObjs {
		layout._offsets = append(layout._offsets, layout._rowWidth)
		layout._rowWidth += aggrObj._payloadSize
		alignWidth()
	}

	layout._aggWidth = layout._rowWidth - layout.dataWidth() - layout.dataOffset()

	return layout
}

func (layout *TupleDataLayout) columnCount() int {
	return len(layout._types)
}

func (layout *TupleDataLayout) childrenOutputCount() int {
	return len(layout._childrenOutputTypes)
}

func (layout *TupleDataLayout) types() []common.LType {
	return common.CopyLTypes(layout._types...)
}

// total Width of each row
func (layout *TupleDataLayout) rowWidth() int {
	return layout._rowWidth
}

// start of the data in each row
func (layout *TupleDataLayout) dataOffset() int {
	return layout._bitmapWidth
}

// total Width of the data
func (layout *TupleDataLayout) dataWidth() int {
	return layout._dataWidth
}

// start of agg
func (layout *TupleDataLayout) aggrOffset() int {
	return layout._bitmapWidth + layout._dataWidth
}

func (layout *TupleDataLayout) aggrIdx() int {
	return layout.columnCount() + layout.childrenOutputCount()
}

func (layout *TupleDataLayout) offsets() []int {
	return util.CopyTo[int](layout._offsets)
}

func (layout *TupleDataLayout) allConst() bool {
	return layout._allConst
}

func (layout *TupleDataLayout) heapSizeOffset() int {
	return layout._heapSizeOffset
}

func (layout *TupleDataLayout) copy() *TupleDataLayout {
	if layout == nil {
		return nil
	}
	res := &TupleDataLayout{}
	res._types = common.CopyLTypes(layout._types...)
	res._childrenOutputTypes = common.CopyLTypes(layout._childrenOutputTypes...)
	res._bitmapWidth = layout._bitmapWidth
	res._dataWidth = layout._dataWidth
	res._aggWidth = layout._aggWidth
	res._rowWidth = layout._rowWidth
	res._offsets = util.CopyTo[int](layout._offsets)
	res._allConst = layout._allConst
	res._heapSizeOffset = layout._heapSizeOffset
	return res
}
