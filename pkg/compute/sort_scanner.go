package compute

import (
	"unsafe"

	"github.com/daviszhen/plan/pkg/chunk"
	"github.com/daviszhen/plan/pkg/common"
	"github.com/daviszhen/plan/pkg/util"
)

type ScanState struct {
	_scanner  *RowDataCollectionScanner
	_blockIdx int
	_entryIdx int
	_ptr      unsafe.Pointer
}

type RowDataCollectionScanner struct {
	_rows         *RowDataCollection
	_heap         *RowDataCollection
	_layout       *RowLayout
	_readState    *ScanState
	_totalCount   int
	_totalScanned int
	_addresses    *chunk.Vector
	_flush        bool
}

func (scan *RowDataCollectionScanner) Scan(output *chunk.Chunk) {
	count := min(util.DefaultVectorSize,
		scan._totalCount-scan._totalScanned)
	if count == 0 {
		output.SetCard(count)
		return
	}
	rowWidth := scan._layout._rowWidth
	scanned := 0
	dataPtrs := chunk.GetSliceInPhyFormatFlat[unsafe.Pointer](scan._addresses)
	for scanned < count {
		dataBlock := scan._rows._blocks[scan._readState._blockIdx]
		scan._readState._ptr = dataBlock._ptr
		next := min(
			dataBlock._count-scan._readState._entryIdx,
			count-scanned,
		)
		dataPtr := util.PointerAdd(scan._readState._ptr,
			scan._readState._entryIdx*rowWidth)
		rowPtr := dataPtr
		for i := 0; i < next; i++ {
			dataPtrs[scanned+i] = rowPtr
			rowPtr = util.PointerAdd(rowPtr, rowWidth)
		}

		scan._readState._entryIdx += next
		if scan._readState._entryIdx == dataBlock._count {
			scan._readState._blockIdx++
			scan._readState._entryIdx = 0
		}
		scanned += next
	}

	util.AssertFunc(scanned == count)
	for colIdx := 0; colIdx < scan._layout.CoumnCount(); colIdx++ {
		Gather(
			scan._addresses,
			chunk.IncrSelectVectorInPhyFormatFlat(),
			output.Data[colIdx],
			chunk.IncrSelectVectorInPhyFormatFlat(),
			count,
			scan._layout,
			colIdx,
			0,
			nil,
		)
	}

	output.SetCard(count)
	scan._totalScanned += scanned
	if scan._flush {
		for i := 0; i < scan._readState._blockIdx; i++ {
			if scan._rows._blocks != nil {
				scan._rows._blocks[i]._ptr = nil
			}
			if scan._heap._blocks != nil {
				scan._heap._blocks[i]._ptr = nil
			}
		}
	}
}

func (scan *RowDataCollectionScanner) Count() int {
	return scan._totalCount
}

func (scan *RowDataCollectionScanner) Remaining() int {
	return scan._totalCount - scan._totalScanned
}

func (scan *RowDataCollectionScanner) Scanned() int {
	return scan._totalScanned
}

func (scan *RowDataCollectionScanner) Reset(flush bool) {
	scan._flush = flush
	scan._totalScanned = 0
	scan._readState._blockIdx = 0
	scan._readState._entryIdx = 0
}

func NewRowDataCollectionScanner(
	row *RowDataCollection,
	heap *RowDataCollection,
	layout *RowLayout,
	flush bool,
) *RowDataCollectionScanner {
	ret := &RowDataCollectionScanner{
		_rows:         row,
		_heap:         heap,
		_layout:       layout,
		_totalCount:   row._count,
		_totalScanned: 0,
		_flush:        flush,
		_addresses:    chunk.NewFlatVector(common.PointerType(), util.DefaultVectorSize),
	}
	ret._readState = &ScanState{
		_scanner: ret,
	}

	return ret
}

type PayloadScanner struct {
	_rows    *RowDataCollection
	_heap    *RowDataCollection
	_scanner *RowDataCollectionScanner
}

func NewPayloadScanner(
	sortedData *SortedData,
	lstate *LocalSort,
	flush bool,
) *PayloadScanner {
	count := sortedData.Count()
	layout := sortedData._layout

	rows := NewRowDataCollection(BLOCK_SIZE, 1)
	rows._count = count

	heap := NewRowDataCollection(BLOCK_SIZE, 1)
	if !layout.AllConstant() {
		heap._count = count
	}

	if flush {
		rows._blocks = sortedData._dataBlocks
		sortedData._dataBlocks = nil
		if !layout.AllConstant() {
			heap._blocks = sortedData._heapBlocks
			sortedData._heapBlocks = nil
		}
	} else {
		for _, block := range sortedData._dataBlocks {
			rows._blocks = append(rows._blocks, block.Copy())
		}

		if !layout.AllConstant() {
			for _, block := range sortedData._heapBlocks {
				heap._blocks = append(heap._blocks, block.Copy())
			}
		}
	}

	scanner := NewRowDataCollectionScanner(rows, heap, layout, flush)

	ret := &PayloadScanner{
		_rows:    rows,
		_heap:    heap,
		_scanner: scanner,
	}

	return ret
}

func (scan *PayloadScanner) Scan(output *chunk.Chunk) {
	scan._scanner.Scan(output)
}

func (scan *PayloadScanner) Scanned() int {
	return scan._scanner.Scanned()
}

func (scan *PayloadScanner) Remaining() int {
	return scan._scanner.Remaining()
}

func Gather(
	rows *chunk.Vector,
	rowSel *chunk.SelectVector,
	col *chunk.Vector,
	colSel *chunk.SelectVector,
	count int,
	layout *RowLayout,
	colNo int,
	buildSize int,
	heapPtr unsafe.Pointer,
) {
	util.AssertFunc(rows.PhyFormat().IsFlat())
	util.AssertFunc(rows.Typ().IsPointer())
	col.SetPhyFormat(chunk.PF_FLAT)
	switch col.Typ().GetInternalType() {
	case common.INT32:
		TemplatedGatherLoop[int32](
			rows,
			rowSel,
			col,
			colSel,
			count,
			layout,
			colNo,
			buildSize,
		)
	case common.INT64:
		TemplatedGatherLoop[int64](
			rows,
			rowSel,
			col,
			colSel,
			count,
			layout,
			colNo,
			buildSize,
		)
	case common.INT128:
		TemplatedGatherLoop[common.Hugeint](
			rows,
			rowSel,
			col,
			colSel,
			count,
			layout,
			colNo,
			buildSize,
		)
	case common.DECIMAL:
		TemplatedGatherLoop[common.Decimal](
			rows,
			rowSel,
			col,
			colSel,
			count,
			layout,
			colNo,
			buildSize,
		)
	case common.DATE:
		TemplatedGatherLoop[common.Date](
			rows,
			rowSel,
			col,
			colSel,
			count,
			layout,
			colNo,
			buildSize,
		)
	case common.VARCHAR:
		GatherVarchar(
			rows,
			rowSel,
			col,
			colSel,
			count,
			layout,
			colNo,
			buildSize,
			heapPtr,
		)
	case common.DOUBLE:
		TemplatedGatherLoop[float64](
			rows,
			rowSel,
			col,
			colSel,
			count,
			layout,
			colNo,
			buildSize,
		)
	default:
		panic("unknown column type")
	}
}

func TemplatedGatherLoop[T any](
	rows *chunk.Vector,
	rowSel *chunk.SelectVector,
	col *chunk.Vector,
	colSel *chunk.SelectVector,
	count int,
	layout *RowLayout,
	colNo int,
	buildSize int,
) {
	offsets := layout.GetOffsets()
	colOffset := offsets[colNo]
	entryIdx, idxInEntry := util.GetEntryIndex(uint64(colNo))
	ptrs := chunk.GetSliceInPhyFormatFlat[unsafe.Pointer](rows)
	dataSlice := chunk.GetSliceInPhyFormatFlat[T](col)
	colMask := chunk.GetMaskInPhyFormatFlat(col)

	for i := 0; i < count; i++ {
		rowIdx := rowSel.GetIndex(i)
		row := ptrs[rowIdx]
		colIdx := colSel.GetIndex(i)
		dataSlice[colIdx] = util.Load[T](util.PointerAdd(row, colOffset))
		rowMask := util.Bitmap{
			Bits: util.PointerToSlice[byte](row, layout._flagWidth),
		}
		if !util.RowIsValidInEntry(
			rowMask.GetEntry(entryIdx),
			idxInEntry) {
			if buildSize > util.DefaultVectorSize && colMask.AllValid() {
				colMask.Init(buildSize)
			}
			colMask.SetInvalid(uint64(colIdx))
		}
	}
}

func GatherVarchar(
	rows *chunk.Vector,
	rowSel *chunk.SelectVector,
	col *chunk.Vector,
	colSel *chunk.SelectVector,
	count int,
	layout *RowLayout,
	colNo int,
	buildSize int,
	baseHeapPtr unsafe.Pointer,
) {
	offsets := layout.GetOffsets()
	colOffset := offsets[colNo]
	heapOffset := layout.GetHeapOffset()
	entryIdx, idxInEntry := util.GetEntryIndex(uint64(colNo))
	ptrs := chunk.GetSliceInPhyFormatFlat[unsafe.Pointer](rows)
	dataSlice := chunk.GetSliceInPhyFormatFlat[common.String](col)
	colMask := chunk.GetMaskInPhyFormatFlat(col)

	for i := 0; i < count; i++ {
		rowIdx := rowSel.GetIndex(i)
		row := ptrs[rowIdx]
		colIdx := colSel.GetIndex(i)
		colPtr := util.PointerAdd(row, colOffset)
		dataSlice[colIdx] = util.Load[common.String](colPtr)
		rowMask := util.Bitmap{
			Bits: util.PointerToSlice[byte](row, layout._flagWidth),
		}
		if !util.RowIsValidInEntry(
			rowMask.GetEntry(entryIdx),
			idxInEntry,
		) {
			if buildSize > util.DefaultVectorSize && colMask.AllValid() {
				colMask.Init(buildSize)
			}
			colMask.SetInvalid(uint64(colIdx))
		} else if baseHeapPtr != nil {
			heapPtrPtr := util.PointerAdd(row, heapOffset)
			heapRowPtr := util.PointerAdd(baseHeapPtr, int(util.Load[uint64](heapPtrPtr)))
			strPtr := unsafe.Pointer(&dataSlice[colIdx])
			util.Store[unsafe.Pointer](
				util.PointerAdd(heapRowPtr, int(util.Load[uint64](strPtr))),
				strPtr,
			)
		}
	}

}
