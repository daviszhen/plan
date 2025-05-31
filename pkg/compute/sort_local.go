package compute

import (
	"unsafe"

	"github.com/daviszhen/plan/pkg/chunk"
	"github.com/daviszhen/plan/pkg/common"
	"github.com/daviszhen/plan/pkg/util"
)

type LocalSort struct {
	_sortState        SortState
	_sortLayout       *SortLayout
	_payloadLayout    *RowLayout
	_radixSortingData *RowDataCollection
	_blobSortingData  *RowDataCollection
	_blobSortingHeap  *RowDataCollection
	_payloadData      *RowDataCollection
	_payloadHeap      *RowDataCollection
	_sortedBlocks     []*SortedBlock
	_addresses        *chunk.Vector
	_sel              *chunk.SelectVector
	_scanner          *PayloadScanner
}

func NewLocalSort(slayout *SortLayout, playout *RowLayout) *LocalSort {
	ret := &LocalSort{
		_sortLayout:    slayout,
		_payloadLayout: playout,
		_addresses:     chunk.NewFlatVector(common.PointerType(), util.DefaultVectorSize),
		_sel:           chunk.IncrSelectVectorInPhyFormatFlat(),
	}

	ret._radixSortingData = NewRowDataCollection(
		EntriesPerBlock(ret._sortLayout._entrySize),
		ret._sortLayout._entrySize)

	//blob
	if !ret._sortLayout._allConstant {
		w := ret._sortLayout._blobLayout.rowWidth()
		ret._blobSortingData = NewRowDataCollection(
			EntriesPerBlock(w),
			w,
		)
		ret._blobSortingHeap = NewRowDataCollection(
			BLOCK_SIZE,
			1,
		)
	}

	//payload
	w := ret._payloadLayout.rowWidth()
	ret._payloadData = NewRowDataCollection(
		EntriesPerBlock(w),
		w,
	)
	ret._payloadHeap = NewRowDataCollection(
		BLOCK_SIZE,
		1,
	)
	return ret
}

func (ls *LocalSort) SinkChunk(sort, payload *chunk.Chunk) {
	util.AssertFunc(sort.Card() == payload.Card())
	dataPtrs := chunk.GetSliceInPhyFormatFlat[unsafe.Pointer](ls._addresses)
	//alloc space on the block
	ls._radixSortingData.Build(sort.Card(), dataPtrs, nil, chunk.IncrSelectVectorInPhyFormatFlat())
	//scatter
	for sortCol := 0; sortCol < sort.ColumnCount(); sortCol++ {
		hasNull := ls._sortLayout._hasNull[sortCol]
		nullsFirst := ls._sortLayout._orderByNullTypes[sortCol] == OBNT_NULLS_FIRST
		desc := ls._sortLayout._orderTypes[sortCol] == OT_DESC
		//copy data from input to the block
		//only copy prefix for varchar
		RadixScatter(
			sort.Data[sortCol],
			sort.Card(),
			ls._sel,
			sort.Card(),
			dataPtrs,
			desc,
			hasNull,
			nullsFirst,
			ls._sortLayout._prefixLengths[sortCol],
			ls._sortLayout._columnSizes[sortCol],
			0,
		)
	}
	//
	if !ls._sortLayout._allConstant {
		blobChunk := &chunk.Chunk{}
		blobChunk.SetCard(sort.Card())
		blobChunk.SetCap(util.DefaultVectorSize)
		for i := 0; i < sort.ColumnCount(); i++ {
			if !ls._sortLayout._constantSize[i] {
				blobChunk.Data = append(blobChunk.Data, sort.Data[i])
			}
		}

		ls._blobSortingData.Build(blobChunk.Card(), dataPtrs, nil, chunk.IncrSelectVectorInPhyFormatFlat())
		blobData := blobChunk.ToUnifiedFormat()
		Scatter(
			blobChunk,
			blobData,
			ls._sortLayout._blobLayout,
			ls._addresses,
			ls._blobSortingHeap,
			ls._sel,
			blobChunk.Card(),
		)
	}
	ls._payloadData.Build(payload.Card(), dataPtrs, nil, chunk.IncrSelectVectorInPhyFormatFlat())
	inputData := payload.ToUnifiedFormat()
	Scatter(
		payload,
		inputData,
		ls._payloadLayout,
		ls._addresses,
		ls._payloadHeap,
		ls._sel,
		payload.Card(),
	)
}

func (ls *LocalSort) Sort(reorderHeap bool) {
	util.AssertFunc(ls._radixSortingData._count == ls._payloadData._count && reorderHeap)
	if ls._radixSortingData._count == 0 {
		return
	}

	lastBk := NewSortedBlock(ls._sortLayout, ls._payloadLayout)
	ls._sortedBlocks = append(ls._sortedBlocks, lastBk)

	sortingBlock := ls.ConcatenateBlocks(ls._radixSortingData)
	lastBk._radixSortingData = append(lastBk._radixSortingData, sortingBlock)
	//var len sorting data
	if !ls._sortLayout._allConstant {
		blobData := ls._blobSortingData
		newBlock := ls.ConcatenateBlocks(blobData)
		lastBk._blobSortingData._dataBlocks = append(lastBk._blobSortingData._dataBlocks,
			newBlock)
	}
	//payload data
	payloadBlock := ls.ConcatenateBlocks(ls._payloadData)
	lastBk._payloadData._dataBlocks = append(lastBk._payloadData._dataBlocks, payloadBlock)
	//sort in memory
	ls.SortInMemory()
	//reorder
	ls.ReOrder(reorderHeap)
}

func (ls *LocalSort) SortInMemory() {
	lastSBk := util.Back(ls._sortedBlocks)
	lastBlock := util.Back(lastSBk._radixSortingData)
	count := lastBlock._count
	//sort addr of row in the sort block
	dataPtr := lastBlock._ptr
	//locate to the addr of the row index
	idxPtr := util.PointerAdd(dataPtr, ls._sortLayout._comparisonSize)
	//for every row
	for i := 0; i < count; i++ {
		util.Store[uint32](uint32(i), idxPtr)
		idxPtr = util.PointerAdd(idxPtr, ls._sortLayout._entrySize)
	}

	//radix sort
	sortingSize := 0
	colOffset := 0
	var ties []bool
	containsString := false
	for i := 0; i < ls._sortLayout._columnCount; i++ {
		sortingSize += ls._sortLayout._columnSizes[i]
		containsString = containsString ||
			ls._sortLayout._logicalTypes[i].GetInternalType().IsVarchar()
		if ls._sortLayout._constantSize[i] && i < ls._sortLayout._columnCount-1 {
			//util a var len column or the last column
			continue
		}

		if ties == nil {
			//first sort
			RadixSort(
				dataPtr,
				count,
				colOffset,
				sortingSize,
				ls._sortLayout,
				containsString,
			)
			ties = make([]bool, count)
			util.Fill[bool](ties, count-1, true)
			ties[count-1] = false
		} else {
			//sort tied tuples
			SubSortTiedTuples(
				dataPtr,
				count,
				colOffset,
				sortingSize,
				ties,
				ls._sortLayout,
				containsString,
			)
		}

		containsString = false
		if ls._sortLayout._constantSize[i] &&
			i == ls._sortLayout._columnCount-1 {
			//all columns are sorted
			//no ties to break due to
			//last column is constant size
			break
		}

		ComputeTies(
			dataPtr,
			count,
			colOffset,
			sortingSize,
			ties,
			ls._sortLayout)
		if !AnyTies(ties, count) {
			//no ties, stop sorting
			break
		}

		if !ls._sortLayout._constantSize[i] {
			SortTiedBlobs(
				lastSBk,
				ties,
				dataPtr,
				count,
				i,
				ls._sortLayout,
			)
			if !AnyTies(ties, count) {
				//no ties, stop sorting
				break
			}
		}

		colOffset += sortingSize
		sortingSize = 0

	}

}

func (ls *LocalSort) ReOrder(reorderHeap bool) {
	sb := util.Back(ls._sortedBlocks)
	lastSBlock := util.Back(sb._radixSortingData)
	sortingPtr := util.PointerAdd(
		lastSBlock._ptr,
		ls._sortLayout._comparisonSize,
	)
	if !ls._sortLayout._allConstant {
		ls.ReOrder2(
			sb._blobSortingData,
			sortingPtr,
			ls._blobSortingHeap,
			reorderHeap,
		)
	}
	ls.ReOrder2(
		sb._payloadData,
		sortingPtr,
		ls._payloadHeap,
		reorderHeap)
}

func (ls *LocalSort) ReOrder2(
	sd *SortedData,
	sortingPtr unsafe.Pointer,
	heap *RowDataCollection,
	reorderHeap bool,
) {
	unorderedDBlock := util.Back(sd._dataBlocks)
	count := unorderedDBlock._count
	unorderedDataPtr := unorderedDBlock._ptr
	orderedDBlock := NewRowDataBlock(
		unorderedDBlock._capacity,
		unorderedDBlock._entrySize,
	)

	orderedDBlock._count = count
	orderedDataPtr := orderedDBlock._ptr

	//reorder fix row
	rowWidth := sd._layout.rowWidth()
	sortingEntrySize := ls._sortLayout._entrySize
	for i := 0; i < count; i++ {
		index := util.Load[uint32](sortingPtr)
		util.PointerCopy(
			orderedDataPtr,
			util.PointerAdd(unorderedDataPtr, int(index)*rowWidth),
			rowWidth,
		)
		orderedDataPtr = util.PointerAdd(orderedDataPtr, rowWidth)
		sortingPtr = util.PointerAdd(sortingPtr, sortingEntrySize)

	}

	sd._dataBlocks = nil
	sd._dataBlocks = append(
		sd._dataBlocks,
		orderedDBlock,
	)
	//deal with the heap
	if !sd._layout.AllConstant() && reorderHeap {
		totalByteOffset := 0
		for _, block := range heap._blocks {
			totalByteOffset += block._byteOffset
		}
		heapBlockSize := max(totalByteOffset, BLOCK_SIZE)
		orderedHeapBlock := NewRowDataBlock(heapBlockSize, 1)
		orderedHeapBlock._count = count
		orderedHeapBlock._byteOffset = totalByteOffset
		orderedHeapPtr := orderedHeapBlock._ptr
		//fill heap
		orderedDataPtr = orderedDBlock._ptr
		heapPointerOffset := sd._layout.GetHeapOffset()
		for i := 0; i < count; i++ {
			heapRowPtr := util.Load[unsafe.Pointer](
				util.PointerAdd(orderedDataPtr, heapPointerOffset),
			)
			util.AssertFunc(util.PointerValid(heapRowPtr))
			heapRowSize := util.Load[uint32](heapRowPtr)
			util.PointerCopy(orderedHeapPtr, heapRowPtr, int(heapRowSize))
			orderedHeapPtr = util.PointerAdd(orderedHeapPtr, int(heapRowSize))
			orderedDataPtr = util.PointerAdd(orderedDataPtr, rowWidth)
		}

		sd._heapBlocks = append(sd._heapBlocks, orderedHeapBlock)
		heap._blocks = nil
		heap._count = 0
	}
}

func (ls *LocalSort) ConcatenateBlocks(rowData *RowDataCollection) *RowDataBlock {
	if len(rowData._blocks) == 1 {
		ret := rowData._blocks[0]
		rowData._blocks[0] = nil
		rowData._count = 0
		return ret
	}
	a := (BLOCK_SIZE + rowData._entrySize - 1) / rowData._entrySize
	b := rowData._count
	capacity := max(a, b)
	newBlock := NewRowDataBlock(capacity, rowData._entrySize)
	newBlock._count = rowData._count
	newBlockPtr := newBlock._ptr
	//copy data in blocks into block
	for i := 0; i < len(rowData._blocks); i++ {
		block := rowData._blocks[i]
		cLen := block._count * rowData._entrySize
		util.PointerCopy(newBlockPtr, block._ptr, cLen)
		newBlockPtr = util.PointerAdd(newBlockPtr, cLen)
	}
	rowData.Close()
	return newBlock
}
