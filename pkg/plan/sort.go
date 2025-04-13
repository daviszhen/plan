// Copyright 2023-2024 daviszhen
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"sort"
	"unsafe"

	"github.com/daviszhen/plan/pkg/chunk"
	"github.com/daviszhen/plan/pkg/common"
	"github.com/daviszhen/plan/pkg/util"
)

type OrderType int

const (
	OT_INVALID OrderType = iota
	OT_DEFAULT
	OT_ASC
	OT_DESC
)

type OrderByNullType int

const (
	OBNT_INVALID OrderByNullType = iota
	OBNT_DEFAULT
	OBNT_NULLS_FIRST
	OBNT_NULLS_LAST
)

const (
	VALUES_PER_RADIX              = 256
	MSD_RADIX_LOCATIONS           = VALUES_PER_RADIX + 1
	INSERTION_SORT_THRESHOLD      = 24
	MSD_RADIX_SORT_SIZE_THRESHOLD = 4
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

type SortedDataType int

const (
	SDT_BLOB    SortedDataType = 0
	SDT_PAYLOAD SortedDataType = 1
)

type SortedData struct {
	_type       SortedDataType
	_layout     *RowLayout
	_dataBlocks []*RowDataBlock
	_heapBlocks []*RowDataBlock
}

func (d SortedData) Count() int {
	cnt := 0
	for _, blk := range d._dataBlocks {
		cnt += blk._count
	}
	return cnt
}

func NewSortedData(typ SortedDataType, layout *RowLayout) *SortedData {
	ret := &SortedData{
		_type:   typ,
		_layout: layout,
	}

	return ret
}

type SortedBlock struct {
	_radixSortingData []*RowDataBlock
	_blobSortingData  *SortedData
	_payloadData      *SortedData
	_sortLayout       *SortLayout
	_payloadLayout    *RowLayout
}

func NewSortedBlock(sortLayout *SortLayout, payloadLayout *RowLayout) *SortedBlock {
	ret := &SortedBlock{
		_sortLayout:    sortLayout,
		_payloadLayout: payloadLayout,
	}

	ret._blobSortingData = NewSortedData(SDT_BLOB, sortLayout._blobLayout)
	ret._payloadData = NewSortedData(SDT_PAYLOAD, payloadLayout)
	return ret
}

type BlockAppendEntry struct {
	_basePtr unsafe.Pointer
	_count   int
}

type RowDataCollection struct {
	_count         int
	_blockCapacity int
	_entrySize     int
	_blocks        []*RowDataBlock
}

func NewRowDataCollection(bcap int, entSize int) *RowDataCollection {
	ret := &RowDataCollection{
		_blockCapacity: bcap,
		_entrySize:     entSize,
	}

	return ret
}

func (cdc *RowDataCollection) Build(
	addedCnt int,
	keyLocs []unsafe.Pointer,
	entrySizes []int,
	sel *chunk.SelectVector) {
	appendEntries := make([]BlockAppendEntry, 0)
	remaining := addedCnt
	{
		//to last block
		cdc._count += remaining
		if len(cdc._blocks) != 0 {
			lastBlock := util.Back(cdc._blocks)
			if lastBlock._count < lastBlock._capacity {
				appendCnt := cdc.AppendToBlock(lastBlock, &appendEntries, remaining, entrySizes)
				remaining -= appendCnt
			}
		}
		for remaining > 0 {
			newBlock := cdc.CreateBlock()
			var offsetEntrySizes []int = nil
			if entrySizes != nil {
				offsetEntrySizes = entrySizes[addedCnt-remaining:]
			}
			appendCnt := cdc.AppendToBlock(newBlock, &appendEntries, remaining, offsetEntrySizes)
			util.AssertFunc(newBlock._count > 0)
			remaining -= appendCnt

		}
	}
	//fill keyLocs
	aidx := 0
	for _, entry := range appendEntries {
		next := aidx + entry._count
		if entrySizes != nil {
			for ; aidx < next; aidx++ {
				keyLocs[aidx] = entry._basePtr
				entry._basePtr = util.PointerAdd(entry._basePtr, entrySizes[aidx])
			}
		} else {
			for ; aidx < next; aidx++ {
				idx := sel.GetIndex(aidx)
				keyLocs[idx] = entry._basePtr
				entry._basePtr = util.PointerAdd(entry._basePtr, cdc._entrySize)
			}
		}
	}
}

func (cdc *RowDataCollection) AppendToBlock(
	block *RowDataBlock,
	appendEntries *[]BlockAppendEntry,
	remaining int,
	entrySizes []int) int {
	appendCnt := 0
	var dataPtr unsafe.Pointer
	if entrySizes != nil {
		util.AssertFunc(cdc._entrySize == 1)
		dataPtr = util.PointerAdd(block._ptr, block._byteOffset)
		for i := 0; i < remaining; i++ {
			if block._byteOffset+entrySizes[i] > block._capacity {
				if block._count == 0 &&
					appendCnt == 0 &&
					entrySizes[i] > block._capacity {
					block._capacity = entrySizes[i]
					block._ptr = util.CRealloc(block._ptr, block._capacity)
					dataPtr = block._ptr
					appendCnt++
					block._byteOffset += entrySizes[i]
				}
				break
			}
			appendCnt++
			block._byteOffset += entrySizes[i]
		}
	} else {
		appendCnt = min(remaining, block._capacity-block._count)
		dataPtr = util.PointerAdd(block._ptr, block._count*block._entrySize)
	}
	*appendEntries = append(*appendEntries, BlockAppendEntry{
		_basePtr: dataPtr,
		_count:   appendCnt,
	})
	block._count += appendCnt
	return appendCnt
}

func (cdc *RowDataCollection) CreateBlock() *RowDataBlock {
	nb := NewRowDataBlock(cdc._blockCapacity, cdc._entrySize)
	cdc._blocks = append(cdc._blocks, nb)
	return nb
}

func (cdc *RowDataCollection) Close() {
	for _, block := range cdc._blocks {
		block.Close()
	}
	cdc._blocks = nil
	cdc._count = 0
}

type SortState int

const (
	SS_INIT SortState = iota
	SS_SORT
	SS_SCAN
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

func RadixSort(
	dataPtr unsafe.Pointer,
	count int,
	colOffset int,
	sortingSize int,
	sortLayout *SortLayout,
	containsString bool,
) {
	if containsString {
		begin := NewPDQIterator(dataPtr, sortLayout._entrySize)
		end := begin.plusCopy(count)
		constants := NewPDQConstants(sortLayout._entrySize, colOffset, sortingSize, end.ptr())
		pdqsortBranchless(begin, &end, constants)
	} else if count <= INSERTION_SORT_THRESHOLD {
		InsertionSort(
			dataPtr,
			nil,
			count,
			0,
			sortLayout._entrySize,
			sortLayout._comparisonSize,
			0,
			false,
		)
	} else if sortingSize <= MSD_RADIX_SORT_SIZE_THRESHOLD {
		RadixSortLSD(
			dataPtr,
			count,
			colOffset,
			sortLayout._entrySize,
			sortingSize,
		)
	} else {
		tempPtr := util.CMalloc(max(count*sortLayout._entrySize, BLOCK_SIZE))
		defer util.CFree(tempPtr)
		preAllocPtr := util.CMalloc(sortingSize * MSD_RADIX_LOCATIONS * int(unsafe.Sizeof(uint64(0))))
		defer util.CFree(preAllocPtr)
		RadixSortMSD(
			dataPtr,
			tempPtr,
			count,
			colOffset,
			sortLayout._entrySize,
			sortingSize,
			0,
			util.PointerToSlice[uint64](preAllocPtr, sortingSize*MSD_RADIX_LOCATIONS),
			false,
		)
	}
}

func SubSortTiedTuples(
	dataPtr unsafe.Pointer,
	count int,
	colOffset int,
	sortingSize int,
	ties []bool,
	layout *SortLayout,
	containsString bool) {
	util.AssertFunc(!ties[count-1])
	for i := 0; i < count; i++ {
		if !ties[i] {
			continue
		}

		var j int
		for j = i + 1; j < count; j++ {
			if !ties[j] {
				break
			}
		}
		RadixSort(
			util.PointerAdd(dataPtr, i*layout._entrySize),
			j-i+1,
			colOffset,
			sortingSize,
			layout,
			containsString,
		)
		i = j
	}
}

func ComputeTies(
	dataPtr unsafe.Pointer,
	count int,
	colOffset int,
	tieSize int,
	ties []bool,
	layout *SortLayout) {
	util.AssertFunc(!ties[count-1])
	util.AssertFunc(colOffset+tieSize <= layout._comparisonSize)
	dataPtr = util.PointerAdd(dataPtr, colOffset)
	for i := 0; i < count-1; i++ {
		ties[i] = ties[i] &&
			util.PointerMemcmp(
				dataPtr,
				util.PointerAdd(dataPtr, layout._entrySize),
				tieSize,
			) == 0
		dataPtr = util.PointerAdd(dataPtr, layout._entrySize)
	}
}

func SortTiedBlobs(
	sb *SortedBlock,
	ties []bool,
	dataPtr unsafe.Pointer,
	count int,
	tieCol int,
	layout *SortLayout) {
	util.AssertFunc(!ties[count-1])
	block := util.Back(sb._blobSortingData._dataBlocks)
	blobPtr := block._ptr
	for i := 0; i < count; i++ {
		if !ties[i] {
			continue
		}
		var j int
		for j = i; j < count; j++ {
			if !ties[j] {
				break
			}
		}
		SortTiedBlobs2(
			dataPtr,
			i,
			j+1,
			tieCol,
			ties,
			blobPtr,
			layout,
		)
		i = j
	}
}

func SortTiedBlobs2(
	dataPtr unsafe.Pointer,
	start int,
	end int,
	tieCol int,
	ties []bool,
	blobPtr unsafe.Pointer,
	layout *SortLayout,
) {
	rowWidth := layout._blobLayout.rowWidth()
	rowPtr := util.PointerAdd(dataPtr, start*layout._entrySize)
	x := int(util.Load[uint32](util.PointerAdd(rowPtr, layout._comparisonSize)))
	blobRowPtr := util.PointerAdd(
		blobPtr,
		x*rowWidth,
	)
	if !TieIsBreakable(
		tieCol,
		blobRowPtr,
		layout,
	) {
		return
	}

	entryPtrsBase := util.CMalloc((end - start) * common.PointerSize)
	defer util.CFree(entryPtrsBase)

	//prepare pointer
	entryPtrs := util.PointerToSlice[unsafe.Pointer](entryPtrsBase, end-start)
	for i := start; i < end; i++ {
		entryPtrs[i-start] = rowPtr
		rowPtr = util.PointerAdd(rowPtr, layout._entrySize)
	}

	//sort string
	order := 1
	if layout._orderTypes[tieCol] == OT_DESC {
		order = -1
	}
	colIdx := layout._sortingToBlobCol[tieCol]
	tieColOffset := layout._blobLayout.GetOffsets()[colIdx]
	logicalType := layout._blobLayout.GetTypes()[colIdx]
	sort.Slice(entryPtrs, func(i, j int) bool {
		lPtr := entryPtrs[i]
		rPtr := entryPtrs[j]
		lIdx := util.Load[uint32](util.PointerAdd(lPtr, layout._comparisonSize))
		rIdx := util.Load[uint32](util.PointerAdd(rPtr, layout._comparisonSize))
		leftPtr := util.PointerAdd(blobPtr, int(lIdx)*rowWidth+tieColOffset)
		rightPtr := util.PointerAdd(blobPtr, int(rIdx)*rowWidth+tieColOffset)
		return order*CompareVal(leftPtr, rightPtr, logicalType) < 0
	})

	//reorder
	tempBasePtr := util.CMalloc((end - start) * layout._entrySize)
	defer util.CFree(tempBasePtr)
	tempPtr := tempBasePtr

	for i := 0; i < end-start; i++ {
		util.PointerCopy(tempPtr, entryPtrs[i], layout._entrySize)
		tempPtr = util.PointerAdd(tempPtr, layout._entrySize)
	}

	util.PointerCopy(
		util.PointerAdd(dataPtr, start*layout._entrySize),
		tempBasePtr,
		(end-start)*layout._entrySize,
	)
	//check ties
	if tieCol < layout._columnCount-1 {
		idxPtr := util.PointerAdd(dataPtr,
			start*layout._entrySize+layout._comparisonSize)
		idxVal := util.Load[uint32](idxPtr)
		currentPtr := util.PointerAdd(blobPtr, int(idxVal)*rowWidth+tieColOffset)
		for i := 0; i < (end - start - 1); i++ {
			idxPtr = util.PointerAdd(idxPtr, layout._entrySize)
			idxVal2 := util.Load[uint32](idxPtr)
			nextPtr := util.PointerAdd(blobPtr, int(idxVal2)*rowWidth+tieColOffset)
			ret := CompareVal(currentPtr, nextPtr, logicalType) == 0
			ties[start+i] = ret
			currentPtr = nextPtr
		}
	}
}

func AnyTies(ties []bool, count int) bool {
	util.AssertFunc(!ties[count-1])
	anyTies := false
	for i := 0; i < count-1; i++ {
		anyTies = anyTies || ties[i]
	}
	return anyTies
}

func RadixScatter(
	v *chunk.Vector,
	vcount int,
	sel *chunk.SelectVector,
	serCount int,
	keyLocs []unsafe.Pointer,
	desc bool,
	hasNull bool,
	nullsFirst bool,
	prefixLen int,
	width int,
	offset int,
) {
	var vdata chunk.UnifiedFormat
	v.ToUnifiedFormat(vcount, &vdata)
	switch v.Typ().GetInternalType() {
	case common.BOOL:
	case common.INT32:
		TemplatedRadixScatter[int32](
			&vdata,
			sel,
			serCount,
			keyLocs,
			desc,
			hasNull,
			nullsFirst,
			offset,
			int32Encoder{},
		)
	case common.VARCHAR:
		RadixScatterStringVector(
			&vdata,
			sel,
			serCount,
			keyLocs,
			desc,
			hasNull,
			nullsFirst,
			prefixLen,
			offset,
		)
	case common.DECIMAL:
		TemplatedRadixScatter[common.Decimal](
			&vdata,
			sel,
			serCount,
			keyLocs,
			desc,
			hasNull,
			nullsFirst,
			offset,
			decimalEncoder{},
		)
	case common.DATE:
		TemplatedRadixScatter[common.Date](
			&vdata,
			sel,
			serCount,
			keyLocs,
			desc,
			hasNull,
			nullsFirst,
			offset,
			dateEncoder{},
		)
	case common.INT128:
		TemplatedRadixScatter[common.Hugeint](
			&vdata,
			sel,
			serCount,
			keyLocs,
			desc,
			hasNull,
			nullsFirst,
			offset,
			hugeEncoder{},
		)
	default:
		panic("usp")
	}
}

func TemplatedRadixScatter[T any](
	vdata *chunk.UnifiedFormat,
	sel *chunk.SelectVector,
	addCount int,
	keyLocs []unsafe.Pointer,
	desc bool,
	hasNull bool,
	nullsFirst bool,
	offset int,
	enc Encoder[T],
) {
	srcSlice := chunk.GetSliceInPhyFormatUnifiedFormat[T](vdata)
	if hasNull {
		mask := vdata.Mask
		valid := byte(0)
		if nullsFirst {
			valid = 1
		}
		invalid := 1 - valid
		for i := 0; i < addCount; i++ {
			idx := sel.GetIndex(i)
			srcIdx := vdata.Sel.GetIndex(idx) + offset
			if mask.RowIsValid(uint64(srcIdx)) {
				//first byte
				util.Store[byte](valid, keyLocs[i])
				enc.EncodeData(util.PointerAdd(keyLocs[i], 1), &srcSlice[srcIdx])
				//desc , invert bits
				if desc {
					for s := 1; s < enc.TypeSize()+1; s++ {
						util.InvertBits(keyLocs[i], s)
					}
				}
			} else {
				util.Store[byte](invalid, keyLocs[i])
				util.Memset(util.PointerAdd(keyLocs[i], 1), 0, enc.TypeSize())
			}
			keyLocs[i] = util.PointerAdd(keyLocs[i], 1+enc.TypeSize())
		}
	} else {
		for i := 0; i < addCount; i++ {
			idx := sel.GetIndex(i)
			srcIdx := vdata.Sel.GetIndex(idx) + offset
			enc.EncodeData(keyLocs[i], &srcSlice[srcIdx])
			if desc {
				for s := 0; s < enc.TypeSize(); s++ {
					util.InvertBits(keyLocs[i], s)
				}
			}
			keyLocs[i] = util.PointerAdd(keyLocs[i], enc.TypeSize())
		}
	}
}

func Scatter(
	columns *chunk.Chunk,
	colData []*chunk.UnifiedFormat,
	layout *RowLayout,
	rows *chunk.Vector,
	stringHeap *RowDataCollection,
	sel *chunk.SelectVector,
	count int,
) {
	if count == 0 {
		return
	}

	ptrs := chunk.GetSliceInPhyFormatFlat[unsafe.Pointer](rows)
	for i := 0; i < count; i++ {
		ridx := sel.GetIndex(i)
		rowPtr := ptrs[ridx]
		bSlice := util.PointerToSlice[uint8](rowPtr, layout.CoumnCount())
		tempMask := util.Bitmap{Bits: bSlice}
		tempMask.SetAllValid(layout.CoumnCount())
	}

	//vcount := columns.card()
	offsets := layout.GetOffsets()
	types := layout.GetTypes()

	//compute the entry size of the variable size columns
	dataLocs := make([]unsafe.Pointer, util.DefaultVectorSize)
	if !layout.AllConstant() {
		entrySizes := make([]int, util.DefaultVectorSize)
		util.Fill(entrySizes, count, common.Int32Size)
		for colNo := 0; colNo < len(types); colNo++ {
			if types[colNo].GetInternalType().IsConstant() {
				continue
			}
			//vec := columns.Data[colNo]
			col := colData[colNo]
			switch types[colNo].GetInternalType() {
			case common.VARCHAR:
				ComputeStringEntrySizes(col, entrySizes, sel, count, 0)
			default:
				panic("usp internal type")
			}
		}
		stringHeap.Build(count, dataLocs, entrySizes, chunk.IncrSelectVectorInPhyFormatFlat())

		heapPointerOffset := layout.GetHeapOffset()
		for i := 0; i < count; i++ {
			rowIdx := sel.GetIndex(i)
			rowPtr := ptrs[rowIdx]
			util.Store[unsafe.Pointer](dataLocs[i], util.PointerAdd(rowPtr, heapPointerOffset))
			util.Store[uint32](uint32(entrySizes[i]), dataLocs[i])
			dataLocs[i] = util.PointerAdd(dataLocs[i], common.Int32Size)
		}
	}

	for colNo := 0; colNo < len(types); colNo++ {
		col := colData[colNo]
		colOffset := offsets[colNo]
		switch types[colNo].GetInternalType() {
		case common.INT32:
			TemplatedScatter[int32](
				col,
				rows,
				sel,
				count,
				colOffset,
				colNo,
				layout,
				chunk.Int32ScatterOp{},
			)
		case common.INT64:
			TemplatedScatter[int64](
				col,
				rows,
				sel,
				count,
				colOffset,
				colNo,
				layout,
				chunk.Int64ScatterOp{},
			)
		case common.VARCHAR:
			ScatterStringVector(
				col,
				rows,
				dataLocs,
				sel,
				count,
				colOffset,
				colNo,
				layout,
			)
		case common.DATE:
			TemplatedScatter[common.Date](
				col,
				rows,
				sel,
				count,
				colOffset,
				colNo,
				layout,
				chunk.DateScatterOp{},
			)
		case common.DECIMAL:
			TemplatedScatter[common.Decimal](
				col,
				rows,
				sel,
				count,
				colOffset,
				colNo,
				layout,
				chunk.DecimalScatterOp{},
			)
		case common.DOUBLE:
			TemplatedScatter[float64](
				col,
				rows,
				sel,
				count,
				colOffset,
				colNo,
				layout,
				chunk.Float64ScatterOp{},
			)
		case common.INT128:
			TemplatedScatter[common.Hugeint](
				col,
				rows,
				sel,
				count,
				colOffset,
				colNo,
				layout,
				chunk.HugeintScatterOp{},
			)
		default:
			panic("usp")
		}
	}
}

func ScatterStringVector(
	col *chunk.UnifiedFormat,
	rows *chunk.Vector,
	strLocs []unsafe.Pointer,
	sel *chunk.SelectVector,
	count int,
	colOffset int,
	colNo int,
	layout *RowLayout,
) {
	strSlice := chunk.GetSliceInPhyFormatUnifiedFormat[common.String](col)
	ptrSlice := chunk.GetSliceInPhyFormatFlat[unsafe.Pointer](rows)

	nullStr := chunk.StringScatterOp{}.NullValue()
	for i := 0; i < count; i++ {
		idx := sel.GetIndex(i)
		colIdx := col.Sel.GetIndex(idx)
		rowPtr := ptrSlice[idx]
		if !col.Mask.RowIsValid(uint64(colIdx)) {
			colMask := util.Bitmap{
				Bits: util.PointerToSlice[byte](rowPtr, layout._flagWidth),
			}
			colMask.SetInvalidUnsafe(uint64(colNo))
			util.Store[common.String](nullStr, util.PointerAdd(rowPtr, colOffset))
		} else {
			str := strSlice[colIdx]
			newStr := common.String{
				Len:  str.Length(),
				Data: strLocs[i],
			}
			//copy varchar data from input chunk to
			//the location on the string heap
			util.PointerCopy(newStr.Data, str.DataPtr(), str.Length())
			//move strLocs[i] to the next position
			strLocs[i] = util.PointerAdd(strLocs[i], str.Length())

			//store new String obj to the row in the blob sort block
			util.Store[common.String](newStr, util.PointerAdd(rowPtr, colOffset))
		}
	}
}

func RadixScatterStringVector(
	vdata *chunk.UnifiedFormat,
	sel *chunk.SelectVector,
	addCount int,
	keyLocs []unsafe.Pointer,
	desc bool,
	hasNull bool,
	nullsFirst bool,
	prefixLen int,
	offset int,
) {
	sourceSlice := chunk.GetSliceInPhyFormatUnifiedFormat[common.String](vdata)
	if hasNull {
		mask := vdata.Mask
		valid := byte(0)
		if nullsFirst {
			valid = 1
		}
		invalid := 1 - valid

		for i := 0; i < addCount; i++ {
			idx := sel.GetIndex(i)
			srcIdx := vdata.Sel.GetIndex(idx) + offset
			if mask.RowIsValid(uint64(srcIdx)) {
				util.Store[byte](valid, keyLocs[i])
				EncodeStringDataPrefix(
					util.PointerAdd(keyLocs[i], 1),
					&sourceSlice[srcIdx],
					prefixLen,
				)
				//invert bits
				if desc {
					for s := 1; s < prefixLen+1; s++ {
						util.InvertBits(keyLocs[i], s)
					}
				}
			} else {
				util.Store[byte](invalid, keyLocs[i])
				util.Memset(
					util.PointerAdd(keyLocs[i], 1),
					0,
					prefixLen,
				)
			}
			keyLocs[i] = util.PointerAdd(keyLocs[i], prefixLen+1)
		}
	} else {
		for i := 0; i < addCount; i++ {
			idx := sel.GetIndex(i)
			srcIdx := vdata.Sel.GetIndex(idx) + offset
			EncodeStringDataPrefix(
				keyLocs[i],
				&sourceSlice[srcIdx],
				prefixLen,
			)
			//invert bits
			if desc {
				for s := 0; s < prefixLen; s++ {
					util.InvertBits(keyLocs[i], s)
				}
			}
			keyLocs[i] = util.PointerAdd(keyLocs[i], prefixLen)
		}
	}
}

func EncodeStringDataPrefix(
	dataPtr unsafe.Pointer,
	value *common.String,
	prefixLen int) {
	l := value.Length()
	util.PointerCopy(dataPtr, value.DataPtr(), min(l, prefixLen))

	if l < prefixLen {
		util.Memset(util.PointerAdd(dataPtr, l), 0, prefixLen-l)
	}
}

func ComputeStringEntrySizes(
	col *chunk.UnifiedFormat,
	entrySizes []int,
	sel *chunk.SelectVector,
	count int,
	offset int,
) {
	data := chunk.GetSliceInPhyFormatUnifiedFormat[common.String](col)
	for i := 0; i < count; i++ {
		idx := sel.GetIndex(i)
		colIdx := col.Sel.GetIndex(idx) + offset
		str := data[colIdx]
		if col.Mask.RowIsValid(uint64(colIdx)) {
			entrySizes[i] += str.Length()
		}
	}
}

func TemplatedScatter[T any](
	col *chunk.UnifiedFormat,
	rows *chunk.Vector,
	sel *chunk.SelectVector,
	count int,
	colOffset int,
	colNo int,
	layout *RowLayout,
	sop chunk.ScatterOp[T],
) {
	data := chunk.GetSliceInPhyFormatUnifiedFormat[T](col)
	ptrs := chunk.GetSliceInPhyFormatFlat[unsafe.Pointer](rows)

	if !col.Mask.AllValid() {
		for i := 0; i < count; i++ {
			idx := sel.GetIndex(i)
			colIdx := col.Sel.GetIndex(idx)
			rowPtr := ptrs[idx]

			isNull := !col.Mask.RowIsValid(uint64(colIdx))
			var val T
			if isNull {
				val = sop.NullValue()
			} else {
				val = data[colIdx]
			}

			util.Store[T](val, util.PointerAdd(rowPtr, colOffset))
			if isNull {
				mask := util.Bitmap{
					Bits: util.PointerToSlice[uint8](ptrs[idx], layout.rowWidth()),
				}
				mask.SetInvalidUnsafe(uint64(colNo))
			}
		}
	} else {
		for i := 0; i < count; i++ {
			idx := sel.GetIndex(i)
			colIdx := col.Sel.GetIndex(idx)
			rowPtr := ptrs[idx]
			util.Store[T](data[colIdx], util.PointerAdd(rowPtr, colOffset))
		}
	}
}

const (
	//size <= this, insert sort
	insertion_sort_threshold = 24

	//partitions size > this, use ninther to choice pivot
	ninther_threshold = 128

	//
	partial_insertion_sort_limit = 8

	block_size = 64

	cacheline_size = 64
)

type PDQConstants struct {
	_tmpBuf         unsafe.Pointer
	_swapOffsetsBuf unsafe.Pointer
	_iterSwapBuf    unsafe.Pointer
	_end            unsafe.Pointer
	_compOffset     int
	_compSize       int
	_entrySize      int
}

func NewPDQConstants(
	entrySize int,
	compOffset int,
	compSize int,
	end unsafe.Pointer,
) *PDQConstants {
	ret := &PDQConstants{
		_entrySize:      entrySize,
		_compOffset:     compOffset,
		_compSize:       compSize,
		_tmpBuf:         util.CMalloc(entrySize),
		_iterSwapBuf:    util.CMalloc(entrySize),
		_swapOffsetsBuf: util.CMalloc(entrySize),
		_end:            end,
	}

	return ret
}

func (pconst *PDQConstants) Close() {
	util.CFree(pconst._tmpBuf)
	util.CFree(pconst._iterSwapBuf)
	util.CFree(pconst._swapOffsetsBuf)
}

type PDQIterator struct {
	_ptr       unsafe.Pointer
	_entrySize int
}

func NewPDQIterator(ptr unsafe.Pointer, entrySize int) *PDQIterator {
	return &PDQIterator{
		_ptr:       ptr,
		_entrySize: entrySize,
	}
}

func (iter *PDQIterator) ptr() unsafe.Pointer {
	return iter._ptr
}

func (iter *PDQIterator) plus(n int) {
	iter._ptr = util.PointerAdd(iter._ptr, n*iter._entrySize)
}

func (iter PDQIterator) plusCopy(n int) PDQIterator {
	return PDQIterator{
		_ptr:       util.PointerAdd(iter._ptr, n*iter._entrySize),
		_entrySize: iter._entrySize,
	}
}

func pdqIterLess(lhs, rhs *PDQIterator) bool {
	return util.PointerLess(lhs.ptr(), rhs.ptr())
}

func pdqIterDiff(lhs, rhs *PDQIterator) int {
	tlen := util.PointerSub(lhs.ptr(), rhs.ptr())
	util.AssertFunc(tlen%int64(lhs._entrySize) == 0)
	util.AssertFunc(tlen >= 0)
	return int(tlen / int64(lhs._entrySize))
}

func pdqIterEqaul(lhs, rhs *PDQIterator) bool {
	return lhs.ptr() == rhs.ptr()
}

func pdqIterNotEqaul(lhs, rhs *PDQIterator) bool {
	return !pdqIterEqaul(lhs, rhs)
}

func pdqsortBranchless(
	begin, end *PDQIterator,
	constants *PDQConstants) {
	if begin == end {
		return
	}
	pdqsortLoop(begin, end, constants, log2(pdqIterDiff(end, begin)) > 0, true, true)
}

//func pdqsort(
//	begin, end *PDQIterator,
//	constants *PDQConstants) {
//	if begin == end {
//		return
//	}
//	pdqsortLoop(begin, end, constants, log2(pdqIterDiff(end, begin)) > 0, true, false)
//}

func log2(diff int) int {
	log := 0
	for {
		diff >>= 1
		if diff <= 0 {
			break
		}
		log++
	}
	return log
}

func pdqsortLoop(
	begin, end *PDQIterator,
	constants *PDQConstants,
	badAllowed bool,
	leftMost bool,
	branchLess bool,
) {
	for {
		size := pdqIterDiff(end, begin)
		//insert sort
		if size < insertion_sort_threshold {
			if leftMost {
				insertSort(begin, end, constants)
			} else {
				//FIXME: has bug
				unguardedInsertSort(begin, end, constants)
			}
			return
		}

		//pivot : median of 3
		//pseudomedian of 9
		s2 := size / 2
		if size > ninther_threshold {
			b0 := begin.plusCopy(s2)
			c0 := end.plusCopy(-1)
			sort3(begin, &b0, &c0, constants)

			a1 := begin.plusCopy(1)
			b1 := begin.plusCopy(s2 - 1)
			c1 := end.plusCopy(-2)
			sort3(&a1, &b1, &c1, constants)

			a2 := begin.plusCopy(2)
			b2 := begin.plusCopy(s2 + 1)
			c2 := end.plusCopy(-3)
			sort3(&a2, &b2, &c2, constants)

			a3 := begin.plusCopy(s2 - 1)
			b3 := begin.plusCopy(s2)
			c3 := begin.plusCopy(s2 + 1)
			sort3(&a3, &b3, &c3, constants)
		} else {
			a0 := begin.plusCopy(s2)
			c0 := end.plusCopy(-1)
			sort3(&a0, begin, &c0, constants)
		}

		if !leftMost {
			a0 := begin.plusCopy(-1)
			if !comp(a0.ptr(), begin.ptr(), constants) {
				b0 := partitionLeft(begin, end, constants)
				b0.plus(1)
				begin = &b0
				continue
			}
		}

		var pivotPos PDQIterator
		var alreadyPartitioned bool
		if branchLess {
			pivotPos, alreadyPartitioned = partitionRightBranchless(begin, end, constants)
		} else {
			pivotPos, alreadyPartitioned = partitionRight(begin, end, constants)
		}

		lSize := pdqIterDiff(&pivotPos, begin)
		x := pivotPos.plusCopy(1)
		rSize := pdqIterDiff(end, &x)
		highlyUnbalanced := lSize < size/8 || rSize < size/8
		if highlyUnbalanced {
			if lSize > insertion_sort_threshold {
				b0 := begin.plusCopy(lSize / 4)
				iterSwap(begin, &b0, constants)

				a1 := pivotPos.plusCopy(-1)
				b1 := pivotPos.plusCopy(-lSize / 4)
				iterSwap(&a1, &b1, constants)

				if lSize > ninther_threshold {
					a2 := begin.plusCopy(1)
					b2 := begin.plusCopy(lSize/4 + 1)
					iterSwap(&a2, &b2, constants)

					a3 := begin.plusCopy(2)
					b3 := begin.plusCopy(lSize/4 + 2)
					iterSwap(&a3, &b3, constants)

					a4 := pivotPos.plusCopy(-2)
					b4 := pivotPos.plusCopy(-(lSize/4 + 1))
					iterSwap(&a4, &b4, constants)

					a5 := pivotPos.plusCopy(-3)
					b5 := pivotPos.plusCopy(-(lSize/4 + 2))
					iterSwap(&a5, &b5, constants)
				}
			}

			if rSize > insertion_sort_threshold {
				a0 := pivotPos.plusCopy(1)
				b0 := pivotPos.plusCopy(rSize/4 + 1)
				iterSwap(&a0, &b0, constants)

				a1 := end.plusCopy(-1)
				b1 := end.plusCopy(-(rSize / 4))
				iterSwap(&a1, &b1, constants)

				if rSize > ninther_threshold {
					a2 := pivotPos.plusCopy(2)
					b2 := pivotPos.plusCopy(rSize/4 + 2)
					iterSwap(&a2, &b2, constants)

					a3 := pivotPos.plusCopy(3)
					b3 := pivotPos.plusCopy(rSize/4 + 3)
					iterSwap(&a3, &b3, constants)

					a4 := end.plusCopy(-2)
					b4 := end.plusCopy(-(1 + rSize/4))
					iterSwap(&a4, &b4, constants)

					a5 := end.plusCopy(-3)
					b5 := end.plusCopy(-(2 + rSize/4))
					iterSwap(&a5, &b5, constants)
				}
			}
		} else {
			if alreadyPartitioned {
				if partialInsertionSort(begin, &pivotPos, constants) {
					x = pivotPos.plusCopy(1)
					if partialInsertionSort(&x, end, constants) {
						return
					}
				}
			}
		}

		//sort left part
		pdqsortLoop(begin, &pivotPos, constants, badAllowed, leftMost, branchLess)
		x = pivotPos.plusCopy(1)
		begin = &x
		leftMost = false
	}
}

func partialInsertionSort(begin *PDQIterator, end *PDQIterator, constants *PDQConstants) bool {
	if pdqIterEqaul(begin, end) {
		return true
	}
	limit := uint64(0)
	for cur := begin.plusCopy(1); pdqIterNotEqaul(&cur, end); cur.plus(1) {
		sift := cur.plusCopy(0)
		sift_1 := cur.plusCopy(-1)
		if comp(sift.ptr(), sift_1.ptr(), constants) {
			tmp := GetTmp(sift.ptr(), constants)
			for {
				Move(sift.ptr(), sift_1._ptr, constants)
				sift.plus(-1)
				if pdqIterNotEqaul(&sift, begin) {
					sift_1.plus(-1)
					if comp(tmp, sift_1.ptr(), constants) {
						continue
					} else {
						break
					}
				}
			}

			Move(sift.ptr(), tmp, constants)
			limit += uint64(pdqIterDiff(&cur, &sift))
		}

		if limit > partial_insertion_sort_limit {
			return false
		}
	}
	return true
}

func partitionRight(begin *PDQIterator, end *PDQIterator, constants *PDQConstants) (PDQIterator, bool) {
	pivot := GetTmp(begin.ptr(), constants)

	first := begin.plusCopy(0)
	last := end.plusCopy(0)

	//find the first one *first >= *pivot in [begin+1,...)
	for {
		first.plus(1)
		if comp(first.ptr(), pivot, constants) {
			continue
		} else {
			break
		}
	}

	//*(begin+1) >= *begin
	if pdqIterDiff(&first, begin) == 1 {
		for pdqIterLess(&first, &last) {
			last.plus(-1)
			//find the first one stricter *last < *pivot
			if !comp(last.ptr(), pivot, constants) {
				continue
			} else {
				break
			}
		}
	} else {
		for {
			last.plus(-1)
			//find the first one stricter *last < *pivot
			if !comp(last.ptr(), pivot, constants) {
				continue
			} else {
				break
			}
		}
	}

	//first >= last, no pair need to be swapped
	alreadyPartitioned := !pdqIterLess(&first, &last)

	//keep swap pairs in the wrong place
	for pdqIterLess(&first, &last) {
		iterSwap(&first, &last, constants)
		for {
			first.plus(1)
			if comp(first.ptr(), pivot, constants) {
				continue
			} else {
				break
			}
		}
		for {
			last.plus(-1)
			if !comp(last.ptr(), pivot, constants) {
				continue
			} else {
				break
			}
		}
	}

	pivotPos := first.plusCopy(-1)
	Move(begin.ptr(), pivotPos.ptr(), constants)
	Move(pivotPos.ptr(), pivot, constants)
	return pivotPos, alreadyPartitioned
}

// partitionRightBranchless split the [begin,end).
// the ones equal to the pivot are put in the right part.
// return
//
//	the position of the pivot.
//	already split rightly
func partitionRightBranchless(
	begin *PDQIterator,
	end *PDQIterator,
	constants *PDQConstants) (PDQIterator, bool) {
	pivot := GetTmp(begin.ptr(), constants)
	first := begin.plusCopy(0)
	last := end.plusCopy(0)

	//find the one *first >= *pivot
	for {
		first.plus(1)
		//pass A[first] < A[pivot]
		if comp(first.ptr(), pivot, constants) {
			continue
		} else {
			break
		}
	}

	//begin + 1 == first. A[first] >= pivot
	//find the *last strictly < *pivot
	if pdqIterDiff(&first, begin) == 1 {
		for pdqIterLess(&first, &last) {
			last.plus(-1)
			//pass A[last] >= pivot
			if !comp(last.ptr(), pivot, constants) {
				continue
			} else {
				break
			}
		}
	} else {
		for {
			last.plus(-1)
			//pass A[last] >= pivot
			if !comp(last.ptr(), pivot, constants) {
				continue
			} else {
				break
			}
		}
	}

	//first >= last, no pair need to be swapped
	alreadyPartitioned := !pdqIterLess(&first, &last)
	{
		//swap data in wrong positions
		if !alreadyPartitioned {
			iterSwap(&first, &last, constants)
			first.plus(1)

			var offsetsLArr [block_size + cacheline_size]byte
			var offsetsRArr [block_size + cacheline_size]byte
			offsetsL := offsetsLArr[:]
			offsetsR := offsetsRArr[:]
			offsetsLBase := first.plusCopy(0)
			offsetsRBase := last.plusCopy(0)
			var numL, numR, startL, startR uint64
			numL, numR, startL, startR = 0, 0, 0, 0
			//block partitioning
			for pdqIterLess(&first, &last) {
				//decide the count of two offsets
				numUnknown := uint64(pdqIterDiff(&last, &first))
				leftSplit, rightSplit := uint64(0), uint64(0)
				if numL == 0 {
					if numR == 0 {
						leftSplit = numUnknown / 2
					} else {
						leftSplit = numUnknown
					}
				} else {
					leftSplit = 0
				}
				if numR == 0 {
					rightSplit = numUnknown - leftSplit
				} else {
					rightSplit = 0
				}

				//fill left offsets
				if leftSplit >= block_size {
					for i := 0; i < block_size; {
						for j := 0; j < 8; j++ {
							offsetsL[numL] = byte(i)
							i++
							if !comp(first.ptr(), pivot, constants) {
								numL += 1
							}
							first.plus(1)
						}
					}
				} else {
					for i := uint64(0); i < leftSplit; {
						offsetsL[numL] = byte(i)
						i++
						if !comp(first.ptr(), pivot, constants) {
							numL += 1
						}
						first.plus(1)
					}
				}

				if rightSplit >= block_size {
					for i := 0; i < block_size; {
						for j := 0; j < 8; j++ {
							i++
							offsetsR[numR] = byte(i)
							last.plus(-1)
							if comp(last.ptr(), pivot, constants) {
								numR += 1
							}
						}
					}
				} else {
					for i := uint64(0); i < rightSplit; {
						i++
						offsetsR[numR] = byte(i)
						last.plus(-1)
						if comp(last.ptr(), pivot, constants) {
							numR += 1
						}
					}
				}

				//swap data denotes by offsets
				num := min(numL, numR)
				swapOffsets(
					&offsetsLBase,
					&offsetsRBase,
					offsetsL[startL:],
					offsetsR[startR:],
					num,
					numL == numR,
					constants,
				)
				numL -= num
				numR -= num
				startL += num
				startR += num

				if numL == 0 {
					startL = 0
					offsetsLBase = first.plusCopy(0)
				}

				if numR == 0 {
					startR = 0
					offsetsRBase = last.plusCopy(0)
				}
			}

			//fil the rest
			if numL != 0 {
				offsetsL = offsetsL[startL:]
				for numL > 0 {
					numL--
					lhs := offsetsLBase.plusCopy(int(offsetsL[numL]))
					last.plus(-1)
					iterSwap(&lhs, &last, constants)
				}
				first = last.plusCopy(0)
			}
			if numR != 0 {
				offsetsR = offsetsR[startR:]
				for numR > 0 {
					numR--
					lhs := offsetsRBase.plusCopy(-int(offsetsR[numR]))
					iterSwap(&lhs, &first, constants)
					first.plus(1)
				}
				last = first.plusCopy(0)
			}
		}

	}

	pivotPos := first.plusCopy(-1)
	Move(begin.ptr(), pivotPos.ptr(), constants)
	Move(pivotPos.ptr(), pivot, constants)
	return pivotPos, alreadyPartitioned
}

func swapOffsets(
	first *PDQIterator,
	last *PDQIterator,
	offsetsL []byte,
	offsetsR []byte,
	num uint64,
	useSwaps bool,
	constants *PDQConstants) {
	if useSwaps {
		for i := uint64(0); i < num; i++ {
			lhs := first.plusCopy(int(offsetsL[i]))
			rhs := last.plusCopy(-int(offsetsR[i]))
			iterSwap(&lhs, &rhs, constants)
		}
	} else if num > 0 {
		lhs := first.plusCopy(int(offsetsL[0]))
		rhs := last.plusCopy(-int(offsetsR[0]))
		tmp := SwapOffsetsGetTmp(lhs.ptr(), constants)
		Move(lhs.ptr(), rhs.ptr(), constants)
		for i := uint64(1); i < num; i++ {
			lhs = first.plusCopy(int(offsetsL[i]))
			Move(rhs.ptr(), lhs.ptr(), constants)
			rhs = last.plusCopy(-int(offsetsR[i]))
			Move(lhs.ptr(), rhs.ptr(), constants)
		}
		Move(rhs.ptr(), tmp, constants)
	}
}

func partitionLeft(begin *PDQIterator, end *PDQIterator, constants *PDQConstants) PDQIterator {
	pivot := GetTmp(begin.ptr(), constants)
	first := begin.plusCopy(0)
	last := end.plusCopy(0)
	for {
		last.plus(-1)
		//pass A[pivot] < A[last]
		if comp(pivot, last.ptr(), constants) {
			continue
		} else {
			break
		}
	}
	//last + 1 == end. A[pivot] >= A[end-1]
	if pdqIterDiff(&last, end) == 1 {
		for pdqIterLess(&first, &last) {
			first.plus(1)
			//pass A[pivot] >= A[first]
			if !comp(pivot, first.ptr(), constants) {
				continue
			} else {
				break
			}
		}
	} else {
		for {
			first.plus(1)
			//pass A[pivot] >= A[first]
			if !comp(pivot, first.ptr(), constants) {
				continue
			} else {
				break
			}
		}
	}

	for pdqIterLess(&first, &last) {
		iterSwap(&first, &last, constants)
		for {
			last.plus(-1)
			//pass A[pivot] < A[last]
			if comp(pivot, last.ptr(), constants) {
				continue
			} else {
				break
			}
		}
		for {
			first.plus(1)
			//pass A[pivot] >= A[first]
			if !comp(pivot, first.ptr(), constants) {
				continue
			} else {
				break
			}
		}
	}

	//move pivot
	Move(begin.ptr(), last.ptr(), constants)
	Move(last.ptr(), pivot, constants)

	return last.plusCopy(0)
}

func comp(l, r unsafe.Pointer, constants *PDQConstants) bool {
	util.AssertFunc(
		l == constants._tmpBuf ||
			l == constants._swapOffsetsBuf ||
			util.PointerLess(l, constants._end))

	util.AssertFunc(
		r == constants._tmpBuf ||
			r == constants._swapOffsetsBuf ||
			util.PointerLess(r, constants._end))

	lAddr := util.PointerAdd(l, constants._compOffset)
	rAddr := util.PointerAdd(r, constants._compOffset)
	return util.PointerMemcmp(lAddr, rAddr, constants._compSize) < 0
}

func GetTmp(src unsafe.Pointer, constants *PDQConstants) unsafe.Pointer {
	util.AssertFunc(src != constants._tmpBuf &&
		src != constants._swapOffsetsBuf &&
		util.PointerLess(src, constants._end))
	util.PointerCopy(constants._tmpBuf, src, constants._entrySize)
	return constants._tmpBuf
}

func SwapOffsetsGetTmp(src unsafe.Pointer, constants *PDQConstants) unsafe.Pointer {
	util.AssertFunc(src != constants._tmpBuf &&
		src != constants._swapOffsetsBuf &&
		util.PointerLess(src, constants._end))
	util.PointerCopy(constants._swapOffsetsBuf, src, constants._entrySize)
	return constants._swapOffsetsBuf
}

func Move(dst, src unsafe.Pointer, constants *PDQConstants) {
	util.AssertFunc(
		dst == constants._tmpBuf ||
			dst == constants._swapOffsetsBuf ||
			util.PointerLess(dst, constants._end))
	util.AssertFunc(src == constants._tmpBuf ||
		src == constants._swapOffsetsBuf ||
		util.PointerLess(src, constants._end))
	util.PointerCopy(dst, src, constants._entrySize)
}

// sort A[a],A[b],A[c]
func sort3(a, b, c *PDQIterator, constants *PDQConstants) {
	sort2(a, b, constants)
	sort2(b, c, constants)
	sort2(a, b, constants)
}

func sort2(a *PDQIterator, b *PDQIterator, constants *PDQConstants) {
	if comp(b.ptr(), a.ptr(), constants) {
		iterSwap(a, b, constants)
	}
}

func iterSwap(lhs *PDQIterator, rhs *PDQIterator, constants *PDQConstants) {
	util.AssertFunc(util.PointerLess(lhs.ptr(), constants._end))
	util.AssertFunc(util.PointerLess(rhs.ptr(), constants._end))
	util.PointerCopy(constants._iterSwapBuf, lhs.ptr(), constants._entrySize)
	util.PointerCopy(lhs.ptr(), rhs.ptr(), constants._entrySize)
	util.PointerCopy(rhs.ptr(), constants._iterSwapBuf, constants._entrySize)
}

// insert sort [begin,end)
func insertSort(
	begin *PDQIterator,
	end *PDQIterator,
	constants *PDQConstants) {
	if pdqIterEqaul(begin, end) {
		return
	}

	for cur := begin.plusCopy(1); pdqIterNotEqaul(&cur, end); cur.plus(1) {
		sift := cur
		sift_1 := cur.plusCopy(-1)
		if comp(sift.ptr(), sift_1.ptr(), constants) {
			//A[sift - 1] > A[sift]
			tmp := GetTmp(sift.ptr(), constants)
			for {
				Move(sift.ptr(), sift_1.ptr(), constants)
				sift.plus(-1)

				if pdqIterNotEqaul(&sift, begin) {
					sift_1.plus(-1)
					if comp(tmp, sift_1.ptr(), constants) {
						continue
					}
				}
				break
			}
			Move(sift.ptr(), tmp, constants)
		}
	}
}

// insert sort [begin,end)
// A[begin - 1] <= anyone in [begin,end)
func unguardedInsertSort(begin *PDQIterator, end *PDQIterator, constants *PDQConstants) {
	if pdqIterEqaul(begin, end) {
		return
	}

	//plusCopy := begin.plusCopy(-1)
	//assertFunc(comp(plusCopy.ptr(), begin.ptr(), constants))

	for cur := begin.plusCopy(1); pdqIterNotEqaul(&cur, end); cur.plus(1) {
		sift := cur
		sift_1 := cur.plusCopy(-1)
		if comp(sift.ptr(), sift_1.ptr(), constants) {
			//A[sift - 1] > A[sift]
			tmp := GetTmp(sift.ptr(), constants)
			for {
				Move(sift.ptr(), sift_1.ptr(), constants)
				sift.plus(-1)

				sift_1.plus(-1)
				//FIXME:here remove the if
				//if !pdqIterLess(&sift_1, begin) {
				if comp(tmp, sift_1.ptr(), constants) {
					continue
				}
				//}
				break
			}
			Move(sift.ptr(), tmp, constants)
		}
	}
}

// InsertionSort adapted in less count of values
func InsertionSort(
	origPtr unsafe.Pointer,
	tempPtr unsafe.Pointer,
	count int,
	colOffset int,
	rowWidth int,
	totalCompWidth int,
	offset int,
	swap bool,
) {
	sourcePtr, targetPtr := origPtr, tempPtr
	if swap {
		sourcePtr, targetPtr = tempPtr, origPtr
	}

	if count > 1 {
		totalOffset := colOffset + offset
		val := util.CMalloc(rowWidth)
		defer util.CFree(val)
		compWidth := totalCompWidth - offset
		for i := 1; i < count; i++ {
			//val <= sourcePtr[i][...]
			util.PointerCopy(
				val,
				util.PointerAdd(sourcePtr, i*rowWidth),
				rowWidth)
			j := i
			//memcmp (sourcePtr[j-1][totalOffset],val[totalOffset],compWidth)
			for j > 0 &&
				util.PointerMemcmp(
					util.PointerAdd(sourcePtr, (j-1)*rowWidth+totalOffset),
					util.PointerAdd(val, totalOffset),
					compWidth,
				) > 0 {
				//memcopy (sourcePtr[j][...],sourcePtr[j-1][...],rowWidth)
				util.PointerCopy(
					util.PointerAdd(sourcePtr, j*rowWidth),
					util.PointerAdd(sourcePtr, (j-1)*rowWidth),
					rowWidth,
				)
				j--
			}
			//memcpy (sourcePtr[j][...],val,rowWidth)
			util.PointerCopy(
				util.PointerAdd(sourcePtr, j*rowWidth),
				val,
				rowWidth,
			)
		}
	}

	if swap {
		util.PointerCopy(
			targetPtr,
			sourcePtr,
			count*rowWidth,
		)
	}
}

func RadixSortLSD(
	dataPtr unsafe.Pointer,
	count int,
	colOffset int,
	rowWidth int,
	sortingSize int,
) {
	temp := util.CMalloc(rowWidth)
	defer util.CFree(temp)
	swap := false

	var counts [VALUES_PER_RADIX]uint64
	for r := 1; r <= sortingSize; r++ {
		util.Fill(counts[:], VALUES_PER_RADIX, 0)
		sourcePtr, targetPtr := dataPtr, temp
		if swap {
			sourcePtr, targetPtr = temp, dataPtr
		}
		offset := colOffset + sortingSize - r
		offsetPtr := util.PointerAdd(sourcePtr, offset)
		for i := 0; i < count; i++ {
			val := util.Load[byte](offsetPtr)
			counts[val]++
			offsetPtr = util.PointerAdd(offsetPtr, rowWidth)
		}

		maxCount := counts[0]
		for val := 1; val < VALUES_PER_RADIX; val++ {
			maxCount = max(maxCount, counts[val])
			counts[val] = counts[val] + counts[val-1]
		}
		if maxCount == uint64(count) {
			continue
		}

		rowPtr := util.PointerAdd(sourcePtr, (count-1)*rowWidth)
		for i := 0; i < count; i++ {
			val := util.Load[byte](util.PointerAdd(rowPtr, offset))
			counts[val]--
			radixOffset := counts[val]
			util.PointerCopy(
				util.PointerAdd(targetPtr, int(radixOffset)*rowWidth),
				rowPtr,
				rowWidth,
			)
			rowPtr = util.PointerAdd(rowPtr, -rowWidth)
		}
		swap = !swap
	}
	if swap {
		util.PointerCopy(
			dataPtr,
			temp,
			count*rowWidth,
		)
	}
}

func RadixSortMSD(
	origPtr unsafe.Pointer,
	tempPtr unsafe.Pointer,
	count int,
	colOffset int,
	rowWidth int,
	compWidth int,
	offset int,
	locations []uint64,
	swap bool,
) {
	sourcePtr, targetPtr := origPtr, tempPtr
	if swap {
		sourcePtr, targetPtr = tempPtr, origPtr
	}

	util.Fill[uint64](locations,
		MSD_RADIX_LOCATIONS,
		0,
	)
	counts := locations[1:]
	totalOffset := colOffset + offset
	offsetPtr := util.PointerAdd(sourcePtr, totalOffset)
	for i := 0; i < count; i++ {
		val := util.Load[byte](offsetPtr)
		counts[val]++
		offsetPtr = util.PointerAdd(offsetPtr, rowWidth)
	}

	maxCount := uint64(0)
	for radix := 0; radix < VALUES_PER_RADIX; radix++ {
		maxCount = max(maxCount, counts[radix])
		counts[radix] += locations[radix]
	}

	if maxCount != uint64(count) {
		rowPtr := sourcePtr
		for i := 0; i < count; i++ {
			val := util.Load[byte](util.PointerAdd(rowPtr, totalOffset))
			radixOffset := locations[val]
			locations[val]++
			util.PointerCopy(
				util.PointerAdd(targetPtr, int(radixOffset)*rowWidth),
				rowPtr,
				rowWidth,
			)
			rowPtr = util.PointerAdd(rowPtr, rowWidth)
		}
		swap = !swap
	}

	if offset == compWidth-1 {
		if swap {
			util.PointerCopy(
				origPtr,
				tempPtr,
				count*rowWidth,
			)
		}
		return
	}

	if maxCount == uint64(count) {
		RadixSortMSD(
			origPtr,
			tempPtr,
			count,
			colOffset,
			rowWidth,
			compWidth,
			offset+1,
			locations[MSD_RADIX_LOCATIONS:],
			swap,
		)
		return
	}

	radixCount := locations[0]
	for radix := 0; radix < VALUES_PER_RADIX; radix++ {
		loc := int(locations[radix]-radixCount) * rowWidth
		if radixCount > INSERTION_SORT_THRESHOLD {
			RadixSortMSD(
				util.PointerAdd(origPtr, loc),
				util.PointerAdd(tempPtr, loc),
				int(radixCount),
				colOffset,
				rowWidth,
				compWidth,
				offset+1,
				locations[MSD_RADIX_LOCATIONS:],
				swap,
			)
		} else if radixCount != 0 {
			InsertionSort(
				util.PointerAdd(origPtr, loc),
				util.PointerAdd(tempPtr, loc),
				int(radixCount),
				colOffset,
				rowWidth,
				compWidth,
				offset+1,
				swap,
			)
		}
		radixCount = locations[radix+1] - locations[radix]
	}
}

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

func TieIsBreakable(
	tieCol int,
	rowPtr unsafe.Pointer,
	layout *SortLayout,
) bool {
	colIdx := layout._sortingToBlobCol[tieCol]
	rowMask := util.Bitmap{
		Bits: util.PointerToSlice[byte](rowPtr, layout._blobLayout._flagWidth),
	}
	entryIdx, idxInEntry := util.GetEntryIndex(uint64(colIdx))
	if !util.RowIsValidInEntry(
		rowMask.GetEntry(entryIdx),
		idxInEntry,
	) {
		//can not create a NULL tie
		return false
	}

	rowLayout := layout._blobLayout
	if !rowLayout.GetTypes()[colIdx].GetInternalType().IsVarchar() {
		//nested type
		return true
	}

	tieColOffset := rowLayout.GetOffsets()[colIdx]
	tieString := util.Load[common.String](util.PointerAdd(rowPtr, tieColOffset))
	return tieString.Length() >= layout._prefixLengths[tieCol]
}

func CompareVal(
	lPtr, rPtr unsafe.Pointer,
	typ common.LType,
) int {
	switch typ.GetInternalType() {
	case common.VARCHAR:
		return TemplatedCompareVal[common.String](
			lPtr,
			rPtr,
			binStringEqualOp,
			binStringLessOp,
		)
	default:
		panic("usp")
	}
}

func TemplatedCompareVal[T any](
	lPtr, rPtr unsafe.Pointer,
	equalOp BinaryOp[T, T, bool],
	lessOp BinaryOp[T, T, bool],
) int {
	lVal := util.Load[T](lPtr)
	rVal := util.Load[T](rPtr)
	eRet := false
	equalOp(&lVal, &rVal, &eRet)
	if eRet {
		return 0
	}
	lRet := false
	lessOp(&lVal, &rVal, &lRet)
	if lRet {
		return -1
	}
	return 1
}

type Encoder[T any] interface {
	EncodeData(unsafe.Pointer, *T)
	TypeSize() int
}

func BSWAP16(x uint16) uint16 {
	return ((x & 0xff00) >> 8) | ((x & 0x00ff) << 8)
}

func BSWAP32(x uint32) uint32 {
	return ((x & 0xff000000) >> 24) | ((x & 0x00ff0000) >> 8) |
		((x & 0x0000ff00) << 8) | ((x & 0x000000ff) << 24)

}

func BSWAP64(x uint64) uint64 {
	return ((x & 0xff00000000000000) >> 56) | ((x & 0x00ff000000000000) >> 40) |
		((x & 0x0000ff0000000000) >> 24) | ((x & 0x000000ff00000000) >> 8) |
		((x & 0x00000000ff000000) << 8) | ((x & 0x0000000000ff0000) << 24) |
		((x & 0x000000000000ff00) << 40) | ((x & 0x00000000000000ff) << 56)

}

func FlipSign(b uint8) uint8 {
	return b ^ 128
}

type int32Encoder struct {
}

func (i int32Encoder) EncodeData(ptr unsafe.Pointer, value *int32) {
	util.Store[uint32](BSWAP32(uint32(*value)), ptr)
	util.Store[uint8](FlipSign(util.Load[uint8](ptr)), ptr)
}

func (i int32Encoder) TypeSize() int {
	return 4
}

// actually it int64
type intEncoder struct {
}

func (i intEncoder) EncodeData(ptr unsafe.Pointer, value *int) {
	util.Store[uint64](BSWAP64(uint64(*value)), ptr)
	util.Store[uint8](FlipSign(util.Load[uint8](ptr)), ptr)
}

func (i intEncoder) TypeSize() int {
	return int(unsafe.Sizeof(int(0)))
}

type decimalEncoder struct {
}

func (decimalEncoder) EncodeData(ptr unsafe.Pointer, dec *common.Decimal) {
	whole, frac, ok := dec.Int64(2)
	util.AssertFunc(ok)
	encodeInt64(ptr, whole)
	encodeInt64(util.PointerAdd(ptr, common.Int64Size), frac)
}
func (decimalEncoder) TypeSize() int {
	return common.DecimalSize
}

type dateEncoder struct{}

func (dateEncoder) EncodeData(ptr unsafe.Pointer, d *common.Date) {
	encodeInt32(ptr, d.Year)
	encodeInt32(util.PointerAdd(ptr, common.Int32Size), d.Month)
	encodeInt32(util.PointerAdd(ptr, 2*common.Int32Size), d.Day)
}

func (dateEncoder) TypeSize() int {
	return common.DateSize
}

type hugeEncoder struct{}

func (hugeEncoder) EncodeData(ptr unsafe.Pointer, d *common.Hugeint) {
	encodeInt64(ptr, d.Upper)
	encodeUint64(util.PointerAdd(ptr, common.Int64Size), d.Lower)
}

func (hugeEncoder) TypeSize() int {
	return common.Int128Size
}

func encodeInt32(ptr unsafe.Pointer, value int32) {
	util.Store[uint32](BSWAP32(uint32(value)), ptr)
	util.Store[uint8](FlipSign(util.Load[uint8](ptr)), ptr)
}

//func encodeUint32(ptr unsafe.Pointer, value uint32) {
//	store[uint32](BSWAP32(value), ptr)
//}

func encodeInt64(ptr unsafe.Pointer, value int64) {
	util.Store[uint64](BSWAP64(uint64(value)), ptr)
	util.Store[uint8](FlipSign(util.Load[uint8](ptr)), ptr)
}

func encodeUint64(ptr unsafe.Pointer, value uint64) {
	util.Store[uint64](BSWAP64(value), ptr)
}
