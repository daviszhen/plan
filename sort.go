package main

import (
	"bytes"
	"unsafe"
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
	_logicalTypes     []LType
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

	blobLayoutTypes := make([]LType, 0)
	for i := 0; i < ret._columnCount; i++ {
		order := orders[i]

		if order.Desc {
			ret._orderTypes = append(ret._orderTypes, OT_DESC)
		} else {
			ret._orderTypes = append(ret._orderTypes, OT_ASC)
		}

		ret._orderByNullTypes = append(ret._orderByNullTypes, OBNT_NULLS_FIRST)
		ret._logicalTypes = append(ret._logicalTypes, order.DataTyp.LTyp)

		interTyp := order.DataTyp.LTyp.getInternalType()
		ret._constantSize = append(ret._constantSize, interTyp.isConstant())

		ret._hasNull = append(ret._hasNull, true)

		colSize := 0
		if ret._hasNull[len(ret._hasNull)-1] {
			//?
			colSize = 1
		}

		ret._prefixLengths = append(ret._prefixLengths, 0)
		if !interTyp.isConstant() && interTyp != VARCHAR {
			panic("usp")
		} else if interTyp == VARCHAR {
			sizeBefore := colSize
			colSize = 12
			ret._prefixLengths[len(ret._prefixLengths)-1] = colSize - sizeBefore
		} else {
			colSize += interTyp.size()
		}

		ret._comparisonSize += colSize
		ret._columnSizes = append(ret._columnSizes, colSize)
	}
	ret._entrySize = ret._comparisonSize + int32Size

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
	_types             []LType
	_aggregates        []*AggrObject
	_flagWidth         int
	_dataWidth         int
	_aggrWidth         int
	_rowWidth          int
	_offsets           []int
	_allConstant       bool
	_heapPointerOffset int
}

func NewRowLayout(types []LType, aggrObjs []*AggrObject) *RowLayout {
	ret := &RowLayout{
		_types:       copyLTypes(types...),
		_allConstant: true,
	}

	ret._flagWidth = entryCount(len(types))
	ret._rowWidth = ret._flagWidth

	for _, lType := range types {
		ret._allConstant = ret._allConstant &&
			lType.getInternalType().isConstant()
	}

	//swizzling
	if !ret._allConstant {
		ret._heapPointerOffset = ret._rowWidth
		ret._rowWidth += int64Size
	}

	for _, lType := range types {
		ret._offsets = append(ret._offsets, ret._rowWidth)
		interTyp := lType.getInternalType()
		if interTyp.isConstant() || interTyp == VARCHAR {
			ret._rowWidth += interTyp.size()
		} else {
			ret._rowWidth += int64Size
		}
	}

	ret._dataWidth = ret._rowWidth - ret._flagWidth
	ret._aggregates = aggrObjs
	for _, obj := range aggrObjs {
		ret._offsets = append(ret._offsets, ret._rowWidth)
		ret._rowWidth += obj._payloadSize
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

func (lay *RowLayout) GetTypes() []LType {
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

func (block *RowDataBlock) Reset() {
	cFree(block._ptr)
	block._ptr = unsafe.Pointer(nil)
	block._count = 0
}

func NewRowDataBlock(capacity int, entrySize int) *RowDataBlock {
	ret := &RowDataBlock{
		_capacity:  capacity,
		_entrySize: entrySize,
	}
	sz := max(BLOCK_SIZE, capacity*entrySize)
	ret._ptr = cMalloc(sz)
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
	sel *SelectVector) {
	appendEntries := make([]BlockAppendEntry, 0)
	remaining := addedCnt
	{
		//to last block
		cdc._count += remaining
		if len(cdc._blocks) != 0 {
			lastBlock := cdc._blocks[len(cdc._blocks)-1]
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
			assertFunc(newBlock._count > 0)
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
				entry._basePtr = pointerAdd(entry._basePtr, entrySizes[aidx])
			}
		} else {
			for ; aidx < next; aidx++ {
				idx := sel.getIndex(aidx)
				keyLocs[idx] = entry._basePtr
				entry._basePtr = pointerAdd(entry._basePtr, cdc._entrySize)
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
		assertFunc(cdc._entrySize == 1)
		dataPtr = pointerAdd(block._ptr, block._byteOffset)
		for i := 0; i < remaining; i++ {
			if block._byteOffset+entrySizes[i] > block._capacity {
				if block._count == 0 &&
					appendCnt == 0 &&
					entrySizes[i] > block._capacity {
					block._capacity = entrySizes[i]
					block._ptr = cRealloc(block._ptr, block._capacity)
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
		dataPtr = pointerAdd(block._ptr, block._count*block._entrySize)
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
	return nb
}

func (cdc *RowDataCollection) Reset() {
	for _, block := range cdc._blocks {
		block.Reset()
	}
	cdc._blocks = nil
	cdc._count = 0
}

type LocalSort struct {
	_sortLayout       *SortLayout
	_payloadLayout    *RowLayout
	_radixSortingData *RowDataCollection
	_blobSortingData  *RowDataCollection
	_blobSortingHeap  *RowDataCollection
	_payloadData      *RowDataCollection
	_payloadHeap      *RowDataCollection
	_sortedBlocks     []*SortedBlock
	_addresses        *Vector
	_sel              *SelectVector
}

func NewLocalSort(slayout *SortLayout, playout *RowLayout) *LocalSort {
	ret := &LocalSort{
		_sortLayout:    slayout,
		_payloadLayout: playout,
		_addresses:     NewFlatVector(pointerType(), defaultVectorSize),
		_sel:           incrSelectVectorInPhyFormatFlat(),
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

func (ls *LocalSort) SinkChunk(sort, payload *Chunk) {
	assertFunc(sort.card() == payload.card())
	dataPtrs := getSliceInPhyFormatFlat[unsafe.Pointer](ls._addresses)
	ls._radixSortingData.Build(sort.card(), dataPtrs, nil, incrSelectVectorInPhyFormatFlat())
	//scatter
	for sortCol := 0; sortCol < sort.columnCount(); sortCol++ {
		hasNull := ls._sortLayout._hasNull[sortCol]
		nullsFirst := ls._sortLayout._orderByNullTypes[sortCol] == OBNT_NULLS_FIRST
		desc := ls._sortLayout._orderTypes[sortCol] == OT_DESC
		RadixScatter(
			sort._data[sortCol],
			sort.card(),
			ls._sel,
			sort.card(),
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
		blobChunk := &Chunk{}
		blobChunk.setCard(sort.card())
		blobChunk.setCap(defaultVectorSize)
		for i := 0; i < sort.columnCount(); i++ {
			if !ls._sortLayout._constantSize[i] {
				blobChunk._data = append(blobChunk._data, sort._data[i])
			}
		}

		ls._blobSortingData.Build(blobChunk.card(), dataPtrs, nil, incrSelectVectorInPhyFormatFlat())
		blobData := blobChunk.ToUnifiedFormat()
		Scatter(
			blobChunk,
			blobData,
			ls._sortLayout._blobLayout,
			ls._addresses,
			ls._blobSortingHeap,
			ls._sel,
			blobChunk.card(),
		)
	}
	ls._payloadData.Build(payload.card(), dataPtrs, nil, incrSelectVectorInPhyFormatFlat())
	inputData := payload.ToUnifiedFormat()
	Scatter(
		payload,
		inputData,
		ls._payloadLayout,
		ls._addresses,
		ls._payloadHeap,
		ls._sel,
		payload.card(),
	)
}

func (ls *LocalSort) Sort(reorderHeap bool) {
	assertFunc(ls._radixSortingData._count == ls._payloadData._count && reorderHeap)
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
	//TODO

}

func (ls *LocalSort) SortInMemory() {
	lastSBk := ls._sortedBlocks[len(ls._sortedBlocks)-1]
	lastBlock := lastSBk._radixSortingData[len(lastSBk._radixSortingData)-1]
	count := lastBlock._count
	dataPtr := lastBlock._ptr
	idxPtr := pointerAdd(dataPtr, ls._sortLayout._comparisonSize)
	for i := 0; i < count; i++ {
		store[uint32](uint32(i), idxPtr)
		idxPtr = pointerAdd(dataPtr, ls._sortLayout._entrySize)
	}

	//radix sort
	sortingSize := 0
	colOffset := 0
	var ties []bool
	containsString := false
	for i := 0; i < ls._sortLayout._columnCount; i++ {
		sortingSize += ls._sortLayout._columnSizes[i]
		containsString = containsString ||
			ls._sortLayout._logicalTypes[i].getInternalType().isVarchar()
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
			fill[bool](ties, count-1, true)
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
		newSlice := pointerToSlice[uint8](newBlockPtr, cLen)
		blocSlice := pointerToSlice[uint8](block._ptr, cLen)
		copy(newSlice, blocSlice)
	}
	rowData.Reset()
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
		panic("usp")

	} else if count <= INSERTION_SORT_THRESHOLD {
		panic("usp")
	} else if sortingSize <= MSD_RADIX_SORT_SIZE_THRESHOLD {
		panic("usp")
	} else {
		panic("usp")
	}
}

func SubSortTiedTuples(ptr unsafe.Pointer, count int, offset int, size int, ties []bool, layout *SortLayout, containsString bool) {
	panic("usp")
}

func ComputeTies(ptr unsafe.Pointer, count int, offset int, size int, ties []bool, layout *SortLayout) {
	panic("usp")
}

func SortTiedBlobs(bk *SortedBlock, ties []bool, ptr unsafe.Pointer, count int, i int, layout *SortLayout) {
	panic("usp")
}

func AnyTies(ties []bool, count int) bool {
	panic("usp")
}

func RadixScatter(
	v *Vector,
	vcount int,
	sel *SelectVector,
	serCount int,
	keyLocs []unsafe.Pointer,
	desc bool,
	hasNull bool,
	nullsFirst bool,
	prefixLen int,
	width int,
	offset int,
) {
	var vdata UnifiedFormat
	v.toUnifiedFormat(vcount, &vdata)
	switch v.typ().getInternalType() {
	case BOOL:
	case INT32:
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
	default:
		panic("usp")
	}
}

func TemplatedRadixScatter[T any](
	vdata *UnifiedFormat,
	sel *SelectVector,
	addCount int,
	keyLocs []unsafe.Pointer,
	desc bool,
	hasNull bool,
	nullsFirst bool,
	offset int,
	enc Encoder[T],
) {
	srcSlice := getSliceInPhyFormatUnifiedFormat[T](vdata)
	if hasNull {
		mask := vdata._mask
		valid := byte(0)
		if nullsFirst {
			valid = 1
		}
		invalid := 1 - valid
		for i := 0; i < addCount; i++ {
			idx := sel.getIndex(i)
			srcIdx := vdata._sel.getIndex(idx) + offset
			if mask.rowIsValid(uint64(srcIdx)) {
				//first byte
				store[byte](valid, keyLocs[i])
				enc.EncodeData(pointerAdd(keyLocs[i], 1), &srcSlice[srcIdx])
				//desc , invert bits
				if desc {
					for s := 1; s < enc.TypeSize()+1; s++ {
						invertBits(keyLocs[i], s)
					}
				}
			} else {
				store[byte](invalid, keyLocs[i])
				memset(pointerAdd(keyLocs[i], 1), 0, enc.TypeSize())
			}
			keyLocs[i] = pointerAdd(keyLocs[i], 1+enc.TypeSize())
		}
	} else {
		for i := 0; i < addCount; i++ {
			idx := sel.getIndex(i)
			srcIdx := vdata._sel.getIndex(idx) + offset
			enc.EncodeData(keyLocs[i], &srcSlice[srcIdx])
			if desc {
				for s := 0; s < enc.TypeSize(); s++ {
					invertBits(keyLocs[i], s)
				}
			}
			keyLocs[i] = pointerAdd(keyLocs[i], enc.TypeSize())
		}
	}
}

func Scatter(
	columns *Chunk,
	colData []*UnifiedFormat,
	layout *RowLayout,
	rows *Vector,
	stringHeap *RowDataCollection,
	sel *SelectVector,
	count int,
) {
	if count == 0 {
		return
	}

	ptrs := getSliceInPhyFormatFlat[unsafe.Pointer](rows)
	for i := 0; i < count; i++ {
		ridx := sel.getIndex(i)
		rowPtr := ptrs[ridx]
		bSlice := pointerToSlice[uint8](rowPtr, layout.CoumnCount())
		tempMask := Bitmap{_bits: bSlice}
		tempMask.setAllValid(layout.CoumnCount())
	}

	//vcount := columns.card()
	offsets := layout.GetOffsets()
	types := layout.GetTypes()

	//compute the entry size of the variable size columns
	dataLocs := make([]unsafe.Pointer, defaultVectorSize)
	if !layout.AllConstant() {
		entrySizes := make([]int, defaultVectorSize)
		fill(entrySizes, count, 4)
		for colNo := 0; colNo < len(types); colNo++ {
			if types[colNo].getInternalType().isConstant() {
				continue
			}
			//vec := columns._data[colNo]
			col := colData[colNo]
			switch types[colNo].getInternalType() {
			case VARCHAR:
				ComputeStringEntrySizes(col, entrySizes, sel, count, 0)
			default:
				panic("usp internal type")
			}
		}
		stringHeap.Build(count, dataLocs, entrySizes, incrSelectVectorInPhyFormatFlat())

		heapPointerOffset := layout.GetHeapOffset()
		for i := 0; i < count; i++ {
			rowIdx := sel.getIndex(i)
			rowPtr := ptrs[rowIdx]
			store[unsafe.Pointer](dataLocs[i], pointerAdd(rowPtr, heapPointerOffset))
			store[uint32](uint32(entrySizes[i]), dataLocs[i])
			dataLocs[i] = pointerAdd(dataLocs[i], 4)
		}
	}

	for colNo := 0; colNo < len(types); colNo++ {
		//vec := columns._data[colNo]
		col := colData[colNo]
		colOffset := offsets[colNo]
		switch types[colNo].getInternalType() {
		case INT32:
			TemplatedScatter[int32](
				col,
				rows,
				sel,
				count,
				colOffset,
				colNo,
				layout,
				int32ScatterOp{},
			)
		case VARCHAR:
			TemplatedScatter[String](
				col,
				rows,
				sel,
				count,
				colOffset,
				colNo,
				layout,
				stringScatterOp{},
			)
		default:
			panic("usp")
		}
	}
}

func ComputeStringEntrySizes(
	col *UnifiedFormat,
	entrySizes []int,
	sel *SelectVector,
	count int,
	offset int,
) {
	data := getSliceInPhyFormatUnifiedFormat[String](col)
	for i := 0; i < count; i++ {
		idx := sel.getIndex(i)
		colIdx := col._sel.getIndex(idx) + offset
		str := data[colIdx]
		if col._mask.rowIsValid(uint64(colIdx)) {
			entrySizes[i] += str.len()
		}
	}
}

func TemplatedScatter[T any](
	col *UnifiedFormat,
	rows *Vector,
	sel *SelectVector,
	count int,
	colOffset int,
	colNo int,
	layout *RowLayout,
	sop ScatterOp[T],
) {
	data := getSliceInPhyFormatUnifiedFormat[T](col)
	ptrs := getSliceInPhyFormatFlat[unsafe.Pointer](rows)

	if !col._mask.AllValid() {
		for i := 0; i < count; i++ {
			idx := sel.getIndex(i)
			colIdx := col._sel.getIndex(idx)
			rowPtr := ptrs[idx]

			isNull := !col._mask.rowIsValid(uint64(colIdx))
			var val T
			if isNull {
				val = sop.nullValue()
			} else {
				val = data[colIdx]
			}

			store[T](val, pointerAdd(rowPtr, colOffset))
			if isNull {
				mask := Bitmap{
					_bits: pointerToSlice[uint8](ptrs[idx], layout.rowWidth()),
				}
				mask.setInvalidUnsafe(uint64(colNo))
			}
		}
	} else {
		for i := 0; i < count; i++ {
			idx := sel.getIndex(i)
			colIdx := col._sel.getIndex(idx)
			rowPtr := ptrs[idx]
			store[T](data[colIdx], pointerAdd(rowPtr, colOffset))
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
	_end            unsafe.Pointer
	_compOffset     int
	_compSize       int
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
	iter._ptr = pointerAdd(iter._ptr, n*iter._entrySize)
}

func (iter *PDQIterator) plusCopy(n int) PDQIterator {
	return PDQIterator{
		_ptr:       pointerAdd(iter._ptr, n*iter._entrySize),
		_entrySize: iter._entrySize,
	}
}

func pdqIterDiff(lhs, rhs *PDQIterator) int {
	tlen := pointerSub(lhs.ptr(), rhs.ptr())
	assertFunc(tlen%int64(lhs._entrySize) == 0)
	assertFunc(tlen >= 0)
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
				unguardedInsertSort(begin, end, constants)
			}
			return
		}

		s2 := size / 2
		if size > ninther_threshold {
			panic("usp")
		} else {
			panic("usp")
		}
	}
}

func comp(l, r unsafe.Pointer, constants *PDQConstants) bool {
	assertFunc(
		l == constants._tmpBuf ||
			l == constants._swapOffsetsBuf ||
			pointerSub(l, constants._end) < 0)

	assertFunc(
		r == constants._tmpBuf ||
			r == constants._swapOffsetsBuf ||
			pointerSub(r, constants._end) < 0)

	lAddr := pointerAdd(l, constants._compOffset)
	rAddr := pointerAdd(r, constants._compOffset)
	lSlice := pointerToSlice[byte](lAddr, constants._compSize)
	rSlice := pointerToSlice[byte](rAddr, constants._compSize)
	return bytes.Compare(lSlice, rSlice) < 0
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

		}
	}
}

func unguardedInsertSort(begin *PDQIterator, end *PDQIterator, constants *PDQConstants) {

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
	store[uint32](BSWAP32(uint32(*value)), ptr)
	store[uint8](FlipSign(load[uint8](ptr)), ptr)
}

func (i int32Encoder) TypeSize() int {
	return 4
}
