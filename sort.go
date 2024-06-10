package main

import (
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

type RowDataBlock struct {
	_ptr       unsafe.Pointer
	_capacity  int
	_entrySize int
	_count     int
	//write offset for var len entry
	_byteOffset int
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

type SortedBlock struct {
	_radixSortingData []*RowDataBlock
	_blobSortingData  []*SortedData
	_payloadData      []*SortedData
	_sortLayout       *SortLayout
	_payloadLayout    *RowLayout
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
	//todo:
	if !ls._sortLayout._allConstant {
		//todo:
	}
	//todo:
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
