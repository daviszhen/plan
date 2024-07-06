package main

import (
	"unsafe"
)

type CrossProduct struct {
	_dataCollect *ColumnDataCollection
}

type CrossProductExec struct {
	_rhs             *ColumnDataCollection
	_scanState       *ColumnDataScanState
	_scanChunk       *Chunk //constant chunk (scanning chunk is the input chunk)
	_positionInChunk int    //position in the scanning chunk different from constant chunk
	_init            bool
	_finish          bool
	_scanInputChunk  bool
}

func NewCrossProductExec(rhs *ColumnDataCollection) *CrossProductExec {
	ret := &CrossProductExec{
		_rhs: rhs,
	}
	ret._rhs.initScanChunk(ret._scanChunk)
	return ret
}

func (cross *CrossProductExec) Reset() {
	cross._init = true
	cross._finish = false
	cross._scanInputChunk = true
	cross._rhs.initScan(cross._scanState)
	cross._positionInChunk = 0
	cross._scanChunk.reset()
}

func (cross *CrossProductExec) NextValue(input, output *Chunk) bool {
	if !cross._init {
		cross.Reset()
	}
	cross._positionInChunk++
	chunkSize := 0
	if cross._scanInputChunk {
		chunkSize = input.card()
	} else {
		chunkSize = cross._scanChunk.card()
	}
	if cross._positionInChunk < chunkSize {
		return true
	}
	//pick next chunk from the collection
	cross._rhs.Scan(cross._scanState, cross._scanChunk)
	cross._positionInChunk = 0
	if cross._scanChunk.card() == 0 {
		return false
	}
	return true
}

func (cross *CrossProductExec) Execute(input, output *Chunk) OperatorResult {
	if cross._rhs.Count() == 0 {
		// no RHS, empty result
		return Done
	}

	if !cross.NextValue(input, output) {
		cross._init = false
		//RHS is read over.
		//switch to the next Chunk on the LHS and reset the RHS
		return NeedMoreInput
	}

	var constChunk *Chunk
	if cross._scanInputChunk {
		constChunk = cross._scanChunk
	} else {
		constChunk = input
	}
	colCnt := constChunk.columnCount()
	colOffset := 0
	if cross._scanInputChunk {
		colOffset = input.columnCount()
	} else {
		colOffset = 0
	}

	output.setCard(constChunk.card())

	//refer constant vector
	for i := 0; i < colCnt; i++ {
		output._data[colOffset+i].reference(constChunk._data[i])
	}

	//scanning chunk. refer a single value
	var scanChunk *Chunk
	if cross._scanInputChunk {
		scanChunk = input
	} else {
		scanChunk = cross._scanChunk
	}
	colCnt = scanChunk.columnCount()
	if cross._scanInputChunk {
		colOffset = 0
	} else {
		colOffset = input.columnCount()
	}

	for i := 0; i < colCnt; i++ {
		referenceInPhyFormatConst(
			output._data[colOffset+i],
			scanChunk._data[i],
			cross._positionInChunk,
			scanChunk.card(),
		)
	}
	return haveMoreOutput
}

type ColumnDataScanState struct {
	chunkIdx      int
	currentRowIdx int
	nextRowIdx    int
	columnIds     []int
}

type ColumnDataMetaData struct {
	_dst    *Chunk
	_vecIdx int
}

type ColumnDataCollection struct {
	_types  []LType
	_count  int
	_chunks []*Chunk
}

func NewColumnDataCollection(typs []LType) *ColumnDataCollection {
	return &ColumnDataCollection{
		_types: typs,
	}
}

func (cdc *ColumnDataCollection) Append(input *Chunk) {
	vecData := make([]UnifiedFormat, len(cdc._types))
	for i := 0; i < len(cdc._types); i++ {
		input._data[i].toUnifiedFormat(input.card(), &vecData[i])
	}

	remaining := input.card()
	for remaining > 0 {
		chunkData := back(cdc._chunks)
		appendAmount := min(remaining, defaultVectorSize-chunkData.card())
		if appendAmount > 0 {
			//enough space
			offset := input.card() - remaining
			//copy
			for i := 0; i < len(cdc._types); i++ {
				metaData := ColumnDataMetaData{
					_dst:    chunkData,
					_vecIdx: i,
				}
				ColumnDataCopySwitch(
					&metaData,
					&vecData[i],
					input._data[i],
					offset,
					appendAmount,
				)
			}
			chunkData._count += appendAmount
		}
		remaining -= appendAmount
		if remaining > 0 {
			newChunk := &Chunk{}
			newChunk.init(cdc._types, defaultVectorSize)
			cdc._chunks = append(cdc._chunks, newChunk)
		}
	}
	cdc._count += input.card()

}

func (cdc *ColumnDataCollection) initScanChunk(chunk *Chunk) {
	chunk.init(cdc._types, defaultVectorSize)
}

func (cdc *ColumnDataCollection) initScan(state *ColumnDataScanState) {
	colIds := make([]int, len(cdc._types))
	for i := 0; i < len(cdc._types); i++ {
		colIds[i] = i
	}
	cdc.initScan2(state, colIds)
}

func (cdc *ColumnDataCollection) initScan2(
	state *ColumnDataScanState,
	colIds []int,
) {
	state.chunkIdx = 0
	state.currentRowIdx = 0
	state.nextRowIdx = 0
	state.columnIds = colIds
}

func (cdc *ColumnDataCollection) Scan(
	state *ColumnDataScanState,
	output *Chunk) bool {
	output.reset()

	var chunkIdx int
	var rowIdx int
	if !cdc.NextScanIndex(state, &chunkIdx, &rowIdx) {
		return false
	}

	srcChunk := cdc._chunks[chunkIdx]

	//read chunk
	for i := 0; i < len(state.columnIds); i++ {
		vecIdx := state.columnIds[i]
		cdc.ReadVector(
			state,
			srcChunk._data[vecIdx],
			output._data[vecIdx],
			srcChunk.card(),
		)
	}
	output.setCard(srcChunk.card())
	return true
}

func (cdc *ColumnDataCollection) ReadVector(
	state *ColumnDataScanState,
	srcVec *Vector,
	dstVec *Vector,
	count int,
) int {
	dstVecType := dstVec.typ()
	dstInterType := dstVecType.getInternalType()
	dstTypeSize := dstInterType.size()
	if count == 0 {
		return 0
	}
	srcPtr := bytesSliceToPointer(srcVec._data)
	dstPtr := bytesSliceToPointer(dstVec._data)
	dstBitmap := getMaskInPhyFormatFlat(dstVec)
	pointerCopy(dstPtr, srcPtr, dstTypeSize*count)
	dstBitmap.copyFrom(srcVec._mask, count)

	if dstInterType == VARCHAR {
		panic("copy vector")
	}

	return count
}

func (cdc *ColumnDataCollection) NextScanIndex(
	state *ColumnDataScanState,
	chunkIdx *int,
	rowIdx *int,
) bool {
	state.currentRowIdx = state.nextRowIdx
	*rowIdx = state.currentRowIdx
	if state.chunkIdx >= len(cdc._chunks) {
		return false
	}

	state.nextRowIdx += cdc._chunks[state.chunkIdx].card()
	state.chunkIdx++
	*chunkIdx = state.chunkIdx
	return true
}

func (cdc *ColumnDataCollection) Count() int {
	return cdc._count
}

type BaseValueCopy[T any] interface {
	Assign(metaData *ColumnDataMetaData, dst, src unsafe.Pointer, dstIdx, srcIdx int)
	Operation(dst, src *T)
}

type int32ValueCopy struct {
}

func (copy *int32ValueCopy) Assign(
	metaData *ColumnDataMetaData,
	dst, src unsafe.Pointer,
	dstIdx, srcIdx int) {
	dPtr := pointerAdd(dst, dstIdx*int32Size)
	sPtr := pointerAdd(src, srcIdx*int32Size)
	copy.Operation((*int32)(dPtr), (*int32)(sPtr))
}

func (copy *int32ValueCopy) Operation(dst, src *int32) {
	*dst = *src
}

type decimalValueCopy struct {
}

func (copy *decimalValueCopy) Assign(
	metaData *ColumnDataMetaData,
	dst, src unsafe.Pointer,
	dstIdx, srcIdx int) {
	dPtr := pointerAdd(dst, dstIdx*decimalSize)
	sPtr := pointerAdd(src, srcIdx*decimalSize)
	copy.Operation((*Decimal)(dPtr), (*Decimal)(sPtr))
}

func (copy *decimalValueCopy) Operation(dst, src *Decimal) {
	*dst = *src
}

type varcharValueCopy struct {
}

func (copy *varcharValueCopy) Assign(
	metaData *ColumnDataMetaData,
	dst, src unsafe.Pointer,
	dstIdx, srcIdx int) {
	dPtr := pointerAdd(dst, dstIdx*varcharSize)
	sPtr := pointerAdd(src, srcIdx*varcharSize)
	copy.Operation((*String)(dPtr), (*String)(sPtr))
}

func (copy *varcharValueCopy) Operation(dst, src *String) {
	if src.len() == 0 {
		dst._len = 0
		dst._data = unsafe.Pointer(uintptr(0))
		return
	}
	dst._data = cMalloc(src.len())
	pointerCopy(dst.data(), src.data(), src.len())
	dst._len = src.len()
}

func ColumnDataCopySwitch(
	metaData *ColumnDataMetaData,
	srcData *UnifiedFormat,
	src *Vector,
	offset int,
	count int,
) {
	switch src.typ().getInternalType() {
	case INT32:
		TemplatedColumnDataCopy[int32](
			metaData,
			srcData,
			src,
			offset,
			count,
			&int32ValueCopy{},
		)
	case DECIMAL:
		TemplatedColumnDataCopy[Decimal](
			metaData,
			srcData,
			src,
			offset,
			count,
			&decimalValueCopy{},
		)
	case VARCHAR:
		TemplatedColumnDataCopy[String](
			metaData,
			srcData,
			src,
			offset,
			count,
			&varcharValueCopy{},
		)
	default:
		panic("usp")
	}
}

func TemplatedColumnDataCopy[T any](
	metaData *ColumnDataMetaData,
	srcData *UnifiedFormat,
	src *Vector,
	offset int,
	count int,
	cp BaseValueCopy[T],
) {
	remaining := count
	for remaining > 0 {
		vec := metaData._dst._data[metaData._vecIdx]
		appendCount := min(defaultVectorSize-metaData._dst.card(), remaining)
		if metaData._dst.card() == 0 {
			vec._mask.setAllInvalid(defaultVectorSize)
		}
		for i := 0; i < appendCount; i++ {
			srcIdx := srcData._sel.getIndex(offset + i)
			if srcData._mask.rowIsValid(uint64(srcIdx)) {
				cp.Assign(metaData,
					bytesSliceToPointer(vec._data),
					bytesSliceToPointer(srcData._data),
					metaData._dst.card()+i,
					srcIdx,
				)
			} else {
				vec._mask.setInvalid(uint64(metaData._dst.card() + i))
			}
		}
		metaData._dst._count += appendCount
		offset += appendCount
		remaining -= appendCount
		if remaining > 0 {
			panic("usp")
		}
	}
}
