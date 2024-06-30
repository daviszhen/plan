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
	_scanChunk       *Chunk
	_positionInChunk int
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

type ColumnDataScanState struct {
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
	ptr := cMalloc(src.len())
	pointerCopy(ptr, src.data(), src.len())
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
