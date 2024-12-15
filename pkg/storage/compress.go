package storage

import (
	"unsafe"

	"github.com/daviszhen/plan/pkg/chunk"
	"github.com/daviszhen/plan/pkg/common"
	"github.com/daviszhen/plan/pkg/util"
)

type CompressType int

const (
	CompressTypeAuto CompressType = iota
	CompressTypeUncompressed
)

type CompressAppendState struct {
	_handle *BufferHandle
}

type CompressInitAppend func(
	segment *ColumnSegment) *CompressAppendState

type CompressAppend func(
	state *CompressAppendState,
	segment *ColumnSegment,
	data *chunk.UnifiedFormat,
	offset IdxType,
	count IdxType,
) IdxType

type CompressFinalizeAppend func(
	segment *ColumnSegment,
	pTyp common.PhyType) IdxType

type CompressRevertAppend func(
	segment *ColumnSegment,
	start IdxType)

type CompressFunction struct {
	_typ            CompressType
	_dataType       common.PhyType
	_initAppend     CompressInitAppend
	_append         CompressAppend
	_finalizeAppend CompressFinalizeAppend
	_revertAppend   CompressRevertAppend
}

func GetUncompressedCompressFunction(
	typ common.PhyType,
) *CompressFunction {
	var cfun *CompressFunction
	switch typ {
	case common.INT32, common.BIT:
		cfun = &CompressFunction{
			_typ:            CompressTypeUncompressed,
			_dataType:       typ,
			_initAppend:     FixedSizeInitAppend,
			_append:         getFixedSizeAppend(typ),
			_finalizeAppend: getFixedSizeFinalizeAppend(typ),
		}
	}
	if cfun == nil {
		panic("usp")
	}

	return cfun
}

func getFixedSizeAppend(typ common.PhyType) CompressAppend {
	switch typ {
	case common.INT32, common.BIT:
		return FixedSizeAppend
	default:
		panic("usp")
	}
	return nil
}

func getFixedSizeFinalizeAppend(typ common.PhyType) CompressFinalizeAppend {
	switch typ {
	case common.INT32, common.BIT:
		return FixedSizeFinalizeAppend
	default:
		panic("usp")
	}
	return nil
}

func FixedSizeInitAppend(segment *ColumnSegment) *CompressAppendState {
	handle := GBufferMgr.Pin(segment._block)
	return &CompressAppendState{
		_handle: handle,
	}
}

type Appender func(
	target unsafe.Pointer,
	targetOffset IdxType,
	data *chunk.UnifiedFormat,
	offset IdxType,
	count IdxType,
)

func getAppender(
	pTyp common.PhyType,
) Appender {
	switch pTyp {
	case common.BIT:
		temp := func(
			target unsafe.Pointer,
			targetOffset IdxType,
			data *chunk.UnifiedFormat,
			offset IdxType,
			count IdxType) {
			StandardFixedSizeAppend[bool](
				target,
				targetOffset,
				data,
				offset,
				count,
				chunk.BoolScatterOp{},
			)
		}
		return temp
	case common.INT32:
		temp := func(
			target unsafe.Pointer,
			targetOffset IdxType,
			data *chunk.UnifiedFormat,
			offset IdxType,
			count IdxType) {
			StandardFixedSizeAppend[int32](
				target,
				targetOffset,
				data,
				offset,
				count,
				chunk.Int32ScatterOp{},
			)
		}
		return temp
	default:
		panic("usp")
	}
	return nil
}

func StandardFixedSizeAppend[T any](
	target unsafe.Pointer,
	targetOffset IdxType,
	data *chunk.UnifiedFormat,
	offset IdxType,
	count IdxType,
	sop chunk.ScatterOp[T],
) {
	srcDataSlice := chunk.GetSliceInPhyFormatUnifiedFormat[T](data)
	dstDataSlice := util.PointerToSlice[T](target, int(count))
	if !data.Mask.AllValid() {
		for i := IdxType(0); i < count; i++ {
			srcIdx := data.Sel.GetIndex(int(offset + i))
			dstIdx := targetOffset + i
			isNull := !data.Mask.RowIsValid(uint64(srcIdx))
			if !isNull {
				dstDataSlice[dstIdx] = srcDataSlice[srcIdx]
			} else {
				dstDataSlice[dstIdx] = sop.NullValue()
			}
		}
	} else {
		for i := IdxType(0); i < count; i++ {
			srcIdx := data.Sel.GetIndex(int(offset + i))
			dstIdx := targetOffset + i
			dstDataSlice[dstIdx] = srcDataSlice[srcIdx]
		}
	}
}

func FixedSizeAppend(
	state *CompressAppendState,
	segment *ColumnSegment,
	data *chunk.UnifiedFormat,
	offset IdxType,
	count IdxType,
) IdxType {
	pTyp := segment._type.GetInternalType()
	target := state._handle.Ptr()
	target = util.PointerAdd(
		target,
		int(IdxType(segment._count.Load())*IdxType(pTyp.Size())))
	maxTupleCount := segment._segmentSize / IdxType(pTyp.Size())
	copyCount := min(count, maxTupleCount-IdxType(segment._count.Load()))
	appender := getAppender(pTyp)
	appender(
		target,
		IdxType(0),
		data,
		offset,
		copyCount,
	)
	segment._count.Add(uint64(copyCount))
	return copyCount
}

func FixedSizeFinalizeAppend(
	segment *ColumnSegment,
	pTyp common.PhyType,
) IdxType {
	return IdxType(segment._count.Load()) * IdxType(pTyp.Size())
}
