package storage

import (
	"unsafe"

	"github.com/daviszhen/plan/pkg/chunk"
	"github.com/daviszhen/plan/pkg/common"
)

type CompressType int

const (
	CompressTypeAuto CompressType = iota
	CompressTypeUncompressed
)

type CompressAppendState struct {
	_handle *BufferHandle
}

type CompressInitAppend func(segment *ColumnSegment) *CompressAppendState
type CompressAppend[T any] func(
	state *CompressAppendState,
	segment *ColumnSegment,
	data *chunk.UnifiedFormat,
	offset IdxType,
	count IdxType,
	appender Appender[T],
	typeSize IdxType,
) IdxType
type CompressFinalizeAppend func(segment *ColumnSegment) IdxType
type CompressRevertAppend func(segment *ColumnSegment, start IdxType)

type CompressFunction[T any] struct {
	_typ            CompressType
	_dataType       common.PhyType
	_initAppend     CompressInitAppend
	_append         CompressAppend[T]
	_finalizeAppend CompressFinalizeAppend
	_revertAppend   CompressRevertAppend
}

func GetUncompressedCompressFunction[T any](
	typ common.PhyType,
) *CompressFunction[T] {
	return &CompressFunction[T]{
		_typ:      CompressTypeUncompressed,
		_dataType: typ,
	}
}

func FixedSizeInitAppend(segment *ColumnSegment) *CompressAppendState {
	handle := GBufferMgr.Pin(segment._block)
	return &CompressAppendState{
		_handle: handle,
	}
}

type Appender[T any] func(
	target unsafe.Pointer,
	targetOffset IdxType,
	data *chunk.UnifiedFormat,
	offset IdxType,
	count IdxType)

func FixedSizeAppend[T any](
	state *CompressAppendState,
	segment *ColumnSegment,
	data *chunk.UnifiedFormat,
	offset IdxType,
	count IdxType,
	appender Appender[T],
	typeSize IdxType,
) IdxType {
	target := state._handle.Ptr()
	maxTupleCount := segment._segmentSize / typeSize
	copyCount := min(count, maxTupleCount-IdxType(segment._count.Load()))
	appender(
		target,
		IdxType(segment._count.Load()),
		data,
		offset,
		copyCount)
	segment._count.Add(uint64(copyCount))
	return copyCount
}

func FixedSizeFinalizeAppend(
	segment *ColumnSegment,
	typeSize IdxType,
) IdxType {
	return IdxType(segment._count.Load()) * typeSize
}
