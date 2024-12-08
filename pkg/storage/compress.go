package storage

import (
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
type CompressAppend func(
	state *CompressAppendState,
	segment *ColumnSegment,
	data *chunk.UnifiedFormat,
	offset IdxType,
	count IdxType,
) IdxType
type CompressFinalizeAppend func(segment *ColumnSegment) IdxType
type CompressRevertAppend func(segment *ColumnSegment, start IdxType)

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
	return &CompressFunction{
		_typ:      CompressTypeUncompressed,
		_dataType: typ,
	}
}
