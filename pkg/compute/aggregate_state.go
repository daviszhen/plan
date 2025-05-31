package compute

import (
	"github.com/daviszhen/plan/pkg/chunk"
	"github.com/daviszhen/plan/pkg/common"
	"github.com/daviszhen/plan/pkg/util"
)

type HashAggrScanState struct {
	_radixIdx     int
	_state        *TupleDataScanState
	_childCnt     int
	_childCnt2    int
	_outputCnt    int
	_filteredCnt1 int
	_filteredCnt2 int
	_childCnt3    int
}

func NewHashAggrScanState() *HashAggrScanState {
	return &HashAggrScanState{
		_state: &TupleDataScanState{},
	}
}

type HashAggrState int

const (
	HAS_INIT HashAggrState = iota
	HAS_BUILD
	HAS_SCAN
)

type TupleDataScanState struct {
	_colIds []int
	//_rowLocs *Vector
	_init       bool
	_pinState   TupleDataPinState
	_chunkState TupleDataChunkState
	_segmentIdx int
	_chunkIdx   int
}

func NewTupleDataScanState(colCnt int, prop TupleDataPinProperties) *TupleDataScanState {
	ret := &TupleDataScanState{}
	ret._pinState._properties = prop
	return ret
}

type AggrHTAppendState struct {
	_htOffsets          *chunk.Vector
	_hashSalts          *chunk.Vector
	_groupCompareVector *chunk.SelectVector
	_noMatchVector      *chunk.SelectVector
	_emptyVector        *chunk.SelectVector
	_newGroups          *chunk.SelectVector
	_addresses          *chunk.Vector
	//unified format of Chunk
	_groupData []*chunk.UnifiedFormat
	//Chunk
	_groupChunk *chunk.Chunk
	_chunkState *TupleDataChunkState
}

func NewAggrHTAppendState() *AggrHTAppendState {
	ret := new(AggrHTAppendState)
	ret._htOffsets = chunk.NewFlatVector(common.BigintType(), util.DefaultVectorSize)
	ret._hashSalts = chunk.NewFlatVector(common.SmallintType(), util.DefaultVectorSize)
	ret._groupCompareVector = chunk.NewSelectVector(util.DefaultVectorSize)
	ret._noMatchVector = chunk.NewSelectVector(util.DefaultVectorSize)
	ret._emptyVector = chunk.NewSelectVector(util.DefaultVectorSize)
	ret._newGroups = chunk.NewSelectVector(util.DefaultVectorSize)
	ret._addresses = chunk.NewVector2(common.PointerType(), util.DefaultVectorSize)
	ret._groupChunk = &chunk.Chunk{}
	return ret
}

type AggrInputData struct {
}

func NewAggrInputData() *AggrInputData {
	return &AggrInputData{}
}

type AggrUnaryInput struct {
	_input     *AggrInputData
	_inputMask *util.Bitmap
	_inputIdx  int
}

func NewAggrUnaryInput(input *AggrInputData, mask *util.Bitmap) *AggrUnaryInput {
	return &AggrUnaryInput{
		_input:     input,
		_inputMask: mask,
		_inputIdx:  0,
	}
}

type AggrFinalizeData struct {
	_result    *chunk.Vector
	_input     *AggrInputData
	_resultIdx int
}

func NewAggrFinalizeData(result *chunk.Vector, input *AggrInputData) *AggrFinalizeData {
	return &AggrFinalizeData{
		_result: result,
		_input:  input,
	}
}

func (data *AggrFinalizeData) ReturnNull() {
	switch data._result.PhyFormat() {
	case chunk.PF_FLAT:
		chunk.SetNullInPhyFormatFlat(data._result, uint64(data._resultIdx), true)
	case chunk.PF_CONST:
		chunk.SetNullInPhyFormatConst(data._result, true)
	default:
		panic("usp")
	}
}
