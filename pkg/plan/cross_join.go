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
	"unsafe"

	"github.com/daviszhen/plan/pkg/chunk"
	"github.com/daviszhen/plan/pkg/common"
	"github.com/daviszhen/plan/pkg/util"
)

type CrossStage int

const (
	CROSS_INIT CrossStage = iota
	CROSS_BUILD
	CROSS_PROBE
)

type CrossProduct struct {
	_crossStage  CrossStage
	_dataCollect *ColumnDataCollection
	_crossExec   *CrossProductExec
	_input       *chunk.Chunk
}

func NewCrossProduct(types []common.LType) *CrossProduct {
	ret := &CrossProduct{}
	ret._dataCollect = NewColumnDataCollection(types)
	ret._crossExec = NewCrossProductExec(ret._dataCollect)
	return ret
}

func (cross *CrossProduct) Sink(input *chunk.Chunk) {
	cross._dataCollect.Append(input)
}

func (cross *CrossProduct) Execute(input, output *chunk.Chunk) (OperatorResult, error) {
	return cross._crossExec.Execute(input, output)
}

type CrossProductExec struct {
	_rhs             *ColumnDataCollection
	_scanState       *ColumnDataScanState
	_scanChunk       *chunk.Chunk //constant chunk (scanning chunk is the input chunk)
	_positionInChunk int          //position in the scanning chunk different from constant chunk
	_init            bool
	_finish          bool
	_scanInputChunk  bool
	_outputExec      *ExprExec
	_outputPosMap    map[int]ColumnBind
}

func NewCrossProductExec(rhs *ColumnDataCollection) *CrossProductExec {
	ret := &CrossProductExec{
		_rhs:       rhs,
		_scanChunk: &chunk.Chunk{},
		_scanState: &ColumnDataScanState{},
	}
	ret._rhs.initScanChunk(ret._scanChunk)
	return ret
}

func (cross *CrossProductExec) Reset() {
	cross._init = true
	cross._finish = false
	cross._scanInputChunk = false
	cross._rhs.initScan(cross._scanState)
	cross._positionInChunk = 0
	cross._scanChunk.Reset()
}

func (cross *CrossProductExec) NextValue(input, output *chunk.Chunk) bool {
	if !cross._init {
		cross.Reset()
	}
	cross._positionInChunk++
	chunkSize := 0
	if cross._scanInputChunk {
		chunkSize = input.Card()
	} else {
		chunkSize = cross._scanChunk.Card()
	}
	if cross._positionInChunk < chunkSize {
		return true
	}
	//pick next chunk from the collection
	cross._rhs.Scan(cross._scanState, cross._scanChunk)
	cross._positionInChunk = 0
	if cross._scanChunk.Card() == 0 {
		return false
	}
	cross._scanInputChunk = true
	return true
}

func (cross *CrossProductExec) Execute(input, output *chunk.Chunk) (OperatorResult, error) {
	if cross._rhs.Count() == 0 {
		// no RHS, empty result
		return Done, nil
	}

	if !cross.NextValue(input, output) {
		cross._init = false
		//RHS is read over.
		//switch to the next Chunk on the LHS and reset the RHS

		return NeedMoreInput, nil
	}

	//err := cross._outputExec.executeExprs(
	//	[]*Chunk{
	//		input,
	//		cross._scanChunk,
	//		nil,
	//	},
	//	output,
	//)
	//if err != nil {
	//	return InvalidOpResult, err
	//}

	var constChunk *chunk.Chunk
	//scanning chunk. refer a single value
	var scanChunk *chunk.Chunk
	for i := 0; i < output.ColumnCount(); i++ {
		if cross._scanInputChunk {
			constChunk = cross._scanChunk
		} else {
			constChunk = input
		}

		if cross._scanInputChunk {
			scanChunk = input
		} else {
			scanChunk = cross._scanChunk
		}

		bind := cross._outputPosMap[i]
		tblIdx := int64(bind.table())
		colIdx := int64(bind.column())
		if cross._scanInputChunk {
			if tblIdx == -2 {
				output.Data[i].Reference(constChunk.Data[colIdx])
			} else if tblIdx == -1 {
				chunk.ReferenceInPhyFormatConst(
					output.Data[i],
					scanChunk.Data[colIdx],
					cross._positionInChunk,
					scanChunk.Card(),
				)
			} else {
				panic("usp")
			}
		} else {
			if tblIdx == -1 {
				output.Data[i].Reference(constChunk.Data[colIdx])
			} else if tblIdx == -2 {
				chunk.ReferenceInPhyFormatConst(
					output.Data[i],
					scanChunk.Data[colIdx],
					cross._positionInChunk,
					scanChunk.Card(),
				)
			} else {
				panic("usp")
			}
		}
	}
	output.SetCard(constChunk.Card())

	//var constChunk *Chunk
	//if cross._scanInputChunk {
	//	constChunk = cross._scanChunk
	//} else {
	//	constChunk = input
	//}
	//colCnt := constChunk.columnCount()
	//colOffset := 0
	//if cross._scanInputChunk {
	//	colOffset = input.columnCount()
	//} else {
	//	colOffset = 0
	//}

	//output.setCard(constChunk.card())

	////refer constant vector
	//for i := 0; i < colCnt; i++ {
	//	output.Data[colOffset+i].reference(constChunk.Data[i])
	//}

	//scanning chunk. refer a single value
	//var scanChunk *Chunk
	//if cross._scanInputChunk {
	//	scanChunk = input
	//} else {
	//	scanChunk = cross._scanChunk
	//}
	//colCnt = scanChunk.columnCount()
	//if cross._scanInputChunk {
	//	colOffset = 0
	//} else {
	//	colOffset = input.columnCount()
	//}
	//
	//for i := 0; i < colCnt; i++ {
	//	referenceInPhyFormatConst(
	//		output.Data[colOffset+i],
	//		scanChunk.Data[i],
	//		cross._positionInChunk,
	//		scanChunk.card(),
	//	)
	//}
	return haveMoreOutput, nil
}

type ColumnDataScanState struct {
	chunkIdx      int
	currentRowIdx int
	nextRowIdx    int
	columnIds     []int
}

type ColumnDataMetaData struct {
	_dst    *chunk.Chunk
	_vecIdx int
}

type ColumnDataCollection struct {
	_types  []common.LType
	_count  int
	_chunks []*chunk.Chunk
}

func NewColumnDataCollection(typs []common.LType) *ColumnDataCollection {
	return &ColumnDataCollection{
		_types: typs,
	}
}

func (cdc *ColumnDataCollection) Append(input *chunk.Chunk) {
	vecData := make([]chunk.UnifiedFormat, len(cdc._types))
	for i := 0; i < len(cdc._types); i++ {
		input.Data[i].ToUnifiedFormat(input.Card(), &vecData[i])
	}

	remaining := input.Card()
	for remaining > 0 {
		if len(cdc._chunks) == 0 {
			newChunk := &chunk.Chunk{}
			newChunk.Init(cdc._types, util.DefaultVectorSize)
			cdc._chunks = append(cdc._chunks, newChunk)
		}
		chunkData := util.Back(cdc._chunks)
		appendAmount := min(remaining, util.DefaultVectorSize-chunkData.Card())
		if appendAmount > 0 {
			//enough space
			offset := input.Card() - remaining
			//copy
			for i := 0; i < len(cdc._types); i++ {
				metaData := ColumnDataMetaData{
					_dst:    chunkData,
					_vecIdx: i,
				}
				ColumnDataCopySwitch(
					&metaData,
					&vecData[i],
					input.Data[i],
					offset,
					appendAmount,
				)
			}
			chunkData.Count += appendAmount
		}
		remaining -= appendAmount
		if remaining > 0 {
			newChunk := &chunk.Chunk{}
			newChunk.Init(cdc._types, util.DefaultVectorSize)
			cdc._chunks = append(cdc._chunks, newChunk)
		}
	}
	cdc._count += input.Card()

}

func (cdc *ColumnDataCollection) initScanChunk(chunk *chunk.Chunk) {
	chunk.Init(cdc._types, util.DefaultVectorSize)
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
	output *chunk.Chunk) bool {
	output.Reset()

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
			srcChunk.Data[vecIdx],
			output.Data[vecIdx],
			srcChunk.Card(),
		)
	}
	output.SetCard(srcChunk.Card())
	return true
}

func (cdc *ColumnDataCollection) ReadVector(
	state *ColumnDataScanState,
	srcVec *chunk.Vector,
	dstVec *chunk.Vector,
	count int,
) int {
	dstVecType := dstVec.Typ()
	dstInterType := dstVecType.GetInternalType()
	dstTypeSize := dstInterType.Size()
	if count == 0 {
		return 0
	}
	srcPtr := util.BytesSliceToPointer(srcVec.Data)
	dstPtr := util.BytesSliceToPointer(dstVec.Data)
	dstBitmap := chunk.GetMaskInPhyFormatFlat(dstVec)
	util.PointerCopy(dstPtr, srcPtr, dstTypeSize*count)
	dstBitmap.CopyFrom(srcVec.Mask, count)

	if dstInterType == common.VARCHAR {
		chunk.Copy(dstVec,
			dstVec,
			chunk.IncrSelectVectorInPhyFormatFlat(),
			count,
			0,
			0,
		)
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

	state.nextRowIdx += cdc._chunks[state.chunkIdx].Card()
	*chunkIdx = state.chunkIdx
	state.chunkIdx++
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
	dPtr := util.PointerAdd(dst, dstIdx*common.Int32Size)
	sPtr := util.PointerAdd(src, srcIdx*common.Int32Size)
	copy.Operation((*int32)(dPtr), (*int32)(sPtr))
}

func (copy *int32ValueCopy) Operation(dst, src *int32) {
	*dst = *src
}

type int64ValueCopy struct {
}

func (copy *int64ValueCopy) Assign(
	metaData *ColumnDataMetaData,
	dst, src unsafe.Pointer,
	dstIdx, srcIdx int) {
	dPtr := util.PointerAdd(dst, dstIdx*common.Int64Size)
	sPtr := util.PointerAdd(src, srcIdx*common.Int64Size)
	copy.Operation((*int64)(dPtr), (*int64)(sPtr))
}

func (copy *int64ValueCopy) Operation(dst, src *int64) {
	*dst = *src
}

type float32ValueCopy struct {
}

func (copy *float32ValueCopy) Assign(
	metaData *ColumnDataMetaData,
	dst, src unsafe.Pointer,
	dstIdx, srcIdx int) {
	dPtr := util.PointerAdd(dst, dstIdx*common.Float32Size)
	sPtr := util.PointerAdd(src, srcIdx*common.Float32Size)
	copy.Operation((*float32)(dPtr), (*float32)(sPtr))
}

func (copy *float32ValueCopy) Operation(dst, src *float32) {
	*dst = *src
}

type decimalValueCopy struct {
}

func (copy *decimalValueCopy) Assign(
	metaData *ColumnDataMetaData,
	dst, src unsafe.Pointer,
	dstIdx, srcIdx int) {
	dPtr := util.PointerAdd(dst, dstIdx*common.DecimalSize)
	sPtr := util.PointerAdd(src, srcIdx*common.DecimalSize)
	copy.Operation((*common.Decimal)(dPtr), (*common.Decimal)(sPtr))
}

func (copy *decimalValueCopy) Operation(dst, src *common.Decimal) {
	*dst = *src
}

type dateValueCopy struct {
}

func (copy *dateValueCopy) Assign(
	metaData *ColumnDataMetaData,
	dst, src unsafe.Pointer,
	dstIdx, srcIdx int) {
	dPtr := util.PointerAdd(dst, dstIdx*common.DateSize)
	sPtr := util.PointerAdd(src, srcIdx*common.DateSize)
	copy.Operation((*common.Date)(dPtr), (*common.Date)(sPtr))
}

func (copy *dateValueCopy) Operation(dst, src *common.Date) {
	*dst = *src
}

type hugeintValueCopy struct {
}

func (copy *hugeintValueCopy) Assign(
	metaData *ColumnDataMetaData,
	dst, src unsafe.Pointer,
	dstIdx, srcIdx int) {
	dPtr := util.PointerAdd(dst, dstIdx*common.Int128Size)
	sPtr := util.PointerAdd(src, srcIdx*common.Int128Size)
	copy.Operation((*common.Hugeint)(dPtr), (*common.Hugeint)(sPtr))
}

func (copy *hugeintValueCopy) Operation(dst, src *common.Hugeint) {
	*dst = *src
}

type varcharValueCopy struct {
}

func (copy *varcharValueCopy) Assign(
	metaData *ColumnDataMetaData,
	dst, src unsafe.Pointer,
	dstIdx, srcIdx int) {
	dPtr := util.PointerAdd(dst, dstIdx*common.VarcharSize)
	sPtr := util.PointerAdd(src, srcIdx*common.VarcharSize)
	copy.Operation((*common.String)(dPtr), (*common.String)(sPtr))
}

func (copy *varcharValueCopy) Operation(dst, src *common.String) {
	if src.Length() == 0 {
		dst.Len = 0
		dst.Data = nil
		return
	}
	dst.Data = util.CMalloc(src.Length())
	util.PointerCopy(dst.DataPtr(), src.DataPtr(), src.Length())
	dst.Len = src.Length()
}

func ColumnDataCopySwitch(
	metaData *ColumnDataMetaData,
	srcData *chunk.UnifiedFormat,
	src *chunk.Vector,
	offset int,
	count int,
) {
	switch src.Typ().GetInternalType() {
	case common.INT32:
		TemplatedColumnDataCopy[int32](
			metaData,
			srcData,
			src,
			offset,
			count,
			&int32ValueCopy{},
		)
	case common.INT64:
		TemplatedColumnDataCopy[int64](
			metaData,
			srcData,
			src,
			offset,
			count,
			&int64ValueCopy{},
		)
	case common.FLOAT:
		TemplatedColumnDataCopy[float32](
			metaData,
			srcData,
			src,
			offset,
			count,
			&float32ValueCopy{},
		)
	case common.DECIMAL:
		TemplatedColumnDataCopy[common.Decimal](
			metaData,
			srcData,
			src,
			offset,
			count,
			&decimalValueCopy{},
		)
	case common.VARCHAR:
		TemplatedColumnDataCopy[common.String](
			metaData,
			srcData,
			src,
			offset,
			count,
			&varcharValueCopy{},
		)
	case common.DATE:
		TemplatedColumnDataCopy[common.Date](
			metaData,
			srcData,
			src,
			offset,
			count,
			&dateValueCopy{},
		)
	case common.INT128:
		TemplatedColumnDataCopy[common.Hugeint](
			metaData,
			srcData,
			src,
			offset,
			count,
			&hugeintValueCopy{},
		)
	default:
		panic("usp")
	}
}

func TemplatedColumnDataCopy[T any](
	metaData *ColumnDataMetaData,
	srcData *chunk.UnifiedFormat,
	src *chunk.Vector,
	offset int,
	count int,
	cp BaseValueCopy[T],
) {
	remaining := count
	for remaining > 0 {
		vec := metaData._dst.Data[metaData._vecIdx]
		appendCount := min(util.DefaultVectorSize-metaData._dst.Card(), remaining)
		if metaData._dst.Card() == 0 {
			vec.Mask.SetAllValid(util.DefaultVectorSize)
		}
		for i := 0; i < appendCount; i++ {
			srcIdx := srcData.Sel.GetIndex(offset + i)
			if srcData.Mask.RowIsValid(uint64(srcIdx)) {
				cp.Assign(metaData,
					util.BytesSliceToPointer(vec.Data),
					util.BytesSliceToPointer(srcData.Data),
					metaData._dst.Card()+i,
					srcIdx,
				)
			} else {
				vec.Mask.SetInvalid(uint64(metaData._dst.Card() + i))
			}
		}
		offset += appendCount
		remaining -= appendCount
		if remaining > 0 {
			panic("usp")
		}
	}
}
