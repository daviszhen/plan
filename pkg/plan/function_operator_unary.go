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
	"fmt"

	"github.com/daviszhen/plan/pkg/chunk"
	"github.com/daviszhen/plan/pkg/util"
)

type CastOp[T any, R any] func(input *T, result *R, strict bool) bool

type UnaryOp[T any, R any] func(input *T, result *R)

type UnaryFunc[T any, R any] func(input *T, result *R)

type UnaryOp2[T any, R any] func(input *T, result *R, mask *util.Bitmap, idx int, data *UnaryData)

//lint:ignore U1000
type UnaryWrapper[T any, R any] interface {
	operation(input *T, result *R, mask *util.Bitmap, idx int, data *UnaryData)
}

//lint:ignore U1000
type GenericUnaryWrapper[T any, R any] struct {
	op UnaryOp2[T, R]
}

func (wrapper *GenericUnaryWrapper[T, R]) operation(
	input *T,
	result *R,
	mask *util.Bitmap,
	idx int,
	data *UnaryData,
) {
	wrapper.op(input, result, mask, idx, data)
}

//lint:ignore U1000
type UnaryOperatorWrapper[T any, R any] struct {
	op UnaryOp[T, R]
}

func (wrapper *UnaryOperatorWrapper[T, R]) operation(
	input *T,
	result *R,
	mask *util.Bitmap,
	idx int,
	data *UnaryData,
) {
	wrapper.op(input, result)
}

//lint:ignore U1000
type UnaryLambdaWrapper[T any, R any] struct {
	fun UnaryFunc[T, R]
}

func (wrapper *UnaryLambdaWrapper[T, R]) operation(
	input *T,
	result *R,
	mask *util.Bitmap,
	idx int,
	data *UnaryData,
) {
	wrapper.fun(input, result)
}

func VectorTryCastOperator[T, R any](
	input *T,
	result *R,
	mask *util.Bitmap,
	idx int,
	data *UnaryData,
	op CastOp[T, R]) {
	ret := op(input, result, false)
	if !ret {
		err := fmt.Sprintf("VectorTryCastOperator[%d]: operation failed", idx)
		data._tryCastData._errorMsg = &err
		data._tryCastData._allConverted = false
		mask.SetInvalid(uint64(idx))
	}
}

func UnaryFunction[T any, R any](
	op UnaryOp[T, R]) ScalarFunc {
	wrapper := &UnaryOperatorWrapper[T, R]{}
	temp := func(input *chunk.Chunk, state *ExprState, result *chunk.Vector) {
		unaryExecStandard[T, R](
			input.Data[0],
			result,
			input.Card(),
			nil,
			false,
			wrapper)
	}
	return temp
}

func UnaryExecuteStandard[T any, R any](
	input, result *chunk.Vector,
	count int,
	data *UnaryData,
	addNulls bool) {

}

func unaryGenericExec[T any, R any](
	input, result *chunk.Vector,
	count int,
	data *UnaryData,
	addNulls bool,
	op UnaryOp2[T, R]) {
	wrapper := &GenericUnaryWrapper[T, R]{
		op: op,
	}
	unaryExecStandard[T, R](
		input, result,
		count,
		data,
		addNulls,
		wrapper)
}

func unaryExecStandard[T any, R any](
	input, result *chunk.Vector,
	count int,
	data *UnaryData,
	addNulls bool,
	wrapper UnaryWrapper[T, R]) {
	if input.PhyFormat().IsConst() {
		result.SetPhyFormat(chunk.PF_CONST)
		lslice := chunk.GetSliceInPhyFormatConst[T](input)
		resSlice := chunk.GetSliceInPhyFormatConst[R](result)
		if chunk.IsNullInPhyFormatConst(input) {
			chunk.SetNullInPhyFormatConst(result, true)
		} else {
			chunk.SetNullInPhyFormatConst(result, false)
			wrapper.operation(&lslice[0], &resSlice[0], chunk.GetMaskInPhyFormatConst(result), 0, data)
		}
	} else if input.PhyFormat().IsFlat() {
		result.SetPhyFormat(chunk.PF_FLAT)
		lslice := chunk.GetSliceInPhyFormatConst[T](input)
		resSlice := chunk.GetSliceInPhyFormatConst[R](result)
		unaryExecFlat[T, R](
			lslice,
			resSlice,
			count,
			chunk.GetMaskInPhyFormatFlat(input),
			chunk.GetMaskInPhyFormatFlat(result),
			data,
			addNulls,
			wrapper,
		)
	} else {
		var uv chunk.UnifiedFormat
		input.ToUnifiedFormat(count, &uv)
		result.SetPhyFormat(chunk.PF_FLAT)
		inputSlice := chunk.GetSliceInPhyFormatUnifiedFormat[T](&uv)
		resSlice := chunk.GetSliceInPhyFormatFlat[R](result)
		unaryExecLoop[T, R](
			inputSlice,
			resSlice,
			count,
			uv.Sel,
			uv.Mask,
			chunk.GetMaskInPhyFormatFlat(result),
			data,
			addNulls,
			wrapper,
		)
	}
}

func unaryExecFlat[T any, R any](
	input []T,
	result []R,
	count int,
	mask *util.Bitmap,
	resMask *util.Bitmap,
	data *UnaryData,
	addNulls bool,
	wrapper UnaryWrapper[T, R],
) {
	if !mask.AllValid() {
		if !addNulls {
			resMask.ShareWith(mask)
		} else {
			resMask.CopyFrom(mask, count)
		}
		baseIdx := 0
		eCnt := util.EntryCount(count)
		for eIdx := 0; eIdx < eCnt; eIdx++ {
			ent := mask.GetEntry(uint64(eIdx))
			next := min(baseIdx+8, count)
			if util.AllValidInEntry(ent) {
				for ; baseIdx < next; baseIdx++ {
					wrapper.operation(&input[baseIdx], &result[baseIdx], resMask, baseIdx, data)
				}
			} else if util.NoneValidInEntry(ent) {
				baseIdx = next
				continue
			} else {
				start := baseIdx
				for ; baseIdx < next; baseIdx++ {
					if util.RowIsValidInEntry(ent, uint64(baseIdx-start)) {
						wrapper.operation(&input[baseIdx], &result[baseIdx], resMask, baseIdx, data)
					}
				}
			}
		}
	} else {
		for i := 0; i < count; i++ {
			wrapper.operation(&input[i], &result[i], resMask, i, data)
		}
	}
}

func unaryExecLoop[T any, R any](
	input []T,
	result []R,
	count int,
	sel *chunk.SelectVector,
	mask *util.Bitmap,
	resMask *util.Bitmap,
	data *UnaryData,
	addNulls bool,
	wrapper UnaryWrapper[T, R],
) {
	if !mask.AllValid() {
		for i := 0; i < count; i++ {
			idx := sel.GetIndex(i)
			if mask.RowIsValidUnsafe(uint64(idx)) {
				wrapper.operation(&input[idx], &result[i], resMask, i, data)
			} else {
				resMask.SetInvalid(uint64(i))
			}
		}
	} else {
		for i := 0; i < count; i++ {
			idx := sel.GetIndex(i)
			wrapper.operation(&input[idx], &result[i], resMask, i, data)
		}
	}
}
