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
	"math"

	"github.com/daviszhen/plan/pkg/chunk"
	"github.com/daviszhen/plan/pkg/common"
	"github.com/daviszhen/plan/pkg/util"
)

type BinaryOp[T any, S any, R any] func(left *T, right *S, result *R)

type BinaryFunc[T any, S any, R any] func(left *T, right *S, result *R, mask *util.Bitmap, idx int)

type BinaryWrapper[T any, S any, R any] interface {
	operation(left *T, right *S, result *R, mask *util.Bitmap, idx int,
		fun BinaryFunc[T, S, R])

	addsNulls() bool
}

//lint:ignore U1000
type BinaryStandardOperatorWrapper[T any, S any, R any] struct {
	op BinaryOp[T, S, R]
}

func (wrapper *BinaryStandardOperatorWrapper[T, S, R]) operation(
	left *T, right *S, result *R, mask *util.Bitmap, idx int,
	fun BinaryFunc[T, S, R]) {
	wrapper.op(left, right, result)
}

func (wrapper *BinaryStandardOperatorWrapper[T, S, R]) addsNulls() bool {
	return false
}

//lint:ignore U1000
type BinarySingleArgumentOperatorWrapper[T any, R any] struct {
	op BinaryOp[T, T, R]
}

func (wrapper *BinarySingleArgumentOperatorWrapper[T, R]) operation(
	left *T, right *T, result *R, mask *util.Bitmap, idx int,
	fun BinaryFunc[T, T, R]) {
	wrapper.op(left, right, result)
}

func (wrapper *BinarySingleArgumentOperatorWrapper[T, R]) addsNulls() bool {
	return false
}

//lint:ignore U1000
type BinaryLambdaWrapper[T any, S any, R any] struct {
}

func (wrapper *BinaryLambdaWrapper[T, S, R]) operation(
	left *T, right *S, result *R, mask *util.Bitmap, idx int,
	fun BinaryFunc[T, S, R]) {
	fun(left, right, result, mask, idx)
}

func (wrapper *BinaryLambdaWrapper[T, S, R]) addsNulls() bool {
	return false
}

//lint:ignore U1000
type BinaryLambdaWrapperWithNulls[T any, S any, R any] struct {
}

func (wrapper *BinaryLambdaWrapperWithNulls[T, S, R]) operation(
	left *T, right *S, result *R, mask *util.Bitmap, idx int,
	fun BinaryFunc[T, S, R]) {
	fun(left, right, result, mask, idx)
}

func (wrapper *BinaryLambdaWrapperWithNulls[T, S, R]) addsNulls() bool {
	return true
}

func substringFuncWithoutLength(s *common.String, offset *int64, result *common.String) {
	length := int64(math.MaxUint32)
	substringFunc(s, offset, &length, result)
}

// +
//
//lint:ignore U1000
func binDateInterAddOp(left *common.Date, right *common.Interval, result *common.Date) {
	*result = left.AddInterval(right)
}

//lint:ignore U1000
func binDateInt32AddOp(left *common.Date, right *int32, result *common.Date) {
	panic("usp")
}

//lint:ignore U1000
func binInt32DateAddOp(left *int32, right *common.Date, result *common.Date) {
	panic("usp")
}

//lint:ignore U1000
func binIntervalIntervalAddOp(left *common.Interval, right *common.Interval, result *common.Interval) {
	panic("usp")
}

//lint:ignore U1000
func binIntervalDateAddOp(left *common.Interval, right *common.Date, result *common.Date) {
	panic("usp")
}

//lint:ignore U1000
func binFloat32Float32AddOp(left *float32, right *float32, result *float32) {
	*result = *left + *right
}

//lint:ignore U1000
func binDecimalDecimalAddOp(left *common.Decimal, right *common.Decimal, result *common.Decimal) {
	d, err := left.Decimal.Add(right.Decimal)
	if err != nil {
		panic(err)
	}
	result.Decimal = d
}

//lint:ignore U1000
func binInt32Int32AddOp(left *int32, right *int32, result *int32) {
	*result = *left + *right
}

// -

//lint:ignore U1000
func binDateInterSubOp(left *common.Date, right *common.Interval, result *common.Date) {
	res := left.SubInterval(right)
	*result = res
}

//lint:ignore U1000
func binFloat32Float32SubOp(left *float32, right *float32, result *float32) {
	*result = *left - *right
}

//lint:ignore U1000
func binDecimalDecimalSubOp(left *common.Decimal, right *common.Decimal, result *common.Decimal) {
	d, err := left.Sub(right.Decimal)
	if err != nil {
		panic(err)
	}
	result.Decimal = d
}

// *
//
//lint:ignore U1000
func binFloat32MultiOp(left, right *float32, result *float32) {
	*result = *left * *right
}

//lint:ignore U1000
func binFloat64MultiOp(left, right *float64, result *float64) {
	*result = *left * *right
}

// *
//
//lint:ignore U1000
func binDecimalDecimalMulOp(left *common.Decimal, right *common.Decimal, result *common.Decimal) {
	d, err := left.Decimal.Mul(right.Decimal)
	if err != nil {
		panic(err)
	}
	result.Decimal = d

}

// /
//
//lint:ignore U1000
func binFloat32DivOp(left, right *float32, result *float32) {
	*result = *left / *right
}

//lint:ignore U1000
func binDecimalDivOp(left, right *common.Decimal, result *common.Decimal) {
	quo, err := left.Decimal.Quo(right.Decimal)
	if err != nil {
		panic(err)
	}
	result.Decimal = quo
}

// = int32
//
//lint:ignore U1000
func binInt32EqualOp(left, right *int32, result *bool) {
	*result = *left == *right
}

//lint:ignore U1000
func binBoolEqualOp(left, right *bool, result *bool) {
	*result = *left == *right
}

// = string
//
//lint:ignore U1000
func binStringEqualOp(left, right *common.String, result *bool) {
	*result = left.Equal(right)
}

// > int32
//
//lint:ignore U1000
func binInt32GreatOp(left, right *int32, result *bool) {
	*result = *left > *right
}

// > float32
//
//lint:ignore U1000
func binFloat32GreatOp(left, right *float32, result *bool) {
	*result = *left > *right
}

// < string
//
//lint:ignore U1000
func binStringLessOp(left, right *common.String, result *bool) {
	*result = left.Less(right)
}

// like
//
//lint:ignore U1000
func binStringLikeOp(left, right *common.String, result *bool) {
	*result = WildcardMatch(right, left)
}

// extract
//
//lint:ignore U1000
func binStringInt32ExtractOp(left *common.String, right *common.Date, result *int32) {
	if left.String() == "year" {
		*result = right.Year
	} else {
		panic("usp")
	}
}

func binaryExecSwitch[T any, S any, R any](
	left, right, result *chunk.Vector,
	count int,
	fun BinaryFunc[T, S, R],
	wrapper BinaryWrapper[T, S, R],
) {
	if left.PhyFormat().IsConst() && right.PhyFormat().IsConst() {
		binaryExecConst[T, S, R](left, right, result, count, fun, wrapper)
	} else if left.PhyFormat().IsFlat() && right.PhyFormat().IsConst() {
		binaryExecFlat[T, S, R](left, right, result, count, fun, wrapper, false, true)
	} else if left.PhyFormat().IsConst() && right.PhyFormat().IsFlat() {
		binaryExecFlat[T, S, R](left, right, result, count, fun, wrapper, true, false)
	} else if left.PhyFormat().IsFlat() && right.PhyFormat().IsFlat() {
		binaryExecFlat[T, S, R](left, right, result, count, fun, wrapper, false, false)
	} else {
		binaryExecGeneric[T, S, R](left, right, result, count, fun, wrapper)
	}
}
func binaryExecConst[T any, S any, R any](
	left, right, result *chunk.Vector,
	count int,
	fun BinaryFunc[T, S, R],
	wrapper BinaryWrapper[T, S, R],
) {
	result.SetPhyFormat(chunk.PF_CONST)
	if chunk.IsNullInPhyFormatConst(left) ||
		chunk.IsNullInPhyFormatConst(right) {
		chunk.SetNullInPhyFormatConst(result, true)
		return
	}
	lSlice := chunk.GetSliceInPhyFormatConst[T](left)
	rSlice := chunk.GetSliceInPhyFormatConst[S](right)
	resSlice := chunk.GetSliceInPhyFormatConst[R](result)

	wrapper.operation(&lSlice[0], &rSlice[0], &resSlice[0], chunk.GetMaskInPhyFormatConst(result), 0, fun)
}

func binaryExecFlat[T any, S any, R any](
	left, right, result *chunk.Vector,
	count int,
	fun BinaryFunc[T, S, R],
	wrapper BinaryWrapper[T, S, R],
	lconst, rconst bool,
) {
	lSlice := chunk.GetSliceInPhyFormatFlat[T](left)
	rSlice := chunk.GetSliceInPhyFormatFlat[S](right)
	if lconst && chunk.IsNullInPhyFormatConst(left) ||
		rconst && chunk.IsNullInPhyFormatConst(right) {
		result.SetPhyFormat(chunk.PF_CONST)
		chunk.SetNullInPhyFormatConst(result, true)
		return
	}

	result.SetPhyFormat(chunk.PF_FLAT)
	resSlice := chunk.GetSliceInPhyFormatFlat[R](result)
	resMask := chunk.GetMaskInPhyFormatFlat(result)
	if lconst {
		if wrapper.addsNulls() {
			resMask.CopyFrom(chunk.GetMaskInPhyFormatFlat(right), count)
		} else {
			chunk.SetMaskInPhyFormatFlat(result, chunk.GetMaskInPhyFormatFlat(right))
		}
	} else if rconst {
		if wrapper.addsNulls() {
			resMask.CopyFrom(chunk.GetMaskInPhyFormatFlat(left), count)
		} else {
			chunk.SetMaskInPhyFormatFlat(result, chunk.GetMaskInPhyFormatFlat(left))
		}
	} else {
		if wrapper.addsNulls() {
			resMask.CopyFrom(chunk.GetMaskInPhyFormatFlat(left), count)
			if resMask.AllValid() {
				resMask.CopyFrom(chunk.GetMaskInPhyFormatFlat(right), count)
			} else {
				resMask.Combine(chunk.GetMaskInPhyFormatFlat(right), count)
			}
		} else {
			chunk.SetMaskInPhyFormatFlat(result, chunk.GetMaskInPhyFormatFlat(left))
			resMask.Combine(chunk.GetMaskInPhyFormatFlat(right), count)
		}
	}
	binaryExecFlatLoop[T, S, R](
		lSlice,
		rSlice,
		resSlice,
		count,
		resMask,
		fun,
		wrapper,
		lconst,
		rconst,
	)
}

func binaryExecFlatLoop[T any, S any, R any](
	ldata []T, rdata []S,
	resData []R,
	count int,
	mask *util.Bitmap,
	fun BinaryFunc[T, S, R],
	wrapper BinaryWrapper[T, S, R],
	lconst, rconst bool,
) {
	if !mask.AllValid() {
		baseIdx := 0
		eCnt := util.EntryCount(count)
		for i := 0; i < eCnt; i++ {
			ent := mask.GetEntry(uint64(i))
			next := min(baseIdx+8, count)
			if util.AllValidInEntry(ent) {
				for ; baseIdx < next; baseIdx++ {
					lidx := baseIdx
					ridx := baseIdx
					if lconst {
						lidx = 0
					}
					if rconst {
						ridx = 0
					}
					wrapper.operation(&ldata[lidx], &rdata[ridx], &resData[baseIdx], mask, baseIdx, fun)
				}
			} else if util.NoneValidInEntry(ent) {
				baseIdx = next
				continue
			} else {
				start := baseIdx
				for ; baseIdx < next; baseIdx++ {
					if util.RowIsValidInEntry(ent, uint64(baseIdx-start)) {
						lidx := baseIdx
						ridx := baseIdx
						if lconst {
							lidx = 0
						}
						if rconst {
							ridx = 0
						}
						wrapper.operation(&ldata[lidx], &rdata[ridx], &resData[baseIdx], mask, baseIdx, fun)
					}
				}
			}
		}
	} else {
		for i := 0; i < count; i++ {
			lidx := i
			ridx := i
			if lconst {
				lidx = 0
			}
			if rconst {
				ridx = 0
			}
			wrapper.operation(&ldata[lidx], &rdata[ridx], &resData[i], mask, i, fun)
		}
	}
}

func binaryExecGeneric[T any, S any, R any](
	left, right, result *chunk.Vector,
	count int,
	fun BinaryFunc[T, S, R],
	wrapper BinaryWrapper[T, S, R],
) {
	var ldata, rdata chunk.UnifiedFormat
	left.ToUnifiedFormat(count, &ldata)
	right.ToUnifiedFormat(count, &rdata)

	lSlice := chunk.GetSliceInPhyFormatUnifiedFormat[T](&ldata)
	rSlice := chunk.GetSliceInPhyFormatUnifiedFormat[S](&rdata)
	result.SetPhyFormat(chunk.PF_FLAT)
	resSlice := chunk.GetSliceInPhyFormatFlat[R](result)
	binaryExecGenericLoop[T, S, R](
		lSlice,
		rSlice,
		resSlice,
		ldata.Sel,
		rdata.Sel,
		count,
		ldata.Mask,
		rdata.Mask,
		result.Mask,
		fun,
		wrapper,
	)
}

func binaryExecGenericLoop[T any, S any, R any](
	ldata []T, rdata []S,
	resData []R,
	lsel *chunk.SelectVector,
	rsel *chunk.SelectVector,
	count int,
	lmask *util.Bitmap,
	rmask *util.Bitmap,
	resMask *util.Bitmap,
	fun BinaryFunc[T, S, R],
	wrapper BinaryWrapper[T, S, R],
) {
	if !lmask.AllValid() || !rmask.AllValid() {
		for i := 0; i < count; i++ {
			lidx := lsel.GetIndex(i)
			ridx := rsel.GetIndex(i)
			if lmask.RowIsValid(uint64(lidx)) && rmask.RowIsValid(uint64(ridx)) {
				wrapper.operation(&ldata[lidx], &rdata[ridx], &resData[i], resMask, i, fun)
			} else {
				resMask.SetInvalid(uint64(i))
			}
		}
	} else {
		for i := 0; i < count; i++ {
			lidx := lsel.GetIndex(i)
			ridx := rsel.GetIndex(i)
			wrapper.operation(&ldata[lidx], &rdata[ridx], &resData[i], resMask, i, fun)
		}
	}
}

func BinaryFunction[T, S, R any](
	op BinaryOp[T, S, R],
) ScalarFunc {
	return ExecuteStandard[T, S, R](op)
}

func ExecuteStandard[T, S, R any](
	op BinaryOp[T, S, R],
) ScalarFunc {
	wrapper := &BinaryStandardOperatorWrapper[T, S, R]{op: op}
	temp := func(input *chunk.Chunk, state *ExprState, result *chunk.Vector) {
		binaryExecSwitch[T, S, R](
			input.Data[0],
			input.Data[1],
			result,
			input.Card(),
			nil, wrapper)
	}
	return temp
}
