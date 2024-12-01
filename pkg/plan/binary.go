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
)

type BinaryOp[T any, S any, R any] func(left *T, right *S, result *R)

type BinaryFunc[T any, S any, R any] func(left *T, right *S, result *R, mask *Bitmap, idx int)

type BinaryWrapper[T any, S any, R any] interface {
	operation(left *T, right *S, result *R, mask *Bitmap, idx int,
		fun BinaryFunc[T, S, R])

	addsNulls() bool
}

//lint:ignore U1000
type BinaryStandardOperatorWrapper[T any, S any, R any] struct {
	op BinaryOp[T, S, R]
}

func (wrapper *BinaryStandardOperatorWrapper[T, S, R]) operation(
	left *T, right *S, result *R, mask *Bitmap, idx int,
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
	left *T, right *T, result *R, mask *Bitmap, idx int,
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
	left *T, right *S, result *R, mask *Bitmap, idx int,
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
	left *T, right *S, result *R, mask *Bitmap, idx int,
	fun BinaryFunc[T, S, R]) {
	fun(left, right, result, mask, idx)
}

func (wrapper *BinaryLambdaWrapperWithNulls[T, S, R]) addsNulls() bool {
	return true
}

func substringFuncWithoutLength(s *String, offset *int64, result *String) {
	length := int64(math.MaxUint32)
	substringFunc(s, offset, &length, result)
}

// +
//
//lint:ignore U1000
func binDateInterAddOp(left *Date, right *Interval, result *Date) {
	*result = left.addInterval(right)
}

//lint:ignore U1000
func binDateInt32AddOp(left *Date, right *int32, result *Date) {
	panic("usp")
}

//lint:ignore U1000
func binInt32DateAddOp(left *int32, right *Date, result *Date) {
	panic("usp")
}

//lint:ignore U1000
func binIntervalIntervalAddOp(left *Interval, right *Interval, result *Interval) {
	panic("usp")
}

//lint:ignore U1000
func binIntervalDateAddOp(left *Interval, right *Date, result *Date) {
	panic("usp")
}

//lint:ignore U1000
func binFloat32Float32AddOp(left *float32, right *float32, result *float32) {
	*result = *left + *right
}

//lint:ignore U1000
func binDecimalDecimalAddOp(left *Decimal, right *Decimal, result *Decimal) {
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
func binDateInterSubOp(left *Date, right *Interval, result *Date) {
	res := left.subInterval(right)
	*result = res
}

//lint:ignore U1000
func binFloat32Float32SubOp(left *float32, right *float32, result *float32) {
	*result = *left - *right
}

//lint:ignore U1000
func binDecimalDecimalSubOp(left *Decimal, right *Decimal, result *Decimal) {
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
func binDecimalDecimalMulOp(left *Decimal, right *Decimal, result *Decimal) {
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
func binDecimalDivOp(left, right *Decimal, result *Decimal) {
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
func binStringEqualOp(left, right *String, result *bool) {
	*result = left.equal(right)
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
func binStringLessOp(left, right *String, result *bool) {
	*result = left.less(right)
}

// like
//
//lint:ignore U1000
func binStringLikeOp(left, right *String, result *bool) {
	*result = WildcardMatch(right, left)
}

// extract
//
//lint:ignore U1000
func binStringInt32ExtractOp(left *String, right *Date, result *int32) {
	if left.String() == "year" {
		*result = right._year
	} else {
		panic("usp")
	}
}

func binaryExecSwitch[T any, S any, R any](
	left, right, result *Vector,
	count int,
	fun BinaryFunc[T, S, R],
	wrapper BinaryWrapper[T, S, R],
) {
	if left.phyFormat().isConst() && right.phyFormat().isConst() {
		binaryExecConst[T, S, R](left, right, result, count, fun, wrapper)
	} else if left.phyFormat().isFlat() && right.phyFormat().isConst() {
		binaryExecFlat[T, S, R](left, right, result, count, fun, wrapper, false, true)
	} else if left.phyFormat().isConst() && right.phyFormat().isFlat() {
		binaryExecFlat[T, S, R](left, right, result, count, fun, wrapper, true, false)
	} else if left.phyFormat().isFlat() && right.phyFormat().isFlat() {
		binaryExecFlat[T, S, R](left, right, result, count, fun, wrapper, false, false)
	} else {
		binaryExecGeneric[T, S, R](left, right, result, count, fun, wrapper)
	}
}
func binaryExecConst[T any, S any, R any](
	left, right, result *Vector,
	count int,
	fun BinaryFunc[T, S, R],
	wrapper BinaryWrapper[T, S, R],
) {
	result.setPhyFormat(PF_CONST)
	if isNullInPhyFormatConst(left) ||
		isNullInPhyFormatConst(right) {
		setNullInPhyFormatConst(result, true)
		return
	}
	lSlice := getSliceInPhyFormatConst[T](left)
	rSlice := getSliceInPhyFormatConst[S](right)
	resSlice := getSliceInPhyFormatConst[R](result)

	wrapper.operation(&lSlice[0], &rSlice[0], &resSlice[0], getMaskInPhyFormatConst(result), 0, fun)
}

func binaryExecFlat[T any, S any, R any](
	left, right, result *Vector,
	count int,
	fun BinaryFunc[T, S, R],
	wrapper BinaryWrapper[T, S, R],
	lconst, rconst bool,
) {
	lSlice := getSliceInPhyFormatFlat[T](left)
	rSlice := getSliceInPhyFormatFlat[S](right)
	if lconst && isNullInPhyFormatConst(left) ||
		rconst && isNullInPhyFormatConst(right) {
		result.setPhyFormat(PF_CONST)
		setNullInPhyFormatConst(result, true)
		return
	}

	result.setPhyFormat(PF_FLAT)
	resSlice := getSliceInPhyFormatFlat[R](result)
	resMask := getMaskInPhyFormatFlat(result)
	if lconst {
		if wrapper.addsNulls() {
			resMask.copyFrom(getMaskInPhyFormatFlat(right), count)
		} else {
			setMaskInPhyFormatFlat(result, getMaskInPhyFormatFlat(right))
		}
	} else if rconst {
		if wrapper.addsNulls() {
			resMask.copyFrom(getMaskInPhyFormatFlat(left), count)
		} else {
			setMaskInPhyFormatFlat(result, getMaskInPhyFormatFlat(left))
		}
	} else {
		if wrapper.addsNulls() {
			resMask.copyFrom(getMaskInPhyFormatFlat(left), count)
			if resMask.AllValid() {
				resMask.copyFrom(getMaskInPhyFormatFlat(right), count)
			} else {
				resMask.combine(getMaskInPhyFormatFlat(right), count)
			}
		} else {
			setMaskInPhyFormatFlat(result, getMaskInPhyFormatFlat(left))
			resMask.combine(getMaskInPhyFormatFlat(right), count)
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
	mask *Bitmap,
	fun BinaryFunc[T, S, R],
	wrapper BinaryWrapper[T, S, R],
	lconst, rconst bool,
) {
	if !mask.AllValid() {
		baseIdx := 0
		eCnt := entryCount(count)
		for i := 0; i < eCnt; i++ {
			ent := mask.getEntry(uint64(i))
			next := min(baseIdx+8, count)
			if AllValidInEntry(ent) {
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
			} else if NoneValidInEntry(ent) {
				baseIdx = next
				continue
			} else {
				start := baseIdx
				for ; baseIdx < next; baseIdx++ {
					if rowIsValidInEntry(ent, uint64(baseIdx-start)) {
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
	left, right, result *Vector,
	count int,
	fun BinaryFunc[T, S, R],
	wrapper BinaryWrapper[T, S, R],
) {
	var ldata, rdata UnifiedFormat
	left.toUnifiedFormat(count, &ldata)
	right.toUnifiedFormat(count, &rdata)

	lSlice := getSliceInPhyFormatUnifiedFormat[T](&ldata)
	rSlice := getSliceInPhyFormatUnifiedFormat[S](&rdata)
	result.setPhyFormat(PF_FLAT)
	resSlice := getSliceInPhyFormatFlat[R](result)
	binaryExecGenericLoop[T, S, R](
		lSlice,
		rSlice,
		resSlice,
		ldata._sel,
		rdata._sel,
		count,
		ldata._mask,
		rdata._mask,
		result._mask,
		fun,
		wrapper,
	)
}

func binaryExecGenericLoop[T any, S any, R any](
	ldata []T, rdata []S,
	resData []R,
	lsel *SelectVector,
	rsel *SelectVector,
	count int,
	lmask *Bitmap,
	rmask *Bitmap,
	resMask *Bitmap,
	fun BinaryFunc[T, S, R],
	wrapper BinaryWrapper[T, S, R],
) {
	if !lmask.AllValid() || !rmask.AllValid() {
		for i := 0; i < count; i++ {
			lidx := lsel.getIndex(i)
			ridx := rsel.getIndex(i)
			if lmask.rowIsValid(uint64(lidx)) && rmask.rowIsValid(uint64(ridx)) {
				wrapper.operation(&ldata[lidx], &rdata[ridx], &resData[i], resMask, i, fun)
			} else {
				resMask.setInvalid(uint64(i))
			}
		}
	} else {
		for i := 0; i < count; i++ {
			lidx := lsel.getIndex(i)
			ridx := rsel.getIndex(i)
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
	temp := func(input *Chunk, state *ExprState, result *Vector) {
		binaryExecSwitch[T, S, R](
			input._data[0],
			input._data[1],
			result,
			input.card(),
			nil, wrapper)
	}
	return temp
}
