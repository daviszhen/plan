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

var (
	//+
	//date + interval
	gBinDateIntervalAdd   binDateInterAddOp
	gBinDateIntervalSub   binDateInterSubOp
	gBinFloat32Float32Add binFloat32Float32AddOp
	gBinDecimalDecimalAdd binDecimalDecimalAddOp

	//-
	//float32 - float32
	gBinFloat32Float32SubOp binFloat32Float32SubOp

	//decimal - decimal
	gBinDecimalDecimalSubOp binDecimalDecimalSubOp

	// *
	gBinFloat32Multi binFloat32MultiOp
	gBinFloat64Multi binFloat64MultiOp

	// /
	gBinFloat32Div binFloat32DivOp
	gBinDecimalDiv binDecimalDivOp

	//decimal * decimal
	gBinDecimalDecimalMulOp binDecimalDecimalMulOp

	// =
	gBinInt32Equal  binInt32EqualOp
	gBinStringEqual binStringEqualOp

	// >
	gBinInt32Great   binInt32GreatOp
	gBinFloat32Great binFloat32GreatOp

	//<
	gBinStringLessOp binStringLessOp

	//like
	gBinStringLike binStringLikeOp

	//extract
	gBinStringInt32Extract binStringInt32ExtractOp

	gBinDateIntervalSingleOpWrapper   binarySingleOpWrapper[Date, Interval, Date]
	gBinInt32BoolSingleOpWrapper      binarySingleOpWrapper[int32, int32, bool]
	gBinFloat32Float32SingleOpWrapper binarySingleOpWrapper[float32, float32, float32]
	gBinFloat64Float64SingleOpWrapper binarySingleOpWrapper[float64, float64, float64]
	gBinFloat32BoolSingleOpWrapper    binarySingleOpWrapper[float32, float32, bool]
	gBinDecimalDecimalOpWrapper       binarySingleOpWrapper[Decimal, Decimal, Decimal]
	gBinStringBoolSingleOpWrapper     binarySingleOpWrapper[String, String, bool]
	gBinStringInt32SingleOpWrapper    binarySingleOpWrapper[String, Date, int32]
)

type binaryOp[T any, S any, R any] interface {
	operation(left *T, right *S, result *R)
}

type binaryFunc[T any, S any, R any] interface {
	fun(left *T, right *S, result *R)
}

type binaryWrapper[T any, S any, R any] interface {
	operation(left *T, right *S, result *R, mask *Bitmap, idx int,
		op binaryOp[T, S, R],
		fun binaryFunc[T, S, R])

	addsNulls() bool
}

type binaryLambdaWrapper[T any, S any, R any] struct {
}

func (wrapper binaryLambdaWrapper[T, S, R]) operation(
	left *T, right *S, result *R, mask *Bitmap, idx int,
	op binaryOp[T, S, R],
	fun binaryFunc[T, S, R]) {
	fun.fun(left, right, result)
}

func (wrapper binaryLambdaWrapper[T, S, R]) addsNulls() bool {
	return false
}

type substringFuncWithoutLength struct {
	substringFunc
}

func (sub substringFuncWithoutLength) fun(s *String, offset *int64, result *String) {
	length := int64(math.MaxUint32)
	sub.substringFunc.fun(s, offset, &length, result)
}

type binarySingleOpWrapper[T any, S any, R any] struct {
}

func (b binarySingleOpWrapper[T, S, R]) operation(left *T, right *S, result *R, mask *Bitmap, idx int,
	op binaryOp[T, S, R],
	fun binaryFunc[T, S, R]) {
	op.operation(left, right, result)
}

func (b binarySingleOpWrapper[T, S, R]) addsNulls() bool {
	return false
}

// +
type binDateInterAddOp struct {
}

func (op binDateInterAddOp) operation(left *Date, right *Interval, result *Date) {
	*result = left.addInterval(right)
}

type binFloat32Float32AddOp struct {
}

func (op binFloat32Float32AddOp) operation(left *float32, right *float32, result *float32) {
	*result = *left + *right
}

type binDecimalDecimalAddOp struct {
}

func (op binDecimalDecimalAddOp) operation(left *Decimal, right *Decimal, result *Decimal) {
	d, err := left.Decimal.Add(right.Decimal)
	if err != nil {
		panic(err)
	}
	result.Decimal = d
}

// -

type binDateInterSubOp struct {
}

func (op binDateInterSubOp) operation(left *Date, right *Interval, result *Date) {
	res := left.subInterval(right)
	*result = res
}

type binFloat32Float32SubOp struct {
}

func (op binFloat32Float32SubOp) operation(left *float32, right *float32, result *float32) {
	*result = *left - *right
}

type binDecimalDecimalSubOp struct {
}

func (op binDecimalDecimalSubOp) operation(left *Decimal, right *Decimal, result *Decimal) {
	d, err := left.Sub(right.Decimal)
	if err != nil {
		panic(err)
	}
	result.Decimal = d
}

// *
type binFloat32MultiOp struct{}

func (op binFloat32MultiOp) operation(left, right *float32, result *float32) {
	*result = *left * *right
}

type binFloat64MultiOp struct{}

func (op binFloat64MultiOp) operation(left, right *float64, result *float64) {
	*result = *left * *right
}

// *
type binDecimalDecimalMulOp struct {
}

func (op binDecimalDecimalMulOp) operation(left *Decimal, right *Decimal, result *Decimal) {
	d, err := left.Decimal.Mul(right.Decimal)
	if err != nil {
		panic(err)
	}
	result.Decimal = d

}

// /
type binFloat32DivOp struct{}

func (op binFloat32DivOp) operation(left, right *float32, result *float32) {
	*result = *left / *right
}

type binDecimalDivOp struct{}

func (op binDecimalDivOp) operation(left, right *Decimal, result *Decimal) {
	quo, err := left.Decimal.Quo(right.Decimal)
	if err != nil {
		panic(err)
	}
	result.Decimal = quo
}

// = int32
type binInt32EqualOp struct {
}

func (op binInt32EqualOp) operation(left, right *int32, result *bool) {
	*result = *left == *right
}

// = string
type binStringEqualOp struct {
}

func (op binStringEqualOp) operation(left, right *String, result *bool) {
	*result = left.equal(right)
}

// > int32
type binInt32GreatOp struct {
}

func (op binInt32GreatOp) operation(left, right *int32, result *bool) {
	*result = *left > *right
}

// > float32
type binFloat32GreatOp struct {
}

func (op binFloat32GreatOp) operation(left, right *float32, result *bool) {
	*result = *left > *right
}

// < string
type binStringLessOp struct {
}

func (op binStringLessOp) operation(left, right *String, result *bool) {
	*result = left.less(right)
}

// like
type binStringLikeOp struct {
}

func (op binStringLikeOp) operation(left, right *String, result *bool) {
	*result = WildcardMatch(right, left)
}

// extract
type binStringInt32ExtractOp struct {
}

func (op binStringInt32ExtractOp) operation(left *String, right *Date, result *int32) {
	if left.String() == "year" {
		*result = right._year
	} else {
		panic("usp")
	}
}

func binaryExecSwitch[T any, S any, R any](
	left, right, result *Vector,
	count int,
	op binaryOp[T, S, R],
	fun binaryFunc[T, S, R],
	wrapper binaryWrapper[T, S, R],
) {
	if left.phyFormat().isConst() && right.phyFormat().isConst() {
		binaryExecConst[T, S, R](left, right, result, count, op, fun, wrapper)
	} else if left.phyFormat().isFlat() && right.phyFormat().isConst() {
		binaryExecFlat[T, S, R](left, right, result, count, op, fun, wrapper, false, true)
	} else if left.phyFormat().isConst() && right.phyFormat().isFlat() {
		binaryExecFlat[T, S, R](left, right, result, count, op, fun, wrapper, true, false)
	} else if left.phyFormat().isFlat() && right.phyFormat().isFlat() {
		binaryExecFlat[T, S, R](left, right, result, count, op, fun, wrapper, false, false)
	} else {
		binaryExecGeneric[T, S, R](left, right, result, count, op, fun, wrapper)
	}
}
func binaryExecConst[T any, S any, R any](
	left, right, result *Vector,
	count int,
	op binaryOp[T, S, R],
	fun binaryFunc[T, S, R],
	wrapper binaryWrapper[T, S, R],
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

	wrapper.operation(&lSlice[0], &rSlice[0], &resSlice[0], getMaskInPhyFormatConst(result), 0, op, fun)
}

func binaryExecFlat[T any, S any, R any](
	left, right, result *Vector,
	count int,
	op binaryOp[T, S, R],
	fun binaryFunc[T, S, R],
	wrapper binaryWrapper[T, S, R],
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
		op,
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
	op binaryOp[T, S, R],
	fun binaryFunc[T, S, R],
	wrapper binaryWrapper[T, S, R],
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
					wrapper.operation(&ldata[lidx], &rdata[ridx], &resData[baseIdx], mask, baseIdx, op, fun)
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
						wrapper.operation(&ldata[lidx], &rdata[ridx], &resData[baseIdx], mask, baseIdx, op, fun)
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
			wrapper.operation(&ldata[lidx], &rdata[ridx], &resData[i], mask, i, op, fun)
		}
	}
}

func binaryExecGeneric[T any, S any, R any](
	left, right, result *Vector,
	count int,
	op binaryOp[T, S, R],
	fun binaryFunc[T, S, R],
	wrapper binaryWrapper[T, S, R],
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
		op,
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
	op binaryOp[T, S, R],
	fun binaryFunc[T, S, R],
	wrapper binaryWrapper[T, S, R],
) {
	if !lmask.AllValid() || !rmask.AllValid() {
		for i := 0; i < count; i++ {
			lidx := lsel.getIndex(i)
			ridx := rsel.getIndex(i)
			if lmask.rowIsValid(uint64(lidx)) && rmask.rowIsValid(uint64(ridx)) {
				wrapper.operation(&ldata[lidx], &rdata[ridx], &resData[i], resMask, i, op, fun)
			} else {
				resMask.setInvalid(uint64(i))
			}
		}
	} else {
		for i := 0; i < count; i++ {
			lidx := lsel.getIndex(i)
			ridx := rsel.getIndex(i)
			wrapper.operation(&ldata[lidx], &rdata[ridx], &resData[i], resMask, i, op, fun)
		}
	}
}
