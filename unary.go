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

package main

type unaryOp[T any, R any] interface {
	operation(input *T, result *R)
}

type unaryFunc[T any, R any] interface {
	fun(input *T, result *R)
}

type unaryWrapper[T any, R any] interface {
	operation(input *T, result *R, mask *Bitmap, idx int, op unaryOp[T, R], fun unaryFunc[T, R])
}

func unaryGenericExec[T any, R any](
	input, result *Vector,
	count int,
	addNulls bool,
	op unaryOp[T, R],
	fun unaryFunc[T, R],
	wrapper unaryWrapper[T, R],
) {
	unaryExecStandard[T, R](
		input,
		result,
		count,
		addNulls,
		op,
		fun,
		wrapper,
	)
}

func unaryExecStandard[T any, R any](
	input, result *Vector,
	count int,
	addNulls bool,
	op unaryOp[T, R],
	fun unaryFunc[T, R],
	wrapper unaryWrapper[T, R],
) {
	if input.phyFormat().isConst() {
		result.setPhyFormat(PF_CONST)
		lslice := getSliceInPhyFormatConst[T](input)
		resSlice := getSliceInPhyFormatConst[R](result)
		if isNullInPhyFormatConst(input) {
			setNullInPhyFormatConst(result, true)
		} else {
			setNullInPhyFormatConst(result, false)
			wrapper.operation(&lslice[0], &resSlice[0], getMaskInPhyFormatConst(result), 0, op, fun)
		}
	} else if input.phyFormat().isFlat() {
		result.setPhyFormat(PF_FLAT)
		lslice := getSliceInPhyFormatConst[T](input)
		resSlice := getSliceInPhyFormatConst[R](result)
		unaryExecFlat[T, R](
			lslice,
			resSlice,
			count,
			getMaskInPhyFormatFlat(input),
			getMaskInPhyFormatFlat(result),
			addNulls,
			op,
			fun,
			wrapper,
		)
	} else {
		var uv UnifiedFormat
		input.toUnifiedFormat(count, &uv)
		result.setPhyFormat(PF_FLAT)
		inputSlice := getSliceInPhyFormatUnifiedFormat[T](&uv)
		resSlice := getSliceInPhyFormatFlat[R](result)
		unaryExecLoop[T, R](
			inputSlice,
			resSlice,
			count,
			uv._sel,
			uv._mask,
			getMaskInPhyFormatFlat(result),
			addNulls,
			op,
			fun,
			wrapper,
		)
	}
}

func unaryExecFlat[T any, R any](
	input []T,
	result []R,
	count int,
	mask *Bitmap,
	resMask *Bitmap,
	addNulls bool,
	op unaryOp[T, R],
	fun unaryFunc[T, R],
	wrapper unaryWrapper[T, R],
) {
	if !mask.AllValid() {
		if !addNulls {
			resMask.shareWith(mask)
		} else {
			resMask.copyFrom(mask, count)
		}
		baseIdx := 0
		eCnt := entryCount(count)
		for eIdx := 0; eIdx < eCnt; eIdx++ {
			ent := mask.getEntry(uint64(eIdx))
			next := min(baseIdx+8, count)
			if AllValidInEntry(ent) {
				for ; baseIdx < next; baseIdx++ {
					wrapper.operation(
						&input[baseIdx],
						&result[baseIdx],
						resMask,
						baseIdx,
						op,
						fun)
				}
			} else if NoneValidInEntry(ent) {
				baseIdx = next
				continue
			} else {
				start := baseIdx
				for ; baseIdx < next; baseIdx++ {
					if rowIsValidInEntry(ent, uint64(baseIdx-start)) {
						wrapper.operation(
							&input[baseIdx],
							&result[baseIdx],
							resMask,
							baseIdx,
							op,
							fun,
						)
					}
				}
			}
		}
	} else {
		for i := 0; i < count; i++ {
			wrapper.operation(
				&input[i],
				&result[i],
				resMask,
				i,
				op,
				fun,
			)
		}
	}
}

func unaryExecLoop[T any, R any](
	input []T,
	result []R,
	count int,
	sel *SelectVector,
	mask *Bitmap,
	resMask *Bitmap,
	addNulls bool,
	op unaryOp[T, R],
	fun unaryFunc[T, R],
	wrapper unaryWrapper[T, R],
) {
	if !mask.AllValid() {
		for i := 0; i < count; i++ {
			idx := sel.getIndex(i)
			if mask.rowIsValidUnsafe(uint64(idx)) {
				wrapper.operation(
					&input[idx],
					&result[i],
					resMask,
					i,
					op,
					fun,
				)
			} else {
				resMask.setInvalid(uint64(i))
			}
		}
	} else {
		for i := 0; i < count; i++ {
			idx := sel.getIndex(i)
			wrapper.operation(
				&input[idx],
				&result[i],
				resMask,
				i,
				op,
				fun,
			)
		}
	}
}
