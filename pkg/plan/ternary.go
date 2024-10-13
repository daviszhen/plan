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
	"math"
	"unsafe"
)

type ternaryOp[A any, B any, C any, R any] interface {
	operation(*A, *B, *C, *R)
}

type ternaryFunc[A any, B any, C any, R any] interface {
	fun(*A, *B, *C, *R)
}

//lint:ignore U1000
type substringFunc struct {
}

const (
	upperLimit = int64(math.MaxUint32)
	lowerLimit = -int64(math.MaxUint32) - 1
)

func isValidRange(slen int64, offset, length int64) bool {
	if slen > upperLimit {
		return false
	}
	if offset < lowerLimit || offset > upperLimit {
		return false
	}

	if length < lowerLimit || length > upperLimit {
		return false
	}
	return true
}

func substringStartEnd(
	slen int64,
	offset int64,
	length int64,
	start *int64,
	end *int64,
) bool {
	if length == 0 {
		return false
	}
	if offset > 0 {
		//from start : start----->offset---->
		*start = min(slen, offset-1)
	} else if offset < 0 {
		//from end: <------offset<------end
		*start = max(slen+offset, 0)
	} else {
		//
		*start = 0
		length--
		if length <= 0 {
			return false
		}
	}
	if length > 0 {
		//left -> right.
		*end = min(slen, *start+length)
	} else {
		//right -> left
		*end = *start
		*start = max(0, *start+length)
	}

	if *start == *end {
		return false
	}
	assertFunc(*start < *end)
	return true
}

func sliceString(sdata unsafe.Pointer, offset, length int64, result *String) {
	result._data = cMalloc(int(length))
	result._len = int(length)
	pointerCopy(
		result._data,
		pointerAdd(sdata, int(offset)),
		int(length))
}

func (sub substringFunc) fun(s *String, offset *int64, length *int64, result *String) {
	slen := s.len()
	sdata := s.data()

	if !isValidRange(int64(slen), *offset, *length) {
		panic(fmt.Sprintf("invalid params slength %d offset %d length %d",
			slen, *offset, *length))
	}

	var start, end int64
	if !substringStartEnd(int64(slen), *offset, *length, &start, &end) {
		*result = String{}
		return
	}
	sliceString(sdata, start, end-start, result)
}

type ternaryWrapper[A any, B any, C any, R any] interface {
	operation(*A, *B, *C, *R, *Bitmap, int,
		ternaryOp[A, B, C, R],
		ternaryFunc[A, B, C, R],
	)
}

//lint:ignore U1000
type ternaryLambdaWrapper[A any, B any, C any, R any] struct {
}

func (wrapper ternaryLambdaWrapper[A, B, C, R]) operation(
	a *A, b *B, c *C, res *R, _ *Bitmap, _ int,
	op ternaryOp[A, B, C, R],
	fun ternaryFunc[A, B, C, R],
) {
	fun.fun(a, b, c, res)
}

func ternaryExecGeneric[A any, B any, C any, R any](
	a, b, c, res *Vector,
	count int,
	op ternaryOp[A, B, C, R],
	fun ternaryFunc[A, B, C, R],
	wrapper ternaryWrapper[A, B, C, R],
) {
	if a.phyFormat().isConst() &&
		b.phyFormat().isConst() &&
		c.phyFormat().isConst() {
		res.setPhyFormat(PF_CONST)
		if isNullInPhyFormatConst(a) ||
			isNullInPhyFormatConst(b) ||
			isNullInPhyFormatConst(c) {
			setNullInPhyFormatConst(res, true)
		} else {
			aSlice := getSliceInPhyFormatConst[A](a)
			bSlice := getSliceInPhyFormatConst[B](b)
			cSlice := getSliceInPhyFormatConst[C](c)
			resSlice := getSliceInPhyFormatConst[R](res)
			resMask := getMaskInPhyFormatConst(res)
			wrapper.operation(
				&aSlice[0],
				&bSlice[0],
				&cSlice[0],
				&resSlice[0],
				resMask,
				0,
				op,
				fun,
			)
		}
	} else {
		res.setPhyFormat(PF_FLAT)
		var adata, bdata, cdata UnifiedFormat
		a.toUnifiedFormat(count, &adata)
		b.toUnifiedFormat(count, &bdata)
		c.toUnifiedFormat(count, &cdata)

		aSlice := getSliceInPhyFormatUnifiedFormat[A](&adata)
		bSlice := getSliceInPhyFormatUnifiedFormat[B](&bdata)
		cSlice := getSliceInPhyFormatUnifiedFormat[C](&cdata)
		resSlice := getSliceInPhyFormatFlat[R](res)
		resMask := getMaskInPhyFormatFlat(res)
		ternaryExecLoop[A, B, C, R](
			aSlice,
			bSlice,
			cSlice,
			resSlice,
			count,
			adata._sel,
			bdata._sel,
			cdata._sel,
			adata._mask,
			bdata._mask,
			cdata._mask,
			resMask,
			op,
			fun,
			wrapper,
		)
	}
}

func ternaryExecLoop[A any, B any, C any, R any](
	adata []A, bdata []B, cdata []C,
	resData []R,
	count int,
	asel, bsel, csel *SelectVector,
	amask, bmask, cmask, resMask *Bitmap,
	op ternaryOp[A, B, C, R],
	fun ternaryFunc[A, B, C, R],
	wrapper ternaryWrapper[A, B, C, R],
) {
	if !amask.AllValid() ||
		!bmask.AllValid() ||
		!cmask.AllValid() {
		for i := 0; i < count; i++ {
			aidx := asel.getIndex(i)
			bidx := bsel.getIndex(i)
			cidx := csel.getIndex(i)
			if amask.rowIsValid(uint64(aidx)) &&
				bmask.rowIsValid(uint64(bidx)) &&
				cmask.rowIsValid(uint64(cidx)) {
				wrapper.operation(
					&adata[aidx],
					&bdata[bidx],
					&cdata[cidx],
					&resData[i],
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
			aidx := asel.getIndex(i)
			bidx := bsel.getIndex(i)
			cidx := csel.getIndex(i)
			wrapper.operation(
				&adata[aidx],
				&bdata[bidx],
				&cdata[cidx],
				&resData[i],
				resMask,
				i,
				op,
				fun,
			)
		}
	}
}
