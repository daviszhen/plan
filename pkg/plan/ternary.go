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

	"github.com/daviszhen/plan/pkg/chunk"
	"github.com/daviszhen/plan/pkg/common"
	"github.com/daviszhen/plan/pkg/util"
)

type TernaryOp[A any, B any, C any, R any] func(*A, *B, *C, *R)

type TernaryFunc[A any, B any, C any, R any] func(*A, *B, *C, *R)

type TernaryWrapper[A any, B any, C any, R any] interface {
	operation(*A, *B, *C, *R, *util.Bitmap, int, TernaryFunc[A, B, C, R])
}

//lint:ignore U1000
type TernaryStandardOperatorWrapper[A any, B any, C any, R any] struct {
	op TernaryOp[A, B, C, R]
}

func (wrapper *TernaryStandardOperatorWrapper[A, B, C, R]) operation(
	a *A, b *B, c *C, res *R, _ *util.Bitmap, _ int,
	fun TernaryFunc[A, B, C, R]) {
	wrapper.op(a, b, c, res)
}

//lint:ignore U1000
type TernaryLambdaWrapper[A any, B any, C any, R any] struct {
	fun TernaryFunc[A, B, C, R]
}

func (wrapper TernaryLambdaWrapper[A, B, C, R]) operation(
	a *A, b *B, c *C, res *R, _ *util.Bitmap, _ int,
	fun TernaryFunc[A, B, C, R]) {
	fun(a, b, c, res)
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
	util.AssertFunc(*start < *end)
	return true
}

func sliceString(sdata unsafe.Pointer, offset, length int64, result *common.String) {
	result.Data = util.CMalloc(int(length))
	result.Len = int(length)
	util.PointerCopy(
		result.Data,
		util.PointerAdd(sdata, int(offset)),
		int(length))
}

func substringFunc(s *common.String, offset *int64, length *int64, result *common.String) {
	slen := s.Length()
	sdata := s.DataPtr()

	if !isValidRange(int64(slen), *offset, *length) {
		panic(fmt.Sprintf("invalid params slength %d offset %d length %d",
			slen, *offset, *length))
	}

	var start, end int64
	if !substringStartEnd(int64(slen), *offset, *length, &start, &end) {
		*result = common.String{}
		return
	}
	sliceString(sdata, start, end-start, result)
}

func TernaryFunction[A any, B any, C any, R any](
	op TernaryOp[A, B, C, R],
) ScalarFunc {
	return TernaryExecStandard[A, B, C, R](op)
}

func TernaryExecStandard[A any, B any, C any, R any](
	op TernaryOp[A, B, C, R],
) ScalarFunc {
	wrapper := &TernaryStandardOperatorWrapper[A, B, C, R]{op: op}
	temp := func(input *chunk.Chunk, state *ExprState, result *chunk.Vector) {
		ternaryExecGeneric[A, B, C, R](
			input.Data[0],
			input.Data[1],
			input.Data[2],
			result,
			input.Card(),
			nil,
			wrapper)

	}
	return temp
}

func ternaryExecGeneric[A any, B any, C any, R any](
	a, b, c, res *chunk.Vector,
	count int,
	fun TernaryFunc[A, B, C, R],
	wrapper TernaryWrapper[A, B, C, R],
) {
	if a.PhyFormat().IsConst() &&
		b.PhyFormat().IsConst() &&
		c.PhyFormat().IsConst() {
		res.SetPhyFormat(chunk.PF_CONST)
		if chunk.IsNullInPhyFormatConst(a) ||
			chunk.IsNullInPhyFormatConst(b) ||
			chunk.IsNullInPhyFormatConst(c) {
			chunk.SetNullInPhyFormatConst(res, true)
		} else {
			aSlice := chunk.GetSliceInPhyFormatConst[A](a)
			bSlice := chunk.GetSliceInPhyFormatConst[B](b)
			cSlice := chunk.GetSliceInPhyFormatConst[C](c)
			resSlice := chunk.GetSliceInPhyFormatConst[R](res)
			resMask := chunk.GetMaskInPhyFormatConst(res)
			wrapper.operation(&aSlice[0], &bSlice[0], &cSlice[0], &resSlice[0], resMask, 0, fun)
		}
	} else {
		res.SetPhyFormat(chunk.PF_FLAT)
		var adata, bdata, cdata chunk.UnifiedFormat
		a.ToUnifiedFormat(count, &adata)
		b.ToUnifiedFormat(count, &bdata)
		c.ToUnifiedFormat(count, &cdata)

		aSlice := chunk.GetSliceInPhyFormatUnifiedFormat[A](&adata)
		bSlice := chunk.GetSliceInPhyFormatUnifiedFormat[B](&bdata)
		cSlice := chunk.GetSliceInPhyFormatUnifiedFormat[C](&cdata)
		resSlice := chunk.GetSliceInPhyFormatFlat[R](res)
		resMask := chunk.GetMaskInPhyFormatFlat(res)
		ternaryExecLoop[A, B, C, R](
			aSlice,
			bSlice,
			cSlice,
			resSlice,
			count,
			adata.Sel,
			bdata.Sel,
			cdata.Sel,
			adata.Mask,
			bdata.Mask,
			cdata.Mask,
			resMask,
			fun,
			wrapper,
		)
	}
}

func ternaryExecLoop[A any, B any, C any, R any](
	adata []A, bdata []B, cdata []C,
	resData []R,
	count int,
	asel, bsel, csel *chunk.SelectVector,
	amask, bmask, cmask, resMask *util.Bitmap,
	fun TernaryFunc[A, B, C, R],
	wrapper TernaryWrapper[A, B, C, R],
) {
	if !amask.AllValid() ||
		!bmask.AllValid() ||
		!cmask.AllValid() {
		for i := 0; i < count; i++ {
			aidx := asel.GetIndex(i)
			bidx := bsel.GetIndex(i)
			cidx := csel.GetIndex(i)
			if amask.RowIsValid(uint64(aidx)) &&
				bmask.RowIsValid(uint64(bidx)) &&
				cmask.RowIsValid(uint64(cidx)) {
				wrapper.operation(&adata[aidx], &bdata[bidx], &cdata[cidx], &resData[i], resMask, i, fun)
			} else {
				resMask.SetInvalid(uint64(i))
			}
		}
	} else {
		for i := 0; i < count; i++ {
			aidx := asel.GetIndex(i)
			bidx := bsel.GetIndex(i)
			cidx := csel.GetIndex(i)
			wrapper.operation(&adata[aidx], &bdata[bidx], &cdata[cidx], &resData[i], resMask, i, fun)
		}
	}
}
