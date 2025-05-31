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

package compute

import (
	"unsafe"

	"github.com/daviszhen/plan/pkg/chunk"
	"github.com/daviszhen/plan/pkg/common"
	"github.com/daviszhen/plan/pkg/util"
)

// AddInPlace left += delta
func AddInPlace(input *chunk.Vector, right int64, cnt int) {
	util.AssertFunc(input.Typ().Id == common.LTID_POINTER)
	if right == 0 {
		return
	}
	switch input.PhyFormat() {
	case chunk.PF_CONST:
		util.AssertFunc(!chunk.IsNullInPhyFormatConst(input))
		data := chunk.GetSliceInPhyFormatConst[unsafe.Pointer](input)
		data[0] = util.PointerAdd(data[0], int(right))
	default:
		util.AssertFunc(input.PhyFormat().IsFlat())
		data := chunk.GetSliceInPhyFormatFlat[unsafe.Pointer](input)
		for i := 0; i < cnt; i++ {
			data[i] = util.PointerAdd(data[i], int(right))
		}
	}
}

func And(left, right, result *chunk.Vector, count int) {

}

type UnaryData struct {
	_tryCastData *VectorTryCastData
}

type VectorTryCastData struct {
	_result       *chunk.Vector
	_errorMsg     *string
	_strict       bool
	_allConverted bool
}

func TryCastLoop[T any, R any](
	src, res *chunk.Vector,
	count int,
	params *CastParams,
	op CastOp[T, R],
) bool {
	tryCastOp := func(input *T, result *R, mask *util.Bitmap, idx int, data *UnaryData) {
		VectorTryCastOperator(input, result, mask, idx, data, op)
	}
	return TemplatedTryCastLoop[T, R](
		src,
		res,
		count,
		params,
		tryCastOp)
}

func TemplatedTryCastLoop[T any, R any](
	src, res *chunk.Vector,
	count int,
	params *CastParams,
	op UnaryOp2[T, R],
) bool {
	input := &VectorTryCastData{
		_result:   res,
		_strict:   params._strict,
		_errorMsg: &params._errorMsg,
	}
	data := &UnaryData{
		_tryCastData: input,
	}
	unaryGenericExec[T, R](
		src, res,
		count,
		data,
		false,
		op)
	return data._tryCastData._allConverted
}
