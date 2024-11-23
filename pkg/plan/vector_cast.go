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

type UnaryData struct {
	_tryCastData *VectorTryCastData
}

type VectorTryCastData struct {
	_result       *Vector
	_errorMsg     *string
	_strict       bool
	_allConverted bool
}

func TryCastLoop[T any, R any](
	src, res *Vector,
	count int,
	params *CastParams,
	op CastOp[T, R],
) bool {
	tryCastOp := &VectorTryCastOperator[T, R]{
		op: op,
	}
	return TemplatedTryCastLoop[T, R](
		src,
		res,
		count,
		params,
		tryCastOp)
}

func TemplatedTryCastLoop[T any, R any](
	src, res *Vector,
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
