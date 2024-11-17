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
)

func ScalarNopFunc(input *Chunk, state *ExprState, result *Vector) {
	assertFunc(input.columnCount() >= 1)
	result.reference(input._data[0])
}

func NopDecimalBind(fun *FunctionV2, args []*Expr) *FunctionData {
	fun._retType = args[0].DataTyp.LTyp
	fun._args[0] = args[0].DataTyp.LTyp
	return nil
}

func BindDecimalAddSubstract(fun *FunctionV2, args []*Expr) *FunctionData {
	panic("usp")
	return nil
}

type AddFunc struct {
}

func (add AddFunc) Func(typ LType) *FunctionV2 {
	assertFunc(typ.isNumeric())
	if typ.id == LTID_DECIMAL {
		return &FunctionV2{
			_name:    "+",
			_args:    []LType{typ},
			_retType: typ,
			_funcTyp: ScalarFunc,
			_scalar:  ScalarNopFunc,
			_bind:    NopDecimalBind,
		}
	} else {
		return &FunctionV2{
			_name:    "+",
			_args:    []LType{typ},
			_retType: typ,
			_funcTyp: ScalarFunc,
			_scalar:  ScalarNopFunc,
		}
	}
}

func (add AddFunc) Func2(lTyp, rTyp LType) *FunctionV2 {
	if lTyp.isNumeric() && lTyp.id == rTyp.id {
		if lTyp.id == LTID_DECIMAL {
			return &FunctionV2{
				_name:    "+",
				_args:    []LType{lTyp, rTyp},
				_retType: lTyp,
				_funcTyp: ScalarFunc,
				_scalar:  nil,
				_bind:    BindDecimalAddSubstract,
			}
		} else if lTyp.isIntegral() && lTyp.id != LTID_HUGEINT {
			return &FunctionV2{
				_name:    "+",
				_args:    []LType{lTyp, rTyp},
				_retType: lTyp,
				_funcTyp: ScalarFunc,
				_scalar:  GetScalarIntegerFunction(lTyp.getInternalType()),
				_bind:    nil,
			}
		} else {
			return &FunctionV2{
				_name:    "+",
				_args:    []LType{lTyp, rTyp},
				_retType: lTyp,
				_funcTyp: ScalarFunc,
				_scalar:  GetScalarBinaryFunction(lTyp.getInternalType()),
				_bind:    nil,
			}
		}
	}
	switch lTyp.id {
	case LTID_DATE:
		panic("usp")
		if rTyp.id == LTID_INTEGER {

		} else if rTyp.id == LTID_INTERVAL {

		} else if rTyp.id == LTID_TIME {

		}
	case LTID_INTEGER:
		panic("usp")
	case LTID_INTERVAL:
		panic("usp")
	case LTID_TIME:
		panic("usp")
	case LTID_TIMESTAMP:
		panic("usp")
	default:
		panic(fmt.Sprintf("no addFunc for %s %s", lTyp, rTyp))
	}
	return nil
}

func (add AddFunc) Register() {

}

func GetScalarIntegerFunction(ptyp PhyType) scalarFunc {
	return nil
}

func GetScalarBinaryFunction(ptyp PhyType) scalarFunc {
	return nil
}
