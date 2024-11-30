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

type SumFunc struct {
}

func (SumFunc) Register(funcList FunctionList) {
	set := NewFunctionSet("sum", AggregateFuncType)

	sumInt := &FunctionV2{
		_name:    "sum",
		_args:    []LType{integer()},
		_retType: integer(),
		_funcTyp: AggregateFuncType,
	}
	sumDec := &FunctionV2{
		_name:    "sum",
		_args:    []LType{decimal(DecimalMaxWidthInt64, 0)},
		_retType: decimal(DecimalMaxWidthInt64, 0),
		_funcTyp: AggregateFuncType,
	}
	set.Add(sumInt)
	set.Add(sumDec)

	funcList.Add("sum", set)
}

type AvgFunc struct {
}

func (AvgFunc) Register(funcList FunctionList) {
	set := NewFunctionSet("avg", AggregateFuncType)

	avgInt := &FunctionV2{
		_name:    "avg",
		_args:    []LType{integer()},
		_retType: double(),
		_funcTyp: AggregateFuncType,
	}
	avgDec := &FunctionV2{
		_name:    "avg",
		_args:    []LType{decimal(DecimalMaxWidthInt64, 0)},
		_retType: decimal(DecimalMaxWidthInt64, 0),
		_funcTyp: AggregateFuncType,
	}
	set.Add(avgInt)
	set.Add(avgDec)

	funcList.Add("avg", set)
}

type CountFunc struct {
}

func (CountFunc) Register(funcList FunctionList) {
	set := NewFunctionSet("count", AggregateFuncType)

	countInt := &FunctionV2{
		_name:    "count",
		_args:    []LType{integer()},
		_retType: integer(),
		_funcTyp: AggregateFuncType,
	}
	countDec := &FunctionV2{
		_name:    "count",
		_args:    []LType{decimal(DecimalMaxWidthInt64, 0)},
		_retType: integer(),
		_funcTyp: AggregateFuncType,
	}
	set.Add(countInt)
	set.Add(countDec)

	funcList.Add("count", set)
}

type MaxFunc struct {
}

func (MaxFunc) Register(funcList FunctionList) {
	set := NewFunctionSet("max", AggregateFuncType)

	maxDec := &FunctionV2{
		_name:    "max",
		_args:    []LType{decimal(DecimalMaxWidthInt64, 0)},
		_retType: decimal(DecimalMaxWidthInt64, 0),
		_funcTyp: AggregateFuncType,
	}
	set.Add(maxDec)

	funcList.Add("max", set)
}
