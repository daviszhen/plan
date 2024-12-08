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
	"github.com/daviszhen/plan/pkg/common"
)

type SumFunc struct {
}

func (SumFunc) Register(funcList FunctionList) {
	set := NewFunctionSet("sum", AggregateFuncType)

	sumInt := GetSumAggr(common.IntegerType().GetInternalType())
	sumInt._name = "sum"

	sumDec := &FunctionV2{
		_name:    "sum",
		_args:    []common.LType{common.DecimalType(common.DecimalMaxWidthInt64, 0)},
		_retType: common.DecimalType(common.DecimalMaxWidthInt64, 0),
		_funcTyp: AggregateFuncType,
		_bind:    BindDecimalSum,
	}
	set.Add(sumInt)
	set.Add(sumDec)

	funcList.Add("sum", set)
}

func BindDecimalSum(fun *FunctionV2, args []*Expr) *FunctionData {
	decTyp := args[0].DataTyp
	*fun = *GetSumAggr(decTyp.GetInternalType())
	fun._name = "sum"
	fun._args[0] = decTyp
	fun._retType = common.DecimalType(common.DecimalMaxWidth, decTyp.Scale)
	return nil
}

type AvgFunc struct {
}

func (AvgFunc) Register(funcList FunctionList) {
	set := NewFunctionSet("avg", AggregateFuncType)

	avgInt := GetAvgAggr(common.DoubleType().GetInternalType(), common.IntegerType().GetInternalType())
	avgInt._name = "avg"

	avgDec := &FunctionV2{
		_name:    "avg",
		_args:    []common.LType{common.DecimalType(common.DecimalMaxWidthInt64, 0)},
		_retType: common.DecimalType(common.DecimalMaxWidthInt64, 0),
		_funcTyp: AggregateFuncType,
		_bind:    BindDecimalAvg,
	}
	set.Add(avgInt)
	set.Add(avgDec)

	funcList.Add("avg", set)
}

func BindDecimalAvg(fun *FunctionV2, args []*Expr) *FunctionData {
	decTyp := args[0].DataTyp
	*fun = *GetAvgAggr(decTyp.GetInternalType(), decTyp.GetInternalType())
	fun._name = "avg"
	fun._args[0] = decTyp
	fun._retType = common.DecimalType(common.DecimalMaxWidth, decTyp.Scale)
	return nil
}

type CountFunc struct {
}

func (CountFunc) Register(funcList FunctionList) {
	set := NewFunctionSet("count", AggregateFuncType)

	countInt := GetCountAggr(
		common.IntegerType().GetInternalType(),
		common.IntegerType().GetInternalType())
	countInt._name = "count"

	countInt64 := GetCountAggr(
		common.BigintType().GetInternalType(),
		common.BigintType().GetInternalType(),
	)
	countInt64._name = "count"

	countVarchar := GetCountAggr(
		common.IntegerType().GetInternalType(),
		common.VarcharType().GetInternalType())
	countVarchar._name = "count"

	set.Add(countInt)
	set.Add(countInt64)
	set.Add(countVarchar)

	funcList.Add("count", set)
}

type MaxFunc struct {
}

func (MaxFunc) Register(funcList FunctionList) {
	set := NewFunctionSet("max", AggregateFuncType)

	maxDec := &FunctionV2{
		_name:    "max",
		_args:    []common.LType{common.DecimalType(common.DecimalMaxWidthInt64, 0)},
		_retType: common.DecimalType(common.DecimalMaxWidthInt64, 0),
		_funcTyp: AggregateFuncType,
		_bind:    BindDecimalMinMax,
	}
	set.Add(maxDec)

	funcList.Add("max", set)
}

func BindDecimalMinMax(fun *FunctionV2, args []*Expr) *FunctionData {
	decTyp := args[0].DataTyp
	name := fun._name
	if name == "max" {
		*fun = *GetMaxAggr(decTyp.GetInternalType(), decTyp.GetInternalType())
	} else if name == "min" {
		*fun = *GetMinAggr(decTyp.GetInternalType(), decTyp.GetInternalType())
	}
	fun._name = name
	fun._args[0] = decTyp
	fun._retType = decTyp
	return nil
}

type MinFunc struct {
}

func (MinFunc) Register(funcList FunctionList) {
	set := NewFunctionSet("min", AggregateFuncType)

	minDec := &FunctionV2{
		_name:    "min",
		_args:    []common.LType{common.DecimalType(common.DecimalMaxWidthInt64, 0)},
		_retType: common.DecimalType(common.DecimalMaxWidthInt64, 0),
		_funcTyp: AggregateFuncType,
		_bind:    BindDecimalMinMax,
	}
	set.Add(minDec)

	funcList.Add("min", set)
}
