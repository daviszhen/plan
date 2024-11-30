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
			_funcTyp: ScalarFuncType,
			_scalar:  ScalarNopFunc,
			_bind:    NopDecimalBind,
		}
	} else {
		return &FunctionV2{
			_name:    "+",
			_args:    []LType{typ},
			_retType: typ,
			_funcTyp: ScalarFuncType,
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
				_funcTyp: ScalarFuncType,
				_scalar:  nil,
				_bind:    BindDecimalAddSubstract,
			}
		} else if lTyp.isIntegral() && lTyp.id != LTID_HUGEINT {
			return &FunctionV2{
				_name:    "+",
				_args:    []LType{lTyp, rTyp},
				_retType: lTyp,
				_funcTyp: ScalarFuncType,
				_scalar:  GetScalarIntegerFunction(lTyp.getInternalType(), "+", true),
				_bind:    nil,
			}
		} else {
			return &FunctionV2{
				_name:    "+",
				_args:    []LType{lTyp, rTyp},
				_retType: lTyp,
				_funcTyp: ScalarFuncType,
				_scalar:  GetScalarBinaryFunction(lTyp.getInternalType(), "+", false),
				_bind:    nil,
			}
		}
	}
	switch lTyp.id {
	case LTID_DATE:
		if rTyp.id == LTID_INTEGER {
			return &FunctionV2{
				_name:    "+",
				_args:    []LType{lTyp, rTyp},
				_retType: dateLTyp(),
				_funcTyp: ScalarFuncType,
				_scalar:  BinaryFunction[Date, int32, Date](binDateInt32AddOp),
			}
		} else if rTyp.id == LTID_INTERVAL {
			return &FunctionV2{
				_name:    "+",
				_args:    []LType{lTyp, rTyp},
				_retType: dateLTyp(),
				_funcTyp: ScalarFuncType,
				_scalar:  BinaryFunction[Date, Interval, Date](binDateInterAddOp),
			}
		} else if rTyp.id == LTID_TIME {
			panic("usp")
		}
	case LTID_INTEGER:
		if rTyp.id == LTID_DATE {
			return &FunctionV2{
				_name:    "+",
				_args:    []LType{lTyp, rTyp},
				_retType: dateLTyp(),
				_funcTyp: ScalarFuncType,
				_scalar:  BinaryFunction[int32, Date, Date](binInt32DateAddOp),
			}
		}
	case LTID_INTERVAL:
		switch rTyp.id {
		case LTID_INTERVAL:
			return &FunctionV2{
				_name:    "+",
				_args:    []LType{lTyp, rTyp},
				_retType: intervalLType(),
				_funcTyp: ScalarFuncType,
				_scalar:  BinaryFunction[Interval, Interval, Interval](binIntervalIntervalAddOp),
			}
		case LTID_DATE:
			return &FunctionV2{
				_name:    "+",
				_args:    []LType{lTyp, rTyp},
				_retType: dateLTyp(),
				_funcTyp: ScalarFuncType,
				_scalar:  BinaryFunction[Interval, Date, Date](binIntervalDateAddOp),
			}
		default:
			panic("usp")
		}
	case LTID_TIME:
		panic("usp")
	case LTID_TIMESTAMP:
		panic("usp")
	default:
		panic(fmt.Sprintf("no addFunc for %s %s", lTyp, rTyp))
	}
	return nil
}

func (add AddFunc) Register(funcList FunctionList) {
	funcs := NewFunctionSet(ET_Add.String(), ScalarFuncType)
	for _, typ := range Numeric() {
		//unary add
		funcs.Add(add.Func(typ))
		//binary add
		funcs.Add(add.Func2(typ, typ))
	}
	//date + integer, integer + date
	funcs.Add(add.Func2(dateLTyp(), integer()))
	funcs.Add(add.Func2(integer(), dateLTyp()))
	//interval + interval
	funcs.Add(add.Func2(intervalLType(), intervalLType()))
	//interval + date|time|timestamp
	//date|time|timestamp + interval
	funcs.Add(add.Func2(dateLTyp(), intervalLType()))
	funcs.Add(add.Func2(intervalLType(), dateLTyp()))

	//funcs.Add(add.Func2(timeLTyp(), intervalLType()))
	//funcs.Add(add.Func2(intervalLType(), timeLTyp()))
	//
	//funcs.Add(add.Func2(timestampLTyp(), intervalLType()))
	//funcs.Add(add.Func2(intervalLType(), timestampLTyp()))
	//
	////time + date, date + time
	//funcs.Add(add.Func2(timeLTyp(), dateLTyp()))
	//funcs.Add(add.Func2(dateLTyp(), timeLTyp()))
	funcList.Add(ET_Add.String(), funcs)
}

type SubFunc struct {
}

func (sub SubFunc) Register(funcList FunctionList) {
	subs := NewFunctionSet(ET_Sub.String(), ScalarFuncType)
	subFloat := &FunctionV2{
		_name:    ET_Sub.String(),
		_args:    []LType{float(), float()},
		_retType: float(),
		_funcTyp: ScalarFuncType,
		_scalar:  BinaryFunction[float32, float32, float32](binFloat32Float32SubOp),
	}

	subDec := &FunctionV2{
		_name:    ET_Sub.String(),
		_args:    []LType{decimal(DecimalMaxWidthInt64, 0), decimal(DecimalMaxWidthInt64, 0)},
		_retType: decimal(DecimalMaxWidthInt64, 0),
		_funcTyp: ScalarFuncType,
		_scalar:  BinaryFunction[Decimal, Decimal, Decimal](binDecimalDecimalSubOp),
	}

	subDate := &FunctionV2{
		_name:    ET_Sub.String(),
		_args:    []LType{dateLTyp(), dateLTyp()},
		_retType: dateLTyp(),
		_funcTyp: ScalarFuncType,
	}

	subs.Add(subFloat)
	subs.Add(subDec)
	subs.Add(subDate)
	funcList.Add(ET_Sub.String(), subs)
}

type LikeFunc struct {
}

func (like LikeFunc) Register(funcList FunctionList) {
	likeFunc := &FunctionV2{
		_name:    ET_Like.String(),
		_args:    []LType{varchar(), varchar()},
		_retType: boolean(),
		_funcTyp: ScalarFuncType,
		_scalar:  BinaryFunction[String, String, bool](binStringLikeOp),
	}
	set := NewFunctionSet(ET_Like.String(), ScalarFuncType)
	set.Add(likeFunc)
	funcList.Add(ET_Like.String(), set)
}

type NotLikeFunc struct {
}

func (like NotLikeFunc) Register(funcList FunctionList) {
	likeFunc := &FunctionV2{
		_name:    ET_NotLike.String(),
		_args:    []LType{varchar(), varchar()},
		_retType: boolean(),
		_funcTyp: ScalarFuncType,
		//_scalar:  BinaryFunction[String, String, bool](binStringLikeOp),
	}
	set := NewFunctionSet(ET_NotLike.String(), ScalarFuncType)
	set.Add(likeFunc)
	funcList.Add(ET_NotLike.String(), set)
}

func GetScalarIntegerFunction(ptyp PhyType, opKind string, checkOverflow bool) ScalarFunc {
	switch opKind {
	case "+":
		return GetScalarIntegerAddFunction(ptyp, checkOverflow)
	case "-":
	case "*":
	}

	return nil
}

func GetScalarBinaryFunction(ptyp PhyType, opKind string, checkOverflow bool) ScalarFunc {
	switch opKind {
	case "+":
		return GetScalarBinaryAddFunction(ptyp, checkOverflow)
	case "-":
	case "*":
	}
	return nil
}

type InFunc struct {
}

func (in InFunc) Register(funcList FunctionList) {
	inInt := &FunctionV2{
		_name:    ET_In.String(),
		_args:    []LType{integer(), integer()},
		_retType: boolean(),
		_funcTyp: ScalarFuncType,
		_scalar:  BinaryFunction[int32, int32, bool](binInt32EqualOp),
	}

	inVarchar := &FunctionV2{
		_name:    ET_In.String(),
		_args:    []LType{varchar(), varchar()},
		_retType: boolean(),
		_funcTyp: ScalarFuncType,
		_scalar:  BinaryFunction[String, String, bool](binStringEqualOp),
	}

	set := NewFunctionSet(ET_In.String(), ScalarFuncType)
	set.Add(inInt)
	set.Add(inVarchar)
	funcList.Add(ET_In.String(), set)
}

type EqualFunc struct {
}

func (equal EqualFunc) Register(funcList FunctionList) {
	set := NewFunctionSet(ET_Equal.String(), ScalarFuncType)

	equalFunc1 := &FunctionV2{
		_name:    ET_Equal.String(),
		_args:    []LType{integer(), integer()},
		_retType: boolean(),
		_funcTyp: ScalarFuncType,
		_scalar:  BinaryFunction[int32, int32, bool](binInt32EqualOp),
	}

	equalStr := &FunctionV2{
		_name:    ET_Equal.String(),
		_args:    []LType{varchar(), varchar()},
		_retType: boolean(),
		_funcTyp: ScalarFuncType,
	}

	equalBool := &FunctionV2{
		_name:    ET_Equal.String(),
		_args:    []LType{boolean(), boolean()},
		_retType: boolean(),
		_funcTyp: ScalarFuncType,
		_scalar:  BinaryFunction[bool, bool, bool](binBoolEqualOp),
	}

	set.Add(equalFunc1)
	set.Add(equalStr)
	set.Add(equalBool)

	funcList.Add(ET_Equal.String(), set)
}

type NotEqualFunc struct {
}

func (equal NotEqualFunc) Register(funcList FunctionList) {
	set := NewFunctionSet(ET_NotEqual.String(), ScalarFuncType)

	notEqualFunc1 := &FunctionV2{
		_name:    ET_NotEqual.String(),
		_args:    []LType{integer(), integer()},
		_retType: boolean(),
		_funcTyp: ScalarFuncType,
	}

	notEqualStr := &FunctionV2{
		_name:    ET_NotEqual.String(),
		_args:    []LType{varchar(), varchar()},
		_retType: boolean(),
		_funcTyp: ScalarFuncType,
	}

	set.Add(notEqualFunc1)
	set.Add(notEqualStr)

	funcList.Add(ET_NotEqual.String(), set)
}

type BoolFunc struct {
}

func (BoolFunc) Register(funcList FunctionList) {
	set1 := NewFunctionSet(ET_And.String(), ScalarFuncType)
	andFunc := &FunctionV2{
		_name:    ET_And.String(),
		_args:    []LType{boolean(), boolean()},
		_retType: boolean(),
		_funcTyp: ScalarFuncType,
		_scalar:  nil,
	}
	set1.Add(andFunc)

	set2 := NewFunctionSet(ET_Or.String(), ScalarFuncType)
	orFunc := &FunctionV2{
		_name:    ET_Or.String(),
		_args:    []LType{boolean(), boolean()},
		_retType: boolean(),
		_funcTyp: ScalarFuncType,
		_scalar:  nil,
	}
	set2.Add(orFunc)

	set3 := NewFunctionSet(ET_Not.String(), ScalarFuncType)
	notFunc := &FunctionV2{
		_name:    ET_And.String(),
		_args:    []LType{boolean(), boolean()},
		_retType: boolean(),
		_funcTyp: ScalarFuncType,
		_scalar:  nil,
	}
	set3.Add(notFunc)

	funcList.Add(ET_And.String(), set1)
	funcList.Add(ET_Or.String(), set2)
	funcList.Add(ET_Not.String(), set3)
}

type Greater struct {
}

func (Greater) Register(funcList FunctionList) {
	set := NewFunctionSet(ET_Greater.String(), ScalarFuncType)
	gtInteger := &FunctionV2{
		_name:    ET_Greater.String(),
		_args:    []LType{float(), float()},
		_retType: boolean(),
		_funcTyp: ScalarFuncType,
		_scalar:  BinaryFunction[float32, float32, bool](binFloat32GreatOp),
	}

	set.Add(gtInteger)

	funcList.Add(ET_Greater.String(), set)
}

type GreaterThan struct {
}

func (GreaterThan) Register(funcList FunctionList) {
	set := NewFunctionSet(ET_GreaterEqual.String(), ScalarFuncType)
	gtInteger := &FunctionV2{
		_name:    ET_GreaterEqual.String(),
		_args:    []LType{integer(), integer()},
		_retType: boolean(),
		_funcTyp: ScalarFuncType,
		_scalar:  nil,
	}
	gtDate := &FunctionV2{
		_name:    ET_GreaterEqual.String(),
		_args:    []LType{dateLTyp(), dateLTyp()},
		_retType: boolean(),
		_funcTyp: ScalarFuncType,
		_scalar:  nil,
	}

	set.Add(gtInteger)
	set.Add(gtDate)

	funcList.Add(ET_GreaterEqual.String(), set)
}

type DateAdd struct {
}

func (DateAdd) Register(funcList FunctionList) {
	set := NewFunctionSet(ET_DateAdd.String(), ScalarFuncType)
	f := &FunctionV2{
		_name:    ET_DateAdd.String(),
		_args:    []LType{dateLTyp(), intervalLType()},
		_retType: dateLTyp(),
		_funcTyp: ScalarFuncType,
		_scalar:  BinaryFunction[Date, Interval, Date](binDateInterAddOp),
	}

	set.Add(f)

	funcList.Add(ET_DateAdd.String(), set)
}

type LessFunc struct {
}

func (LessFunc) Register(funcList FunctionList) {
	set := NewFunctionSet(ET_Less.String(), ScalarFuncType)
	l := &FunctionV2{
		_name:    ET_Less.String(),
		_args:    []LType{dateLTyp(), dateLTyp()},
		_retType: boolean(),
		_funcTyp: ScalarFuncType,
	}

	lInt := &FunctionV2{
		_name:    ET_Less.String(),
		_args:    []LType{integer(), integer()},
		_retType: boolean(),
		_funcTyp: ScalarFuncType,
	}

	lFloat := &FunctionV2{
		_name:    ET_Less.String(),
		_args:    []LType{float(), float()},
		_retType: boolean(),
		_funcTyp: ScalarFuncType,
	}

	lDouble := &FunctionV2{
		_name:    ET_Less.String(),
		_args:    []LType{double(), double()},
		_retType: boolean(),
		_funcTyp: ScalarFuncType,
	}

	set.Add(l)
	set.Add(lInt)
	set.Add(lFloat)
	set.Add(lDouble)
	funcList.Add(ET_Less.String(), set)
}

type LessEqualFunc struct {
}

func (LessEqualFunc) Register(funcList FunctionList) {
	set := NewFunctionSet(ET_LessEqual.String(), ScalarFuncType)
	leDate := &FunctionV2{
		_name:    ET_LessEqual.String(),
		_args:    []LType{dateLTyp(), dateLTyp()},
		_retType: boolean(),
		_funcTyp: ScalarFuncType,
	}
	leInt := &FunctionV2{
		_name:    ET_LessEqual.String(),
		_args:    []LType{integer(), integer()},
		_retType: boolean(),
		_funcTyp: ScalarFuncType,
	}
	leFloat := &FunctionV2{
		_name:    ET_LessEqual.String(),
		_args:    []LType{float(), float()},
		_retType: boolean(),
		_funcTyp: ScalarFuncType,
	}
	set.Add(leDate)
	set.Add(leInt)
	set.Add(leFloat)
	funcList.Add(ET_LessEqual.String(), set)
}

type MultiplyFunc struct {
}

func (MultiplyFunc) Register(funcList FunctionList) {
	set := NewFunctionSet(ET_Mul.String(), ScalarFuncType)

	mulFloat := &FunctionV2{
		_name:    ET_Mul.String(),
		_args:    []LType{float(), float()},
		_retType: float(),
		_funcTyp: ScalarFuncType,
		_scalar:  BinaryFunction[float32, float32, float32](binFloat32MultiOp),
	}

	mulDouble := &FunctionV2{
		_name:    ET_Mul.String(),
		_args:    []LType{double(), double()},
		_retType: double(),
		_funcTyp: ScalarFuncType,
		_scalar:  BinaryFunction[float64, float64, float64](binFloat64MultiOp),
	}

	mulDec := &FunctionV2{
		_name:    ET_Mul.String(),
		_args:    []LType{decimal(DecimalMaxWidthInt64, 0), decimal(DecimalMaxWidthInt64, 0)},
		_retType: decimal(DecimalMaxWidthInt64, 0),
		_funcTyp: ScalarFuncType,
		_scalar:  BinaryFunction[Decimal, Decimal, Decimal](binDecimalDecimalMulOp),
	}

	set.Add(mulFloat)
	set.Add(mulDouble)
	set.Add(mulDec)

	funcList.Add(ET_Mul.String(), set)
}

type DevideFunc struct {
}

func (DevideFunc) Register(funcList FunctionList) {
	set := NewFunctionSet(ET_Div.String(), ScalarFuncType)

	divFloat := &FunctionV2{
		_name:    ET_Div.String(),
		_args:    []LType{float(), float()},
		_retType: float(),
		_funcTyp: ScalarFuncType,
		_scalar:  BinaryFunction[float32, float32, float32](binFloat32DivOp),
	}

	divDec := &FunctionV2{
		_name:    ET_Div.String(),
		_args:    []LType{decimal(DecimalMaxWidthInt64, 0), decimal(DecimalMaxWidthInt64, 0)},
		_retType: decimal(DecimalMaxWidthInt64, 0),
		_funcTyp: ScalarFuncType,
		_scalar:  BinaryFunction[Decimal, Decimal, Decimal](binDecimalDivOp),
	}

	set.Add(divFloat)
	set.Add(divDec)

	funcList.Add(ET_Div.String(), set)
}

type CaseFunc struct {
}

func (CaseFunc) Register(funcList FunctionList) {
	set := NewFunctionSet(ET_Case.String(), ScalarFuncType)

	caseDec := &FunctionV2{
		_name: ET_Case.String(),
		_args: []LType{
			decimal(DecimalMaxWidthInt64, 0),
			boolean(),
			decimal(DecimalMaxWidthInt64, 0)},
		_retType: decimal(DecimalMaxWidthInt64, 0),
		_funcTyp: ScalarFuncType,
	}

	divInt := &FunctionV2{
		_name:    ET_Case.String(),
		_args:    []LType{integer(), boolean(), integer()},
		_retType: integer(),
		_funcTyp: ScalarFuncType,
	}

	set.Add(caseDec)
	set.Add(divInt)

	funcList.Add(ET_Case.String(), set)
}
