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
	maxWidth := 0
	maxScale := 0
	maxWidthOverScale := 0
	bindData := &FunctionData{
		_funDataTyp: DecimalBindData,
	}

	for _, arg := range args {
		if arg.DataTyp.LTyp.id == LTID_UNKNOWN {
			continue
		}
		maxWidth = max(maxWidth, arg.DataTyp.LTyp.width)
		maxScale = max(maxScale, arg.DataTyp.LTyp.scale)
		maxWidthOverScale = max(maxWidthOverScale, arg.DataTyp.LTyp.width-arg.DataTyp.LTyp.scale)
	}
	assertFunc(maxWidth > 0)
	//for add/sub, plus 1 extra on the width
	requireWidth := max(maxScale+maxWidthOverScale, maxWidth) + 1
	if requireWidth > DecimalMaxWidthInt64 &&
		maxWidth <= DecimalMaxWidthInt64 {
		bindData._checkOverflow = true
		requireWidth = DecimalMaxWidthInt64
	}

	if requireWidth > DecimalMaxWidth {
		bindData._checkOverflow = true
		requireWidth = DecimalMaxWidth
	}
	resTyp := decimal(requireWidth, maxScale)
	//cast all input types
	for i, arg := range args {
		scale := arg.DataTyp.LTyp.scale
		if scale == resTyp.scale &&
			arg.DataTyp.LTyp.getInternalType() == resTyp.getInternalType() {
			fun._args[i] = arg.DataTyp.LTyp
		} else {
			fun._args[i] = resTyp
		}
	}
	fun._retType = resTyp
	if bindData._checkOverflow {
		fun._scalar = GetScalarBinaryFunction(resTyp.getInternalType(), fun._name, true)
	} else {
		fun._scalar = GetScalarBinaryFunction(resTyp.getInternalType(), fun._name, false)
	}
	return bindData
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

func negateInterval(input *Interval, result *Interval) {
	negateInt32(&input._months, &result._months)
	negateInt32(&input._days, &result._days)
	negateInt32(&input._year, &result._year)
}

func DecimalNegateBind(fun *FunctionV2, args []*Expr) *FunctionData {
	decTyp := args[0].DataTyp.LTyp
	fun._scalar = GetScalarUnaryFunction(decTyp, "-")
	fun._args[0] = decTyp
	fun._retType = decTyp
	return nil
}

func (sub SubFunc) Func(typ LType) *FunctionV2 {
	if typ.id == LTID_INTERVAL {
		return &FunctionV2{
			_name:    "-",
			_args:    []LType{typ},
			_retType: typ,
			_funcTyp: ScalarFuncType,
			_scalar:  UnaryFunction[Interval, Interval](negateInterval),
		}
	} else if typ.id == LTID_DECIMAL {
		return &FunctionV2{
			_name:    "-",
			_args:    []LType{typ},
			_retType: typ,
			_funcTyp: ScalarFuncType,
			_bind:    DecimalNegateBind,
		}
	} else {
		assertFunc(typ.isNumeric())
		return &FunctionV2{
			_name:    "-",
			_args:    []LType{typ},
			_retType: typ,
			_funcTyp: ScalarFuncType,
			_scalar:  GetScalarUnaryFunction(typ, "-"),
		}
	}
}

func (sub SubFunc) Func2(lTyp, rTyp LType) *FunctionV2 {
	if lTyp.isNumeric() && lTyp.id == rTyp.id {
		if lTyp.id == LTID_DECIMAL {
			return &FunctionV2{
				_name:    "-",
				_args:    []LType{lTyp, rTyp},
				_retType: lTyp,
				_funcTyp: ScalarFuncType,
				_scalar:  nil,
				_bind:    BindDecimalAddSubstract,
			}
		} else if lTyp.isIntegral() && lTyp.id != LTID_HUGEINT {
			return &FunctionV2{
				_name:    "-",
				_args:    []LType{lTyp, rTyp},
				_retType: lTyp,
				_funcTyp: ScalarFuncType,
				_scalar:  GetScalarIntegerFunction(lTyp.getInternalType(), "-", true),
				_bind:    nil,
			}
		} else {
			return &FunctionV2{
				_name:    "-",
				_args:    []LType{lTyp, rTyp},
				_retType: lTyp,
				_funcTyp: ScalarFuncType,
				_scalar:  GetScalarBinaryFunction(lTyp.getInternalType(), "-", false),
				_bind:    nil,
			}
		}
	}
	switch lTyp.id {
	case LTID_DATE:
		if rTyp.id == LTID_DATE {
			return &FunctionV2{
				_name:    "-",
				_args:    []LType{lTyp, rTyp},
				_retType: bigint(),
				_funcTyp: ScalarFuncType,
				_scalar:  BinaryFunction[Date, Date, int64](nil),
			}
		} else if rTyp.id == LTID_INTEGER {
			return &FunctionV2{
				_name:    "-",
				_args:    []LType{lTyp, rTyp},
				_retType: dateLTyp(),
				_funcTyp: ScalarFuncType,
				_scalar:  BinaryFunction[Date, int32, Date](nil),
			}
		} else if rTyp.id == LTID_INTERVAL {
			return &FunctionV2{
				_name:    "-",
				_args:    []LType{lTyp, rTyp},
				_retType: dateLTyp(),
				_funcTyp: ScalarFuncType,
				_scalar:  BinaryFunction[Date, Interval, Date](nil),
			}
		}
	case LTID_INTERVAL:
		switch rTyp.id {
		case LTID_INTERVAL:
			return &FunctionV2{
				_name:    "-",
				_args:    []LType{lTyp, rTyp},
				_retType: intervalLType(),
				_funcTyp: ScalarFuncType,
				_scalar:  BinaryFunction[Interval, Interval, Interval](nil),
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

func (sub SubFunc) Register(funcList FunctionList) {
	subs := NewFunctionSet(ET_Sub.String(), ScalarFuncType)

	for _, typ := range Numeric() {
		//unary
		subs.Add(sub.Func(typ))
		//binary
		subs.Add(sub.Func2(typ, typ))
	}
	//date - date
	subs.Add(sub.Func2(dateLTyp(), dateLTyp()))
	//date - integer
	subs.Add(sub.Func2(dateLTyp(), integer()))
	//timestamp - timestamp
	//interval - interval
	subs.Add(sub.Func2(intervalLType(), intervalLType()))
	//date - interval
	subs.Add(sub.Func2(dateLTyp(), intervalLType()))
	//-interval
	subs.Add(sub.Func(intervalLType()))
	funcList.Add(ET_Sub.String(), subs)
}

type MultiplyFunc struct {
}

func (MultiplyFunc) Register(funcList FunctionList) {
	set := NewFunctionSet(ET_Mul.String(), ScalarFuncType)

	for _, typ := range Numeric() {
		if typ.id == LTID_DECIMAL {
			fun := &FunctionV2{
				_name:    "*",
				_args:    []LType{typ, typ},
				_retType: typ,
				_funcTyp: ScalarFuncType,
				_scalar:  nil,
				_bind:    BindDecimalMultiply,
			}
			set.Add(fun)
		} else if typ.isIntegral() && typ.id != LTID_HUGEINT {
			fun := &FunctionV2{
				_name:    "*",
				_args:    []LType{typ, typ},
				_retType: typ,
				_funcTyp: ScalarFuncType,
				_scalar:  GetScalarIntegerFunction(typ.getInternalType(), "*", true),
			}
			set.Add(fun)
		} else {
			fun := &FunctionV2{
				_name:    "*",
				_args:    []LType{typ, typ},
				_retType: typ,
				_funcTyp: ScalarFuncType,
				_scalar:  GetScalarBinaryFunction(typ.getInternalType(), "*", false),
			}
			set.Add(fun)
		}
	}

	funcList.Add(ET_Mul.String(), set)
}

func BindDecimalMultiply(fun *FunctionV2, args []*Expr) *FunctionData {
	bindData := &FunctionData{
		_funDataTyp: DecimalBindData,
	}
	resWidth, resScale := 0, 0
	maxWidth := 0
	for _, arg := range args {
		if arg.DataTyp.LTyp.id == LTID_UNKNOWN {
			continue
		}
		if arg.DataTyp.LTyp.width > maxWidth {
			maxWidth = arg.DataTyp.LTyp.width
		}
		resWidth += arg.DataTyp.LTyp.width
		resScale += arg.DataTyp.LTyp.scale
	}
	assertFunc(maxWidth > 0)
	if resScale > DecimalMaxWidth {
		panic(fmt.Sprintf("scale %d greater than %d", resScale, DecimalMaxWidth))
	}
	if resWidth > DecimalMaxWidthInt64 &&
		maxWidth <= DecimalMaxWidthInt64 &&
		resScale < DecimalMaxWidthInt64 {
		bindData._checkOverflow = true
		resWidth = DecimalMaxWidthInt64
	}
	if resWidth > DecimalMaxWidth {
		bindData._checkOverflow = true
		resWidth = DecimalMaxWidth
	}

	resTyp := decimal(resWidth, resScale)
	for i, arg := range args {
		if arg.DataTyp.LTyp.getInternalType() == resTyp.getInternalType() {
			fun._args[i] = arg.DataTyp.LTyp
		} else {
			fun._args[i] = decimal(resWidth, arg.DataTyp.LTyp.scale)
		}
	}
	fun._retType = resTyp
	if bindData._checkOverflow {
		fun._scalar = GetScalarBinaryFunction(resTyp.getInternalType(), "*", true)
	} else {
		fun._scalar = GetScalarBinaryFunction(resTyp.getInternalType(), "*", false)
	}
	return bindData
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
		return GetScalarIntegerSubFunction(ptyp, checkOverflow)
	case "*":
		return GetScalarIntegerMulFunction(ptyp, checkOverflow)
	}

	return nil
}

func GetScalarIntegerMulFunction(ptyp PhyType, overflow bool) ScalarFunc {
	if overflow {
		return GetScalarIntegerMulFunctionWithOverflow(ptyp)
	} else {
		return GetScalarIntegerMulFunctionWithoutOverflow(ptyp)
	}
}

func GetScalarIntegerMulFunctionWithOverflow(ptyp PhyType) ScalarFunc {
	switch ptyp {
	case INT8:
		return BinaryFunction[int8, int8, int8](mulInt8CheckOf)
	case INT16:
		return BinaryFunction[int16, int16, int16](mulInt16CheckOf)
	case INT32:
		return BinaryFunction[int32, int32, int32](mulInt32CheckOf)
	case INT64:
		return BinaryFunction[int64, int64, int64](mulInt64CheckOf)
	case UINT8:
		return BinaryFunction[uint8, uint8, uint8](mulUint8CheckOf)
	case UINT16:
		return BinaryFunction[uint16, uint16, uint16](mulUint16CheckOf)
	case UINT32:
		return BinaryFunction[uint32, uint32, uint32](mulUint32CheckOf)
	case UINT64:
		return BinaryFunction[uint64, uint64, uint64](mulUint64CheckOf)
	case DECIMAL:
		return BinaryFunction[Decimal, Decimal, Decimal](binDecimalDecimalMulOp)
	default:
		panic("not implement")
	}
	return nil
}

func mulUint64CheckOf(left *uint64, right *uint64, result *uint64) {
	if *left > *right {
		left, right = right, left
	}
	if *left > math.MaxUint32 {
		panic("uint64 * uint64 overflow")
	}
	c := uint32(*right >> 32)
	d := uint32(math.MaxUint32 & *right)
	r := *left * uint64(c)
	s := *left * uint64(d)
	if r > math.MaxUint32 {
		panic("uint64 * uint64 overflow")
	}
	r <<= 32
	if math.MaxUint64-s < r {
		panic("uint64 * uint64 overflow")
	}
	mulUint64(left, right, result)
}

func mulUint32CheckOf(left *uint32, right *uint32, result *uint32) {
	ul := uint64(*left)
	ur := uint64(*right)
	uresult := uint64(0)
	mulUint64(&ul, &ur, &uresult)
	if uresult > math.MaxUint32 {
		panic("uint32 * uint32 overflow")
	}
	*result = uint32(uresult)
}

func mulUint16CheckOf(left *uint16, right *uint16, result *uint16) {
	ul := uint32(*left)
	ur := uint32(*right)
	uresult := uint32(0)
	mulUint32(&ul, &ur, &uresult)
	if uresult > math.MaxUint16 {
		panic("uint16 * uint16 overflow")
	}
	*result = uint16(uresult)
}

func mulUint8CheckOf(left *uint8, right *uint8, result *uint8) {
	ul := uint16(*left)
	ur := uint16(*right)
	uresult := uint16(0)
	mulUint16(&ul, &ur, &uresult)
	if uresult > math.MaxUint8 {
		panic("uint8 * uint8 overflow")
	}
	*result = uint8(uresult)
}

func mulInt64CheckOf(left *int64, right *int64, result *int64) {
	if *left == math.MinInt64 {
		if *right == 0 {
			*result = 0
			return
		}
		if *right == 1 {
			*result = *left
			return
		}
		panic("int64 * int64 overflow")
	}
	if *right == math.MinInt64 {
		if *left == 0 {
			*result = 0
			return
		}
		if *left == 1 {
			*result = *right
			return
		}
		panic("int64 * int64 overflow")
	}
	leftNonNegative := uint64(*left)
	rightNonNegative := uint64(*right)
	leftHighBits := leftNonNegative >> 32
	leftLowBits := leftNonNegative & 0xffffffff
	rightHighBits := rightNonNegative >> 32
	rightLowBits := rightNonNegative & 0xffffffff

	if leftHighBits == 0 {
		if rightHighBits != 0 {
			lowLow := leftLowBits * rightLowBits
			lowHigh := leftLowBits * rightHighBits
			highBits := lowHigh + (lowLow >> 32)
			if (highBits & 0xffffff80000000) != 0 {
				panic("int64 * int64 overflow")
			}
		}
	} else if rightHighBits == 0 {
		lowLow := leftLowBits * rightLowBits
		highLow := leftHighBits * rightLowBits
		highBits := highLow + (lowLow >> 32)
		if (highBits & 0xffffff80000000) != 0 {
			panic("int64 * int64 overflow")
		}
	} else {
		panic("int64 * int64 overflow")
	}
	*result = *left * *right
}

func mulInt32CheckOf(left *int32, right *int32, result *int32) {
	ul := int64(*left)
	ur := int64(*right)
	ures := int64(0)
	mulInt64(&ul, &ur, &ures)
	if ures < math.MinInt32 || ures > math.MaxInt32 {
		panic("int32 * int32 overflow")
	}
	*result = int32(ures)
}

func mulInt16CheckOf(left *int16, right *int16, result *int16) {
	ul := int32(*left)
	ur := int32(*right)
	ures := int32(0)
	mulInt32(&ul, &ur, &ures)
	if ures < math.MinInt16 || ures > math.MaxInt16 {
		panic("int16 * int16 overflow")
	}
	*result = int16(ures)
}

func mulInt8CheckOf(left *int8, right *int8, result *int8) {
	ul := int16(*left)
	ur := int16(*right)
	ures := int16(0)
	mulInt16(&ul, &ur, &ures)
	if ures < math.MinInt8 || ures > math.MaxInt8 {
		panic("int8 * int8 overflow")
	}
	*result = int8(ures)
}

func GetScalarIntegerMulFunctionWithoutOverflow(ptyp PhyType) ScalarFunc {
	switch ptyp {
	case INT8:
		return BinaryFunction[int8, int8, int8](mulInt8)
	case INT16:
		return BinaryFunction[int16, int16, int16](mulInt16)
	case INT32:
		return BinaryFunction[int32, int32, int32](mulInt32)
	case INT64:
		return BinaryFunction[int64, int64, int64](mulInt64)
	case UINT8:
		return BinaryFunction[uint8, uint8, uint8](mulUint8)
	case UINT16:
		return BinaryFunction[uint16, uint16, uint16](mulUint16)
	case UINT32:
		return BinaryFunction[uint32, uint32, uint32](mulUint32)
	case UINT64:
		return BinaryFunction[uint64, uint64, uint64](mulUint64)
	case DECIMAL:
		return BinaryFunction[Decimal, Decimal, Decimal](binDecimalDecimalMulOp)
	default:
		panic("usp")
	}
	return nil
}

func mulUint64(left *uint64, right *uint64, result *uint64) {
	*result = *left * *right
}

func mulUint32(left *uint32, right *uint32, result *uint32) {
	*result = *left * *right
}

func mulUint16(left *uint16, right *uint16, result *uint16) {
	*result = *left * *right
}

func mulUint8(left *uint8, right *uint8, result *uint8) {
	*result = *left * *right
}

func mulInt64(left *int64, right *int64, result *int64) {
	*result = *left * *right
}

func mulInt32(left *int32, right *int32, result *int32) {
	*result = *left * *right
}

func mulInt16(left *int16, right *int16, result *int16) {
	*result = *left * *right
}

func mulInt8(left *int8, right *int8, result *int8) {
	*result = *left * *right
}

func GetScalarIntegerSubFunction(ptyp PhyType, overflow bool) ScalarFunc {
	if overflow {
		return GetScalarIntegerSubFunctionWithOverflow(ptyp)
	} else {
		return GetScalarIntegerSubFunctionWithoutOverflow(ptyp)
	}
}

func GetScalarIntegerSubFunctionWithOverflow(ptyp PhyType) ScalarFunc {
	switch ptyp {
	case INT8:
		return BinaryFunction[int8, int8, int8](subInt8CheckOf)
	case INT16:
		return BinaryFunction[int16, int16, int16](subInt16CheckOf)
	case INT32:
		return BinaryFunction[int32, int32, int32](subInt32CheckOf)
	case INT64:
		return BinaryFunction[int64, int64, int64](subInt64CheckOf)
	case UINT8:
		return BinaryFunction[uint8, uint8, uint8](subUint8CheckOf)
	case UINT16:
		return BinaryFunction[uint16, uint16, uint16](subUint16CheckOf)
	case UINT32:
		return BinaryFunction[uint32, uint32, uint32](subUint32CheckOf)
	case UINT64:
		return BinaryFunction[uint64, uint64, uint64](subUint64CheckOf)
	case DECIMAL:
		return BinaryFunction[Decimal, Decimal, Decimal](binDecimalDecimalSubOp)
	default:
		panic("not implement")
	}
	return nil
}

func subUint64CheckOf(left *uint64, right *uint64, result *uint64) {
	if *right > *left {
		panic("uint64 - uint64 overflow")
	}
	subUint64(left, right, result)
}

func subUint32CheckOf(left *uint32, right *uint32, result *uint32) {
	if *right > *left {
		panic("uint32 - uint32 overflow")
	}
	subUint32(left, right, result)
}

func subUint16CheckOf(left *uint16, right *uint16, result *uint16) {
	if *right > *left {
		panic("uint16 - uint16 overflow")
	}
	subUint16(left, right, result)
}

func subUint8CheckOf(left *uint8, right *uint8, result *uint8) {
	if *right > *left {
		panic("uint8 - uint8 overflow")
	}
	subUint8(left, right, result)
}

func subInt64CheckOf(left *int64, right *int64, result *int64) {
	if *right < 0 {
		if math.MaxInt64+*right < *left {
			panic("int64 - int64 overflow")
		}
	} else {
		if math.MinInt64+*right > *left {
			panic("int64 - int64 overflow")
		}
	}
	*result = *left - *right
}

func subInt32CheckOf(left *int32, right *int32, result *int32) {
	ul := int64(*left)
	ur := int64(*right)
	ures := int64(0)
	subInt64(&ul, &ur, &ures)
	if ures < math.MinInt32 || ures > math.MaxInt32 {
		panic("int32 - int32 overflow")
	}
	*result = int32(ures)
}

func subInt16CheckOf(left *int16, right *int16, result *int16) {
	ul := int32(*left)
	ur := int32(*right)
	ures := int32(0)
	subInt32(&ul, &ur, &ures)
	if ures < math.MinInt16 || ures > math.MaxInt16 {
		panic("int16 - int16 overflow")
	}
	*result = int16(ures)
}

func subInt8CheckOf(left *int8, right *int8, result *int8) {
	ul := int16(*left)
	ur := int16(*right)
	ures := int16(0)
	subInt16(&ul, &ur, &ures)
	if ures < math.MinInt8 || ures > math.MaxInt8 {
		panic("int8 - int8 overflow")
	}
	*result = int8(ures)
}
func GetScalarIntegerSubFunctionWithoutOverflow(ptyp PhyType) ScalarFunc {
	switch ptyp {
	case INT8:
		return BinaryFunction[int8, int8, int8](subInt8)
	case INT16:
		return BinaryFunction[int16, int16, int16](subInt16)
	case INT32:
		return BinaryFunction[int32, int32, int32](subInt32)
	case INT64:
		return BinaryFunction[int64, int64, int64](subInt64)
	case UINT8:
		return BinaryFunction[uint8, uint8, uint8](subUint8)
	case UINT16:
		return BinaryFunction[uint16, uint16, uint16](subUint16)
	case UINT32:
		return BinaryFunction[uint32, uint32, uint32](subUint32)
	case UINT64:
		return BinaryFunction[uint64, uint64, uint64](subUint64)
	case DECIMAL:
		return BinaryFunction[Decimal, Decimal, Decimal](binDecimalDecimalSubOp)
	default:
		panic("usp")
	}
	return nil
}

func subUint64(left *uint64, right *uint64, result *uint64) {
	*result = *left - *right
}

func subUint32(left *uint32, right *uint32, result *uint32) {
	*result = *left - *right
}

func subUint16(left *uint16, right *uint16, result *uint16) {
	*result = *left - *right
}

func subUint8(left *uint8, right *uint8, result *uint8) {
	*result = *left - *right
}

func subInt64(left *int64, right *int64, result *int64) {
	*result = *left - *right
}

func subInt32(left *int32, right *int32, result *int32) {
	*result = *left - *right
}

func subInt16(left *int16, right *int16, result *int16) {
	*result = *left - *right
}

func subInt8(left *int8, right *int8, result *int8) {
	*result = *left - *right
}

func GetScalarBinaryFunction(ptyp PhyType, opKind string, checkOverflow bool) ScalarFunc {
	switch opKind {
	case "+":
		return GetScalarBinaryAddFunction(ptyp, checkOverflow)
	case "-":
		return GetScalarBinarySubFunction(ptyp, checkOverflow)
	case "*":
		return GetScalarBinaryMulFunction(ptyp, checkOverflow)
	}
	return nil
}

func GetScalarBinaryMulFunction(ptyp PhyType, overflow bool) ScalarFunc {
	if overflow {
		return GetScalarBinaryMulFunctionWithOverflow(ptyp)
	} else {
		return GetScalarBinaryMulFunctionWithoutOverflow(ptyp)
	}
}

func GetScalarBinaryMulFunctionWithOverflow(ptyp PhyType) ScalarFunc {
	switch ptyp {
	case INT128:
		return BinaryFunction[Hugeint, Hugeint, Hugeint](mulHugeint)
	case FLOAT:
		return BinaryFunction[float32, float32, float32](mulFloat32)
	case DOUBLE:
		return BinaryFunction[float64, float64, float64](mulFloat64)
	default:
		return GetScalarIntegerMulFunction(ptyp, true)
	}
}

func GetScalarBinaryMulFunctionWithoutOverflow(ptyp PhyType) ScalarFunc {
	switch ptyp {
	case INT128:
		return BinaryFunction[Hugeint, Hugeint, Hugeint](mulHugeint)
	case FLOAT:
		return BinaryFunction[float32, float32, float32](mulFloat32)
	case DOUBLE:
		return BinaryFunction[float64, float64, float64](mulFloat64)
	default:
		return GetScalarIntegerMulFunction(ptyp, false)
	}
}

func mulFloat64(left *float64, right *float64, result *float64) {
	*result = *left * *right
}

func mulFloat32(left *float32, right *float32, result *float32) {
	*result = *left * *right
}

func mulHugeint(left *Hugeint, right *Hugeint, result *Hugeint) {
	panic("usp")
}

func GetScalarBinarySubFunction(ptyp PhyType, overflow bool) ScalarFunc {
	if overflow {
		return GetScalarBinarySubFunctionWithOverflow(ptyp)
	} else {
		return GetScalarBinarySubFunctionWithoutOverflow(ptyp)
	}
}

func GetScalarBinarySubFunctionWithoutOverflow(ptyp PhyType) ScalarFunc {
	switch ptyp {
	case INT128:
		return BinaryFunction[Hugeint, Hugeint, Hugeint](subHugeint)
	case FLOAT:
		return BinaryFunction[float32, float32, float32](subFloat32)
	case DOUBLE:
		return BinaryFunction[float64, float64, float64](subFloat64)
	default:
		return GetScalarIntegerSubFunction(ptyp, false)
	}
}

func GetScalarBinarySubFunctionWithOverflow(ptyp PhyType) ScalarFunc {
	switch ptyp {
	case INT128:
		return BinaryFunction[Hugeint, Hugeint, Hugeint](subHugeint)
	case FLOAT:
		return BinaryFunction[float32, float32, float32](subFloat32)
	case DOUBLE:
		return BinaryFunction[float64, float64, float64](subFloat64)
	default:
		return GetScalarIntegerSubFunction(ptyp, true)
	}
}

func subFloat64(left *float64, right *float64, result *float64) {
	*result = *left - *right
}

func subFloat32(left *float32, right *float32, result *float32) {
	*result = *left - *right
}

func GetScalarUnaryFunction(typ LType, opKind string) ScalarFunc {
	switch opKind {
	case "+":
	case "-":
		return GetScalarUnarySubFunction(typ)
	case "*":
	}
	return nil
}

func negateInt8(input *int8, result *int8) {
	res := *input
	if res == math.MinInt8 {
		panic("-int8 overflow")
	}
	*result = -res
}

func negateInt16(input *int16, result *int16) {
	res := *input
	if res == math.MinInt16 {
		panic("-int16 overflow")
	}
	*result = -res
}

func negateInt32(input *int32, result *int32) {
	res := *input
	if res == math.MinInt32 {
		panic("-int32 overflow")
	}
	*result = -res
}

func negateInt64(input *int64, result *int64) {
	res := *input
	if res == math.MinInt64 {
		panic("-int64 overflow")
	}
	*result = -res
}

func negateUint8(input *uint8, result *uint8) {
	panic("-uint8 overflow")
}

func negateUint16(input *uint16, result *uint16) {
	panic("-uint16 overflow")
}

func negateUint32(input *uint32, result *uint32) {
	panic("-uint32 overflow")
}

func negateUint64(input *uint64, result *uint64) {
	panic("-uint64 overflow")
}

func negateFloat(input *float32, result *float32) {
	*result = -*input
}

func negateDouble(input *float64, result *float64) {
	*result = -*input
}

func GetScalarUnarySubFunction(typ LType) ScalarFunc {
	var fun ScalarFunc
	switch typ.id {
	case LTID_TINYINT:
		fun = UnaryFunction[int8, int8](negateInt8)
	case LTID_SMALLINT:
		fun = UnaryFunction[int16, int16](negateInt16)
	case LTID_INTEGER:
		fun = UnaryFunction[int32, int32](negateInt32)
	case LTID_BIGINT:
		fun = UnaryFunction[int64, int64](negateInt64)
	case LTID_UTINYINT:
		fun = UnaryFunction[uint8, uint8](negateUint8)
	case LTID_USMALLINT:
		fun = UnaryFunction[uint16, uint16](negateUint16)
	case LTID_UINTEGER:
		fun = UnaryFunction[uint32, uint32](negateUint32)
	case LTID_UBIGINT:
		fun = UnaryFunction[uint64, uint64](negateUint64)
	case LTID_HUGEINT:
		fun = UnaryFunction[Hugeint, Hugeint](negateHugeint)
	case LTID_FLOAT:
		fun = UnaryFunction[float32, float32](negateFloat)
	case LTID_DOUBLE:
		fun = UnaryFunction[float64, float64](negateDouble)
	case LTID_DECIMAL:
		fun = UnaryFunction[Decimal, Decimal](negateDecimal)
	}
	if fun == nil {
		panic("usp")
	}
	return fun
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
	gt1 := &FunctionV2{
		_name:    ET_Greater.String(),
		_args:    []LType{float(), float()},
		_retType: boolean(),
		_funcTyp: ScalarFuncType,
		_scalar:  BinaryFunction[float32, float32, bool](binFloat32GreatOp),
	}

	gt2 := &FunctionV2{
		_name: ET_Greater.String(),
		_args: []LType{
			decimal(DecimalMaxWidthInt64, 0),
			decimal(DecimalMaxWidthInt64, 0)},
		_retType: boolean(),
		_funcTyp: ScalarFuncType,
	}

	gt3 := &FunctionV2{
		_name: ET_Greater.String(),
		_args: []LType{
			dateLTyp(),
			dateLTyp()},
		_retType: boolean(),
		_funcTyp: ScalarFuncType,
	}

	gt4 := &FunctionV2{
		_name: ET_Greater.String(),
		_args: []LType{
			integer(),
			integer()},
		_retType: boolean(),
		_funcTyp: ScalarFuncType,
		_scalar:  BinaryFunction[int32, int32, bool](binInt32GreatOp),
	}

	set.Add(gt1)
	set.Add(gt2)
	set.Add(gt3)
	set.Add(gt4)

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

	gtFloat := &FunctionV2{
		_name:    ET_GreaterEqual.String(),
		_args:    []LType{float(), float()},
		_retType: boolean(),
		_funcTyp: ScalarFuncType,
		_scalar:  nil,
	}

	set.Add(gtInteger)
	set.Add(gtDate)
	set.Add(gtFloat)

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

type DateSub struct {
}

func (DateSub) Register(funcList FunctionList) {
	set := NewFunctionSet(ET_DateSub.String(), ScalarFuncType)
	f := &FunctionV2{
		_name:    ET_DateSub.String(),
		_args:    []LType{dateLTyp(), intervalLType()},
		_retType: dateLTyp(),
		_funcTyp: ScalarFuncType,
		_scalar:  BinaryFunction[Date, Interval, Date](binDateInterSubOp),
	}

	set.Add(f)

	funcList.Add(ET_DateSub.String(), set)
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
		_bind:    BindDecimalCaseWhen,
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

func BindDecimalCaseWhen(fun *FunctionV2, args []*Expr) *FunctionData {
	//type of else
	fun._retType = args[0].DataTyp.LTyp
	return nil
}

type ExtractFunc struct {
}

func (ExtractFunc) Register(funcList FunctionList) {
	set := NewFunctionSet(ET_Extract.String(), ScalarFuncType)

	extract := &FunctionV2{
		_name: ET_Extract.String(),
		_args: []LType{
			varchar(),
			dateLTyp()},
		_retType: integer(),
		_funcTyp: ScalarFuncType,
		_scalar:  BinaryFunction[String, Date, int32](binStringInt32ExtractOp),
	}

	set.Add(extract)

	funcList.Add(ET_Extract.String(), set)
}

type SubstringFunc struct {
}

func (SubstringFunc) Register(funcList FunctionList) {
	set := NewFunctionSet(ET_Substring.String(), ScalarFuncType)

	substr1 := &FunctionV2{
		_name: ET_Substring.String(),
		_args: []LType{
			varchar(),
			integer(),
			integer(),
		},
		_retType: varchar(),
		_funcTyp: ScalarFuncType,
		_scalar:  TernaryFunction[String, int64, int64, String](substringFunc),
	}

	substr2 := &FunctionV2{
		_name: ET_Substring.String(),
		_args: []LType{
			varchar(),
			integer(),
		},
		_retType: varchar(),
		_funcTyp: ScalarFuncType,
		_scalar:  BinaryFunction[String, int64, String](substringFuncWithoutLength),
	}

	set.Add(substr1)
	set.Add(substr2)

	funcList.Add(ET_Substring.String(), set)
}
