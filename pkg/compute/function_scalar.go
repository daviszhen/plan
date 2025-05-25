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
	"fmt"
	"math"

	"github.com/daviszhen/plan/pkg/chunk"
	"github.com/daviszhen/plan/pkg/common"
	"github.com/daviszhen/plan/pkg/util"
)

func ScalarNopFunc(input *chunk.Chunk, state *ExprState, result *chunk.Vector) {
	util.AssertFunc(input.ColumnCount() >= 1)
	result.Reference(input.Data[0])
}

func NopDecimalBind(fun *FunctionV2, args []*Expr) *FunctionData {
	fun._retType = args[0].DataTyp
	fun._args[0] = args[0].DataTyp
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
		if arg.DataTyp.Id == common.LTID_UNKNOWN {
			continue
		}
		maxWidth = max(maxWidth, arg.DataTyp.Width)
		maxScale = max(maxScale, arg.DataTyp.Scale)
		maxWidthOverScale = max(maxWidthOverScale, arg.DataTyp.Width-arg.DataTyp.Scale)
	}
	util.AssertFunc(maxWidth > 0)
	//for add/sub, plus 1 extra on the Width
	requireWidth := max(maxScale+maxWidthOverScale, maxWidth) + 1
	if requireWidth > common.DecimalMaxWidthInt64 &&
		maxWidth <= common.DecimalMaxWidthInt64 {
		bindData._checkOverflow = true
		requireWidth = common.DecimalMaxWidthInt64
	}

	if requireWidth > common.DecimalMaxWidth {
		bindData._checkOverflow = true
		requireWidth = common.DecimalMaxWidth
	}
	resTyp := common.DecimalType(requireWidth, maxScale)
	//cast all input types
	for i, arg := range args {
		scale := arg.DataTyp.Scale
		if scale == resTyp.Scale &&
			arg.DataTyp.GetInternalType() == resTyp.GetInternalType() {
			fun._args[i] = arg.DataTyp
		} else {
			fun._args[i] = resTyp
		}
	}
	fun._retType = resTyp
	if bindData._checkOverflow {
		fun._scalar = GetScalarBinaryFunction(resTyp.GetInternalType(), fun._name, true)
	} else {
		fun._scalar = GetScalarBinaryFunction(resTyp.GetInternalType(), fun._name, false)
	}
	return bindData
}

type AddFunc struct {
}

func (add AddFunc) Func(typ common.LType) *FunctionV2 {
	util.AssertFunc(typ.IsNumeric())
	if typ.Id == common.LTID_DECIMAL {
		return &FunctionV2{
			_name:    "+",
			_args:    []common.LType{typ},
			_retType: typ,
			_funcTyp: ScalarFuncType,
			_scalar:  ScalarNopFunc,
			_bind:    NopDecimalBind,
		}
	} else {
		return &FunctionV2{
			_name:    "+",
			_args:    []common.LType{typ},
			_retType: typ,
			_funcTyp: ScalarFuncType,
			_scalar:  ScalarNopFunc,
		}
	}
}

func (add AddFunc) Func2(lTyp, rTyp common.LType) *FunctionV2 {
	if lTyp.IsNumeric() && lTyp.Id == rTyp.Id {
		if lTyp.Id == common.LTID_DECIMAL {
			return &FunctionV2{
				_name:    "+",
				_args:    []common.LType{lTyp, rTyp},
				_retType: lTyp,
				_funcTyp: ScalarFuncType,
				_scalar:  nil,
				_bind:    BindDecimalAddSubstract,
			}
		} else if lTyp.IsIntegral() && lTyp.Id != common.LTID_HUGEINT {
			return &FunctionV2{
				_name:    "+",
				_args:    []common.LType{lTyp, rTyp},
				_retType: lTyp,
				_funcTyp: ScalarFuncType,
				_scalar:  GetScalarIntegerFunction(lTyp.GetInternalType(), "+", true),
				_bind:    nil,
			}
		} else {
			return &FunctionV2{
				_name:    "+",
				_args:    []common.LType{lTyp, rTyp},
				_retType: lTyp,
				_funcTyp: ScalarFuncType,
				_scalar:  GetScalarBinaryFunction(lTyp.GetInternalType(), "+", false),
				_bind:    nil,
			}
		}
	}
	switch lTyp.Id {
	case common.LTID_DATE:
		if rTyp.Id == common.LTID_INTEGER {
			return &FunctionV2{
				_name:    "+",
				_args:    []common.LType{lTyp, rTyp},
				_retType: common.DateType(),
				_funcTyp: ScalarFuncType,
				_scalar:  BinaryFunction[common.Date, int32, common.Date](binDateInt32AddOp),
			}
		} else if rTyp.Id == common.LTID_INTERVAL {
			return &FunctionV2{
				_name:    "+",
				_args:    []common.LType{lTyp, rTyp},
				_retType: common.DateType(),
				_funcTyp: ScalarFuncType,
				_scalar:  BinaryFunction[common.Date, common.Interval, common.Date](binDateInterAddOp),
			}
		} else if rTyp.Id == common.LTID_TIME {
			panic("usp")
		}
	case common.LTID_INTEGER:
		if rTyp.Id == common.LTID_DATE {
			return &FunctionV2{
				_name:    "+",
				_args:    []common.LType{lTyp, rTyp},
				_retType: common.DateType(),
				_funcTyp: ScalarFuncType,
				_scalar:  BinaryFunction[int32, common.Date, common.Date](binInt32DateAddOp),
			}
		}
	case common.LTID_INTERVAL:
		switch rTyp.Id {
		case common.LTID_INTERVAL:
			return &FunctionV2{
				_name:    "+",
				_args:    []common.LType{lTyp, rTyp},
				_retType: common.IntervalType(),
				_funcTyp: ScalarFuncType,
				_scalar:  BinaryFunction[common.Interval, common.Interval, common.Interval](binIntervalIntervalAddOp),
			}
		case common.LTID_DATE:
			return &FunctionV2{
				_name:    "+",
				_args:    []common.LType{lTyp, rTyp},
				_retType: common.DateType(),
				_funcTyp: ScalarFuncType,
				_scalar:  BinaryFunction[common.Interval, common.Date, common.Date](binIntervalDateAddOp),
			}
		default:
			panic("usp")
		}
	case common.LTID_TIME:
		panic("usp")
	case common.LTID_TIMESTAMP:
		panic("usp")
	default:
		panic(fmt.Sprintf("no addFunc for %s %s", lTyp, rTyp))
	}
	return nil
}

func (add AddFunc) Register(funcList FunctionList) {
	funcs := NewFunctionSet(ET_Add.String(), ScalarFuncType)
	for _, typ := range common.Numeric() {
		//unary add
		funcs.Add(add.Func(typ))
		//binary add
		funcs.Add(add.Func2(typ, typ))
	}
	//date + integer, integer + date
	funcs.Add(add.Func2(common.DateType(), common.IntegerType()))
	funcs.Add(add.Func2(common.IntegerType(), common.DateType()))
	//interval + interval
	funcs.Add(add.Func2(common.IntervalType(), common.IntervalType()))
	//interval + date|time|timestamp
	//date|time|timestamp + interval
	funcs.Add(add.Func2(common.DateType(), common.IntervalType()))
	funcs.Add(add.Func2(common.IntervalType(), common.DateType()))

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

func negateInterval(input *common.Interval, result *common.Interval) {
	negateInt32(&input.Months, &result.Months)
	negateInt32(&input.Days, &result.Days)
	negateInt32(&input.Year, &result.Year)
}

func DecimalNegateBind(fun *FunctionV2, args []*Expr) *FunctionData {
	decTyp := args[0].DataTyp
	fun._scalar = GetScalarUnaryFunction(decTyp, "-")
	fun._args[0] = decTyp
	fun._retType = decTyp
	return nil
}

func (sub SubFunc) Func(typ common.LType) *FunctionV2 {
	if typ.Id == common.LTID_INTERVAL {
		return &FunctionV2{
			_name:    "-",
			_args:    []common.LType{typ},
			_retType: typ,
			_funcTyp: ScalarFuncType,
			_scalar:  UnaryFunction[common.Interval, common.Interval](negateInterval),
		}
	} else if typ.Id == common.LTID_DECIMAL {
		return &FunctionV2{
			_name:    "-",
			_args:    []common.LType{typ},
			_retType: typ,
			_funcTyp: ScalarFuncType,
			_bind:    DecimalNegateBind,
		}
	} else {
		util.AssertFunc(typ.IsNumeric())
		return &FunctionV2{
			_name:    "-",
			_args:    []common.LType{typ},
			_retType: typ,
			_funcTyp: ScalarFuncType,
			_scalar:  GetScalarUnaryFunction(typ, "-"),
		}
	}
}

func (sub SubFunc) Func2(lTyp, rTyp common.LType) *FunctionV2 {
	if lTyp.IsNumeric() && lTyp.Id == rTyp.Id {
		if lTyp.Id == common.LTID_DECIMAL {
			return &FunctionV2{
				_name:    "-",
				_args:    []common.LType{lTyp, rTyp},
				_retType: lTyp,
				_funcTyp: ScalarFuncType,
				_scalar:  nil,
				_bind:    BindDecimalAddSubstract,
			}
		} else if lTyp.IsIntegral() && lTyp.Id != common.LTID_HUGEINT {
			return &FunctionV2{
				_name:    "-",
				_args:    []common.LType{lTyp, rTyp},
				_retType: lTyp,
				_funcTyp: ScalarFuncType,
				_scalar:  GetScalarIntegerFunction(lTyp.GetInternalType(), "-", true),
				_bind:    nil,
			}
		} else {
			return &FunctionV2{
				_name:    "-",
				_args:    []common.LType{lTyp, rTyp},
				_retType: lTyp,
				_funcTyp: ScalarFuncType,
				_scalar:  GetScalarBinaryFunction(lTyp.GetInternalType(), "-", false),
				_bind:    nil,
			}
		}
	}
	switch lTyp.Id {
	case common.LTID_DATE:
		if rTyp.Id == common.LTID_DATE {
			return &FunctionV2{
				_name:    "-",
				_args:    []common.LType{lTyp, rTyp},
				_retType: common.BigintType(),
				_funcTyp: ScalarFuncType,
				_scalar:  BinaryFunction[common.Date, common.Date, int64](nil),
			}
		} else if rTyp.Id == common.LTID_INTEGER {
			return &FunctionV2{
				_name:    "-",
				_args:    []common.LType{lTyp, rTyp},
				_retType: common.DateType(),
				_funcTyp: ScalarFuncType,
				_scalar:  BinaryFunction[common.Date, int32, common.Date](nil),
			}
		} else if rTyp.Id == common.LTID_INTERVAL {
			return &FunctionV2{
				_name:    "-",
				_args:    []common.LType{lTyp, rTyp},
				_retType: common.DateType(),
				_funcTyp: ScalarFuncType,
				_scalar:  BinaryFunction[common.Date, common.Interval, common.Date](nil),
			}
		}
	case common.LTID_INTERVAL:
		switch rTyp.Id {
		case common.LTID_INTERVAL:
			return &FunctionV2{
				_name:    "-",
				_args:    []common.LType{lTyp, rTyp},
				_retType: common.IntervalType(),
				_funcTyp: ScalarFuncType,
				_scalar:  BinaryFunction[common.Interval, common.Interval, common.Interval](nil),
			}
		default:
			panic("usp")
		}
	case common.LTID_TIME:
		panic("usp")
	case common.LTID_TIMESTAMP:
		panic("usp")
	default:
		panic(fmt.Sprintf("no addFunc for %s %s", lTyp, rTyp))
	}
	return nil
}

func (sub SubFunc) Register(funcList FunctionList) {
	subs := NewFunctionSet(ET_Sub.String(), ScalarFuncType)

	for _, typ := range common.Numeric() {
		//unary
		subs.Add(sub.Func(typ))
		//binary
		subs.Add(sub.Func2(typ, typ))
	}
	//date - date
	subs.Add(sub.Func2(common.DateType(), common.DateType()))
	//date - integer
	subs.Add(sub.Func2(common.DateType(), common.IntegerType()))
	//timestamp - timestamp
	//interval - interval
	subs.Add(sub.Func2(common.IntervalType(), common.IntervalType()))
	//date - interval
	subs.Add(sub.Func2(common.DateType(), common.IntervalType()))
	//-interval
	subs.Add(sub.Func(common.IntervalType()))
	funcList.Add(ET_Sub.String(), subs)
}

type MultiplyFunc struct {
}

func (MultiplyFunc) Register(funcList FunctionList) {
	set := NewFunctionSet(ET_Mul.String(), ScalarFuncType)

	for _, typ := range common.Numeric() {
		if typ.Id == common.LTID_DECIMAL {
			fun := &FunctionV2{
				_name:    "*",
				_args:    []common.LType{typ, typ},
				_retType: typ,
				_funcTyp: ScalarFuncType,
				_scalar:  nil,
				_bind:    BindDecimalMultiply,
			}
			set.Add(fun)
		} else if typ.IsIntegral() && typ.Id != common.LTID_HUGEINT {
			fun := &FunctionV2{
				_name:    "*",
				_args:    []common.LType{typ, typ},
				_retType: typ,
				_funcTyp: ScalarFuncType,
				_scalar:  GetScalarIntegerFunction(typ.GetInternalType(), "*", true),
			}
			set.Add(fun)
		} else {
			fun := &FunctionV2{
				_name:    "*",
				_args:    []common.LType{typ, typ},
				_retType: typ,
				_funcTyp: ScalarFuncType,
				_scalar:  GetScalarBinaryFunction(typ.GetInternalType(), "*", false),
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
		if arg.DataTyp.Id == common.LTID_UNKNOWN {
			continue
		}
		if arg.DataTyp.Width > maxWidth {
			maxWidth = arg.DataTyp.Width
		}
		resWidth += arg.DataTyp.Width
		resScale += arg.DataTyp.Scale
	}
	util.AssertFunc(maxWidth > 0)
	if resScale > common.DecimalMaxWidth {
		panic(fmt.Sprintf("Scale %d greater than %d", resScale, common.DecimalMaxWidth))
	}
	if resWidth > common.DecimalMaxWidthInt64 &&
		maxWidth <= common.DecimalMaxWidthInt64 &&
		resScale < common.DecimalMaxWidthInt64 {
		bindData._checkOverflow = true
		resWidth = common.DecimalMaxWidthInt64
	}
	if resWidth > common.DecimalMaxWidth {
		bindData._checkOverflow = true
		resWidth = common.DecimalMaxWidth
	}

	resTyp := common.DecimalType(resWidth, resScale)
	for i, arg := range args {
		if arg.DataTyp.GetInternalType() == resTyp.GetInternalType() {
			fun._args[i] = arg.DataTyp
		} else {
			fun._args[i] = common.DecimalType(resWidth, arg.DataTyp.Scale)
		}
	}
	fun._retType = resTyp
	if bindData._checkOverflow {
		fun._scalar = GetScalarBinaryFunction(resTyp.GetInternalType(), "*", true)
	} else {
		fun._scalar = GetScalarBinaryFunction(resTyp.GetInternalType(), "*", false)
	}
	return bindData
}

type DevideFunc struct {
}

func (DevideFunc) Register(funcList FunctionList) {
	set := NewFunctionSet(ET_Div.String(), ScalarFuncType)

	divFloat := &FunctionV2{
		_name:    ET_Div.String(),
		_args:    []common.LType{common.FloatType(), common.FloatType()},
		_retType: common.FloatType(),
		_funcTyp: ScalarFuncType,
		_scalar:  BinaryFunction[float32, float32, float32](binFloat32DivOp),
	}

	divDec := &FunctionV2{
		_name:    ET_Div.String(),
		_args:    []common.LType{common.DecimalType(common.DecimalMaxWidthInt64, 0), common.DecimalType(common.DecimalMaxWidthInt64, 0)},
		_retType: common.DecimalType(common.DecimalMaxWidthInt64, 0),
		_funcTyp: ScalarFuncType,
		_bind:    BindDecimalDivide,
	}

	set.Add(divFloat)
	set.Add(divDec)

	funcList.Add(ET_Div.String(), set)
}

func BindDecimalDivide(fun *FunctionV2, args []*Expr) *FunctionData {
	fun._retType = args[0].DataTyp
	for i, arg := range args {
		fun._args[i] = arg.DataTyp
	}
	fun._scalar = BinaryFunction[common.Decimal, common.Decimal, common.Decimal](binDecimalDivOp)
	return nil
}

type LikeFunc struct {
}

func (like LikeFunc) Register(funcList FunctionList) {
	likeFunc := &FunctionV2{
		_name:    ET_Like.String(),
		_args:    []common.LType{common.VarcharType(), common.VarcharType()},
		_retType: common.BooleanType(),
		_funcTyp: ScalarFuncType,
		_scalar:  BinaryFunction[common.String, common.String, bool](binStringLikeOp),
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
		_args:    []common.LType{common.VarcharType(), common.VarcharType()},
		_retType: common.BooleanType(),
		_funcTyp: ScalarFuncType,
		//_scalar:  BinaryFunction[String, String, bool](binStringLikeOp),
	}
	set := NewFunctionSet(ET_NotLike.String(), ScalarFuncType)
	set.Add(likeFunc)
	funcList.Add(ET_NotLike.String(), set)
}

func GetScalarIntegerFunction(ptyp common.PhyType, opKind string, checkOverflow bool) ScalarFunc {
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

func GetScalarIntegerMulFunction(ptyp common.PhyType, overflow bool) ScalarFunc {
	if overflow {
		return GetScalarIntegerMulFunctionWithOverflow(ptyp)
	} else {
		return GetScalarIntegerMulFunctionWithoutOverflow(ptyp)
	}
}

func GetScalarIntegerMulFunctionWithOverflow(ptyp common.PhyType) ScalarFunc {
	switch ptyp {
	case common.INT8:
		return BinaryFunction[int8, int8, int8](mulInt8CheckOf)
	case common.INT16:
		return BinaryFunction[int16, int16, int16](mulInt16CheckOf)
	case common.INT32:
		return BinaryFunction[int32, int32, int32](mulInt32CheckOf)
	case common.INT64:
		return BinaryFunction[int64, int64, int64](mulInt64CheckOf)
	case common.UINT8:
		return BinaryFunction[uint8, uint8, uint8](mulUint8CheckOf)
	case common.UINT16:
		return BinaryFunction[uint16, uint16, uint16](mulUint16CheckOf)
	case common.UINT32:
		return BinaryFunction[uint32, uint32, uint32](mulUint32CheckOf)
	case common.UINT64:
		return BinaryFunction[uint64, uint64, uint64](mulUint64CheckOf)
	case common.DECIMAL:
		return BinaryFunction[common.Decimal, common.Decimal, common.Decimal](binDecimalDecimalMulOp)
	default:
		panic("not implement")
	}
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

func GetScalarIntegerMulFunctionWithoutOverflow(ptyp common.PhyType) ScalarFunc {
	switch ptyp {
	case common.INT8:
		return BinaryFunction[int8, int8, int8](mulInt8)
	case common.INT16:
		return BinaryFunction[int16, int16, int16](mulInt16)
	case common.INT32:
		return BinaryFunction[int32, int32, int32](mulInt32)
	case common.INT64:
		return BinaryFunction[int64, int64, int64](mulInt64)
	case common.UINT8:
		return BinaryFunction[uint8, uint8, uint8](mulUint8)
	case common.UINT16:
		return BinaryFunction[uint16, uint16, uint16](mulUint16)
	case common.UINT32:
		return BinaryFunction[uint32, uint32, uint32](mulUint32)
	case common.UINT64:
		return BinaryFunction[uint64, uint64, uint64](mulUint64)
	case common.DECIMAL:
		return BinaryFunction[common.Decimal, common.Decimal, common.Decimal](binDecimalDecimalMulOp)
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

func GetScalarIntegerSubFunction(ptyp common.PhyType, overflow bool) ScalarFunc {
	if overflow {
		return GetScalarIntegerSubFunctionWithOverflow(ptyp)
	} else {
		return GetScalarIntegerSubFunctionWithoutOverflow(ptyp)
	}
}

func GetScalarIntegerSubFunctionWithOverflow(ptyp common.PhyType) ScalarFunc {
	switch ptyp {
	case common.INT8:
		return BinaryFunction[int8, int8, int8](subInt8CheckOf)
	case common.INT16:
		return BinaryFunction[int16, int16, int16](subInt16CheckOf)
	case common.INT32:
		return BinaryFunction[int32, int32, int32](subInt32CheckOf)
	case common.INT64:
		return BinaryFunction[int64, int64, int64](subInt64CheckOf)
	case common.UINT8:
		return BinaryFunction[uint8, uint8, uint8](subUint8CheckOf)
	case common.UINT16:
		return BinaryFunction[uint16, uint16, uint16](subUint16CheckOf)
	case common.UINT32:
		return BinaryFunction[uint32, uint32, uint32](subUint32CheckOf)
	case common.UINT64:
		return BinaryFunction[uint64, uint64, uint64](subUint64CheckOf)
	case common.DECIMAL:
		return BinaryFunction[common.Decimal, common.Decimal, common.Decimal](binDecimalDecimalSubOp)
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
func GetScalarIntegerSubFunctionWithoutOverflow(ptyp common.PhyType) ScalarFunc {
	switch ptyp {
	case common.INT8:
		return BinaryFunction[int8, int8, int8](subInt8)
	case common.INT16:
		return BinaryFunction[int16, int16, int16](subInt16)
	case common.INT32:
		return BinaryFunction[int32, int32, int32](subInt32)
	case common.INT64:
		return BinaryFunction[int64, int64, int64](subInt64)
	case common.UINT8:
		return BinaryFunction[uint8, uint8, uint8](subUint8)
	case common.UINT16:
		return BinaryFunction[uint16, uint16, uint16](subUint16)
	case common.UINT32:
		return BinaryFunction[uint32, uint32, uint32](subUint32)
	case common.UINT64:
		return BinaryFunction[uint64, uint64, uint64](subUint64)
	case common.DECIMAL:
		return BinaryFunction[common.Decimal, common.Decimal, common.Decimal](binDecimalDecimalSubOp)
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

func GetScalarBinaryFunction(ptyp common.PhyType, opKind string, checkOverflow bool) ScalarFunc {
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

func GetScalarBinaryMulFunction(ptyp common.PhyType, overflow bool) ScalarFunc {
	if overflow {
		return GetScalarBinaryMulFunctionWithOverflow(ptyp)
	} else {
		return GetScalarBinaryMulFunctionWithoutOverflow(ptyp)
	}
}

func GetScalarBinaryMulFunctionWithOverflow(ptyp common.PhyType) ScalarFunc {
	switch ptyp {
	case common.INT128:
		return BinaryFunction[common.Hugeint, common.Hugeint, common.Hugeint](mulHugeint)
	case common.FLOAT:
		return BinaryFunction[float32, float32, float32](mulFloat32)
	case common.DOUBLE:
		return BinaryFunction[float64, float64, float64](mulFloat64)
	default:
		return GetScalarIntegerMulFunction(ptyp, true)
	}
}

func GetScalarBinaryMulFunctionWithoutOverflow(ptyp common.PhyType) ScalarFunc {
	switch ptyp {
	case common.INT128:
		return BinaryFunction[common.Hugeint, common.Hugeint, common.Hugeint](mulHugeint)
	case common.FLOAT:
		return BinaryFunction[float32, float32, float32](mulFloat32)
	case common.DOUBLE:
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

func mulHugeint(left *common.Hugeint, right *common.Hugeint, result *common.Hugeint) {
	panic("usp")
}

func GetScalarBinarySubFunction(ptyp common.PhyType, overflow bool) ScalarFunc {
	if overflow {
		return GetScalarBinarySubFunctionWithOverflow(ptyp)
	} else {
		return GetScalarBinarySubFunctionWithoutOverflow(ptyp)
	}
}

func GetScalarBinarySubFunctionWithoutOverflow(ptyp common.PhyType) ScalarFunc {
	switch ptyp {
	case common.INT128:
		return BinaryFunction[common.Hugeint, common.Hugeint, common.Hugeint](subHugeint)
	case common.FLOAT:
		return BinaryFunction[float32, float32, float32](subFloat32)
	case common.DOUBLE:
		return BinaryFunction[float64, float64, float64](subFloat64)
	default:
		return GetScalarIntegerSubFunction(ptyp, false)
	}
}

func GetScalarBinarySubFunctionWithOverflow(ptyp common.PhyType) ScalarFunc {
	switch ptyp {
	case common.INT128:
		return BinaryFunction[common.Hugeint, common.Hugeint, common.Hugeint](subHugeint)
	case common.FLOAT:
		return BinaryFunction[float32, float32, float32](subFloat32)
	case common.DOUBLE:
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

func GetScalarUnaryFunction(typ common.LType, opKind string) ScalarFunc {
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

func GetScalarUnarySubFunction(typ common.LType) ScalarFunc {
	var fun ScalarFunc
	switch typ.Id {
	case common.LTID_TINYINT:
		fun = UnaryFunction[int8, int8](negateInt8)
	case common.LTID_SMALLINT:
		fun = UnaryFunction[int16, int16](negateInt16)
	case common.LTID_INTEGER:
		fun = UnaryFunction[int32, int32](negateInt32)
	case common.LTID_BIGINT:
		fun = UnaryFunction[int64, int64](negateInt64)
	case common.LTID_UTINYINT:
		fun = UnaryFunction[uint8, uint8](negateUint8)
	case common.LTID_USMALLINT:
		fun = UnaryFunction[uint16, uint16](negateUint16)
	case common.LTID_UINTEGER:
		fun = UnaryFunction[uint32, uint32](negateUint32)
	case common.LTID_UBIGINT:
		fun = UnaryFunction[uint64, uint64](negateUint64)
	case common.LTID_HUGEINT:
		fun = UnaryFunction[common.Hugeint, common.Hugeint](common.NegateHugeint)
	case common.LTID_FLOAT:
		fun = UnaryFunction[float32, float32](negateFloat)
	case common.LTID_DOUBLE:
		fun = UnaryFunction[float64, float64](negateDouble)
	case common.LTID_DECIMAL:
		fun = UnaryFunction[common.Decimal, common.Decimal](common.NegateDecimal)
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
		_args:    []common.LType{common.IntegerType(), common.IntegerType()},
		_retType: common.BooleanType(),
		_funcTyp: ScalarFuncType,
		_scalar:  BinaryFunction[int32, int32, bool](binInt32EqualOp),
	}

	inVarchar := &FunctionV2{
		_name:    ET_In.String(),
		_args:    []common.LType{common.VarcharType(), common.VarcharType()},
		_retType: common.BooleanType(),
		_funcTyp: ScalarFuncType,
		_scalar:  BinaryFunction[common.String, common.String, bool](binStringEqualOp),
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
		_args:    []common.LType{common.IntegerType(), common.IntegerType()},
		_retType: common.BooleanType(),
		_funcTyp: ScalarFuncType,
		_scalar:  BinaryFunction[int32, int32, bool](binInt32EqualOp),
	}

	equalStr := &FunctionV2{
		_name:    ET_Equal.String(),
		_args:    []common.LType{common.VarcharType(), common.VarcharType()},
		_retType: common.BooleanType(),
		_funcTyp: ScalarFuncType,
	}

	equalBool := &FunctionV2{
		_name:    ET_Equal.String(),
		_args:    []common.LType{common.BooleanType(), common.BooleanType()},
		_retType: common.BooleanType(),
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
		_args:    []common.LType{common.IntegerType(), common.IntegerType()},
		_retType: common.BooleanType(),
		_funcTyp: ScalarFuncType,
	}

	notEqualStr := &FunctionV2{
		_name:    ET_NotEqual.String(),
		_args:    []common.LType{common.VarcharType(), common.VarcharType()},
		_retType: common.BooleanType(),
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
		_args:    []common.LType{common.BooleanType(), common.BooleanType()},
		_retType: common.BooleanType(),
		_funcTyp: ScalarFuncType,
		_scalar:  nil,
	}
	set1.Add(andFunc)

	set2 := NewFunctionSet(ET_Or.String(), ScalarFuncType)
	orFunc := &FunctionV2{
		_name:    ET_Or.String(),
		_args:    []common.LType{common.BooleanType(), common.BooleanType()},
		_retType: common.BooleanType(),
		_funcTyp: ScalarFuncType,
		_scalar:  nil,
	}
	set2.Add(orFunc)

	set3 := NewFunctionSet(ET_Not.String(), ScalarFuncType)
	notFunc := &FunctionV2{
		_name:    ET_And.String(),
		_args:    []common.LType{common.BooleanType(), common.BooleanType()},
		_retType: common.BooleanType(),
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
		_args:    []common.LType{common.FloatType(), common.FloatType()},
		_retType: common.BooleanType(),
		_funcTyp: ScalarFuncType,
		_scalar:  BinaryFunction[float32, float32, bool](binFloat32GreatOp),
	}

	gt2 := &FunctionV2{
		_name: ET_Greater.String(),
		_args: []common.LType{
			common.DecimalType(common.DecimalMaxWidthInt64, 0),
			common.DecimalType(common.DecimalMaxWidthInt64, 0)},
		_retType: common.BooleanType(),
		_funcTyp: ScalarFuncType,
	}

	gt3 := &FunctionV2{
		_name: ET_Greater.String(),
		_args: []common.LType{
			common.DateType(),
			common.DateType()},
		_retType: common.BooleanType(),
		_funcTyp: ScalarFuncType,
	}

	gt4 := &FunctionV2{
		_name: ET_Greater.String(),
		_args: []common.LType{
			common.IntegerType(),
			common.IntegerType()},
		_retType: common.BooleanType(),
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
		_args:    []common.LType{common.IntegerType(), common.IntegerType()},
		_retType: common.BooleanType(),
		_funcTyp: ScalarFuncType,
		_scalar:  nil,
	}
	gtDate := &FunctionV2{
		_name:    ET_GreaterEqual.String(),
		_args:    []common.LType{common.DateType(), common.DateType()},
		_retType: common.BooleanType(),
		_funcTyp: ScalarFuncType,
		_scalar:  nil,
	}

	gtFloat := &FunctionV2{
		_name:    ET_GreaterEqual.String(),
		_args:    []common.LType{common.FloatType(), common.FloatType()},
		_retType: common.BooleanType(),
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
		_args:    []common.LType{common.DateType(), common.IntervalType()},
		_retType: common.DateType(),
		_funcTyp: ScalarFuncType,
		_scalar:  BinaryFunction[common.Date, common.Interval, common.Date](binDateInterAddOp),
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
		_args:    []common.LType{common.DateType(), common.IntervalType()},
		_retType: common.DateType(),
		_funcTyp: ScalarFuncType,
		_scalar:  BinaryFunction[common.Date, common.Interval, common.Date](binDateInterSubOp),
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
		_args:    []common.LType{common.DateType(), common.DateType()},
		_retType: common.BooleanType(),
		_funcTyp: ScalarFuncType,
	}

	lInt := &FunctionV2{
		_name:    ET_Less.String(),
		_args:    []common.LType{common.IntegerType(), common.IntegerType()},
		_retType: common.BooleanType(),
		_funcTyp: ScalarFuncType,
	}

	lFloat := &FunctionV2{
		_name:    ET_Less.String(),
		_args:    []common.LType{common.FloatType(), common.FloatType()},
		_retType: common.BooleanType(),
		_funcTyp: ScalarFuncType,
	}

	lDouble := &FunctionV2{
		_name:    ET_Less.String(),
		_args:    []common.LType{common.DoubleType(), common.DoubleType()},
		_retType: common.BooleanType(),
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
		_args:    []common.LType{common.DateType(), common.DateType()},
		_retType: common.BooleanType(),
		_funcTyp: ScalarFuncType,
	}
	leInt := &FunctionV2{
		_name:    ET_LessEqual.String(),
		_args:    []common.LType{common.IntegerType(), common.IntegerType()},
		_retType: common.BooleanType(),
		_funcTyp: ScalarFuncType,
	}
	leFloat := &FunctionV2{
		_name:    ET_LessEqual.String(),
		_args:    []common.LType{common.FloatType(), common.FloatType()},
		_retType: common.BooleanType(),
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
		_args: []common.LType{
			common.DecimalType(common.DecimalMaxWidthInt64, 0),
			common.BooleanType(),
			common.DecimalType(common.DecimalMaxWidthInt64, 0)},
		_retType: common.DecimalType(common.DecimalMaxWidthInt64, 0),
		_funcTyp: ScalarFuncType,
		_bind:    BindDecimalCaseWhen,
	}

	divInt := &FunctionV2{
		_name:    ET_Case.String(),
		_args:    []common.LType{common.IntegerType(), common.BooleanType(), common.IntegerType()},
		_retType: common.IntegerType(),
		_funcTyp: ScalarFuncType,
	}

	set.Add(caseDec)
	set.Add(divInt)

	funcList.Add(ET_Case.String(), set)
}

func BindDecimalCaseWhen(fun *FunctionV2, args []*Expr) *FunctionData {
	//type of else
	fun._retType = args[0].DataTyp
	return nil
}

type ExtractFunc struct {
}

func (ExtractFunc) Register(funcList FunctionList) {
	set := NewFunctionSet(ET_Extract.String(), ScalarFuncType)

	extract := &FunctionV2{
		_name: ET_Extract.String(),
		_args: []common.LType{
			common.VarcharType(),
			common.DateType()},
		_retType: common.IntegerType(),
		_funcTyp: ScalarFuncType,
		_scalar:  BinaryFunction[common.String, common.Date, int32](binStringInt32ExtractOp),
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
		_args: []common.LType{
			common.VarcharType(),
			common.IntegerType(),
			common.IntegerType(),
		},
		_retType: common.VarcharType(),
		_funcTyp: ScalarFuncType,
		_scalar:  TernaryFunction[common.String, int64, int64, common.String](substringFunc),
	}

	substr2 := &FunctionV2{
		_name: ET_Substring.String(),
		_args: []common.LType{
			common.VarcharType(),
			common.IntegerType(),
		},
		_retType: common.VarcharType(),
		_funcTyp: ScalarFuncType,
		_scalar:  BinaryFunction[common.String, int64, common.String](substringFuncWithoutLength),
	}

	set.Add(substr1)
	set.Add(substr2)

	funcList.Add(ET_Substring.String(), set)
}
