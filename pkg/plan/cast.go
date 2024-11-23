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
	"strconv"
	"strings"
	"time"

	dec "github.com/govalues/decimal"
)

var (
	// int32 => ...
	gTryCastInt32ToInt32   tryCastInt32ToInt32
	gTryCastInt32ToFloat32 tryCastInt32ToFloat32
	gTryCastInt32ToFloat64 tryCastInt32ToFloat64

	// bigint =>
	gTryCastBigintToInt32   tryCastBigintToInt32
	gTryCastBigintToDecimal tryCastBigintToDecimal

	// float32 => ...
	gTryCastFloat32ToInt32   tryCastFloat32ToInt32
	gTryCastFloat32ToFloat64 tryCastFloat32ToFloat64

	// decimal => ...
	gTryCastDecimalToFloat32 tryCastDecimalToFloat32

	// varchar => ...
	gTryCastVarcharToDate tryCastVarcharToDate

	gTryCastVarcharToInterval tryCastVarcharToInterval
)

//lint:ignore U1000
type tryCastInt32ToInt32 struct {
}

func (numCast tryCastInt32ToInt32) operation(input *int32, result *int32, _ bool) bool {
	*result = int32(*input)
	return true
}

//lint:ignore U1000
type tryCastInt32ToFloat32 struct {
}

func (numCast tryCastInt32ToFloat32) operation(input *int32, result *float32, _ bool) bool {
	*result = float32(*input)
	return true
}

//lint:ignore U1000
type tryCastInt32ToFloat64 struct {
}

func (numCast tryCastInt32ToFloat64) operation(input *int32, result *float64, _ bool) bool {
	*result = float64(*input)
	return true
}

//lint:ignore U1000
type tryCastInt32ToDecimal struct {
	tScale int
}

func (numCast tryCastInt32ToDecimal) operation(input *int32, result *Decimal, _ bool) bool {
	nDec, err := dec.NewFromInt64(int64(*input), 0, numCast.tScale)
	if err != nil {
		panic(err)
	}

	*result = Decimal{
		Decimal: nDec,
	}
	return true
}

//lint:ignore U1000
type tryCastDecimalToFloat32 struct {
}

func (numCast tryCastDecimalToFloat32) operation(input *Decimal, result *float32, _ bool) bool {
	v, ok := input.Float64()
	assertFunc(ok)
	*result = float32(v)
	return true
}

//lint:ignore U1000
type tryCastBigintToInt32 struct{}

func (numCast tryCastBigintToInt32) operation(input *Hugeint, result *int32, _ bool) bool {
	val := int32(input._lower)
	if uint64(val) != input._lower {
		fmt.Println(input)
	}
	*result = val
	return true
}

//lint:ignore U1000
type tryCastBigintToDecimal struct{}

func (numCast tryCastBigintToDecimal) operation(input *Hugeint, result *Decimal, _ bool) bool {
	panic("usp")
}

//lint:ignore U1000
type tryCastDecimalToDecimal struct {
	dstScale int
	srcScale int
}

func (numCast tryCastDecimalToDecimal) operation(input *Decimal, result *Decimal, _ bool) bool {
	//if numCast.srcScale == numCast.dstScale {
	//	*result = *input
	//} else if numCast.srcScale > numCast.dstScale {
	//
	//} else {
	//
	//}
	w, f, ok := input.Int64(numCast.dstScale)
	if ok {
		ndec, err := dec.NewFromInt64(w, f, numCast.dstScale)
		if err != nil {
			panic(err)
		}
		result.Decimal = ndec
	} else {

		ndec, err := dec.ParseExact(input.String(), numCast.dstScale)
		if err != nil {
			panic(err)
		}
		result.Decimal = ndec
	}
	return true
}

//lint:ignore U1000
type tryCastFloat32ToInt32 struct {
}

func (numCast tryCastFloat32ToInt32) operation(input *float32, result *int32, _ bool) bool {
	*result = int32(*input)
	return true
}

//lint:ignore U1000
type tryCastFloat32ToFloat64 struct {
}

func (numCast tryCastFloat32ToFloat64) operation(input *float32, result *float64, _ bool) bool {
	*result = float64(*input)
	return true
}

//lint:ignore U1000
type tryCastVarcharToDate struct {
}

func (numCast tryCastVarcharToDate) operation(input *String, result *Date, _ bool) bool {
	ti, err := time.Parse(time.DateOnly, input.String())
	if err != nil {
		panic(err)
	}
	y, m, d := ti.Date()
	*result = Date{
		_year:  int32(y),
		_month: int32(m),
		_day:   int32(d),
	}
	return true
}

//lint:ignore U1000
type tryCastVarcharToInterval struct {
}

func (numCast tryCastVarcharToInterval) operation(input *String, result *Interval, _ bool) bool {
	is := input.String()
	seps := strings.Split(is, " ")
	if len(seps) != 2 {
		panic(fmt.Sprintf("invalid interval string %v", is))
	}
	switch strings.ToLower(seps[1]) {
	case "year":
		result._unit = "year"
		parseInt, err := strconv.ParseInt(seps[0], 10, 64)
		if err != nil {
			panic(fmt.Sprintf("invalid interval string %v", is))
		}
		result._year = int32(parseInt)
	case "month":
		result._unit = "month"
		parseInt, err := strconv.ParseInt(seps[0], 10, 64)
		if err != nil {
			panic(fmt.Sprintf("invalid interval string %v", is))
		}
		result._months = int32(parseInt)

	case "day":
		result._unit = "day"
		parseInt, err := strconv.ParseInt(seps[0], 10, 64)
		if err != nil {
			panic(fmt.Sprintf("invalid interval string %v", is))
		}
		result._days = int32(parseInt)
	default:
		panic(fmt.Sprintf("invalid interval unit %v", is))
	}
	return true
}

func castExec(
	source, result *Vector,
	count int,
) {
	panic("usp")
}

func AddCastToType(expr *Expr, dstTyp LType, tryCast bool) (*Expr, error) {
	var err error
	var retExpr *Expr
	if expr.DataTyp.LTyp.equal(dstTyp) {
		return expr, nil
	}

	castInfo := castFuncs.GetCastFunc(expr.DataTyp.LTyp, dstTyp)

	args := []*Expr{
		expr, //expr to be cast
		//target type saved in DataTyp field
		{
			Typ: ET_IConst,
			DataTyp: ExprDataType{
				LTyp: dstTyp,
			},
		},
	}

	retExpr = &Expr{
		Typ:    ET_Func,
		SubTyp: ET_SubFunc,
		Svalue: ET_Cast.String(),
		DataTyp: ExprDataType{
			LTyp: dstTyp,
		},
		Children:   args,
		IsOperator: ET_Cast.isOperator(),
		BindInfo:   nil,
		FunImpl: &FunctionV2{
			_name:          ET_Cast.String(),
			_args:          []LType{expr.DataTyp.LTyp, dstTyp},
			_retType:       dstTyp,
			_funcTyp:       ScalarFuncType,
			_boundCastInfo: castInfo,
		},
	}

	return retExpr, err
}

//lint:ignore U1000
type NumericTryCast[T any, R any] struct {
	op CastOp[T, R]
}

func (cast NumericTryCast[T, R]) operation(input *T, result *R, strict bool) bool {
	if cast.op == nil {
		panic("usp")
	}
	//FIXME: add overflow check
	return cast.op.operation(input, result, strict)
}

type BindCastFuncType func(input *BindCastInput, src, dst LType) *BoundCastInfo

type BindCastFunc struct {
	_fun  BindCastFuncType
	_info *BindCastInfo
}

type CastFunctionSet struct {
	_bindFuncs []*BindCastFunc
}

func NoCast(src *Vector, res *Vector, count int, params *CastParams) bool {
	res.reference(src)
	return true
}

var (
	defNoCast = &BoundCastInfo{
		_fun: NoCast,
	}
)

func (castSet *CastFunctionSet) GetCastFunc(src, dst LType) *BoundCastInfo {
	if src.equal(dst) {
		return defNoCast
	}

	for i := len(castSet._bindFuncs); i > 0; i-- {
		bindFunc := castSet._bindFuncs[i-1]
		input := &BindCastInput{
			_funcSet: castSet,
			_info:    bindFunc._info,
		}
		ret := bindFunc._fun(input, src, dst)
		if ret != nil && ret._fun != nil {
			return ret
		}
	}
	panic("usp")
}

func NewCastFunctionSet() *CastFunctionSet {
	ret := &CastFunctionSet{}
	ret._bindFuncs = append(ret._bindFuncs, &BindCastFunc{
		_fun: DefaultCastFunc,
	})
	return ret
}

func (castSet *CastFunctionSet) ImplicitCastCost(src, dst LType) int64 {
	return implicitCast(src, dst)
}
