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
	"strconv"
	"strings"
	"time"

	dec "github.com/govalues/decimal"

	"github.com/daviszhen/plan/pkg/chunk"
	"github.com/daviszhen/plan/pkg/common"
	"github.com/daviszhen/plan/pkg/util"
)

func tryCastInt32ToInt32(input *int32, result *int32, _ bool) bool {
	*result = int32(*input)
	return true
}

func tryCastInt32ToInt64(input *int32, result *int64, _ bool) bool {
	*result = int64(*input)
	return true
}

func tryCastInt32ToHugeint(input *int32, result *common.Hugeint, _ bool) bool {
	result.Upper = 0
	result.Lower = uint64(*input)
	return true
}

func tryCastInt32ToFloat32(input *int32, result *float32, _ bool) bool {
	*result = float32(*input)
	return true
}

func tryCastInt32ToFloat64(input *int32, result *float64, _ bool) bool {
	*result = float64(*input)
	return true
}

func tryCastInt32ToDecimal(input *int32, result *common.Decimal, tScale int, _ bool) bool {
	nDec, err := dec.NewFromInt64(int64(*input), 0, tScale)
	if err != nil {
		panic(err)
	}

	*result = common.Decimal{
		Decimal: nDec,
	}
	return true
}

func tryCastDecimalToFloat32(input *common.Decimal, result *float32, _ bool) bool {
	v, ok := input.Float64()
	util.AssertFunc(ok)
	*result = float32(v)
	return true
}

func tryCastBigintToInt32(input *common.Hugeint, result *int32, _ bool) bool {
	val := int32(input.Lower)
	if uint64(val) != input.Lower {
		fmt.Println(input)
	}
	*result = val
	return true
}

func tryCastBigintToFloat32(input *common.Hugeint, result *float32, _ bool) bool {
	switch input.Upper {
	case -1:
		*result = -float32(math.MaxUint64-input.Lower) - 1
	default:
		*result = float32(input.Lower) +
			float32(input.Upper)*float32(math.MaxUint64)
	}
	return true
}

func tryCastBigintToDecimal(input *common.Hugeint, result *common.Decimal, _ bool) bool {
	panic("usp")
}

func tryCastDecimalToDecimal(input *common.Decimal, result *common.Decimal, srcScale, dstScale int, _ bool) bool {
	//if numCast.srcScale == numCast.dstScale {
	//	*result = *input
	//} else if numCast.srcScale > numCast.dstScale {
	//
	//} else {
	//
	//}
	w, f, ok := input.Int64(dstScale)
	if ok {
		ndec, err := dec.NewFromInt64(w, f, dstScale)
		if err != nil {
			panic(err)
		}
		result.Decimal = ndec
	} else {

		ndec, err := dec.ParseExact(input.String(), dstScale)
		if err != nil {
			panic(err)
		}
		result.Decimal = ndec
	}
	return true
}

func tryCastFloat32ToInt32(input *float32, result *int32, _ bool) bool {
	*result = int32(*input)
	return true
}

func tryCastFloat32ToFloat64(input *float32, result *float64, _ bool) bool {
	*result = float64(*input)
	return true
}

func tryCastFloat32ToDecimal(input *float32, result *common.Decimal, _ bool) bool {
	res, err := dec.NewFromFloat64(float64(*input))
	if err != nil {
		panic(err)
	}
	result.Decimal = res
	return true
}

func tryCastVarcharToDate(input *common.String, result *common.Date, _ bool) bool {
	ti, err := time.Parse(time.DateOnly, input.String())
	if err != nil {
		panic(err)
	}
	y, m, d := ti.Date()
	*result = common.Date{
		Year:  int32(y),
		Month: int32(m),
		Day:   int32(d),
	}
	return true
}

func tryCastVarcharToInterval(input *common.String, result *common.Interval, _ bool) bool {
	is := input.String()
	seps := strings.Split(is, " ")
	if len(seps) != 2 {
		panic(fmt.Sprintf("invalid interval string %v", is))
	}
	switch strings.ToLower(seps[1]) {
	case "year":
		result.Unit = "year"
		parseInt, err := strconv.ParseInt(seps[0], 10, 64)
		if err != nil {
			panic(fmt.Sprintf("invalid interval string %v", is))
		}
		result.Year = int32(parseInt)
	case "month":
		result.Unit = "month"
		parseInt, err := strconv.ParseInt(seps[0], 10, 64)
		if err != nil {
			panic(fmt.Sprintf("invalid interval string %v", is))
		}
		result.Months = int32(parseInt)

	case "day":
		result.Unit = "day"
		parseInt, err := strconv.ParseInt(seps[0], 10, 64)
		if err != nil {
			panic(fmt.Sprintf("invalid interval string %v", is))
		}
		result.Days = int32(parseInt)
	default:
		panic(fmt.Sprintf("invalid interval unit %v", is))
	}
	return true
}

func castExec(
	source, result *chunk.Vector,
	count int,
) {
	panic("usp")
}

func AddCastToType(expr *Expr, dstTyp common.LType, tryCast bool) (*Expr, error) {
	var err error
	var retExpr *Expr
	if expr.DataTyp.Equal(dstTyp) {
		return expr, nil
	}

	castInfo := castFuncs.GetCastFunc(expr.DataTyp, dstTyp)

	args := []*Expr{
		expr, //expr to be cast
		//target type saved in DataTyp field
		{
			Typ:     ET_IConst,
			DataTyp: dstTyp,
		},
	}

	retExpr = &Expr{
		Typ:        ET_Func,
		SubTyp:     ET_SubFunc,
		Svalue:     ET_Cast.String(),
		DataTyp:    dstTyp,
		Children:   args,
		IsOperator: ET_Cast.isOperator(),
		BindInfo:   nil,
		FunImpl: &FunctionV2{
			_name:          ET_Cast.String(),
			_args:          []common.LType{expr.DataTyp, dstTyp},
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
	return cast.op(input, result, strict)
}

type BindCastFuncType func(input *BindCastInput, src, dst common.LType) *BoundCastInfo

type BindCastFunc struct {
	_fun  BindCastFuncType
	_info *BindCastInfo
}

type CastFunctionSet struct {
	_bindFuncs []*BindCastFunc
}

func NoCast(src *chunk.Vector, res *chunk.Vector, count int, params *CastParams) bool {
	res.Reference(src)
	return true
}

var (
	defNoCast = &BoundCastInfo{
		_fun: NoCast,
	}
)

func (castSet *CastFunctionSet) GetCastFunc(src, dst common.LType) *BoundCastInfo {
	if src.Equal(dst) {
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

func (castSet *CastFunctionSet) ImplicitCastCost(src, dst common.LType) int64 {
	return common.ImplicitCast(src, dst)
}
