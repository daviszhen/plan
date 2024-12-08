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
	"unsafe"

	"github.com/daviszhen/plan/pkg/chunk"
	"github.com/daviszhen/plan/pkg/common"
	"github.com/daviszhen/plan/pkg/util"
)

var aggNames = map[string]int{
	"min":   1,
	"count": 1,
	"sum":   1,
	"max":   1,
	"avg":   1,
}

func IsAgg(name string) bool {
	if _, ok := aggNames[name]; ok {
		return ok
	}
	return false
}

type FuncNullHandling int

const (
	DefaultNullHandling FuncNullHandling = 0
	SpecialHandling     FuncNullHandling = 1
)

type FuncSideEffects int

const (
	NoSideEffects  FuncSideEffects = 0
	HasSideEffects FuncSideEffects = 1
)

type FuncType int

const (
	ScalarFuncType    FuncType = 0
	AggregateFuncType FuncType = 1
	TableFuncType     FuncType = 2
)

type FunctionV2 struct {
	_name         string
	_args         []common.LType
	_retType      common.LType
	_funcTyp      FuncType
	_sideEffects  FuncSideEffects
	_nullHandling FuncNullHandling

	_scalar        ScalarFunc
	_bind          bindScalarFunc
	_boundCastInfo *BoundCastInfo

	_stateSize aggrStateSize
	_init      aggrInit
	_update    aggrUpdate
	_combine   aggrCombine
	_finalize  aggrFinalize
	//_func         aggrFunction
	_simpleUpdate aggrSimpleUpdate
	//_window       aggrWindow
}

func (fun *FunctionV2) Copy() *FunctionV2 {
	ret := &FunctionV2{
		_name:         fun._name,
		_args:         util.CopyTo(fun._args),
		_retType:      fun._retType,
		_funcTyp:      fun._funcTyp,
		_sideEffects:  fun._sideEffects,
		_nullHandling: fun._nullHandling,
		_scalar:       fun._scalar,
		_bind:         fun._bind,
		_stateSize:    fun._stateSize,
		_init:         fun._init,
		_update:       fun._update,
		_combine:      fun._combine,
		_finalize:     fun._finalize,
		//_func:         fun._func,
		_simpleUpdate: fun._simpleUpdate,
		//_window:       fun._window,
	}
	return ret
}

type ScalarFunc func(*chunk.Chunk, *ExprState, *chunk.Vector)

type aggrStateSize func() int
type aggrInit func(pointer unsafe.Pointer)
type aggrUpdate func([]*chunk.Vector, *AggrInputData, int, *chunk.Vector, int)
type aggrCombine func(*chunk.Vector, *chunk.Vector, *AggrInputData, int)
type aggrFinalize func(*chunk.Vector, *AggrInputData, *chunk.Vector, int, int)

// type aggrFunction func(*AggrFunc, []*Expr)
type aggrSimpleUpdate func([]*chunk.Vector, *AggrInputData, int, unsafe.Pointer, int)

//type aggrWindow func([]*Vector, *Bitmap, *AggrInputData)

type bindScalarFunc func(fun *FunctionV2, args []*Expr) *FunctionData

const (
	DecimalBindData    = "decimal"
	DecimalNegBindData = "decimalNeg"
)

type FunctionData struct {
	_funDataTyp    string
	_checkOverflow bool
	_boundTyp      common.LTypeId
}

func (fdata FunctionData) copy() *FunctionData {
	return &FunctionData{}
}

type FunctionSet struct {
	_name      string
	_functions []*FunctionV2
	_funcTyp   FuncType
}

func NewFunctionSet(name string, ftyp FuncType) *FunctionSet {
	ret := &FunctionSet{
		_name:    name,
		_funcTyp: ftyp,
	}
	return ret
}

func (set *FunctionSet) Add(fun *FunctionV2) {
	set._functions = append(set._functions, fun)
}

func (set *FunctionSet) GetFunc(offset int) *FunctionV2 {
	util.AssertFunc(offset < len(set._functions))
	//!!!note copy instead of referring directly
	return set._functions[offset].Copy()
}

func (set *FunctionSet) GetFuncByArgs(args []common.LType) *FunctionV2 {
	idx := gFuncBinder.BindFunc(set._name, set, args)
	if idx == -1 {
		panic(fmt.Sprintf("function %s impl not found", set._name))
	}
	return set.GetFunc(idx)
}

var gFuncBinder FunctionBinder

type FunctionBinder struct {
}

func (binder *FunctionBinder) BindScalarFunc(
	name string,
	args []*Expr,
	subTyp ET_SubTyp,
	isOperator bool,
) *Expr {
	fset := scalarFuncs[name]
	if fset == nil {
		panic(fmt.Sprintf("function %s not found", name))
	}
	best := binder.BindFunc2(name, fset, args)
	if best == -1 {
		panic(fmt.Sprintf("function %s not found %v", name, args))
	}

	fun := fset.GetFunc(best)
	return binder.BindScalarFunc2(fun, args, subTyp, isOperator)
}

func (binder *FunctionBinder) BindScalarFunc2(
	fun *FunctionV2,
	args []*Expr,
	subTyp ET_SubTyp,
	isOperator bool,
) *Expr {
	var bindInfo *FunctionData
	if fun._bind != nil {
		bindInfo = fun._bind(fun, args)
	}

	return &Expr{
		Typ:        ET_Func,
		SubTyp:     subTyp,
		Svalue:     fun._name,
		DataTyp:    fun._retType,
		Children:   args,
		IsOperator: isOperator,
		BindInfo:   bindInfo,
		FunImpl:    fun,
	}
}

func (binder *FunctionBinder) CastToFuncArgs(fun *FunctionV2, args []*Expr) {
	var err error
	for i := 0; i < len(args); i++ {
		targetType := fun._args[i]
		if args[i].DataTyp.Id == common.LTID_LAMBDA {
			continue
		}
		castRes := RequireCast(args[i].DataTyp, targetType)
		if castRes == DIFFERENT_TYPES {
			args[i], err = AddCastToType(args[i], targetType, false)
			if err != nil {
				panic(err)
			}
		}
	}
}

func (binder *FunctionBinder) BindAggrFunc(
	name string,
	args []*Expr,
	subTyp ET_SubTyp,
	isOperator bool,
) *Expr {
	fset := aggrFuncs[name]
	if fset == nil {
		panic(fmt.Sprintf("function %s not found", name))
	}
	best := binder.BindFunc2(name, fset, args)
	if best == -1 {
		panic(fmt.Sprintf("function %s not found %v", name, args))
	}

	fun := fset.GetFunc(best)
	return binder.BindAggrFunc2(fun, args, subTyp, isOperator)
}

func (binder *FunctionBinder) BindAggrFunc2(
	fun *FunctionV2,
	args []*Expr,
	subTyp ET_SubTyp,
	isOperator bool,
) *Expr {
	var bindInfo *FunctionData
	if fun._bind != nil {
		bindInfo = fun._bind(fun, args)
	}
	binder.CastToFuncArgs(fun, args)
	return &Expr{
		Typ:        ET_Func,
		SubTyp:     subTyp,
		Svalue:     fun._name,
		DataTyp:    fun._retType,
		Children:   args,
		IsOperator: isOperator,
		BindInfo:   bindInfo,
		FunImpl:    fun,
	}
}

type LTypeCmpResult int

const (
	IDENTICAL_TYPE  LTypeCmpResult = 0
	TARGET_IS_ANY   LTypeCmpResult = 1
	DIFFERENT_TYPES LTypeCmpResult = 2
)

func RequireCast(src, dst common.LType) LTypeCmpResult {
	if dst.Id == common.LTID_ANY {
		return TARGET_IS_ANY
	}
	if src.Id == dst.Id {
		return IDENTICAL_TYPE
	}
	return DIFFERENT_TYPES
}

func (binder *FunctionBinder) BindFunc2(
	name string,
	set *FunctionSet,
	args []*Expr,
) int {
	args2 := make([]common.LType, 0)
	for _, arg := range args {
		args2 = append(args2, arg.DataTyp)
	}
	return binder.BindFuncByArgs(name, set, args2)
}

func (binder *FunctionBinder) BindFunc(
	name string,
	set *FunctionSet,
	args []common.LType,
) int {
	return binder.BindFuncByArgs(name, set, args)
}

func (binder *FunctionBinder) BindFuncByArgs(
	name string,
	set *FunctionSet,
	args []common.LType,
) int {
	funs := binder.BindFuncByArgs2(name, set, args)
	if len(funs) == 0 {
		return -1
	} else if len(funs) > 1 {
		panic(fmt.Sprintf("multiple func imp for %v", name))
	}

	return funs[0]
}

func (binder *FunctionBinder) BindFuncByArgs2(
	name string,
	set *FunctionSet,
	args []common.LType,
) []int {
	bestFunc := -1
	lowestCost := int64(math.MaxInt64)
	candidates := make([]int, 0)
	for i, fun := range set._functions {
		cost := binder.BindFuncCost(fun, args)
		if cost < 0 {
			continue
		}
		if cost == lowestCost {
			candidates = append(candidates, i)
			continue
		}
		if cost > lowestCost {
			continue
		}
		candidates = candidates[:0]
		lowestCost = cost
		bestFunc = i
	}
	if bestFunc == -1 {
		return candidates
	}
	candidates = append(candidates, bestFunc)
	return candidates
}

func (binder *FunctionBinder) BindFuncCost(
	fun *FunctionV2,
	args []common.LType,
) int64 {
	if len(fun._args) != len(args) {
		return -1
	}

	cost := int64(0)
	for i, arg := range args {
		castCost := castFuncs.ImplicitCastCost(arg, fun._args[i])
		if castCost >= 0 {
			cost += castCost
		} else {
			return -1
		}
	}

	return cost
}

func init() {
	castFuncs = NewCastFunctionSet()
	RegisterAggrs()
	RegisterOps()
}

type FunctionList map[string]*FunctionSet

func (flist FunctionList) Add(name string, set *FunctionSet) {
	if _, ok := flist[name]; ok {
		panic(fmt.Sprintf("function %s already registered", name))
	}
	flist[name] = set
}

var scalarFuncs = make(FunctionList)
var aggrFuncs = make(FunctionList)
var castFuncs *CastFunctionSet

func RegisterOps() {
	AddFunc{}.Register(scalarFuncs)
	SubFunc{}.Register(scalarFuncs)
	MultiplyFunc{}.Register(scalarFuncs)
	DevideFunc{}.Register(scalarFuncs)
	LikeFunc{}.Register(scalarFuncs)
	NotLikeFunc{}.Register(scalarFuncs)
	InFunc{}.Register(scalarFuncs)
	EqualFunc{}.Register(scalarFuncs)
	NotEqualFunc{}.Register(scalarFuncs)
	BoolFunc{}.Register(scalarFuncs)
	Greater{}.Register(scalarFuncs)
	GreaterThan{}.Register(scalarFuncs)
	DateAdd{}.Register(scalarFuncs)
	DateSub{}.Register(scalarFuncs)
	LessFunc{}.Register(scalarFuncs)
	LessEqualFunc{}.Register(scalarFuncs)
	CaseFunc{}.Register(scalarFuncs)
	ExtractFunc{}.Register(scalarFuncs)
	SubstringFunc{}.Register(scalarFuncs)
}

func RegisterAggrs() {
	SumFunc{}.Register(aggrFuncs)
	AvgFunc{}.Register(aggrFuncs)
	CountFunc{}.Register(aggrFuncs)
	MaxFunc{}.Register(aggrFuncs)
	MinFunc{}.Register(aggrFuncs)
}
