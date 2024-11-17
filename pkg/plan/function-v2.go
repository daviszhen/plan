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
)

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
	ScalarFunc    FuncType = 0
	AggregateFunc FuncType = 1
	TableFunc     FuncType = 2
)

type FunctionV2 struct {
	_name         string
	_args         []LType
	_retType      LType
	_funcTyp      FuncType
	_sideEffects  FuncSideEffects
	_nullHandling FuncNullHandling

	_scalar scalarFunc
	_bind   bindScalarFunc

	_stateSize aggrStateSize
	_init      aggrInit
	_update    aggrUpdate
	_combine   aggrCombine
	_finalize  aggrFinalize
	//_func         aggrFunction
	_simpleUpdate aggrSimpleUpdate
	//_window       aggrWindow
}

type scalarFunc func(*Chunk, *ExprState, *Vector)

type aggrStateSize func() int
type aggrInit func(pointer unsafe.Pointer)
type aggrUpdate func([]*Vector, *AggrInputData, int, *Vector, int)
type aggrCombine func(*Vector, *Vector, *AggrInputData, int)
type aggrFinalize func(*Vector, *AggrInputData, *Vector, int, int)

// type aggrFunction func(*AggrFunc, []*Expr)
type aggrSimpleUpdate func([]*Vector, *AggrInputData, int, unsafe.Pointer, int)

//type aggrWindow func([]*Vector, *Bitmap, *AggrInputData)

type bindScalarFunc func(fun *FunctionV2, args []*Expr) *FunctionData

type FunctionData struct{}

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
	assertFunc(offset < len(set._functions))
	return set._functions[offset]
}

func (set *FunctionSet) GetFuncByArgs(args []LType) *FunctionV2 {
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
	argsTypes []LType,
	isOperator bool,
) *Expr {
	fset := scalarFuncs[name]
	if fset == nil {
		panic(fmt.Sprintf("function %s not found", name))
	}
	best := binder.BindFunc(name, fset, argsTypes)
	if best == -1 {
		return nil
	}

	fun := fset.GetFunc(best)
	return binder.BindScalarFunc2(fun, args, argsTypes, isOperator)
}

func (binder *FunctionBinder) BindScalarFunc2(
	fun *FunctionV2,
	args []*Expr,
	argsTypes []LType,
	isOperator bool,
) *Expr {
	var bindInfo *FunctionData
	if fun._bind != nil {
		bindInfo = fun._bind(fun, args)
	}

}

func (binder *FunctionBinder) BindFunc(
	name string,
	set *FunctionSet,
	args []LType,
) int {
	return binder.BindFuncByArgs(name, set, args)
}

func (binder *FunctionBinder) BindFuncByArgs(
	name string,
	set *FunctionSet,
	args []LType,
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
	args []LType,
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
	args []LType,
) int64 {
	if len(fun._args) != len(args) {
		return -1
	}

	cost := int64(0)
	for i, arg := range args {
		castCost := implicitCast(arg, fun._args[i])
		if castCost >= 0 {
			cost += castCost
		} else {
			return -1
		}
	}

	return cost
}

func init() {

}

var scalarFuncs = make(map[string]*FunctionSet)

func registerOps() {

}
