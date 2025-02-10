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
	"unsafe"

	dec "github.com/govalues/decimal"

	"github.com/daviszhen/plan/pkg/chunk"
	"github.com/daviszhen/plan/pkg/common"
	"github.com/daviszhen/plan/pkg/util"
)

func StateSize[T any, STATE State[T]]() int {
	var val STATE
	size := unsafe.Sizeof(val)
	return int(size)
}

func UnaryAggregate[ResultT any, STATE State[ResultT], InputT any, OP AggrOp[ResultT, InputT]](
	inputTyp common.LType,
	retTyp common.LType,
	nullHandling FuncNullHandling,
	aop AggrOp[ResultT, InputT],
	sop StateOp[ResultT],
	addOp AddOp[ResultT, InputT],
	top TypeOp[ResultT],
) *FunctionV2 {
	var size aggrStateSize
	var init aggrInit
	var update aggrUpdate
	var combine aggrCombine
	var finalize aggrFinalize
	var simpleUpdate aggrSimpleUpdate
	size = func() int {
		var val State[ResultT]
		return int(unsafe.Sizeof(val))
	}
	init = func(pointer unsafe.Pointer) {
		aop.Init((*State[ResultT])(pointer), sop)
	}
	update = func(inputs []*chunk.Vector, data *AggrInputData, inputCount int, states *chunk.Vector, count int) {
		util.AssertFunc(inputCount == 1)
		UnaryScatter[ResultT, STATE, InputT, OP](inputs[0], states, data, count, aop, sop, addOp, top)
	}
	combine = func(source *chunk.Vector, target *chunk.Vector, data *AggrInputData, count int) {
		Combine[ResultT, STATE, InputT, OP](source, target, data, count, aop, sop, addOp, top)
	}
	finalize = func(states *chunk.Vector, data *AggrInputData, result *chunk.Vector, count int, offset int) {
		Finalize[ResultT, STATE, InputT, OP](states, data, result, count, offset, aop, sop, addOp, top)
	}
	simpleUpdate = func(inputs []*chunk.Vector, data *AggrInputData, inputCount int, state unsafe.Pointer, count int) {
		util.AssertFunc(inputCount == 1)
		UnaryUpdate[ResultT, STATE, InputT, OP](inputs[0], data, state, count, aop, sop, addOp, top)
	}
	return &FunctionV2{
		_funcTyp:      AggregateFuncType,
		_args:         []common.LType{inputTyp},
		_retType:      retTyp,
		_stateSize:    size,
		_init:         init,
		_update:       update,
		_combine:      combine,
		_finalize:     finalize,
		_nullHandling: nullHandling,
		_simpleUpdate: simpleUpdate,
	}
}

func GetSumAggr(pTyp common.PhyType) *FunctionV2 {
	switch pTyp {
	case common.INT32:
		fun := UnaryAggregate[common.Hugeint, State[common.Hugeint], int32, SumOp[common.Hugeint, int32]](
			common.IntegerType(),
			common.HugeintType(),
			DefaultNullHandling,
			SumOp[common.Hugeint, int32]{},
			&SumStateOp[common.Hugeint]{},
			&HugeintAdd{},
			&common.Hugeint{},
		)
		return fun
	case common.DECIMAL:
		fun := UnaryAggregate[common.Decimal, State[common.Decimal], common.Decimal, SumOp[common.Decimal, common.Decimal]](
			common.DecimalType(common.DecimalMaxWidth, 0),
			common.DecimalType(common.DecimalMaxWidth, 0),
			DefaultNullHandling,
			SumOp[common.Decimal, common.Decimal]{},
			&SumStateOp[common.Decimal]{},
			&DecimalAdd{},
			&common.Decimal{},
		)
		return fun
	default:
		panic("usp")
	}
}

func GetAvgAggr(retPhyTyp common.PhyType, inputPhyTyp common.PhyType) *FunctionV2 {
	switch inputPhyTyp {
	case common.INT32:
		switch retPhyTyp {
		case common.DOUBLE:
			var d Double
			fun := UnaryAggregate[float64, State[float64], int32, AvgOp[float64, int32]](
				common.IntegerType(),
				common.DoubleType(),
				DefaultNullHandling,
				AvgOp[float64, int32]{},
				&AvgStateOp[float64]{},
				&DoubleInt32Add{},
				d,
			)
			return fun
		default:
			panic("usp")
		}
	case common.DOUBLE:
		switch retPhyTyp {
		case common.DOUBLE:
			var d Double
			fun := UnaryAggregate[float64, State[float64], float64, AvgOp[float64, float64]](
				common.DoubleType(),
				common.DoubleType(),
				DefaultNullHandling,
				AvgOp[float64, float64]{},
				&AvgStateOp[float64]{},
				&DoubleAdd{},
				d,
			)
			return fun
		default:
			panic("usp")
		}
	case common.DECIMAL:
		switch retPhyTyp {
		case common.DECIMAL:
			fun := UnaryAggregate[common.Decimal, State[common.Decimal], common.Decimal, AvgOp[common.Decimal, common.Decimal]](
				common.DecimalType(common.DecimalMaxWidth, 0),
				common.DecimalType(common.DecimalMaxWidth, 0),
				DefaultNullHandling,
				AvgOp[common.Decimal, common.Decimal]{},
				&AvgStateOp[common.Decimal]{},
				&DecimalAdd{},
				&common.Decimal{},
			)
			return fun
		default:
			panic("usp")
		}
	default:
		panic("usp")
	}
}

func GetCountAggr(retPhyTyp common.PhyType, inputPhyTyp common.PhyType) *FunctionV2 {
	switch inputPhyTyp {
	case common.INT32:
		switch retPhyTyp {
		case common.INT32:
			fun := UnaryAggregate[common.Hugeint, State[common.Hugeint], int32, SumOp[common.Hugeint, int32]](
				common.IntegerType(),
				common.HugeintType(),
				DefaultNullHandling,
				CountOp[common.Hugeint, int32]{},
				&CountStateOp[common.Hugeint]{},
				&HugeintAdd{},
				&common.Hugeint{},
			)
			return fun
		default:
			panic("usp")
		}
	case common.INT64:
		switch retPhyTyp {
		case common.INT64:
			fun := UnaryAggregate[common.Hugeint, State[common.Hugeint], int64, SumOp[common.Hugeint, int64]](
				common.BigintType(),
				common.HugeintType(),
				DefaultNullHandling,
				CountOp[common.Hugeint, int64]{},
				&CountStateOp[common.Hugeint]{},
				&HugeintAddInt64{},
				&common.Hugeint{},
			)
			return fun
		default:
			panic("usp")
		}
	case common.VARCHAR:
		switch retPhyTyp {
		case common.INT32:
			fun := UnaryAggregate[common.Hugeint, State[common.Hugeint], int32, SumOp[common.Hugeint, int32]](
				common.VarcharType(),
				common.HugeintType(),
				DefaultNullHandling,
				CountOp[common.Hugeint, int32]{},
				&CountStateOp[common.Hugeint]{},
				&HugeintAdd{},
				&common.Hugeint{},
			)
			return fun
		default:
			panic("usp")
		}
	default:
		panic("usp")
	}
}

func GetMaxAggr(retPhyTyp common.PhyType, inputPhyTyp common.PhyType) *FunctionV2 {
	switch inputPhyTyp {
	case common.DECIMAL:
		switch retPhyTyp {
		case common.DECIMAL:
			fun := UnaryAggregate[common.Decimal, State[common.Decimal], common.Decimal, MinMaxOp[common.Decimal, common.Decimal]](
				common.DecimalType(common.DecimalMaxWidth, 0),
				common.DecimalType(common.DecimalMaxWidth, 0),
				DefaultNullHandling,
				MinMaxOp[common.Decimal, common.Decimal]{},
				&MaxStateOp[common.Decimal]{},
				&DecimalAdd{},
				&common.Decimal{},
			)
			return fun
		default:
			panic("usp")
		}
	default:
		panic("usp")
	}
}

func GetMinAggr(retPhyTyp common.PhyType, inputPhyTyp common.PhyType) *FunctionV2 {
	switch inputPhyTyp {
	case common.DECIMAL:
		switch retPhyTyp {
		case common.DECIMAL:
			fun := UnaryAggregate[common.Decimal, State[common.Decimal], common.Decimal, MinMaxOp[common.Decimal, common.Decimal]](
				common.DecimalType(common.DecimalMaxWidth, 0),
				common.DecimalType(common.DecimalMaxWidth, 0),
				DefaultNullHandling,
				MinMaxOp[common.Decimal, common.Decimal]{},
				&MinStateOp[common.Decimal]{},
				&DecimalAdd{},
				&common.Decimal{},
			)
			return fun
		default:
			panic("usp")
		}
	default:
		panic("usp")
	}
}

type StateType int

const (
	STATE_SUM StateType = iota
	STATE_AVG
	STATE_COUNT
	STATE_MAX
	STATE_MIN
)

type State[T any] struct {
	_typ   StateType
	_isset bool
	_value T
	_count uint64
}

func (state *State[T]) Init() {
	switch state._typ {
	case STATE_SUM:
		state._isset = false
	case STATE_AVG, STATE_COUNT:
		state._count = 0
	case STATE_MAX, STATE_MIN:
		state._isset = false
	default:
		panic("usp")
	}

}

func (state *State[T]) Combine(other *State[T], add TypeOp[T]) {
	switch state._typ {
	case STATE_SUM:
		state._isset = other._isset || state._isset
		add.Add(&state._value, &other._value)
	case STATE_AVG:
		add.Add(&state._value, &other._value)
		state._count += other._count
	case STATE_COUNT:
		state._count += other._count
	case STATE_MAX:
		panic("usp")
	case STATE_MIN:
		panic("usp")
	default:
		panic("usp")
	}

}

func (state *State[T]) SetIsset(b bool) {
	state._isset = b
}
func (state *State[T]) SetValue(val T) {
	state._value = val
}

func (state *State[T]) GetIsset() bool {
	return state._isset
}

func (state *State[T]) GetValue() T {
	return state._value
}

type StateOp[T any] interface {
	Init(*State[T])
	Combine(*State[T], *State[T], *AggrInputData, TypeOp[T])
	AddValues(*State[T], int)
}

type TypeOp[T any] interface {
	Add(*T, *T)
	Mul(*T, *T)
	Less(*T, *T) bool
	Greater(*T, *T) bool
}

type AddOp[ResultT any, InputT any] interface {
	AddNumber(*State[ResultT], *InputT, TypeOp[ResultT])
	AddConstant(*State[ResultT], *InputT, int, TypeOp[ResultT])
	Assign(*State[ResultT], *InputT)
	Execute(*State[ResultT], *InputT, TypeOp[ResultT])
}

type AggrOp[ResultT any, InputT any] interface {
	Init(*State[ResultT], StateOp[ResultT])
	Combine(*State[ResultT], *State[ResultT], *AggrInputData, StateOp[ResultT], TypeOp[ResultT])
	Operation(*State[ResultT], *InputT, *AggrUnaryInput, StateOp[ResultT], AddOp[ResultT, InputT], TypeOp[ResultT])
	ConstantOperation(*State[ResultT], *InputT, *AggrUnaryInput, int, StateOp[ResultT], AddOp[ResultT, InputT], TypeOp[ResultT])
	Finalize(*State[ResultT], *ResultT, *AggrFinalizeData)
	IgnoreNull() bool
}

type SumStateOp[T any] struct {
}

func (SumStateOp[T]) Init(s *State[T]) {
	s._typ = STATE_SUM
	s.Init()
}
func (SumStateOp[T]) Combine(
	src *State[T],
	target *State[T],
	_ *AggrInputData,
	top TypeOp[T]) {
	src.Combine(target, top)
}
func (SumStateOp[T]) AddValues(s *State[T], _ int) {

	s.SetIsset(true)
}

type AvgStateOp[T any] struct {
}

func (as *AvgStateOp[T]) Init(s *State[T]) {
	s._typ = STATE_AVG
	s.Init()
}

func (as *AvgStateOp[T]) Combine(
	src *State[T],
	target *State[T],
	_ *AggrInputData,
	top TypeOp[T]) {
	src.Combine(target, top)
}

func (as *AvgStateOp[T]) AddValues(s *State[T], cnt int) {
	s._count += uint64(cnt)
}

type CountStateOp[T any] struct {
}

func (as *CountStateOp[T]) Init(s *State[T]) {
	s._typ = STATE_COUNT
	s.Init()
}

func (as *CountStateOp[T]) Combine(
	src *State[T],
	target *State[T],
	_ *AggrInputData,
	top TypeOp[T]) {
	src.Combine(target, top)
}

func (as *CountStateOp[T]) AddValues(s *State[T], cnt int) {
	s._count += uint64(cnt)
}

type MaxStateOp[T any] struct {
}

func (as *MaxStateOp[T]) Init(s *State[T]) {
	s._typ = STATE_MAX
	s.Init()
}

func (as *MaxStateOp[T]) Combine(
	src *State[T],
	target *State[T],
	_ *AggrInputData,
	top TypeOp[T]) {
	if !src._isset {
		return
	}
	if !target._isset {
		target._isset = src._isset
		target._value = src._value
	} else if top.Less(&target._value, &src._value) {
		target._value = src._value
	}
}

func (as *MaxStateOp[T]) AddValues(s *State[T], cnt int) {

}

type MinStateOp[T any] struct {
}

func (as *MinStateOp[T]) Init(s *State[T]) {
	s._typ = STATE_MIN
	s.Init()
}

func (as *MinStateOp[T]) Combine(
	src *State[T],
	target *State[T],
	_ *AggrInputData,
	top TypeOp[T]) {
	if !src._isset {
		return
	}
	if !target._isset {
		target._isset = src._isset
		target._value = src._value
	} else if top.Greater(&target._value, &src._value) {
		target._value = src._value
	}
}

func (as *MinStateOp[T]) AddValues(s *State[T], cnt int) {

}

type HugeintAdd struct {
}

func (*HugeintAdd) addValue(result *common.Hugeint, value uint64, positive int) {
	result.Lower += value
	overflow := 0
	if result.Lower < value {
		overflow = 1
	}
	if overflow^positive == 0 {
		result.Upper += -1 + 2*int64(positive)
	}
}

func (hadd *HugeintAdd) AddNumber(state *State[common.Hugeint], input *int32, top TypeOp[common.Hugeint]) {
	pos := 0
	if *input >= 0 {
		pos = 1
	}

	hadd.addValue(&state._value, uint64(*input), pos)

}

func (*HugeintAdd) AddConstant(*State[common.Hugeint], *int32, int, TypeOp[common.Hugeint]) {
	//TODO:
	panic("usp")
}

func (*HugeintAdd) Assign(*State[common.Hugeint], *int32)                          { panic("usp") }
func (*HugeintAdd) Execute(*State[common.Hugeint], *int32, TypeOp[common.Hugeint]) { panic("usp") }

type HugeintAddInt64 struct {
}

func (*HugeintAddInt64) addValue(result *common.Hugeint, value uint64, positive int) {
	result.Lower += value
	overflow := 0
	if result.Lower < value {
		overflow = 1
	}
	if overflow^positive == 0 {
		result.Upper += -1 + 2*int64(positive)
	}
}

func (hadd *HugeintAddInt64) AddNumber(state *State[common.Hugeint], input *int64, top TypeOp[common.Hugeint]) {
	pos := 0
	if *input >= 0 {
		pos = 1
	}

	hadd.addValue(&state._value, uint64(*input), pos)

}

func (*HugeintAddInt64) AddConstant(*State[common.Hugeint], *int64, int, TypeOp[common.Hugeint]) {
	//TODO:
	panic("usp")
}

func (*HugeintAddInt64) Assign(*State[common.Hugeint], *int64)                          { panic("usp") }
func (*HugeintAddInt64) Execute(*State[common.Hugeint], *int64, TypeOp[common.Hugeint]) { panic("usp") }

type DecimalAdd struct {
}

func (dAdd *DecimalAdd) AddNumber(state *State[common.Decimal], input *common.Decimal, top TypeOp[common.Decimal]) {
	state._value.Add(&state._value, input)
}
func (*DecimalAdd) AddConstant(*State[common.Decimal], *common.Decimal, int, TypeOp[common.Decimal]) {
	panic("usp decimalAdd addconstant")
}

func (*DecimalAdd) Assign(s *State[common.Decimal], input *common.Decimal) {
	s._value = *input
}
func (*DecimalAdd) Execute(s *State[common.Decimal], input *common.Decimal, top TypeOp[common.Decimal]) {
	if s._typ == STATE_MAX {
		if top.Greater(input, &s._value) {
			s._value = *input
		}
	} else if s._typ == STATE_MIN {
		if top.Less(input, &s._value) {
			s._value = *input
		}
	} else {
		panic("usp")
	}
}

type DoubleAdd struct{}

func (DoubleAdd) AddNumber(
	state *State[float64],
	input *float64,
	top TypeOp[float64]) {
	state._value = state._value + *input
}

func (DoubleAdd) AddConstant(
	*State[float64],
	*float64,
	int,
	TypeOp[float64]) {
	panic("usp doubleAdd addconstant")
}

func (*DoubleAdd) Assign(*State[float64], *float64)                   { panic("usp") }
func (*DoubleAdd) Execute(*State[float64], *float64, TypeOp[float64]) { panic("usp") }

type DoubleInt32Add struct{}

func (DoubleInt32Add) AddNumber(
	state *State[float64],
	input *int32,
	top TypeOp[float64]) {
	state._value = state._value + float64(*input)
}

func (DoubleInt32Add) AddConstant(
	*State[float64],
	*int32,
	int,
	TypeOp[float64]) {
	panic("usp doubleAdd addconstant")
}

func (*DoubleInt32Add) Assign(*State[float64], *int32)                   { panic("usp") }
func (*DoubleInt32Add) Execute(*State[float64], *int32, TypeOp[float64]) { panic("usp") }

type Double float64

func (Double) Add(lhs, rhs *float64) {
	*lhs = (*lhs) + (*rhs)
}
func (Double) Mul(lhs, rhs *float64) {
	*lhs = (*lhs) * (*rhs)
}

func (Double) Less(lhs, rhs *float64) bool {
	return *lhs < *rhs
}
func (Double) Greater(lhs, rhs *float64) bool {
	return *lhs > *rhs
}

//func (*DoubleAdd)AddNumber(*State[ResultT], *InputT, TypeOp[ResultT]){}
//AddConstant(*State[ResultT], *InputT, int, TypeOp[ResultT])

type SumOp[ResultT any, InputT any] struct {
}

func (s SumOp[ResultT, InputT]) Init(
	s2 *State[ResultT],
	sop StateOp[ResultT]) {
	var val ResultT
	s2.SetValue(val)
	sop.Init(s2)
}

func (s SumOp[ResultT, InputT]) Combine(
	src *State[ResultT],
	target *State[ResultT],
	data *AggrInputData,
	sop StateOp[ResultT],
	top TypeOp[ResultT]) {
	sop.Combine(src, target, data, top)
}

func (s SumOp[ResultT, InputT]) Operation(
	s3 *State[ResultT],
	input *InputT,
	data *AggrUnaryInput,
	sop StateOp[ResultT],
	aop AddOp[ResultT, InputT],
	top TypeOp[ResultT]) {
	sop.AddValues(s3, 1)
	aop.AddNumber(s3, input, top)
}

func (s SumOp[ResultT, InputT]) ConstantOperation(
	s3 *State[ResultT],
	input *InputT,
	data *AggrUnaryInput,
	count int,
	sop StateOp[ResultT],
	aop AddOp[ResultT, InputT],
	top TypeOp[ResultT]) {
	sop.AddValues(s3, count)
	aop.AddConstant(s3, input, count, top)
}

func (s SumOp[ResultT, InputT]) Finalize(
	s3 *State[ResultT],
	target *ResultT,
	data *AggrFinalizeData) {

	if !s3.GetIsset() {
		data.ReturnNull()
	} else {
		*target = s3.GetValue()
	}
}

func (s SumOp[ResultT, InputT]) IgnoreNull() bool {
	return true
}

type AvgOp[ResultT any, InputT any] struct {
}

func (AvgOp[ResultT, InputT]) Init(
	s2 *State[ResultT],
	sop StateOp[ResultT]) {
	var val ResultT
	s2.SetValue(val)
	sop.Init(s2)
}

func (AvgOp[ResultT, InputT]) Combine(
	src *State[ResultT],
	target *State[ResultT],
	data *AggrInputData,
	sop StateOp[ResultT],
	top TypeOp[ResultT]) {
	sop.Combine(src, target, data, top)
}

func (AvgOp[ResultT, InputT]) Operation(
	s3 *State[ResultT],
	input *InputT,
	data *AggrUnaryInput,
	sop StateOp[ResultT],
	aop AddOp[ResultT, InputT],
	top TypeOp[ResultT]) {
	sop.AddValues(s3, 1)
	aop.AddNumber(s3, input, top)

}

func (AvgOp[ResultT, InputT]) ConstantOperation(
	s3 *State[ResultT],
	input *InputT,
	data *AggrUnaryInput,
	count int,
	sop StateOp[ResultT],
	aop AddOp[ResultT, InputT],
	top TypeOp[ResultT]) {
	sop.AddValues(s3, count)
	aop.AddConstant(s3, input, count, top)
}

func (AvgOp[ResultT, InputT]) Finalize(
	s3 *State[ResultT],
	target *ResultT,
	data *AggrFinalizeData) {
	if s3._count == 0 {
		data.ReturnNull()
	} else {
		var rt = s3.GetValue()
		switch v := any(rt).(type) {
		case float64:
			c := float64(s3._count)
			r := v / c
			*target = any(r).(ResultT)
		case common.Decimal:
			c := dec.MustNew(int64(s3._count), 0)
			quo, err := v.Quo(c)
			if err != nil {
				panic(err)
			}
			res := common.Decimal{
				Decimal: quo,
			}
			*target = any(res).(ResultT)
		default:
			panic("unmatched cast")
		}
	}
}

func (AvgOp[ResultT, InputT]) IgnoreNull() bool {
	return true
}

type CountOp[ResultT any, InputT any] struct {
}

func (CountOp[ResultT, InputT]) Init(
	s2 *State[ResultT],
	sop StateOp[ResultT]) {
	var val ResultT
	s2.SetValue(val)
	sop.Init(s2)
}

func (CountOp[ResultT, InputT]) Combine(
	src *State[ResultT],
	target *State[ResultT],
	data *AggrInputData,
	sop StateOp[ResultT],
	top TypeOp[ResultT]) {
	sop.Combine(src, target, data, top)
}

func (CountOp[ResultT, InputT]) Operation(
	s3 *State[ResultT],
	input *InputT,
	data *AggrUnaryInput,
	sop StateOp[ResultT],
	aop AddOp[ResultT, InputT],
	top TypeOp[ResultT]) {
	sop.AddValues(s3, 1)
	aop.AddNumber(s3, input, top)

}

func (CountOp[ResultT, InputT]) ConstantOperation(
	s3 *State[ResultT],
	input *InputT,
	data *AggrUnaryInput,
	count int,
	sop StateOp[ResultT],
	aop AddOp[ResultT, InputT],
	top TypeOp[ResultT]) {
	sop.AddValues(s3, count)
	aop.AddConstant(s3, input, count, top)
}

func (CountOp[ResultT, InputT]) Finalize(
	s3 *State[ResultT],
	target *ResultT,
	data *AggrFinalizeData) {
	if s3._count == 0 {
		data.ReturnNull()
	} else {
		ret := common.Hugeint{
			Lower: s3._count,
		}
		*target = any(ret).(ResultT)
	}
}

func (CountOp[ResultT, InputT]) IgnoreNull() bool {
	return true
}

type MinMaxOp[ResultT any, InputT any] struct {
}

func (MinMaxOp[ResultT, InputT]) Init(
	s2 *State[ResultT],
	sop StateOp[ResultT]) {
	var val ResultT
	s2.SetValue(val)
	sop.Init(s2)
}

func (MinMaxOp[ResultT, InputT]) Combine(
	src *State[ResultT],
	target *State[ResultT],
	data *AggrInputData,
	sop StateOp[ResultT],
	top TypeOp[ResultT]) {
	sop.Combine(src, target, data, top)
}

func (MinMaxOp[ResultT, InputT]) Operation(
	s3 *State[ResultT],
	input *InputT,
	data *AggrUnaryInput,
	sop StateOp[ResultT],
	aop AddOp[ResultT, InputT],
	top TypeOp[ResultT]) {
	if !s3._isset {
		aop.Assign(s3, input)
		s3._isset = true
	} else {
		aop.Execute(s3, input, top)
	}
}

func (MinMaxOp[ResultT, InputT]) ConstantOperation(
	s3 *State[ResultT],
	input *InputT,
	data *AggrUnaryInput,
	count int,
	sop StateOp[ResultT],
	aop AddOp[ResultT, InputT],
	top TypeOp[ResultT]) {
	if !s3._isset {
		aop.Assign(s3, input)
		s3._isset = true
	} else {
		aop.Execute(s3, input, top)
	}
}

func (MinMaxOp[ResultT, InputT]) Finalize(
	s3 *State[ResultT],
	target *ResultT,
	data *AggrFinalizeData) {
	if !s3._isset {
		data.ReturnNull()
	} else {
		*target = s3._value
	}
}

func (MinMaxOp[ResultT, InputT]) IgnoreNull() bool {
	return true
}

func UnaryScatter[ResultT any, STATE State[ResultT], InputT any, OP AggrOp[ResultT, InputT]](
	input *chunk.Vector,
	states *chunk.Vector,
	data *AggrInputData,
	count int,
	aop AggrOp[ResultT, InputT],
	sop StateOp[ResultT],
	addOp AddOp[ResultT, InputT],
	top TypeOp[ResultT],
) {
	if input.PhyFormat().IsConst() &&
		states.PhyFormat().IsConst() {
		if aop.IgnoreNull() && chunk.IsNullInPhyFormatConst(input) {
			return
		}
		inputSlice := chunk.GetSliceInPhyFormatConst[InputT](input)
		statesPtrSlice := chunk.GetSliceInPhyFormatConst[unsafe.Pointer](states)
		inputData := NewAggrUnaryInput(data, chunk.GetMaskInPhyFormatConst(input))
		aop.ConstantOperation((*State[ResultT])(statesPtrSlice[0]), &inputSlice[0], inputData, count, sop, addOp, top)
	} else if input.PhyFormat().IsFlat() && states.PhyFormat().IsFlat() {
		inputSlice := chunk.GetSliceInPhyFormatFlat[InputT](input)
		statesPtrSlice := chunk.GetSliceInPhyFormatFlat[unsafe.Pointer](states)
		UnaryFlatLoop[ResultT, STATE, InputT, OP](
			inputSlice,
			data,
			statesPtrSlice,
			chunk.GetMaskInPhyFormatFlat(input),
			count,
			aop,
			sop,
			addOp,
			top,
		)
	} else {
		var idata, sdata chunk.UnifiedFormat
		input.ToUnifiedFormat(count, &idata)
		states.ToUnifiedFormat(count, &sdata)
		UnaryScatterLoop[ResultT, STATE, InputT](
			chunk.GetSliceInPhyFormatUnifiedFormat[InputT](&idata),
			data,
			chunk.GetSliceInPhyFormatUnifiedFormat[unsafe.Pointer](&sdata),
			idata.Sel,
			sdata.Sel,
			idata.Mask,
			count,
			aop,
			sop,
			addOp,
			top,
		)
	}
}

func UnaryFlatLoop[ResultT any, STATE State[ResultT], InputT any, OP AggrOp[ResultT, InputT]](
	inputSlice []InputT,
	data *AggrInputData,
	statesPtrSlice []unsafe.Pointer,
	mask *util.Bitmap,
	count int,
	aop AggrOp[ResultT, InputT],
	sop StateOp[ResultT],
	addOp AddOp[ResultT, InputT],
	top TypeOp[ResultT],
) {
	if aop.IgnoreNull() && !mask.AllValid() {
		input := NewAggrUnaryInput(data, mask)
		baseIdx := &input._inputIdx
		*baseIdx = 0
		eCnt := util.EntryCount(count)
		for eIdx := 0; eIdx < eCnt; eIdx++ {
			e := mask.GetEntry(uint64(eIdx))
			next := min(*baseIdx+8, count)
			if util.AllValidInEntry(e) {
				for ; *baseIdx < next; *baseIdx++ {
					aop.Operation((*State[ResultT])(statesPtrSlice[*baseIdx]), &inputSlice[*baseIdx], input, sop, addOp, top)
				}
			} else if util.NoneValidInEntry(e) {
				*baseIdx = next
				continue
			} else {
				start := *baseIdx
				for ; *baseIdx < next; *baseIdx++ {
					if util.RowIsValidInEntry(e, uint64(*baseIdx-start)) {
						aop.Operation((*State[ResultT])(statesPtrSlice[*baseIdx]), &inputSlice[*baseIdx], input, sop, addOp, top)
					}
				}
			}
		}
	} else {
		input := NewAggrUnaryInput(data, mask)
		i := &input._inputIdx
		for *i = 0; *i < count; *i++ {
			aop.Operation((*State[ResultT])(statesPtrSlice[*i]), &inputSlice[*i], input, sop, addOp, top)
		}
	}
}

func UnaryScatterLoop[ResultT any, STATE State[ResultT], InputT any](
	inputSlice []InputT,
	data *AggrInputData,
	statesPtrSlice []unsafe.Pointer,
	isel *chunk.SelectVector,
	ssel *chunk.SelectVector,
	mask *util.Bitmap,
	count int,
	aop AggrOp[ResultT, InputT],
	sop StateOp[ResultT],
	addOp AddOp[ResultT, InputT],
	top TypeOp[ResultT],
) {
	if aop.IgnoreNull() && !mask.AllValid() {
		input := NewAggrUnaryInput(data, mask)
		for i := 0; i < count; i++ {
			input._inputIdx = isel.GetIndex(i)
			sidx := ssel.GetIndex(i)
			if mask.RowIsValid(uint64(input._inputIdx)) {
				aop.Operation((*State[ResultT])(statesPtrSlice[sidx]), &inputSlice[input._inputIdx], input, sop, addOp, top)
			}
		}
	} else {
		input := NewAggrUnaryInput(data, mask)
		for i := 0; i < count; i++ {
			input._inputIdx = isel.GetIndex(i)
			sidx := ssel.GetIndex(i)
			aop.Operation((*State[ResultT])(statesPtrSlice[sidx]), &inputSlice[input._inputIdx], input, sop, addOp, top)
		}
	}
}

func Combine[ResultT any, STATE State[ResultT], InputT any, OP AggrOp[ResultT, InputT]](
	source *chunk.Vector,
	target *chunk.Vector,
	data *AggrInputData,
	count int,
	aop AggrOp[ResultT, InputT],
	sop StateOp[ResultT],
	addOp AddOp[ResultT, InputT],
	top TypeOp[ResultT],
) {
	util.AssertFunc(source.Typ().IsPointer())
	util.AssertFunc(target.Typ().IsPointer())
	sourcePtrSlice := chunk.GetSliceInPhyFormatFlat[unsafe.Pointer](source)
	targetPtrSlice := chunk.GetSliceInPhyFormatFlat[unsafe.Pointer](target)
	for i := 0; i < count; i++ {
		aop.Combine((*State[ResultT])(sourcePtrSlice[i]),
			(*State[ResultT])(targetPtrSlice[i]),
			data,
			sop,
			top,
		)
	}
}

func Finalize[ResultT any, STATE State[ResultT], InputT any, OP AggrOp[ResultT, InputT]](
	states *chunk.Vector,
	data *AggrInputData,
	result *chunk.Vector,
	count int,
	offset int,
	aop AggrOp[ResultT, InputT],
	sop StateOp[ResultT],
	addOp AddOp[ResultT, InputT],
	top TypeOp[ResultT],
) {
	if states.PhyFormat().IsConst() {
		result.SetPhyFormat(chunk.PF_CONST)
		statePtrSlice := chunk.GetSliceInPhyFormatFlat[unsafe.Pointer](states)
		resultSlice := chunk.GetSliceInPhyFormatFlat[ResultT](result)
		final := NewAggrFinalizeData(result, data)
		aop.Finalize((*State[ResultT])(statePtrSlice[0]), &resultSlice[0], final)
	} else {
		util.AssertFunc(states.PhyFormat().IsFlat())
		result.SetPhyFormat(chunk.PF_FLAT)
		statePtrSlice := chunk.GetSliceInPhyFormatFlat[unsafe.Pointer](states)
		resultSlice := chunk.GetSliceInPhyFormatFlat[ResultT](result)
		final := NewAggrFinalizeData(result, data)
		for i := 0; i < count; i++ {
			final._resultIdx = i + offset
			aop.Finalize((*State[ResultT])(statePtrSlice[i]), &resultSlice[final._resultIdx], final)
		}
	}
}

func UnaryUpdate[ResultT any, STATE State[ResultT], InputT any, OP AggrOp[ResultT, InputT]](
	input *chunk.Vector,
	data *AggrInputData,
	statePtr unsafe.Pointer,
	count int,
	aop AggrOp[ResultT, InputT],
	sop StateOp[ResultT],
	addOp AddOp[ResultT, InputT],
	top TypeOp[ResultT],
) {
	switch input.PhyFormat() {
	case chunk.PF_CONST:
		if aop.IgnoreNull() && chunk.IsNullInPhyFormatConst(input) {
			return
		}
		inputSlice := chunk.GetSliceInPhyFormatFlat[InputT](input)
		inputData := NewAggrUnaryInput(data, chunk.GetMaskInPhyFormatConst(input))
		aop.ConstantOperation((*State[ResultT])(statePtr), &inputSlice[0], inputData, count, sop, addOp, top)
	case chunk.PF_FLAT:
		inputSlice := chunk.GetSliceInPhyFormatFlat[InputT](input)
		UnaryFlatUpdateLoop[ResultT, STATE, InputT, OP](
			inputSlice,
			data,
			statePtr,
			count,
			chunk.GetMaskInPhyFormatFlat(input),
			aop,
			sop,
			addOp,
			top,
		)
	default:
		var idata chunk.UnifiedFormat
		input.ToUnifiedFormat(count, &idata)
		UnaryUpdateLoop[ResultT, STATE, InputT, OP](
			chunk.GetSliceInPhyFormatUnifiedFormat[InputT](&idata),
			data,
			statePtr,
			count,
			idata.Mask,
			idata.Sel,
			aop,
			sop,
			addOp,
			top,
		)
	}
}

func UnaryFlatUpdateLoop[ResultT any, STATE State[ResultT], InputT any, OP AggrOp[ResultT, InputT]](
	inputSlice []InputT,
	data *AggrInputData,
	statePtr unsafe.Pointer,
	count int,
	mask *util.Bitmap,
	aop AggrOp[ResultT, InputT],
	sop StateOp[ResultT],
	addOp AddOp[ResultT, InputT],
	top TypeOp[ResultT],
) {
	input := NewAggrUnaryInput(data, mask)
	baseIdx := &input._inputIdx
	*baseIdx = 0
	eCnt := util.EntryCount(count)
	for eIdx := 0; eIdx < eCnt; eIdx++ {
		e := mask.GetEntry(uint64(eIdx))
		next := min(*baseIdx+8, count)
		if !aop.IgnoreNull() || util.AllValidInEntry(e) {
			for ; *baseIdx < next; *baseIdx++ {
				aop.Operation((*State[ResultT])(statePtr), &inputSlice[*baseIdx], input, sop, addOp, top)
			}
		} else if util.NoneValidInEntry(e) {
			*baseIdx = next
			continue
		} else {
			start := *baseIdx
			for ; *baseIdx < next; *baseIdx++ {
				if util.RowIsValidInEntry(e, uint64(*baseIdx-start)) {
					aop.Operation((*State[ResultT])(statePtr), &inputSlice[*baseIdx], input, sop, addOp, top)
				}
			}
		}
	}
}

func UnaryUpdateLoop[ResultT any, STATE State[ResultT], InputT any, OP AggrOp[ResultT, InputT]](
	inputSlice []InputT,
	data *AggrInputData,
	statePtr unsafe.Pointer,
	count int,
	mask *util.Bitmap,
	selVec *chunk.SelectVector,
	aop AggrOp[ResultT, InputT],
	sop StateOp[ResultT],
	addOp AddOp[ResultT, InputT],
	top TypeOp[ResultT],
) {
	input := NewAggrUnaryInput(data, mask)
	if aop.IgnoreNull() && !mask.AllValid() {
		for i := 0; i < count; i++ {
			input._inputIdx = selVec.GetIndex(i)
			if mask.RowIsValid(uint64(input._inputIdx)) {
				aop.Operation((*State[ResultT])(statePtr), &inputSlice[input._inputIdx], input, sop, addOp, top)
			}
		}
	} else {
		for i := 0; i < count; i++ {
			input._inputIdx = selVec.GetIndex(i)
			aop.Operation((*State[ResultT])(statePtr), &inputSlice[input._inputIdx], input, sop, addOp, top)
		}
	}
}

func FinalizeStates(
	layout *TupleDataLayout,
	addresses *chunk.Vector,
	result *chunk.Chunk,
	aggrIdx int,
) {
	AddInPlace(addresses, int64(layout.aggrOffset()), result.Card())

	aggrObjs := layout._aggregates
	var target *chunk.Vector
	for i := 0; i < len(aggrObjs); i++ {
		aggr := aggrObjs[i]
		sameType := aggr._retType == aggr._func._retType.GetInternalType()
		targetOffset := aggrIdx + i
		if sameType {
			target = result.Data[targetOffset]
		} else {
			target = chunk.NewVector2(aggr._func._retType, util.DefaultVectorSize)
		}

		aggr._func._finalize(addresses, NewAggrInputData(), target, result.Card(), 0)

		if !sameType {
			//TODO: put the cast in build plan stage
			//cast
			src := target.Typ()
			dst := result.Data[targetOffset].Typ()
			castInfo := castFuncs.GetCastFunc(src, dst)
			castParams := &CastParams{}
			castInfo._fun(target, result.Data[targetOffset], result.Card(), castParams)
		}

		//next aggr state
		AddInPlace(addresses, int64(aggr._payloadSize), result.Card())
	}
}
