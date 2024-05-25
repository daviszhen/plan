package main

import (
	"unsafe"
)

func StateSize[T any, STATE State[T]]() int {
	var val STATE
	size := unsafe.Sizeof(val)
	return int(size)
}

func UnaryAggregate[ResultT any, STATE State[ResultT], InputT any, OP AggrOp[ResultT, InputT]](
	inputTyp LType,
	retTyp LType,
	nullHandling FuncNullHandling,
	aop AggrOp[ResultT, InputT],
	sop StateOp[ResultT],
	addOp AddOp[ResultT, InputT],
	top TypeOp[ResultT],
) *AggrFunc {
	var init aggrInit
	var update aggrUpdate
	init = func(pointer unsafe.Pointer) {
		aop.Init(*(*STATE)(pointer), sop)
	}
	update = func(inputs []*Vector, data *AggrInputData, inputCount int, states *Vector, count int) {
		assertFunc(inputCount == 1)
		UnaryScatter[ResultT, STATE, InputT, OP](inputs[0], states, data, count, aop, sop, addOp, top)
	}
	return &AggrFunc{
		_args:         []LType{inputTyp},
		_retType:      retTyp,
		_stateSize:    StateSize[ResultT, STATE],
		_init:         init,
		_update:       update,
		_combine:      nil,
		_finalize:     nil,
		_nullHandling: nullHandling,
		_simpleUpdate: nil,
	}
}

func GetSumAggr(pTyp PhyType) *AggrFunc {
	switch pTyp {
	case INT32:
		fun := UnaryAggregate[Hugeint, *SumState[Hugeint], int32, SumOp[Hugeint, int32]](
			integer(),
			hugeint(),
			DEFAULT_NULL_HANDLING,
			SumOp[Hugeint, int32]{},
			&SumStateOp[Hugeint]{},
			&HugeintAdd{},
			&Hugeint{},
		)
		return fun
	default:
		panic("usp")
	}
}

type TypeOp[T any] interface {
	Add(*T, *T)
	Mul(*T, *T)
}

type State[T any] interface {
	Init()
	Combine(State[T], TypeOp[T])
	SetIsset(bool)
	SetValue(T)
	Isset() bool
	GetValue() T
}

type SumState[T any] struct {
	_isset bool
	_value T
}

func (sum *SumState[T]) Init() {
	sum._isset = false
}

func (sum *SumState[T]) Combine(s State[T], add TypeOp[T]) {
	other := s.(*SumState[T])
	sum._isset = other._isset || sum._isset
	add.Add(&sum._value, &other._value)
}

func (sum *SumState[T]) SetIsset(bool) {
	sum._isset = true
}
func (sum *SumState[T]) SetValue(val T) {
	sum._value = val
}

func (sum *SumState[T]) Isset() bool {
	return sum._isset
}

func (sum *SumState[T]) GetValue() T {
	return sum._value
}

type StateOp[T any] interface {
	Init(State[T])
	Combine(State[T], State[T], *AggrInputData, TypeOp[T])
	AddValues(State[T], int)
}

type AddOp[ResultT any, InputT any] interface {
	AddNumber(State[ResultT], *InputT, TypeOp[ResultT])
	AddConstant(State[ResultT], *InputT, int, TypeOp[ResultT])
}

type AggrOp[ResultT any, InputT any] interface {
	Init(State[ResultT], StateOp[ResultT])
	Combine(State[ResultT], State[ResultT], *AggrInputData, StateOp[ResultT], TypeOp[ResultT])
	Operation(State[ResultT], *InputT, *AggrUnaryInput, StateOp[ResultT], AddOp[ResultT, InputT], TypeOp[ResultT])
	ConstantOperation(State[ResultT], *InputT, *AggrUnaryInput, int, StateOp[ResultT], AddOp[ResultT, InputT], TypeOp[ResultT])
	Finalize(State[ResultT], *ResultT, *AggrFinalizeData)
	IgnoreNull() bool
}

type SumStateOp[T any] struct {
}

func (SumStateOp[T]) Init(s State[T]) {
	s.Init()
}
func (SumStateOp[T]) Combine(src State[T], target State[T], _ *AggrInputData, top TypeOp[T]) {
	src.Combine(target, top)
}
func (SumStateOp[T]) AddValues(s State[T], _ int) {
	s.SetIsset(true)
}

type HugeintAdd struct {
}

func (*HugeintAdd) AddNumber(State[Hugeint], *int32, TypeOp[Hugeint]) {}

func (*HugeintAdd) AddConstant(State[Hugeint], *int32, int, TypeOp[Hugeint]) {

}

type SumOp[ResultT any, InputT any] struct {
}

func (s SumOp[ResultT, InputT]) Init(s2 State[ResultT], sop StateOp[ResultT]) {
	var val ResultT
	s2.SetValue(val)
	sop.Init(s2)
}

func (s SumOp[ResultT, InputT]) Combine(src State[ResultT], target State[ResultT], data *AggrInputData,
	sop StateOp[ResultT], top TypeOp[ResultT]) {
	sop.Combine(src, target, data, top)
}

func (s SumOp[ResultT, InputT]) Operation(s3 State[ResultT], input *InputT, data *AggrUnaryInput,
	sop StateOp[ResultT], aop AddOp[ResultT, InputT], top TypeOp[ResultT]) {
	sop.AddValues(s3, 1)
	aop.AddNumber(s3, input, top)
}

func (s SumOp[ResultT, InputT]) ConstantOperation(s3 State[ResultT], input *InputT, data *AggrUnaryInput, count int,
	sop StateOp[ResultT], aop AddOp[ResultT, InputT], top TypeOp[ResultT]) {
	sop.AddValues(s3, count)
	aop.AddConstant(s3, input, count, top)
}

func (s SumOp[ResultT, InputT]) Finalize(s3 State[ResultT], target *ResultT, data *AggrFinalizeData) {
	if s3.Isset() {
		data.ReturnNull()
	} else {
		*target = s3.GetValue()
	}
}

func (s SumOp[ResultT, InputT]) IgnoreNull() bool {
	return true
}

func UnaryScatter[ResultT any, STATE State[ResultT], InputT any, OP AggrOp[ResultT, InputT]](
	input *Vector,
	states *Vector,
	data *AggrInputData,
	count int,
	aop AggrOp[ResultT, InputT],
	sop StateOp[ResultT],
	addOp AddOp[ResultT, InputT],
	top TypeOp[ResultT],
) {
	if input.phyFormat().isConst() && states.phyFormat().isConst() {
		if aop.IgnoreNull() && isNullInPhyFormatConst(input) {
			return
		}
		inputSlice := getSliceInPhyFormatConst[InputT](input)
		statesSlice := getSliceInPhyFormatConst[STATE](states)
		inputData := NewAggrUnaryInput(data, getMaskInPhyFormatConst(input))
		aop.ConstantOperation(statesSlice[0], &inputSlice[0], inputData, count, sop, addOp, top)
	} else if input.phyFormat().isFlat() && states.phyFormat().isFlat() {
		//TODO:
	} else {
		//TODO:
	}
}
