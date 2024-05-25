package main

import (
	"unsafe"
)

func StateSize[T any, STATE State[T]]() int {
	var val STATE
	size := unsafe.Sizeof(val)
	return int(size)
}

func StateInit[T any, STATE State[T], OP AggrOp[T]](
	pointer unsafe.Pointer,
	aop AggrOp[T],
	sop StateOp[T],
) {
	aop.Init(*(*STATE)(pointer), sop)
}

func UnaryScatterUpdate[T any, STATE State[T], InputT any, OP AggrOp[T]]([]*Vector, *AggrInputData, int, *Vector, int) {

}

func StateCombine[T any, STATE State[T], OP AggrOp[T]](*Vector, *Vector, *AggrInputData, int) {}

func StateFinalize[T any, STATE State[T], ResultT any, OP AggrOp[T]](*Vector, *AggrInputData, *Vector, int, int) {
}

func UnaryUpdate[T any, STATE State[T], InputT any, OP AggrOp[T]]([]*Vector, *AggrInputData, int, unsafe.Pointer, int) {

}

func UnaryAggregate[T any, STATE State[T], InputT any, ResultT any, OP AggrOp[T]](
	inputTyp LType,
	retTyp LType,
	nullHandling FuncNullHandling,
) *AggrFunc {
	return &AggrFunc{
		_args:         []LType{inputTyp},
		_retType:      retTyp,
		_stateSize:    StateSize[T, STATE],
		_init:         StateInit[T, STATE, OP],
		_update:       UnaryScatterUpdate[T, STATE, InputT, OP],
		_combine:      StateCombine[T, STATE, OP],
		_finalize:     StateFinalize[T, STATE, ResultT, OP],
		_nullHandling: nullHandling,
		_simpleUpdate: UnaryUpdate[T, STATE, InputT, OP],
	}
}

func GetSumAggr(pTyp PhyType) *AggrFunc {
	switch pTyp {
	case INT32:
		fun := UnaryAggregate[Hugeint, *SumState[Hugeint], int32, Hugeint, SumOp[Hugeint]](
			integer(),
			hugeint(),
			DEFAULT_NULL_HANDLING,
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

type AddOp[T any] interface {
	AddNumber(State[T], *T, TypeOp[T])
	AddConstant(State[T], *T, int, TypeOp[T])
}

type AggrOp[T any] interface {
	Init(State[T], StateOp[T])
	Combine(State[T], State[T], *AggrInputData, StateOp[T], TypeOp[T])
	Operation(State[T], *T, *AggrInputData, StateOp[T], AddOp[T], TypeOp[T])
	ConstantOperation(State[T], *T, *AggrInputData, int, StateOp[T], AddOp[T], TypeOp[T])
	Finalize(State[T], *T, *AggrFinalizeData)
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

func (*HugeintAdd) AddNumber(State[Hugeint], *Hugeint, TypeOp[Hugeint]) {}

func (*HugeintAdd) AddConstant(State[Hugeint], *Hugeint, int, TypeOp[Hugeint]) {

}

type SumOp[T any] struct {
}

func (s SumOp[T]) Init(s2 State[T], sop StateOp[T]) {
	var val T
	s2.SetValue(val)
	sop.Init(s2)
}

func (s SumOp[T]) Combine(src State[T], target State[T], data *AggrInputData,
	sop StateOp[T], top TypeOp[T]) {
	sop.Combine(src, target, data, top)
}

func (s SumOp[T]) Operation(s3 State[T], input *T, data *AggrInputData,
	sop StateOp[T], aop AddOp[T], top TypeOp[T]) {
	sop.AddValues(s3, 1)
	aop.AddNumber(s3, input, top)
}

func (s SumOp[T]) ConstantOperation(s3 State[T], input *T, data *AggrInputData, count int,
	sop StateOp[T], aop AddOp[T], top TypeOp[T]) {
	sop.AddValues(s3, count)
	aop.AddConstant(s3, input, count, top)
}

func (s SumOp[T]) Finalize(s3 State[T], target *T, data *AggrFinalizeData) {
	if s3.Isset() {
		data.ReturnNull()
	} else {
		*target = s3.GetValue()
	}
}

func (s SumOp[T]) IgnoreNull() bool {
	return true
}
