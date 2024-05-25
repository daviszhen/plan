package main

import (
	"unsafe"
)

func StateSize[State any]() int {
	//TODO:
	return 0
}

func StateInit[State any, OP any](pointer unsafe.Pointer) {

}

func UnaryScatterUpdate[State any, T any, OP any]([]*Vector, *AggrInputData, int, *Vector, int) {

}

func StateCombine[State any, OP any](*Vector, *Vector, *AggrInputData, int) {}

func StateFinalize[State any, ResultT any, OP any](*Vector, *AggrInputData, *Vector, int, int) {}

func UnaryUpdate[State any, InputT any, OP any]([]*Vector, *AggrInputData, int, unsafe.Pointer, int) {

}

func UnaryAggregate[State any, InputT any, ResultT any, OP any](
	inputTyp LType,
	retTyp LType,
	nullHandling FuncNullHandling,
) *AggrFunc {
	return &AggrFunc{
		_args:         []LType{inputTyp},
		_retType:      retTyp,
		_stateSize:    StateSize[State],
		_init:         StateInit[State, OP],
		_update:       UnaryScatterUpdate[State, InputT, OP],
		_combine:      StateCombine[State, OP],
		_finalize:     StateFinalize[State, ResultT, OP],
		_nullHandling: nullHandling,
		_simpleUpdate: UnaryUpdate[State, InputT, OP],
	}
}

func GetSumAggr(pTyp PhyType) *AggrFunc {
	switch pTyp {
	case INT32:
		fun := UnaryAggregate[SumState[Hugeint], int32, Hugeint](
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

type StateOp[T any] interface {
	Init(State[T])
	Combine(State[T], State[T], *AggrInputData)
	AddValues(State[T], int)
}

type AddOp[T any] interface {
	AddNumber(State[T], *T, TypeOp[T])
	AddConstant(State[T], *T, int, TypeOp[T])
}

type AggrOp[T any] interface {
	Init(State[T])
	Combine(State[T], State[T], *AggrInputData, StateOp[T])
	Operation(State[T], *AggrInputData, StateOp[T], AggrOp[T], TypeOp[T])
	ConstantOperation(State[T], *AggrInputData, int, StateOp[T], AggrOp[T], TypeOp[T])
}
