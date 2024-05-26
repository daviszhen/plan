package main

import (
	"fmt"
	"testing"
	"unsafe"
)

func Test_SumState(t *testing.T) {
	{
		var val State[Hugeint]
		size := unsafe.Sizeof(val)
		fmt.Println(size)

		space := make([]byte, size)
		ptr := bytesSliceToPointer(space)
		//var s State[Hugeint]
		state := *(*State[Hugeint])(ptr)
		var hval Hugeint
		state.SetValue(hval)
		state.Init()

		sop := SumStateOp[Hugeint]{}
		sumop := SumOp[Hugeint, int32]{}
		sumop.Init(&state, sop)
	}
	{
		var val State[Hugeint]
		size := unsafe.Sizeof(val)
		fmt.Println(size)

		space := make([]byte, size+20)
		ptr := bytesSliceToPointer(space)
		state := *(*State[Hugeint])(ptr)
		var hval Hugeint
		state.SetValue(hval)
		state.Init()

		sop := SumStateOp[Hugeint]{}
		sumop := SumOp[Hugeint, int32]{}
		sumop.Init(&state, sop)
	}
}
