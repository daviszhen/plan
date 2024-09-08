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
