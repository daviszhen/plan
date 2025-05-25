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

	"github.com/daviszhen/plan/pkg/chunk"
	"github.com/daviszhen/plan/pkg/common"
	"github.com/daviszhen/plan/pkg/util"
)

// AddInPlace left += delta
func AddInPlace(input *chunk.Vector, right int64, cnt int) {
	util.AssertFunc(input.Typ().Id == common.LTID_POINTER)
	if right == 0 {
		return
	}
	switch input.PhyFormat() {
	case chunk.PF_CONST:
		util.AssertFunc(!chunk.IsNullInPhyFormatConst(input))
		data := chunk.GetSliceInPhyFormatConst[unsafe.Pointer](input)
		data[0] = util.PointerAdd(data[0], int(right))
	default:
		util.AssertFunc(input.PhyFormat().IsFlat())
		data := chunk.GetSliceInPhyFormatFlat[unsafe.Pointer](input)
		for i := 0; i < cnt; i++ {
			data[i] = util.PointerAdd(data[i], int(right))
		}
	}
}

func And(left, right, result *chunk.Vector, count int) {

}
