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

package compute

import (
	"github.com/daviszhen/plan/pkg/common"
	"github.com/daviszhen/plan/pkg/util"
)

func GetDistinctIndices(aggregates []*Expr) []int {
	indices := make([]int, 0)
	for i, aggr := range aggregates {
		if aggr.FunctionInfo.FunImpl._aggrType == NON_DISTINCT {
			continue
		}
		indices = append(indices, i)
	}
	return indices
}

func CreateDistinctAggrCollectionInfo(aggregates []*Expr) *DistinctAggrCollectionInfo {
	indices := GetDistinctIndices(aggregates)
	if len(indices) == 0 {
		return nil
	}
	return NewDistinctAggrCollectionInfo(aggregates, indices)
}

// TODO: add project on aggregate
func createGroupChunkTypes(groups []*Expr) []common.LType {
	if len(groups) == 0 {
		return nil
	}
	groupIndices := make(IntSet)
	for gidx := range groups {
		groupIndices.insert(gidx)
	}
	maxIdx := groupIndices.max()
	util.AssertFunc(maxIdx >= 0)
	types := make([]common.LType, maxIdx+1)
	for i := 0; i < len(types); i++ {
		types[i] = common.Null()
	}
	for gidx, group := range groups {
		types[gidx] = group.DataTyp
	}
	return types
}

const (
	LOAD_FACTOR = 1.5
	HASH_WIDTH  = 8
	BLOCK_SIZE  = 256*1024 - 8
)
