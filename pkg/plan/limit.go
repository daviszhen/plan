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
	"math"

	"github.com/daviszhen/plan/pkg/chunk"
	"github.com/daviszhen/plan/pkg/common"
	"github.com/daviszhen/plan/pkg/util"
)

type LimitState int

const (
	LIMIT_INIT LimitState = iota
	LIMIT_SCAN
)

type Limit struct {
	_state         LimitState
	_limit         uint64
	_offset        uint64
	_childTypes    []common.LType
	_data          *ColumnDataCollection
	_currentOffset uint64
	_reader        *LimitReader
}

type LimitReader struct {
	_currentOffset uint64
	_scanState     *ColumnDataScanState
}

func (limit *Limit) Sink(chunk *chunk.Chunk) SinkResult {
	var maxElement uint64
	if !limit.ComputeOffset(chunk, &maxElement) {
		return SinkResDone
	}

	maxCard := maxElement - limit._currentOffset
	//drop rest part
	if maxCard < uint64(chunk.Card()) {
		chunk.SetCard(int(maxCard))
	}

	limit._data.Append(chunk)
	limit._currentOffset += uint64(chunk.Card())
	if limit._currentOffset == maxElement {
		return SinkResDone
	}
	return SinkResNeedMoreInput
}

func (limit *Limit) ComputeOffset(
	chunk *chunk.Chunk,
	maxElement *uint64) bool {
	if limit._limit != math.MaxUint64 {
		*maxElement = limit._limit + limit._offset
		if limit._limit == 0 ||
			limit._currentOffset >= *maxElement {
			return false
		}
		return true
	}
	panic("usp")
}

func (limit *Limit) GetData(read *chunk.Chunk) SourceResult {
	if limit._reader == nil {
		limit._reader = &LimitReader{
			_currentOffset: 0,
			_scanState:     &ColumnDataScanState{},
		}
		limit._data.initScan(limit._reader._scanState)
	}
	for limit._reader._currentOffset <
		limit._limit+limit._offset {

		limit._data.Scan(limit._reader._scanState, read)
		if read.Card() == 0 {
			return SrcResDone
		}

		if limit.HandleOffset(read, &limit._reader._currentOffset) {
			break
		}
	}
	if read.Card() > 0 {
		return SrcResHaveMoreOutput
	} else {
		return SrcResDone
	}
}

func (limit *Limit) HandleOffset(
	input *chunk.Chunk,
	currentOffset *uint64,
) bool {
	maxElement := uint64(0)
	if limit._limit == math.MaxUint64 {
		maxElement = limit._limit
	} else {
		maxElement = limit._limit + limit._offset
	}

	inputSize := input.Card()
	if *currentOffset < limit._offset {
		if *currentOffset+uint64(input.Card()) >
			limit._offset {
			startPosition := limit._offset - *currentOffset
			chunkCount := min(limit._limit,
				uint64(input.Card())-startPosition)
			sel := chunk.NewSelectVector(util.DefaultVectorSize)
			for i := 0; i < int(chunkCount); i++ {
				sel.SetIndex(i, int(startPosition)+i)
			}
			input.Slice(input, sel, int(chunkCount), 0)
		} else {
			*currentOffset += uint64(input.Card())
			return false
		}
	} else {
		chunkCount := 0
		if *currentOffset+uint64(input.Card()) >= maxElement {
			chunkCount = int(maxElement - *currentOffset)
		} else {
			chunkCount = input.Card()
		}
		input.Reference(input)
		input.SetCard(chunkCount)
	}
	*currentOffset += uint64(inputSize)
	return true
}

func NewLimit(typs []common.LType, limitExpr, offsetExpr *Expr) *Limit {
	ret := &Limit{
		_childTypes: typs,
	}
	if limitExpr == nil {
		ret._limit = math.MaxUint64
	} else {
		ret._limit = uint64(limitExpr.Ivalue)
	}
	if offsetExpr == nil {
		ret._offset = 0
	} else {
		ret._offset = uint64(offsetExpr.Ivalue)
	}
	ret._data = NewColumnDataCollection(typs)
	return ret
}
