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

package main

import (
	"math"
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
	_childTypes    []LType
	_data          *ColumnDataCollection
	_currentOffset uint64
	_reader        *LimitReader
}

type LimitReader struct {
	_currentOffset uint64
	_scanState     *ColumnDataScanState
}

func (limit *Limit) Sink(chunk *Chunk) SinkResult {
	var maxElement uint64
	if !limit.ComputeOffset(chunk, &maxElement) {
		return SinkResDone
	}

	maxCard := maxElement - limit._currentOffset
	//drop rest part
	if maxCard < uint64(chunk.card()) {
		chunk.setCard(int(maxCard))
	}

	limit._data.Append(chunk)
	limit._currentOffset += uint64(chunk.card())
	if limit._currentOffset == maxElement {
		return SinkResDone
	}
	return SinkResNeedMoreInput
}

func (limit *Limit) ComputeOffset(
	chunk *Chunk,
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

func (limit *Limit) GetData(read *Chunk) SourceResult {
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
		if read.card() == 0 {
			return SrcResDone
		}

		if limit.HandleOffset(read, &limit._reader._currentOffset) {
			break
		}
	}
	if read.card() > 0 {
		return SrcResHaveMoreOutput
	} else {
		return SrcResDone
	}
}

func (limit *Limit) HandleOffset(
	input *Chunk,
	currentOffset *uint64,
) bool {
	maxElement := uint64(0)
	if limit._limit == math.MaxUint64 {
		maxElement = limit._limit
	} else {
		maxElement = limit._limit + limit._offset
	}

	inputSize := input.card()
	if *currentOffset < limit._offset {
		if *currentOffset+uint64(input.card()) >
			limit._offset {
			startPosition := limit._offset - *currentOffset
			chunkCount := min(limit._limit,
				uint64(input.card())-startPosition)
			sel := NewSelectVector(defaultVectorSize)
			for i := 0; i < int(chunkCount); i++ {
				sel.setIndex(i, int(startPosition)+i)
			}
			input.slice(input, sel, int(chunkCount), 0)
		} else {
			*currentOffset += uint64(input.card())
			return false
		}
	} else {
		chunkCount := 0
		if *currentOffset+uint64(input.card()) >= maxElement {
			chunkCount = int(maxElement - *currentOffset)
		} else {
			chunkCount = input.card()
		}
		input.reference(input)
		input.setCard(chunkCount)
	}
	*currentOffset += uint64(inputSize)
	return true
}

func NewLimit(typs []LType, limitExpr, offsetExpr *Expr) *Limit {
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
