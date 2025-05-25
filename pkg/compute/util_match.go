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
	"unsafe"

	"github.com/daviszhen/plan/pkg/chunk"
	"github.com/daviszhen/plan/pkg/common"
	"github.com/daviszhen/plan/pkg/util"
)

func Match(
	columns *chunk.Chunk,
	colData []*chunk.UnifiedFormat,
	layout *TupleDataLayout,
	rows *chunk.Vector,
	predicates []ET_SubTyp,
	sel *chunk.SelectVector,
	cnt int,
	noMatch *chunk.SelectVector,
	noMatchCnt *int,
) int {
	TemplatedMatch(
		columns,
		colData,
		layout,
		rows,
		predicates,
		sel,
		&cnt,
		noMatch,
		noMatchCnt,
		noMatch != nil,
	)
	return cnt
}

func TemplatedMatch(
	columns *chunk.Chunk,
	colData []*chunk.UnifiedFormat,
	layout *TupleDataLayout,
	rows *chunk.Vector,
	predicates []ET_SubTyp,
	sel *chunk.SelectVector,
	cnt *int,
	noMatch *chunk.SelectVector,
	noMatchCnt *int,
	noMatchSel bool,
) {
	for i := 0; i < len(predicates); i++ {
		vec := columns.Data[i]
		col := colData[i]
		TemplatedMatchOp(
			vec,
			col,
			predicates[i],
			layout,
			rows,
			sel,
			cnt,
			i,
			noMatch,
			noMatchCnt,
			*cnt,
			noMatchSel,
		)
	}
}

func TemplatedMatchOp(
	vec *chunk.Vector,
	col *chunk.UnifiedFormat,
	predTyp ET_SubTyp,
	layout *TupleDataLayout,
	rows *chunk.Vector,
	sel *chunk.SelectVector,
	cnt *int,
	colNo int,
	noMatch *chunk.SelectVector,
	noMatchCnt *int,
	originalCnt int,
	noMatchSel bool,
) {
	if *cnt == 0 {
		return
	}
	colOffset := layout.offsets()[colNo]
	switch predTyp {
	case ET_Equal, ET_In:
		pTyp := layout.types()[colNo].GetInternalType()
		switch pTyp {
		case common.INT32:
			TemplatedMatchType[int32](
				col,
				rows,
				layout._rowWidth,
				sel,
				cnt,
				colOffset,
				colNo,
				noMatch,
				noMatchCnt,
				noMatchSel,
				equalOp[int32]{},
			)
		case common.INT64:
			TemplatedMatchType[int64](
				col,
				rows,
				layout._rowWidth,
				sel,
				cnt,
				colOffset,
				colNo,
				noMatch,
				noMatchCnt,
				noMatchSel,
				equalOp[int64]{},
			)
		case common.INT8:
			TemplatedMatchType[int8](
				col,
				rows,
				layout._rowWidth,
				sel,
				cnt,
				colOffset,
				colNo,
				noMatch,
				noMatchCnt,
				noMatchSel,
				equalOp[int8]{},
			)
		case common.VARCHAR:
			TemplatedMatchType[common.String](
				col,
				rows,
				layout._rowWidth,
				sel,
				cnt,
				colOffset,
				colNo,
				noMatch,
				noMatchCnt,
				noMatchSel,
				equalStrOp{},
			)
		case common.DATE:
			TemplatedMatchType[common.Date](
				col,
				rows,
				layout._rowWidth,
				sel,
				cnt,
				colOffset,
				colNo,
				noMatch,
				noMatchCnt,
				noMatchSel,
				equalDateOp{},
			)
		case common.DECIMAL:
			TemplatedMatchType[common.Decimal](
				col,
				rows,
				layout._rowWidth,
				sel,
				cnt,
				colOffset,
				colNo,
				noMatch,
				noMatchCnt,
				noMatchSel,
				equalDecimalOp{},
			)
		case common.INT128:
			TemplatedMatchType[common.Hugeint](
				col,
				rows,
				layout._rowWidth,
				sel,
				cnt,
				colOffset,
				colNo,
				noMatch,
				noMatchCnt,
				noMatchSel,
				equalHugeintOp{},
			)
		default:
			panic("usp")
		}
	case ET_NotEqual:
		pTyp := layout.types()[colNo].GetInternalType()
		switch pTyp {
		case common.INT32:
			TemplatedMatchType[int32](
				col,
				rows,
				layout._rowWidth,
				sel,
				cnt,
				colOffset,
				colNo,
				noMatch,
				noMatchCnt,
				noMatchSel,
				notEqualOp[int32]{},
			)
		default:
			panic("usp")
		}
	default:
		panic("usp")
	}

}

func TemplatedMatchType[T any](
	col *chunk.UnifiedFormat,
	rows *chunk.Vector,
	rowWidth int,
	sel *chunk.SelectVector,
	cnt *int,
	colOffset int,
	colNo int,
	noMatch *chunk.SelectVector,
	noMatchCnt *int,
	noMatchSel bool,
	cmp CompareOp[T],
) {
	entryIdx, idxInEntry := util.GetEntryIndex(uint64(colNo))
	dataSlice := chunk.GetSliceInPhyFormatUnifiedFormat[T](col)
	ptrs := chunk.GetSliceInPhyFormatFlat[unsafe.Pointer](rows)
	matchCnt := 0
	if !col.Mask.AllValid() {
		for i := 0; i < *cnt; i++ {
			idx := sel.GetIndex(i)
			row := util.PointerToSlice[uint8](ptrs[idx], rowWidth)
			mask := util.Bitmap{Bits: row}
			isNull := !util.RowIsValidInEntry(mask.GetEntry(entryIdx), idxInEntry)

			colIdx := col.Sel.GetIndex(idx)
			if !col.Mask.RowIsValid(uint64(colIdx)) {
				if isNull {
					sel.SetIndex(matchCnt, idx)
					matchCnt++
				} else {
					if noMatchSel {
						noMatch.SetIndex(*noMatchCnt, idx)
						(*noMatchCnt)++
					}
				}
			} else {
				val := util.Load[T](util.PointerAdd(ptrs[idx], colOffset))
				if !isNull &&
					cmp.operation(&dataSlice[colIdx], &val) {
					sel.SetIndex(matchCnt, idx)
					matchCnt++
				} else {
					if noMatchSel {
						noMatch.SetIndex(*noMatchCnt, idx)
						*noMatchCnt++
					}
				}
			}
		}
	} else {
		for i := 0; i < *cnt; i++ {
			idx := sel.GetIndex(i)
			row := util.PointerToSlice[uint8](ptrs[idx], rowWidth)
			mask := util.Bitmap{Bits: row}
			isNull := !util.RowIsValidInEntry(mask.GetEntry(entryIdx), idxInEntry)
			colIdx := col.Sel.GetIndex(idx)
			val := util.Load[T](util.PointerAdd(ptrs[idx], colOffset))
			if !isNull && cmp.operation(&dataSlice[colIdx], &val) {
				sel.SetIndex(matchCnt, idx)
				matchCnt++
			} else {
				if noMatchSel {
					noMatch.SetIndex(*noMatchCnt, idx)
					(*noMatchCnt)++
				}
			}
		}
	}
	*cnt = matchCnt
}
