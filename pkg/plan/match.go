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
)

func Match(
	columns *Chunk,
	colData []*UnifiedFormat,
	layout *TupleDataLayout,
	rows *Vector,
	predicates []ET_SubTyp,
	sel *SelectVector,
	cnt int,
	noMatch *SelectVector,
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
	columns *Chunk,
	colData []*UnifiedFormat,
	layout *TupleDataLayout,
	rows *Vector,
	predicates []ET_SubTyp,
	sel *SelectVector,
	cnt *int,
	noMatch *SelectVector,
	noMatchCnt *int,
	noMatchSel bool,
) {
	for i := 0; i < len(predicates); i++ {
		vec := columns._data[i]
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
	vec *Vector,
	col *UnifiedFormat,
	predTyp ET_SubTyp,
	layout *TupleDataLayout,
	rows *Vector,
	sel *SelectVector,
	cnt *int,
	colNo int,
	noMatch *SelectVector,
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
		pTyp := layout.types()[colNo].getInternalType()
		switch pTyp {
		case INT32:
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
		case INT8:
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
		case VARCHAR:
			TemplatedMatchType[String](
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
		case DATE:
			TemplatedMatchType[Date](
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
		case DECIMAL:
			TemplatedMatchType[Decimal](
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
		case INT128:
			TemplatedMatchType[Hugeint](
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
		pTyp := layout.types()[colNo].getInternalType()
		switch pTyp {
		case INT32:
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
	col *UnifiedFormat,
	rows *Vector,
	rowWidth int,
	sel *SelectVector,
	cnt *int,
	colOffset int,
	colNo int,
	noMatch *SelectVector,
	noMatchCnt *int,
	noMatchSel bool,
	cmp CompareOp[T],
) {
	entryIdx, idxInEntry := getEntryIndex(uint64(colNo))
	dataSlice := getSliceInPhyFormatUnifiedFormat[T](col)
	ptrs := getSliceInPhyFormatFlat[unsafe.Pointer](rows)
	matchCnt := 0
	if !col._mask.AllValid() {
		for i := 0; i < *cnt; i++ {
			idx := sel.getIndex(i)
			row := pointerToSlice[uint8](ptrs[idx], rowWidth)
			mask := Bitmap{_bits: row}
			isNull := !rowIsValidInEntry(mask.getEntry(entryIdx), idxInEntry)

			colIdx := col._sel.getIndex(idx)
			if !col._mask.rowIsValid(uint64(colIdx)) {
				if isNull {
					sel.setIndex(matchCnt, idx)
					matchCnt++
				} else {
					if noMatchSel {
						noMatch.setIndex(*noMatchCnt, idx)
						(*noMatchCnt)++
					}
				}
			} else {
				val := load[T](pointerAdd(ptrs[idx], colOffset))
				if !isNull &&
					cmp.operation(&dataSlice[colIdx], &val) {
					sel.setIndex(matchCnt, idx)
					matchCnt++
				} else {
					if noMatchSel {
						noMatch.setIndex(*noMatchCnt, idx)
						*noMatchCnt++
					}
				}
			}
		}
	} else {
		for i := 0; i < *cnt; i++ {
			idx := sel.getIndex(i)
			row := pointerToSlice[uint8](ptrs[idx], rowWidth)
			mask := Bitmap{_bits: row}
			isNull := !rowIsValidInEntry(mask.getEntry(entryIdx), idxInEntry)
			colIdx := col._sel.getIndex(idx)
			val := load[T](pointerAdd(ptrs[idx], colOffset))
			//if colOffset == 33 {

			//}
			if !isNull && cmp.operation(&dataSlice[colIdx], &val) {
				sel.setIndex(matchCnt, idx)
				matchCnt++
			} else {
				if noMatchSel {
					noMatch.setIndex(*noMatchCnt, idx)
					(*noMatchCnt)++
				}
			}
		}
	}
	*cnt = matchCnt
}
