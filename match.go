package main

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
		switch layout.types()[colNo].getInternalType() {
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
			row := pointerToBytesSlice(ptrs[idx], rowWidth)
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
			row := pointerToBytesSlice(ptrs[idx], rowWidth)
			mask := Bitmap{_bits: row}
			isNull := !rowIsValidInEntry(mask.getEntry(entryIdx), idxInEntry)
			colIdx := col._sel.getIndex(idx)
			val := load[T](pointerAdd(ptrs[idx], colOffset))
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