package main

type BooleanOp interface {
	opWithoutNull(bool, bool) bool
	opWithNull(bool, bool, bool, bool) (bool, bool)
}

var gAndOp AndOp

type AndOp struct {
}

/*
TRUE  AND TRUE   = TRUE

TRUE  AND FALSE  = FALSE
FALSE AND TRUE   = FALSE
FALSE AND FALSE  = FALSE
*/
func (add AndOp) opWithoutNull(left, right bool) bool {
	return left && right
}

/*
true: both are true
false: either is false. neglect NULL
NULL: otherwise

TRUE  AND TRUE   = TRUE

TRUE  AND FALSE  = FALSE
FALSE AND TRUE   = FALSE
FALSE AND FALSE  = FALSE

FALSE AND NULL   = FALSE
NULL  AND FALSE  = FALSE

TRUE  AND NULL   = NULL
NULL  AND TRUE   = NULL
NULL  AND NULL   = NULL
*/
func (add AndOp) opWithNull(left, right, lnull, rnull bool) (null bool, result bool) {
	if lnull && rnull {
		//NULL  AND NULL   = NULL
		return true, true
	} else if lnull {
		//NULL  AND FALSE  = FALSE
		//NULL  AND TRUE   = NULL
		//NULL  AND NULL   = NULL
		return right, right
	} else if rnull {
		//FALSE AND NULL   = FALSE
		//TRUE  AND NULL   = NULL
		//NULL  AND NULL   = NULL
		return left, left
	} else {
		//TRUE  AND TRUE   = TRUE
		//
		//TRUE  AND FALSE  = FALSE
		//FALSE AND TRUE   = FALSE
		//FALSE AND FALSE  = FALSE
		return false, left && right
	}
}

type CompareOp[T any] interface {
	operation(left, right *T) bool
}

type equalOp[T comparable] struct {
}

func (e equalOp[T]) operation(left, right *T) bool {
	return *left == *right
}

func selectOperation(left, right *Vector, sel *SelectVector, count int, trueSel, falseSel *SelectVector, subTyp ET_SubTyp) int {
	switch subTyp {
	case ET_Equal:
		switch left.typ().getInternalType() {
		case INT32:
			return selectBinary[int32](left, right, sel, count, trueSel, falseSel, equalOp[int32]{})
		case BOOL, UINT8, INT8, UINT16, INT16, UINT32, UINT64, INT64, FLOAT, DOUBLE, INTERVAL, LIST, STRUCT, VARCHAR, INT128, UNKNOWN, BIT, INVALID:
			panic("usp")
		default:
			panic("usp")
		}
	default:
		panic("usp")
	}

}

func selectBinary[T any](left, right *Vector, sel *SelectVector, count int, trueSel, falseSel *SelectVector, cmpOp CompareOp[T]) int {
	if sel == nil {
		sel = incrSelectVectorInPhyFormatFlat()
	}
	if left.phyFormat().isConst() && right.phyFormat().isConst() {
		return selectConst[T](left, right, sel, count, trueSel, falseSel, cmpOp)
	} else if left.phyFormat().isConst() && right.phyFormat().isFlat() {
		return selectFlat[T](left, right, sel, count, trueSel, falseSel, cmpOp, true, false)
	} else if left.phyFormat().isFlat() && right.phyFormat().isConst() {
		return selectFlat[T](left, right, sel, count, trueSel, falseSel, cmpOp, false, true)
	} else if left.phyFormat().isFlat() && right.phyFormat().isFlat() {
		return selectFlat[T](left, right, sel, count, trueSel, falseSel, cmpOp, false, false)
	} else {
		return selectGeneric[T](left, right, sel, count, trueSel, falseSel, cmpOp)
	}
}

func selectGeneric[T any](left, right *Vector, sel *SelectVector, count int, trueSel, falseSel *SelectVector, cmpOp CompareOp[T]) int {
	var ldata, rdata UnifiedFormat
	left.toUnifiedFormat(count, &ldata)
	right.toUnifiedFormat(count, &rdata)
	lslice := getSliceInPhyFormatUnifiedFormat[T](&ldata)
	rslice := getSliceInPhyFormatUnifiedFormat[T](&rdata)
	return selectGenericLoopSwitch[T](lslice, rslice,
		ldata._sel, rdata._sel,
		sel,
		count,
		ldata._mask, rdata._mask,
		trueSel, falseSel,
		cmpOp)
}

func selectGenericLoopSwitch[T any](
	ldata, rdata []T,
	lsel, rsel *SelectVector,
	resSel *SelectVector,
	count int,
	lmask, rmask *Bitmap,
	trueSel, falseSel *SelectVector,
	cmpOp CompareOp[T]) int {
	if !lmask.AllValid() || !rmask.AllValid() {
		return selectGenericLoopSelSwitch[T](
			ldata, rdata,
			lsel, rsel,
			resSel,
			count,
			lmask, rmask,
			trueSel, falseSel, cmpOp, false)
	} else {
		return selectGenericLoopSelSwitch[T](
			ldata, rdata,
			lsel, rsel,
			resSel,
			count,
			lmask, rmask,
			trueSel, falseSel, cmpOp, true)
	}
}

func selectGenericLoopSelSwitch[T any](
	ldata, rdata []T,
	lsel, rsel *SelectVector,
	resSel *SelectVector,
	count int,
	lmask, rmask *Bitmap,
	trueSel, falseSel *SelectVector,
	cmpOp CompareOp[T],
	noNull bool,
) int {
	if trueSel != nil && falseSel != nil {
		return selectGenericLoop[T](
			ldata, rdata,
			lsel, rsel,
			resSel,
			count,
			lmask, rmask,
			trueSel, falseSel,
			cmpOp,
			noNull,
			true, true,
		)
	} else if trueSel != nil {
		return selectGenericLoop[T](
			ldata, rdata,
			lsel, rsel,
			resSel,
			count,
			lmask, rmask,
			trueSel, falseSel,
			cmpOp,
			noNull,
			true, false,
		)
	} else {
		return selectGenericLoop[T](
			ldata, rdata,
			lsel, rsel,
			resSel,
			count,
			lmask, rmask,
			trueSel, falseSel,
			cmpOp,
			noNull,
			false, true,
		)
	}
}

func selectGenericLoop[T any](
	ldata, rdata []T,
	lsel, rsel *SelectVector,
	resSel *SelectVector,
	count int,
	lmask, rmask *Bitmap,
	trueSel, falseSel *SelectVector,
	cmpOp CompareOp[T],
	noNull bool,
	hasTrueSel, hasFalseSel bool,
) int {
	trueCount, falseCount := 0, 0
	for i := 0; i < count; i++ {
		resIdx := resSel.getIndex(i)
		lidx := lsel.getIndex(i)
		ridx := rsel.getIndex(i)
		if (noNull || lmask.rowIsValid(uint64(lidx)) && rmask.rowIsValid(uint64(ridx))) &&
			cmpOp.operation(&ldata[lidx], &rdata[ridx]) {
			if hasTrueSel {
				trueSel.setIndex(trueCount, resIdx)
				trueCount++
			}
		} else {
			if hasFalseSel {
				falseSel.setIndex(falseCount, resIdx)
				falseCount++
			}
		}
	}
	if hasTrueSel {
		return trueCount
	} else {
		return count - falseCount
	}
}

func selectConst[T any](left, right *Vector, sel *SelectVector, count int, trueSel, falseSel *SelectVector, cmpOp CompareOp[T]) int {
	ldata := getSliceInPhyFormatConst[T](left)
	rdata := getSliceInPhyFormatConst[T](right)
	if isNullInPhyFormatConst(left) ||
		isNullInPhyFormatConst(right) ||
		!cmpOp.operation(&ldata[0], &rdata[0]) {
		if falseSel != nil {
			for i := 0; i < count; i++ {
				falseSel.setIndex(i, sel.getIndex(i))
			}
		}
		return 0
	} else {
		if trueSel != nil {
			for i := 0; i < count; i++ {
				trueSel.setIndex(i, sel.getIndex(i))
			}
		}
		return count
	}
}

func selectFlat[T any](left, right *Vector,
	sel *SelectVector,
	count int,
	trueSel, falseSel *SelectVector,
	cmpOp CompareOp[T],
	leftConst, rightConst bool) int {
	ldata := getSliceInPhyFormatFlat[T](left)
	rdata := getSliceInPhyFormatFlat[T](right)
	if leftConst && isNullInPhyFormatConst(left) {
		if falseSel != nil {
			for i := 0; i < count; i++ {
				falseSel.setIndex(i, sel.getIndex(i))
			}
		}
		return 0
	}
	if rightConst && isNullInPhyFormatConst(right) {
		if falseSel != nil {
			for i := 0; i < count; i++ {
				falseSel.setIndex(i, sel.getIndex(i))
			}
		}
		return 0
	}

	if leftConst {
		return selectFlatLoopSwitch[T](
			ldata,
			rdata,
			sel,
			count,
			getMaskInPhyFormatFlat(right),
			trueSel,
			falseSel,
			cmpOp,
			leftConst,
			rightConst)
	} else if rightConst {
		return selectFlatLoopSwitch[T](
			ldata,
			rdata,
			sel,
			count,
			getMaskInPhyFormatFlat(left),
			trueSel,
			falseSel,
			cmpOp,
			leftConst,
			rightConst)
	} else {
		merge := getMaskInPhyFormatFlat(left)
		rMask := getMaskInPhyFormatFlat(right)
		merge.combine(rMask, count)
		return selectFlatLoopSwitch[T](
			ldata,
			rdata,
			sel,
			count,
			merge,
			trueSel,
			falseSel,
			cmpOp,
			leftConst,
			rightConst)
	}
}

func selectFlatLoopSwitch[T any](
	ldata, rdata []T,
	sel *SelectVector,
	count int,
	mask *Bitmap,
	trueSel, falseSel *SelectVector,
	cmpOp CompareOp[T],
	leftConst, rightConst bool) int {
	if trueSel != nil && falseSel != nil {
		return selectFlatLoop[T](
			ldata, rdata,
			sel,
			count,
			mask,
			trueSel, falseSel,
			cmpOp,
			leftConst, rightConst,
			true, true)
	} else if trueSel != nil {
		return selectFlatLoop[T](
			ldata, rdata,
			sel,
			count,
			mask,
			trueSel, falseSel,
			cmpOp,
			leftConst, rightConst,
			true, false)
	} else {
		return selectFlatLoop[T](
			ldata, rdata,
			sel,
			count,
			mask,
			trueSel, falseSel,
			cmpOp,
			leftConst, rightConst,
			false, true)
	}
}

func selectFlatLoop[T any](
	ldata, rdata []T,
	sel *SelectVector,
	count int,
	mask *Bitmap,
	trueSel, falseSel *SelectVector,
	cmpOp CompareOp[T],
	leftConst, rightConst bool,
	hasTrueSel, hasFalseSel bool,
) int {
	trueCount, falseCount := 0, 0
	baseIdx := 0
	entryCount := mask.entryCount(count)
	for eidx := 0; eidx < entryCount; eidx++ {
		entry := mask.getEntry(uint64(eidx))
		next := min(baseIdx+8, count)
		if mask.AllValidInEntry(entry) {
			//all valid: perform operation
			for ; baseIdx < next; baseIdx++ {
				resIdx := sel.getIndex(baseIdx)
				lidx := baseIdx
				if leftConst {
					lidx = 0
				}
				ridx := baseIdx
				if rightConst {
					ridx = 0
				}
				res := cmpOp.operation(&ldata[lidx], &rdata[ridx])
				if hasTrueSel {
					trueSel.setIndex(trueCount, resIdx)
					if res {
						trueCount++
					}
				}
				if hasFalseSel {
					falseSel.setIndex(falseCount, resIdx)
					if !res {
						falseCount++
					}
				}
			}
		} else if mask.NoneValidInEntry(entry) {
			//skip all
			if hasFalseSel {
				for ; baseIdx < next; baseIdx++ {
					resIdx := sel.getIndex(baseIdx)
					falseSel.setIndex(falseCount, resIdx)
					falseCount++
				}
			}
			baseIdx = next
			continue
		} else {
			//partially
			start := baseIdx
			for ; baseIdx < next; baseIdx++ {
				resIdx := sel.getIndex(baseIdx)
				lidx := baseIdx
				if leftConst {
					lidx = 0
				}
				ridx := baseIdx
				if rightConst {
					ridx = 0
				}
				res := mask.rowIsValidInEntry(entry, uint64(baseIdx-start)) &&
					cmpOp.operation(&ldata[lidx], &rdata[ridx])
				if hasTrueSel {
					trueSel.setIndex(trueCount, resIdx)
					if res {
						trueCount++
					}
				}
				if hasFalseSel {
					falseSel.setIndex(falseCount, resIdx)
					if !res {
						falseCount++
					}
				}
			}
		}
	}
	if hasTrueSel {
		return trueCount
	} else {
		return count - falseCount
	}
}

func compareOperations(left, right, result *Vector, count int, subTyp ET_SubTyp) {
	switch subTyp {
	case ET_Equal:
		switch left.typ().getInternalType() {
		case INT32:
			binaryExecSwitch[int32, bool](left, right, result, count, gBinInt32Equal, nil, gInt32BinarySingleOpWrapper)
		default:
			panic("usp")
		}
	default:
		panic("usp")
	}
}
