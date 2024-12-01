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

// =

//lint:ignore U1000
type equalOp[T comparable] struct {
}

func (e equalOp[T]) operation(left, right *T) bool {
	return *left == *right
}

// String
type equalStrOp struct {
}

func (e equalStrOp) operation(left, right *String) bool {
	return left.equal(right)
}

// Date
//
//lint:ignore U1000
type equalDateOp struct {
}

func (e equalDateOp) operation(left, right *Date) bool {
	return left.equal(right)
}

// Decimal
type equalDecimalOp struct {
}

func (e equalDecimalOp) operation(left, right *Decimal) bool {
	return left.equal(right)
}

type equalHugeintOp struct {
}

func (e equalHugeintOp) operation(left, right *Hugeint) bool {
	return left.equal(right)
}

// <>

//lint:ignore U1000
type notEqualOp[T comparable] struct {
}

func (e notEqualOp[T]) operation(left, right *T) bool {
	return *left != *right
}

// String
//
//lint:ignore U1000
type notEqualStrOp struct {
}

func (e notEqualStrOp) operation(left, right *String) bool {
	return !left.equal(right)
}

// in
//
//lint:ignore U1000
type inOp[T comparable] struct{}

func (e inOp[T]) operation(left, right *T) bool {
	return *left == *right
}

// String
//
//lint:ignore U1000
type inStrOp struct{}

func (e inStrOp) operation(left, right *String) bool {
	return left.equal(right)
}

// <

// int32
//
//lint:ignore U1000
type lessInt32Op struct {
}

func (e lessInt32Op) operation(left, right *int32) bool {
	return *left < *right
}

// date
//
//lint:ignore U1000
type lessDateOp struct {
}

func (e lessDateOp) operation(left, right *Date) bool {
	return left.less(right)
}

// float64
//
//lint:ignore U1000
type lessFloat64Op struct {
}

func (e lessFloat64Op) operation(left, right *float64) bool {
	return greaterFloat[float64](*right, *left)
}

// <=

// int32
//
//lint:ignore U1000
type lessEqualInt32Op struct {
}

func (e lessEqualInt32Op) operation(left, right *int32) bool {
	return *left <= *right
}

// float32
//
//lint:ignore U1000
type lessEqualFloat32Op struct {
}

func (e lessEqualFloat32Op) operation(left, right *float32) bool {
	return *left <= *right
}

// date
//
//lint:ignore U1000
type lessEqualDateOp struct {
}

func (e lessEqualDateOp) operation(left, right *Date) bool {
	return left.less(right) || left.equal(right)
}

//>

// float32
//
//lint:ignore U1000
type greatFloat32Op struct {
}

func (e greatFloat32Op) operation(left, right *float32) bool {
	return *left > *right
}

// int32
//
//lint:ignore U1000
type greatInt32Op struct {
}

func (e greatInt32Op) operation(left, right *int32) bool {
	return *left > *right
}

type greatHugeintOp struct {
}

func (e greatHugeintOp) operation(left, right *Hugeint) bool {
	upperBigger := left._upper > right._upper
	upperEqual := left._upper == right._upper
	lowerBigger := left._lower > right._lower
	return upperBigger || upperEqual && lowerBigger
}

type lessHugeintOp struct {
}

func (e lessHugeintOp) operation(left, right *Hugeint) bool {
	upperSmaller := left._upper < right._upper
	upperEqual := left._upper == right._upper
	lowerSmaller := left._lower < right._lower
	return upperSmaller || upperEqual && lowerSmaller
}

// decimal
//
//lint:ignore U1000
type greatDecimalOp struct {
}

func (e greatDecimalOp) operation(left, right *Decimal) bool {
	res, err := left.Decimal.Sub(right.Decimal)
	if err != nil {
		panic(err)
	}
	return res.IsPos()
}

// date
//
//lint:ignore U1000
type greatDateOp struct {
}

func (e greatDateOp) operation(left, right *Date) bool {
	return right.less(left)
}

// >=

// int32
//
//lint:ignore U1000
type greatEqualInt32Op struct {
}

func (e greatEqualInt32Op) operation(left, right *int32) bool {
	return *left >= *right
}

// date
//
//lint:ignore U1000
type greatEqualDateOp struct {
}

func (e greatEqualDateOp) operation(left, right *Date) bool {
	return right.less(left) || right.equal(left)
}

//lint:ignore U1000
type greatEqualFloat32Op struct {
}

func (e greatEqualFloat32Op) operation(left, right *float32) bool {
	return *left >= *right
}

// like
//
//lint:ignore U1000
type likeOp struct {
}

// wildcardMatch
func wildcardMatch(pattern, target string) bool {
	var p = 0
	var t = 0
	var positionOfPercentPlusOne int = -1
	var positionOfTargetEncounterPercent int = -1
	plen := len(pattern)
	tlen := len(target)
	for t < tlen {
		//%
		if p < plen && pattern[p] == '%' {
			p++
			positionOfPercentPlusOne = p
			if p >= plen {
				//pattern end with %
				return true
			}
			//means % matches empty
			positionOfTargetEncounterPercent = t
		} else if p < plen && (pattern[p] == '_' || pattern[p] == target[t]) { //match or _
			p++
			t++
		} else {
			if positionOfPercentPlusOne == -1 {
				//have not matched a %
				return false
			}
			if positionOfTargetEncounterPercent == -1 {
				return false
			}
			//backtrace to last % position + 1
			p = positionOfPercentPlusOne
			//means % matches multiple characters
			positionOfTargetEncounterPercent++
			t = positionOfTargetEncounterPercent
		}
	}
	//skip %
	for p < plen && pattern[p] == '%' {
		p++
	}
	return p >= plen
}

func (e likeOp) operation(left, right *String) bool {
	return wildcardMatch(right.String(), left.String())
}

// not like
//
//lint:ignore U1000
type notLikeOp struct {
}

func (e notLikeOp) operation(left, right *String) bool {
	return !wildcardMatch(right.String(), left.String())
}

func selectOperation(left, right *Vector, sel *SelectVector, count int, trueSel, falseSel *SelectVector, subTyp ET_SubTyp) int {
	switch subTyp {
	case ET_Equal:
		switch left.typ().getInternalType() {
		case INT32:
			return selectBinary[int32](left, right, sel, count, trueSel, falseSel, equalOp[int32]{})
		case VARCHAR:
			return selectBinary[String](left, right, sel, count, trueSel, falseSel, equalStrOp{})
		case BOOL:
			return selectBinary[bool](left, right, sel, count, trueSel, falseSel, equalOp[bool]{})
		case UINT8, INT8, UINT16, INT16, UINT32, UINT64, INT64, FLOAT, DOUBLE, INTERVAL, LIST, STRUCT, INT128, UNKNOWN, BIT, INVALID:
			panic("usp")
		default:
			panic("usp")
		}
	case ET_NotEqual, ET_NotIn:
		switch left.typ().getInternalType() {
		case INT32:
			return selectBinary[int32](left, right, sel, count, trueSel, falseSel, notEqualOp[int32]{})
		case VARCHAR:
			return selectBinary[String](left, right, sel, count, trueSel, falseSel, notEqualStrOp{})
		case BOOL, UINT8, INT8, UINT16, INT16, UINT32, UINT64, INT64, FLOAT, DOUBLE, INTERVAL, LIST, STRUCT, INT128, UNKNOWN, BIT, INVALID:
			panic("usp")
		default:
			panic("usp")
		}
	case ET_In:
		switch left.typ().getInternalType() {
		case INT32:
			return selectBinary[int32](left, right, sel, count, trueSel, falseSel, inOp[int32]{})
		case VARCHAR:
			return selectBinary[String](left, right, sel, count, trueSel, falseSel, inStrOp{})
		case BOOL, UINT8, INT8, UINT16, INT16, UINT32, UINT64, INT64, FLOAT, DOUBLE, INTERVAL, LIST, STRUCT, INT128, UNKNOWN, BIT, INVALID:
			panic("usp")
		default:
			panic("usp")
		}
	case ET_Greater:
		switch left.typ().getInternalType() {
		case INT32:
			return selectBinary[int32](left, right, sel, count, trueSel, falseSel, greatInt32Op{})
		case INT128:
			return selectBinary[Hugeint](left, right, sel, count, trueSel, falseSel, greatHugeintOp{})
		case DATE:
			return selectBinary[Date](left, right, sel, count, trueSel, falseSel, greatDateOp{})
		case FLOAT:
			return selectBinary[float32](left, right, sel, count, trueSel, falseSel, greatFloat32Op{})
		case DECIMAL:
			return selectBinary[Decimal](left, right, sel, count, trueSel, falseSel, greatDecimalOp{})
		case BOOL, UINT8, INT8, UINT16, INT16, UINT32, UINT64, INT64, DOUBLE, INTERVAL, LIST, STRUCT, VARCHAR, UNKNOWN, BIT, INVALID:
			panic("usp")
		default:
			panic("usp")
		}
	case ET_GreaterEqual:
		switch left.typ().getInternalType() {
		case INT32:
			return selectBinary[int32](left, right, sel, count, trueSel, falseSel, greatEqualInt32Op{})
		case DATE:
			return selectBinary[Date](left, right, sel, count, trueSel, falseSel, greatEqualDateOp{})
		case FLOAT:
			return selectBinary[float32](left, right, sel, count, trueSel, falseSel, greatEqualFloat32Op{})
		case BOOL, UINT8, INT8, UINT16, INT16, UINT32, UINT64, INT64, DOUBLE, INTERVAL, LIST, STRUCT, VARCHAR, INT128, UNKNOWN, BIT, INVALID:
			panic("usp")
		default:
			panic("usp")
		}
	case ET_Less:
		switch left.typ().getInternalType() {
		case INT32:
			return selectBinary[int32](left, right, sel, count, trueSel, falseSel, lessInt32Op{})
		case DATE:
			return selectBinary[Date](left, right, sel, count, trueSel, falseSel, lessDateOp{})
		case DOUBLE:
			return selectBinary[float64](left, right, sel, count, trueSel, falseSel, lessFloat64Op{})
		case BOOL, UINT8, INT8, UINT16, INT16, UINT32, UINT64, INT64, FLOAT, INTERVAL, LIST, STRUCT, VARCHAR, INT128, UNKNOWN, BIT, INVALID:
			panic("usp")
		default:
			panic("usp")
		}
	case ET_LessEqual:
		switch left.typ().getInternalType() {
		case INT32:
			return selectBinary[int32](left, right, sel, count, trueSel, falseSel, lessEqualInt32Op{})
		case DATE:
			return selectBinary[Date](left, right, sel, count, trueSel, falseSel, lessEqualDateOp{})
		case FLOAT:
			return selectBinary[float32](left, right, sel, count, trueSel, falseSel, lessEqualFloat32Op{})
		case BOOL, UINT8, INT8, UINT16, INT16, UINT32, UINT64, INT64, DOUBLE, INTERVAL, LIST, STRUCT, VARCHAR, INT128, UNKNOWN, BIT, INVALID:
			panic("usp")
		default:
			panic("usp")
		}
	case ET_Like:
		switch left.typ().getInternalType() {
		case VARCHAR:
			return selectBinary[String](left, right, sel, count, trueSel, falseSel, likeOp{})
		default:
			panic("usp")
		}
	case ET_NotLike:
		switch left.typ().getInternalType() {
		case VARCHAR:
			return selectBinary[String](left, right, sel, count, trueSel, falseSel, notLikeOp{})
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
	entryCount := entryCount(count)
	for eidx := 0; eidx < entryCount; eidx++ {
		entry := mask.getEntry(uint64(eidx))
		next := min(baseIdx+8, count)
		if AllValidInEntry(entry) {
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
		} else if NoneValidInEntry(entry) {
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
				res := rowIsValidInEntry(entry, uint64(baseIdx-start)) &&
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
