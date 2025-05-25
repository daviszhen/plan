package compute

import (
	"unsafe"

	"github.com/daviszhen/plan/pkg/chunk"
	"github.com/daviszhen/plan/pkg/common"
	"github.com/daviszhen/plan/pkg/util"
)

type Scan struct {
	_keyData    []*chunk.UnifiedFormat
	_pointers   *chunk.Vector
	_count      int
	_selVec     *chunk.SelectVector
	_foundMatch []bool
	_ht         *JoinHashTable
	_finished   bool
	_leftChunk  *chunk.Chunk
}

func NewScan(ht *JoinHashTable) *Scan {
	return &Scan{
		_pointers: chunk.NewFlatVector(common.PointerType(), util.DefaultVectorSize),
		_selVec:   chunk.NewSelectVector(util.DefaultVectorSize),
		_ht:       ht,
	}
}

func (scan *Scan) initSelVec(curSel *chunk.SelectVector) {
	nonEmptyCnt := 0
	ptrs := chunk.GetSliceInPhyFormatFlat[unsafe.Pointer](scan._pointers)
	cnt := scan._count
	for i := 0; i < cnt; i++ {
		idx := curSel.GetIndex(i)
		if ptrs[idx] != nil {
			{
				scan._selVec.SetIndex(nonEmptyCnt, idx)
				nonEmptyCnt++
			}
		}
	}
	scan._count = nonEmptyCnt

}

func (scan *Scan) Next(keys, left, result *chunk.Chunk) {
	if scan._finished {
		return
	}
	switch scan._ht._joinType {
	case LOT_JoinTypeInner:
		scan.NextInnerJoin(keys, left, result)
	case LOT_JoinTypeMARK, LOT_JoinTypeAntiMARK:
		scan.NextMarkJoin(keys, left, result)
	case LOT_JoinTypeSEMI:
		scan.NextSemiJoin(keys, left, result)
	case LOT_JoinTypeANTI:
		scan.NextAntiJoin(keys, left, result)
	case LOT_JoinTypeLeft:
		scan.NextLeftJoin(keys, left, result)
	default:
		panic("Unknown join type")
	}
}

func (scan *Scan) NextLeftJoin(keys, left, result *chunk.Chunk) {
	scan.NextInnerJoin(keys, left, result)
	if result.Card() == 0 {
		remainingCount := 0
		sel := chunk.NewSelectVector(util.DefaultVectorSize)
		for i := 0; i < left.Card(); i++ {
			if !scan._foundMatch[i] {
				sel.SetIndex(remainingCount, i)
				remainingCount++
			}
		}
		if remainingCount > 0 {
			result.Slice(left, sel, remainingCount, 0)
			for i := left.ColumnCount(); i < result.ColumnCount(); i++ {
				vec := result.Data[i]
				vec.SetPhyFormat(chunk.PF_CONST)
				chunk.SetNullInPhyFormatConst(vec, true)
			}
		}
		scan._finished = true
	}
}

func (scan *Scan) NextAntiJoin(keys, left, result *chunk.Chunk) {
	scan.ScanKeyMatches(keys)
	scan.NextSemiOrAntiJoin(keys, left, result, false)
	scan._finished = true
}

func (scan *Scan) NextSemiJoin(keys, left, result *chunk.Chunk) {
	scan.ScanKeyMatches(keys)
	scan.NextSemiOrAntiJoin(keys, left, result, true)
	scan._finished = true
}

func (scan *Scan) NextSemiOrAntiJoin(keys, left, result *chunk.Chunk, Match bool) {
	util.AssertFunc(left.ColumnCount() == result.ColumnCount())
	util.AssertFunc(keys.Card() == left.Card())

	sel := chunk.NewSelectVector(util.DefaultVectorSize)
	resultCount := 0
	for i := 0; i < keys.Card(); i++ {
		if scan._foundMatch[i] == Match {
			sel.SetIndex(resultCount, i)
			resultCount++
		}
	}

	if resultCount > 0 {
		result.Slice(left, sel, resultCount, 0)
	} else {
		util.AssertFunc(result.Card() == 0)
	}
}

func (scan *Scan) NextMarkJoin(keys, left, result *chunk.Chunk) {
	//assertFunc(result.columnCount() ==
	//	left.columnCount()+1)
	util.AssertFunc(util.Back(result.Data).Typ().Id == common.LTID_BOOLEAN)
	util.AssertFunc(scan._ht.count() > 0)
	scan.ScanKeyMatches(keys)
	scan.constructMarkJoinResult(keys, left, result)
	scan._finished = true
}

func (scan *Scan) constructMarkJoinResult(keys, left, result *chunk.Chunk) {
	result.SetCard(left.Card())
	for i := 0; i < left.ColumnCount(); i++ {
		result.Data[i].Reference(left.Data[i])
	}

	markVec := util.Back(result.Data)
	markVec.SetPhyFormat(chunk.PF_FLAT)
	markSlice := chunk.GetSliceInPhyFormatFlat[bool](markVec)
	markMask := chunk.GetMaskInPhyFormatFlat(markVec)

	for colIdx := 0; colIdx < keys.ColumnCount(); colIdx++ {
		var vdata chunk.UnifiedFormat
		keys.Data[colIdx].ToUnifiedFormat(keys.Card(), &vdata)
		if !vdata.Mask.AllValid() {
			for i := 0; i < keys.Card(); i++ {
				idx := vdata.Sel.GetIndex(i)
				markMask.Set(
					uint64(i),
					vdata.Mask.RowIsValidUnsafe(uint64(idx)))
			}
		}
	}

	if scan._foundMatch != nil {
		for i := 0; i < left.Card(); i++ {
			markSlice[i] = scan._foundMatch[i]
		}
	} else {
		for i := 0; i < left.Card(); i++ {
			markSlice[i] = false
		}
	}
}

func (scan *Scan) ScanKeyMatches(keys *chunk.Chunk) {
	matchSel := chunk.NewSelectVector(util.DefaultVectorSize)
	noMatchSel := chunk.NewSelectVector(util.DefaultVectorSize)
	for scan._count > 0 {
		matchCount := scan.resolvePredicates(keys, matchSel, noMatchSel)
		noMatchCount := scan._count - matchCount

		for i := 0; i < matchCount; i++ {
			scan._foundMatch[matchSel.GetIndex(i)] = true
		}

		scan.advancePointers(noMatchSel, noMatchCount)
	}
}

func (scan *Scan) NextInnerJoin(keys, left, result *chunk.Chunk) {
	util.AssertFunc(result.ColumnCount() ==
		left.ColumnCount()+len(scan._ht._buildTypes))
	if scan._count == 0 {
		return
	}
	resVec := chunk.NewSelectVector(util.DefaultVectorSize)
	resCnt := scan.InnerJoin(keys, resVec)
	if resCnt > 0 {
		//left part result
		result.Slice(left, resVec, resCnt, 0)
		//right part result
		for i := 0; i < len(scan._ht._buildTypes); i++ {
			vec := result.Data[left.ColumnCount()+i]
			util.AssertFunc(vec.Typ() == scan._ht._buildTypes[i])
			scan.gatherResult2(
				vec,
				resVec,
				resCnt,
				i+len(scan._ht._keyTypes))
		}
		scan.advancePointers2()
	}
}

func (scan *Scan) gatherResult(
	result *chunk.Vector,
	resVec *chunk.SelectVector,
	selVec *chunk.SelectVector,
	cnt int,
	colNo int,
) {
	scan._ht._dataCollection.gather(
		scan._ht._dataCollection._layout,
		scan._pointers,
		selVec,
		cnt,
		colNo,
		result,
		resVec,
	)
}

func (scan *Scan) gatherResult2(
	result *chunk.Vector,
	selVec *chunk.SelectVector,
	cnt int,
	colIdx int,
) {
	resVec := chunk.IncrSelectVectorInPhyFormatFlat()
	scan.gatherResult(result, resVec, selVec, cnt, colIdx)
}

func (scan *Scan) InnerJoin(keys *chunk.Chunk, resVec *chunk.SelectVector) int {
	for {
		resCnt := scan.resolvePredicates(
			keys,
			resVec,
			nil,
		)
		if len(scan._foundMatch) != 0 {
			for i := 0; i < resCnt; i++ {
				idx := resVec.GetIndex(i)
				scan._foundMatch[idx] = true
			}
		}
		if resCnt > 0 {
			return resCnt
		}

		scan.advancePointers2()
		if scan._count == 0 {
			return 0
		}
	}
}

func (scan *Scan) advancePointers2() {
	scan.advancePointers(scan._selVec, scan._count)
}

func (scan *Scan) advancePointers(sel *chunk.SelectVector, cnt int) {
	newCnt := 0
	ptrs := chunk.GetSliceInPhyFormatFlat[unsafe.Pointer](scan._pointers)

	for i := 0; i < cnt; i++ {
		idx := sel.GetIndex(i)
		temp := util.PointerAdd(ptrs[idx], scan._ht._pointerOffset)
		ptrs[idx] = util.Load[unsafe.Pointer](temp)
		if ptrs[idx] != nil {
			scan._selVec.SetIndex(newCnt, idx)
			newCnt++
		}
	}

	scan._count = newCnt
}

func (scan *Scan) resolvePredicates(
	keys *chunk.Chunk,
	matchSel *chunk.SelectVector,
	noMatchSel *chunk.SelectVector,
) int {
	for i := 0; i < scan._count; i++ {
		matchSel.SetIndex(i, scan._selVec.GetIndex(i))
	}
	noMatchCount := 0
	return Match(
		keys,
		scan._keyData,
		scan._ht._layout,
		scan._pointers,
		scan._ht._predTypes,
		matchSel,
		scan._count,
		noMatchSel,
		&noMatchCount,
	)
}
