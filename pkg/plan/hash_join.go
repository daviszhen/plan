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
	"github.com/daviszhen/plan/pkg/storage"
	"github.com/daviszhen/plan/pkg/util"
)

type HashJoinStage int

const (
	HJS_INIT HashJoinStage = iota
	HJS_BUILD
	HJS_PROBE
	HJS_SCAN_HT
	HJS_DONE
)

type HashJoin struct {
	_conds []*Expr

	//types of the keys. the type of left part in the join on condition expr.
	_keyTypes []common.LType

	//types of right children of join.
	_buildTypes []common.LType

	_ht *JoinHashTable

	//for hash join
	_buildExec *ExprExec

	_joinKeys *chunk.Chunk

	_buildChunk *chunk.Chunk

	_hjs      HashJoinStage
	_scan     *Scan
	_probExec *ExprExec
	//types of the output Chunk in Scan.Next
	_scanNextTyps []common.LType

	//colIdx of the left(right) Children in the output Chunk in Scan.Next
	_leftIndice  []int
	_rightIndice []int
	_markIndex   int
}

func NewHashJoin(op *PhysicalOperator, conds []*Expr) *HashJoin {
	hj := new(HashJoin)
	hj._hjs = HJS_INIT
	hj._conds = copyExprs(conds...)
	for _, cond := range conds {
		hj._keyTypes = append(hj._keyTypes, cond.Children[0].DataTyp)
	}

	for i, output := range op.Children[0].Outputs {
		hj._scanNextTyps = append(hj._scanNextTyps, output.DataTyp)
		hj._leftIndice = append(hj._leftIndice, i)
	}

	if op.JoinTyp != LOT_JoinTypeSEMI && op.JoinTyp != LOT_JoinTypeANTI {
		//right child output types
		rightIdxOffset := len(hj._scanNextTyps)
		for i, output := range op.Children[1].Outputs {
			hj._buildTypes = append(hj._buildTypes, output.DataTyp)
			hj._scanNextTyps = append(hj._scanNextTyps, output.DataTyp)
			hj._rightIndice = append(hj._rightIndice, rightIdxOffset+i)
		}
	}

	if op.JoinTyp == LOT_JoinTypeMARK || op.JoinTyp == LOT_JoinTypeAntiMARK {
		hj._scanNextTyps = append(hj._scanNextTyps, common.BooleanType())
		hj._markIndex = len(hj._scanNextTyps) - 1
	}
	//
	hj._buildChunk = &chunk.Chunk{}
	hj._buildChunk.Init(hj._buildTypes, util.DefaultVectorSize)

	//
	hj._buildExec = &ExprExec{}
	for _, cond := range hj._conds {
		//FIXME: Children[1] may not be from right part
		//FIX it in the build stage.
		hj._buildExec.addExpr(cond.Children[1])
	}

	hj._joinKeys = &chunk.Chunk{}
	hj._joinKeys.Init(hj._keyTypes, util.DefaultVectorSize)

	hj._ht = NewJoinHashTable(conds, hj._buildTypes, op.JoinTyp)

	hj._probExec = &ExprExec{}
	for _, cond := range hj._conds {
		hj._probExec.addExpr(cond.Children[0])
	}
	return hj
}

func (hj *HashJoin) Build(input *chunk.Chunk) error {
	var err error
	//evaluate the right part
	hj._joinKeys.Reset()
	err = hj._buildExec.executeExprs([]*chunk.Chunk{nil, input, nil},
		hj._joinKeys)
	if err != nil {
		return err
	}

	//build th
	if len(hj._buildTypes) != 0 {
		hj._ht.Build(hj._joinKeys, input)
	} else {
		hj._buildChunk.SetCard(input.Card())
		hj._ht.Build(hj._joinKeys, hj._buildChunk)
	}
	return nil
}

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

type JoinHashTable struct {
	_conds []*Expr

	//types of keys in equality comparison
	_equalTypes []common.LType

	_keyTypes []common.LType

	_buildTypes []common.LType

	_predTypes []ET_SubTyp

	_layout *TupleDataLayout

	_joinType LOT_JoinType

	_finalized bool

	//does any of key elements contain NULL
	_hasNull bool

	// total size of tuple
	_tupleSize int

	//next pointer offset in tuple
	_pointerOffset int

	//size of entry
	_entrySize int

	_dataCollection *TupleDataCollection
	_pinState       *TupleDataPinState
	_chunkState     *TupleDataChunkState
	_hashMap        []unsafe.Pointer
	_bitmask        int
}

func NewJoinHashTable(conds []*Expr,
	buildTypes []common.LType,
	joinTyp LOT_JoinType) *JoinHashTable {
	ht := &JoinHashTable{
		_conds:      copyExprs(conds...),
		_buildTypes: common.CopyLTypes(buildTypes...),
		_joinType:   joinTyp,
	}
	for _, cond := range conds {
		typ := cond.Children[0].DataTyp
		if cond.SubTyp == ET_Equal || cond.SubTyp == ET_In {
			util.AssertFunc(len(ht._equalTypes) == len(ht._keyTypes))
			ht._equalTypes = append(ht._equalTypes, typ)
		}
		ht._predTypes = append(ht._predTypes, cond.SubTyp)
		ht._keyTypes = append(ht._keyTypes, typ)
	}
	util.AssertFunc(len(ht._equalTypes) != 0)
	layoutTypes := make([]common.LType, 0)
	layoutTypes = append(layoutTypes, ht._keyTypes...)
	layoutTypes = append(layoutTypes, ht._buildTypes...)
	layoutTypes = append(layoutTypes, common.HashType())
	// init layout
	ht._layout = NewTupleDataLayout(layoutTypes, nil, nil, true, true)
	offsets := ht._layout.offsets()
	ht._tupleSize = offsets[len(ht._keyTypes)+len(ht._buildTypes)]

	//?
	ht._pointerOffset = offsets[len(offsets)-1]
	ht._entrySize = ht._layout.rowWidth()

	ht._dataCollection = NewTupleDataCollection(ht._layout)
	ht._pinState = NewTupleDataPinState()
	ht._dataCollection.InitAppend(ht._pinState, PIN_PRRP_KEEP_PINNED)
	return ht
}

func (jht *JoinHashTable) Build(keys *chunk.Chunk, payload *chunk.Chunk) {
	util.AssertFunc(!jht._finalized)
	util.AssertFunc(keys.Card() == payload.Card())
	if keys.Card() == 0 {
		return
	}
	var keyData []*chunk.UnifiedFormat
	var curSel *chunk.SelectVector
	sel := chunk.NewSelectVector(util.DefaultVectorSize)
	addedCnt := jht.prepareKeys(keys,
		&keyData,
		&curSel,
		sel,
		true,
	)
	if addedCnt < keys.Card() {
		jht._hasNull = true
	}
	if addedCnt == 0 {
		return
	}

	//hash keys
	hashValues := chunk.NewVector2(common.HashType(), util.DefaultVectorSize)
	jht.hash(keys, curSel, addedCnt, hashValues)

	//append data collection
	sourceChunk := &chunk.Chunk{}
	sourceChunk.Init(jht._layout.types(), util.DefaultVectorSize)
	for i := 0; i < keys.ColumnCount(); i++ {
		sourceChunk.Data[i].Reference(keys.Data[i])
	}
	colOffset := keys.ColumnCount()
	util.AssertFunc(len(jht._buildTypes) == payload.ColumnCount())
	for i := 0; i < payload.ColumnCount(); i++ {
		sourceChunk.Data[colOffset+i].Reference(payload.Data[i])
	}
	colOffset += payload.ColumnCount()
	sourceChunk.Data[colOffset].Reference(hashValues)
	sourceChunk.SetCard(keys.Card())
	if addedCnt < keys.Card() {
		sourceChunk.SliceItself(curSel, addedCnt)
	}

	jht._chunkState = NewTupleDataChunkState(jht._layout.columnCount(), 0)
	//save data collection
	jht._dataCollection.Append(
		jht._pinState,
		jht._chunkState,
		sourceChunk,
		chunk.IncrSelectVectorInPhyFormatFlat(),
		sourceChunk.Card())
}

func (jht *JoinHashTable) hash(
	keys *chunk.Chunk,
	sel *chunk.SelectVector,
	count int,
	hashes *chunk.Vector,
) {
	chunk.HashTypeSwitch(keys.Data[0], hashes, sel, count, sel != nil)
	//combine hash
	for i := 1; i < len(jht._equalTypes); i++ {
		chunk.CombineHashTypeSwitch(hashes, keys.Data[i], sel, count, sel != nil)
	}
}

func (jht *JoinHashTable) prepareKeys(
	keys *chunk.Chunk,
	keyData *[]*chunk.UnifiedFormat,
	curSel **chunk.SelectVector,
	sel *chunk.SelectVector,
	buildSide bool) int {
	*keyData = keys.ToUnifiedFormat()

	//which keys are NULL
	*curSel = chunk.IncrSelectVectorInPhyFormatFlat()

	addedCount := keys.Card()
	for i := 0; i < keys.ColumnCount(); i++ {
		if (*keyData)[i].Mask.AllValid() {
			continue
		}
		addedCount = filterNullValues(
			(*keyData)[i],
			*curSel,
			addedCount,
			sel,
		)
		*curSel = sel
	}
	return addedCount
}

func filterNullValues(
	vdata *chunk.UnifiedFormat,
	sel *chunk.SelectVector,
	count int,
	result *chunk.SelectVector) int {

	res := 0
	for i := 0; i < count; i++ {
		idx := sel.GetIndex(i)
		keyIdx := vdata.Sel.GetIndex(idx)
		if vdata.Mask.RowIsValid(uint64(keyIdx)) {
			result.SetIndex(res, idx)
			res++
		}
	}
	return res
}

func pointerTableCap(cnt int) int {
	return max(int(util.NextPowerOfTwo(uint64(cnt*2))), 1024)
}

func (jht *JoinHashTable) InitPointerTable() {
	pCap := pointerTableCap(jht._dataCollection.Count())
	util.AssertFunc(util.IsPowerOfTwo(uint64(pCap)))
	jht._hashMap = make([]unsafe.Pointer, pCap)
	jht._bitmask = pCap - 1
}

func (jht *JoinHashTable) Finalize() {
	jht.InitPointerTable()
	hashes := chunk.NewFlatVector(common.HashType(), util.DefaultVectorSize)
	hashSlice := chunk.GetSliceInPhyFormatFlat[uint64](hashes)
	iter := NewTupleDataChunkIterator2(
		jht._dataCollection,
		PIN_PRRP_KEEP_PINNED,
		false,
	)
	for {
		//reset hashes
		for j := 0; j < util.DefaultVectorSize; j++ {
			hashSlice[j] = uint64(0)
		}
		rowLocs := iter.GetRowLocations()
		count := iter.GetCurrentChunkCount()
		for i := 0; i < count; i++ {
			hashSlice[i] = util.Load[uint64](
				util.PointerAdd(rowLocs[i],
					jht._pointerOffset))
		}
		jht.InsertHashes(hashes, count, rowLocs)
		next := iter.Next()
		if !next {
			break
		}
	}
	jht._finalized = true
}

//func (jht *JoinHashTable) printHashMap() {
//	pointers := jht._hashMap
//	for i, ptr := range pointers {
//		fmt.Println("bucket", i, "base", ptr)
//		next := ptr
//		dedup := make(map[unsafe.Pointer]struct{})
//		dedup[ptr] = struct{}{}
//		for next != nil {
//			val := load[unsafe.Pointer](pointerAdd(next, jht._pointerOffset))
//			fmt.Println("    base", next, "next", val)
//			if _, has := dedup[val]; has {
//				fmt.Println("    base", ptr, "loop")
//				panic("get a loop in bucket")
//				//break
//			}
//			dedup[val] = struct{}{}
//			next = val
//		}
//	}
//}

func (jht *JoinHashTable) InsertHashes(hashes *chunk.Vector, cnt int, keyLocs []unsafe.Pointer) {
	jht.ApplyBitmask(hashes, cnt)
	hashes.Flatten(cnt)
	util.AssertFunc(hashes.PhyFormat().IsFlat())
	pointers := jht._hashMap
	indices := chunk.GetSliceInPhyFormatFlat[uint64](hashes)
	InsertHashesLoop(pointers, indices, cnt, keyLocs, jht._pointerOffset)
}

func InsertHashesLoop(
	pointers []unsafe.Pointer,
	indices []uint64,
	cnt int,
	keyLocs []unsafe.Pointer,
	pointerOffset int,
) {
	for i := 0; i < cnt; i++ {
		idx := indices[i]
		//save prev into the pointer in tuple

		util.Store[unsafe.Pointer](pointers[idx], util.PointerAdd(keyLocs[i], pointerOffset))
		//pointer to current tuple
		pointers[idx] = keyLocs[i]
		base := keyLocs[i]
		cur := util.Load[unsafe.Pointer](util.PointerAdd(keyLocs[i], pointerOffset))
		if base == cur {
			panic("insert loop in bucket")
		}
	}
}

func (jht *JoinHashTable) ApplyBitmask(hashes *chunk.Vector, cnt int) {
	if hashes.PhyFormat().IsConst() {
		indices := chunk.GetSliceInPhyFormatConst[uint64](hashes)
		indices[0] &= uint64(jht._bitmask)
	} else {
		hashes.Flatten(cnt)
		indices := chunk.GetSliceInPhyFormatFlat[uint64](hashes)
		for i := 0; i < cnt; i++ {
			indices[i] &= uint64(jht._bitmask)
		}
	}
}

func (jht *JoinHashTable) ApplyBitmask2(
	hashes *chunk.Vector,
	sel *chunk.SelectVector,
	cnt int,
	pointers *chunk.Vector,
) {
	var data chunk.UnifiedFormat
	hashes.ToUnifiedFormat(cnt, &data)
	hashSlice := chunk.GetSliceInPhyFormatUnifiedFormat[uint64](&data)
	resSlice := chunk.GetSliceInPhyFormatFlat[unsafe.Pointer](pointers)
	mainHt := jht._hashMap
	for i := 0; i < cnt; i++ {
		rIdx := sel.GetIndex(i)
		hIdx := data.Sel.GetIndex(rIdx)
		hVal := hashSlice[hIdx]
		bucket := mainHt[(hVal & uint64(jht._bitmask))]
		resSlice[rIdx] = bucket
	}

}

func (jht *JoinHashTable) Probe(keys *chunk.Chunk) *Scan {
	var curSel *chunk.SelectVector
	newScan := jht.initScan(keys, &curSel)
	if newScan._count == 0 {
		return newScan
	}
	hashes := chunk.NewFlatVector(common.HashType(), util.DefaultVectorSize)
	jht.hash(keys, curSel, newScan._count, hashes)

	jht.ApplyBitmask2(hashes, curSel, newScan._count, newScan._pointers)
	newScan.initSelVec(curSel)
	return newScan
}

func (jht *JoinHashTable) initScan(keys *chunk.Chunk, curSel **chunk.SelectVector) *Scan {
	util.AssertFunc(jht.count() > 0)
	util.AssertFunc(jht._finalized)
	newScan := NewScan(jht)
	if jht._joinType != LOT_JoinTypeInner {
		newScan._foundMatch = make([]bool, util.DefaultVectorSize)
	}

	newScan._count = jht.prepareKeys(
		keys,
		&newScan._keyData,
		curSel,
		newScan._selVec,
		false)
	return newScan
}

func (jht *JoinHashTable) count() int {
	return jht._dataCollection.Count()
}

type JoinScan struct {
}

func (js *JoinScan) Next() {

}

func (js *JoinScan) NextInnerJoin() {

}

/*
TupleDataLayout
format:

					bitmap | heap_size_offset? | data columns | aggr fields |
	                |------|----------------------------------|-------------|
	                  |                |                               |    |
	                  |                |                               |    |

bitmapWidth-----------|                |                               |    |
dataWidth------------------------------|                               |    |
aggrWidth--------------------------------------------------------------|    |
rowWidth--------------------------------------------------------------------|

offsets: start of data columns and aggr fields
*/
type TupleDataLayout struct {
	//types of the data columns
	_types               []common.LType
	_childrenOutputTypes []common.LType

	//Width of bitmap header
	_bitmapWidth int

	//Width of data part
	_dataWidth int

	//Width of agg state part
	_aggWidth int

	//Width of entire row
	_rowWidth int

	//offsets to the columns and agg data in each row
	_offsets []int

	//all columns in this layout are constant size
	_allConst bool

	//offset to the heap size of each row
	_heapSizeOffset int

	_aggregates []*AggrObject
}

func NewTupleDataLayout(types []common.LType, aggrObjs []*AggrObject, childrenOutputTypes []common.LType, align bool, needHeapOffset bool) *TupleDataLayout {
	layout := &TupleDataLayout{
		_types:               common.CopyLTypes(types...),
		_childrenOutputTypes: common.CopyLTypes(childrenOutputTypes...),
	}

	alignWidth := func() {
		if align {
			layout._rowWidth = util.AlignValue8(layout._rowWidth)
		}
	}

	layout._bitmapWidth = util.EntryCount(len(layout._types) + len(layout._childrenOutputTypes))
	layout._rowWidth = layout._bitmapWidth
	alignWidth()

	//all columns + children output columns are constant size
	for _, lType := range append(layout._types, layout._childrenOutputTypes...) {
		layout._allConst = layout._allConst &&
			lType.GetInternalType().IsConstant()
	}

	if needHeapOffset && !layout._allConst {
		layout._heapSizeOffset = layout._rowWidth
		layout._rowWidth += common.Int64Size
		alignWidth()
	}

	//data columns + children output columns
	for _, lType := range append(layout._types, layout._childrenOutputTypes...) {
		layout._offsets = append(layout._offsets, layout._rowWidth)
		if lType.GetInternalType().IsConstant() ||
			lType.GetInternalType().IsVarchar() {
			layout._rowWidth += lType.GetInternalType().Size()
		} else {
			//for variable length types, pointer to the actual data
			layout._rowWidth += common.Int64Size
		}
		alignWidth()
	}

	layout._dataWidth = layout._rowWidth - layout._bitmapWidth

	layout._aggregates = aggrObjs
	for _, aggrObj := range aggrObjs {
		layout._offsets = append(layout._offsets, layout._rowWidth)
		layout._rowWidth += aggrObj._payloadSize
		alignWidth()
	}

	layout._aggWidth = layout._rowWidth - layout.dataWidth() - layout.dataOffset()

	return layout
}

func (layout *TupleDataLayout) columnCount() int {
	return len(layout._types)
}

func (layout *TupleDataLayout) childrenOutputCount() int {
	return len(layout._childrenOutputTypes)
}

func (layout *TupleDataLayout) types() []common.LType {
	return common.CopyLTypes(layout._types...)
}

// total Width of each row
func (layout *TupleDataLayout) rowWidth() int {
	return layout._rowWidth
}

// start of the data in each row
func (layout *TupleDataLayout) dataOffset() int {
	return layout._bitmapWidth
}

// total Width of the data
func (layout *TupleDataLayout) dataWidth() int {
	return layout._dataWidth
}

// start of agg
func (layout *TupleDataLayout) aggrOffset() int {
	return layout._bitmapWidth + layout._dataWidth
}

func (layout *TupleDataLayout) aggrIdx() int {
	return layout.columnCount() + layout.childrenOutputCount()
}

func (layout *TupleDataLayout) offsets() []int {
	return util.CopyTo[int](layout._offsets)
}

func (layout *TupleDataLayout) allConst() bool {
	return layout._allConst
}

func (layout *TupleDataLayout) heapSizeOffset() int {
	return layout._heapSizeOffset
}

func (layout *TupleDataLayout) copy() *TupleDataLayout {
	if layout == nil {
		return nil
	}
	res := &TupleDataLayout{}
	res._types = common.CopyLTypes(layout._types...)
	res._childrenOutputTypes = common.CopyLTypes(layout._childrenOutputTypes...)
	res._bitmapWidth = layout._bitmapWidth
	res._dataWidth = layout._dataWidth
	res._aggWidth = layout._aggWidth
	res._rowWidth = layout._rowWidth
	res._offsets = util.CopyTo[int](layout._offsets)
	res._allConst = layout._allConst
	res._heapSizeOffset = layout._heapSizeOffset
	return res
}

type TupleDataCollection struct {
	_layout   *TupleDataLayout
	_count    uint64
	_dedup    map[unsafe.Pointer]struct{}
	_alloc    *TupleDataAllocator
	_segments []*TupleDataSegment
}

func NewTupleDataCollection(layout *TupleDataLayout) *TupleDataCollection {
	ret := &TupleDataCollection{
		_layout: layout.copy(),
		_dedup:  make(map[unsafe.Pointer]struct{}),
		_alloc:  NewTupleDataAllocator(storage.GBufferMgr, layout),
	}
	return ret
}

func (tuple *TupleDataCollection) Count() int {
	return int(tuple._count)
}

func (tuple *TupleDataCollection) Append(
	pinState *TupleDataPinState,
	chunkState *TupleDataChunkState,
	chunk *chunk.Chunk,
	appendSel *chunk.SelectVector,
	appendCount int,
) {
	//to unified format
	toUnifiedFormat(chunkState, chunk)
	tuple.AppendUnified(pinState, chunkState, chunk, nil, appendSel, appendCount)
}

func (tuple *TupleDataCollection) AppendUnified(
	pinState *TupleDataPinState,
	chunkState *TupleDataChunkState,
	chunk *chunk.Chunk,
	childrenOutput *chunk.Chunk,
	appendSel *chunk.SelectVector,
	cnt int) {
	if cnt == -1 {
		cnt = chunk.Card()
	}
	if cnt == 0 {
		return
	}
	//evaluate the heap size
	if !tuple._layout.allConst() {
		tuple.computeHeapSizes(chunkState, chunk, childrenOutput, appendSel, cnt)
	}

	//allocate buffer space for incoming Chunk in chunkstate
	tuple.Build(pinState, chunkState, 0, uint64(cnt))

	//fill row
	tuple.scatter(chunkState, chunk, appendSel, cnt, false)

	if childrenOutput != nil {
		//scatter rows
		tuple.scatter(chunkState, childrenOutput, appendSel, cnt, true)
	}
}

func (tuple *TupleDataCollection) checkDupAll() {
	//cnt := 0
	////all dup
	//dedup := make(map[unsafe.Pointer]int)
	//for xid, xpart := range tuple._parts {
	//	xrowLocs := getSliceInPhyFormatFlat[unsafe.Pointer](xpart.rowLocations)
	//	for _, loc := range xrowLocs {
	//		if uintptr(loc) == 0 {
	//			continue
	//		}
	//		if xcnt, has := dedup[loc]; has {

	//			panic("dup loc2")
	//		}
	//		dedup[loc] = cnt
	//		cnt++
	//	}
	//}
}

func (tuple *TupleDataCollection) scatter(
	state *TupleDataChunkState,
	data *chunk.Chunk,
	appendSel *chunk.SelectVector,
	cnt int,
	isChildrenOutput bool,
) {
	rowLocations := chunk.GetSliceInPhyFormatFlat[unsafe.Pointer](state._rowLocations)
	//set bitmap
	if !isChildrenOutput {
		bitsCnt := tuple._layout.columnCount() + tuple._layout.childrenOutputCount()
		maskBytes := util.SizeInBytes(bitsCnt)
		for i := 0; i < cnt; i++ {
			util.Memset(rowLocations[i], 0xFF, maskBytes)
		}
	}

	if !tuple._layout.allConst() {
		heapSizeOffset := tuple._layout.heapSizeOffset()
		heapSizes := chunk.GetSliceInPhyFormatFlat[uint64](state._heapSizes)
		for i := 0; i < cnt; i++ {
			util.Store[uint64](heapSizes[i], util.PointerAdd(rowLocations[i], heapSizeOffset))
		}
	}
	if isChildrenOutput {
		for i, colIdx := range state._childrenOutputIds {
			tuple.scatterVector(state, data.Data[i], colIdx, appendSel, cnt)
		}
	} else {
		for i, coldIdx := range state._columnIds {
			tuple.scatterVector(state, data.Data[i], coldIdx, appendSel, cnt)
		}
	}
}

func (tuple *TupleDataCollection) scatterVector(
	state *TupleDataChunkState,
	src *chunk.Vector,
	colIdx int,
	appendSel *chunk.SelectVector,
	cnt int) {
	TupleDataTemplatedScatterSwitch(
		src,
		&state._data[colIdx],
		appendSel,
		cnt,
		tuple._layout,
		state._rowLocations,
		state._heapLocations,
		colIdx)
}

// convert chunk into state.data unified format
func toUnifiedFormat(state *TupleDataChunkState, chunk *chunk.Chunk) {
	for i, colId := range state._columnIds {
		chunk.Data[i].ToUnifiedFormat(chunk.Card(), &state._data[colId])
	}
}

func toUnifiedFormatForChildrenOutput(state *TupleDataChunkState, chunk *chunk.Chunk) {
	for i, colIdx := range state._childrenOutputIds {
		chunk.Data[i].ToUnifiedFormat(chunk.Card(), &state._data[colIdx])
	}
}

// result refers to chunk state.data unified format
func getVectorData(state *TupleDataChunkState, result []*chunk.UnifiedFormat) {
	vectorData := state._data
	for i := 0; i < len(result); i++ {
		target := result[i]
		target.Sel = vectorData[i].Sel
		target.Data = vectorData[i].Data
		target.Mask = vectorData[i].Mask
		target.PTypSize = vectorData[i].PTypSize
	}
}

// evaluate the heap size of columns
func (tuple *TupleDataCollection) computeHeapSizes(
	state *TupleDataChunkState,
	data *chunk.Chunk,
	childrenOutput *chunk.Chunk,
	appendSel *chunk.SelectVector,
	cnt int) {

	//heapSizes records the heap size of every row
	heapSizesSlice := chunk.GetSliceInPhyFormatFlat[uint64](state._heapSizes)
	util.Fill[uint64](heapSizesSlice, util.Size(heapSizesSlice), 0)

	for i := 0; i < data.ColumnCount(); i++ {
		tuple.evaluateHeapSizes(state._heapSizes, data.Data[i], &state._data[i], appendSel, cnt)
	}

	for i := 0; i < childrenOutput.ColumnCount(); i++ {
		colIdx := state._childrenOutputIds[i]
		tuple.evaluateHeapSizes(state._heapSizes, childrenOutput.Data[i], &state._data[colIdx], appendSel, cnt)
	}
}

func (tuple *TupleDataCollection) evaluateHeapSizes(
	sizes *chunk.Vector,
	src *chunk.Vector,
	srcUni *chunk.UnifiedFormat,
	appendSel *chunk.SelectVector,
	cnt int) {
	//only care varchar type
	pTyp := src.Typ().GetInternalType()
	switch pTyp {
	case common.VARCHAR:
	default:
		return
	}
	heapSizeSlice := chunk.GetSliceInPhyFormatFlat[uint64](sizes)
	srcSel := srcUni.Sel
	srcMask := srcUni.Mask

	switch pTyp {
	case common.VARCHAR:
		srcSlice := chunk.GetSliceInPhyFormatUnifiedFormat[common.String](srcUni)
		for i := 0; i < cnt; i++ {
			srcIdx := srcSel.GetIndex(appendSel.GetIndex(i))
			if srcMask.RowIsValid(uint64(srcIdx)) {
				heapSizeSlice[i] += uint64(srcSlice[srcIdx].Length())
			} else {
				heapSizeSlice[i] += uint64(common.String{}.NullLen())
			}
		}

	default:
		panic("usp phy type")
	}
}

func (tuple *TupleDataCollection) gather(
	layout *TupleDataLayout,
	rowLocs *chunk.Vector,
	scanSel *chunk.SelectVector,
	scanCnt int,
	colId int,
	result *chunk.Vector,
	targetSel *chunk.SelectVector) {
	TupleDataTemplatedGatherSwitch(
		layout,
		rowLocs,
		colId,
		scanSel,
		scanCnt,
		result,
		targetSel,
	)
}

func (tuple *TupleDataCollection) Gather(
	layout *TupleDataLayout,
	rowLocs *chunk.Vector,
	scanSel *chunk.SelectVector,
	scanCnt int,
	colIds []int,
	result *chunk.Chunk,
	targetSel *chunk.SelectVector,
) {
	for i := 0; i < len(colIds); i++ {
		tuple.gather(
			layout,
			rowLocs,
			scanSel,
			scanCnt,
			colIds[i],
			result.Data[i],
			targetSel)
	}
}

func TupleDataTemplatedScatterSwitch(
	src *chunk.Vector,
	srcFormat *chunk.UnifiedFormat,
	appendSel *chunk.SelectVector,
	cnt int,
	layout *TupleDataLayout,
	rowLocations *chunk.Vector,
	heapLocations *chunk.Vector,
	colIdx int) {
	pTyp := src.Typ().GetInternalType()
	switch pTyp {
	case common.INT32:
		TupleDataTemplatedScatter[int32](
			srcFormat,
			appendSel,
			cnt,
			layout,
			rowLocations,
			heapLocations,
			colIdx,
			chunk.Int32ScatterOp{},
		)
	case common.INT64:
		TupleDataTemplatedScatter[int64](
			srcFormat,
			appendSel,
			cnt,
			layout,
			rowLocations,
			heapLocations,
			colIdx,
			chunk.Int64ScatterOp{},
		)
	case common.UINT64:
		TupleDataTemplatedScatter[uint64](
			srcFormat,
			appendSel,
			cnt,
			layout,
			rowLocations,
			heapLocations,
			colIdx,
			chunk.Uint64ScatterOp{},
		)
	case common.VARCHAR:
		TupleDataTemplatedScatter[common.String](
			srcFormat,
			appendSel,
			cnt,
			layout,
			rowLocations,
			heapLocations,
			colIdx,
			chunk.StringScatterOp{},
		)
	case common.INT8:
		TupleDataTemplatedScatter[int8](
			srcFormat,
			appendSel,
			cnt,
			layout,
			rowLocations,
			heapLocations,
			colIdx,
			chunk.Int8ScatterOp{},
		)
	case common.DECIMAL:
		TupleDataTemplatedScatter[common.Decimal](
			srcFormat,
			appendSel,
			cnt,
			layout,
			rowLocations,
			heapLocations,
			colIdx,
			chunk.DecimalScatterOp{},
		)
	case common.DATE:
		TupleDataTemplatedScatter[common.Date](
			srcFormat,
			appendSel,
			cnt,
			layout,
			rowLocations,
			heapLocations,
			colIdx,
			chunk.DateScatterOp{},
		)
	case common.DOUBLE:
		TupleDataTemplatedScatter[float64](
			srcFormat,
			appendSel,
			cnt,
			layout,
			rowLocations,
			heapLocations,
			colIdx,
			chunk.Float64ScatterOp{},
		)
	case common.INT128:
		TupleDataTemplatedScatter[common.Hugeint](
			srcFormat,
			appendSel,
			cnt,
			layout,
			rowLocations,
			heapLocations,
			colIdx,
			chunk.HugeintScatterOp{},
		)
	default:
		panic("usp ")
	}
}

func TupleDataTemplatedScatter[T any](
	srcFormat *chunk.UnifiedFormat,
	appendSel *chunk.SelectVector,
	cnt int,
	layout *TupleDataLayout,
	rowLocations *chunk.Vector,
	heapLocations *chunk.Vector,
	colIdx int,
	nVal chunk.ScatterOp[T],
) {
	srcSel := srcFormat.Sel
	srcSlice := chunk.GetSliceInPhyFormatUnifiedFormat[T](srcFormat)
	srcMask := srcFormat.Mask

	targetLocs := chunk.GetSliceInPhyFormatFlat[unsafe.Pointer](rowLocations)
	targetHeapLocs := chunk.GetSliceInPhyFormatFlat[unsafe.Pointer](heapLocations)
	offsetInRow := layout.offsets()[colIdx]
	if srcMask.AllValid() {
		for i := 0; i < cnt; i++ {
			srcIdx := srcSel.GetIndex(appendSel.GetIndex(i))
			TupleDataValueStore[T](srcSlice[srcIdx], targetLocs[i], offsetInRow, &targetHeapLocs[i], nVal)
		}
	} else {
		for i := 0; i < cnt; i++ {
			srcIdx := srcSel.GetIndex(appendSel.GetIndex(i))
			if srcMask.RowIsValid(uint64(srcIdx)) {
				TupleDataValueStore[T](srcSlice[srcIdx], targetLocs[i], offsetInRow, &targetHeapLocs[i], nVal)
			} else {
				TupleDataValueStore[T](nVal.NullValue(), targetLocs[i], offsetInRow, &targetHeapLocs[i], nVal)
				bSlice := util.PointerToSlice[uint8](targetLocs[i], layout._rowWidth)
				tempMask := util.Bitmap{Bits: bSlice}
				tempMask.SetInvalidUnsafe(uint64(colIdx))
			}
		}
	}

}

func TupleDataValueStore[T any](src T, rowLoc unsafe.Pointer, offsetInRow int, heapLoc *unsafe.Pointer, nVal chunk.ScatterOp[T]) {
	nVal.Store(src, rowLoc, offsetInRow, heapLoc)
}

func TupleDataTemplatedGatherSwitch(
	layout *TupleDataLayout,
	rowLocs *chunk.Vector,
	colIdx int,
	scanSel *chunk.SelectVector,
	scanCnt int,
	target *chunk.Vector,
	targetSel *chunk.SelectVector,
) {
	pTyp := target.Typ().GetInternalType()
	switch pTyp {
	case common.INT32:
		TupleDataTemplatedGather[int32](
			layout,
			rowLocs,
			colIdx,
			scanSel,
			scanCnt,
			target,
			targetSel,
		)
	case common.INT64:
		TupleDataTemplatedGather[int64](
			layout,
			rowLocs,
			colIdx,
			scanSel,
			scanCnt,
			target,
			targetSel,
		)
	case common.VARCHAR:
		TupleDataTemplatedGather[common.String](
			layout,
			rowLocs,
			colIdx,
			scanSel,
			scanCnt,
			target,
			targetSel,
		)
	case common.DECIMAL:
		TupleDataTemplatedGather[common.Decimal](
			layout,
			rowLocs,
			colIdx,
			scanSel,
			scanCnt,
			target,
			targetSel,
		)
	case common.DATE:
		TupleDataTemplatedGather[common.Date](
			layout,
			rowLocs,
			colIdx,
			scanSel,
			scanCnt,
			target,
			targetSel,
		)
	case common.INT128:
		TupleDataTemplatedGather[common.Hugeint](
			layout,
			rowLocs,
			colIdx,
			scanSel,
			scanCnt,
			target,
			targetSel,
		)
	default:
		panic("usp phy type")
	}
}

func TupleDataTemplatedGather[T any](
	layout *TupleDataLayout,
	rowLocs *chunk.Vector,
	colIdx int,
	scanSel *chunk.SelectVector,
	scanCnt int,
	target *chunk.Vector,
	targetSel *chunk.SelectVector,
) {
	srcLocs := chunk.GetSliceInPhyFormatFlat[unsafe.Pointer](rowLocs)
	targetData := chunk.GetSliceInPhyFormatFlat[T](target)
	targetBitmap := chunk.GetMaskInPhyFormatFlat(target)
	entryIdx, idxInEntry := util.GetEntryIndex(uint64(colIdx))
	offsetInRow := layout.offsets()[colIdx]
	for i := 0; i < scanCnt; i++ {
		base := srcLocs[scanSel.GetIndex(i)]
		srcRow := util.PointerToSlice[byte](base, layout._rowWidth)
		targetIdx := targetSel.GetIndex(i)
		rowMask := util.Bitmap{Bits: srcRow}
		if util.RowIsValidInEntry(
			rowMask.GetEntry(entryIdx),
			idxInEntry) {
			targetData[targetIdx] = util.Load[T](util.PointerAdd(base, offsetInRow))

		} else {
			targetBitmap.SetInvalid(uint64(targetIdx))
		}
	}
}

func (tuple *TupleDataCollection) InitScan(state *TupleDataScanState) {
	////////
	state._pinState._rowHandles = make(map[uint32]*storage.BufferHandle)
	state._pinState._heapHandles = make(map[uint32]*storage.BufferHandle)
	state._pinState._properties = PIN_PRRP_KEEP_PINNED

	state._segmentIdx = 0
	state._chunkIdx = 0
	state._chunkState = *NewTupleDataChunkState(tuple._layout.columnCount(), tuple._layout.childrenOutputCount())
	//read children output also
	state._chunkState._columnIds = state._colIds
	state._chunkState._columnIds = append(state._chunkState._columnIds, state._chunkState._childrenOutputIds...)
	////////
	state._init = true
}

func (tuple *TupleDataCollection) NextScanIndex(
	state *TupleDataScanState,
	segIdx *int,
	chunkIdx *int) bool {
	if state._segmentIdx >= util.Size(tuple._segments) {
		//no data
		return false
	}

	for state._chunkIdx >=
		util.Size(tuple._segments[state._segmentIdx]._chunks) {
		state._segmentIdx++
		state._chunkIdx = 0
		if state._segmentIdx >= util.Size(tuple._segments) {
			//no data
			return false
		}
	}

	*segIdx = state._segmentIdx
	*chunkIdx = state._chunkIdx
	state._chunkIdx++
	return true
}

func (tuple *TupleDataCollection) ScanAtIndex(
	pinState *TupleDataPinState,
	chunkState *TupleDataChunkState,
	colIdxs []int,
	segIndx, chunkIndx int,
	result *chunk.Chunk) {
	seg := tuple._segments[segIndx]
	data := seg._chunks[chunkIndx]
	seg._allocator.InitChunkState(
		seg,
		pinState,
		chunkState,
		chunkIndx,
		false,
	)
	result.Reset()
	tuple.Gather(
		tuple._layout,
		chunkState._rowLocations,
		chunk.IncrSelectVectorInPhyFormatFlat(),
		int(data._count),
		colIdxs,
		result,
		chunk.IncrSelectVectorInPhyFormatFlat(),
	)
	result.SetCard(int(data._count))
}

func (tuple *TupleDataCollection) Scan(state *TupleDataScanState, result *chunk.Chunk) bool {
	oldSegIdx := state._segmentIdx
	var segIdx, chunkIdx int
	if !tuple.NextScanIndex(state, &segIdx, &chunkIdx) {
		return false
	}

	if uint32(oldSegIdx) != INVALID_INDEX &&
		segIdx != oldSegIdx {
		tuple.FinalizePinState2(&state._pinState,
			tuple._segments[oldSegIdx])
	}

	tuple.ScanAtIndex(
		&state._pinState,
		&state._chunkState,
		state._chunkState._columnIds,
		segIdx,
		chunkIdx,
		result,
	)
	return true
}

// Build allocate space for chunk
// may create new segment or new block
func (tuple *TupleDataCollection) Build(
	pinState *TupleDataPinState,
	chunkState *TupleDataChunkState,
	appendOffset uint64,
	appendCount uint64,
) {
	seg := util.Back(tuple._segments)
	seg._allocator.Build(seg, pinState, chunkState, appendOffset, appendCount)
	tuple._count += appendCount
}

func (tuple *TupleDataCollection) InitAppend(pinState *TupleDataPinState, prop TupleDataPinProperties) {
	pinState._properties = prop
	if len(tuple._segments) == 0 {
		tuple._segments = append(tuple._segments,
			NewTupleDataSegment(tuple._alloc))
	}
}

func (tuple *TupleDataCollection) FinalizePinState(pinState *TupleDataPinState) {
	util.AssertFunc(len(tuple._segments) == 1)
	chunk := NewTupleDataChunk()
	tuple._alloc.ReleaseOrStoreHandles(pinState, util.Back(tuple._segments), chunk, true)
}

func (tuple *TupleDataCollection) FinalizePinState2(pinState *TupleDataPinState, seg *TupleDataSegment) {
	util.AssertFunc(len(tuple._segments) == 1)
	chunk := NewTupleDataChunk()
	tuple._alloc.ReleaseOrStoreHandles(pinState, seg, chunk, true)
}

func (tuple *TupleDataCollection) Unpin() {
	for _, seg := range tuple._segments {
		seg.Unpin()
	}
}

func (tuple *TupleDataCollection) ChunkCount() int {
	cnt := 0
	for _, segment := range tuple._segments {
		cnt += segment.ChunkCount()
	}
	return cnt
}
