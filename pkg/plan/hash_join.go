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
	_keyTypes []LType

	//types of right children of join.
	_buildTypes []LType

	_ht *JoinHashTable

	//for hash join
	_buildExec *ExprExec

	_joinKeys *Chunk

	_buildChunk *Chunk

	_hjs      HashJoinStage
	_scan     *Scan
	_probExec *ExprExec
	//types of the output Chunk in Scan.Next
	_scanNextTyps []LType

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
		hj._keyTypes = append(hj._keyTypes, cond.Children[0].DataTyp.LTyp)
	}

	for i, output := range op.Children[0].Outputs {
		hj._scanNextTyps = append(hj._scanNextTyps, output.DataTyp.LTyp)
		hj._leftIndice = append(hj._leftIndice, i)
	}

	if op.JoinTyp != LOT_JoinTypeSEMI && op.JoinTyp != LOT_JoinTypeANTI {
		//right child output types
		rightIdxOffset := len(hj._scanNextTyps)
		for i, output := range op.Children[1].Outputs {
			hj._buildTypes = append(hj._buildTypes, output.DataTyp.LTyp)
			hj._scanNextTyps = append(hj._scanNextTyps, output.DataTyp.LTyp)
			hj._rightIndice = append(hj._rightIndice, rightIdxOffset+i)
		}
	}

	if op.JoinTyp == LOT_JoinTypeMARK || op.JoinTyp == LOT_JoinTypeAntiMARK {
		hj._scanNextTyps = append(hj._scanNextTyps, boolean())
		hj._markIndex = len(hj._scanNextTyps) - 1
	}
	//
	hj._buildChunk = &Chunk{}
	hj._buildChunk.init(hj._buildTypes, defaultVectorSize)

	//
	hj._buildExec = &ExprExec{}
	for _, cond := range hj._conds {
		//FIXME: Children[1] may not be from right part
		//FIX it in the build stage.
		hj._buildExec.addExpr(cond.Children[1])
	}

	hj._joinKeys = &Chunk{}
	hj._joinKeys.init(hj._keyTypes, defaultVectorSize)

	hj._ht = NewJoinHashTable(conds, hj._buildTypes, op.JoinTyp)

	hj._probExec = &ExprExec{}
	for _, cond := range hj._conds {
		hj._probExec.addExpr(cond.Children[0])
	}
	return hj
}

func (hj *HashJoin) Build(input *Chunk) error {
	var err error
	//evaluate the right part
	hj._joinKeys.reset()
	err = hj._buildExec.executeExprs([]*Chunk{nil, input, nil},
		hj._joinKeys)
	if err != nil {
		return err
	}

	//build th
	if len(hj._buildTypes) != 0 {
		hj._ht.Build(hj._joinKeys, input)
	} else {
		hj._buildChunk.setCard(input.card())
		hj._ht.Build(hj._joinKeys, hj._buildChunk)
	}
	return nil
}

type Scan struct {
	_keyData    []*UnifiedFormat
	_pointers   *Vector
	_count      int
	_selVec     *SelectVector
	_foundMatch []bool
	_ht         *JoinHashTable
	_finished   bool
	_leftChunk  *Chunk
}

func NewScan(ht *JoinHashTable) *Scan {
	return &Scan{
		_pointers: NewFlatVector(pointerType(), defaultVectorSize),
		_selVec:   NewSelectVector(defaultVectorSize),
		_ht:       ht,
	}
}

func (scan *Scan) initSelVec(curSel *SelectVector) {
	nonEmptyCnt := 0
	ptrs := getSliceInPhyFormatFlat[unsafe.Pointer](scan._pointers)
	cnt := scan._count
	for i := 0; i < cnt; i++ {
		idx := curSel.getIndex(i)
		if ptrs[idx] != nil {
			{
				scan._selVec.setIndex(nonEmptyCnt, idx)
				nonEmptyCnt++
			}
		}
	}
	scan._count = nonEmptyCnt

}

func (scan *Scan) Next(keys, left, result *Chunk) {
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

func (scan *Scan) NextLeftJoin(keys, left, result *Chunk) {
	scan.NextInnerJoin(keys, left, result)
	if result.card() == 0 {
		remainingCount := 0
		sel := NewSelectVector(defaultVectorSize)
		for i := 0; i < left.card(); i++ {
			if !scan._foundMatch[i] {
				sel.setIndex(remainingCount, i)
				remainingCount++
			}
		}
		if remainingCount > 0 {
			result.slice(left, sel, remainingCount, 0)
			for i := left.columnCount(); i < result.columnCount(); i++ {
				vec := result._data[i]
				vec.setPhyFormat(PF_CONST)
				setNullInPhyFormatConst(vec, true)
			}
		}
		scan._finished = true
	}
}

func (scan *Scan) NextAntiJoin(keys, left, result *Chunk) {
	scan.ScanKeyMatches(keys)
	scan.NextSemiOrAntiJoin(keys, left, result, false)
	scan._finished = true
}

func (scan *Scan) NextSemiJoin(keys, left, result *Chunk) {
	scan.ScanKeyMatches(keys)
	scan.NextSemiOrAntiJoin(keys, left, result, true)
	scan._finished = true
}

func (scan *Scan) NextSemiOrAntiJoin(keys, left, result *Chunk, Match bool) {
	assertFunc(left.columnCount() == result.columnCount())
	assertFunc(keys.card() == left.card())

	sel := NewSelectVector(defaultVectorSize)
	resultCount := 0
	for i := 0; i < keys.card(); i++ {
		if scan._foundMatch[i] == Match {
			sel.setIndex(resultCount, i)
			resultCount++
		}
	}

	if resultCount > 0 {
		result.slice(left, sel, resultCount, 0)
	} else {
		assertFunc(result.card() == 0)
	}
}

func (scan *Scan) NextMarkJoin(keys, left, result *Chunk) {
	//assertFunc(result.columnCount() ==
	//	left.columnCount()+1)
	assertFunc(back(result._data).typ().id == LTID_BOOLEAN)
	assertFunc(scan._ht.count() > 0)
	scan.ScanKeyMatches(keys)
	scan.constructMarkJoinResult(keys, left, result)
	scan._finished = true
}

func (scan *Scan) constructMarkJoinResult(keys, left, result *Chunk) {
	result.setCard(left.card())
	for i := 0; i < left.columnCount(); i++ {
		result._data[i].reference(left._data[i])
	}

	markVec := back(result._data)
	markVec.setPhyFormat(PF_FLAT)
	markSlice := getSliceInPhyFormatFlat[bool](markVec)
	markMask := getMaskInPhyFormatFlat(markVec)

	for colIdx := 0; colIdx < keys.columnCount(); colIdx++ {
		var vdata UnifiedFormat
		keys._data[colIdx].toUnifiedFormat(keys.card(), &vdata)
		if !vdata._mask.AllValid() {
			for i := 0; i < keys.card(); i++ {
				idx := vdata._sel.getIndex(i)
				markMask.set(
					uint64(i),
					vdata._mask.rowIsValidUnsafe(uint64(idx)))
			}
		}
	}

	if scan._foundMatch != nil {
		for i := 0; i < left.card(); i++ {
			markSlice[i] = scan._foundMatch[i]
		}
	} else {
		for i := 0; i < left.card(); i++ {
			markSlice[i] = false
		}
	}
}

func (scan *Scan) ScanKeyMatches(keys *Chunk) {
	matchSel := NewSelectVector(defaultVectorSize)
	noMatchSel := NewSelectVector(defaultVectorSize)
	for scan._count > 0 {
		matchCount := scan.resolvePredicates(keys, matchSel, noMatchSel)
		noMatchCount := scan._count - matchCount

		for i := 0; i < matchCount; i++ {
			scan._foundMatch[matchSel.getIndex(i)] = true
		}

		scan.advancePointers(noMatchSel, noMatchCount)
	}
}

func (scan *Scan) NextInnerJoin(keys, left, result *Chunk) {
	assertFunc(result.columnCount() ==
		left.columnCount()+len(scan._ht._buildTypes))
	if scan._count == 0 {
		return
	}
	resVec := NewSelectVector(defaultVectorSize)
	resCnt := scan.InnerJoin(keys, resVec)
	if resCnt > 0 {
		//left part result
		result.slice(left, resVec, resCnt, 0)
		//right part result
		for i := 0; i < len(scan._ht._buildTypes); i++ {
			vec := result._data[left.columnCount()+i]
			assertFunc(vec.typ() == scan._ht._buildTypes[i])
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
	result *Vector,
	resVec *SelectVector,
	selVec *SelectVector,
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
	result *Vector,
	selVec *SelectVector,
	cnt int,
	colIdx int,
) {
	resVec := incrSelectVectorInPhyFormatFlat()
	scan.gatherResult(result, resVec, selVec, cnt, colIdx)
}

func (scan *Scan) InnerJoin(keys *Chunk, resVec *SelectVector) int {
	for {
		resCnt := scan.resolvePredicates(
			keys,
			resVec,
			nil,
		)
		if len(scan._foundMatch) != 0 {
			for i := 0; i < resCnt; i++ {
				idx := resVec.getIndex(i)
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

func (scan *Scan) advancePointers(sel *SelectVector, cnt int) {
	newCnt := 0
	ptrs := getSliceInPhyFormatFlat[unsafe.Pointer](scan._pointers)

	for i := 0; i < cnt; i++ {
		idx := sel.getIndex(i)
		temp := pointerAdd(ptrs[idx], scan._ht._pointerOffset)
		ptrs[idx] = load[unsafe.Pointer](temp)
		if ptrs[idx] != nil {
			scan._selVec.setIndex(newCnt, idx)
			newCnt++
		}
	}

	scan._count = newCnt
}

func (scan *Scan) resolvePredicates(
	keys *Chunk,
	matchSel *SelectVector,
	noMatchSel *SelectVector,
) int {
	for i := 0; i < scan._count; i++ {
		matchSel.setIndex(i, scan._selVec.getIndex(i))
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
	_equalTypes []LType

	_keyTypes []LType

	_buildTypes []LType

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

	_hashMap []unsafe.Pointer
	_bitmask int
}

func NewJoinHashTable(conds []*Expr,
	buildTypes []LType,
	joinTyp LOT_JoinType) *JoinHashTable {
	ht := &JoinHashTable{
		_conds:      copyExprs(conds...),
		_buildTypes: copyLTypes(buildTypes...),
		_joinType:   joinTyp,
	}
	for _, cond := range conds {
		typ := cond.Children[0].DataTyp.LTyp
		if cond.SubTyp == ET_Equal || cond.SubTyp == ET_In {
			assertFunc(len(ht._equalTypes) == len(ht._keyTypes))
			ht._equalTypes = append(ht._equalTypes, typ)
		}
		ht._predTypes = append(ht._predTypes, cond.SubTyp)
		ht._keyTypes = append(ht._keyTypes, typ)
	}
	assertFunc(len(ht._equalTypes) != 0)
	layoutTypes := make([]LType, 0)
	layoutTypes = append(layoutTypes, ht._keyTypes...)
	layoutTypes = append(layoutTypes, ht._buildTypes...)
	layoutTypes = append(layoutTypes, hashType())
	// init layout
	ht._layout = NewTupleDataLayout(layoutTypes, nil, true, true)
	offsets := ht._layout.offsets()
	ht._tupleSize = offsets[len(ht._keyTypes)+len(ht._buildTypes)]

	//?
	ht._pointerOffset = offsets[len(offsets)-1]
	ht._entrySize = ht._layout.rowWidth()

	ht._dataCollection = NewTupleDataCollection(ht._layout, nil)
	return ht
}

func (jht *JoinHashTable) Build(keys *Chunk, payload *Chunk) {
	assertFunc(!jht._finalized)
	assertFunc(keys.card() == payload.card())
	if keys.card() == 0 {
		return
	}
	var keyData []*UnifiedFormat
	var curSel *SelectVector
	sel := NewSelectVector(defaultVectorSize)
	addedCnt := jht.prepareKeys(keys,
		&keyData,
		&curSel,
		sel,
		true,
	)
	if addedCnt < keys.card() {
		jht._hasNull = true
	}
	if addedCnt == 0 {
		return
	}

	//hash keys
	hashValues := NewVector(hashType(), defaultVectorSize)
	jht.hash(keys, curSel, addedCnt, hashValues)

	//append data collection
	sourceChunk := &Chunk{}
	sourceChunk.init(jht._layout.types(), defaultVectorSize)
	for i := 0; i < keys.columnCount(); i++ {
		sourceChunk._data[i].reference(keys._data[i])
	}
	colOffset := keys.columnCount()
	assertFunc(len(jht._buildTypes) == payload.columnCount())
	for i := 0; i < payload.columnCount(); i++ {
		sourceChunk._data[colOffset+i].reference(payload._data[i])
	}
	colOffset += payload.columnCount()
	sourceChunk._data[colOffset].reference(hashValues)
	sourceChunk.setCard(keys.card())
	if addedCnt < keys.card() {
		sourceChunk.sliceItself(curSel, addedCnt)
	}
	//save data collection
	//FIXME:
	jht._dataCollection.Append(
		nil,
		nil,
		sourceChunk,
		incrSelectVectorInPhyFormatFlat(),
		sourceChunk.card())
}

func (jht *JoinHashTable) hash(
	keys *Chunk,
	sel *SelectVector,
	count int,
	hashes *Vector,
) {
	HashTypeSwitch(keys._data[0], hashes, sel, count, sel != nil)
	//combine hash
	for i := 1; i < len(jht._equalTypes); i++ {
		CombineHashTypeSwitch(hashes, keys._data[i], sel, count, sel != nil)
	}
}

func (jht *JoinHashTable) prepareKeys(
	keys *Chunk,
	keyData *[]*UnifiedFormat,
	curSel **SelectVector,
	sel *SelectVector,
	buildSide bool) int {
	*keyData = keys.ToUnifiedFormat()

	//which keys are NULL
	*curSel = incrSelectVectorInPhyFormatFlat()

	addedCount := keys.card()
	for i := 0; i < keys.columnCount(); i++ {
		if (*keyData)[i]._mask.AllValid() {
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
	vdata *UnifiedFormat,
	sel *SelectVector,
	count int,
	result *SelectVector) int {

	res := 0
	for i := 0; i < count; i++ {
		idx := sel.getIndex(i)
		keyIdx := vdata._sel.getIndex(idx)
		if vdata._mask.rowIsValid(uint64(keyIdx)) {
			result.setIndex(res, idx)
			res++
		}
	}
	return res
}

func pointerTableCap(cnt int) int {
	return max(int(nextPowerOfTwo(uint64(cnt*2))), 1024)
}

func (jht *JoinHashTable) InitPointerTable() {
	pCap := pointerTableCap(jht._dataCollection.Count())
	assertFunc(isPowerOfTwo(uint64(pCap)))
	jht._hashMap = make([]unsafe.Pointer, pCap)
	jht._bitmask = pCap - 1
}

func (jht *JoinHashTable) Finalize() {
	jht.InitPointerTable()
	hashes := NewFlatVector(hashType(), defaultVectorSize)
	hashSlice := getSliceInPhyFormatFlat[uint64](hashes)
	dedup := make(map[unsafe.Pointer]struct{})
	for _, part := range jht._dataCollection._parts {
		rowLocs := getSliceInPhyFormatFlat[unsafe.Pointer](part.rowLocations)
		for _, loc := range rowLocs {
			if loc != nil {
				if _, has := dedup[loc]; has {
					panic("has dup row loc")
				}
				dedup[loc] = struct{}{}
			}
		}
		//reset hashes
		for i := 0; i < defaultVectorSize; i++ {
			hashSlice[i] = uint64(0)
		}
		for j := 0; j < part._count; j++ {
			hashSlice[j] = load[uint64](pointerAdd(rowLocs[j], jht._pointerOffset))

		}
		jht.InsertHashes(hashes, part._count, rowLocs)
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

func (jht *JoinHashTable) InsertHashes(hashes *Vector, cnt int, keyLocs []unsafe.Pointer) {
	jht.ApplyBitmask(hashes, cnt)
	hashes.flatten(cnt)
	assertFunc(hashes.phyFormat().isFlat())
	pointers := jht._hashMap
	indices := getSliceInPhyFormatFlat[uint64](hashes)
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

		store[unsafe.Pointer](pointers[idx], pointerAdd(keyLocs[i], pointerOffset))
		//pointer to current tuple
		pointers[idx] = keyLocs[i]
		base := keyLocs[i]
		cur := load[unsafe.Pointer](pointerAdd(keyLocs[i], pointerOffset))
		if base == cur {
			panic("insert loop in bucket")
		}
	}
}

func (jht *JoinHashTable) ApplyBitmask(hashes *Vector, cnt int) {
	if hashes.phyFormat().isConst() {
		indices := getSliceInPhyFormatConst[uint64](hashes)
		indices[0] &= uint64(jht._bitmask)
	} else {
		hashes.flatten(cnt)
		indices := getSliceInPhyFormatFlat[uint64](hashes)
		for i := 0; i < cnt; i++ {
			indices[i] &= uint64(jht._bitmask)
		}
	}
}

func (jht *JoinHashTable) ApplyBitmask2(
	hashes *Vector,
	sel *SelectVector,
	cnt int,
	pointers *Vector,
) {
	var data UnifiedFormat
	hashes.toUnifiedFormat(cnt, &data)
	hashSlice := getSliceInPhyFormatUnifiedFormat[uint64](&data)
	resSlice := getSliceInPhyFormatFlat[unsafe.Pointer](pointers)
	mainHt := jht._hashMap
	for i := 0; i < cnt; i++ {
		rIdx := sel.getIndex(i)
		hIdx := data._sel.getIndex(rIdx)
		hVal := hashSlice[hIdx]
		bucket := mainHt[(hVal & uint64(jht._bitmask))]
		resSlice[rIdx] = bucket
	}

}

func (jht *JoinHashTable) Probe(keys *Chunk) *Scan {
	var curSel *SelectVector
	newScan := jht.initScan(keys, &curSel)
	if newScan._count == 0 {
		return newScan
	}
	hashes := NewFlatVector(hashType(), defaultVectorSize)
	jht.hash(keys, curSel, newScan._count, hashes)

	jht.ApplyBitmask2(hashes, curSel, newScan._count, newScan._pointers)
	newScan.initSelVec(curSel)
	return newScan
}

func (jht *JoinHashTable) initScan(keys *Chunk, curSel **SelectVector) *Scan {
	assertFunc(jht.count() > 0)
	assertFunc(jht._finalized)
	newScan := NewScan(jht)
	if jht._joinType != LOT_JoinTypeInner {
		newScan._foundMatch = make([]bool, defaultVectorSize)
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
	_types []LType

	//width of bitmap header
	_bitmapWidth int

	//width of data part
	_dataWidth int

	//width of agg state part
	_aggWidth int

	//width of entire row
	_rowWidth int

	//offsets to the columns and agg data in each row
	_offsets []int

	//all columns in this layout are constant size
	_allConst bool

	//offset to the heap size of each row
	_heapSizeOffset int

	_aggregates []*AggrObject
}

func NewTupleDataLayout(types []LType, aggrObjs []*AggrObject, align bool, needHeapOffset bool) *TupleDataLayout {
	layout := &TupleDataLayout{
		_types: copyLTypes(types...),
	}

	alignWidth := func() {
		if align {
			layout._rowWidth = alignValue(layout._rowWidth)
		}
	}

	layout._bitmapWidth = entryCount(len(layout._types))
	layout._rowWidth = layout._bitmapWidth
	alignWidth()

	//all columns are constant size
	for _, lType := range layout._types {
		layout._allConst = layout._allConst &&
			lType.getInternalType().isConstant()
	}

	if needHeapOffset && !layout._allConst {
		layout._heapSizeOffset = layout._rowWidth
		layout._rowWidth += int64Size
		alignWidth()
	}

	//data columns
	for _, lType := range layout._types {
		layout._offsets = append(layout._offsets, layout._rowWidth)
		if lType.getInternalType().isConstant() ||
			lType.getInternalType().isVarchar() {
			layout._rowWidth += lType.getInternalType().size()
		} else {
			//for variable length types, pointer to the actual data
			layout._rowWidth += int64Size
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

func (layout *TupleDataLayout) types() []LType {
	return copyLTypes(layout._types...)
}

// total width of each row
func (layout *TupleDataLayout) rowWidth() int {
	return layout._rowWidth
}

// start of the data in each row
func (layout *TupleDataLayout) dataOffset() int {
	return layout._bitmapWidth
}

// total width of the data
func (layout *TupleDataLayout) dataWidth() int {
	return layout._dataWidth
}

// start of agg
func (layout *TupleDataLayout) aggrOffset() int {
	return layout._bitmapWidth + layout._dataWidth
}

func (layout *TupleDataLayout) offsets() []int {
	return copyTo[int](layout._offsets)
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
	res._types = copyLTypes(layout._types...)
	res._bitmapWidth = layout._bitmapWidth
	res._dataWidth = layout._dataWidth
	res._aggWidth = layout._aggWidth
	res._rowWidth = layout._rowWidth
	res._offsets = copyTo[int](layout._offsets)
	res._allConst = layout._allConst
	res._heapSizeOffset = layout._heapSizeOffset
	return res
}

type TupleDataCollection struct {
	_layout         *TupleDataLayout
	_rawInputLayout *TupleDataLayout
	_count          uint64
	_parts          []*TupleRows
	_dedup          map[unsafe.Pointer]struct{}
	_alloc          *TupleDataAllocator
	_segments       []*TupleDataSegment
}

type TupleRows struct {
	rowLocations    *Vector
	_count          int
	rawInputRowLocs *Vector
}

type TuplePart struct {
	data          []UnifiedFormat
	rowLocations  *Vector
	heapLocations *Vector
	heapSizes     *Vector

	rawInputData      []UnifiedFormat
	rawInputRowLocs   *Vector
	rawInputHeapLocs  *Vector
	rawInputHeapSizes *Vector
	_count            int
}

func NewTuplePart(cnt int) *TuplePart {
	ret := &TuplePart{
		data:          make([]UnifiedFormat, cnt),
		rowLocations:  NewVector(pointerType(), defaultVectorSize),
		heapLocations: NewVector(pointerType(), defaultVectorSize),
		heapSizes:     NewVector(ubigintType(), defaultVectorSize),
	}
	return ret
}

func (part *TuplePart) prepareForRawInput(cnt int) {
	part.rawInputData = make([]UnifiedFormat, cnt)
	part.rawInputRowLocs = NewVector(pointerType(), defaultVectorSize)
	part.rawInputHeapLocs = NewVector(pointerType(), defaultVectorSize)
	part.rawInputHeapSizes = NewVector(ubigintType(), defaultVectorSize)
}

func (part *TuplePart) toTupleRows() *TupleRows {
	ret := &TupleRows{
		rowLocations: NewVector(pointerType(), defaultVectorSize),
		_count:       part._count,
	}

	dst := ret.rowLocations.getData()
	copy(dst, part.rowLocations.getData())

	if part.rawInputRowLocs != nil {
		ret.rawInputRowLocs = NewVector(pointerType(), defaultVectorSize)
		//raw input
		copy(ret.rawInputRowLocs.getData(), part.rawInputRowLocs.getData())
	}

	return ret
}

func NewTupleDataCollection(layout *TupleDataLayout, rawInputLayout *TupleDataLayout) *TupleDataCollection {
	ret := &TupleDataCollection{
		_layout:         layout.copy(),
		_rawInputLayout: rawInputLayout.copy(),
		_dedup:          make(map[unsafe.Pointer]struct{}),
	}
	return ret
}

func (tuple *TupleDataCollection) Count() int {
	return int(tuple._count)
}

func (tuple *TupleDataCollection) Append(
	pinState *TupleDataPinState,
	chunkState *TupleDataChunkState,
	chunk *Chunk,
	appendSel *SelectVector,
	appendCount int,
) {
	//to unified format
	toUnifiedFormat(chunkState, chunk)
	tuple.AppendUnified(pinState, chunkState, chunk, nil, appendSel, appendCount)
}

func (tuple *TupleDataCollection) AppendUnified(
	pinState *TupleDataPinState,
	chunkState *TupleDataChunkState,
	chunk *Chunk,
	rawInput *Chunk,
	appendSel *SelectVector,
	cnt int,
) {
	if cnt == -1 {
		cnt = chunk.card()
	}
	if cnt == 0 {
		return
	}
	//evaluate the heap size
	if !tuple._layout.allConst() {
		tuple.computeHeapSizes(chunkState, chunk, appendSel, cnt)
	}

	//allocate buffer space for incoming Chunk in chunkstate
	tuple.Build(pinState, chunkState, 0, uint64(cnt))

	//fill row
	tuple.scatter(chunkState, chunk, appendSel, cnt)

	if rawInput != nil {
		//evaluate the heap size of raw input
		if !tuple._rawInputLayout.allConst() {
			tuple.computeHeapSizesForRawInput(chunkState, rawInput, appendSel, cnt)
		}

		//allocate space for every row for raw input
		tuple.buildBufferSpaceForRawInput(chunkState, cnt)

		//scatter rows
		tuple.scatterRawInput(chunkState, rawInput, appendSel, cnt)
	}

	//chunkState._count = cnt
	//tuple.savePart(part)
}

func (tuple *TupleDataCollection) savePart(part *TuplePart) {
	rows := part.toTupleRows()
	tuple._count += uint64(rows._count)
	tuple._parts = append(tuple._parts, rows)
	rowLocs := getSliceInPhyFormatFlat[unsafe.Pointer](rows.rowLocations)
	for i, loc := range rowLocs {
		if i >= rows._count {
			break
		}
		if loc == nil {
			continue
		}

		if _, has := tuple._dedup[loc]; has {
			panic("duplicate row location")
		}
		tuple._dedup[loc] = struct{}{}
	}
	tuple.checkDupAll()
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
	chunk *Chunk,
	appendSel *SelectVector,
	cnt int) {
	rowLocations := getSliceInPhyFormatFlat[unsafe.Pointer](state._rowLocations)
	//set bitmap
	maskBytes := sizeInBytes(tuple._layout.columnCount())
	for i := 0; i < cnt; i++ {
		memset(rowLocations[i], 0xFF, maskBytes)
	}

	if !tuple._layout.allConst() {
		heapSizeOffset := tuple._layout.heapSizeOffset()
		heapSizes := getSliceInPhyFormatFlat[uint64](state._heapSizes)
		for i := 0; i < cnt; i++ {
			store[uint64](heapSizes[i], pointerAdd(rowLocations[i], heapSizeOffset))
		}
	}
	for _, i := range state._columnIds {
		tuple.scatterVector(state, chunk._data[i], int(i), appendSel, cnt)
	}
}

func (tuple *TupleDataCollection) scatterRawInput(
	state *TupleDataChunkState,
	chunk *Chunk,
	appendSel *SelectVector,
	cnt int) {
	rowLocations := getSliceInPhyFormatFlat[unsafe.Pointer](state.rawInputRowLocs)
	//set bitmap
	maskBytes := sizeInBytes(tuple._rawInputLayout.columnCount())
	for i := 0; i < cnt; i++ {
		memset(rowLocations[i], 0xFF, maskBytes)
	}

	if !tuple._rawInputLayout.allConst() {
		heapSizeOffset := tuple._rawInputLayout.heapSizeOffset()
		heapSizes := getSliceInPhyFormatFlat[uint64](state.rawInputHeapSizes)
		for i := 0; i < cnt; i++ {
			store[uint64](heapSizes[i], pointerAdd(rowLocations[i], heapSizeOffset))
		}
	}
	for i := 0; i < tuple._rawInputLayout.columnCount(); i++ {
		tuple.scatterVectorForRawInput(state, chunk._data[i], i, appendSel, cnt)
	}
}

func (tuple *TupleDataCollection) scatterVector(
	state *TupleDataChunkState,
	src *Vector,
	colIdx int,
	appendSel *SelectVector,
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

func (tuple *TupleDataCollection) scatterVectorForRawInput(
	state *TupleDataChunkState,
	src *Vector,
	colIdx int,
	appendSel *SelectVector,
	cnt int) {
	TupleDataTemplatedScatterSwitch(
		src,
		&state.rawInputData[colIdx],
		appendSel,
		cnt,
		tuple._rawInputLayout,
		state.rawInputRowLocs,
		state.rawInputHeapLocs,
		colIdx)
}

// convert chunk into state.data unified format
func toUnifiedFormat(state *TupleDataChunkState, chunk *Chunk) {
	for _, colId := range state._columnIds {
		chunk._data[colId].toUnifiedFormat(chunk.card(), &state._data[colId])
	}
}

func toUnifiedFormatForRawInput(state *TupleDataChunkState, chunk *Chunk) {
	if chunk == nil {
		return
	}
	for i, vec := range chunk._data {
		vec.toUnifiedFormat(chunk.card(), &state.rawInputData[i])
	}
}

// result refers to chunk state.data unified format
func getVectorData(state *TupleDataChunkState, result []*UnifiedFormat) {
	vectorData := state._data
	for i := 0; i < len(vectorData); i++ {
		target := result[i]
		target._sel = state._data[i]._sel
		target._data = state._data[i]._data
		target._mask = state._data[i]._mask
		target._pTypSize = state._data[i]._pTypSize
	}
}

func (tuple *TupleDataCollection) buildBufferSpace(part *TuplePart, cnt int) {
	rowLocs := getSliceInPhyFormatFlat[unsafe.Pointer](part.rowLocations)
	heapSizes := getSliceInPhyFormatFlat[uint64](part.heapSizes)
	heapLocs := getSliceInPhyFormatFlat[unsafe.Pointer](part.heapLocations)

	for i := 0; i < cnt; i++ {
		rowLocs[i] = cMalloc(tuple._layout.rowWidth())
		if rowLocs[i] == nil {
			panic("row loc is null")
		}
		if _, has := tuple._dedup[rowLocs[i]]; has {
			panic("duplicate row location 2")
		}

		if tuple._layout.allConst() {
			continue
		}
		//FIXME: do not init heapSizes here
		//initHeapSizes(rowLocs, heapSizes, i, cnt, tuple._layout.heapSizeOffset())
		if heapSizes[i] == 0 {
			continue
		}
		heapLocs[i] = cMalloc(int(heapSizes[i]))
		if heapLocs[i] == nil {
			panic("heap loc is null")
		}
	}
}

func (tuple *TupleDataCollection) buildBufferSpaceForRawInput(state *TupleDataChunkState, cnt int) {
	rowLocs := getSliceInPhyFormatFlat[unsafe.Pointer](state.rawInputRowLocs)
	heapSizes := getSliceInPhyFormatFlat[uint64](state.rawInputHeapSizes)
	heapLocs := getSliceInPhyFormatFlat[unsafe.Pointer](state.rawInputHeapLocs)

	for i := 0; i < cnt; i++ {
		rowLocs[i] = cMalloc(tuple._rawInputLayout.rowWidth())
		if rowLocs[i] == nil {
			panic("row loc is null")
		}
		if _, has := tuple._dedup[rowLocs[i]]; has {
			panic("duplicate row location 2")
		}

		if tuple._rawInputLayout.allConst() {
			continue
		}
		//FIXME: do not init heapSizes here
		//initHeapSizes(rowLocs, heapSizes, i, cnt, tuple._rawInputLayout.heapSizeOffset())
		if heapSizes[i] == 0 {
			continue
		}
		heapLocs[i] = cMalloc(int(heapSizes[i]))
		if heapLocs[i] == nil {
			panic("heap loc is null")
		}
	}
}

// evaluate the heap size of columns
func (tuple *TupleDataCollection) computeHeapSizes(
	state *TupleDataChunkState,
	chunk *Chunk,
	appendSel *SelectVector,
	cnt int) {

	//heapSizes records the heap size of every row
	heapSizesSlice := getSliceInPhyFormatFlat[uint64](state._heapSizes)
	fill[uint64](heapSizesSlice, size(heapSizesSlice), 0)

	for i := 0; i < chunk.columnCount(); i++ {
		tuple.evaluateHeapSizes(state._heapSizes, chunk._data[i], &state._data[i], appendSel, cnt)
	}
}

func (tuple *TupleDataCollection) computeHeapSizesForRawInput(state *TupleDataChunkState, chunk *Chunk, appendSel *SelectVector, cnt int) {
	heapSizesSlice := getSliceInPhyFormatFlat[uint64](state.rawInputHeapSizes)
	fill[uint64](heapSizesSlice, size(heapSizesSlice), 0)
	for i := 0; i < chunk.columnCount(); i++ {
		tuple.evaluateHeapSizes(state.rawInputHeapSizes, chunk._data[i], &state.rawInputData[i], appendSel, cnt)
	}
}

func (tuple *TupleDataCollection) evaluateHeapSizes(
	sizes *Vector,
	src *Vector,
	srcUni *UnifiedFormat,
	appendSel *SelectVector,
	cnt int) {
	//only care varchar type
	pTyp := src.typ().getInternalType()
	switch pTyp {
	case VARCHAR:
	default:
		return
	}
	heapSizeSlice := getSliceInPhyFormatFlat[uint64](sizes)
	srcSel := srcUni._sel
	srcMask := srcUni._mask

	switch pTyp {
	case VARCHAR:
		srcSlice := getSliceInPhyFormatUnifiedFormat[String](srcUni)
		for i := 0; i < cnt; i++ {
			srcIdx := srcSel.getIndex(appendSel.getIndex(i))
			if srcMask.rowIsValid(uint64(srcIdx)) {
				heapSizeSlice[i] += uint64(srcSlice[srcIdx].len())
			} else {
				heapSizeSlice[i] += uint64(String{}.nullLen())
			}
		}

	default:
		panic("usp phy type")
	}
}

func (tuple *TupleDataCollection) gather(
	layout *TupleDataLayout,
	rowLocs *Vector,
	scanSel *SelectVector,
	scanCnt int,
	colId int,
	result *Vector,
	targetSel *SelectVector) {
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
	rowLocs *Vector,
	scanSel *SelectVector,
	scanCnt int,
	colIds []int,
	result *Chunk,
	targetSel *SelectVector,
) {
	for i := 0; i < len(colIds); i++ {
		tuple.gather(
			layout,
			rowLocs,
			scanSel,
			scanCnt,
			colIds[i],
			result._data[i],
			targetSel)
	}
}

func TupleDataTemplatedScatterSwitch(
	src *Vector,
	srcFormat *UnifiedFormat,
	appendSel *SelectVector,
	cnt int,
	layout *TupleDataLayout,
	rowLocations *Vector,
	heapLocations *Vector,
	colIdx int) {
	pTyp := src.typ().getInternalType()
	switch pTyp {
	case INT32:
		TupleDataTemplatedScatter[int32](
			srcFormat,
			appendSel,
			cnt,
			layout,
			rowLocations,
			heapLocations,
			colIdx,
			int32ScatterOp{},
		)
	case UINT64:
		TupleDataTemplatedScatter[uint64](
			srcFormat,
			appendSel,
			cnt,
			layout,
			rowLocations,
			heapLocations,
			colIdx,
			uint64ScatterOp{},
		)
	case VARCHAR:
		TupleDataTemplatedScatter[String](
			srcFormat,
			appendSel,
			cnt,
			layout,
			rowLocations,
			heapLocations,
			colIdx,
			stringScatterOp{},
		)
	case INT8:
		TupleDataTemplatedScatter[int8](
			srcFormat,
			appendSel,
			cnt,
			layout,
			rowLocations,
			heapLocations,
			colIdx,
			int8ScatterOp{},
		)
	case DECIMAL:
		TupleDataTemplatedScatter[Decimal](
			srcFormat,
			appendSel,
			cnt,
			layout,
			rowLocations,
			heapLocations,
			colIdx,
			decimalScatterOp{},
		)
	case DATE:
		TupleDataTemplatedScatter[Date](
			srcFormat,
			appendSel,
			cnt,
			layout,
			rowLocations,
			heapLocations,
			colIdx,
			dateScatterOp{},
		)
	//if layout.offsets()[colIdx] == 33 {
	//	rowPtrs := getSliceInPhyFormatFlat[unsafe.Pointer](rowLocations)
	//	dt := load[Date](pointerAdd(rowPtrs[0], 33))

	//}
	case DOUBLE:
		TupleDataTemplatedScatter[float64](
			srcFormat,
			appendSel,
			cnt,
			layout,
			rowLocations,
			heapLocations,
			colIdx,
			float64ScatterOp{},
		)
	default:
		panic("usp ")
	}
}

func TupleDataTemplatedScatter[T any](
	srcFormat *UnifiedFormat,
	appendSel *SelectVector,
	cnt int,
	layout *TupleDataLayout,
	rowLocations *Vector,
	heapLocations *Vector,
	colIdx int,
	nVal ScatterOp[T],
) {
	srcSel := srcFormat._sel
	srcSlice := getSliceInPhyFormatUnifiedFormat[T](srcFormat)
	srcMask := srcFormat._mask

	targetLocs := getSliceInPhyFormatFlat[unsafe.Pointer](rowLocations)
	targetHeapLocs := getSliceInPhyFormatFlat[unsafe.Pointer](heapLocations)
	offsetInRow := layout.offsets()[colIdx]
	if srcMask.AllValid() {
		for i := 0; i < cnt; i++ {
			srcIdx := srcSel.getIndex(appendSel.getIndex(i))
			TupleDataValueStore[T](srcSlice[srcIdx], targetLocs[i], offsetInRow, &targetHeapLocs[i], nVal)
		}
	} else {
		for i := 0; i < cnt; i++ {
			srcIdx := srcSel.getIndex(appendSel.getIndex(i))
			if srcMask.rowIsValid(uint64(srcIdx)) {
				TupleDataValueStore[T](srcSlice[srcIdx], targetLocs[i], offsetInRow, &targetHeapLocs[i], nVal)
			} else {
				TupleDataValueStore[T](nVal.nullValue(), targetLocs[i], offsetInRow, &targetHeapLocs[i], nVal)
				bSlice := pointerToSlice[uint8](targetLocs[i], layout._rowWidth)
				tempMask := Bitmap{_bits: bSlice}
				tempMask.setInvalidUnsafe(uint64(colIdx))
			}
		}
	}

}

func TupleDataValueStore[T any](src T, rowLoc unsafe.Pointer, offsetInRow int, heapLoc *unsafe.Pointer, nVal ScatterOp[T]) {
	nVal.store(src, rowLoc, offsetInRow, heapLoc)
}

func TupleDataTemplatedGatherSwitch(
	layout *TupleDataLayout,
	rowLocs *Vector,
	colIdx int,
	scanSel *SelectVector,
	scanCnt int,
	target *Vector,
	targetSel *SelectVector,
) {
	pTyp := target.typ().getInternalType()
	switch pTyp {
	case INT32:
		TupleDataTemplatedGather[int32](
			layout,
			rowLocs,
			colIdx,
			scanSel,
			scanCnt,
			target,
			targetSel,
		)
	case VARCHAR:
		TupleDataTemplatedGather[String](
			layout,
			rowLocs,
			colIdx,
			scanSel,
			scanCnt,
			target,
			targetSel,
		)
	case DECIMAL:
		TupleDataTemplatedGather[Decimal](
			layout,
			rowLocs,
			colIdx,
			scanSel,
			scanCnt,
			target,
			targetSel,
		)
	case DATE:
		TupleDataTemplatedGather[Date](
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
	rowLocs *Vector,
	colIdx int,
	scanSel *SelectVector,
	scanCnt int,
	target *Vector,
	targetSel *SelectVector,
) {
	srcLocs := getSliceInPhyFormatFlat[unsafe.Pointer](rowLocs)
	targetData := getSliceInPhyFormatFlat[T](target)
	targetBitmap := getMaskInPhyFormatFlat(target)
	entryIdx, idxInEntry := getEntryIndex(uint64(colIdx))
	offsetInRow := layout.offsets()[colIdx]
	for i := 0; i < scanCnt; i++ {
		base := srcLocs[scanSel.getIndex(i)]
		srcRow := pointerToSlice[byte](base, layout._rowWidth)
		targetIdx := targetSel.getIndex(i)
		rowMask := Bitmap{_bits: srcRow}
		if rowIsValidInEntry(
			rowMask.getEntry(entryIdx),
			idxInEntry) {
			targetData[targetIdx] = load[T](pointerAdd(base, offsetInRow))

		} else {
			targetBitmap.setInvalid(uint64(targetIdx))
		}
	}
}

func (tuple *TupleDataCollection) InitScan(state *AggrHashTableScanState) {
	state._partIdx = 0
	state._partCnt = len(tuple._parts)
	state._init = true
}

func (tuple *TupleDataCollection) Scan(state *AggrHashTableScanState, result, rawInput *Chunk) bool {
	if state._partIdx >= len(tuple._parts) {
		return false
	}
	part := tuple._parts[state._partIdx]
	state._partIdx++

	//temp
	state._rowLocs = NewVector(pointerType(), defaultVectorSize)
	copy(state._rowLocs.getData(), part.rowLocations.getData())
	state._rawInputRowLocs = NewVector(pointerType(), defaultVectorSize)
	copy(state._rawInputRowLocs.getData(), part.rawInputRowLocs.getData())

	tuple.Gather(
		tuple._layout,
		state._rowLocs,
		incrSelectVectorInPhyFormatFlat(),
		part._count,
		state._colIds,
		result,
		incrSelectVectorInPhyFormatFlat(),
	)
	result.setCard(part._count)

	//raw input

	tuple.Gather(
		tuple._rawInputLayout,
		state._rawInputRowLocs,
		incrSelectVectorInPhyFormatFlat(),
		part._count,
		state._rawInputColIds,
		rawInput,
		incrSelectVectorInPhyFormatFlat(),
	)
	rawInput.setCard(part._count)
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
	seg := back(tuple._segments)
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
	assertFunc(len(tuple._segments) == 1)
	chunk := NewTupleDataChunk()
	tuple._alloc.ReleaseOrStoreHandles(pinState, back(tuple._segments), chunk, true)
}

func (tuple *TupleDataCollection) Unpin() {
	for _, seg := range tuple._segments {
		seg.Unpin()
	}
}
