package main

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

	//right child output types
	rightIdxOffset := len(hj._scanNextTyps)
	for i, output := range op.Children[1].Outputs {
		hj._buildTypes = append(hj._buildTypes, output.DataTyp.LTyp)
		hj._scanNextTyps = append(hj._scanNextTyps, output.DataTyp.LTyp)
		hj._rightIndice = append(hj._rightIndice, rightIdxOffset+i)
	}

	//
	hj._buildChunk = &Chunk{}
	hj._buildChunk.init(hj._buildTypes, defaultVectorSize)

	//
	hj._buildExec = &ExprExec{}
	for _, cond := range hj._conds {
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
	ptrs := getSliceInPhyFormatFlat[*byte](scan._pointers)
	cnt := scan._count
	for i := 0; i < cnt; i++ {
		idx := curSel.getIndex(i)
		if ptrs[idx] != nil {
			t := load[*byte](ptrs[idx])
			if t != nil {
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
	default:
		panic("Unknown join type")
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
	ptrs := getSliceInPhyFormatFlat[*byte](scan._pointers)
	for i := 0; i < cnt; i++ {
		idx := sel.getIndex(i)
		temp := pointerAdd(ptrs[idx], scan._ht._pointerOffset)
		ptrs[idx] = load[*byte](temp)
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

	_hashMap [][]byte
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
		if cond.SubTyp == ET_Equal {
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
	ht._layout = NewTupleDataLayout(layoutTypes, false, true)
	offsets := ht._layout.offsets()
	ht._tupleSize = offsets[len(ht._keyTypes)+len(ht._buildTypes)]

	//?
	ht._pointerOffset = offsets[len(offsets)-1]
	ht._entrySize = ht._layout.rowWidth()

	ht._dataCollection = NewTupleDataCollection(ht._layout)
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
	jht._dataCollection.Append(sourceChunk)
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
	jht._hashMap = make([][]byte, pCap)
	jht._bitmask = pCap - 1
}

func (jht *JoinHashTable) Finalize() {
	jht.InitPointerTable()
	hashes := NewFlatVector(hashType(), defaultVectorSize)
	hashSlice := getSliceInPhyFormatFlat[uint64](hashes)
	for i, part := range jht._dataCollection._parts {
		rowLocs := getSliceInPhyFormatFlat[[]byte](part.rowLocations)
		for j := 0; j < part._count; j++ {
			hashSlice[i] = load[uint64](&rowLocs[j][jht._pointerOffset])
			jht.InsertHashes(hashes, part._count, rowLocs)
		}
	}
	jht._finalized = true
}

func (jht *JoinHashTable) InsertHashes(hashes *Vector, cnt int, keyLocs [][]byte) {
	jht.ApplyBitmask(hashes, cnt)
	hashes.flatten(cnt)
	assertFunc(hashes.phyFormat().isFlat())
	pointers := jht._hashMap
	indices := getSliceInPhyFormatFlat[uint64](hashes)
	InsertHashesLoop(pointers, indices, cnt, keyLocs, jht._pointerOffset)
}

func InsertHashesLoop(
	pointers [][]byte,
	indices []uint64,
	cnt int,
	keyLocs [][]byte,
	pointerOffset int,
) {
	for i := 0; i < cnt; i++ {
		idx := indices[i]
		//save prev into the pointer in tuple
		store[[]byte](pointers[idx], &keyLocs[i][pointerOffset])
		//pointer to current tuple
		pointers[idx] = keyLocs[i]
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
	resSlice := getSliceInPhyFormatFlat[*byte](pointers)
	mainHt := jht._hashMap
	for i := 0; i < cnt; i++ {
		rIdx := sel.getIndex(i)
		hIdx := data._sel.getIndex(rIdx)
		hVal := hashSlice[hIdx]
		bucket := mainHt[(hVal & uint64(jht._bitmask))]
		if bucket != nil {
			resSlice[rIdx] = &bucket[0]
		}
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
}

func NewTupleDataLayout(types []LType, align bool, needHeapOffset bool) *TupleDataLayout {
	layout := &TupleDataLayout{
		_types: copyLTypes(types...),
	}

	layout._bitmapWidth = entryCount(len(layout._types))
	layout._rowWidth = layout._bitmapWidth

	//all columns are constant size
	for _, lType := range layout._types {
		layout._allConst = layout._allConst &&
			lType.getInternalType().isConstant()
	}

	if needHeapOffset && !layout._allConst {
		layout._heapSizeOffset = layout._rowWidth
		layout._rowWidth += int32Size
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
	}

	layout._dataWidth = layout._rowWidth - layout._bitmapWidth

	//TODO: allocate aggr states

	layout._aggWidth = layout._rowWidth - layout._dataWidth - layout._bitmapWidth

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
	_layout *TupleDataLayout
	_count  int
	_parts  []*TuplePart
}

type TuplePart struct {
	data          []UnifiedFormat
	rowLocations  *Vector
	heapLocations *Vector
	heapSizes     *Vector
	_count        int
}

func NewTupleDataCollection(layout *TupleDataLayout) *TupleDataCollection {
	ret := &TupleDataCollection{
		_layout: layout.copy(),
	}
	return ret
}

func (tuple *TupleDataCollection) Count() int {
	return tuple._count
}

func (tuple *TupleDataCollection) Append(chunk *Chunk) {
	//to unified format
	part := &TuplePart{
		data:          make([]UnifiedFormat, chunk.columnCount()),
		rowLocations:  NewVector(pointerType(), defaultVectorSize),
		heapLocations: NewVector(pointerType(), defaultVectorSize),
		heapSizes:     NewVector(ubigintType(), defaultVectorSize),
	}
	tuple.toUnifiedFormat(part, chunk)

	//evaluate the heap size
	if !tuple._layout.allConst() {
		tuple.computeHeapSizes(part, chunk, chunk.card())
	}

	//allocate space for every row
	tuple.buildBufferSpace(part, chunk.card())

	//fill row
	tuple.scatter(part, chunk, chunk.card())

	part._count = chunk.card()
	tuple._count += part._count
	tuple._parts = append(tuple._parts, part)
}

func (tuple *TupleDataCollection) scatter(part *TuplePart, chunk *Chunk, cnt int) {
	rowLocations := getSliceInPhyFormatFlat[[]byte](part.rowLocations)
	//set bitmap
	maskBytes := sizeInBytes(tuple._layout.columnCount())
	for i := 0; i < cnt; i++ {
		memsetBytes(&rowLocations[i][0], 0xFF, maskBytes)
	}

	if !tuple._layout.allConst() {
		heapSizeOffset := tuple._layout.heapSizeOffset()
		heapSizes := getSliceInPhyFormatFlat[uint64](part.heapSizes)
		for i := 0; i < cnt; i++ {
			store[uint64](heapSizes[i], &rowLocations[i][heapSizeOffset])
		}
	}
	for i := 0; i < tuple._layout.columnCount(); i++ {
		tuple.scatterVector(part, chunk._data[i], i, cnt)
	}
}

func (tuple *TupleDataCollection) scatterVector(
	part *TuplePart,
	src *Vector,
	colIdx int,
	cnt int) {
	TupleDataTemplatedScatterSwitch(
		src,
		&part.data[colIdx],
		cnt,
		tuple._layout,
		part.rowLocations,
		part.heapLocations,
		colIdx)
}

func (tuple *TupleDataCollection) toUnifiedFormat(part *TuplePart, chunk *Chunk) {
	for i, vec := range chunk._data {
		vec.toUnifiedFormat(chunk.card(), &part.data[i])
	}
}

func (tuple *TupleDataCollection) buildBufferSpace(part *TuplePart, cnt int) {
	rowLocs := getSliceInPhyFormatFlat[[]byte](part.rowLocations)
	heapSizes := getSliceInPhyFormatFlat[uint64](part.heapSizes)
	heapLocs := getSliceInPhyFormatFlat[[]byte](part.heapLocations)

	for i := 0; i < cnt; i++ {
		rowLocs[i] = make([]byte, tuple._layout.rowWidth())
		if tuple._layout.allConst() {
			continue
		}
		initHeapSizes(rowLocs, heapSizes, cnt, tuple._layout.heapSizeOffset())
		heapLocs[i] = make([]byte, heapSizes[i])
	}
}

func initHeapSizes(rowLocs [][]byte, heapSizes []uint64, cnt int, heapSizeOffset int) {
	for i := 0; i < cnt; i++ {
		heapSizes[i] = load[uint64](&rowLocs[i][heapSizeOffset])
	}
}

func (tuple *TupleDataCollection) computeHeapSizes(part *TuplePart, chunk *Chunk, cnt int) {

	for i := 0; i < chunk.columnCount(); i++ {
		tuple.evaluateHeapSizes(part.heapSizes, chunk._data[i], &part.data[i], cnt)
	}
}

func (tuple *TupleDataCollection) evaluateHeapSizes(sizes *Vector, src *Vector, srcUni *UnifiedFormat, cnt int) {
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
			srcIdx := srcSel.getIndex(i)
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
	rowLocs *Vector,
	scanSel *SelectVector,
	scanCnt int,
	colId int,
	result *Vector,
	targetSel *SelectVector) {
	TupleDataTemplatedGatherSwitch(
		tuple._layout,
		rowLocs,
		colId,
		scanSel,
		scanCnt,
		result,
		targetSel,
	)
}

func TupleDataTemplatedScatterSwitch(
	src *Vector,
	srcFormat *UnifiedFormat,
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
			cnt,
			layout,
			rowLocations,
			heapLocations,
			colIdx,
			int32NullValue{},
		)
	case UINT64:
		TupleDataTemplatedScatter[uint64](
			srcFormat,
			cnt,
			layout,
			rowLocations,
			heapLocations,
			colIdx,
			uint64NullValue{},
		)
	default:
		panic("usp ")
	}
}

func TupleDataTemplatedScatter[T any](
	srcFormat *UnifiedFormat,
	cnt int,
	layout *TupleDataLayout,
	rowLocations *Vector,
	heapLocations *Vector,
	colIdx int,
	nVal nullValue[T],
) {
	srcSel := srcFormat._sel
	srcSlice := getSliceInPhyFormatUnifiedFormat[T](srcFormat)
	srcMask := srcFormat._mask

	targetLocs := getSliceInPhyFormatFlat[[]byte](rowLocations)
	targetHeapLocs := getSliceInPhyFormatFlat[[]byte](heapLocations)
	offsetInRow := layout.offsets()[colIdx]
	if srcMask.AllValid() {
		for i := 0; i < cnt; i++ {
			srcIdx := srcSel.getIndex(i)
			TupleDataValueStore[T](srcSlice[srcIdx], targetLocs[i], offsetInRow, targetHeapLocs[i])
		}
	} else {
		for i := 0; i < cnt; i++ {
			srcIdx := srcSel.getIndex(i)
			if srcMask.rowIsValid(uint64(srcIdx)) {
				TupleDataValueStore[T](srcSlice[srcIdx], targetLocs[i], offsetInRow, targetHeapLocs[i])
			} else {
				TupleDataValueStore[T](nVal.value(), targetLocs[i], offsetInRow, targetHeapLocs[i])
				tempMask := Bitmap{_bits: targetLocs[i]}
				tempMask.setInvalidUnsafe(uint64(colIdx))
			}
		}
	}

}

func TupleDataValueStore[T any](src T, rowLoc []byte, offsetInRow int, heapLoc []byte) {
	store[T](src, &rowLoc[offsetInRow])
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
	srcLocs := getSliceInPhyFormatFlat[*byte](rowLocs)
	targetData := getSliceInPhyFormatFlat[T](target)
	targetBitmap := getMaskInPhyFormatFlat(target)
	entryIdx, idxInEntry := getEntryIndex(uint64(colIdx))
	offsetInRow := layout.offsets()[colIdx]
	for i := 0; i < scanCnt; i++ {
		srcRow := toBytesSlice(srcLocs[scanSel.getIndex(i)], layout._rowWidth)
		targetIdx := targetSel.getIndex(i)
		rowMask := Bitmap{_bits: srcRow}
		if rowIsValidInEntry(
			rowMask.getEntry(entryIdx),
			idxInEntry) {
			targetData[targetIdx] = load[T](&srcRow[offsetInRow])
		} else {
			targetBitmap.setInvalid(uint64(targetIdx))
		}
	}
}
