package main

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
}

func NewHashJoin(op *PhysicalOperator, conds []*Expr) *HashJoin {
	hj := new(HashJoin)
	hj._conds = copyExprs(conds...)
	for _, cond := range conds {
		hj._keyTypes = append(hj._keyTypes, cond.Children[0].DataTyp.LTyp)
	}

	//right child output types
	for _, output := range op.Children[1].Outputs {
		hj._buildTypes = append(hj._buildTypes, output.DataTyp.LTyp)
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

type JoinHashTable struct {
	_conds []*Expr

	//types of keys in equality comparison
	_equalTypes []LType

	_keyTypes []LType

	_buildTypes []LType

	_predTypes []ET_SubTyp

	_layout *TupleDataLayout

	_joinType LOT_JoinType

	_bitmask uint64

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
		curSel = &sel
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

func (jht *JoinHashTable) Finalize() {

}

func (jht *JoinHashTable) Probe() {

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
}

type TuplePart struct {
	data          []UnifiedFormat
	rowLocations  *Vector
	heapLocations *Vector
	heapSizes     *Vector
}

func NewTupleDataCollection(layout *TupleDataLayout) *TupleDataCollection {
	ret := &TupleDataCollection{
		_layout: layout.copy(),
	}
	return ret
}

func (tuple *TupleDataCollection) Append(chunk *Chunk) {
	//TODO:
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

	//fill row
}

func (tuple *TupleDataCollection) toUnifiedFormat(part *TuplePart, chunk *Chunk) {
	for i, vec := range chunk._data {
		vec.toUnifiedFormat(chunk.card(), &part.data[i])
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
		panic("usp phy type")
	}
	heapSizeSlice := getSliceInPhyFormatFlat[uint64](sizes)
	srcSel := srcUni._sel
	srcMask := srcUni._mask

	switch pTyp {
	case VARCHAR:
		srcSlice := getSliceInPhyFormatUnifiedFormat[String](srcUni)
		//TODO:
	default:
		panic("usp phy type")
	}
}
