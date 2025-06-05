package compute

import (
	"unsafe"

	"github.com/daviszhen/plan/pkg/chunk"
	"github.com/daviszhen/plan/pkg/common"
	"github.com/daviszhen/plan/pkg/util"
)

type JoinHashTable struct {
	_conds []*Expr

	//types of keys in equality comparison
	_equalTypes []common.LType

	_keyTypes []common.LType

	_buildTypes []common.LType

	_predTypes []string

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
		if cond.FunImpl._name == FuncEqual || cond.FunImpl._name == FuncIn {
			util.AssertFunc(len(ht._equalTypes) == len(ht._keyTypes))
			ht._equalTypes = append(ht._equalTypes, typ)
		}
		ht._predTypes = append(ht._predTypes, cond.FunImpl._name)
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
