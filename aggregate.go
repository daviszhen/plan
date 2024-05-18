package main

import (
	"unsafe"
)

const (
	LOAD_FACTOR = 1.5
	HASH_WIDTH  = 8
	BLOCK_SIZE  = 256*1024 - 8
)

type aggrHTEntry struct {
	_salt       uint16
	_pageOffset uint16
	_pageNr     uint32
	_rowPtr     unsafe.Pointer
}

var (
	aggrEntrySize int
)

func init() {
	aggrEntrySize = int(unsafe.Sizeof(aggrHTEntry{}))
}

type AggrHTAppendState struct {
	_htOffsets          *Vector
	_hashSalts          *Vector
	_groupCompareVector *SelectVector
	_noMatchVector      *SelectVector
	_emptyVector        *SelectVector
	_newGroups          *SelectVector
	_addresses          *Vector
	_groupData          []*UnifiedFormat
	_groupChunk         *Chunk
	_chunkState         *TuplePart
}

type AggrHashTable struct {
	_layout       *TupleDataLayout
	_payloadTypes []LType

	_capacity        int
	_tupleSize       int
	_tuplesPerBlock  int
	_dataCollection  *TupleDataCollection
	_payloadHdsPtrs  []unsafe.Pointer
	_hashesHdlPtr    unsafe.Pointer
	_hashOffset      int
	_hashPrefixShift uint64
	_bitmask         uint64
	_finalized       bool
	_predicates      []ET_SubTyp
}

type AggrType int

const (
	NON_DISTINCT AggrType = iota + 1
	DISTINCT
)

type AggrObject struct {
	_func        *AggrFunc
	_childCount  int
	_payloadSize int
	_aggrType    AggrType
	_retType     PhyType
}

type AggrInputData struct {
}

type aggrStateSize func() int
type aggrInit func(pointer unsafe.Pointer)
type aggrUpdate func([]*Vector, *AggrInputData, int, *Vector, int)
type aggrFinalize func(*Vector, *AggrInputData, *Vector, int, int)
type aggrFunction func(*AggrFunc, []*Expr)
type aggrSimpleUpdate func([]*Vector, *AggrInputData, int, unsafe.Pointer, int)
type aggrWindow func([]*Vector, *Bitmap, *AggrInputData)

type AggrFunc struct {
	_stateSize    aggrStateSize
	_init         aggrInit
	_update       aggrUpdate
	_finalize     aggrFinalize
	_func         aggrFunction
	_simpleUpdate aggrSimpleUpdate
	_window       aggrWindow
}

func NewAggrHashTable(
	groupTypes []LType,
	payloadTypes []LType,
	aggrObjs []*AggrObject,
	initCap int,
) *AggrHashTable {
	ret := new(AggrHashTable)
	//hash column in the end of tuple
	groupTypes = append(groupTypes, hashType())
	ret._layout = NewTupleDataLayout(groupTypes, aggrObjs, false, true)

	ret._tupleSize = ret._layout._rowWidth
	ret._tuplesPerBlock = 0 //?

	ret._hashOffset = ret._layout._offsets[ret._layout.columnCount()-1]
	ret._dataCollection = NewTupleDataCollection(ret._layout)
	//allocate hash header
	ret._hashesHdlPtr = bytesSliceToPointer(make([]byte, BLOCK_SIZE))
	ret._hashPrefixShift = (HASH_WIDTH - 2) * 8
	ret.Resize(initCap)
	ret._predicates = make([]ET_SubTyp, ret._layout.columnCount()-1)
	for i := 0; i < len(ret._predicates); i++ {
		ret._predicates[i] = ET_Equal
	}
	return ret
}

func (aht *AggrHashTable) AddChunk2(
	state *AggrHTAppendState,
	groups *Chunk,
	payload *Chunk,
) int {
	hashes := NewFlatVector(hashType(), defaultVectorSize)
	groups.Hash(hashes)

	return aht.AddChunk(
		state,
		groups,
		hashes,
		payload,
	)
}

func (aht *AggrHashTable) AddChunk(
	state *AggrHTAppendState,
	groups *Chunk,
	groupHashes *Vector,
	payload *Chunk) int {
	assertFunc(!aht._finalized)
	if groups.card() == 0 {
		return 0
	}

	newGroupCount := aht.FindOrCreateGroups(
		state,
		groups,
		groupHashes,
		state._addresses,
		state._newGroups)
	AddInPlace(state._addresses, int64(aht._layout.aggrOffset()), payload.card())

	payloadIdx := 0
	for _, aggr := range aht._layout._aggregates {
		UpdateStates(
			aggr,
			state._addresses,
			payload,
			payloadIdx,
			payload.card(),
		)
		payloadIdx += aggr._childCount
		AddInPlace(state._addresses, int64(aggr._payloadSize), payload.card())
	}
	return newGroupCount
}

func (aht *AggrHashTable) FindOrCreateGroups(
	state *AggrHTAppendState,
	groups *Chunk,
	groupHashes *Vector,
	addresses *Vector,
	newGroupsOut *SelectVector,
) int {
	assertFunc(!aht._finalized)
	assertFunc(groups.columnCount()+1 == aht._layout.columnCount())
	assertFunc(groupHashes.typ().id == hashType().id)
	assertFunc(state._htOffsets.phyFormat().isFlat())
	assertFunc(state._htOffsets.typ().id == LTID_BIGINT)
	assertFunc(addresses.typ().id == pointerType().id)
	assertFunc(state._hashSalts.typ().id == LTID_SMALLINT)

	//assertFunc(aht.Count()+groups.card() <= aht.MaxCap())
	//resize if needed
	if aht._capacity-aht.Count() <= groups.card() || aht.Count() > aht.ResizeThreshold() {
		aht.Resize(aht._capacity * 2)
	}
	assertFunc(aht._capacity-aht.Count() >= groups.card())
	groupHashes.flatten(groups.card())
	groupHashesSlice := getSliceInPhyFormatFlat[uint64](groupHashes)

	addresses.flatten(groups.card())
	addresessSlice := getSliceInPhyFormatFlat[unsafe.Pointer](addresses)

	htOffsetsPtr := getSliceInPhyFormatFlat[uint64](state._htOffsets)
	hashSaltsPtr := getSliceInPhyFormatFlat[uint16](state._hashSalts)
	for i := 0; i < groups.card(); i++ {
		ele := groupHashesSlice[i]
		assertFunc((ele & aht._bitmask) == (ele % uint64(aht._capacity)))
		htOffsetsPtr[i] = ele & aht._bitmask
		hashSaltsPtr[i] = uint16(ele >> aht._hashPrefixShift)
	}

	selVec := incrSelectVectorInPhyFormatFlat()
	if state._groupChunk.columnCount() == 0 {
		state._groupChunk.init(aht._layout.types(), 0)
	}

	assertFunc(state._groupChunk.columnCount() ==
		len(aht._layout.types()))

	for i := 0; i < groups.columnCount(); i++ {
		state._groupChunk._data[i].reference(groups._data[i])
	}

	state._groupChunk._data[groups.columnCount()].reference(groupHashes)
	state._groupChunk.setCard(groups.card())

	toUnifiedFormat(state._chunkState, state._groupChunk)

	if state._groupData == nil {
		state._groupData = make([]*UnifiedFormat, state._groupChunk.columnCount())
	}

	getVectorData(state._chunkState, state._groupData)

	newGroupCount := 0
	remainingEntries := groups.card()
	for remainingEntries > 0 {
		newEntryCount := 0
		needCompareCount := 0
		noMatchCount := 0

		//check the entry exists or nit
		htEntrySlice := pointerToSlice[aggrHTEntry](aht._hashesHdlPtr, aht._capacity)
		for i := 0; i < remainingEntries; i++ {
			idx := selVec.getIndex(i)
			htEntry := htEntrySlice[htOffsetsPtr[idx]]

			if htEntry._pageNr == 0 {
				//empty cell

				htEntry._pageNr = 1
				htEntry._salt = uint16(groupHashesSlice[idx] >> aht._hashPrefixShift)

				state._emptyVector.setIndex(newEntryCount, idx)
				newEntryCount++

				newGroupsOut.setIndex(newGroupCount, idx)
				newGroupCount++
			} else {
				if htEntry._salt == hashSaltsPtr[idx] {
					//salt equal. need compare again
					state._groupCompareVector.setIndex(needCompareCount, idx)
					needCompareCount++
				} else {
					state._noMatchVector.setIndex(noMatchCount, idx)
					noMatchCount++
				}
			}

		}

		if newEntryCount > 0 {
			aht._dataCollection.AppendUnified(
				state._chunkState,
				state._groupChunk,
				state._emptyVector,
				newEntryCount,
			)

			//init aggr states
			InitStates(
				aht._layout,
				state._chunkState.rowLocations,
				incrSelectVectorInPhyFormatFlat(),
				newEntryCount)

			//update htEntry & save address
			rowLocations := getSliceInPhyFormatFlat[unsafe.Pointer](state._chunkState.rowLocations)
			for j := 0; j < newEntryCount; j++ {
				idx := state._emptyVector.getIndex(j)
				htEntry := htEntrySlice[htOffsetsPtr[idx]]
				htEntry._pageNr = 1 //TOOD: refine. do not mean anything
				htEntry._rowPtr = rowLocations[j]
				addresessSlice[idx] = rowLocations[j]
			}
		}

		if needCompareCount > 0 {
			//get address
			for j := 0; j < needCompareCount; j++ {
				idx := state._groupCompareVector.getIndex(j)
				htEntry := htEntrySlice[htOffsetsPtr[idx]]
				addresessSlice[idx] = htEntry._rowPtr
			}

			Match(
				state._groupChunk,
				state._groupData,
				aht._layout,
				addresses,
				aht._predicates,
				state._groupCompareVector,
				needCompareCount,
				state._noMatchVector,
				&noMatchCount,
			)
		}

		for i := 0; i < noMatchCount; i++ {
			idx := state._noMatchVector.getIndex(i)
			htOffsetsPtr[idx]++
			if htOffsetsPtr[idx] >= uint64(aht._capacity) {
				htOffsetsPtr[idx] = 0
			}
		}

		selVec = state._noMatchVector
		remainingEntries = noMatchCount
	}

	return newGroupCount
}

func (aht *AggrHashTable) Resize(size int) {
	assertFunc(!aht._finalized)
	assertFunc(size >= defaultVectorSize)
	assertFunc(isPowerOfTwo(uint64(size)))
	assertFunc(size >= aht._capacity)

	aht._capacity = size
	aht._bitmask = uint64(aht._capacity - 1)
	byteSize := aht._capacity * aggrEntrySize
	if byteSize > BLOCK_SIZE {
		aht._hashesHdlPtr = bytesSliceToPointer(make([]byte, byteSize))
	}

	if aht.Count() != 0 {
		//TODO:
	}
}

func (aht *AggrHashTable) Count() int {
	return aht._dataCollection.Count()
}

func (aht *AggrHashTable) ResizeThreshold() int {
	return int(float32(aht._capacity) / LOAD_FACTOR)
}

func InitStates(
	layout *TupleDataLayout,
	addresses *Vector,
	sel *SelectVector,
	cnt int,
) {
	if cnt == 0 {
		return
	}

	pointers := getSliceInPhyFormatFlat[unsafe.Pointer](addresses)
	offsets := layout.offsets()
	aggrIdx := layout.columnCount()

	for _, aggr := range layout._aggregates {
		for i := 0; i < cnt; i++ {
			rowIdx := sel.getIndex(i)
			row := pointers[rowIdx]
			aggr._func._init(pointerAdd(row, offsets[aggrIdx]))
		}
		aggrIdx++
	}
}

func UpdateStates(
	aggr *AggrObject,
	addresses *Vector,
	payload *Chunk,
	argIdx int,
	cnt int,
) {
	inputData := &AggrInputData{}
	var input []*Vector
	if aggr._childCount != 0 {
		input = []*Vector{payload._data[argIdx]}
	}
	aggr._func._update(
		input,
		inputData,
		aggr._childCount,
		addresses,
		cnt,
	)
}
