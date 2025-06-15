package compute

import (
	"fmt"
	"math"
	"unsafe"

	"github.com/daviszhen/plan/pkg/chunk"
	"github.com/daviszhen/plan/pkg/common"
	"github.com/daviszhen/plan/pkg/storage"
	"github.com/daviszhen/plan/pkg/util"
)

type aggrHTEntry struct {
	_salt       uint16
	_pageOffset uint16
	_pageNr     uint32
}

func (ent *aggrHTEntry) clean() {
	ent._salt = 0
	ent._pageOffset = 0
	ent._pageNr = 0
}

func (ent *aggrHTEntry) String() string {
	return fmt.Sprintf("salt:%d offset:%d nr:%d", ent._salt, ent._pageOffset, ent._pageNr)
}

var (
	aggrEntrySize int
)

func init() {
	aggrEntrySize = int(unsafe.Sizeof(aggrHTEntry{}))
}

type RadixPartitionedHashTable struct {
	_groupingSet     GroupingSet
	_nullGroups      []int
	_groupedAggrData *GroupedAggrData
	_groupTypes      []common.LType
	_radixLimit      int
	_groupingValues  []*chunk.Value
	_finalizedHT     *GroupedAggrHashTable
	_printHash       bool
	_finalized       bool
}

func NewRadixPartitionedHashTable(
	groupingSet GroupingSet,
	aggrData *GroupedAggrData,
) *RadixPartitionedHashTable {
	ret := new(RadixPartitionedHashTable)
	ret._groupingSet = groupingSet
	ret._groupedAggrData = aggrData
	ret._finalizedHT = nil

	for i := 0; i < aggrData.GroupCount(); i++ {
		if !ret._groupingSet.find(i) {
			ret._nullGroups = append(ret._nullGroups, i)
		}
	}

	ret._radixLimit = 10000

	if ret._groupingSet.empty() {
		ret._groupTypes = append(ret._groupTypes, common.TinyintType())
	}

	for ent := range ret._groupingSet.ordered() {
		util.AssertFunc(ent < len(ret._groupedAggrData._groupTypes))
		ret._groupTypes = append(ret._groupTypes,
			ret._groupedAggrData._groupTypes[ent])
	}
	ret.SetGroupingValues()
	return ret
}

type GroupedAggrHashTable struct {
	_layout       *TupleDataLayout
	_payloadTypes []common.LType

	_capacity        int
	_tupleSize       int
	_tuplesPerBlock  int
	_dataCollection  *TupleDataCollection
	_pinState        *TupleDataPinState
	_payloadHdsPtrs  []unsafe.Pointer
	_hashesHdl       *storage.BufferHandle
	_hashesHdlPtr    unsafe.Pointer
	_hashOffset      int
	_hashPrefixShift uint64
	_bitmask         uint64
	_finalized       bool
	_predicates      []string
	_printHash       bool
	_bufMgr          *storage.BufferManager
}

func NewGroupedAggrHashTable(
	groupTypes []common.LType,
	payloadTypes []common.LType,
	childrenOutputTypes []common.LType,
	aggrObjs []*AggrObject,
	initCap int,
	bufMgr *storage.BufferManager,
) *GroupedAggrHashTable {
	ret := new(GroupedAggrHashTable)
	ret._bufMgr = bufMgr
	//hash column in the end of tuple
	groupTypes = append(groupTypes, common.HashType())
	ret._layout = NewTupleDataLayout(groupTypes, aggrObjs, childrenOutputTypes, true, true)
	ret._payloadTypes = payloadTypes

	ret._tupleSize = ret._layout._rowWidth
	ret._tuplesPerBlock = int(storage.BLOCK_SIZE / uint64(ret._tupleSize))

	ret._hashOffset = ret._layout._offsets[ret._layout.columnCount()-1]
	ret._dataCollection = NewTupleDataCollection(ret._layout)
	ret._pinState = NewTupleDataPinState()
	ret._dataCollection.InitAppend(ret._pinState, PIN_PRRP_KEEP_PINNED)

	//allocate hash header
	ret._hashesHdl = ret._bufMgr.Allocate(storage.BLOCK_SIZE, true, nil)
	ret._hashesHdlPtr = ret._hashesHdl.Ptr()
	ret._hashPrefixShift = (HASH_WIDTH - 2) * 8
	ret.Resize(initCap)
	ret._predicates = make([]string, ret._layout.columnCount()-1)
	for i := 0; i < len(ret._predicates); i++ {
		ret._predicates[i] = FuncEqual
	}
	return ret
}

func (aht *GroupedAggrHashTable) AddChunk2(
	state *AggrHTAppendState,
	groups *chunk.Chunk,
	payload *chunk.Chunk,
	childrenOutput *chunk.Chunk,
	filter []int,
) int {
	hashes := chunk.NewFlatVector(common.HashType(), util.DefaultVectorSize)
	groups.Hash(hashes)
	return aht.AddChunk(
		state,
		groups,
		hashes,
		payload,
		childrenOutput,
		filter,
	)
}

func (aht *GroupedAggrHashTable) AddChunk(
	state *AggrHTAppendState,
	groups *chunk.Chunk,
	groupHashes *chunk.Vector,
	payload *chunk.Chunk,
	childrenOutput *chunk.Chunk,
	filter []int,
) int {
	util.AssertFunc(!aht._finalized)
	if groups.Card() == 0 {
		return 0
	}

	newGroupCount := aht.FindOrCreateGroups(
		state,
		groups,
		groupHashes,
		state._addresses,
		state._newGroups,
		childrenOutput,
	)
	AddInPlace(state._addresses, int64(aht._layout.aggrOffset()), payload.Card())

	filterIdx := 0
	payloadIdx := 0
	for i, aggr := range aht._layout._aggregates {
		if filterIdx >= len(filter) || i < filter[filterIdx] {
			payloadIdx += aggr._childCount
			AddInPlace(state._addresses, int64(aggr._payloadSize), payload.Card())
			continue
		}
		util.AssertFunc(i == filter[filterIdx])
		UpdateStates(
			aggr,
			state._addresses,
			payload,
			payloadIdx,
			payload.Card(),
		)
		payloadIdx += aggr._childCount
		AddInPlace(state._addresses, int64(aggr._payloadSize), payload.Card())
		filterIdx++
	}
	return newGroupCount
}

func (aht *GroupedAggrHashTable) FindOrCreateGroups(
	state *AggrHTAppendState,
	groups *chunk.Chunk,
	groupHashes *chunk.Vector,
	addresses *chunk.Vector,
	newGroupsOut *chunk.SelectVector,
	childrenOutput *chunk.Chunk,
) int {
	util.AssertFunc(!aht._finalized)
	util.AssertFunc(groups.ColumnCount()+1 == aht._layout.columnCount())
	util.AssertFunc(groupHashes.Typ().Id == common.HashType().Id)
	util.AssertFunc(state._htOffsets.PhyFormat().IsFlat())
	util.AssertFunc(state._htOffsets.Typ().Id == common.LTID_BIGINT)
	util.AssertFunc(addresses.Typ().Id == common.PointerType().Id)
	util.AssertFunc(state._hashSalts.Typ().Id == common.LTID_SMALLINT)

	//assertFunc(aht.Count()+groups.card() <= aht.MaxCap())
	//resize if needed
	if aht._capacity-aht.Count() <= groups.Card() || aht.Count() > aht.ResizeThreshold() {
		aht.Resize(aht._capacity * 2)
	}
	util.AssertFunc(aht._capacity-aht.Count() >= groups.Card())
	groupHashes.Flatten(groups.Card())
	groupHashesSlice := chunk.GetSliceInPhyFormatFlat[uint64](groupHashes)

	addresses.Flatten(groups.Card())
	addresessSlice := chunk.GetSliceInPhyFormatFlat[unsafe.Pointer](addresses)

	htOffsetsPtr := chunk.GetSliceInPhyFormatFlat[uint64](state._htOffsets)
	hashSaltsPtr := chunk.GetSliceInPhyFormatFlat[uint16](state._hashSalts)
	for i := 0; i < groups.Card(); i++ {
		ele := groupHashesSlice[i]
		util.AssertFunc((ele & aht._bitmask) == (ele % uint64(aht._capacity)))
		htOffsetsPtr[i] = ele & aht._bitmask
		hashSaltsPtr[i] = uint16(ele >> aht._hashPrefixShift)
	}

	selVec := chunk.IncrSelectVectorInPhyFormatFlat()
	if state._groupChunk.ColumnCount() == 0 {
		state._groupChunk.Init(aht._layout.types(), util.DefaultVectorSize)
	}

	util.AssertFunc(state._groupChunk.ColumnCount() ==
		len(aht._layout.types()))

	for i := 0; i < groups.ColumnCount(); i++ {
		state._groupChunk.Data[i].Reference(groups.Data[i])
	}

	state._groupChunk.Data[groups.ColumnCount()].Reference(groupHashes)
	state._groupChunk.SetCard(groups.Card())

	//prepare data structure holding incoming Chunk
	state._chunkState = NewTupleDataChunkState(aht._layout.columnCount(), aht._layout.childrenOutputCount())

	//groupChunk converted into chunkstate.data unified format
	toUnifiedFormat(state._chunkState, state._groupChunk)
	toUnifiedFormatForChildrenOutput(state._chunkState, childrenOutput)

	if state._groupData == nil {
		state._groupData = make([]*chunk.UnifiedFormat, state._groupChunk.ColumnCount())
		for i := 0; i < state._groupChunk.ColumnCount(); i++ {
			state._groupData[i] = &chunk.UnifiedFormat{}
		}
	}

	//group data refers to the chunk state.data unified format
	getVectorData(state._chunkState, state._groupData)

	newGroupCount := 0
	remainingEntries := groups.Card()
	for remainingEntries > 0 {
		newEntryCount := 0
		needCompareCount := 0
		noMatchCount := 0

		//check the entry exists or nit
		htEntrySlice := util.PointerToSlice[aggrHTEntry](aht._hashesHdlPtr, aht._capacity)
		for i := 0; i < remainingEntries; i++ {
			idx := selVec.GetIndex(i)
			htEntry := &htEntrySlice[htOffsetsPtr[idx]]
			if htEntry._pageNr == 0 {
				//empty cell

				htEntry._pageNr = 1
				htEntry._salt = uint16(groupHashesSlice[idx] >> aht._hashPrefixShift)

				state._emptyVector.SetIndex(newEntryCount, idx)
				newEntryCount++

				newGroupsOut.SetIndex(newGroupCount, idx)
				newGroupCount++
			} else {
				if htEntry._salt == hashSaltsPtr[idx] {
					//salt equal. need compare again
					state._groupCompareVector.SetIndex(needCompareCount, idx)
					needCompareCount++
				} else {
					state._noMatchVector.SetIndex(noMatchCount, idx)
					noMatchCount++
				}
			}

		}

		if newEntryCount > 0 {
			aht._dataCollection.AppendUnified(
				aht._pinState,
				state._chunkState,
				state._groupChunk,
				childrenOutput,
				state._emptyVector,
				newEntryCount,
			)

			//init aggr states
			InitStates(
				aht._layout,
				state._chunkState._rowLocations,
				chunk.IncrSelectVectorInPhyFormatFlat(),
				newEntryCount)

			//newly created block
			var blockId int
			if !util.Empty(aht._payloadHdsPtrs) {
				blockId = util.Size(aht._payloadHdsPtrs) - 1
			}
			aht.UpdateBlockPointers()
			blockPtr := aht._payloadHdsPtrs[blockId]
			blockEnd := util.PointerAdd(blockPtr, aht._tuplesPerBlock*aht._tupleSize)

			//update htEntry & save address
			rowLocations := chunk.GetSliceInPhyFormatFlat[unsafe.Pointer](state._chunkState._rowLocations)
			for j := 0; j < newEntryCount; j++ {
				rowLoc := rowLocations[j]
				if util.PointerLess(blockEnd, rowLoc) ||
					util.PointerLess(rowLoc, blockPtr) {
					blockId++
					util.AssertFunc(blockId < util.Size(aht._payloadHdsPtrs))
					blockPtr = aht._payloadHdsPtrs[blockId]
					blockEnd = util.PointerAdd(blockPtr, aht._tuplesPerBlock*aht._tupleSize)
				}
				util.AssertFunc(
					util.PointerLessEqual(blockPtr, rowLoc) &&
						util.PointerLess(rowLoc, blockEnd))
				util.AssertFunc(util.PointerSub(rowLoc, blockPtr)%int64(aht._tupleSize) == 0)
				idx := state._emptyVector.GetIndex(j)
				htEntry := &htEntrySlice[htOffsetsPtr[idx]]
				htEntry._pageNr = uint32(blockId + 1)
				htEntry._pageOffset = uint16(util.PointerSub(rowLoc, blockPtr) / int64(aht._tupleSize))
				addresessSlice[idx] = rowLoc
			}
		}

		if needCompareCount > 0 {
			//get address
			for j := 0; j < needCompareCount; j++ {
				idx := state._groupCompareVector.GetIndex(j)
				htEntry := &htEntrySlice[htOffsetsPtr[idx]]
				pagePtr := aht._payloadHdsPtrs[htEntry._pageNr-1]
				pageOffset := int(htEntry._pageOffset) * aht._tupleSize
				addresessSlice[idx] = util.PointerAdd(pagePtr, pageOffset)
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
			idx := state._noMatchVector.GetIndex(i)
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

func (aht *GroupedAggrHashTable) FetchAggregates(groups, result *chunk.Chunk) {
	util.AssertFunc(groups.ColumnCount()+1 == aht._layout.columnCount())
	for i := 0; i < result.ColumnCount(); i++ {
		util.AssertFunc(result.Data[i].Typ().Id == aht._payloadTypes[i].Id)
	}
	result.SetCard(groups.Card())
	if groups.Card() == 0 {
		return
	}

	appendState := NewAggrHTAppendState()
	addresses := chunk.NewVector2(common.PointerType(), util.DefaultVectorSize)
	hashes := chunk.NewFlatVector(common.HashType(), util.DefaultVectorSize)
	groups.Hash(hashes)
	if aht._printHash {
		println("scan hash")
		hashes.Print(groups.Card())
		groups.Print()
	}

	newGroupCnt := aht.FindOrCreateGroups(appendState, groups, hashes, addresses, appendState._newGroups, nil)
	util.AssertFunc(newGroupCnt == 0)

	//fetch the agg
	FinalizeStates(aht._layout, addresses, result, 0)
	if aht._printHash {
		fmt.Println("scan result")
		result.Print()
	}
}

func (aht *GroupedAggrHashTable) Scan(state *TupleDataScanState, result *chunk.Chunk) int {
	//get groupby data
	ret := aht._dataCollection.Scan(state, result)
	if !ret {
		return 0
	}

	//FIXME:
	//get aggr states
	//substract 1 fro removing the hash value of group by
	groupCols := aht._layout.columnCount() - 1 + aht._layout.childrenOutputCount()
	FinalizeStates(aht._layout, state._chunkState._rowLocations, result, groupCols)

	return result.Card()
}

func (aht *GroupedAggrHashTable) Resize(size int) {
	util.AssertFunc(!aht._finalized)
	util.AssertFunc(size >= util.DefaultVectorSize)
	util.AssertFunc(util.IsPowerOfTwo(uint64(size)))
	util.AssertFunc(size >= aht._capacity)

	//fmt.Println("resize from ", aht._capacity, "to", size)
	aht._capacity = size
	aht._bitmask = uint64(aht._capacity - 1)
	byteSize := aht._capacity * aggrEntrySize
	if byteSize > BLOCK_SIZE {
		aht._hashesHdl = aht._bufMgr.Allocate(uint64(byteSize), true, nil)
		aht._hashesHdlPtr = aht._hashesHdl.Ptr()
	}
	hashesArr := util.PointerToSlice[aggrHTEntry](aht._hashesHdlPtr, aht._capacity)
	for i := 0; i < len(hashesArr); i++ {
		hashesArr[i].clean()
	}
	if aht.Count() != 0 {
		util.AssertFunc(!util.Empty(aht._payloadHdsPtrs))
		aht._dataCollection.checkDupAll()
		blockId := 0
		blockPtr := aht._payloadHdsPtrs[blockId]
		blockEnt := util.PointerAdd(blockPtr, aht._tuplesPerBlock*aht._tupleSize)

		iter := NewTupleDataChunkIterator2(
			aht._dataCollection,
			PIN_PRRP_KEEP_PINNED,
			false,
		)

		for {
			rowLocs := iter.GetRowLocations()
			for i := 0; i < iter.GetCurrentChunkCount(); i++ {
				rowLoc := rowLocs[i]
				if util.PointerLess(blockEnt, rowLoc) ||
					util.PointerLess(rowLoc, blockPtr) {
					blockId++
					util.AssertFunc(blockId < len(aht._payloadHdsPtrs))
					blockPtr = aht._payloadHdsPtrs[blockId]
					blockEnt = util.PointerAdd(blockPtr, aht._tuplesPerBlock*aht._tupleSize)
				}

				util.AssertFunc(
					util.PointerLessEqual(blockPtr, rowLoc) &&
						util.PointerLess(rowLoc, blockEnt))
				util.AssertFunc(util.PointerSub(rowLoc, blockPtr)%int64(aht._tupleSize) == 0)

				hash := util.Load[uint64](util.PointerAdd(rowLoc, aht._hashOffset))
				util.AssertFunc((hash & aht._bitmask) == (hash % uint64(aht._capacity)))
				util.AssertFunc((hash >> aht._hashPrefixShift) <= math.MaxUint16)
				entIdx := hash & aht._bitmask
				for hashesArr[entIdx]._pageNr > 0 {
					entIdx++
					if entIdx >= uint64(aht._capacity) {
						entIdx = 0
					}
				}
				htEnt := &hashesArr[entIdx]
				util.AssertFunc(htEnt._pageNr == 0)
				htEnt._salt = uint16(hash >> aht._hashPrefixShift)
				htEnt._pageNr = uint32(1 + blockId)
				htEnt._pageOffset = uint16(util.PointerSub(rowLoc, blockPtr) / int64(aht._tupleSize))
			}

			next := iter.Next()
			if !next {
				break
			}
		}
	}

	aht.Verify()
}

func (aht *GroupedAggrHashTable) Verify() {
	hashesArr := util.PointerToSlice[aggrHTEntry](aht._hashesHdlPtr, aht._capacity)
	count := 0
	for i := 0; i < aht._capacity; i++ {
		hEnt := hashesArr[i]
		if hEnt._pageNr > 0 {
			util.AssertFunc(int(hEnt._pageOffset) < aht._tuplesPerBlock)
			util.AssertFunc(int(hEnt._pageNr) <= util.Size(aht._payloadHdsPtrs))
			ptr := util.PointerAdd(
				aht._payloadHdsPtrs[hEnt._pageNr-1],
				int(hEnt._pageOffset)*aht._tupleSize)
			hash := util.Load[uint64](util.PointerAdd(ptr, aht._hashOffset))
			util.AssertFunc(uint64(hEnt._salt) == (hash >> aht._hashPrefixShift))
			count++
		}
	}
	util.AssertFunc(count == aht.Count())
}

func (aht *GroupedAggrHashTable) Count() int {
	return aht._dataCollection.Count()
}

func (aht *GroupedAggrHashTable) ResizeThreshold() int {
	return int(float32(aht._capacity) / LOAD_FACTOR)
}

func (aht *GroupedAggrHashTable) UpdateBlockPointers() {
	for id, handle := range aht._pinState._rowHandles {
		if len(aht._payloadHdsPtrs) == 0 ||
			id > uint32(util.Size(aht._payloadHdsPtrs))-1 {
			need := id - uint32(util.Size(aht._payloadHdsPtrs)) + 1
			aht._payloadHdsPtrs = append(aht._payloadHdsPtrs,
				make([]unsafe.Pointer, need)...)
		}
		aht._payloadHdsPtrs[id] = handle.Ptr()
	}
}

func (aht *GroupedAggrHashTable) Finalize() {
	if aht._finalized {
		return
	}
	//TODO:
	aht._dataCollection.FinalizePinState(aht._pinState)
	aht._dataCollection.Unpin()

	aht._finalized = true
}
