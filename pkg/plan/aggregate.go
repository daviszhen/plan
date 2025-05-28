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
	"fmt"
	"math"
	"unsafe"

	"github.com/daviszhen/plan/pkg/chunk"
	"github.com/daviszhen/plan/pkg/common"
	"github.com/daviszhen/plan/pkg/storage"
	"github.com/daviszhen/plan/pkg/util"
)

func GetDistinctIndices(aggregates []*Expr) []int {
	indices := make([]int, 0)
	for i, aggr := range aggregates {
		if aggr.AggrTyp == NON_DISTINCT {
			continue
		}
		indices = append(indices, i)
	}
	return indices
}

func CreateDistinctAggrCollectionInfo(aggregates []*Expr) *DistinctAggrCollectionInfo {
	indices := GetDistinctIndices(aggregates)
	if len(indices) == 0 {
		return nil
	}
	return NewDistinctAggrCollectionInfo(aggregates, indices)
}

// TODO: add project on aggregate
func createGroupChunkTypes(groups []*Expr) []common.LType {
	if len(groups) == 0 {
		return nil
	}
	groupIndices := make(IntSet)
	for gidx := range groups {
		groupIndices.insert(gidx)
	}
	maxIdx := groupIndices.max()
	util.AssertFunc(maxIdx >= 0)
	types := make([]common.LType, maxIdx+1)
	for i := 0; i < len(types); i++ {
		types[i] = common.Null()
	}
	for gidx, group := range groups {
		types[gidx] = group.DataTyp
	}
	return types
}

func (haggr *HashAggr) SinkDistinctGrouping(
	data, childrenOutput *chunk.Chunk,
	groupingIdx int,
) {
	distinctInfo := haggr._distinctCollectionInfo
	distinctData := haggr._groupings[groupingIdx]._distinctData

	var tableIdx int
	var has bool
	dump := &chunk.Chunk{}
	for _, idx := range distinctInfo._indices {
		//aggr := haggr._groupedAggrData._aggregates[idx]

		if tableIdx, has = distinctInfo._tableMap[idx]; !has {
			panic(fmt.Sprintf("no table index for index %d", idx))
		}

		radixTable := distinctData._radixTables[tableIdx]
		if radixTable == nil {
			continue
		}
		radixTable.Sink(data, dump, childrenOutput, []int{})
	}
}

func (haggr *HashAggr) SinkDistinct(chunk, childrenOutput *chunk.Chunk) {
	for i := 0; i < len(haggr._groupings); i++ {
		haggr.SinkDistinctGrouping(chunk, childrenOutput, i)
	}
}

func (haggr *HashAggr) Sink(data *chunk.Chunk) {
	payload := &chunk.Chunk{}
	payload.Init(haggr._groupedAggrData._payloadTypes, util.DefaultVectorSize)
	offset := len(haggr._groupedAggrData._groupTypes)
	for i := 0; i < len(haggr._groupedAggrData._payloadTypes); i++ {
		payload.Data[i].Reference(data.Data[offset+i])
	}
	payload.SetCard(data.Card())

	childrenOutput := &chunk.Chunk{}
	childrenOutput.Init(haggr._groupedAggrData._childrenOutputTypes, util.DefaultVectorSize)
	offset = offset + len(haggr._groupedAggrData._payloadTypes)
	for i := 0; i < len(haggr._groupedAggrData._childrenOutputTypes); i++ {
		childrenOutput.Data[i].Reference(data.Data[offset+i])
	}
	childrenOutput.SetCard(data.Card())

	if haggr._distinctCollectionInfo != nil {
		haggr.SinkDistinct(data, childrenOutput)
	}

	for _, grouping := range haggr._groupings {
		grouping._tableData._printHash = haggr._printHash
		grouping._tableData.Sink(data, payload, childrenOutput, haggr._nonDistinctFilter)
	}
}

func (haggr *HashAggr) FetechAggregates(state *HashAggrScanState, groups, output *chunk.Chunk) OperatorResult {
	//1. table_data.GetData
	for {
		if state._radixIdx >= len(haggr._groupings) {
			break
		}
		grouping := haggr._groupings[state._radixIdx]
		radixTable := grouping._tableData
		radixTable.FetchAggregates(groups, output)
		if output.Card() != 0 {
			return haveMoreOutput
		}
		state._radixIdx++
		if state._radixIdx >= len(haggr._groupings) {
			break
		}
		state._state = &TupleDataScanState{}
	}

	if output.Card() == 0 {
		return Done
	} else {
		return haveMoreOutput
	}
}

func (haggr *HashAggr) GetData(state *HashAggrScanState, output, rawInput *chunk.Chunk) OperatorResult {
	//1. table_data.GetData
	for {
		if state._radixIdx >= len(haggr._groupings) {
			break
		}
		grouping := haggr._groupings[state._radixIdx]
		radixTable := grouping._tableData
		radixTable.GetData(state._state, output, rawInput)
		if output.Card() != 0 {
			return haveMoreOutput
		}
		state._radixIdx++
		if state._radixIdx >= len(haggr._groupings) {
			break
		}
		state._state = &TupleDataScanState{}
	}

	//2. run filter
	if output.Card() == 0 {
		return Done
	} else {
		return haveMoreOutput
	}
}

func (haggr *HashAggr) Finalize() {
	haggr.FinalizeInternal(true)
}

func (haggr *HashAggr) FinalizeInternal(checkDistinct bool) {
	if checkDistinct && haggr._distinctCollectionInfo != nil {
		haggr.FinalizeDistinct()
	}
	for i := 0; i < len(haggr._groupings); i++ {
		grouping := haggr._groupings[i]
		grouping._tableData.Finalize()
	}
}

func (haggr *HashAggr) FinalizeDistinct() {
	for i := 0; i < len(haggr._groupings); i++ {
		grouping := haggr._groupings[i]
		distinctData := grouping._distinctData

		for tableIdx := 0; tableIdx < len(distinctData._radixTables); tableIdx++ {
			if distinctData._radixTables[tableIdx] == nil {
				continue
			}
			radixTable := distinctData._radixTables[tableIdx]
			radixTable.Finalize()
		}
	}

	//finish distinct
	for i := 0; i < len(haggr._groupings); i++ {
		grouping := haggr._groupings[i]
		haggr.DistinctGrouping(grouping, i)
	}
}

func (haggr *HashAggr) DistinctGrouping(
	groupingData *HashAggrGroupingData,
	groupingIdx int,
) {
	aggregates := haggr._distinctCollectionInfo._aggregates
	data := groupingData._distinctData

	//fake input chunk
	//group by
	groupChunk := &chunk.Chunk{}
	groupChunk.Init(haggr._inputGroupTypes, util.DefaultVectorSize)

	groups := haggr._groupedAggrData._groups
	groupbySize := util.Size(groups)

	aggrInputChunk := &chunk.Chunk{}
	aggrInputChunk.Init(haggr._groupedAggrData._payloadTypes, util.DefaultVectorSize)

	payloadIdx := 0
	nextPayloadIdx := 0

	for i := 0; i < len(haggr._groupedAggrData._aggregates); i++ {
		aggr := aggregates[i]

		//forward payload idx
		payloadIdx = nextPayloadIdx
		nextPayloadIdx = payloadIdx + len(aggr.Children)

		//skip non distinct
		if !data.IsDistinct(i) {
			continue
		}

		tableIdx := data._info._tableMap[i]
		radixTable := data._radixTables[tableIdx]

		//groupby + distinct payload
		outputChunk := &chunk.Chunk{}
		outputChunk.Init(data._groupedAggrData[tableIdx]._groupTypes, util.DefaultVectorSize)

		//children output
		childrenChunk := &chunk.Chunk{}
		childrenChunk.Init(data._groupedAggrData[tableIdx]._childrenOutputTypes, util.DefaultVectorSize)

		//get all data from distinct aggr hashtable
		//sink them into main hashtable

		scanState := &TupleDataScanState{}

		for {
			outputChunk.Reset()
			groupChunk.Reset()
			aggrInputChunk.Reset()
			childrenChunk.Reset()

			res := radixTable.GetData(scanState, outputChunk, childrenChunk)
			switch res {
			case Done:
				util.AssertFunc(outputChunk.Card() == 0)
			case InvalidOpResult:
				panic("invalid op result")
			}
			if outputChunk.Card() == 0 {
				break
			}

			//composite the
			groupedAggrData := data._groupedAggrData[tableIdx]
			for groupIdx := 0; groupIdx < groupbySize; groupIdx++ {
				//group := groupedAggrData._groups[groupIdx]
				groupChunk.Data[groupIdx].Reference(outputChunk.Data[groupIdx])
			}
			groupChunk.SetCard(outputChunk.Card())

			for childIdx := 0; childIdx < len(groupedAggrData._groups)-groupbySize; childIdx++ {
				aggrInputChunk.Data[payloadIdx+childIdx].Reference(
					outputChunk.Data[groupbySize+childIdx])
			}
			aggrInputChunk.SetCard(outputChunk.Card())
			groupingData._tableData.Sink(groupChunk, aggrInputChunk, childrenChunk, []int{i})
		}
	}
}

const (
	LOAD_FACTOR = 1.5
	HASH_WIDTH  = 8
	BLOCK_SIZE  = 256*1024 - 8
)

func (rpht *RadixPartitionedHashTable) SetGroupingValues() {
	groupFuncs := rpht._groupedAggrData._groupingFuncs
	for _, group := range groupFuncs {
		util.AssertFunc(len(group) < 64)
		for i, gval := range group {
			groupingValue := int64(0)
			if !rpht._groupingSet.find(gval) {
				//do not group on this value
				groupingValue += 1 << (len(group) - (i + 1))
			}
			rpht._groupingValues = append(rpht._groupingValues,
				&chunk.Value{
					Typ: common.BigintType(),
					I64: groupingValue,
				})
		}

	}
}

func (rpht *RadixPartitionedHashTable) Sink(data, payload, childrenOutput *chunk.Chunk, filter []int) {
	if rpht._finalizedHT == nil {
		fmt.Println("init aggregate finalize ht")
		//prepare aggr objs
		aggrObjs := CreateAggrObjects(rpht._groupedAggrData._bindings)

		rpht._finalizedHT = NewGroupedAggrHashTable(
			rpht._groupTypes,
			rpht._groupedAggrData._payloadTypes,
			rpht._groupedAggrData._childrenOutputTypes,
			aggrObjs,
			2*util.DefaultVectorSize,
			storage.GBufferMgr,
		)
		rpht._finalizedHT._printHash = rpht._printHash
	}
	groupChunk := &chunk.Chunk{}
	groupChunk.Init(rpht._groupTypes, util.DefaultVectorSize)
	for i, idx := range rpht._groupingSet.ordered() {
		groupChunk.Data[i].Reference(data.Data[idx])
	}
	groupChunk.SetCard(data.Card())
	state := NewAggrHTAppendState()
	rpht._finalizedHT.AddChunk2(
		state,
		groupChunk,
		payload,
		childrenOutput,
		filter,
	)
}

func (rpht *RadixPartitionedHashTable) FetchAggregates(groups, result *chunk.Chunk) {
	groupChunk := &chunk.Chunk{}
	groupChunk.Init(rpht._groupTypes, util.DefaultVectorSize)
	for i, idx := range rpht._groupingSet.ordered() {
		groupChunk.Data[i].Reference(groups.Data[idx])
	}
	groupChunk.SetCard(groups.Card())
	rpht._finalizedHT.FetchAggregates(groups, result)
}

func (rpht *RadixPartitionedHashTable) GetData(state *TupleDataScanState, output, childrenOutput *chunk.Chunk) OperatorResult {
	if !state._init {
		if rpht._finalizedHT == nil {
			return Done
		}
		layout := rpht._finalizedHT._layout
		for i := 0; i < layout.columnCount()-1; i++ {
			state._colIds = append(state._colIds, i)
		}

		rpht._finalizedHT._dataCollection.InitScan(state)
	}

	scanTyps := make([]common.LType, 0)
	//FIXME:
	//groupby types + children output types +aggr return types
	scanTyps = append(scanTyps, rpht._groupTypes...)
	scanTyps = append(scanTyps, rpht._groupedAggrData._childrenOutputTypes...)
	scanTyps = append(scanTyps, rpht._groupedAggrData._aggrReturnTypes...)
	scanChunk := &chunk.Chunk{}
	scanChunk.Init(scanTyps, util.DefaultVectorSize)
	cnt := rpht._finalizedHT.Scan(state, scanChunk)
	output.SetCard(cnt)

	for i, ent := range rpht._groupingSet.ordered() {
		output.Data[ent].Reference(scanChunk.Data[i])
	}

	for ent := range rpht._nullGroups {
		output.Data[ent].SetPhyFormat(chunk.PF_CONST)
		chunk.SetNullInPhyFormatConst(output.Data[ent], true)
	}

	util.AssertFunc(rpht._groupingSet.count()+len(rpht._nullGroups) == rpht._groupedAggrData.GroupCount())
	for i := 0; i < len(rpht._groupedAggrData._aggregates); i++ {
		output.Data[rpht._groupedAggrData.GroupCount()+i].Reference(scanChunk.Data[len(rpht._groupTypes)+len(rpht._groupedAggrData._childrenOutputTypes)+i])
	}

	util.AssertFunc(len(rpht._groupedAggrData._groupingFuncs) == len(rpht._groupingValues))
	for i := 0; i < len(rpht._groupedAggrData._groupingFuncs); i++ {
		output.Data[rpht._groupedAggrData.GroupCount()+len(rpht._groupedAggrData._aggregates)+i].ReferenceValue(rpht._groupingValues[i])
	}

	//split the children output chunk from the scan chunk
	for i := 0; i < len(rpht._groupedAggrData._childrenOutputTypes); i++ {
		childrenOutput.Data[i].Reference(scanChunk.Data[len(rpht._groupTypes)+i])
	}
	childrenOutput.SetCard(cnt)

	if output.Card() == 0 {
		return Done
	} else {
		return haveMoreOutput
	}
}

func (rpht *RadixPartitionedHashTable) Finalize() {
	util.AssertFunc(!rpht._finalized)
	rpht._finalized = true
	rpht._finalizedHT.Finalize()
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
	ret._predicates = make([]ET_SubTyp, ret._layout.columnCount()-1)
	for i := 0; i < len(ret._predicates); i++ {
		ret._predicates[i] = ET_Equal
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

func InitStates(
	layout *TupleDataLayout,
	addresses *chunk.Vector,
	sel *chunk.SelectVector,
	cnt int,
) {
	if cnt == 0 {
		return
	}

	pointers := chunk.GetSliceInPhyFormatFlat[unsafe.Pointer](addresses)
	offsets := layout.offsets()
	aggrIdx := layout.aggrIdx()

	for _, aggr := range layout._aggregates {
		for i := 0; i < cnt; i++ {
			rowIdx := sel.GetIndex(i)
			row := pointers[rowIdx]
			aggr._func._init(util.PointerAdd(row, offsets[aggrIdx]))
		}
		aggrIdx++
	}
}

func UpdateStates(
	aggr *AggrObject,
	addresses *chunk.Vector,
	payload *chunk.Chunk,
	argIdx int,
	cnt int,
) {
	inputData := &AggrInputData{}
	var input []*chunk.Vector
	if aggr._childCount != 0 {
		input = []*chunk.Vector{payload.Data[argIdx]}
	}
	aggr._func._update(
		input,
		inputData,
		aggr._childCount,
		addresses,
		cnt,
	)
}
