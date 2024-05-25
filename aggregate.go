package main

import (
	"math"
	"sort"
	"unsafe"
)

type IntSet map[int]struct{}

func (is IntSet) insert(id int) {
	is[id] = struct{}{}
}

func (is IntSet) max() int {
	maxKey := math.MinInt
	for key, _ := range is {
		maxKey = max(maxKey, key)
	}
	return maxKey
}

type GroupingSet map[int]struct{}

func (gs GroupingSet) insert(id int) {
	gs[id] = struct{}{}
}

func (gs GroupingSet) find(id int) bool {
	_, ok := gs[id]
	return ok
}

func (gs GroupingSet) empty() bool {
	return len(gs) == 0
}
func (gs GroupingSet) ordered() []int {
	ret := make([]int, 0, len(gs))
	for id := range gs {
		ret = append(ret, id)
	}
	sort.Ints(ret)
	return ret
}

type GroupedAggrData struct {
	_groups          []*Expr
	_groupingFuncs   [][]int //GROUPING functions
	_groupTypes      []LType
	_aggregates      []*Expr
	_payloadTypes    []LType
	_paramExprs      []*Expr //param exprs of the aggr function
	_aggrReturnTypes []LType
	_bindings        []*Expr //pointer to aggregates
}

func (gad *GroupedAggrData) GroupCount() int {
	return len(gad._groups)
}

func (gad *GroupedAggrData) InitGroupby(
	groups []*Expr,
	exprs []*Expr,
	groupingFuncs [][]int,
) {
	gad.InitGroupbyGroups(groups)
	gad.SetGroupingFuncs(groupingFuncs)

	//aggr exprs
	for _, aggr := range exprs {
		gad._bindings = append(gad._bindings, aggr)
		gad._aggrReturnTypes = append(gad._aggrReturnTypes, aggr.DataTyp.LTyp)
		for _, child := range aggr.Children {
			gad._payloadTypes = append(gad._payloadTypes, child.DataTyp.LTyp)
			gad._paramExprs = append(gad._paramExprs, child)
		}
		gad._aggregates = append(gad._aggregates, aggr)
	}
}

func (gad *GroupedAggrData) InitDistinct(
	aggr *Expr,
	groups []*Expr,
) {
	gad.InitDistinctGroups(groups)
	gad._aggrReturnTypes = append(gad._aggrReturnTypes, aggr.DataTyp.LTyp)
	for _, child := range aggr.Children {
		gad._groupTypes = append(gad._groupTypes, child.DataTyp.LTyp)
		gad._groups = append(gad._groups, child.copy())
		gad._payloadTypes = append(gad._payloadTypes, child.DataTyp.LTyp)
	}
}

func (gad *GroupedAggrData) InitDistinctGroups(
	groups []*Expr,
) {
	if len(groups) == 0 {
		return
	}

	for _, group := range groups {
		gad._groupTypes = append(gad._groupTypes, group.DataTyp.LTyp)
		gad._groups = append(gad._groups, group.copy())
	}
}

func (gad *GroupedAggrData) SetGroupingFuncs(funcs [][]int) {
	gad._groupingFuncs = funcs
}

func (gad *GroupedAggrData) InitGroupbyGroups(groups []*Expr) {
	for _, g := range groups {
		gad._groupTypes = append(gad._groupTypes, g.DataTyp.LTyp)
	}
	gad._groups = groups
}

type HashAggrGroupingData struct {
	_tableData    *RadixPartitionedHashTable
	_distinctData *DistinctAggrData
}

func NewHashAggrGroupingData(
	groupingSet GroupingSet,
	aggrData *GroupedAggrData,
	info *DistinctAggrCollectionInfo,
) *HashAggrGroupingData {
	ret := &HashAggrGroupingData{}
	ret._tableData = NewRadixPartitionedHashTable(groupingSet, aggrData)
	if info != nil {
		ret._distinctData = NewDistinctAggrData(info, groupingSet, aggrData._groups)
	}

	return ret
}

type DistinctAggrData struct {
	_groupedAggrData []*GroupedAggrData
	_radixTables     []*RadixPartitionedHashTable
	_groupingSets    []GroupingSet
	_info            *DistinctAggrCollectionInfo
}

func NewDistinctAggrData(
	info *DistinctAggrCollectionInfo,
	groups GroupingSet,
	groupExprs []*Expr,
) *DistinctAggrData {
	ret := new(DistinctAggrData)
	ret._info = info

	ret._groupedAggrData = make([]*GroupedAggrData, info._tableCount)
	ret._radixTables = make([]*RadixPartitionedHashTable, info._tableCount)
	ret._groupingSets = make([]GroupingSet, info._tableCount)
	for i := 0; i < len(ret._groupingSets); i++ {
		ret._groupingSets[i] = make(GroupingSet)
	}

	//init hash table
	for _, index := range info._indices {
		aggr := info._aggregates[index]
		if _, ok := info._tableMap[index]; !ok {
			panic("no such index in table map")
		}
		tableIdx := info._tableMap[index]
		if ret._radixTables[tableIdx] != nil {
			continue
		}
		groupingSet := ret._groupingSets[tableIdx]
		for group, _ := range groups {
			groupingSet.insert(group)
		}

		groupBySize := len(groupExprs)
		for gIdx := 0; gIdx < len(aggr.Children); gIdx++ {
			groupingSet.insert(gIdx + groupBySize)
		}

		//create hash table
		ret._groupedAggrData[tableIdx] = &GroupedAggrData{}
		ret._groupedAggrData[tableIdx].InitDistinct(aggr, groupExprs)
		ret._radixTables[tableIdx] = NewRadixPartitionedHashTable(groupingSet, ret._groupedAggrData[tableIdx])
	}

	return ret
}

type DistinctAggrCollectionInfo struct {
	_indices         []int //distinct aggr indice
	_tableCount      int
	_tableIndices    []int
	_tableMap        map[int]int
	_aggregates      []*Expr
	_totalChildCount int
}

func (daci *DistinctAggrCollectionInfo) CreateTableIndexMap() int {
	//create table for every distinct aggr
	//some aggrs may share same table
	tableInputs := make([]*Expr, 0)
	for _, aggrIdx := range daci._indices {
		aggr := daci._aggregates[aggrIdx]
		found := findIf[*Expr](tableInputs, func(t *Expr) bool {
			if len(aggr.Children) != len(t.Children) {
				return false
			}
			for i := 0; i < len(aggr.Children); i++ {
				child := aggr.Children[i]
				oChild := t.Children[i]
				if child.ColRef.column() != oChild.ColRef.column() {
					return false
				}
			}
			return true
		})
		if found != -1 {
			daci._tableMap[aggrIdx] = found
			continue
		}
		daci._tableMap[aggrIdx] = len(tableInputs)
		tableInputs = append(tableInputs, aggr)
	}
	assertFunc(len(daci._tableMap) == len(daci._indices))
	assertFunc(len(tableInputs) <= len(daci._indices))

	return len(tableInputs)
}

func NewDistinctAggrCollectionInfo(
	aggregates []*Expr,
	indices []int,
) *DistinctAggrCollectionInfo {
	ret := &DistinctAggrCollectionInfo{}
	ret._indices = indices
	ret._aggregates = aggregates
	ret._tableCount = ret.CreateTableIndexMap()

	for _, aggr := range aggregates {
		if aggr.AggrTyp == NON_DISTINCT {
			continue
		}
		ret._totalChildCount += len(aggr.Children)
	}

	return ret
}

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

type HashAggr struct {
	_types                  []LType
	_groupedAggrData        *GroupedAggrData
	_groupingSets           []GroupingSet
	_groupings              []*HashAggrGroupingData
	_distinctCollectionInfo *DistinctAggrCollectionInfo
	_inputGroupTypes        []LType
	_nonDistinctFilter      []int
	_distinctFilter         []int
}

func NewHashAggr(
	types []LType,
	aggrExprs []*Expr,
	groups []*Expr,
	groupingSets []GroupingSet,
	groupingFuncs [][]int,
) *HashAggr {
	ha := &HashAggr{}
	ha._types = types
	ha._groupingSets = groupingSets

	//prepare grouping sets
	if len(ha._groupingSets) == 0 {
		set := make(GroupingSet)
		for i := 0; i < len(groups); i++ {
			set.insert(i)
		}
		ha._groupingSets = append(ha._groupingSets, set)
	}

	//prepare input group types
	ha._inputGroupTypes = createGroupChunkTypes(groups)

	//prepare grouped aggr data
	ha._groupedAggrData = &GroupedAggrData{}
	ha._groupedAggrData.InitGroupby(groups, aggrExprs, groupingFuncs)

	//prepare distinct or non-distinct filter
	for i, aggr := range ha._groupedAggrData._aggregates {
		if aggr.AggrTyp == DISTINCT {
			ha._distinctFilter = append(ha._distinctFilter, i)
		} else if aggr.AggrTyp == NON_DISTINCT {
			ha._nonDistinctFilter = append(ha._nonDistinctFilter, i)
		}
	}

	ha._distinctCollectionInfo = CreateDistinctAggrCollectionInfo(ha._groupedAggrData._aggregates)

	for i := 0; i < len(ha._groupingSets); i++ {
		ha._groupings = append(ha._groupings,
			NewHashAggrGroupingData(
				ha._groupingSets[i],
				ha._groupedAggrData,
				ha._distinctCollectionInfo))
	}

	return ha
}

// TODO: add project on aggregate
func createGroupChunkTypes(groups []*Expr) []LType {
	if len(groups) == 0 {
		return nil
	}
	groupIndices := make(IntSet)
	for _, group := range groups {
		groupIndices.insert(int(group.ColRef.column()))
	}
	maxIdx := groupIndices.max()
	assertFunc(maxIdx >= 0)
	types := make([]LType, maxIdx+1)
	for i := 0; i < len(types); i++ {
		types[i] = null()
	}
	for _, group := range groups {
		types[group.ColRef.column()] = group.DataTyp.LTyp
	}
	return types
}

func (haggr *HashAggr) Sink(chunk *Chunk) {
	if haggr._distinctCollectionInfo != nil {
		panic("usp")
	}
	payload := &Chunk{}
	payload.init(haggr._groupedAggrData._payloadTypes, defaultVectorSize)
	offset := len(haggr._groupedAggrData._groupTypes)
	for i := 0; i < len(haggr._groupedAggrData._payloadTypes); i++ {
		payload._data[i].reference(chunk._data[offset+i])
	}
	payload.setCard(chunk.card())
	for _, grouping := range haggr._groupings {
		grouping._tableData.Sink(chunk, payload, haggr._nonDistinctFilter)
	}
}

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

func NewAggrHTAppendState() *AggrHTAppendState {
	ret := new(AggrHTAppendState)
	ret._htOffsets = NewFlatVector(bigint(), defaultVectorSize)
	ret._hashSalts = NewFlatVector(smallint(), defaultVectorSize)
	ret._groupCompareVector = NewSelectVector(defaultVectorSize)
	ret._noMatchVector = NewSelectVector(defaultVectorSize)
	ret._emptyVector = NewSelectVector(defaultVectorSize)
	ret._newGroups = NewSelectVector(defaultVectorSize)
	ret._addresses = NewVector(pointerType(), defaultVectorSize)
	ret._groupChunk = &Chunk{}
	return ret
}

type RadixPartitionedHashTable struct {
	_groupingSet     GroupingSet
	_nullGroups      []int
	_groupedAggrData *GroupedAggrData
	_groupTypes      []LType
	_radixLimit      int
	_groupingValues  []*Value
	_finalizedHT     *GroupedAggrHashTable
}

func NewRadixPartitionedHashTable(
	groupingSet GroupingSet,
	aggrData *GroupedAggrData,
) *RadixPartitionedHashTable {
	ret := new(RadixPartitionedHashTable)
	ret._groupingSet = groupingSet
	ret._groupedAggrData = aggrData

	for i := 0; i < aggrData.GroupCount(); i++ {
		if !ret._groupingSet.find(i) {
			ret._nullGroups = append(ret._nullGroups, i)
		}
	}

	ret._radixLimit = 10000

	if ret._groupingSet.empty() {
		ret._groupTypes = append(ret._groupTypes, tinyint())
	}

	for ent := range ret._groupingSet.ordered() {
		assertFunc(ent < len(ret._groupedAggrData._groupTypes))
		ret._groupTypes = append(ret._groupTypes,
			ret._groupedAggrData._groupTypes[ent])
	}
	ret.SetGroupingValues()
	return ret
}

func (rpht *RadixPartitionedHashTable) SetGroupingValues() {
	groupFuncs := rpht._groupedAggrData._groupingFuncs
	for _, group := range groupFuncs {
		assertFunc(len(group) < 64)
		for i, gval := range group {
			groupingValue := int64(0)
			if !rpht._groupingSet.find(gval) {
				//do not group on this value
				groupingValue += 1 << (len(group) - (i + 1))
			}
			rpht._groupingValues = append(rpht._groupingValues,
				&Value{
					_typ: bigint(),
					_i64: groupingValue,
				})
		}

	}
}

func (rpht *RadixPartitionedHashTable) Sink(chunk, payload *Chunk, filter []int) {
	if rpht._finalizedHT == nil {
		//prepare aggr objs
		aggrObjs := CreateAggrObjects(rpht._groupedAggrData._bindings)

		rpht._finalizedHT = NewGroupedAggrHashTable(
			rpht._groupTypes,
			rpht._groupedAggrData._payloadTypes,
			aggrObjs,
			2*defaultVectorSize,
		)
	}
	groupChunk := &Chunk{}
	groupChunk.init(rpht._groupTypes, defaultVectorSize)
	for i, idx := range rpht._groupingSet.ordered() {
		groupChunk._data[i].reference(chunk._data[idx])
	}
	groupChunk.setCard(chunk.card())
	state := NewAggrHTAppendState()
	rpht._finalizedHT.AddChunk2(
		state,
		groupChunk,
		payload,
		filter,
	)
}

type GroupedAggrHashTable struct {
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
	NON_DISTINCT AggrType = iota
	DISTINCT
)

type AggrObject struct {
	_func        *AggrFunc
	_childCount  int
	_payloadSize int
	_aggrType    AggrType
	_retType     PhyType
}

func NewAggrObject(aggr *Expr) *AggrObject {
	assertFunc(aggr.SubTyp == ET_SubFunc)
	ret := new(AggrObject)
	ret._func = nil //TODO: fix me
	ret._childCount = len(aggr.Children)
	ret._payloadSize = 0 //TOOD: fix me
	ret._aggrType = aggr.AggrTyp
	ret._retType = aggr.DataTyp.LTyp.getInternalType()
	return ret
}

func CreateAggrObjects(aggregates []*Expr) []*AggrObject {
	ret := make([]*AggrObject, 0)
	for _, aggr := range aggregates {
		ret = append(ret, NewAggrObject(aggr))
	}
	return ret
}

type AggrInputData struct {
}

type aggrStateSize func() int
type aggrInit func(pointer unsafe.Pointer)
type aggrUpdate func([]*Vector, *AggrInputData, int, *Vector, int)
type aggrCombine func(*Vector, *Vector, *AggrInputData, int)
type aggrFinalize func(*Vector, *AggrInputData, *Vector, int, int)
type aggrFunction func(*AggrFunc, []*Expr)
type aggrSimpleUpdate func([]*Vector, *AggrInputData, int, unsafe.Pointer, int)
type aggrWindow func([]*Vector, *Bitmap, *AggrInputData)

type FuncNullHandling int

const (
	DEFAULT_NULL_HANDLING FuncNullHandling = 0
	SPECIAL_HANDLING                       = 1
)

type AggrFunc struct {
	_args         []LType
	_retType      LType
	_stateSize    aggrStateSize
	_init         aggrInit
	_update       aggrUpdate
	_combine      aggrCombine
	_finalize     aggrFinalize
	_func         aggrFunction
	_simpleUpdate aggrSimpleUpdate
	_window       aggrWindow
	_nullHandling FuncNullHandling
}

func NewGroupedAggrHashTable(
	groupTypes []LType,
	payloadTypes []LType,
	aggrObjs []*AggrObject,
	initCap int,
) *GroupedAggrHashTable {
	ret := new(GroupedAggrHashTable)
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

func (aht *GroupedAggrHashTable) AddChunk2(
	state *AggrHTAppendState,
	groups *Chunk,
	payload *Chunk,
	filter []int,
) int {
	hashes := NewFlatVector(hashType(), defaultVectorSize)
	groups.Hash(hashes)

	return aht.AddChunk(
		state,
		groups,
		hashes,
		payload,
		filter,
	)
}

func (aht *GroupedAggrHashTable) AddChunk(
	state *AggrHTAppendState,
	groups *Chunk,
	groupHashes *Vector,
	payload *Chunk,
	filter []int,
) int {
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

	filterIdx := 0
	payloadIdx := 0
	for i, aggr := range aht._layout._aggregates {
		if filterIdx >= len(filter) || i < filter[filterIdx] {
			payloadIdx += aggr._childCount
			AddInPlace(state._addresses, int64(aggr._payloadSize), payload.card())
			continue
		}
		assertFunc(i == filter[filterIdx])
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

func (aht *GroupedAggrHashTable) FindOrCreateGroups(
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
		state._groupChunk.init(aht._layout.types(), defaultVectorSize)
	}

	assertFunc(state._groupChunk.columnCount() ==
		len(aht._layout.types()))

	for i := 0; i < groups.columnCount(); i++ {
		state._groupChunk._data[i].reference(groups._data[i])
	}

	state._groupChunk._data[groups.columnCount()].reference(groupHashes)
	state._groupChunk.setCard(groups.card())

	if state._chunkState == nil {
		state._chunkState = NewTuplePart(aht._layout.columnCount())
	}

	toUnifiedFormat(state._chunkState, state._groupChunk)

	if state._groupData == nil {
		state._groupData = make([]*UnifiedFormat, state._groupChunk.columnCount())
		for i := 0; i < state._groupChunk.columnCount(); i++ {
			state._groupData[i] = &UnifiedFormat{}
		}
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

func (aht *GroupedAggrHashTable) Resize(size int) {
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

func (aht *GroupedAggrHashTable) Count() int {
	return aht._dataCollection.Count()
}

func (aht *GroupedAggrHashTable) ResizeThreshold() int {
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