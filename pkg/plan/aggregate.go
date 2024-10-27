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
	"sort"
	"unsafe"

	"github.com/daviszhen/plan/pkg/storage"
	"github.com/daviszhen/plan/pkg/util"
)

type IntSet map[int]struct{}

func (is IntSet) insert(id int) {
	is[id] = struct{}{}
}

func (is IntSet) max() int {
	maxKey := math.MinInt
	for key := range is {
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

func (gs GroupingSet) count() int {
	return len(gs)
}

type GroupedAggrData struct {
	_groups              []*Expr
	_groupingFuncs       [][]int //GROUPING functions
	_groupTypes          []LType
	_aggregates          []*Expr
	_payloadTypes        []LType
	_paramExprs          []*Expr //param exprs of the aggr function
	_aggrReturnTypes     []LType
	_bindings            []*Expr //pointer to aggregates
	_childrenOutputTypes []LType
	_refChildrenOutput   []*Expr
}

func (gad *GroupedAggrData) GroupCount() int {
	return len(gad._groups)
}

func (gad *GroupedAggrData) InitGroupby(
	groups []*Expr,
	exprs []*Expr,
	groupingFuncs [][]int,
	refChildrenOutput []*Expr,
) {
	gad.InitGroupbyGroups(groups)
	gad.SetGroupingFuncs(groupingFuncs)
	gad.InitChildrenOutput(refChildrenOutput)

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
	rawInputTypes []LType,
) {
	gad.InitDistinctGroups(groups)
	gad._aggrReturnTypes = append(gad._aggrReturnTypes, aggr.DataTyp.LTyp)
	for _, child := range aggr.Children {
		gad._groupTypes = append(gad._groupTypes, child.DataTyp.LTyp)
		gad._groups = append(gad._groups, child.copy())
		gad._payloadTypes = append(gad._payloadTypes, child.DataTyp.LTyp)
	}
	gad._childrenOutputTypes = rawInputTypes
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

func (gad *GroupedAggrData) InitChildrenOutput(outputs []*Expr) {
	for _, output := range outputs {
		gad._childrenOutputTypes = append(gad._childrenOutputTypes, output.DataTyp.LTyp)
	}
	gad._refChildrenOutput = outputs
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
		ret._distinctData = NewDistinctAggrData(info, groupingSet, aggrData._groups, aggrData._childrenOutputTypes)
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
	rawInputTypes []LType,
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
		for group := range groups {
			groupingSet.insert(group)
		}

		groupBySize := len(groupExprs)
		for gIdx := 0; gIdx < len(aggr.Children); gIdx++ {
			groupingSet.insert(gIdx + groupBySize)
		}

		//create hash table
		ret._groupedAggrData[tableIdx] = &GroupedAggrData{}
		ret._groupedAggrData[tableIdx].InitDistinct(aggr, groupExprs, rawInputTypes)
		ret._radixTables[tableIdx] = NewRadixPartitionedHashTable(groupingSet, ret._groupedAggrData[tableIdx])
	}

	return ret
}

type DistinctAggrCollectionInfo struct {
	_indices    []int //distinct aggr indice
	_tableCount int
	//_tableIndices    []int
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

type HashAggrScanState struct {
	_radixIdx     int
	_state        *TupleDataScanState
	_childCnt     int
	_childCnt2    int
	_outputCnt    int
	_filteredCnt1 int
	_filteredCnt2 int
	_childCnt3    int
}

func NewHashAggrScanState() *HashAggrScanState {
	return &HashAggrScanState{
		_state: &TupleDataScanState{},
	}
}

type HashAggrState int

const (
	HAS_INIT HashAggrState = iota
	HAS_BUILD
	HAS_SCAN
)

type HashAggr struct {
	_has                    HashAggrState
	_types                  []LType
	_groupedAggrData        *GroupedAggrData
	_groupingSets           []GroupingSet
	_groupings              []*HashAggrGroupingData
	_distinctCollectionInfo *DistinctAggrCollectionInfo
	_inputGroupTypes        []LType
	_nonDistinctFilter      []int
	_distinctFilter         []int
	_printHash              bool
}

func NewHashAggr(
	types []LType,
	aggrExprs []*Expr,
	groups []*Expr,
	groupingSets []GroupingSet,
	groupingFuncs [][]int,
	refChildrenOutput []*Expr,
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
	ha._groupedAggrData.InitGroupby(groups, aggrExprs, groupingFuncs, refChildrenOutput)

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

	childrenOutput := &Chunk{}
	childrenOutput.init(haggr._groupedAggrData._childrenOutputTypes, defaultVectorSize)
	offset = offset + len(haggr._groupedAggrData._payloadTypes)
	for i := 0; i < len(haggr._groupedAggrData._childrenOutputTypes); i++ {
		childrenOutput._data[i].reference(chunk._data[offset+i])
	}
	childrenOutput.setCard(chunk.card())

	for _, grouping := range haggr._groupings {
		grouping._tableData._printHash = haggr._printHash
		grouping._tableData.Sink(chunk, payload, childrenOutput, haggr._nonDistinctFilter)
	}
}

func (haggr *HashAggr) FetechAggregates(state *HashAggrScanState, groups, output *Chunk) OperatorResult {
	//1. table_data.GetData
	for {
		if state._radixIdx >= len(haggr._groupings) {
			break
		}
		grouping := haggr._groupings[state._radixIdx]
		radixTable := grouping._tableData
		radixTable.FetchAggregates(groups, output)
		if output.card() != 0 {
			return haveMoreOutput
		}
		state._radixIdx++
		if state._radixIdx >= len(haggr._groupings) {
			break
		}
		state._state = &TupleDataScanState{}
	}

	if output.card() == 0 {
		return Done
	} else {
		return haveMoreOutput
	}
}

func (haggr *HashAggr) GetData(state *HashAggrScanState, output, rawInput *Chunk) OperatorResult {
	//1. table_data.GetData
	for {
		if state._radixIdx >= len(haggr._groupings) {
			break
		}
		grouping := haggr._groupings[state._radixIdx]
		radixTable := grouping._tableData
		radixTable.GetData(state._state, output, rawInput)
		if output.card() != 0 {
			return haveMoreOutput
		}
		state._radixIdx++
		if state._radixIdx >= len(haggr._groupings) {
			break
		}
		state._state = &TupleDataScanState{}
	}

	//2. run filter
	if output.card() == 0 {
		return Done
	} else {
		return haveMoreOutput
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

type AggrHTAppendState struct {
	_htOffsets          *Vector
	_hashSalts          *Vector
	_groupCompareVector *SelectVector
	_noMatchVector      *SelectVector
	_emptyVector        *SelectVector
	_newGroups          *SelectVector
	_addresses          *Vector
	//unified format of Chunk
	_groupData []*UnifiedFormat
	//Chunk
	_groupChunk *Chunk
	_chunkState *TupleDataChunkState
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
	_printHash       bool
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

func (rpht *RadixPartitionedHashTable) Sink(chunk, payload, childrenOutput *Chunk, filter []int) {
	if rpht._finalizedHT == nil {
		fmt.Println("init aggregate finalize ht")
		//prepare aggr objs
		aggrObjs := CreateAggrObjects(rpht._groupedAggrData._bindings)

		rpht._finalizedHT = NewGroupedAggrHashTable(
			rpht._groupTypes,
			rpht._groupedAggrData._payloadTypes,
			rpht._groupedAggrData._childrenOutputTypes,
			aggrObjs,
			2*defaultVectorSize,
			storage.GBufferMgr,
		)
		rpht._finalizedHT._printHash = rpht._printHash
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
		childrenOutput,
		filter,
	)
}

func (rpht *RadixPartitionedHashTable) FetchAggregates(groups, result *Chunk) {
	groupChunk := &Chunk{}
	groupChunk.init(rpht._groupTypes, defaultVectorSize)
	for i, idx := range rpht._groupingSet.ordered() {
		groupChunk._data[i].reference(groups._data[idx])
	}
	groupChunk.setCard(groups.card())
	rpht._finalizedHT.FetchAggregates(groups, result)
}

func (rpht *RadixPartitionedHashTable) GetData(state *TupleDataScanState, output, childrenOutput *Chunk) OperatorResult {
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

	scanTyps := make([]LType, 0)
	//FIXME:
	//groupby types + children output types +aggr return types
	scanTyps = append(scanTyps, rpht._groupTypes...)
	scanTyps = append(scanTyps, rpht._groupedAggrData._childrenOutputTypes...)
	scanTyps = append(scanTyps, rpht._groupedAggrData._aggrReturnTypes...)
	scanChunk := &Chunk{}
	scanChunk.init(scanTyps, defaultVectorSize)
	cnt := rpht._finalizedHT.Scan(state, scanChunk)
	output.setCard(cnt)

	for i, ent := range rpht._groupingSet.ordered() {
		output._data[ent].reference(scanChunk._data[i])
	}

	for ent := range rpht._nullGroups {
		output._data[ent].setPhyFormat(PF_CONST)
		setNullInPhyFormatConst(output._data[ent], true)
	}

	assertFunc(rpht._groupingSet.count()+len(rpht._nullGroups) == rpht._groupedAggrData.GroupCount())
	for i := 0; i < len(rpht._groupedAggrData._aggregates); i++ {
		output._data[rpht._groupedAggrData.GroupCount()+i].reference(scanChunk._data[len(rpht._groupTypes)+len(rpht._groupedAggrData._childrenOutputTypes)+i])
	}

	assertFunc(len(rpht._groupedAggrData._groupingFuncs) == len(rpht._groupingValues))
	for i := 0; i < len(rpht._groupedAggrData._groupingFuncs); i++ {
		output._data[rpht._groupedAggrData.GroupCount()+len(rpht._groupedAggrData._aggregates)+i].referenceValue(rpht._groupingValues[i])
	}

	//split the children output chunk from the scan chunk
	for i := 0; i < len(rpht._groupedAggrData._childrenOutputTypes); i++ {
		childrenOutput._data[i].reference(scanChunk._data[len(rpht._groupTypes)+i])
	}
	childrenOutput.setCard(cnt)

	if output.card() == 0 {
		return Done
	} else {
		return haveMoreOutput
	}
}

type TupleDataScanState struct {
	_colIds []int
	//_rowLocs *Vector
	_init bool

	_pinState   TupleDataPinState
	_chunkState TupleDataChunkState
	_segmentIdx int
	_chunkIdx   int
}

func NewTupleDataScanState(colCnt int, prop TupleDataPinProperties) *TupleDataScanState {
	ret := &TupleDataScanState{}
	ret._pinState._properties = prop
	return ret
}

type GroupedAggrHashTable struct {
	_layout       *TupleDataLayout
	_payloadTypes []LType

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
	_predicates      []ET_SubTyp
	_printHash       bool
	_bufMgr          *storage.BufferManager
}

type AggrType int

const (
	NON_DISTINCT AggrType = iota
	DISTINCT
)

type AggrObject struct {
	_name        string
	_func        *AggrFunc
	_childCount  int
	_payloadSize int
	_aggrType    AggrType
	_retType     PhyType
}

func NewAggrObject(aggr *Expr) *AggrObject {
	assertFunc(aggr.SubTyp == ET_SubFunc)
	ret := new(AggrObject)
	ret._childCount = len(aggr.Children)
	ret._aggrType = aggr.AggrTyp
	ret._retType = aggr.DataTyp.LTyp.getInternalType()
	ret._name = aggr.Svalue
	switch aggr.Svalue {
	case "sum":
		ret._func = GetSumAggr(aggr.DataTyp.LTyp.getInternalType())
		ret._payloadSize = ret._func._stateSize()
	case "avg":
		assertFunc(len(aggr.Children) != 0)
		ret._func = GetAvgAggr(aggr.DataTyp.LTyp.getInternalType(), aggr.Children[0].DataTyp.LTyp.getInternalType())
		ret._payloadSize = ret._func._stateSize()
	case "count":
		assertFunc(len(aggr.Children) != 0)
		ret._func = GetCountAggr(aggr.DataTyp.LTyp.getInternalType(), aggr.Children[0].DataTyp.LTyp.getInternalType())
		ret._payloadSize = ret._func._stateSize()
	case "max":
		assertFunc(len(aggr.Children) != 0)
		ret._func = GetMaxAggr(aggr.DataTyp.LTyp.getInternalType(), aggr.Children[0].DataTyp.LTyp.getInternalType())
		ret._payloadSize = ret._func._stateSize()
	case "min":
		assertFunc(len(aggr.Children) != 0)
		ret._func = GetMinAggr(aggr.DataTyp.LTyp.getInternalType(), aggr.Children[0].DataTyp.LTyp.getInternalType())
		ret._payloadSize = ret._func._stateSize()
	default:
		panic("usp")
	}

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

func NewAggrInputData() *AggrInputData {
	return &AggrInputData{}
}

type AggrUnaryInput struct {
	_input     *AggrInputData
	_inputMask *Bitmap
	_inputIdx  int
}

func NewAggrUnaryInput(input *AggrInputData, mask *Bitmap) *AggrUnaryInput {
	return &AggrUnaryInput{
		_input:     input,
		_inputMask: mask,
		_inputIdx:  0,
	}
}

type AggrFinalizeData struct {
	_result    *Vector
	_input     *AggrInputData
	_resultIdx int
}

func NewAggrFinalizeData(result *Vector, input *AggrInputData) *AggrFinalizeData {
	return &AggrFinalizeData{
		_result: result,
		_input:  input,
	}
}

func (data *AggrFinalizeData) ReturnNull() {
	switch data._result.phyFormat() {
	case PF_FLAT:
		setNullInPhyFormatFlat(data._result, uint64(data._resultIdx), true)
	case PF_CONST:
		setNullInPhyFormatConst(data._result, true)
	default:
		panic("usp")
	}
}

type aggrStateSize func() int
type aggrInit func(pointer unsafe.Pointer)
type aggrUpdate func([]*Vector, *AggrInputData, int, *Vector, int)
type aggrCombine func(*Vector, *Vector, *AggrInputData, int)
type aggrFinalize func(*Vector, *AggrInputData, *Vector, int, int)

// type aggrFunction func(*AggrFunc, []*Expr)
type aggrSimpleUpdate func([]*Vector, *AggrInputData, int, unsafe.Pointer, int)

//type aggrWindow func([]*Vector, *Bitmap, *AggrInputData)

type FuncNullHandling int

const (
	DEFAULT_NULL_HANDLING FuncNullHandling = 0
	SPECIAL_HANDLING      FuncNullHandling = 1
)

type AggrFunc struct {
	_args      []LType
	_retType   LType
	_stateSize aggrStateSize
	_init      aggrInit
	_update    aggrUpdate
	_combine   aggrCombine
	_finalize  aggrFinalize
	//_func         aggrFunction
	_simpleUpdate aggrSimpleUpdate
	//_window       aggrWindow
	_nullHandling FuncNullHandling
}

func NewGroupedAggrHashTable(
	groupTypes []LType,
	payloadTypes []LType,
	childrenOutputTypes []LType,
	aggrObjs []*AggrObject,
	initCap int,
	bufMgr *storage.BufferManager,
) *GroupedAggrHashTable {
	ret := new(GroupedAggrHashTable)
	ret._bufMgr = bufMgr
	//hash column in the end of tuple
	groupTypes = append(groupTypes, hashType())
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
	groups *Chunk,
	payload *Chunk,
	childrenOutput *Chunk,
	filter []int,
) int {
	hashes := NewFlatVector(hashType(), defaultVectorSize)
	groups.Hash(hashes)

	//	hashes.print(groups.card())
	//}
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
	groups *Chunk,
	groupHashes *Vector,
	payload *Chunk,
	childrenOutput *Chunk,
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
		state._newGroups,
		childrenOutput,
	)
	AddInPlace(state._addresses, int64(aht._layout.aggrOffset()), payload.card())
	//fmt.Println("address", "tcnt", groups.card(), "new group count", newGroupCount, "equal", groups.card() == newGroupCount)
	//state._addresses.print(groups.card())

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
		filterIdx++
	}
	return newGroupCount
}

func (aht *GroupedAggrHashTable) FindOrCreateGroups(
	state *AggrHTAppendState,
	groups *Chunk,
	groupHashes *Vector,
	addresses *Vector,
	newGroupsOut *SelectVector,
	childrenOutput *Chunk,
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

	//if state._chunkState == nil {
	//prepare data structure holding incoming Chunk
	state._chunkState = NewTupleDataChunkState(aht._layout.columnCount(), aht._layout.childrenOutputCount())
	//}

	//groupChunk converted into chunkstate.data unified format
	toUnifiedFormat(state._chunkState, state._groupChunk)
	toUnifiedFormatForChildrenOutput(state._chunkState, childrenOutput)

	if state._groupData == nil {
		state._groupData = make([]*UnifiedFormat, state._groupChunk.columnCount())
		for i := 0; i < state._groupChunk.columnCount(); i++ {
			state._groupData[i] = &UnifiedFormat{}
		}
	}

	//group data refers to the chunk state.data unified format
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
			htEntry := &htEntrySlice[htOffsetsPtr[idx]]
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
				incrSelectVectorInPhyFormatFlat(),
				newEntryCount)

			//newly created block
			var blockId int
			if !empty(aht._payloadHdsPtrs) {
				blockId = size(aht._payloadHdsPtrs) - 1
			}
			aht.UpdateBlockPointers()
			blockPtr := aht._payloadHdsPtrs[blockId]
			blockEnd := util.PointerAdd(blockPtr, aht._tuplesPerBlock*aht._tupleSize)

			//update htEntry & save address
			rowLocations := getSliceInPhyFormatFlat[unsafe.Pointer](state._chunkState._rowLocations)
			for j := 0; j < newEntryCount; j++ {
				rowLoc := rowLocations[j]
				if pointerLess(blockEnd, rowLoc) ||
					pointerLess(rowLoc, blockPtr) {
					blockId++
					assertFunc(blockId < size(aht._payloadHdsPtrs))
					blockPtr = aht._payloadHdsPtrs[blockId]
					blockEnd = util.PointerAdd(blockPtr, aht._tuplesPerBlock*aht._tupleSize)
				}
				assertFunc(
					pointerLessEqual(blockPtr, rowLoc) &&
						pointerLess(rowLoc, blockEnd))
				assertFunc(pointerSub(rowLoc, blockPtr)%int64(aht._tupleSize) == 0)
				idx := state._emptyVector.getIndex(j)
				htEntry := &htEntrySlice[htOffsetsPtr[idx]]
				htEntry._pageNr = uint32(blockId + 1)
				htEntry._pageOffset = uint16(pointerSub(rowLoc, blockPtr) / int64(aht._tupleSize))
				addresessSlice[idx] = rowLoc
				//fmt.Println("new rowloc", rowLoc)
			}
		}

		if needCompareCount > 0 {
			//get address
			for j := 0; j < needCompareCount; j++ {
				idx := state._groupCompareVector.getIndex(j)
				htEntry := &htEntrySlice[htOffsetsPtr[idx]]
				pagePtr := aht._payloadHdsPtrs[htEntry._pageNr-1]
				pageOffset := int(htEntry._pageOffset) * aht._tupleSize
				addresessSlice[idx] = pointerAdd(pagePtr, pageOffset)
				//fmt.Println("old rowloc", addresessSlice[idx])
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

func (aht *GroupedAggrHashTable) FetchAggregates(groups, result *Chunk) {
	assertFunc(groups.columnCount()+1 == aht._layout.columnCount())
	for i := 0; i < result.columnCount(); i++ {
		assertFunc(result._data[i].typ().id == aht._payloadTypes[i].id)
	}
	result.setCard(groups.card())
	if groups.card() == 0 {
		return
	}

	appendState := NewAggrHTAppendState()
	addresses := NewVector(pointerType(), defaultVectorSize)
	hashes := NewFlatVector(hashType(), defaultVectorSize)
	groups.Hash(hashes)
	if aht._printHash {
		println("scan hash")
		hashes.print(groups.card())
		groups.print()
	}

	newGroupCnt := aht.FindOrCreateGroups(appendState, groups, hashes, addresses, appendState._newGroups, nil)
	assertFunc(newGroupCnt == 0)

	//fetch the agg
	FinalizeStates(aht._layout, addresses, result, 0)
	if aht._printHash {
		fmt.Println("scan result")
		result.print()
	}
}

func (aht *GroupedAggrHashTable) Scan(state *TupleDataScanState, result *Chunk) int {
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

	return result.card()
}

func (aht *GroupedAggrHashTable) Resize(size int) {
	assertFunc(!aht._finalized)
	assertFunc(size >= defaultVectorSize)
	assertFunc(isPowerOfTwo(uint64(size)))
	assertFunc(size >= aht._capacity)

	//fmt.Println("resize from ", aht._capacity, "to", size)
	aht._capacity = size
	aht._bitmask = uint64(aht._capacity - 1)
	byteSize := aht._capacity * aggrEntrySize
	if byteSize > BLOCK_SIZE {
		aht._hashesHdl = aht._bufMgr.Allocate(uint64(byteSize), true, nil)
		aht._hashesHdlPtr = aht._hashesHdl.Ptr()
	}
	hashesArr := pointerToSlice[aggrHTEntry](aht._hashesHdlPtr, aht._capacity)
	for i := 0; i < len(hashesArr); i++ {
		hashesArr[i].clean()
	}
	if aht.Count() != 0 {
		assertFunc(!empty(aht._payloadHdsPtrs))
		aht._dataCollection.checkDupAll()
		blockId := 0
		blockPtr := aht._payloadHdsPtrs[blockId]
		blockEnt := pointerAdd(blockPtr, aht._tuplesPerBlock*aht._tupleSize)

		iter := NewTupleDataChunkIterator2(
			aht._dataCollection,
			PIN_PRRP_KEEP_PINNED,
			false,
		)

		for {
			rowLocs := iter.GetRowLocations()
			for i := 0; i < iter.GetCurrentChunkCount(); i++ {
				rowLoc := rowLocs[i]
				if pointerLess(blockEnt, rowLoc) ||
					pointerLess(rowLoc, blockPtr) {
					blockId++
					assertFunc(blockId < len(aht._payloadHdsPtrs))
					blockPtr = aht._payloadHdsPtrs[blockId]
					blockEnt = pointerAdd(blockPtr, aht._tuplesPerBlock*aht._tupleSize)
				}

				assertFunc(
					pointerLessEqual(blockPtr, rowLoc) &&
						pointerLess(rowLoc, blockEnt))
				assertFunc(pointerSub(rowLoc, blockPtr)%int64(aht._tupleSize) == 0)

				hash := load[uint64](pointerAdd(rowLoc, aht._hashOffset))
				assertFunc((hash & aht._bitmask) == (hash % uint64(aht._capacity)))
				assertFunc((hash >> aht._hashPrefixShift) <= math.MaxUint16)
				entIdx := hash & aht._bitmask
				for hashesArr[entIdx]._pageNr > 0 {
					entIdx++
					if entIdx >= uint64(aht._capacity) {
						entIdx = 0
					}
				}
				htEnt := &hashesArr[entIdx]
				assertFunc(htEnt._pageNr == 0)
				htEnt._salt = uint16(hash >> aht._hashPrefixShift)
				htEnt._pageNr = uint32(1 + blockId)
				htEnt._pageOffset = uint16(pointerSub(rowLoc, blockPtr) / int64(aht._tupleSize))
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
	hashesArr := pointerToSlice[aggrHTEntry](aht._hashesHdlPtr, aht._capacity)
	count := 0
	for i := 0; i < aht._capacity; i++ {
		hEnt := hashesArr[i]
		if hEnt._pageNr > 0 {
			assertFunc(int(hEnt._pageOffset) < aht._tuplesPerBlock)
			assertFunc(int(hEnt._pageNr) <= size(aht._payloadHdsPtrs))
			ptr := pointerAdd(
				aht._payloadHdsPtrs[hEnt._pageNr-1],
				int(hEnt._pageOffset)*aht._tupleSize)
			hash := load[uint64](pointerAdd(ptr, aht._hashOffset))
			assertFunc(uint64(hEnt._salt) == (hash >> aht._hashPrefixShift))
			count++
		}
	}
	assertFunc(count == aht.Count())
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
			id > uint32(size(aht._payloadHdsPtrs))-1 {
			need := id - uint32(size(aht._payloadHdsPtrs)) + 1
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
	addresses *Vector,
	sel *SelectVector,
	cnt int,
) {
	if cnt == 0 {
		return
	}

	pointers := getSliceInPhyFormatFlat[unsafe.Pointer](addresses)
	offsets := layout.offsets()
	aggrIdx := layout.aggrIdx()

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
