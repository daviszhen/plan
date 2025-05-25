package compute

import (
	"fmt"
	"unsafe"

	"github.com/daviszhen/plan/pkg/chunk"
	"github.com/daviszhen/plan/pkg/common"
	"github.com/daviszhen/plan/pkg/storage"
	"github.com/daviszhen/plan/pkg/util"
)

type HashAggr struct {
	_has                    HashAggrState
	_types                  []common.LType
	_groupedAggrData        *GroupedAggrData
	_groupingSets           []GroupingSet
	_groupings              []*HashAggrGroupingData
	_distinctCollectionInfo *DistinctAggrCollectionInfo
	_inputGroupTypes        []common.LType
	_nonDistinctFilter      []int
	_distinctFilter         []int
	_printHash              bool
}

func NewHashAggr(
	types []common.LType,
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
