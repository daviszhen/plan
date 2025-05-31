package compute

import (
	"math"
	"sort"

	"github.com/daviszhen/plan/pkg/common"
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
	_groupTypes          []common.LType
	_aggregates          []*Expr
	_payloadTypes        []common.LType
	_paramExprs          []*Expr //param exprs of the aggr function
	_aggrReturnTypes     []common.LType
	_bindings            []*Expr //pointer to aggregates
	_childrenOutputTypes []common.LType
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
		gad._aggrReturnTypes = append(gad._aggrReturnTypes, aggr.DataTyp)
		for _, child := range aggr.Children {
			gad._payloadTypes = append(gad._payloadTypes, child.DataTyp)
			gad._paramExprs = append(gad._paramExprs, child)
		}
		gad._aggregates = append(gad._aggregates, aggr)
	}
}

func (gad *GroupedAggrData) InitDistinct(
	aggr *Expr,
	groups []*Expr,
	rawInputTypes []common.LType,
) {
	gad.InitDistinctGroups(groups)
	gad._aggrReturnTypes = append(gad._aggrReturnTypes, aggr.DataTyp)
	for _, child := range aggr.Children {
		gad._groupTypes = append(gad._groupTypes, child.DataTyp)
		gad._groups = append(gad._groups, child.copy())
		gad._payloadTypes = append(gad._payloadTypes, child.DataTyp)
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
		gad._groupTypes = append(gad._groupTypes, group.DataTyp)
		gad._groups = append(gad._groups, group.copy())
	}
}

func (gad *GroupedAggrData) SetGroupingFuncs(funcs [][]int) {
	gad._groupingFuncs = funcs
}

func (gad *GroupedAggrData) InitGroupbyGroups(groups []*Expr) {
	for _, g := range groups {
		gad._groupTypes = append(gad._groupTypes, g.DataTyp)
	}
	gad._groups = groups
}

func (gad *GroupedAggrData) InitChildrenOutput(outputs []*Expr) {
	for _, output := range outputs {
		gad._childrenOutputTypes = append(gad._childrenOutputTypes, output.DataTyp)
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

func (dad *DistinctAggrData) IsDistinct(index int) bool {
	if !util.Empty(dad._radixTables) {
		if _, has := dad._info._tableMap[index]; has {
			return true
		}
	}
	return false
}

func NewDistinctAggrData(
	info *DistinctAggrCollectionInfo,
	groups GroupingSet,
	groupExprs []*Expr,
	rawInputTypes []common.LType,
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
		found := util.FindIf[*Expr](tableInputs, func(t *Expr) bool {
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
	util.AssertFunc(len(daci._tableMap) == len(daci._indices))
	util.AssertFunc(len(tableInputs) <= len(daci._indices))

	return len(tableInputs)
}

func NewDistinctAggrCollectionInfo(
	aggregates []*Expr,
	indices []int,
) *DistinctAggrCollectionInfo {
	ret := &DistinctAggrCollectionInfo{
		_tableMap: make(map[int]int),
	}
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

type AggrType int

const (
	NON_DISTINCT AggrType = iota
	DISTINCT
)

type AggrObject struct {
	_name        string
	_func        *FunctionV2
	_childCount  int
	_payloadSize int
	_aggrType    AggrType
	_retType     common.PhyType
}

func NewAggrObject(aggr *Expr) *AggrObject {
	util.AssertFunc(aggr.SubTyp == ET_SubFunc)
	ret := new(AggrObject)
	ret._childCount = len(aggr.Children)
	ret._aggrType = aggr.AggrTyp
	ret._retType = aggr.DataTyp.GetInternalType()
	ret._name = aggr.Svalue
	ret._func = aggr.FunImpl
	ret._payloadSize = ret._func._stateSize()
	return ret
}

func CreateAggrObjects(aggregates []*Expr) []*AggrObject {
	ret := make([]*AggrObject, 0)
	for _, aggr := range aggregates {
		ret = append(ret, NewAggrObject(aggr))
	}
	return ret
}
