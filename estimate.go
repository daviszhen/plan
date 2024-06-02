package main

import (
	"fmt"
	"math"
	"sort"
)

type RelationAttributes struct {
	originalName string
	columns      Set
	cardinality  float64
}

func NewRelationAttributes() *RelationAttributes {
	return &RelationAttributes{
		columns: make(Set),
	}
}

type ColumnBind [2]uint64
type ColumnBindSet map[ColumnBind]bool
type ColumnBindMap map[ColumnBind]ColumnBind

func NewColumnBind(a, b uint64) ColumnBind {
	return ColumnBind{a, b}
}

func (bind ColumnBind) table() uint64 {
	return bind[0]
}

func (bind ColumnBind) column() uint64 {
	return bind[1]
}

func (bind ColumnBind) less(other ColumnBind) bool {
	return bind.table() < other.table() || bind.table() == other.table() && bind.column() < other.column()
}

func (bind ColumnBind) String() string {
	return fmt.Sprintf("[%d %d]", int64(bind.table()), int64(bind.column()))
}

func (set ColumnBindSet) find(bind ColumnBind) bool {
	if _, has := set[bind]; has {
		return true
	}
	return false
}

func (set ColumnBindSet) insert(bind ColumnBind) {
	set[bind] = true
}
func (set ColumnBindSet) merge(other ColumnBindSet) {
	for bind := range other {
		set.insert(bind)
	}
}

func (set ColumnBindSet) empty() bool {
	return len(set) == 0
}

func (set ColumnBindSet) clear() {
	for key := range set {
		delete(set, key)
	}
}

func (cmap ColumnBindMap) find(bind ColumnBind) bool {
	if _, has := cmap[bind]; has {
		return true
	}
	return false
}

func (cmap ColumnBindMap) get(bind ColumnBind) ColumnBind {
	if !cmap.find(bind) {
		panic("does no exist")
	}
	return cmap[bind]
}

func (cmap ColumnBindMap) insert(key, value ColumnBind) {
	cmap[key] = value
}

type RelationToTDom struct {
	equivalentRelations ColumnBindSet
	tdomHll             uint64
	tdomNoHll           uint64
	hasTdomHll          bool
	filters             []*FilterInfo
}

func NewRelationToTDom(cset ColumnBindSet) *RelationToTDom {
	return &RelationToTDom{
		equivalentRelations: cset,
		tdomNoHll:           math.MaxUint64,
	}
}

func (dom *RelationToTDom) less(o *RelationToTDom) bool {
	if dom.hasTdomHll && o.hasTdomHll {
		return dom.tdomHll > o.tdomHll
	}
	if dom.hasTdomHll {
		return dom.tdomHll > o.tdomNoHll
	}
	if o.hasTdomHll {
		return dom.tdomNoHll > o.tdomHll
	}
	return dom.tdomNoHll > o.tdomNoHll
}

const (
	defaultSelectivity = float64(0.2)
)

type CardinalityEstimator struct {
	// relation_id -> relation attributes
	relationAttributes map[uint64]*RelationAttributes

	//(relation_id, column_id) -> (original table, original column)
	relationColumnToOriginalColumn ColumnBindMap

	relationsToTDoms []*RelationToTDom
}

func NewCardinalityEstimator() *CardinalityEstimator {
	return &CardinalityEstimator{
		relationAttributes:             make(map[uint64]*RelationAttributes),
		relationColumnToOriginalColumn: make(ColumnBindMap),
	}
}

func (est *CardinalityEstimator) InitCardinalityEstimatorProps(nodeOps []*NodeOp,
	filterInfos []*FilterInfo) error {
	est.InitEquivalentRelations(filterInfos)
	est.InitTotalDomains()
	for _, nodeOp := range nodeOps {
		joinNode := nodeOp.node
		op := nodeOp.op
		joinNode.setBaseCard(float64(op.EstimatedCard()))
		if op.Typ == LOT_JOIN {
			if op.JoinTyp == LOT_JoinTypeLeft {
				panic("usp left join here")
			}
		}
		est.EstimateBaseTableCard(joinNode, op)
		err := est.UpdateTotalDomains(joinNode, op)
		if err != nil {
			return err
		}
	}
	sort.Slice(est.relationsToTDoms, func(i, j int) bool {
		return est.relationsToTDoms[i].less(est.relationsToTDoms[j])
	})
	return nil
}

func (est *CardinalityEstimator) InitEquivalentRelations(filterInfos []*FilterInfo) {
	for _, filter := range filterInfos {
		if est.SingleColumnFilter(filter) {
			est.AddRelationTdom(filter)
			continue
		} else if est.EmptyFilter(filter) {
			continue
		}
		equivalentSet := est.DetermineMatchingEquivalentSets(filter)
		est.AddToEquivalenceSets(filter, equivalentSet)
	}
}

func (est *CardinalityEstimator) SingleColumnFilter(filterInfo *FilterInfo) bool {
	if filterInfo.leftSet != nil && filterInfo.rightSet != nil {
		return false
	}
	if est.EmptyFilter(filterInfo) {
		return false
	}
	return true
}

func (est *CardinalityEstimator) EmptyFilter(filterInfo *FilterInfo) bool {
	if filterInfo.leftSet == nil && filterInfo.rightSet == nil {
		return true
	}
	return false
}

func (est *CardinalityEstimator) AddRelationTdom(filterInfo *FilterInfo) {
	for _, dom := range est.relationsToTDoms {
		if dom.equivalentRelations.find(filterInfo.leftBinding) {
			return
		}
	}
	set := make(ColumnBindSet)
	set.insert(ColumnBind{filterInfo.leftBinding[0], filterInfo.leftBinding[1]})
	newTdom := NewRelationToTDom(set)
	est.relationsToTDoms = append(est.relationsToTDoms, newTdom)
}

func (est *CardinalityEstimator) DetermineMatchingEquivalentSets(filterInfo *FilterInfo) []uint64 {
	ret := make([]uint64, 0)
	for i, dom := range est.relationsToTDoms {
		if dom.equivalentRelations.find(filterInfo.leftBinding) {
			ret = append(ret, uint64(i))
		} else if dom.equivalentRelations.find(filterInfo.rightBinding) {
			ret = append(ret, uint64(i))
		}
	}
	return ret
}

func (est *CardinalityEstimator) AddToEquivalenceSets(filterInfo *FilterInfo, set []uint64) {
	if len(set) > 2 {
		panic("should be <= 2")
	}
	if len(set) > 1 {
		for bind := range est.relationsToTDoms[set[1]].equivalentRelations {
			est.relationsToTDoms[set[0]].equivalentRelations.insert(bind)
		}
		est.relationsToTDoms[set[1]].equivalentRelations.clear()
		est.relationsToTDoms[set[0]].filters = append(est.relationsToTDoms[set[0]].filters, filterInfo)
	} else if len(set) == 1 {
		tdom := est.relationsToTDoms[set[0]]
		tdom.equivalentRelations.insert(filterInfo.leftBinding)
		tdom.equivalentRelations.insert(filterInfo.rightBinding)
		tdom.filters = append(tdom.filters, filterInfo)
	} else if len(set) == 0 {
		cbset := make(ColumnBindSet)
		cbset.insert(filterInfo.leftBinding)
		cbset.insert(filterInfo.rightBinding)
		tdom := NewRelationToTDom(cbset)
		tdom.filters = append(tdom.filters, filterInfo)
		est.relationsToTDoms = append(est.relationsToTDoms, tdom)
	}
}

func (est *CardinalityEstimator) InitTotalDomains() {
	removed := make([]int, 0)
	for i, dom := range est.relationsToTDoms {
		if dom.equivalentRelations.empty() {
			removed = append(removed, i)
		}
	}
	for i := len(removed) - 1; i >= 0; i-- {
		est.relationsToTDoms = erase(est.relationsToTDoms, removed[i])
	}
}

func (est *CardinalityEstimator) EstimateBaseTableCard(node *JoinNode, op *LogicalOperator) {
	hasLogicalFilter := IsLogicalFilter(op)
	if node.set.count() != 1 {
		panic("should be 1")
	}
	relId := node.set.relations[0]
	lowestCardFound := node.getBaseCard()
	for col := range est.relationAttributes[relId].columns {
		cardAfterFilters := node.getBaseCard()
		key := ColumnBind{relId, col}
		var tableFilters *TableFilterSet
		if est.relationColumnToOriginalColumn.find(key) {
			actualBind := est.relationColumnToOriginalColumn.get(key)
			tableFilters = est.GetTableFilters(op, actualBind[0])
		}
		if tableFilters != nil {
			//TODO:
		}
		if hasLogicalFilter {
			cardAfterFilters = cardAfterFilters * defaultSelectivity
		}
		lowestCardFound = min(cardAfterFilters, lowestCardFound)
	}
	node.setEstimatedCard(lowestCardFound)
}

func (est *CardinalityEstimator) GetTableFilters(op *LogicalOperator, tableIndex uint64) *TableFilterSet {
	get := getLogicalGet(op, tableIndex)
	if get != nil {
		//TODO:
		return nil
	} else {
		return nil
	}
}

func (est *CardinalityEstimator) UpdateTotalDomains(node *JoinNode, op *LogicalOperator) error {
	relId := node.set.relations[0]
	est.relationAttributes[relId].cardinality = node.getCard()
	distinctCount := uint64(node.getBaseCard())
	var get *LogicalOperator
	var catalogTable *CatalogTable
	var err error
	getUpdated := true
	for col := range est.relationAttributes[relId].columns {
		key := ColumnBind{relId, col}
		if est.relationColumnToOriginalColumn.find(key) {
			actualBind := est.relationColumnToOriginalColumn.get(key)
			if get == nil || get.Index != actualBind[0] {
				get = getLogicalGet(op, actualBind[0])
				getUpdated = true
			} else {
				getUpdated = false
			}
		} else {
			getUpdated = false
		}

		if getUpdated {
			if get != nil {
				catalogTable, err = tpchCatalog().Table(get.Database, get.Table)
				if err != nil {
					return err
				}
			} else {
				catalogTable = nil
			}
		}
		if catalogTable != nil && est.relationColumnToOriginalColumn.find(key) {
			actualBind := est.relationColumnToOriginalColumn.get(key)
			baseStats := catalogTable.getStats(actualBind[1])
			if baseStats != nil {
				distinctCount = baseStats.getDistinctCount()
			}

			if distinctCount > uint64(node.getBaseCard()) {
				distinctCount = uint64(node.getBaseCard())
			}
		} else {
			distinctCount = uint64(node.getBaseCard())
		}
		for _, dom := range est.relationsToTDoms {
			if !dom.equivalentRelations.find(key) {
				continue
			}
			if catalogTable != nil {
				if dom.tdomHll < distinctCount {
					dom.tdomHll = distinctCount
					dom.hasTdomHll = true
				}
				if dom.tdomNoHll > distinctCount {
					dom.tdomNoHll = distinctCount
				}
			} else {
				if dom.tdomNoHll > distinctCount && !dom.hasTdomHll {
					dom.tdomNoHll = distinctCount
				}
			}
			break
		}
	}
	return nil
}

func (est *CardinalityEstimator) AddRelationColumnMapping(get *LogicalOperator, relId uint64) error {
	catalogTable, err := tpchCatalog().Table(get.Database, get.Table)
	if err != nil {
		return err
	}

	//TODO: refine
	for i := range catalogTable.Columns {
		key := ColumnBind{relId, uint64(i)}
		value := ColumnBind{get.Index, uint64(i)}
		est.AddRelationToColumnMapping(key, value)
	}
	return nil
}

func (est *CardinalityEstimator) AddRelationToColumnMapping(key, value ColumnBind) {
	est.relationColumnToOriginalColumn[key] = value
}

func (est *CardinalityEstimator) AddColumnToRelationMap(tableIndex, columnIndex uint64) {
	if _, has := est.relationAttributes[tableIndex]; !has {
		est.relationAttributes[tableIndex] = NewRelationAttributes()
	}
	est.relationAttributes[tableIndex].columns.insert(columnIndex)
}

func (est *CardinalityEstimator) CopyRelationMap(cmap ColumnBindMap) {
	for key, value := range est.relationColumnToOriginalColumn {
		if cmap.find(key) {
			panic("key exists")
		}
		cmap.insert(key, value)
	}
}

func (est *CardinalityEstimator) EstimateCrossProduct(left, right *JoinNode) float64 {
	if left.getCard() >= (math.MaxFloat64 / right.getCard()) {
		return math.MaxFloat64
	}
	return left.getCard() * right.getCard()
}

func (est *CardinalityEstimator) EstimateCardWithSet(newset *JoinRelationSet) float64 {
	numerator := float64(1)
	actualset := make(Set)
	for _, relId := range newset.relations {
		numerator *= est.relationAttributes[relId].cardinality
		actualset.insert(relId)
	}
	subgraphs := make([]*Subgraph2Denominator, 0)
	done := false
	foundMatch := false
	for _, rel2Dom := range est.relationsToTDoms {
		if done {
			break
		}
		for _, filter := range rel2Dom.filters {
			if !actualset.find(filter.leftBinding[0]) || !actualset.find(filter.rightBinding[0]) {
				continue
			}
			foundMatch = false
			for j, subgraph := range subgraphs {
				leftIn := subgraph.relations.find(filter.leftBinding[0])
				rightIn := subgraph.relations.find(filter.rightBinding[0])
				if leftIn && rightIn {
					foundMatch = true
					continue
				}
				if !leftIn && !rightIn {
					continue
				}
				var findTable uint64
				if leftIn {
					findTable = filter.rightBinding[0]
				} else {
					findTable = filter.leftBinding[0]
				}
				nextSubgraph := j + 1
				FindSubgraphMatchAndMerge(subgraph, findTable, nextSubgraph, subgraphs)
				subgraph.relations.insert(findTable)
				UpdateDenom(subgraph, rel2Dom)
				foundMatch = true
				break
			}
			if !foundMatch {
				sub := NewSubgraph2Denominator()
				sub.relations.insert(filter.leftBinding[0])
				sub.relations.insert(filter.rightBinding[0])
				UpdateDenom(sub, rel2Dom)
				subgraphs = append(subgraphs, sub)
			}
			//remove empty
			empties := make([]int, 0)
			for j, subgraph := range subgraphs {
				if subgraph.relations.empty() {
					empties = append(empties, j)
				}
			}
			for j := len(empties) - 1; j >= 0; j-- {
				subgraphs = erase(subgraphs, empties[j])
			}
			if len(subgraphs) == 1 && subgraphs[0].relations.size() == newset.count() {
				done = true
				break
			}
		}
	}
	denom := float64(1)
	for _, match := range subgraphs {
		if match.denom > denom {
			denom = match.denom
		}
	}
	if denom == 0 {
		denom = 1
	}
	return numerator / denom
}

func (est *CardinalityEstimator) MergeBindings(bindIdx uint64,
	relId uint64,
	cmaps []ColumnBindMap) {
	for _, cmap := range cmaps {
		for relBind, actualBind := range cmap {
			if actualBind.table() == bindIdx {
				est.AddRelationToColumnMapping(ColumnBind{relId, relBind.column()}, actualBind)
			}
		}
	}
}

func UpdateDenom(rel2Dem *Subgraph2Denominator, rel2TDom *RelationToTDom) {
	if rel2TDom.hasTdomHll {
		rel2Dem.denom *= float64(rel2TDom.tdomHll)
	} else {
		rel2Dem.denom *= float64(rel2TDom.tdomNoHll)
	}

}

func FindSubgraphMatchAndMerge(mergeTo *Subgraph2Denominator,
	findMe uint64,
	next int,
	subgraphs []*Subgraph2Denominator) {
	for i := next; i < len(subgraphs); i++ {
		if subgraphs[i].relations.find(findMe) {
			for key := range subgraphs[i].relations {
				mergeTo.relations.insert(key)
			}
			subgraphs[i].relations.clear()
			mergeTo.denom *= subgraphs[i].denom
			return
		}
	}
}

func IsLogicalFilter(op *LogicalOperator) bool {
	return op.Typ == LOT_Filter
}

type TableFilterSet struct {
}

func getLogicalGet(op *LogicalOperator, tableIndex uint64) *LogicalOperator {
	switch op.Typ {
	case LOT_Scan:
		return op
	case LOT_Filter:
		return getLogicalGet(op.Children[0], tableIndex)
	case LOT_Project:
		return getLogicalGet(op.Children[0], tableIndex)
	case LOT_JOIN:
		if op.JoinTyp == LOT_JoinTypeMARK || op.JoinTyp == LOT_JoinTypeLeft {
			panic("usp " + op.JoinTyp.String())
		}
	default:
		return nil
	}
	return nil
}

type EstimatedProperties struct {
	card float64
	cost float64
}

func NewEstimatedProperties(card, cost float64) *EstimatedProperties {
	return &EstimatedProperties{
		card: card,
		cost: cost,
	}
}

func (e EstimatedProperties) Copy() *EstimatedProperties {
	return &EstimatedProperties{
		card: e.card,
		cost: e.cost,
	}
}

func (e EstimatedProperties) getCost() float64 {
	return e.cost
}

func (e *EstimatedProperties) setCost(c float64) {
	e.cost = c
}

func (e *EstimatedProperties) setCard(c float64) {
	e.card = c
}

func (e EstimatedProperties) getCard() float64 {
	return e.card
}

type Subgraph2Denominator struct {
	relations Set
	denom     float64
}

func NewSubgraph2Denominator() *Subgraph2Denominator {
	return &Subgraph2Denominator{
		relations: make(Set),
		denom:     1,
	}
}
