package main

import "math"

type RelationAttributes struct {
    originalName string
    columns      Set
    cardinality  float64
}

type ColumnBindSet map[[2]uint64]bool
type ColumnBindMap map[[2]uint64][2]uint64

func (set ColumnBindSet) find(bind [2]uint64) bool {
    if _, has := set[bind]; has {
        return true
    }
    return false
}

func (set ColumnBindSet) insert(bind [2]uint64) {
    set[bind] = true
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
    filterInfos []*FilterInfo) {

}

func (est *CardinalityEstimator) InitEquivalentRelations(filterInfos []*FilterInfo) {
    for _, filter := range filterInfos {
        if est.SingleColumnFilter(filter) {
            est.AddRelationTdom(filter)
            continue
        } else if est.EmptyFilter(filter) {
            continue
        }
        //TODO: 
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
    set.insert([2]uint64{filterInfo.leftBinding[0], filterInfo.leftBinding[1]})
    newTdom := NewRelationToTDom(set)
    est.relationsToTDoms = append(est.relationsToTDoms, newTdom)
}
