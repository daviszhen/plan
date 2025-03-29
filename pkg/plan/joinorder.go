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
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"

	"github.com/xlab/treeprint"

	"github.com/daviszhen/plan/pkg/storage"
	"github.com/daviszhen/plan/pkg/util"
)

func (b *Builder) joinOrder(root *LogicalOperator) (*LogicalOperator, error) {
	joinOrder := NewJoinOrderOptimizer(b.txn)
	return joinOrder.Optimize(root)
}

type SingleJoinRelation struct {
	op     *LogicalOperator
	parent *LogicalOperator
}

type JoinRelationSet struct {
	relations []uint64
}

func NewJoinRelationSet(rels []uint64) *JoinRelationSet {
	ret := &JoinRelationSet{relations: util.CopyTo(rels)}
	ret.sort()
	return ret
}

func (irs *JoinRelationSet) Equal(o Equal) bool {
	return slices.Equal(irs.relations, o.(*JoinRelationSet).relations)
}

func (irs *JoinRelationSet) sort() {
	sort.Slice(irs.relations, func(i, j int) bool {
		return irs.relations[i] < irs.relations[j]
	})
}

func (irs *JoinRelationSet) count() int {
	return len(irs.relations)
}

func isSubset(super, sub *JoinRelationSet) bool {
	if len(sub.relations) > len(super.relations) {
		return false
	}
	j := 0
	for i := 0; i < len(super.relations); i++ {
		if sub.relations[j] == super.relations[i] {
			j++
			if j == len(sub.relations) {
				return true
			}
		}
	}
	return false
}

func (irs *JoinRelationSet) String() string {
	if irs == nil {
		return ""
	}
	bb := strings.Builder{}
	bb.WriteString("[")
	for i, r := range irs.relations {
		if i > 0 {
			bb.WriteString(", ")
		}
		bb.WriteString(fmt.Sprintf("%d", r))
	}
	bb.WriteString("]")
	return bb.String()
}

type UnorderedSet map[uint64]bool

func (set UnorderedSet) orderedKeys() []uint64 {
	keys := make([]uint64, 0, len(set))
	for key := range set {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		return keys[i] < keys[j]
	})
	return keys
}

func (set UnorderedSet) insert(keys ...uint64) {
	for _, key := range keys {
		set[key] = true
	}
}

func (set UnorderedSet) find(key uint64) bool {
	_, has := set[key]
	return has
}

func (set UnorderedSet) clear() {
	for key := range set {
		delete(set, key)
	}
}

func (set UnorderedSet) empty() bool {
	return len(set) == 0
}

func (set UnorderedSet) size() int {
	return len(set)
}

type treeNode struct {
	relation *JoinRelationSet
	children map[uint64]*treeNode
}

func newTreeNode() *treeNode {
	return &treeNode{children: make(map[uint64]*treeNode)}
}

func (node *treeNode) Print(tree treeprint.Tree) {
	if node == nil {
		return
	}
	tree = tree.AddMetaNode("relations", node.relation.String())
	for key, child := range node.children {
		child.Print(tree.AddMetaBranch("child", key))
	}
}

func (node *treeNode) String() string {
	tree := treeprint.New()
	node.Print(tree)
	return tree.String()
}

type JoinRelationSetManager struct {
	root *treeNode
}

func NewJoinRelationSetManager() *JoinRelationSetManager {
	ret := &JoinRelationSetManager{
		root: newTreeNode(),
	}
	return ret
}

func (jrsm *JoinRelationSetManager) union(left, right *JoinRelationSet) *JoinRelationSet {
	dedup := make(UnorderedSet)
	dedup.insert(left.relations...)
	dedup.insert(right.relations...)
	return NewJoinRelationSet(dedup.orderedKeys())
}

func (jrsm *JoinRelationSetManager) getRelation(relations UnorderedSet) *JoinRelationSet {
	curNode := jrsm.root
	keys := relations.orderedKeys()
	for _, relId := range keys {
		if next, has := curNode.children[relId]; !has {
			next = newTreeNode()
			curNode.children[relId] = next
			curNode = next
		} else {
			curNode = next
		}
	}
	if curNode.relation == nil {
		curNode.relation = NewJoinRelationSet(keys)
	}
	return curNode.relation
}

func (jrsm *JoinRelationSetManager) getRelation2(relation uint64) *JoinRelationSet {
	set := make(UnorderedSet)
	set[relation] = true
	return jrsm.getRelation(set)
}

func (jrsm *JoinRelationSetManager) String() string {
	return jrsm.root.String()
}

type FilterInfo struct {
	set          *JoinRelationSet
	filterIndex  int
	leftSet      *JoinRelationSet
	rightSet     *JoinRelationSet
	leftBinding  ColumnBind
	rightBinding ColumnBind
}

type neighborInfo struct {
	neighbor *JoinRelationSet
	filters  []*FilterInfo
}

func (neigh *neighborInfo) appendFilter(f *FilterInfo) {
	neigh.filters = append(neigh.filters, f)
}

type queryEdge struct {
	neighbors []*neighborInfo
	children  map[uint64]*queryEdge
}

func (edge *queryEdge) Print(prefix []uint64) string {
	source := strings.Builder{}
	source.WriteByte('[')
	for i, u := range prefix {
		if i > 0 {
			source.WriteByte(',')
		}
		source.WriteString(fmt.Sprintf("%d", u))
	}
	source.WriteByte(']')

	sb := strings.Builder{}
	for _, neighbor := range edge.neighbors {
		sb.WriteString(fmt.Sprintf("%s -> %s\n", source.String(), neighbor.neighbor.String()))
	}
	for k, v := range edge.children {
		newPrefix := util.CopyTo(prefix)
		newPrefix = append(newPrefix, k)
		sb.WriteString(v.Print(newPrefix))
	}
	return sb.String()
}

func (edge *queryEdge) String() string {
	return edge.Print([]uint64{})
}

func newQueryEdge() *queryEdge {
	return &queryEdge{
		children: make(map[uint64]*queryEdge),
	}
}

type QueryGraph struct {
	root *queryEdge
}

func NewQueryGraph() *QueryGraph {
	return &QueryGraph{
		root: newQueryEdge(),
	}
}

func (graph *QueryGraph) String() string {
	return graph.root.String()
}

func (graph *QueryGraph) getQueryEdge(set *JoinRelationSet) *queryEdge {
	info := graph.root
	for _, rel := range set.relations {
		if next, has := info.children[rel]; !has {
			info.children[rel] = newQueryEdge()
			info = info.children[rel]
		} else {
			info = next
		}
	}
	return info
}

func (graph *QueryGraph) CreateEdge(left, right *JoinRelationSet, info *FilterInfo) {
	node := graph.getQueryEdge(left)
	for _, neighbor := range node.neighbors {
		if neighbor.neighbor == right {
			if info != nil {
				neighbor.appendFilter(info)
			}
			return
		}
	}
	newNode := &neighborInfo{
		neighbor: right,
	}
	if info != nil {
		newNode.appendFilter(info)
	}
	node.neighbors = append(node.neighbors, newNode)
}

func (graph *QueryGraph) enumNeighbors(node *JoinRelationSet, callback func(info *neighborInfo) bool) {
	for j := 0; j < len(node.relations); j++ {
		info := graph.root
		for i := j; i < len(node.relations); i++ {
			if next, has := info.children[node.relations[i]]; !has {
				break
			} else {
				info = next
			}
			for _, neighbor := range info.neighbors {
				if callback(neighbor) {
					return
				}
			}
		}
	}
}

func (graph *QueryGraph) GetNeighbors(node *JoinRelationSet, excludeSet UnorderedSet) (ret []uint64) {
	dedup := make(UnorderedSet)
	graph.enumNeighbors(node, func(info *neighborInfo) bool {
		if !joinRelationSetIsExcluded(info.neighbor, excludeSet) {
			dedup.insert(info.neighbor.relations[0])
		}
		return false
	})
	for k := range dedup {
		ret = append(ret, k)
	}
	return
}

func (graph *QueryGraph) GetConnections(node, other *JoinRelationSet) (conns []*neighborInfo) {
	graph.enumNeighbors(node, func(info *neighborInfo) bool {
		if isSubset(other, info.neighbor) {
			conns = append(conns, info)
		}
		return false
	})
	return
}

func joinRelationSetIsExcluded(node *JoinRelationSet, set UnorderedSet) bool {
	if _, has := set[node.relations[0]]; has {
		return true
	}
	return false
}

type JoinNode struct {
	set            *JoinRelationSet
	info           *neighborInfo
	left, right    *JoinNode
	baseCard       float64
	estimatedProps *EstimatedProperties
}

func NewJoinNode(set *JoinRelationSet, baseCard float64) *JoinNode {
	return &JoinNode{
		set:            set,
		estimatedProps: NewEstimatedProperties(baseCard, 0),
	}
}

func (jnode *JoinNode) getCard() float64 {
	return jnode.estimatedProps.getCard()
}

func (jnode *JoinNode) getBaseCard() float64 {
	//if jnode.set.count() > 1 {
	//    panic("usp call on intermediate join node")
	//}
	return jnode.baseCard
}

func (jnode *JoinNode) setBaseCard(f float64) {
	if f == 0 {
		panic("zero")
	}
	jnode.baseCard = f
}

func (jnode *JoinNode) getCost() float64 {
	return jnode.estimatedProps.getCost()
}

func (jnode *JoinNode) setEstimatedCard(f float64) {
	if f == 0 {
		panic("zero")
	}
	jnode.estimatedProps.setCard(f)
}

type NodeOp struct {
	node *JoinNode
	op   *LogicalOperator
}

type Equal interface {
	Equal(key Equal) bool
}

type planMap map[Equal]*JoinNode

func (pmap planMap) set(set *JoinRelationSet, node *JoinNode) {
	pmap[set] = node
}

func (pmap planMap) get(set *JoinRelationSet) *JoinNode {
	for key, value := range pmap {
		if key.Equal(set) {
			return value
		}
	}
	return nil
}

type JoinOrderOptimizer struct {
	relations []*SingleJoinRelation //relation node
	//table index -> relation id
	relationMapping map[uint64]uint64
	filters         []*Expr
	filterInfos     []*FilterInfo
	setManager      *JoinRelationSetManager
	queryGraph      *QueryGraph
	plans           planMap
	estimator       *CardinalityEstimator
	txn             *storage.Txn
}

func NewJoinOrderOptimizer(txn *storage.Txn) *JoinOrderOptimizer {
	return &JoinOrderOptimizer{
		relationMapping: make(map[uint64]uint64),
		setManager:      NewJoinRelationSetManager(),
		plans:           make(planMap),
		queryGraph:      NewQueryGraph(),
		estimator:       NewCardinalityEstimator(txn),
		txn:             txn,
	}
}

func (joinOrder *JoinOrderOptimizer) createEdge(left, right *Expr, info *FilterInfo) {
	leftRelations := make(map[uint64]bool)
	rightRelations := make(map[uint64]bool)
	joinOrder.collectRelation(left, leftRelations)
	joinOrder.collectRelation(right, rightRelations)
	joinOrder.getColumnBind(left, &info.leftBinding)
	joinOrder.getColumnBind(right, &info.rightBinding)
	if len(leftRelations) != 0 && len(rightRelations) != 0 {
		info.leftSet = joinOrder.setManager.getRelation(leftRelations)
		info.rightSet = joinOrder.setManager.getRelation(rightRelations)
		if info.leftSet != info.rightSet {
			//disjoint or not
			if isDisjoint(leftRelations, rightRelations) {
				//add edge into query graph
				joinOrder.queryGraph.CreateEdge(info.leftSet, info.rightSet, info)
				joinOrder.queryGraph.CreateEdge(info.rightSet, info.leftSet, info)
			}
		}
	}
}

func (joinOrder *JoinOrderOptimizer) Optimize(root *LogicalOperator) (*LogicalOperator, error) {
	noReorder, filterOps, err := joinOrder.extractJoinRelations(root, nil)
	if err != nil {
		return nil, err
	}
	if !noReorder {
		return root, nil
	}

	if len(joinOrder.relations) <= 1 {
		return root, nil
	}

	//extract filters and dedup them
	for _, filterOp := range filterOps {
		switch filterOp.Typ {
		case LOT_JOIN:
			if filterOp.JoinTyp != LOT_JoinTypeInner {
				panic("usp join type")
			}
			joinOrder.filters = append(joinOrder.filters, splitExprsByAnd(filterOp.OnConds)...)
		case LOT_Filter:
			joinOrder.filters = append(joinOrder.filters, splitExprsByAnd(filterOp.Filters)...)
		default:
			panic(fmt.Sprintf("usp op type %d", filterOp.Typ))
		}
	}
	//create join edge from filters
	for i, filter := range joinOrder.filters {
		filterRelations := make(map[uint64]bool)
		joinOrder.collectRelation(filter, filterRelations)
		filterSet := joinOrder.setManager.getRelation(filterRelations)
		info := &FilterInfo{
			set:         filterSet,
			filterIndex: i,
		}
		joinOrder.filterInfos = append(joinOrder.filterInfos, info)
		//comparison operator => join predicate
		switch filter.Typ {
		case ET_Func:
			switch filter.SubTyp {
			case ET_In, ET_NotIn:
				//in or not in has been splited
				for j, child := range filter.Children {
					if j == 0 {
						continue
					}
					joinOrder.createEdge(filter.Children[0], child, info)
				}
			case ET_SubFunc:
			case ET_And, ET_Or, ET_Equal, ET_NotEqual, ET_Like, ET_GreaterEqual, ET_Less, ET_Greater:
				joinOrder.createEdge(filter.Children[0], filter.Children[1], info)
			default:
				panic(fmt.Sprintf("usp %v", filter.SubTyp))
			}
		default:
			panic(fmt.Sprintf("usp operator type %d", filter.Typ))
		}
	}

	//prepare for dp algorithm
	nodesOpts := make([]*NodeOp, 0)
	for i, relation := range joinOrder.relations {
		set := joinOrder.setManager.getRelation2(uint64(i))
		nodesOpts = append(nodesOpts, &NodeOp{
			node: NewJoinNode(set, 0),
			op:   relation.op,
		})
	}

	err = joinOrder.estimator.InitCardinalityEstimatorProps(nodesOpts, joinOrder.filterInfos)
	if err != nil {
		return nil, err
	}

	for _, nodeOp := range nodesOpts {

		joinOrder.plans.set(nodeOp.node.set, nodeOp.node)
	}
	err = joinOrder.solveJoinOrder()
	if err != nil {
		return nil, err
	}
	//get optimal plan
	relations := make(map[uint64]bool, 0)
	for i := 0; i < len(joinOrder.relations); i++ {
		relations[uint64(i)] = true
	}
	set := joinOrder.setManager.getRelation(relations)
	final := joinOrder.plans.get(set)
	if final == nil {
		joinOrder.generateCrossProduct()
		err = joinOrder.solveJoinOrder()
		if err != nil {
			return nil, err
		}
		final = joinOrder.plans.get(set)
		if final == nil {
			return nil, errors.New("no plan any more")
		}
	}
	if final == nil {
		return nil, errors.New("final plan is nil")
	}
	checkExprIsValid(root)
	return joinOrder.rewritePlan(root, final)
}

func (joinOrder *JoinOrderOptimizer) generateCrossProduct() {
	for i := 0; i < len(joinOrder.relations); i++ {
		left := joinOrder.setManager.getRelation2(uint64(i))
		for j := 0; j < len(joinOrder.relations); j++ {
			if i != j {
				right := joinOrder.setManager.getRelation2(uint64(j))
				joinOrder.queryGraph.CreateEdge(left, right, nil)
				joinOrder.queryGraph.CreateEdge(right, left, nil)
			}
		}
	}
}

func (joinOrder *JoinOrderOptimizer) extractJoinRelation(rel *SingleJoinRelation) (*LogicalOperator, error) {
	children := rel.parent.Children
	for i := 0; i < len(children); i++ {
		if children[i] == rel.op {
			ret := children[i]
			children = util.Erase(children, i)
			rel.parent.Children = children
			return ret, nil
		}
	}
	return nil, errors.New("no relation in parent node")
}

type GenerateJoinRelation struct {
	set *JoinRelationSet
	op  *LogicalOperator
}

func (joinOrder *JoinOrderOptimizer) generateJoins(extractedRels []*LogicalOperator, node *JoinNode) (*GenerateJoinRelation, error) {
	var resultOp *LogicalOperator
	var leftNode, rightNode, resultRel *JoinRelationSet
	if node.left != nil && node.right != nil && node.info != nil {
		left, err := joinOrder.generateJoins(extractedRels, node.left)
		if err != nil {
			return nil, err
		}
		right, err := joinOrder.generateJoins(extractedRels, node.right)
		if err != nil {
			return nil, err
		}

		if len(node.info.filters) == 0 {
			//cross join
			resultOp = &LogicalOperator{
				Typ:     LOT_JOIN,
				JoinTyp: LOT_JoinTypeCross,
				Children: []*LogicalOperator{
					left.op,
					right.op,
				},
			}
		} else {
			resultOp = &LogicalOperator{
				Typ:     LOT_JOIN,
				JoinTyp: LOT_JoinTypeInner,
				Children: []*LogicalOperator{
					left.op,
					right.op,
				},
			}
			for _, filter := range node.info.filters {
				if joinOrder.filters[filter.filterIndex] == nil {
					return nil, errors.New("filter is nil")
				}
				condition := joinOrder.filters[filter.filterIndex]
				if !(condition.SubTyp == ET_Equal || condition.SubTyp == ET_In) {
					continue
				}
				util.AssertFunc(condition.SubTyp != ET_Less)
				check := isSubset(left.set, filter.leftSet) && isSubset(right.set, filter.rightSet) ||
					isSubset(left.set, filter.rightSet) && isSubset(right.set, filter.leftSet)
				if !check {
					return nil, errors.New("filter sets are not intersects")
				}
				cond := &Expr{
					Typ:     condition.Typ,
					SubTyp:  condition.SubTyp,
					DataTyp: condition.DataTyp,
					FunImpl: condition.FunImpl,
				}
				invert := !isSubset(left.set, filter.leftSet)
				if !(condition.SubTyp == ET_Equal || condition.SubTyp == ET_In) {
					invert = false
				}
				if condition.SubTyp == ET_In || condition.SubTyp == ET_NotIn {
					cond.Children = []*Expr{condition.Children[0], condition.Children[1]}
					//TODO: fixme
					//if !invert {
					//	cond.In = condition.In
					//	cond.Children = []*Expr{condition.Children[0]}
					//} else {
					//	cond.In = condition.Children[0]
					//	cond.Children = []*Expr{condition.In}
					//}
				} else {
					if !invert {
						cond.Children = []*Expr{condition.Children[0], condition.Children[1]}
					} else {
						cond.Children = []*Expr{condition.Children[1], condition.Children[0]}
					}
				}

				//if invert {
				//	//TODO:
				//}
				checkExprs(cond)
				resultOp.OnConds = append(resultOp.OnConds, cond.copy())
				//remove this filter
				joinOrder.filters[filter.filterIndex] = nil
			}

			//if there is no condition for inner, convert it to cross
			if len(resultOp.OnConds) == 0 {
				resultOp.JoinTyp = LOT_JoinTypeCross
			}
		}
		leftNode = left.set
		rightNode = right.set
		resultRel = joinOrder.setManager.union(leftNode, rightNode)
	} else {
		resultRel = node.set
		resultOp = extractedRels[node.set.relations[0]]
	}
	resultOp.estimatedProps = node.estimatedProps.Copy()
	resultOp.estimatedCard = uint64(resultOp.estimatedProps.getCard())
	resultOp.hasEstimatedCard = true
	if resultOp.Typ == LOT_Filter &&
		len(resultOp.Children) != 0 &&
		resultOp.Children[0].Typ == LOT_Scan {
		filterProps := resultOp.estimatedProps
		childOp := resultOp.Children[0]
		childOp.estimatedProps = NewEstimatedProperties(filterProps.getCard()/defaultSelectivity, filterProps.getCost())
		childOp.estimatedCard = uint64(childOp.estimatedProps.getCard())
		childOp.hasEstimatedCard = true
	}
	//push down remaining filters
	//TODO:
	for _, info := range joinOrder.filterInfos {
		if joinOrder.filters[info.filterIndex] != nil {
			//filter is subset of current relation
			if info.set.count() > 0 && isSubset(resultRel, info.set) {
				filter := joinOrder.filters[info.filterIndex]
				if !(filter.SubTyp == ET_Equal || filter.SubTyp == ET_In) {
					continue
				}
				if leftNode == nil || info.leftSet == nil {
					resultOp = pushFilter(resultOp, filter.copy())
					joinOrder.filters[info.filterIndex] = nil
					continue
				}
				foundSubset := false
				invert := false
				if isSubset(leftNode, info.leftSet) && isSubset(rightNode, info.rightSet) {
					foundSubset = true
				} else if isSubset(rightNode, info.leftSet) && isSubset(leftNode, info.rightSet) {
					invert = true
					foundSubset = true
				}
				if !foundSubset {
					resultOp = pushFilter(resultOp, filter.copy())
					joinOrder.filters[info.filterIndex] = nil
					continue
				}
				if !(filter.SubTyp == ET_Equal || filter.SubTyp == ET_In) {
					invert = false
				}
				cond := &Expr{
					Typ:      filter.Typ,
					SubTyp:   filter.SubTyp,
					DataTyp:  filter.DataTyp,
					Children: []*Expr{nil, nil},
					FunImpl:  filter.FunImpl,
				}
				if !invert {
					cond.Children[0], cond.Children[1] = filter.Children[0], filter.Children[1]
				} else {
					cond.Children[0], cond.Children[1] = filter.Children[1], filter.Children[0]
				}
				//if invert {
				//	//TODO
				//}
				cur := resultOp
				if cur.Typ == LOT_Filter {
					cur = cur.Children[0]
				}
				checkExprs(cond)
				if cur.Typ == LOT_JOIN && cur.JoinTyp == LOT_JoinTypeCross {
					next := &LogicalOperator{
						Typ:      LOT_JOIN,
						JoinTyp:  LOT_JoinTypeInner,
						Children: cur.Children,
						OnConds:  []*Expr{cond.copy()},
					}
					if cur == resultOp {
						resultOp = next
					} else {
						resultOp.Children[0] = next
					}
				} else {
					if cond.SubTyp == ET_Equal || cond.SubTyp == ET_In {
						resultOp.OnConds = append(resultOp.OnConds, cond.copy())
					} else {
						next := &LogicalOperator{
							Typ:      LOT_Filter,
							Filters:  []*Expr{cond.copy()},
							Children: []*LogicalOperator{resultOp},
						}
						resultOp = next
					}
				}
			}
		}
	}
	return &GenerateJoinRelation{
		set: resultRel,
		op:  resultOp,
	}, nil
}

func (joinOrder *JoinOrderOptimizer) rewritePlan(root *LogicalOperator, node *JoinNode) (*LogicalOperator, error) {
	rootIsJoin := len(root.Children) > 1

	extractedRelations := make([]*LogicalOperator, 0)
	for _, rel := range joinOrder.relations {
		exRel, err := joinOrder.extractJoinRelation(rel)
		if err != nil {
			return nil, err
		}
		extractedRelations = append(extractedRelations, exRel)
	}
	checkExprIsValid(root)
	joinTree, err := joinOrder.generateJoins(extractedRelations, node)
	if err != nil {
		return nil, err
	}
	checkExprIsValid(joinTree.op)
	//pushdown remaining filters
	for _, filter := range joinOrder.filters {
		if filter != nil {
			joinTree.op = pushFilter(joinTree.op, filter.copy())
		}
	}
	checkExprIsValid(joinTree.op)
	if rootIsJoin {
		return joinTree.op, nil
	}
	if len(root.Children) != 1 {
		return nil, errors.New("multiple children")
	}
	op := root
	parent := root
	for op.Typ != LOT_JOIN && (op.JoinTyp != LOT_JoinTypeCross && op.JoinTyp != LOT_JoinTypeInner) {
		if len(op.Children) != 1 {
			return nil, errors.New("multiple children")
		}
		parent = op
		op = op.Children[0]
	}
	parent.Children[0] = joinTree.op
	return root, nil
}

func pushFilter(node *LogicalOperator, expr *Expr) *LogicalOperator {
	if node.Typ != LOT_Filter {
		filter := &LogicalOperator{Typ: LOT_Filter, Children: []*LogicalOperator{node}}
		node = filter
	}
	node.Filters = append(node.Filters, expr)
	return node
}

func (joinOrder *JoinOrderOptimizer) solveJoinOrder() error {
	return joinOrder.greedy()
}

func (joinOrder *JoinOrderOptimizer) greedy() (err error) {
	var joinRelations []*JoinRelationSet
	for i := 0; i < len(joinOrder.relations); i++ {
		joinRelations = append(joinRelations, joinOrder.setManager.getRelation2(uint64(i)))
	}

	for len(joinRelations) > 1 {
		bestLeft, bestRight := 0, 0
		var best *JoinNode
		for i := 0; i < len(joinRelations); i++ {
			left := joinRelations[i]
			for j := i + 1; j < len(joinRelations); j++ {
				right := joinRelations[j]
				conns := joinOrder.queryGraph.GetConnections(left, right)
				if len(conns) != 0 {
					node, err := joinOrder.emitPair(left, right, conns)
					if err != nil {
						return err
					}

					err = joinOrder.updateDPTree(node)
					if err != nil {
						return err
					}
					if best == nil || node.getCost() < best.getCost() {
						best = node
						bestLeft = i
						bestRight = j
					}
				}
			}
		}
		if best == nil {
			smallestPlans := make([]*JoinNode, 2)
			smallestIndex := make([]uint64, 2)
			for i := 0; i < 2; i++ {
				p := joinOrder.plans.get(joinRelations[i])
				if p != nil {
					smallestPlans[i] = p
					smallestIndex[i] = uint64(i)
					if p == nil {
						return errors.New("no plan")
					}
				}
			}

			for i := 2; i < len(joinRelations); i++ {
				p := joinOrder.plans.get(joinRelations[i])
				if p == nil {
					return errors.New("plan is nil")
				} else {
					for j := 0; j < 2; j++ {
						if smallestPlans[j] == nil ||
							smallestPlans[j].getBaseCard() > p.getBaseCard() {
							smallestPlans[j] = p
							smallestIndex[j] = uint64(i)
							break
						}
					}
				}
			}
			if smallestPlans[0] == nil || smallestPlans[1] == nil {
				return errors.New("no plan")
			}
			if smallestIndex[0] == smallestIndex[1] {
				return errors.New("smallest indices are same")
			}
			left := smallestPlans[0].set
			right := smallestPlans[1].set
			joinOrder.queryGraph.CreateEdge(left, right, nil)
			conns := joinOrder.queryGraph.GetConnections(left, right)
			if conns == nil {
				return errors.New("conns are nil")
			}
			best, err = joinOrder.emitPair(left, right, conns)
			if err != nil {
				return err
			}
			bestLeft = int(smallestIndex[0])
			bestRight = int(smallestIndex[1])
			err = joinOrder.updateDPTree(best)
			if err != nil {
				return err
			}
			if bestLeft > bestRight {
				bestLeft, bestRight = bestRight, bestLeft
			}
		}
		joinRelations = util.Erase(joinRelations, bestRight)
		joinRelations = util.Erase(joinRelations, bestLeft)
		joinRelations = append(joinRelations, best.set)
	}
	return nil
}

func (joinOrder *JoinOrderOptimizer) updateDPTree(newPlan *JoinNode) error {
	//TODO: full plan
	newSet := newPlan.set
	excludeSet := make(UnorderedSet)
	excludeSet.insert(newSet.relations...)
	neighbors := joinOrder.queryGraph.GetNeighbors(newSet, excludeSet)
	allNeighbors := joinOrder.getAllNeighborSets(excludeSet, neighbors)
	for _, neighbor := range allNeighbors {
		neiRel := joinOrder.setManager.getRelation(neighbor)
		combineSet := joinOrder.setManager.union(newSet, neiRel)
		combinePlan := joinOrder.plans.get(combineSet)
		if combinePlan != nil {
			conns := joinOrder.queryGraph.GetConnections(newSet, neiRel)
			p := joinOrder.plans.get(neiRel)
			if p != nil {
				updatedPlan, err := joinOrder.emitPair(newSet, neiRel, conns)
				if err != nil {
					return err
				}
				if updatedPlan.getCost() < combinePlan.getCost() {
					err = joinOrder.updateDPTree(updatedPlan)
					if err != nil {
						return err
					}
				}
			}
		}
	}
	return nil
}

func (joinOrder *JoinOrderOptimizer) getAllNeighborSets(excludeSet UnorderedSet, neighbors []uint64) []UnorderedSet {
	sort.Slice(neighbors, func(i, j int) bool {
		return neighbors[i] < neighbors[j]
	})
	ret := make([]UnorderedSet, 0)
	added := make([]UnorderedSet, 0)
	for _, nei := range neighbors {
		x := make(UnorderedSet)
		x.insert(nei)
		added = append(added, x)
		y := make(UnorderedSet)
		y.insert(nei)
		ret = append(ret, y)
	}

	for {
		added = joinOrder.addSuperSets(added, neighbors)
		ret = append(ret, added...)
		if len(added) == 0 {
			break
		}
	}
	return ret
}

func (joinOrder *JoinOrderOptimizer) addSuperSets(current []UnorderedSet, allNeighbors []uint64) []UnorderedSet {
	var ret []UnorderedSet
	for _, nei := range allNeighbors {
		for _, cur := range current {
			keys := cur.orderedKeys()
			if keys[len(keys)-1] >= nei {
				continue
			}
			if _, has := cur[nei]; !has {
				newSet := make(UnorderedSet)
				for x := range cur {
					newSet.insert(x)
				}
				newSet.insert(nei)
				ret = append(ret, newSet)
			}
		}
	}
	return ret
}

func (joinOrder *JoinOrderOptimizer) getPlan(set *JoinRelationSet) (*JoinNode, error) {
	plan := joinOrder.plans.get(set)
	return plan, nil
}

func (joinOrder *JoinOrderOptimizer) createJoinTree(set *JoinRelationSet, info []*neighborInfo, left, right *JoinNode) (*JoinNode, error) {
	plan, err := joinOrder.getPlan(set)
	if err != nil {
		return nil, err
	}
	if left.getBaseCard() < right.getBaseCard() {
		return joinOrder.createJoinTree(set, info, right, left)
	}
	var expectCard float64
	var bestConn *neighborInfo
	if plan != nil {
		expectCard = plan.getCard()
		bestConn = info[len(info)-1]
	} else if len(info) == 0 {
		//cross product
		expectCard = joinOrder.estimator.EstimateCrossProduct(left, right)
	} else {
		//normal join
		expectCard = joinOrder.estimator.EstimateCardWithSet(set)
		bestConn = info[len(info)-1]
	}
	cost := joinOrder.ComputeCost(left, right, expectCard)
	return &JoinNode{
		set:   set,
		info:  bestConn,
		left:  left,
		right: right,
		estimatedProps: &EstimatedProperties{
			card: expectCard,
			cost: cost,
		},
	}, nil
}
func (joinOrder *JoinOrderOptimizer) ComputeCost(left, right *JoinNode, expectedCard float64) float64 {
	return expectedCard + left.getCost() + right.getCost()
}

func (joinOrder *JoinOrderOptimizer) emitPair(left, right *JoinRelationSet, info []*neighborInfo) (*JoinNode, error) {
	leftPlan, err := joinOrder.getPlan(left)
	if err != nil {
		return nil, err
	}
	if leftPlan == nil {
		return nil, errors.New("left plan is nil " + left.String())
	}
	rightPlan, err := joinOrder.getPlan(right)
	if err != nil {
		return nil, err
	}
	if rightPlan == nil {
		return nil, errors.New("right plan is nil " + right.String())
	}
	newSet := joinOrder.setManager.union(left, right)
	newPlan, err := joinOrder.createJoinTree(newSet, info, leftPlan, rightPlan)
	if err != nil {
		return nil, err
	}
	tplan, err := joinOrder.getPlan(newSet)
	if err != nil {
		return nil, err
	}
	if tplan == nil || newPlan.getCost() < tplan.getCost() {
		//TODO: missing a lot
		//if len(newSet.relations) == len(joinOrder.relations) {
		//	//TODO:
		//}
		joinOrder.plans.set(newSet, newPlan)
		return newPlan, nil
	}
	return tplan, nil
}

func (joinOrder *JoinOrderOptimizer) extractJoinRelations(root, parent *LogicalOperator) (nonReorder bool, filterOps []*LogicalOperator, err error) {
	op := root
	for len(op.Children) == 1 &&
		op.Typ != LOT_Project &&
		op.Typ != LOT_Scan {
		if op.Typ == LOT_Filter {
			filterOps = append(filterOps, op)
		}

		if op.Typ == LOT_AggGroup {
			optimizer := NewJoinOrderOptimizer(joinOrder.txn)
			op.Children[0], err = optimizer.Optimize(op.Children[0])
			if err != nil {
				return false, nil, err
			}
			return false, filterOps, err
		}
		op = op.Children[0]
	}

	if op.Typ == LOT_JOIN {
		if op.JoinTyp == LOT_JoinTypeInner {
			filterOps = append(filterOps, op)
		} else if op.JoinTyp == LOT_JoinTypeCross {
			//TODO:
		} else {
			//non-inner join, be not reordered
			nonReorder = true
			//TODO: tpchQ13
		}
	}

	if nonReorder {
		//setop or non-inner join
		cmaps := make([]ColumnBindMap, 0)
		for i, child := range op.Children {
			optimizer := NewJoinOrderOptimizer(joinOrder.txn)
			op.Children[i], err = optimizer.Optimize(child)
			if err != nil {
				return false, nil, err
			}
			cmap := make(ColumnBindMap)
			optimizer.estimator.CopyRelationMap(cmap)
			cmaps = append(cmaps, cmap)
		}
		//get all table
		tables := make(UnorderedSet)
		getTableRefers(root, tables)
		relation := &SingleJoinRelation{op: root, parent: parent}
		relId := len(joinOrder.relations)
		for tabId := range tables {
			joinOrder.estimator.MergeBindings(tabId, uint64(relId), cmaps)
			joinOrder.relationMapping[tabId] = uint64(relId)
		}
		joinOrder.relations = append(joinOrder.relations, relation)
		return true, filterOps, err
	}
	switch op.Typ {
	case LOT_JOIN:
		leftReorder, leftFilters, err := joinOrder.extractJoinRelations(op.Children[0], op)
		if err != nil {
			return false, nil, err
		}
		filterOps = append(filterOps, leftFilters...)
		rightReorder, rightFilters, err := joinOrder.extractJoinRelations(op.Children[1], op)
		if err != nil {
			return false, nil, err
		}
		filterOps = append(filterOps, rightFilters...)
		return leftReorder && rightReorder, filterOps, err
	case LOT_Scan:
		tableIndex := op.Index
		relation := &SingleJoinRelation{op: root, parent: parent}
		relationId := len(joinOrder.relations)
		err = joinOrder.estimator.AddRelationColumnMapping(op, uint64(relationId))
		if err != nil {
			return false, nil, err
		}
		joinOrder.relationMapping[tableIndex] = uint64(relationId)
		joinOrder.relations = append(joinOrder.relations, relation)
		return true, filterOps, err
	case LOT_Project:
		tableIndex := op.Index
		relation := &SingleJoinRelation{op: root, parent: parent}
		relationId := len(joinOrder.relations)
		optimizer := NewJoinOrderOptimizer(joinOrder.txn)
		op.Children[0], err = optimizer.Optimize(op.Children[0])
		if err != nil {
			return false, nil, err
		}
		cmap := make(ColumnBindMap)
		optimizer.estimator.CopyRelationMap(cmap)
		joinOrder.relationMapping[tableIndex] = uint64(relationId)
		for key, value := range cmap {
			newkey := ColumnBind{tableIndex, key[1]}
			joinOrder.estimator.AddRelationToColumnMapping(newkey, value)
			joinOrder.estimator.AddColumnToRelationMap(value[0], value[1])
		}
		joinOrder.relations = append(joinOrder.relations, relation)
		return true, filterOps, err
	default:
		panic(fmt.Sprintf("usp operator type %d", op.Typ))
	}
}

func (joinOrder *JoinOrderOptimizer) collectRelation(e *Expr, set map[uint64]bool) {
	switch e.Typ {
	case ET_Column:
		index := e.ColRef[0]
		if relId, has := joinOrder.relationMapping[index]; !has {
			panic(fmt.Sprintf("there is no table index %d in relation mapping", index))
		} else {
			joinOrder.estimator.AddColumnToRelationMap(relId, e.ColRef[1])
			set[relId] = true
			//if joinOrder.relations[relId].op.Index != index {
			//	panic("no such relation")
			//}
		}
	case ET_SConst, ET_IConst, ET_FConst, ET_DecConst, ET_BConst:
	case ET_Func:
	default:
		panic("usp")
	}
	for _, child := range e.Children {
		joinOrder.collectRelation(child, set)
	}
}

func (joinOrder *JoinOrderOptimizer) getColumnBind(e *Expr, cb *ColumnBind) {
	if e == nil {
		return
	}
	switch e.Typ {
	case ET_Column:
		index := e.ColRef[0]
		if relId, has := joinOrder.relationMapping[index]; !has {
			panic(fmt.Sprintf("there is no table index %d in relation mapping", index))
		} else {
			cb[0] = relId
			cb[1] = e.ColRef[1]
		}

	case ET_SConst, ET_IConst, ET_FConst, ET_DecConst, ET_BConst:
	case ET_Func:
	default:
		panic("usp")
	}
	for _, child := range e.Children {
		joinOrder.getColumnBind(child, cb)
	}
}

func getTableRefers(root *LogicalOperator, set UnorderedSet) {
	if root == nil {
		return
	}
	switch root.Typ {
	case LOT_Limit:
		getTableRefers(root.Children[0], set)
	case LOT_Order:
		collectTableRefersOfExprs(root.OrderBys, set)
		getTableRefers(root.Children[0], set)
	case LOT_Scan:
		set.insert(root.Index)
	case LOT_JOIN:
		//if root.JoinTyp == LOT_JoinTypeMARK {
		//	collectTableRefersOfExprs(root.OnConds, set)
		//	getTableRefers(root.Children[0], set)
		//	break
		//}
		collectTableRefersOfExprs(root.OnConds, set)
		for _, child := range root.Children {
			getTableRefers(child, set)
		}
	case LOT_AggGroup:
		set.insert(root.Index, root.Index2)
		collectTableRefersOfExprs(root.GroupBys, set)
		collectTableRefersOfExprs(root.Aggs, set)
		for _, child := range root.Children {
			getTableRefers(child, set)
		}
	case LOT_Project:
		set.insert(root.Index)
		collectTableRefersOfExprs(root.Projects, set)
		getTableRefers(root.Children[0], set)
	case LOT_Filter:
		collectTableRefersOfExprs(root.Filters, set)
		getTableRefers(root.Children[0], set)
	default:
		panic("usp")
	}

}

func collectTableRefersOfExprs(exprs []*Expr, set UnorderedSet) {
	for _, expr := range exprs {
		collectTableRefers(expr, set)
	}
}

func collectTableRefers(e *Expr, set UnorderedSet) {
	if e == nil {
		return
	}
	switch e.Typ {
	case ET_Column:
		index := e.ColRef[0]
		set.insert(index)
	case ET_SConst, ET_IConst, ET_DateConst, ET_IntervalConst, ET_BConst, ET_FConst, ET_NConst, ET_DecConst:

	case ET_Func:

	default:
		panic("usp")
	}
	for _, child := range e.Children {
		collectTableRefers(child, set)
	}
}
