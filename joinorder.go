package main

import (
	"errors"
	"fmt"
	"github.com/xlab/treeprint"
	"math"
	"slices"
	"sort"
	"strings"
)

func (b *Builder) joinOrder(root *LogicalOperator) (*LogicalOperator, error) {
	joinOrder := NewJoinOrderOptimizer()
	return joinOrder.Optimize(root)
}

func (b *Builder) joinOrderImpl(root *LogicalOperator) (*LogicalOperator, error) {
	//1. construct graphs
	//2. split into multiple connected-sub-graph
	//3. decide join order for every connected-sub-graph
	//3.1 baseline algo2 : DPSub
	//3.2 advanced algo : DPhyp
	//4. composite the complete join
	var g *Graph
	var err error
	g, err = constructGraph(root)
	fmt.Println("graph", g.String())
	g.BuildCsg()
	csgs := g.uf.getCsgs()
	//fmt.Println("csg count", len(csgs))
	if len(csgs) != 1 {
		panic("not support multiple connected sub graph")
	}
	algo := &DPSub{
		g:    g,
		csgs: csgs,
	}
	root, err = algo.build()
	return root, err
}

type subplan struct {
	left  uint64
	right uint64
	cost  *Stats
}

type DPSub struct {
	g        *Graph
	csgs     []uint64
	bestPlan map[uint64]*subplan
}

func (dp *DPSub) build() (*LogicalOperator, error) {
	var err error
	var ret *LogicalOperator
	ret, err = dp.buildcsg(dp.csgs[0])
	return ret, err
}

func (dp *DPSub) buildcsg(csg uint64) (*LogicalOperator, error) {
	var err error
	//evaluate the size of csg
	csgNodes := dp.g.uf.getCsg(csg)
	if len(csgNodes) > 20 {
		panic("too many nodes in one csg")
	}
	dp.bestPlan = make(map[uint64]*subplan)
	//init
	seq := NewNumber(csgNodes)
	var table *CatalogTable
	for _, node := range csgNodes {
		no := seq.nodeToNumber(node)
		//get cost of table
		gNode := dp.g.Node(node)
		table, err = tpchCatalog().Table("tpch", gNode.name)
		if err != nil {
			return nil, err
		}
		bitset := numberIntoBitSet(no)
		dp.bestPlan[bitset] = &subplan{
			left:  bitset,
			right: math.MaxUint64,
			cost:  table.Stats.Copy(),
		}
	}
	//enum all set
	for s := 1; s < (1 << len(csgNodes)); s++ {
		//check s is connected
		if !dp.bitsetIsConnected(s, seq) {
			//fmt.Printf("s %d is not connected\n", s)
			continue
		}
		for left := s & (s - 1); left != 0; left = s & (left - 1) {
			right := s & (^left)
			if left&right != 0 {
				panic("left intersect right != Empty")
			}
			if left|right != s {
				panic("left union right != s")
			}

			//bestPlan
			leftPlan := dp.bestPlan[uint64(left)]
			rightPlan := dp.bestPlan[uint64(right)]
			if leftPlan == nil || rightPlan == nil {
				//panic(fmt.Sprintf("left or right plan is nil. %v %v", leftPlan == nil, rightPlan == nil))
				continue
			}

			//check left, right is connected independently
			if !dp.bitsetIsConnected(left, seq) ||
				!dp.bitsetIsConnected(right, seq) {
				fmt.Printf("left %d or right %d is not connected\n", left, right)
				continue
			}

			//check left is connected to the right
			//is really necessary?
			if !dp.twoBitsetAreConnected(left, right, seq) {
				fmt.Printf("left %d are not connected to right %d \n", left, right)
				continue
			}

			//cost
			newRowCount := leftPlan.cost.RowCount * rightPlan.cost.RowCount
			if pre, has := dp.bestPlan[uint64(s)]; !has || pre != nil && pre.cost.RowCount > newRowCount {
				curPlan := &subplan{
					left:  uint64(left),
					right: uint64(right),
					cost: &Stats{
						RowCount: newRowCount,
					},
				}
				dp.bestPlan[uint64(s)] = curPlan
			}
		}
	}
	//construct best plan
	finalPlan := dp.constructBestPlan((1<<len(csgNodes))-1, seq)
	return finalPlan, err
}

func (dp *DPSub) constructBestPlan(s int, seq *Number) *LogicalOperator {
	if sub, has := dp.bestPlan[uint64(s)]; !has {
		panic(fmt.Sprintf("no best plan for %d", s))
	} else {
		if sub.right == math.MaxUint64 {
			node := seq.numberToNode(bitsetToNumber(sub.left))
			scan := dp.g.Scan(node)
			return &LogicalOperator{
				Typ:      LOT_Scan,
				Index:    node,
				Database: scan.Database,
				Table:    scan.Table,
				Filters:  scan.Filters,
				Stats:    sub.cost,
			}
		} else {
			left := dp.constructBestPlan(int(sub.left), seq)
			right := dp.constructBestPlan(int(sub.right), seq)
			//fill join conds
			conds := dp.constructJoinConds(int(sub.left), int(sub.right), seq)
			return &LogicalOperator{
				Typ:      LOT_JOIN,
				JoinTyp:  LOT_JoinTypeInner,
				OnConds:  conds,
				Stats:    sub.cost.Copy(),
				Children: []*LogicalOperator{left, right},
			}
		}
	}
}

func (dp *DPSub) constructJoinConds(left, right int, seq *Number) []*Expr {
	//nodes in left
	leftNodes := dp.nodesOfSet(left, seq)
	//nodes in right
	rightNodes := dp.nodesOfSet(right, seq)
	//construct join conds
	conds := make([]*Expr, 0)
	for _, l := range leftNodes {
		for _, r := range rightNodes {
			conds = append(conds, dp.g.GetMetaOfEdge(l, r)...)
		}
	}
	return conds
}

func (dp *DPSub) nodesOfSet(s int, seq *Number) []uint64 {
	nodes := make([]uint64, 0)
	for i := 0; i < 32; i++ {
		no := 1 << i
		if no&s != 0 {
			node := seq.numberToNode(i)
			nodes = append(nodes, node)
		}
	}
	return nodes
}

func (dp *DPSub) twoBitsetAreConnected(left, right int, seq *Number) bool {
	leftNodes := dp.nodesOfSet(left, seq)
	rightNodes := dp.nodesOfSet(right, seq)
	return dp.g.TwoGroupNodesAreConnected(leftNodes, rightNodes)
}

func (dp *DPSub) bitsetIsConnected(s int, seq *Number) bool {
	//nodes in s
	nodes := dp.nodesOfSet(s, seq)
	//check
	news := seq.nodesToNumberBitSet(nodes)
	if news != s {
		panic("new != s")
	}
	//check nodes is connected
	return dp.g.NodesAreConnected(nodes)
}

// make number for nodes
// node -> number
type Number struct {
	nodes []uint64       // number -> node
	label map[uint64]int // node -> number
}

func NewNumber(nodes []uint64) *Number {
	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i] < nodes[j]
	})
	req := &Number{
		nodes: make([]uint64, 0),
		label: make(map[uint64]int),
	}

	for _, n := range nodes {
		req.nodes = append(req.nodes, n)
		req.label[n] = len(req.nodes) - 1
	}
	return req
}

func (seq *Number) nodeToNumber(n uint64) int {
	return seq.label[n]
}

func (seq *Number) numberToNode(i int) uint64 {
	return seq.nodes[i]
}

func (seq *Number) nodesToNumberBitSet(nodes []uint64) int {
	ret := uint64(0)
	for _, n := range nodes {
		ret |= numberIntoBitSet(seq.nodeToNumber(n))
	}
	return int(ret)
}

func numberIntoBitSet(node int) uint64 {
	return 1 << node
}

func bitsetToNumber(bitset uint64) int {
	if bitset == 0 {
		panic("bitset is 0")
	}
	if bitset&(bitset-1) != 0 {
		panic("bitset is not power of 2")
	}
	if bitset == 1 {
		return 0
	}
	if bitset == 2 {
		return 1
	}
	ret := 0
	for bitset != 0 {
		bitset = bitset >> 1
		ret++
	}
	return ret - 1
}

func constructGraph(root *LogicalOperator) (*Graph, error) {
	var err error
	//var conds []*Expr
	g := NewGraph()

	//collect scan nodes
	collectScanNodes(root, g.scans)

	//collect join conditions
	//conds = collectJoinConds(root)
	//for _, cond := range conds {
	//	left, right := collectRelation(cond, 0)
	//	//fmt.Println("cond", cond.String())
	//	//printRelations("left", left)
	//	//printRelations("right", right)
	//	if len(left) == 0 || len(right) == 0 {
	//		continue
	//	}
	//	for l, lv := range left {
	//		if lv == nil {
	//			panic("must have expr for left")
	//		}
	//		//_, ltable := getTableNameFromExprs(lv)
	//		for r, rv := range right {
	//			if rv == nil {
	//				panic("must have expr for right")
	//			}
	//			//_, rtable := getTableNameFromExprs(rv)
	//			lscan := g.Scan(l)
	//			rscan := g.Scan(r)
	//			g.AddEdge(
	//				&GNode{index: l, db: lscan.Database, name: lscan.Table},
	//				&GNode{index: r, db: rscan.Database, name: rscan.Table},
	//				cond)
	//		}
	//	}
	//}

	return g, err
}

func collectScanNodes(root *LogicalOperator, scans map[uint64]*LogicalOperator) {
	switch root.Typ {
	case LOT_Scan:
		scans[root.Index] = root
	default:
	}
	for _, c := range root.Children {
		collectScanNodes(c, scans)
	}
}

func collectJoinConds(root *LogicalOperator) []*Expr {
	var ret []*Expr
	switch root.Typ {
	case LOT_JOIN:
		if root.JoinTyp != LOT_JoinTypeInner {
			panic(fmt.Sprintf("usp join type %v", root.JoinTyp))
		}
		ret = append(ret, root.OnConds...)
	case LOT_Scan:
	default:
		//TODO: neither join nor scan under join
	}
	for _, c := range root.Children {
		t := collectJoinConds(c)
		ret = append(ret, t...)
	}
	return ret
}

type GNode struct {
	//relation
	db    string
	name  string
	index uint64
}

func (g *GNode) Equal(o *GNode) bool {
	return g.index == o.index &&
		g.db == o.db &&
		g.name == o.name
}

func (g *GNode) Unique() uint64 {
	return g.index
}

func (g *GNode) String() string {
	return fmt.Sprintf("%d - %s.%s", g.index, g.db, g.name)
}

type Graph struct {
	edges map[[2]uint64][]*Expr
	nodes map[uint64]*GNode
	scans map[uint64]*LogicalOperator

	uf *UnionFind
}

func NewGraph() *Graph {
	return &Graph{
		edges: make(map[[2]uint64][]*Expr),
		nodes: make(map[uint64]*GNode),
		scans: make(map[uint64]*LogicalOperator),
		uf:    &UnionFind{},
	}
}

func (g *Graph) BuildCsg() {
	g.uf.build(g)
}

func (g *Graph) Scan(n uint64) *LogicalOperator {
	return g.scans[n]
}

func (g *Graph) Node(n uint64) *GNode {
	return g.nodes[n]
}

func (g *Graph) AddNode(n *GNode) {
	if e, has := g.nodes[n.Unique()]; !has {
		g.nodes[n.Unique()] = n
	} else if !e.Equal(n) {
		panic(fmt.Sprintf("node conflict. %v %v", e, n))
	}
}

func (g *Graph) AddEdge(from, to *GNode, conds ...*Expr) {
	g.AddNode(from)
	g.AddNode(to)
	if from.Unique() > to.Unique() {
		from, to = to, from
	} else if from.Unique() == to.Unique() {
		panic("no loop")
	}
	p := [2]uint64{from.Unique(), to.Unique()}
	if _, has := g.edges[p]; !has {
		g.edges[p] = conds
	}
}

func (g *Graph) HasEdge(from, to uint64) bool {
	if from > to {
		from, to = to, from
	}
	p := [2]uint64{from, to}
	_, has := g.edges[p]
	return has
}

func (g *Graph) GetMetaOfEdge(from, to uint64) []*Expr {
	if from > to {
		from, to = to, from
	}
	p := [2]uint64{from, to}
	ret, _ := g.edges[p]
	return ret
}

// NodesAreConnected checks nodes are connected by some edges.
// the edges are constructed exactly by nodes.
// there is no node in edges that is not in nodes
func (g *Graph) NodesAreConnected(nodes []uint64) bool {
	if len(nodes) == 0 || len(nodes) == 1 {
		return true
	}
	if len(nodes) == 2 {
		return g.HasEdge(nodes[0], nodes[1])
	}
	//bfs
	queue := make([]uint64, 0)
	visited := make(map[uint64]bool)
	queue = append(queue, nodes[0])
	visited[nodes[0]] = true
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		//choose one neighbor
		for _, next := range nodes {
			if next == cur {
				continue
			}
			if _, has := visited[next]; has {
				continue
			}
			if g.HasEdge(cur, next) {
				queue = append(queue, next)
				visited[next] = true
			}
		}
	}
	return len(visited) == len(nodes)
}

// TwoGroupNodesAreConnected checks two groups of nodes are connected by some edges.
// the edges are constructed exactly by nodes.
// there is no node in edges that is not in nodes
func (g *Graph) TwoGroupNodesAreConnected(group1, group2 []uint64) bool {
	if len(group1) == 0 || len(group2) == 0 {
		return true
	}
	for _, n1 := range group1 {
		for _, n2 := range group2 {
			if g.HasEdge(n1, n2) {
				return true
			}
		}
	}
	return false
}

func (g *Graph) String() string {
	tree := treeprint.NewWithRoot("Graph")
	nodes := tree.AddMetaBranch("Nodes", len(g.nodes))
	for i, n := range g.nodes {
		nodes.AddMetaNode(fmt.Sprintf("node %d", i), n.String())
	}
	edges := tree.AddMetaBranch("Edges", len(g.edges))
	i := 0
	for e := range g.edges {
		edges.AddMetaNode(fmt.Sprintf("edge %d", i),
			fmt.Sprintf("%s - %s", g.nodes[e[0]].String(), g.nodes[e[1]].String()))
		i++
	}
	return tree.String()
}

type UnionFind struct {
	parent map[uint64]uint64
}

// return roots of csgs
func (uf *UnionFind) getCsgs() []uint64 {
	ret := make([]uint64, 0)
	for n := range uf.parent {
		if uf.find(n) == n {
			ret = append(ret, n)
		}
	}
	return ret
}

// return nodes in same csg that parent node is p
func (uf *UnionFind) getCsg(p uint64) []uint64 {
	ret := make([]uint64, 0)
	for n := range uf.parent {
		if uf.find(n) == p {
			ret = append(ret, n)
		}
	}
	return ret
}

func (uf *UnionFind) build(g *Graph) {
	uf.parent = make(map[uint64]uint64)
	for n := range g.nodes {
		uf.parent[n] = n
	}

	for e := range g.edges {
		uf.union(e[0], e[1])
	}

	for n := range g.nodes {
		uf.find(n)
	}
}

func (uf *UnionFind) union(x, y uint64) {
	uf.parent[uf.find(x)] = uf.find(y)
}

func (uf *UnionFind) find(n uint64) uint64 {
	var p uint64
	var has bool
	if p, has = uf.parent[n]; !has {
		panic(fmt.Sprintf("unionfind no such node %d", p))
	}
	if p == n {
		return n
	} else {
		pp := uf.find(p)
		if pp != p {
			uf.parent[n] = pp
		}
		return pp
	}
}

type SingleJoinRelation struct {
	op     *LogicalOperator
	parent *LogicalOperator
}

type JoinRelationSet struct {
	relations []uint64
}

func NewJoinRelationSet(rels []uint64) *JoinRelationSet {
	ret := &JoinRelationSet{relations: copy(rels)}
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

func (jrs *JoinRelationSet) String() string {
	if jrs == nil {
		return ""
	}
	bb := strings.Builder{}
	bb.WriteString("[")
	for i, r := range jrs.relations {
		if i > 0 {
			bb.WriteString(", ")
		}
		bb.WriteString(fmt.Sprintf("%d", r))
	}
	bb.WriteString("]")
	return bb.String()
}

type Item[T uint64] struct {
	vec []T
	val *JoinRelationSet
}

func (item *Item[T]) append(v ...T) {
	item.vec = append(item.vec, v...)
	item.sort()
}

func (item *Item[T]) sort() {
	sort.Slice(item.vec, func(i, j int) bool {
		return item.vec[i] < item.vec[j]
	})
}

func (item Item[T]) less(o *Item[T]) bool {
	alen := len(item.vec)
	blen := len(o.vec)
	l := Min(alen, blen)
	for i := 0; i < l; i++ {
		if item.vec[i] < o.vec[i] {
			return true
		}
	}
	if alen < blen {
		return true
	}
	return false
}

func itemLess[T uint64](a, b *Item[T]) bool {
	return a.less(b)
}

type Set map[uint64]bool

func (set Set) orderedKeys() []uint64 {
	keys := make([]uint64, 0, len(set))
	for key, _ := range set {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		return keys[i] < keys[j]
	})
	return keys
}

func (set Set) insert(keys ...uint64) {
	for _, key := range keys {
		set[key] = true
	}
}

func (set Set) find(key uint64) bool {
	_, has := set[key]
	return has
}

func (set Set) clear() {
	for key, _ := range set {
		delete(set, key)
	}
}

func (set Set) empty() bool {
	return len(set) == 0
}

func (set Set) size() int {
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
	dedup := make(Set)
	dedup.insert(left.relations...)
	dedup.insert(right.relations...)
	return NewJoinRelationSet(dedup.orderedKeys())
}

func (jrsm *JoinRelationSetManager) getRelation(relations Set) *JoinRelationSet {
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
	set := make(Set)
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
		newPrefix := copy(prefix)
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

func (graph *QueryGraph) GetNeighbors(node *JoinRelationSet, excludeSet Set) (ret []uint64) {
	dedup := make(Set)
	graph.enumNeighbors(node, func(info *neighborInfo) bool {
		if !joinRelationSetIsExcluded(info.neighbor, excludeSet) {
			dedup.insert(info.neighbor.relations[0])
		}
		return false
	})
	for k, _ := range dedup {
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

func joinRelationSetIsExcluded(node *JoinRelationSet, set Set) bool {
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
	relations []*SingleJoinRelation
	//table index -> relation number
	relationMapping map[uint64]uint64
	filters         []*Expr
	filterInfos     []*FilterInfo
	setManager      *JoinRelationSetManager
	queryGraph      *QueryGraph
	plans           planMap
	estimator       *CardinalityEstimator
}

func NewJoinOrderOptimizer() *JoinOrderOptimizer {
	return &JoinOrderOptimizer{
		relationMapping: make(map[uint64]uint64),
		setManager:      NewJoinRelationSetManager(),
		plans:           make(planMap),
		queryGraph:      NewQueryGraph(),
		estimator:       NewCardinalityEstimator(),
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
			joinOrder.filters = append(joinOrder.filters, filterOp.OnConds...)
		case LOT_Filter:
			joinOrder.filters = append(joinOrder.filters, filterOp.Filters...)
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
		case ET_And, ET_Or, ET_Equal, ET_Like, ET_GreaterEqual, ET_Less:
			leftRelations := make(map[uint64]bool)
			rightRelations := make(map[uint64]bool)
			joinOrder.collectRelation(filter.Children[0], leftRelations)
			joinOrder.collectRelation(filter.Children[1], rightRelations)
			joinOrder.getColumnBind(filter.Children[0], &info.leftBinding)
			joinOrder.getColumnBind(filter.Children[1], &info.rightBinding)
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
		default:
			panic(fmt.Sprintf("usp operator type %d", filter.Typ))
		}
	}

	fmt.Println("join set manager\n", joinOrder.setManager)
	fmt.Println("query graph\n", joinOrder.queryGraph)

	//prepare for dp algorithm
	var nodesOpts []*NodeOp
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
		fmt.Println("node op set", nodeOp.node.set)
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
			children = erase(children, i)
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
				check := isSubset(left.set, filter.leftSet) && isSubset(right.set, filter.rightSet) ||
					isSubset(left.set, filter.rightSet) && isSubset(right.set, filter.leftSet)
				if !check {
					return nil, errors.New("filter sets are not intersects")
				}
				cond := &Expr{Typ: condition.Typ}
				invert := !isSubset(left.set, filter.leftSet)
				if !invert {
					cond.Children = []*Expr{condition.Children[0], condition.Children[1]}
				} else {
					cond.Children = []*Expr{condition.Children[1], condition.Children[0]}
				}
				if invert {
					//TODO:
				}
				resultOp.OnConds = append(resultOp.OnConds, cond)
				//remove this filter
				joinOrder.filters[filter.filterIndex] = nil
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
				if leftNode == nil || info.leftSet == nil {
					resultOp = pushFilter(resultOp, filter)
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
					resultOp = pushFilter(resultOp, filter)
					joinOrder.filters[info.filterIndex] = nil
					continue
				}
				cond := &Expr{Typ: filter.Typ}
				if !invert {
					cond.Children[0], cond.Children[1] = filter.Children[0], filter.Children[1]
				} else {
					cond.Children[0], cond.Children[1] = filter.Children[1], filter.Children[0]
				}
				if invert {
					//TODO
				}
				cur := resultOp
				if cur.Typ == LOT_Filter {
					cur = cur.Children[0]
				}
				if cur.Typ == LOT_JOIN && cur.JoinTyp == LOT_JoinTypeCross {
					next := &LogicalOperator{
						Typ:      LOT_JOIN,
						JoinTyp:  LOT_JoinTypeInner,
						Children: cur.Children,
						OnConds:  []*Expr{cond},
					}
					if cur == resultOp {
						resultOp = next
					} else {
						resultOp.Children[0] = next
					}
				} else {
					resultOp.OnConds = append(resultOp.OnConds, cond)
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

	var extractedRelations []*LogicalOperator
	for _, rel := range joinOrder.relations {
		exRel, err := joinOrder.extractJoinRelation(rel)
		if err != nil {
			return nil, err
		}
		extractedRelations = append(extractedRelations, exRel)
	}

	joinTree, err := joinOrder.generateJoins(extractedRelations, node)
	if err != nil {
		return nil, err
	}
	//pushdown remaining filters
	for _, filter := range joinOrder.filters {
		if filter != nil {
			joinTree.op = pushFilter(joinTree.op, filter)
		}
	}

	if rootIsJoin {
		return joinTree.op, nil
	}
	if len(joinTree.op.Children) != 1 {
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

func (joinOrder *JoinOrderOptimizer) greedy() error {
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
			bestConn, err := joinOrder.emitPair(left, right, conns)
			if err != nil {
				return err
			}
			bestLeft = int(smallestIndex[0])
			bestRight = int(smallestIndex[1])
			err = joinOrder.updateDPTree(bestConn)
			if err != nil {
				return err
			}
			if bestLeft > bestRight {
				bestLeft, bestRight = bestRight, bestLeft
			}
		}
		joinRelations = erase(joinRelations, bestRight)
		joinRelations = erase(joinRelations, bestLeft)
		joinRelations = append(joinRelations, best.set)
	}
	return nil
}

func (joinOrder *JoinOrderOptimizer) updateDPTree(newPlan *JoinNode) error {
	//TODO: full plan
	newSet := newPlan.set
	excludeSet := make(Set)
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

func (joinOrder *JoinOrderOptimizer) getAllNeighborSets(excludeSet Set, neighbors []uint64) []Set {
	sort.Slice(neighbors, func(i, j int) bool {
		return neighbors[i] < neighbors[j]
	})
	var ret []Set
	var added []Set
	for _, nei := range neighbors {
		x := make(Set)
		x.insert(nei)
		added = append(added, x)
		y := make(Set)
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

func (joinOrder *JoinOrderOptimizer) addSuperSets(current []Set, allNeighbors []uint64) []Set {
	var ret []Set
	for _, nei := range allNeighbors {
		for _, cur := range current {
			keys := cur.orderedKeys()
			if keys[len(keys)-1] >= nei {
				continue
			}
			if _, has := cur[nei]; !has {
				newSet := make(Set)
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
		if len(newSet.relations) == len(joinOrder.relations) {
			//TODO:
		}
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
			optimizer := NewJoinOrderOptimizer()
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
		} else {
			nonReorder = true
		}
	}

	if nonReorder {
		//setop or non-inner join
		cmaps := make([]ColumnBindMap, 0)
		for i, child := range op.Children {
			optimizer := NewJoinOrderOptimizer()
			op.Children[i], err = optimizer.Optimize(child)
			if err != nil {
				return false, nil, err
			}
			cmap := make(ColumnBindMap)
			optimizer.estimator.CopyRelationMap(cmap)
			cmaps = append(cmaps, cmap)
		}
		//get all table
		tables := make(Set)
		getTableRefers(root, tables)
		relation := &SingleJoinRelation{op: root, parent: parent}
		relId := len(joinOrder.relations)
		for tabId, _ := range tables {
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
		optimizer := NewJoinOrderOptimizer()
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
	return
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
			if joinOrder.relations[relId].op.Index != index {
				panic("no such relation")
			}
		}
	case ET_Equal,
		ET_And,
		ET_Like,
		ET_GreaterEqual,
		ET_Less,
		ET_Or,
		ET_Sub,
		ET_Mul:
	case ET_SConst, ET_IConst:
	case ET_Between:
		joinOrder.collectRelation(e.Between, set)
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
	case ET_Equal,
		ET_And,
		ET_Like,
		ET_GreaterEqual,
		ET_Less,
		ET_Or,
		ET_Sub,
		ET_Mul:

	case ET_SConst, ET_IConst:
	case ET_Between:
		joinOrder.getColumnBind(e.Between, cb)
	default:
		panic("usp")
	}
	for _, child := range e.Children {
		joinOrder.getColumnBind(child, cb)
	}
}

func getTableRefers(root *LogicalOperator, set Set) {
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
		if root.JoinTyp == LOT_JoinTypeMARK {
			collectTableRefersOfExprs(root.OnConds, set)
			getTableRefers(root.Children[0], set)
			break
		}
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

func collectTableRefersOfExprs(exprs []*Expr, set Set) {
	for _, expr := range exprs {
		collectTableRefers(expr, set)
	}
}

func collectTableRefers(e *Expr, set Set) {
	if e == nil {
		return
	}
	switch e.Typ {
	case ET_Column:
		index := e.ColRef[0]
		set.insert(index)
	case ET_Equal,
		ET_And,
		ET_Like,
		ET_NotLike,
		ET_Greater,
		ET_GreaterEqual,
		ET_Less,
		ET_Or,
		ET_Sub,
		ET_Mul:

	case ET_SConst, ET_IConst, ET_DateConst, ET_IntervalConst, ET_BConst, ET_FConst:

	case ET_Func:
	case ET_Between:
		collectTableRefers(e.Between, set)
	default:
		panic("usp")
	}
	for _, child := range e.Children {
		collectTableRefers(child, set)
	}
}
