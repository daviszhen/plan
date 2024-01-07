package main

import (
	"fmt"
	"github.com/xlab/treeprint"
	"math"
	"sort"
)

func (b *Builder) joinOrder(root *LogicalOperator) (*LogicalOperator, error) {
	var err error
	var sub *LogicalOperator
	switch root.Typ {
	case LOT_JOIN:
		if root.JoinTyp != LOT_JoinTypeInner {
			panic(fmt.Sprintf("usp join type %v in joinorder", root.JoinTyp))
		}
		//TODO: maybe it is wrong
		return b.joinOrderImpl(root)
	}

	for i, child := range root.Children {
		sub, err = b.joinOrder(child)
		if err != nil {
			return nil, err
		}
		root.Children[i] = sub
	}
	return root, err
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
	var conds []*Expr
	g := NewGraph()

	//collect scan nodes
	collectScanNodes(root, g.scans)

	//collect join conditions
	conds = collectJoinConds(root)
	for _, cond := range conds {
		left, right := collectRelation(cond, 0)
		//fmt.Println("cond", cond.String())
		//printRelations("left", left)
		//printRelations("right", right)
		if len(left) == 0 || len(right) == 0 {
			continue
		}
		for l, lv := range left {
			if lv == nil {
				panic("must have expr for left")
			}
			//_, ltable := getTableNameFromExprs(lv)
			for r, rv := range right {
				if rv == nil {
					panic("must have expr for right")
				}
				//_, rtable := getTableNameFromExprs(rv)
				lscan := g.Scan(l)
				rscan := g.Scan(r)
				g.AddEdge(
					&GNode{index: l, db: lscan.Database, name: lscan.Table},
					&GNode{index: r, db: rscan.Database, name: rscan.Table},
					cond)
			}
		}
	}

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
