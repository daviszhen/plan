package main

import (
	"fmt"
	"github.com/xlab/treeprint"
)

func (b *Builder) joinOrder(root *LogicalOperator) (*LogicalOperator, error) {
	//var err error

	//1. construct graphs
	//2. split into multiple connected-sub-graph
	//3. decide join order for every connected-sub-graph
	//3.1 baseline algo1: DPSize
	//3.2 baseline algo2 : DPSub
	//3.3 advanced algo : DPhyp
	//4. composite the complete join
	var g *Graph
	var err error
	g, err = constructGraph(root)
	fmt.Println("graph", g.String())
	uf := &UnionFind{}
	uf.build(g)
	csgs := uf.getCsgs()
	fmt.Println("csg count", len(csgs))
	if len(csgs) != 1 {
		panic("not support multiple connected sub graph")
	}
	algo := &DPSize{
		g:    g,
		csgs: csgs,
		uf:   uf,
	}
	root, err = algo.build()
	return root, err
}

type DPSize struct {
	g        *Graph
	csgs     []uint64
	uf       *UnionFind
	bestPlan map[uint64]*LogicalOperator
}

func (dp *DPSize) build() (*LogicalOperator, error) {
	var err error
	var ret *LogicalOperator
	for _, csg := range dp.csgs {
		ret, err = dp.buildcsg(csg)
		if err != nil {
			return nil, err
		}
	}
	return ret, err
}

func (dp *DPSize) buildcsg(csg uint64) (*LogicalOperator, error) {
	var err error
	//evaluate the size of csg
	csgNodes := dp.uf.getCsg(csg)
	if len(csgNodes) > 20 {
		panic("too many nodes in one csg")
	}
	dp.bestPlan = make(map[uint64]*LogicalOperator)
	//init
	seq := NewSeq(csgNodes)
	for _, node := range csgNodes {
		num := seq.segNo(node)
		dp.bestPlan[makeSet(num)] = &LogicalOperator{
			Typ:   LOT_Scan,
			Index: node,
		}
	}
	//enum all set
	for size := 2; size <= len(csgNodes); size++ {
		//size of left plan
		for size1 := 1; size1 < size; size1++ {
			//size of right plan
			//size2 := size - size1

			//TODO: enum set that size is size1 and size2
			//may have more effective method
		}
	}
	return nil, err
}

// make sequence number for nodes
type SeqNumber struct {
	nodes []uint64
	label map[uint64]int
}

func NewSeq(nodes []uint64) *SeqNumber {
	req := &SeqNumber{
		nodes: make([]uint64, 0),
		label: make(map[uint64]int),
	}

	for _, n := range nodes {
		req.nodes = append(req.nodes, n)
		req.label[n] = len(req.nodes) - 1
	}
	return req
}

func (seq *SeqNumber) segNo(n uint64) int {
	return seq.label[n]
}

func (seq *SeqNumber) nodeOfSeqNo(i int) uint64 {
	return seq.nodes[i]
}

func makeSet(node int) uint64 {
	return 1 << node
}

func constructGraph(root *LogicalOperator) (*Graph, error) {
	var err error
	var conds []*Expr
	conds = collectJoinConds(root)
	g := NewGraph()

	for _, cond := range conds {
		left, right := collectRelation(cond, 0)
		//fmt.Println("cond", cond.String())
		//printRelations("left", left)
		//printRelations("right", right)
		if len(left) == 0 || len(right) == 0 {
			continue
		}
		for l := range left {
			for r := range right {
				g.AddEdge(&GNode{index: l}, &GNode{index: r}, cond)
			}
		}
	}

	return g, err
}

func collectJoinConds(root *LogicalOperator) []*Expr {
	var ret []*Expr
	switch root.Typ {
	case LOT_JOIN:
		if root.JoinTyp != LOT_JoinTypeInner {
			panic(fmt.Sprintf("usp join type %v", root.JoinTyp))
		}
		ret = append(ret, root.OnConds...)
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

type Edge struct {
	from  *GNode
	to    *GNode
	conds []*Expr
}

type Graph struct {
	edges map[[2]uint64][]*Expr

	nodes map[uint64]*GNode
}

func NewGraph() *Graph {
	return &Graph{
		edges: make(map[[2]uint64][]*Expr),
		nodes: make(map[uint64]*GNode),
	}
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

func (g *Graph) String() string {
	tree := treeprint.NewWithRoot("Graph")
	nodes := tree.AddMetaBranch("Nodes", len(g.nodes))
	for i, n := range g.nodes {
		nodes.AddMetaNode(fmt.Sprintf("node %d", i), n.String())
	}
	edges := tree.AddMetaBranch("Edges", len(g.edges))
	i := 0
	for e, _ := range g.edges {
		edges.AddMetaNode(fmt.Sprintf("edge %d", i),
			fmt.Sprintf("%s - %s", g.nodes[e[0]].String(), g.nodes[e[1]].String()))
		i++
	}
	return tree.String()
}

type UnionFind struct {
	parent map[uint64]uint64
}

//return roots of csgs
func (uf *UnionFind) getCsgs() []uint64 {
	ret := make([]uint64, 0)
	for n, _ := range uf.parent {
		if uf.find(n) == n {
			ret = append(ret, n)
		}
	}
	return ret
}

//return nodes in same csg that parent node is p
func (uf *UnionFind) getCsg(p uint64) []uint64 {
	ret := make([]uint64, 0)
	for n, _ := range uf.parent {
		if uf.find(n) == p {
			ret = append(ret, n)
		}
	}
	return ret
}

func (uf *UnionFind) build(g *Graph) {
	uf.parent = make(map[uint64]uint64)
	for n, _ := range g.nodes {
		uf.parent[n] = n
	}

	for e, _ := range g.edges {
		uf.union(e[0], e[1])
	}

	for n, _ := range g.nodes {
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