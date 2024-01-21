package main

import (
	"fmt"
	"testing"
)

func Test_Graph(t *testing.T) {
	g := NewGraph()

	g.AddEdge(&GNode{index: 1, db: "db1", name: "t1"}, &GNode{index: 2, db: "db1", name: "t2"})
	g.AddEdge(&GNode{index: 1, db: "db1", name: "t1"}, &GNode{index: 3, db: "db1", name: "t3"})
	g.AddEdge(&GNode{index: 2, db: "db1", name: "t2"}, &GNode{index: 3, db: "db1", name: "t3"})
	fmt.Println(g.String())
}

func TestTreeNode(t *testing.T) {
	root := newTreeNode()
	root.relation = NewJoinRelationSet([]uint64{1, 2, 3})
	root.children[1] = newTreeNode()
	root.children[2] = newTreeNode()
	root.children[3] = newTreeNode()
	fmt.Println(root.String())
}

func TestNewJoinRelationSetManager(t *testing.T) {
	m := NewJoinRelationSetManager()
	set := make(Set)
	set.insert(1, 2, 3, 4, 5, 6, 7)
	m.getRelation(set)
	fmt.Println(m)
}
