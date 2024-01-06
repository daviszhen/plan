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
