package main

import (
	"fmt"
	"testing"

	"github.com/xlab/treeprint"
)

func TestTreePrint(t *testing.T) {
	tree := treeprint.New()
	tree.AddNode("root")
	proj := tree.AddMetaBranch("abc", "Projects:")
	proj.AddNode("n1")
	sub := proj.AddBranch("n2")
	sub.AddNode("n3")
	sub.AddNode("n4")
	tree.AddMetaBranch("def", "Projects2:")

	fmt.Println(tree.String())
}
