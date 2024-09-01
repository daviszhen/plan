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
