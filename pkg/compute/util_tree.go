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

package compute

import (
	"fmt"
	"strings"

	"github.com/xlab/treeprint"

	"github.com/daviszhen/plan/pkg/storage"
)

func WriteMapTree[K comparable, V any](tree treeprint.Tree, m map[K]V) {
	for k, v := range m {
		tree.AddNode(fmt.Sprintf("%v : %v", k, v))
	}
}

func WriteExprsTree(tree treeprint.Tree, exprs []*Expr) {
	for i, e := range exprs {
		p := tree.AddBranch(fmt.Sprintf("%d", i))
		p.AddNode(e.String())
	}
}

func WriteExprTree(tree treeprint.Tree, expr *Expr) {
	tree.AddNode(expr.String())
}

func listColDefsToTree(tree treeprint.Tree, colDefs []*storage.ColumnDefinition) {
	for _, colDef := range colDefs {
		consStrs := make([]string, 0)
		for _, cons := range colDef.Constraints {
			consStrs = append(consStrs, cons.String())
		}
		tree.AddMetaNode(
			fmt.Sprintf("%v %v", colDef.Name, colDef.Type),
			strings.Join(consStrs, ","),
		)
	}
}
