// Copyright 2023-2026 daviszhen
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

// ============================================================
// ExprIterator — generic expression tree traversal
// Mirrors DuckDB ExpressionIterator.
// ============================================================

// ExprEnumerateChildren calls f for every direct child of expr.
func ExprEnumerateChildren(expr *Expr, f func(child *Expr)) {
	if expr == nil {
		return
	}
	for _, child := range expr.Children {
		f(child)
	}
}

// ExprEnumerateNodes calls f for every node in the expression tree
// in pre-order (root before children).
func ExprEnumerateNodes(expr *Expr, f func(node *Expr)) {
	if expr == nil {
		return
	}
	f(expr)
	for _, child := range expr.Children {
		ExprEnumerateNodes(child, f)
	}
}

// ExprEnumerateLeafs calls f for every leaf node in the expression tree.
func ExprEnumerateLeafs(expr *Expr, f func(leaf *Expr)) {
	if expr == nil {
		return
	}
	if len(expr.Children) == 0 {
		f(expr)
		return
	}
	for _, child := range expr.Children {
		ExprEnumerateLeafs(child, f)
	}
}

// TransformExpressions performs a bottom-up transformation of the expression tree.
// f receives each node after its children have been transformed.
// If f returns non-nil, the returned expression replaces the current node.
// If f returns nil, the current node is kept (but its children may have changed).
func TransformExpressions(expr *Expr, f func(node *Expr) *Expr) *Expr {
	if expr == nil {
		return nil
	}
	for i, child := range expr.Children {
		expr.Children[i] = TransformExpressions(child, f)
	}
	if replacement := f(expr); replacement != nil {
		return replacement
	}
	return expr
}

// ReplaceExpression replaces all occurrences of 'from' with 'to' in the expression tree.
// Uses pointer equality (same *Expr instance).
func ReplaceExpression(expr *Expr, from, to *Expr) *Expr {
	if expr == nil {
		return nil
	}
	if expr == from {
		return to
	}
	for i, child := range expr.Children {
		expr.Children[i] = ReplaceExpression(child, from, to)
	}
	return expr
}
