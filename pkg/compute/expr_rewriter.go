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
// Expression Rewriter Framework
// Mirrors DuckDB ExpressionRewriter + Rule.
//
// ExpressionRewriter implements LogicalOperatorVisitor so it can
// traverse a LogicalOperator tree and rewrite all expressions
// within the visitor framework.
//
// Usage (standalone expression):
//
//	rewriter := NewExpressionRewriter([]RewriteRule{...})
//	rewritten := rewriter.Rewrite(expr)
//
// Usage (whole plan via visitor):
//
//	rewriter := NewExpressionRewriter([]RewriteRule{...})
//	rewriter.Self = rewriter
//	rewriter.VisitOperator(root)
// ============================================================

// RewriteRule represents a single expression rewrite rule.
// Each rule has a matcher tree (Root) and an Apply method.
type RewriteRule interface {
	// Root returns the expression matcher that determines whether
	// this rule applies to a given expression node.
	Root() ExprMatcher
	// Apply performs the rewrite. It receives the matched root expression
	// and all captured bindings (bindings[0] is the root).
	// Returns (newExpr, true) if a rewrite was performed,
	// or (nil, false) if no rewrite should happen.
	Apply(root *Expr, bindings []*Expr) (*Expr, bool)
}

// ExpressionRewriter applies a set of rewrite rules to an expression tree
// until a fixed point is reached (no more changes).
// It also implements LogicalOperatorVisitor so it can traverse a plan tree
// and rewrite all expressions in-place, mirroring DuckDB's approach.
type ExpressionRewriter struct {
	BaseVisitor
	rules []RewriteRule
}

// NewExpressionRewriter creates a rewriter with the given rules.
// Callers must set Self before using the visitor methods:
//
//	rw := NewExpressionRewriter(rules)
//	rw.Self = rw
//	rw.VisitOperator(root)
func NewExpressionRewriter(rules []RewriteRule) *ExpressionRewriter {
	return &ExpressionRewriter{rules: rules}
}

// --- LogicalOperatorVisitor implementation ---

// VisitOperator overrides BaseVisitor.VisitOperator.
// It first visits children (bottom-up), then visits all expressions
// on the current operator. Mirrors DuckDB ExpressionRewriter::VisitOperator.
func (rw *ExpressionRewriter) VisitOperator(op *LogicalOperator) {
	rw.VisitOperatorChildren(op)
	rw.VisitOperatorExpressions(op)
}

// VisitExpression overrides BaseVisitor.VisitExpression.
// It applies all rewrite rules to the expression until a fixed point
// is reached (no more changes). Mirrors DuckDB ExpressionRewriter::VisitExpression.
func (rw *ExpressionRewriter) VisitExpression(expr **Expr) {
	if expr == nil || *expr == nil || len(rw.rules) == 0 {
		return
	}
	changed := true
	for changed {
		changed = false
		*expr = rw.rewriteNode(*expr, &changed)
	}
}

// --- Expression-level rewrite API ---

// Rewrite applies all rules to the expression tree until no rule fires.
// Rules are applied in a bottom-up order (children first, then parent).
func (rw *ExpressionRewriter) Rewrite(expr *Expr) *Expr {
	if expr == nil || len(rw.rules) == 0 {
		return expr
	}
	changed := true
	for changed {
		changed = false
		expr = rw.rewriteNode(expr, &changed)
	}
	return expr
}

// rewriteNode recursively rewrites a single node and its children.
func (rw *ExpressionRewriter) rewriteNode(expr *Expr, changed *bool) *Expr {
	if expr == nil {
		return nil
	}
	// Bottom-up: rewrite children first.
	for i, child := range expr.Children {
		expr.Children[i] = rw.rewriteNode(child, changed)
	}
	// Try each rule on the current node.
	for _, rule := range rw.rules {
		var bindings []*Expr
		if ok, newBindings := rule.Root().Match(expr, bindings); ok {
			if result, modified := rule.Apply(expr, newBindings); modified {
				*changed = true
				return result
			}
		}
	}
	return expr
}

// RewriteOnce applies rules for exactly one pass (no fixed-point loop).
// Useful when the caller wants to control the iteration externally.
func (rw *ExpressionRewriter) RewriteOnce(expr *Expr) *Expr {
	if expr == nil || len(rw.rules) == 0 {
		return expr
	}
	changed := false
	return rw.rewriteNode(expr, &changed)
}
