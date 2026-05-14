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
// Usage:
//   rewriter := NewExpressionRewriter([]RewriteRule{
//       &ArithmeticSimplificationRule{},
//       &ConstantFoldingRule{},
//   })
//   rewritten := rewriter.Rewrite(expr)
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
type ExpressionRewriter struct {
	rules []RewriteRule
}

// NewExpressionRewriter creates a rewriter with the given rules.
func NewExpressionRewriter(rules []RewriteRule) *ExpressionRewriter {
	return &ExpressionRewriter{rules: rules}
}

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
