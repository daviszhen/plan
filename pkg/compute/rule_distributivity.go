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

// DistributivityRule factorizes OR expressions with common AND children:
//
//	(A AND B) OR (A AND C) → A AND (B OR C)
//
// This delegates to the existing distributeExpr function in expr_rule.go.
// The rule is placed in the ExpressionRewriter pipeline so it is re-applied
// after join reordering and filter pushdown, which may expose new opportunities.
type DistributivityRule struct{}

func (r *DistributivityRule) Root() ExprMatcher {
	return NewDisjunctionMatcher()
}

func (r *DistributivityRule) Apply(root *Expr, bindings []*Expr) (*Expr, bool) {
	if root == nil {
		return nil, false
	}
	result := distributeExpr(root)
	if result == root {
		return nil, false
	}
	return result, true
}
