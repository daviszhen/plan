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

// ComparisonSimplificationRule simplifies comparison expressions:
//
//	col = NULL   → FALSE
//	col <> NULL  → FALSE
//	NULL = col   → FALSE
//	NULL <> col  → FALSE
//	TRUE = col   → col (boolean reduction)
//	col = TRUE   → col
//	FALSE = col  → NOT(col)
//
// Note: Constant folding of arithmetic children (col = 1+2 → col = 3) is
// handled by ConstantFoldingRule, which runs before this rule.
type ComparisonSimplificationRule struct{}

func (r *ComparisonSimplificationRule) Root() ExprMatcher {
	return &FuncExprMatcher{
		FuncNameMatcher: NewManyFuncNameMatcher(
			FuncEqual, FuncNotEqual, FuncGreater, FuncGreaterEqual,
			FuncLess, FuncLessEqual,
		),
		Children: []ExprMatcher{&AnyExprMatcher{}, &AnyExprMatcher{}},
	}
}

func (r *ComparisonSimplificationRule) Apply(root *Expr, bindings []*Expr) (*Expr, bool) {
	if root == nil || len(bindings) < 3 {
		return nil, false
	}
	// bindings[0]=root, [1]=left, [2]=right
	left := bindings[1]
	right := bindings[2]

	leftConst := left.Typ == ET_Const
	rightConst := right.Typ == ET_Const

	// NULL in comparison always produces NULL/FALSE (not TRUE).
	if leftConst && left.IsNull() {
		return r.simplifyNullComparison(root.FuncName())
	}
	if rightConst && right.IsNull() {
		return r.simplifyNullComparison(root.FuncName())
	}

	// Boolean constant on one side: simplify.
	if leftConst && left.ConstValue.Type == ConstTypeBoolean {
		return r.simplifyBoolConst(root.FuncName(), left.ConstValue.Boolean, right)
	}
	if rightConst && right.ConstValue.Type == ConstTypeBoolean {
		return r.simplifyBoolConst(root.FuncName(), right.ConstValue.Boolean, left, true)
	}

	return nil, false
}

// simplifyNullComparison handles: col = NULL → FALSE, col <> NULL → FALSE.
func (r *ComparisonSimplificationRule) simplifyNullComparison(name string) (*Expr, bool) {
	// For standard comparison: NULL → FALSE
	// IS NULL / IS NOT NULL use special functions and don't match here.
	return makeBoolConst(false), true
}

// simplifyBoolConst handles boolean constant in comparison.
// TRUE = x → x, FALSE = x → NOT(x) (only for equality comparisons).
func (r *ComparisonSimplificationRule) simplifyBoolConst(name string, val bool, other *Expr, constOnRight ...bool) (*Expr, bool) {
	switch name {
	case FuncEqual:
		if val {
			return other, true
		}
		// FALSE = x → NOT(x)
		return r.makeNot(other), true
	case FuncNotEqual:
		if val {
			// TRUE <> x → NOT(x)
			return r.makeNot(other), true
		}
		// FALSE <> x → x
		return other, true
	}
	// For >, <, etc. with boolean, leave as-is (unusual).
	return nil, false
}

func (r *ComparisonSimplificationRule) makeNot(expr *Expr) *Expr {
	binder := FunctionBinder{}
	return binder.BindScalarFunc(FuncNot, []*Expr{expr}, false)
}
