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

import "github.com/daviszhen/plan/pkg/common"

// BooleanSimplificationRule simplifies boolean expressions:
//
//	x AND TRUE → x          TRUE AND x → x
//	x AND FALSE → FALSE      FALSE AND x → FALSE
//	x OR TRUE → TRUE         TRUE OR x → TRUE
//	x OR FALSE → x           FALSE OR x → x
//	NOT(NOT(x)) → x
type BooleanSimplificationRule struct{}

func (r *BooleanSimplificationRule) Root() ExprMatcher {
	return &FuncExprMatcher{
		FuncNameMatcher: NewManyFuncNameMatcher(FuncAnd, FuncOr, FuncNot),
	}
}

func (r *BooleanSimplificationRule) Apply(root *Expr, bindings []*Expr) (*Expr, bool) {
	if root == nil || len(bindings) < 1 {
		return nil, false
	}

	name := root.FuncName()

	switch name {
	case FuncAnd:
		return r.simplifyAnd(root)
	case FuncOr:
		return r.simplifyOr(root)
	case FuncNot:
		return r.simplifyNot(root)
	}
	return nil, false
}

func (r *BooleanSimplificationRule) simplifyAnd(expr *Expr) (*Expr, bool) {
	if len(expr.Children) != 2 {
		return nil, false
	}
	left := expr.Children[0]
	right := expr.Children[1]

	leftConst := isConstBool(left)
	rightConst := isConstBool(right)

	// x AND TRUE → x
	if rightConst && isTrue(right) {
		return left, true
	}
	// TRUE AND x → x
	if leftConst && isTrue(left) {
		return right, true
	}
	// x AND FALSE → FALSE
	if rightConst && isFalse(right) {
		return makeBoolConst(false), true
	}
	// FALSE AND x → FALSE
	if leftConst && isFalse(left) {
		return makeBoolConst(false), true
	}
	return nil, false
}

func (r *BooleanSimplificationRule) simplifyOr(expr *Expr) (*Expr, bool) {
	if len(expr.Children) != 2 {
		return nil, false
	}
	left := expr.Children[0]
	right := expr.Children[1]

	leftConst := isConstBool(left)
	rightConst := isConstBool(right)

	// x OR TRUE → TRUE
	if rightConst && isTrue(right) {
		return makeBoolConst(true), true
	}
	// TRUE OR x → TRUE
	if leftConst && isTrue(left) {
		return makeBoolConst(true), true
	}
	// x OR FALSE → x
	if rightConst && isFalse(right) {
		return left, true
	}
	// FALSE OR x → x
	if leftConst && isFalse(left) {
		return right, true
	}
	return nil, false
}

func (r *BooleanSimplificationRule) simplifyNot(expr *Expr) (*Expr, bool) {
	if len(expr.Children) != 1 {
		return nil, false
	}
	child := expr.Children[0]

	// NOT(TRUE) → FALSE
	if isConstBool(child) && isTrue(child) {
		return makeBoolConst(false), true
	}
	// NOT(FALSE) → TRUE
	if isConstBool(child) && isFalse(child) {
		return makeBoolConst(true), true
	}
	// NOT(NOT(x)) → x
	if child.IsConjunction() || child.IsDisjunction() {
		// Don't simplify NOT(AND/OR) — leave to other rules
		return nil, false
	}
	return nil, false
}

func isConstBool(e *Expr) bool {
	return e != nil && e.Typ == ET_Const && e.ConstValue.Type == ConstTypeBoolean
}

func isTrue(e *Expr) bool {
	return isConstBool(e) && e.ConstValue.Boolean
}

func isFalse(e *Expr) bool {
	return isConstBool(e) && !e.ConstValue.Boolean
}

func makeBoolConst(val bool) *Expr {
	return &Expr{
		Typ:        ET_Const,
		DataTyp:    common.BooleanType(),
		ConstValue: NewBooleanConst(val),
	}
}
