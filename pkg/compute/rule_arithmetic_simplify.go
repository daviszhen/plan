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

// ArithmeticSimplificationRule simplifies arithmetic expressions:
//
//	x + 0 → x       x - 0 → x
//	0 + x → x       x * 1 → x
//	x * 0 → 0       1 * x → x
type ArithmeticSimplificationRule struct{}

func (r *ArithmeticSimplificationRule) Root() ExprMatcher {
	return &FuncExprMatcher{
		FuncNameMatcher: NewManyFuncNameMatcher(FuncAdd, FuncSubtract, FuncMultiply),
		Children:        []ExprMatcher{&AnyExprMatcher{}, &AnyExprMatcher{}},
	}
}

func (r *ArithmeticSimplificationRule) Apply(root *Expr, bindings []*Expr) (*Expr, bool) {
	if root == nil || len(bindings) < 3 {
		return nil, false
	}
	// bindings[0] = root, [1] = left child, [2] = right child
	left := bindings[1]
	right := bindings[2]

	name := root.FuncName()

	// Identify which (if any) child is a constant zero or one.
	leftIsConst := left.Typ == ET_Const
	rightIsConst := right.Typ == ET_Const

	switch name {
	case FuncAdd:
		// x + 0 → x
		if rightIsConst && isConstZero(right) {
			return left, true
		}
		// 0 + x → x
		if leftIsConst && isConstZero(left) {
			return right, true
		}
	case FuncSubtract:
		// x - 0 → x
		if rightIsConst && isConstZero(right) {
			return left, true
		}
	case FuncMultiply:
		// x * 1 → x
		if rightIsConst && isConstOne(right) {
			return left, true
		}
		// 1 * x → x
		if leftIsConst && isConstOne(left) {
			return right, true
		}
		// x * 0 → 0 (integer 0)
		if rightIsConst && isConstZero(right) {
			return makeZeroConst(root.DataTyp), true
		}
		// 0 * x → 0
		if leftIsConst && isConstZero(left) {
			return makeZeroConst(root.DataTyp), true
		}
	}
	return nil, false
}

func isConstZero(e *Expr) bool {
	if e == nil || e.Typ != ET_Const {
		return false
	}
	switch e.ConstValue.Type {
	case ConstTypeInteger:
		return e.ConstValue.Integer == 0
	case ConstTypeFloat:
		return e.ConstValue.Float == 0
	case ConstTypeNull:
		return false
	}
	return false
}

func isConstOne(e *Expr) bool {
	if e == nil || e.Typ != ET_Const {
		return false
	}
	switch e.ConstValue.Type {
	case ConstTypeInteger:
		return e.ConstValue.Integer == 1
	case ConstTypeFloat:
		return e.ConstValue.Float == 1.0
	case ConstTypeNull:
		return false
	}
	return false
}

func makeZeroConst(typ common.LType) *Expr {
	if typ.IsIntegral() {
		return &Expr{
			Typ:        ET_Const,
			DataTyp:    typ,
			ConstValue: NewIntegerConst(0),
		}
	}
	return &Expr{
		Typ:        ET_Const,
		DataTyp:    typ,
		ConstValue: NewFloatConst(0),
	}
}
