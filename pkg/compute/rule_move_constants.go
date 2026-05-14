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

// MoveConstantsRule moves constants across comparison operators to expose
// foldable constant expressions on one side:
//
//	col + 5 = 10   → col = 5    (10 - 5 → constant-folded to 5)
//	5 + col = 10   → col = 5
//	col - 5 = 10   → col = 15   (10 + 5)
//	10 - col = 5   → col = 5    (10 - 5)
//	col * 2 = 10   → col = 5    (10 / 2)
//
// After this rule fires, ConstantFoldingRule simplifies the new constant
// expression on the right side (e.g., 10-5 → 5).
type MoveConstantsRule struct{}

func (r *MoveConstantsRule) Root() ExprMatcher {
	// Match comparison expressions where at least one side is foldable.
	return &FuncExprMatcher{
		FuncNameMatcher: NewManyFuncNameMatcher(
			FuncEqual, FuncNotEqual, FuncGreater, FuncGreaterEqual,
			FuncLess, FuncLessEqual,
		),
		Children: []ExprMatcher{&FoldableConstantMatcher{}, &AnyExprMatcher{}},
		Policy:   PolicyUnordered,
	}
}

func (r *MoveConstantsRule) Apply(root *Expr, bindings []*Expr) (*Expr, bool) {
	if root == nil || len(root.Children) != 2 {
		return nil, false
	}

	left := root.Children[0]
	right := root.Children[1]

	// Identify the constant side and the arithmetic (column) side.
	var constSide, arithSide *Expr
	var constIsRight bool
	if left.IsFoldable() && left.Typ == ET_Const {
		constSide, arithSide = left, right
	} else if right.IsFoldable() && right.Typ == ET_Const {
		constSide, arithSide = right, left
		constIsRight = true
	} else {
		return nil, false
	}

	// Non-constant side must be an arithmetic expression (+, -, *).
	if !arithSide.isFunc() || !isMoveConstantsOp(arithSide.FuncName()) {
		return nil, false
	}
	if len(arithSide.Children) != 2 {
		return nil, false
	}

	// Find the constant child and the non-constant child within the arithmetic.
	var arithConst, varSide *Expr
	var arithConstIsRight bool
	if arithSide.Children[0].Typ == ET_Const {
		arithConst = arithSide.Children[0]
		varSide = arithSide.Children[1]
	} else if arithSide.Children[1].Typ == ET_Const {
		arithConst = arithSide.Children[1]
		varSide = arithSide.Children[0]
		arithConstIsRight = true
	} else {
		return nil, false
	}

	opName := root.FuncName()
	arithOp := arithSide.FuncName()

	// Compute the new constant by applying the inverse operation.
	newConst, ok := r.invertOp(arithOp, arithConstIsRight, constSide, arithConst)
	if !ok {
		return nil, false
	}

	// Build the new comparison: varSide <op> newConst.
	// If the original comparison had const on the LEFT, we need to flip the
	// order: newConst <op> varSide → flip to varSide <flipped_op> newConst.
	leftExpr, rightExpr := varSide, newConst
	actualOp := opName
	if !constIsRight {
		// Constant was on left: e.g., 10 = col + 5 → col + 5 = 10
		// Now result should be col = 5 (varSide on left, newConst on right).
		// But if op was >, 10 > col+5 means col+5 < 10, so op flips.
		actualOp = r.flipComparison(opName)
		leftExpr, rightExpr = varSide, newConst
	}

	return r.buildComparison(actualOp, leftExpr, rightExpr)
}

func (r *MoveConstantsRule) invertOp(arithOp string, constOnRight bool, outerConst, innerConst *Expr) (*Expr, bool) {
	// The original expression is: var <arithOp> innerConst <cmpOp> outerConst
	// We need: var = inverse_arith_op(outerConst, innerConst)
	// inverse_arith_op depends on the operation and which side had the constant.

	// For brevity, build the inverse using a FunctionBinder and let
	// ConstantFoldingRule fold it afterward.
	binder := FunctionBinder{}

	switch arithOp {
	case FuncAdd:
		// col + c = val → col = val - c
		// c + col = val → col = val - c (same)
		return binder.BindScalarFunc(FuncSubtract,
			[]*Expr{outerConst.Copy(), innerConst.Copy()}, IsOperator(FuncSubtract)), true

	case FuncSubtract:
		if constOnRight {
			// col - c = val → col = val + c
			return binder.BindScalarFunc(FuncAdd,
				[]*Expr{outerConst.Copy(), innerConst.Copy()}, IsOperator(FuncAdd)), true
		}
		// c - col = val → col = c - val
		return binder.BindScalarFunc(FuncSubtract,
			[]*Expr{innerConst.Copy(), outerConst.Copy()}, IsOperator(FuncSubtract)), true

	case FuncMultiply:
		// col * c = val → col = val / c (only if c ≠ 0)
		if r.isConstZero(innerConst) {
			return nil, false
		}
		return binder.BindScalarFunc(FuncDivide,
			[]*Expr{outerConst.Copy(), innerConst.Copy()}, IsOperator(FuncDivide)), true
	}
	return nil, false
}

func (r *MoveConstantsRule) flipComparison(op string) string {
	switch op {
	case FuncGreater:
		return FuncLess
	case FuncGreaterEqual:
		return FuncLessEqual
	case FuncLess:
		return FuncGreater
	case FuncLessEqual:
		return FuncGreaterEqual
	default:
		// = and <> are symmetric.
		return op
	}
}

func (r *MoveConstantsRule) buildComparison(op string, left, right *Expr) (*Expr, bool) {
	if left == nil || right == nil {
		return nil, false
	}
	binder := FunctionBinder{}
	// Ensure types match: cast right to left's type if needed.
	return binder.BindScalarFunc(op,
		[]*Expr{left.Copy(), right.Copy()}, IsOperator(op)), true
}

func (r *MoveConstantsRule) isConstZero(e *Expr) bool {
	if e == nil || e.Typ != ET_Const {
		return false
	}
	if e.ConstValue.Type == ConstTypeInteger {
		return e.ConstValue.Integer == 0
	}
	if e.ConstValue.Type == ConstTypeFloat {
		return e.ConstValue.Float == 0
	}
	return false
}

func isMoveConstantsOp(name string) bool {
	return name == FuncAdd || name == FuncSubtract || name == FuncMultiply
}
