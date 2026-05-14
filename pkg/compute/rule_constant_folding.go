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

import (
	"fmt"

	"github.com/daviszhen/plan/pkg/chunk"
	"github.com/daviszhen/plan/pkg/common"
)

// ConstantFoldingRule folds expressions with all-constant children into
// a single ET_Const expression. For example: 1+2 → 3.
//
// It uses the expression executor to evaluate the expression at plan time.
type ConstantFoldingRule struct{}

func (r *ConstantFoldingRule) Root() ExprMatcher {
	return &FoldableConstantMatcher{}
}

func (r *ConstantFoldingRule) Apply(root *Expr, bindings []*Expr) (*Expr, bool) {
	if root == nil {
		return nil, false
	}
	// Already a constant — nothing to fold.
	if root.Typ == ET_Const {
		return nil, false
	}
	// Only fold if the expression is foldable.
	if !root.IsFoldable() {
		return nil, false
	}
	// Only fold if all children are literal constants (ET_Const).
	// This avoids trying to execute complex expressions like aggregates or subqueries.
	for _, child := range root.Children {
		if child.Typ != ET_Const {
			return nil, false
		}
	}

	// Execute the expression with zero-length input to get the constant result.
	value, err := evaluateConstantExpr(root)
	if err != nil {
		return nil, false
	}
	// If evaluation produced a null (unsupported type), don't fold.
	if value.Type == ConstTypeNull && root.Typ != ET_Const {
		return nil, false
	}

	return &Expr{
		Typ:        ET_Const,
		DataTyp:    root.DataTyp,
		ConstValue: value,
		BaseInfo:   root.BaseInfo.Copy(),
	}, true
}

// evaluateConstantExpr evaluates an expression with all-constant children
// and returns the resulting ConstValue. Panics are caught and returned as errors.
func evaluateConstantExpr(expr *Expr) (cv ConstValue, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("constant folding panic: %v", r)
		}
	}()

	exec := NewExprExec(expr)
	// Create an empty chunk — the expression has only constants, so no actual data needed.
	emptyChunk := &chunk.Chunk{}
	emptyChunk.Init(nil, 1)
	emptyChunk.SetCard(1)

	result := &chunk.Chunk{}
	result.Init([]common.LType{expr.DataTyp}, 1)

	err = exec.executeExprs([]*chunk.Chunk{emptyChunk}, result)
	if err != nil {
		cv = ConstValue{}
		return
	}

	// Extract the constant value from the result vector.
	vec := result.Data[0]
	cv = vectorToConstValue(vec, expr.DataTyp)
	return
}

// vectorToConstValue extracts the first element from a result vector as a ConstValue.
func vectorToConstValue(vec *chunk.Vector, typ common.LType) ConstValue {
	if vec == nil {
		return NewNullConst()
	}
	if vec.PhyFormat().IsConst() {
		if chunk.IsNullInPhyFormatConst(vec) {
			return NewNullConst()
		}
		switch typ.Id {
		case common.LTID_BOOLEAN:
			v := chunk.GetSliceInPhyFormatConst[bool](vec)
			return NewBooleanConst(v[0])
		case common.LTID_TINYINT:
			v := chunk.GetSliceInPhyFormatConst[int8](vec)
			return NewIntegerConst(int64(v[0]))
		case common.LTID_SMALLINT:
			v := chunk.GetSliceInPhyFormatConst[int16](vec)
			return NewIntegerConst(int64(v[0]))
		case common.LTID_INTEGER:
			v := chunk.GetSliceInPhyFormatConst[int32](vec)
			return NewIntegerConst(int64(v[0]))
		case common.LTID_BIGINT:
			v := chunk.GetSliceInPhyFormatConst[int64](vec)
			return NewIntegerConst(v[0])
		case common.LTID_UTINYINT:
			v := chunk.GetSliceInPhyFormatConst[uint8](vec)
			return NewIntegerConst(int64(v[0]))
		case common.LTID_USMALLINT:
			v := chunk.GetSliceInPhyFormatConst[uint16](vec)
			return NewIntegerConst(int64(v[0]))
		case common.LTID_UINTEGER:
			v := chunk.GetSliceInPhyFormatConst[uint32](vec)
			return NewIntegerConst(int64(v[0]))
		case common.LTID_UBIGINT:
			v := chunk.GetSliceInPhyFormatConst[uint64](vec)
			return NewIntegerConst(int64(v[0]))
		case common.LTID_FLOAT:
			v := chunk.GetSliceInPhyFormatConst[float32](vec)
			return NewFloatConst(float64(v[0]))
		case common.LTID_DOUBLE:
			v := chunk.GetSliceInPhyFormatConst[float64](vec)
			return NewFloatConst(v[0])
		case common.LTID_VARCHAR:
			v := chunk.GetSliceInPhyFormatConst[common.String](vec)
			return NewStringConst(v[0].String())
		case common.LTID_DATE:
			v := chunk.GetSliceInPhyFormatConst[common.Date](vec)
			return NewDateConst(fmt.Sprintf("%d-%02d-%02d", v[0].Year, v[0].Month, v[0].Day))
		default:
			// Unsupported type for constant folding — return null.
			return NewNullConst()
		}
	}
	// Non-const vector — cannot extract a single value.
	return NewNullConst()
}
