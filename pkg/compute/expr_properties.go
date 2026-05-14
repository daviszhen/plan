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
// Expression property queries
// Mirrors DuckDB Expression::{IsConstant,IsFoldable,IsVolatile,
// IsScalar,IsAggregate,IsWindow,HasSubquery,HasParameter,
// PropagatesNullValues,CanThrow}
//
// Design: recursive descent with type-specific overrides.
// Leaf types (Column, Const, Subquery) have hard-coded answers.
// Function expressions delegate to Function metadata.
// All other types recursively query children.
// ============================================================

// IsConstant returns true if the expression is a compile-time constant.
// A constant expression contains no column references, subqueries, or
// volatile functions.
func (e *Expr) IsConstant() bool {
	if e == nil {
		return true
	}
	switch e.Typ {
	case ET_Column:
		return false
	case ET_Subquery:
		return false
	case ET_Const:
		return true
	case ET_Func:
		if e.isVolatileFunc() {
			return false
		}
		for _, child := range e.Children {
			if !child.IsConstant() {
				return false
			}
		}
		return true
	default:
		for _, child := range e.Children {
			if !child.IsConstant() {
				return false
			}
		}
		return true
	}
}

// IsFoldable returns true if the expression can be folded into a constant
// at compile time. For now this is equivalent to IsConstant.
// In the future this may allow bound parameters or correlated columns
// that are known to be constant for a given execution context.
func (e *Expr) IsFoldable() bool {
	if e == nil {
		return true
	}
	switch e.Typ {
	case ET_Column:
		return false
	case ET_Subquery:
		return false
	case ET_Const:
		return true
	case ET_Func:
		if e.isVolatileFunc() {
			return false
		}
		for _, child := range e.Children {
			if !child.IsFoldable() {
				return false
			}
		}
		return true
	default:
		for _, child := range e.Children {
			if !child.IsFoldable() {
				return false
			}
		}
		return true
	}
}

// IsVolatile returns true if the expression contains a volatile function
// (e.g. random(), now()) or any volatile sub-expression.
func (e *Expr) IsVolatile() bool {
	if e == nil {
		return false
	}
	switch e.Typ {
	case ET_Func:
		if e.isVolatileFunc() {
			return true
		}
		for _, child := range e.Children {
			if child.IsVolatile() {
				return true
			}
		}
		return false
	default:
		for _, child := range e.Children {
			if child.IsVolatile() {
				return true
			}
		}
		return false
	}
}

// IsScalar returns true if the expression does not contain column references,
// aggregates, or window functions.
func (e *Expr) IsScalar() bool {
	if e == nil {
		return true
	}
	switch e.Typ {
	case ET_Column:
		return false
	case ET_Func:
		if e.isAggregateFunc() || e.isWindowFunc() {
			return false
		}
		for _, child := range e.Children {
			if !child.IsScalar() {
				return false
			}
		}
		return true
	default:
		for _, child := range e.Children {
			if !child.IsScalar() {
				return false
			}
		}
		return true
	}
}

// IsAggregate returns true if the expression contains an aggregate function.
func (e *Expr) IsAggregate() bool {
	if e == nil {
		return false
	}
	switch e.Typ {
	case ET_Func:
		if e.isAggregateFunc() {
			return true
		}
		for _, child := range e.Children {
			if child.IsAggregate() {
				return true
			}
		}
		return false
	default:
		for _, child := range e.Children {
			if child.IsAggregate() {
				return true
			}
		}
		return false
	}
}

// HasSubquery returns true if the expression contains a subquery.
func (e *Expr) HasSubquery() bool {
	if e == nil {
		return false
	}
	if e.Typ == ET_Subquery {
		return true
	}
	for _, child := range e.Children {
		if child.HasSubquery() {
			return true
		}
	}
	return false
}

// PropagatesNullValues returns true if the expression propagates NULL values
// from its children. Most functions and operators propagate NULL (if any child
// is NULL, the result is NULL). Special cases like IS NULL, COALESCE, CASE,
// AND, OR do not propagate NULL.
func (e *Expr) PropagatesNullValues() bool {
	if e == nil {
		return true
	}
	switch e.Typ {
	case ET_Func:
		if e.IsConjunction() || e.IsDisjunction() || e.IsBetween() || e.IsCaseExpr() {
			return false
		}
		if e.GetFuncInfo().FunImpl.HasSpecialNullHandling() {
			return false
		}
		for _, child := range e.Children {
			if !child.PropagatesNullValues() {
				return false
			}
		}
		return true
	default:
		for _, child := range e.Children {
			if !child.PropagatesNullValues() {
				return false
			}
		}
		return true
	}
}

// CanThrow returns true if the expression may throw a runtime error.
// This delegates to the function's error mode for function expressions.
func (e *Expr) CanThrow() bool {
	if e == nil {
		return false
	}
	switch e.Typ {
	case ET_Func:
		if e.GetFuncInfo().FunImpl != nil && e.GetFuncInfo().FunImpl.CanThrow() {
			return true
		}
		for _, child := range e.Children {
			if child.CanThrow() {
				return true
			}
		}
		return false
	default:
		for _, child := range e.Children {
			if child.CanThrow() {
				return true
			}
		}
		return false
	}
}

// ============================================================
// Internal helpers
// ============================================================

func (e *Expr) isVolatileFunc() bool {
	if e.Typ != ET_Func || e.GetFuncInfo().FunImpl == nil {
		return false
	}
	return e.GetFuncInfo().FunImpl.IsVolatile()
}

func (e *Expr) isAggregateFunc() bool {
	if e.Typ != ET_Func || e.GetFuncInfo().FunImpl == nil {
		return false
	}
	return e.GetFuncInfo().FunImpl.IsAggregateType()
}

func (e *Expr) isWindowFunc() bool {
	if e.Typ != ET_Func || e.GetFuncInfo().FunImpl == nil {
		return false
	}
	// TODO: add WindowFuncType when window functions are implemented
	return false
}
