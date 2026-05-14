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
	"github.com/daviszhen/plan/pkg/common"
)

// ============================================================
// ExprMatcher — core interface
// Mirrors DuckDB ExpressionMatcher.
// ============================================================

// ExprMatcher matches an expression node. On success, appends the matched
// expression to bindings and returns (true, newBindings).
type ExprMatcher interface {
	Match(expr *Expr, bindings []*Expr) (bool, []*Expr)
}

// ============================================================
// Attribute matchers — match expression properties
// Mirrors DuckDB ExpressionTypeMatcher, FunctionMatcher, TypeMatcher.
// ============================================================

// ExprTypeMatcher matches an expression ET (expression type).
type ExprTypeMatcher interface {
	Match(et ET) bool
}

// SpecificExprTypeMatcher matches a single ET.
type SpecificExprTypeMatcher struct {
	Typ ET
}

func (m *SpecificExprTypeMatcher) Match(et ET) bool {
	return m.Typ == et
}

// ManyExprTypeMatcher matches any ET in a set.
type ManyExprTypeMatcher struct {
	Types map[ET]struct{}
}

func NewManyExprTypeMatcher(types ...ET) *ManyExprTypeMatcher {
	m := &ManyExprTypeMatcher{Types: make(map[ET]struct{})}
	for _, t := range types {
		m.Types[t] = struct{}{}
	}
	return m
}

func (m *ManyExprTypeMatcher) Match(et ET) bool {
	_, ok := m.Types[et]
	return ok
}

// FuncNameMatcher matches a function name.
type FuncNameMatcher interface {
	Match(name string) bool
}

// SpecificFuncNameMatcher matches a single function name.
type SpecificFuncNameMatcher struct {
	Name string
}

func (m *SpecificFuncNameMatcher) Match(name string) bool {
	return m.Name == name
}

// ManyFuncNameMatcher matches any name in a set.
type ManyFuncNameMatcher struct {
	Names map[string]struct{}
}

func NewManyFuncNameMatcher(names ...string) *ManyFuncNameMatcher {
	m := &ManyFuncNameMatcher{Names: make(map[string]struct{})}
	for _, n := range names {
		m.Names[n] = struct{}{}
	}
	return m
}

func (m *ManyFuncNameMatcher) Match(name string) bool {
	_, ok := m.Names[name]
	return ok
}

// DataTypeMatcher matches a data type (common.LType).
type DataTypeMatcher interface {
	Match(dt common.LType) bool
}

// SpecificDataTypeMatcher matches a single LType.
type SpecificDataTypeMatcher struct {
	Type common.LType
}

func (m *SpecificDataTypeMatcher) Match(dt common.LType) bool {
	return m.Type.Id == dt.Id
}

// NumericDataTypeMatcher matches any numeric type.
type NumericDataTypeMatcher struct{}

func (m *NumericDataTypeMatcher) Match(dt common.LType) bool {
	return dt.IsNumeric()
}

// IntegerDataTypeMatcher matches any integral type.
type IntegerDataTypeMatcher struct{}

func (m *IntegerDataTypeMatcher) Match(dt common.LType) bool {
	return dt.IsIntegral()
}

// ============================================================
// Leaf expression matchers
// ============================================================

// AnyExprMatcher matches any expression.
type AnyExprMatcher struct{}

func (m *AnyExprMatcher) Match(expr *Expr, bindings []*Expr) (bool, []*Expr) {
	if expr == nil {
		return false, bindings
	}
	return true, append(bindings, expr)
}

// TypeExprMatcher matches expressions of a specific ET.
type TypeExprMatcher struct {
	Typ ET
}

func (m *TypeExprMatcher) Match(expr *Expr, bindings []*Expr) (bool, []*Expr) {
	if expr == nil || expr.Typ != m.Typ {
		return false, bindings
	}
	return true, append(bindings, expr)
}

// ConstExprMatcher matches ET_Const expressions.
type ConstExprMatcher struct{}

func (m *ConstExprMatcher) Match(expr *Expr, bindings []*Expr) (bool, []*Expr) {
	if expr == nil || expr.Typ != ET_Const {
		return false, bindings
	}
	return true, append(bindings, expr)
}

// ColumnExprMatcher matches ET_Column expressions.
// If Table or Name are non-empty, they act as additional filters.
type ColumnExprMatcher struct {
	Table string
	Name  string
}

func (m *ColumnExprMatcher) Match(expr *Expr, bindings []*Expr) (bool, []*Expr) {
	if expr == nil || expr.Typ != ET_Column {
		return false, bindings
	}
	if m.Table != "" && expr.Table != m.Table {
		return false, bindings
	}
	if m.Name != "" && expr.Name != m.Name {
		return false, bindings
	}
	return true, append(bindings, expr)
}

// ============================================================
// SetMatcher — child collection matching
// Mirrors DuckDB SetMatcher.
// ============================================================

type SetPolicy int

const (
	// PolicyOrdered: all matchers must match, in exact order.
	PolicyOrdered SetPolicy = iota
	// PolicyUnordered: all matchers must match, order does not matter,
	// each matcher matches a unique entry.
	PolicyUnordered
	// PolicySome: only some matchers need to match, order does not matter,
	// each matcher matches a unique entry.
	PolicySome
	// PolicySomeOrdered: only some matchers need to match, in prefix order.
	PolicySomeOrdered
)

// SetMatch matches a slice of matchers against a slice of expression entries.
func SetMatch(matchers []ExprMatcher, entries []*Expr, bindings []*Expr, policy SetPolicy) (bool, []*Expr) {
	switch policy {
	case PolicyOrdered:
		if len(matchers) != len(entries) {
			return false, bindings
		}
		for i, m := range matchers {
			ok, newBindings := m.Match(entries[i], bindings)
			if !ok {
				return false, bindings
			}
			bindings = newBindings
		}
		return true, bindings

	case PolicySomeOrdered:
		if len(entries) < len(matchers) {
			return false, bindings
		}
		for i, m := range matchers {
			ok, newBindings := m.Match(entries[i], bindings)
			if !ok {
				return false, bindings
			}
			bindings = newBindings
		}
		return true, bindings

	case PolicyUnordered:
		if len(matchers) != len(entries) {
			return false, bindings
		}
		return setMatchRecursive(matchers, entries, bindings, make(map[int]struct{}), 0)

	case PolicySome:
		if len(matchers) > len(entries) {
			return false, bindings
		}
		return setMatchRecursive(matchers, entries, bindings, make(map[int]struct{}), 0)

	default:
		return false, bindings
	}
}

// setMatchRecursive uses backtracking to find a unique mapping from matchers to entries.
func setMatchRecursive(matchers []ExprMatcher, entries []*Expr, bindings []*Expr, excluded map[int]struct{}, mIdx int) (bool, []*Expr) {
	if mIdx == len(matchers) {
		return true, bindings
	}
	prevCount := len(bindings)
	for eIdx := 0; eIdx < len(entries); eIdx++ {
		if _, ok := excluded[eIdx]; ok {
			continue
		}
		ok, newBindings := matchers[mIdx].Match(entries[eIdx], bindings)
		if ok {
			newExcluded := make(map[int]struct{}, len(excluded)+1)
			for k := range excluded {
				newExcluded[k] = struct{}{}
			}
			newExcluded[eIdx] = struct{}{}
			if found, finalBindings := setMatchRecursive(matchers, entries, newBindings, newExcluded, mIdx+1); found {
				return true, finalBindings
			}
		}
		bindings = bindings[:prevCount]
	}
	return false, bindings
}

// ============================================================
// FuncExprMatcher — matches function/operator expressions
// Mirrors DuckDB FunctionExpressionMatcher.
// ============================================================

type FuncExprMatcher struct {
	// TypeMatcher optionally matches the expression ET (default: ET_Func).
	TypeMatcher ExprTypeMatcher
	// FuncNameMatcher optionally matches the function name.
	FuncNameMatcher FuncNameMatcher
	// DataTypeMatcher optionally matches the return data type.
	DataTypeMatcher DataTypeMatcher
	// Children matchers for child expressions.
	Children []ExprMatcher
	// Policy for matching children.
	Policy SetPolicy
}

func (m *FuncExprMatcher) Match(expr *Expr, bindings []*Expr) (bool, []*Expr) {
	if expr == nil {
		return false, bindings
	}
	// Check expression type.
	if m.TypeMatcher != nil {
		if !m.TypeMatcher.Match(expr.Typ) {
			return false, bindings
		}
	} else if expr.Typ != ET_Func {
		return false, bindings
	}
	// Check function name.
	if m.FuncNameMatcher != nil {
		if expr.Typ != ET_Func || expr.Info == nil {
			return false, bindings
		}
		fi := expr.GetFuncInfo()
		if fi == nil || fi.FunImpl == nil || !m.FuncNameMatcher.Match(expr.FuncName()) {
			return false, bindings
		}
	}
	// Check return data type.
	if m.DataTypeMatcher != nil {
		if !m.DataTypeMatcher.Match(expr.DataTyp) {
			return false, bindings
		}
	}
	// Bind self.
	bindings = append(bindings, expr)
	// Match children.
	if len(m.Children) > 0 {
		policy := m.Policy
		if policy == 0 && len(m.Children) > 0 {
			policy = PolicyOrdered
		}
		ok, newBindings := SetMatch(m.Children, expr.Children, bindings, policy)
		if !ok {
			return false, bindings[:len(bindings)-1]
		}
		return true, newBindings
	}
	return true, bindings
}

// ============================================================
// ComparisonExprMatcher — matches comparison expressions
// Convenience wrapper over FuncExprMatcher for compare ops.
// ============================================================

type ComparisonExprMatcher struct {
	// Children matchers for left/right operands.
	Children []ExprMatcher
	// Policy for matching children.
	Policy SetPolicy
}

func NewComparisonExprMatcher() *ComparisonExprMatcher {
	return &ComparisonExprMatcher{}
}

func (m *ComparisonExprMatcher) Match(expr *Expr, bindings []*Expr) (bool, []*Expr) {
	if expr == nil || expr.Typ != ET_Func || expr.Info == nil {
		return false, bindings
	}
	fi := expr.GetFuncInfo()
	if fi == nil || fi.FunImpl == nil {
		return false, bindings
	}
	opTyp := GetOperatorType(expr.FuncName())
	if opTyp != OpTypeCompare {
		return false, bindings
	}
	return (&FuncExprMatcher{
		FuncNameMatcher: NewManyFuncNameMatcher(FuncEqual, FuncNotEqual, FuncGreater, FuncGreaterEqual, FuncLess, FuncLessEqual),
		Children:        m.Children,
		Policy:          m.Policy,
	}).Match(expr, bindings)
}

// ============================================================
// AggregateExprMatcher — matches aggregate expressions
// Mirrors DuckDB AggregateExpressionMatcher.
// ============================================================

type AggregateExprMatcher struct {
	// FuncNameMatcher optionally matches the aggregate function name.
	FuncNameMatcher FuncNameMatcher
	// Children matchers for child expressions.
	Children []ExprMatcher
	// Policy for matching children.
	Policy SetPolicy
}

func (m *AggregateExprMatcher) Match(expr *Expr, bindings []*Expr) (bool, []*Expr) {
	if expr == nil || !expr.IsAggregate() {
		return false, bindings
	}
	if m.FuncNameMatcher != nil && !m.FuncNameMatcher.Match(expr.FuncName()) {
		return false, bindings
	}
	bindings = append(bindings, expr)
	if len(m.Children) > 0 {
		policy := m.Policy
		if policy == 0 {
			policy = PolicyOrdered
		}
		ok, newBindings := SetMatch(m.Children, expr.Children, bindings, policy)
		if !ok {
			return false, bindings[:len(bindings)-1]
		}
		return true, newBindings
	}
	return true, bindings
}

// ============================================================
// LogicalOperator matchers
// Mirrors DuckDB LogicalOperatorMatcher.
// ============================================================

// LogicalOpMatcher matches a LogicalOperator by type or properties.
type LogicalOpMatcher interface {
	Match(op *LogicalOperator) bool
}

// SpecificLogicalOpMatcher matches a single LOT.
type SpecificLogicalOpMatcher struct {
	Typ LOT
}

func (m *SpecificLogicalOpMatcher) Match(op *LogicalOperator) bool {
	return op != nil && op.Typ == m.Typ
}

// AnyLogicalOpMatcher matches any LogicalOperator.
type AnyLogicalOpMatcher struct{}

func (m *AnyLogicalOpMatcher) Match(op *LogicalOperator) bool {
	return op != nil
}

// LogicalOpWithChildMatcher matches a LogicalOperator that has a child matching another matcher.
type LogicalOpWithChildMatcher struct {
	OpMatcher    LogicalOpMatcher
	ChildMatcher LogicalOpMatcher
	ChildIndex   int
}

func (m *LogicalOpWithChildMatcher) Match(op *LogicalOperator) bool {
	if !m.OpMatcher.Match(op) {
		return false
	}
	if m.ChildIndex >= len(op.Children) {
		return false
	}
	return m.ChildMatcher.Match(op.Children[m.ChildIndex])
}

// ============================================================
// FoldableConstantMatcher — matches expressions foldable to constant
// Mirrors DuckDB FoldableConstantMatcher.
// ============================================================

type FoldableConstantMatcher struct{}

func (m *FoldableConstantMatcher) Match(expr *Expr, bindings []*Expr) (bool, []*Expr) {
	if expr == nil || !expr.IsFoldable() {
		return false, bindings
	}
	return true, append(bindings, expr)
}

// ============================================================
// StableExpressionMatcher — matches non-volatile stable expressions
// Mirrors DuckDB StableExpressionMatcher.
// ============================================================

type StableExpressionMatcher struct{}

func (m *StableExpressionMatcher) Match(expr *Expr, bindings []*Expr) (bool, []*Expr) {
	if expr == nil || expr.IsVolatile() {
		return false, bindings
	}
	return true, append(bindings, expr)
}

// ============================================================
// ExpressionEqualityMatcher — matches an expression equal to a given one
// Mirrors DuckDB ExpressionEqualityMatcher.
// Uses pointer equality for simplicity.
// ============================================================

type ExpressionEqualityMatcher struct {
	Expression *Expr
}

func (m *ExpressionEqualityMatcher) Match(expr *Expr, bindings []*Expr) (bool, []*Expr) {
	if expr == nil || m.Expression == nil {
		return false, bindings
	}
	if expr != m.Expression {
		return false, bindings
	}
	return true, append(bindings, expr)
}
