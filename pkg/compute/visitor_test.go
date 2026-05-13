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
	"testing"

	"github.com/daviszhen/plan/pkg/common"
	"github.com/daviszhen/plan/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- helpers ---

func mkCol(table, name string, tag, pos uint64) *Expr {
	return &Expr{Typ: ET_Column, BaseInfo: BaseInfo{Table: table, Name: name, ColRef: ColumnBind{tag, pos}}, DataTyp: common.IntegerType()}
}

func mkConst(v int64) *Expr {
	return &Expr{Typ: ET_Const, ConstValue: NewIntegerConst(v), DataTyp: common.IntegerType()}
}

func mkFunc(name string, children ...*Expr) *Expr {
	binder := FunctionBinder{}
	return binder.BindScalarFunc(name, children, IsOperator(name))
}

func mkOrderby(child *Expr) *Expr {
	return &Expr{Typ: ET_Orderby, Children: []*Expr{child}, Info: &OrderByInfo{Desc: false}}
}

// ============================================================
// Stage 1: EnumerateExpressions tests — one per LOT type
// ============================================================

func countEnumerated(op *LogicalOperator) int {
	n := 0
	EnumerateExpressions(op, func(expr **Expr) { n++ })
	return n
}

func TestEnumerateExpressions_Scan(t *testing.T) {
	op := &LogicalOperator{
		Typ:     LOT_Scan,
		Filters: []*Expr{mkCol("t", "a", 1, 0)},
		Outputs: []*Expr{mkCol("t", "a", 1, 0), mkCol("t", "b", 1, 1)},
		Info:    &ScanOpInfo{Database: "public", Table: "t"},
	}
	assert.Equal(t, 3, countEnumerated(op)) // 1 filter + 2 outputs
}

func TestEnumerateExpressions_Filter(t *testing.T) {
	op := &LogicalOperator{
		Typ:     LOT_Filter,
		Filters: []*Expr{mkFunc("=", mkCol("t", "a", 1, 0), mkConst(1))},
		Outputs: []*Expr{mkCol("t", "a", 1, 0)},
	}
	assert.Equal(t, 2, countEnumerated(op))
}

func TestEnumerateExpressions_Project(t *testing.T) {
	op := &LogicalOperator{
		Typ:      LOT_Project,
		Projects: []*Expr{mkCol("t", "a", 1, 0), mkCol("t", "b", 1, 1)},
		Outputs:  []*Expr{mkCol("t", "a", 1, 0)},
	}
	assert.Equal(t, 3, countEnumerated(op)) // 2 projects + 1 output
}

func TestEnumerateExpressions_Join(t *testing.T) {
	op := &LogicalOperator{
		Typ:     LOT_JOIN,
		Filters: []*Expr{mkConst(1)},
		Outputs: []*Expr{mkCol("t", "a", 1, 0)},
		Info: &JoinOpInfo{
			JoinTyp: LOT_JoinTypeInner,
			OnConds: []*Expr{mkFunc("=", mkCol("t", "a", 1, 0), mkCol("s", "b", 2, 0))},
		},
	}
	assert.Equal(t, 3, countEnumerated(op)) // 1 filter + 1 output + 1 onCond
}

func TestEnumerateExpressions_AggGroup(t *testing.T) {
	op := &LogicalOperator{
		Typ:     LOT_AggGroup,
		Filters: []*Expr{mkConst(1)},
		Outputs: []*Expr{mkCol("t", "a", 1, 0)},
		Info: &AggOpInfo{
			GroupBys: []*Expr{mkCol("t", "a", 1, 0)},
			Aggs:     []*Expr{mkCol("t", "b", 1, 1), mkCol("t", "c", 1, 2)},
		},
	}
	assert.Equal(t, 5, countEnumerated(op)) // 1 filter + 1 output + 1 groupby + 2 aggs
}

func TestEnumerateExpressions_Order(t *testing.T) {
	op := &LogicalOperator{
		Typ:     LOT_Order,
		Outputs: []*Expr{mkCol("t", "a", 1, 0)},
		Info:    &OrderOpInfo{OrderBys: []*Expr{mkOrderby(mkCol("t", "a", 1, 0))}},
	}
	assert.Equal(t, 2, countEnumerated(op)) // 1 output + 1 orderby
}

func TestEnumerateExpressions_Limit(t *testing.T) {
	op := &LogicalOperator{
		Typ:     LOT_Limit,
		Outputs: []*Expr{mkCol("t", "a", 1, 0)},
		Info:    &LimitOpInfo{Limit: mkConst(10), Offset: mkConst(5)},
	}
	assert.Equal(t, 3, countEnumerated(op)) // 1 output + limit + offset
}

func TestEnumerateExpressions_Limit_NoOffset(t *testing.T) {
	op := &LogicalOperator{
		Typ:  LOT_Limit,
		Info: &LimitOpInfo{Limit: mkConst(10)},
	}
	assert.Equal(t, 1, countEnumerated(op)) // limit only
}

func TestEnumerateExpressions_Insert(t *testing.T) {
	op := &LogicalOperator{
		Typ:     LOT_Insert,
		Outputs: []*Expr{mkCol("t", "a", 1, 0)},
		Info:    &InsertOpInfo{},
	}
	assert.Equal(t, 1, countEnumerated(op)) // 1 output only
}

func TestEnumerateExpressions_CreateTable(t *testing.T) {
	op := &LogicalOperator{Typ: LOT_CreateTable, Info: &CreateTableOpInfo{}}
	assert.Equal(t, 0, countEnumerated(op))
}

func TestEnumerateExpressions_CreateSchema(t *testing.T) {
	op := &LogicalOperator{Typ: LOT_CreateSchema, Info: &CreateSchemaOpInfo{}}
	assert.Equal(t, 0, countEnumerated(op))
}

func TestEnumerateExpressions_NilInfo(t *testing.T) {
	op := &LogicalOperator{Typ: LOT_Scan, Filters: []*Expr{mkConst(1)}}
	assert.Equal(t, 1, countEnumerated(op)) // no panic
}

// ============================================================
// Stage 1: EnumerateExprChildren tests — one per ET type
// ============================================================

func countChildren(expr *Expr) int {
	n := 0
	EnumerateExprChildren(expr, func(child **Expr) { n++ })
	return n
}

func TestEnumerateExprChildren_Column(t *testing.T) {
	assert.Equal(t, 0, countChildren(mkCol("t", "a", 1, 0)))
}

func TestEnumerateExprChildren_Const(t *testing.T) {
	assert.Equal(t, 0, countChildren(mkConst(42)))
}

func TestEnumerateExprChildren_Func_Binary(t *testing.T) {
	e := mkFunc("=", mkCol("t", "a", 1, 0), mkConst(1))
	assert.Equal(t, 2, countChildren(e))
}

func TestEnumerateExprChildren_Func_Unary(t *testing.T) {
	// Manually construct a func with 1 child
	e := &Expr{Typ: ET_Func, Children: []*Expr{mkConst(1)}, Info: &FunctionInfo{}}
	assert.Equal(t, 1, countChildren(e))
}

func TestEnumerateExprChildren_Orderby(t *testing.T) {
	assert.Equal(t, 1, countChildren(mkOrderby(mkCol("t", "a", 1, 0))))
}

func TestEnumerateExprChildren_Join(t *testing.T) {
	e := &Expr{Typ: ET_Join, Children: []*Expr{mkCol("t", "a", 1, 0), mkCol("s", "b", 2, 0)}}
	assert.Equal(t, 2, countChildren(e))
}

func TestEnumerateExprChildren_List(t *testing.T) {
	e := &Expr{Typ: ET_List, Children: []*Expr{mkConst(1), mkConst(2), mkConst(3)}}
	assert.Equal(t, 3, countChildren(e))
}

func TestEnumerateExprChildren_Table(t *testing.T) {
	e := &Expr{Typ: ET_TABLE}
	assert.Equal(t, 0, countChildren(e))
}

func TestEnumerateExprChildren_ValuesList(t *testing.T) {
	e := &Expr{Typ: ET_ValuesList}
	assert.Equal(t, 0, countChildren(e))
}

func TestEnumerateExprChildren_CTE(t *testing.T) {
	e := &Expr{Typ: ET_CTE}
	assert.Equal(t, 0, countChildren(e))
}

func TestEnumerateExprChildren_Nil(t *testing.T) {
	n := 0
	EnumerateExprChildren(nil, func(child **Expr) { n++ })
	assert.Equal(t, 0, n)
}

// ============================================================
// Stage 2: BaseVisitor default behavior
// ============================================================

func buildTestPlan() *LogicalOperator {
	// Project → Filter → Join → (ScanL, ScanR)
	scanL := &LogicalOperator{
		Typ: LOT_Scan, Index: 1,
		Filters: []*Expr{mkFunc("=", mkCol("n", "k", 1, 0), mkConst(1))},
		Outputs: []*Expr{mkCol("n", "k", 1, 0)},
		Info:    &ScanOpInfo{Database: "public", Table: "nation"},
	}
	scanR := &LogicalOperator{
		Typ: LOT_Scan, Index: 2,
		Outputs: []*Expr{mkCol("r", "k", 2, 0)},
		Info:    &ScanOpInfo{Database: "public", Table: "region"},
	}
	join := &LogicalOperator{
		Typ: LOT_JOIN, Index: 3,
		Outputs:  []*Expr{mkCol("n", "k", 1, 0), mkCol("r", "k", 2, 0)},
		Info:     &JoinOpInfo{JoinTyp: LOT_JoinTypeInner, OnConds: []*Expr{mkFunc("=", mkCol("n", "rk", 1, 1), mkCol("r", "k", 2, 0))}},
		Children: []*LogicalOperator{scanL, scanR},
	}
	filter := &LogicalOperator{
		Typ: LOT_Filter, Index: 4,
		Filters:  []*Expr{mkFunc("=", mkCol("n", "k", 1, 0), mkConst(5))},
		Outputs:  []*Expr{mkCol("n", "k", 1, 0)},
		Children: []*LogicalOperator{join},
	}
	project := &LogicalOperator{
		Typ: LOT_Project, Index: 5,
		Projects: []*Expr{mkCol("n", "k", 1, 0)},
		Outputs:  []*Expr{mkCol("n", "k", 1, 0)},
		Children: []*LogicalOperator{filter},
	}
	return project
}

func TestBaseVisitor_DefaultTraversal(t *testing.T) {
	plan := buildTestPlan()
	v := &BaseVisitor{}
	v.Self = v
	// Should not panic — traverses entire tree doing nothing
	v.VisitOperator(plan)
}

// ============================================================
// Stage 3: Override tests
// ============================================================

// 3.2 — Override VisitOperator
type countingVisitor struct {
	BaseVisitor
	OpCount int
}

func (v *countingVisitor) VisitOperator(op *LogicalOperator) {
	v.OpCount++
	DefaultVisitOperator(v.Self, op)
}

func TestOverride_VisitOperator(t *testing.T) {
	plan := buildTestPlan()
	v := &countingVisitor{}
	v.Self = v
	v.VisitOperator(plan)
	assert.Equal(t, 5, v.OpCount) // project + filter + join + scanL + scanR
}

// 3.3 — Override VisitOperatorChildren (skip scan)
type skipScanVisitor struct {
	BaseVisitor
	VisitedOps []LOT
}

func (v *skipScanVisitor) VisitOperator(op *LogicalOperator) {
	v.VisitedOps = append(v.VisitedOps, op.Typ)
	DefaultVisitOperator(v.Self, op)
}

func (v *skipScanVisitor) VisitOperatorChildren(op *LogicalOperator) {
	for _, child := range op.Children {
		if child.Typ != LOT_Scan {
			v.Self.VisitOperator(child)
		}
	}
}

func TestOverride_VisitOperatorChildren_SkipScan(t *testing.T) {
	plan := buildTestPlan()
	v := &skipScanVisitor{}
	v.Self = v
	v.VisitOperator(plan)
	// Should visit: Project, Filter, Join — but NOT ScanL, ScanR
	assert.Equal(t, []LOT{LOT_Project, LOT_Filter, LOT_JOIN}, v.VisitedOps)
}

// 3.4 — Override VisitOperatorExpressions (skip expressions)
type noExprVisitor struct {
	BaseVisitor
	OpCount   int
	ExprCount int
}

func (v *noExprVisitor) VisitOperator(op *LogicalOperator) {
	v.OpCount++
	DefaultVisitOperator(v.Self, op)
}

func (v *noExprVisitor) VisitOperatorExpressions(op *LogicalOperator) {
	// intentionally empty — skip all expressions
}

func (v *noExprVisitor) VisitExpression(expr **Expr) {
	v.ExprCount++
	DefaultVisitExpression(v.Self, expr)
}

func TestOverride_VisitOperatorExpressions_Skip(t *testing.T) {
	plan := buildTestPlan()
	v := &noExprVisitor{}
	v.Self = v
	v.VisitOperator(plan)
	assert.Equal(t, 5, v.OpCount)
	assert.Equal(t, 0, v.ExprCount) // no expressions visited
}

// 3.5 — Override VisitExpression (direct replace)
type directReplaceVisitor struct {
	BaseVisitor
}

func (v *directReplaceVisitor) VisitExpression(expr **Expr) {
	if (*expr).Typ == ET_Const {
		*expr = mkConst(999)
		return
	}
	DefaultVisitExpression(v.Self, expr)
}

func TestOverride_VisitExpression_DirectReplace(t *testing.T) {
	// Filter with: (a = 1)
	op := &LogicalOperator{
		Typ:     LOT_Filter,
		Filters: []*Expr{mkFunc("=", mkCol("t", "a", 1, 0), mkConst(1))},
	}
	v := &directReplaceVisitor{}
	v.Self = v
	v.VisitOperator(op)
	// The top-level filter expr is a Func, not replaced.
	// But its child const(1) should be replaced with const(999).
	require.Equal(t, ET_Func, op.Filters[0].Typ)
	require.Equal(t, ET_Const, op.Filters[0].Children[1].Typ)
	assert.Equal(t, int64(999), op.Filters[0].Children[1].ConstValue.Integer)
}

// 3.6 — Override VisitExpressionChildren (shallow)
type shallowVisitor struct {
	BaseVisitor
	ReplaceCount int
}

func (v *shallowVisitor) VisitExpressionChildren(expr *Expr) {
	// intentionally empty — don't recurse
}

func (v *shallowVisitor) VisitReplace(expr *Expr) *Expr {
	v.ReplaceCount++
	return nil
}

func TestOverride_VisitExpressionChildren_Shallow(t *testing.T) {
	// 3-level: func(func(col, const), const)
	inner := mkFunc("=", mkCol("t", "a", 1, 0), mkConst(1))
	outer := mkFunc("=", inner, mkConst(2))
	op := &LogicalOperator{
		Typ:     LOT_Filter,
		Filters: []*Expr{outer},
	}
	v := &shallowVisitor{}
	v.Self = v
	v.VisitOperator(op)
	// Only the top-level expr's VisitReplace is called (1 time for the filter expr).
	// Children are NOT recursed because VisitExpressionChildren is empty.
	assert.Equal(t, 1, v.ReplaceCount)
}

// 3.7 — Override VisitReplace (column replacer)
type columnReplacer struct {
	BaseVisitor
	OldBind ColumnBind
	NewBind ColumnBind
}

func (r *columnReplacer) VisitReplace(expr *Expr) *Expr {
	if expr.Typ == ET_Column && expr.ColRef == r.OldBind {
		newExpr := &Expr{
			Typ:      ET_Column,
			BaseInfo: expr.BaseInfo,
			DataTyp:  expr.DataTyp,
		}
		newExpr.ColRef = r.NewBind
		return newExpr
	}
	return nil
}

func TestOverride_VisitReplace_ColumnReplacer(t *testing.T) {
	col1 := mkCol("t", "a", 1, 0)
	col2 := mkCol("t", "b", 1, 1)
	op := &LogicalOperator{
		Typ:     LOT_Filter,
		Filters: []*Expr{mkFunc("=", col1, col2)},
	}
	r := &columnReplacer{OldBind: ColumnBind{1, 0}, NewBind: ColumnBind{9, 9}}
	r.Self = r
	r.VisitOperator(op)
	// col1 (1,0) should be replaced with (9,9)
	assert.Equal(t, ColumnBind{9, 9}, op.Filters[0].Children[0].ColRef)
	// col2 (1,1) should NOT be replaced
	assert.Equal(t, ColumnBind{1, 1}, op.Filters[0].Children[1].ColRef)
}

// 3.8 — Multi-level inheritance
type level1Visitor struct {
	BaseVisitor
	Log []string
}

func (v *level1Visitor) VisitOperator(op *LogicalOperator) {
	v.Log = append(v.Log, op.Typ.String())
	DefaultVisitOperator(v.Self, op)
}

type level2Visitor struct {
	level1Visitor
	ReplaceCount int
}

func (v *level2Visitor) VisitReplace(expr *Expr) *Expr {
	if expr.Typ == ET_Column {
		v.ReplaceCount++
	}
	return nil
}

func TestOverride_MultiLevel(t *testing.T) {
	plan := buildTestPlan()
	v := &level2Visitor{}
	v.Self = v // Self points to outermost
	v.VisitOperator(plan)
	// VisitOperator from level1 should log all ops
	assert.Equal(t, 5, len(v.Log))
	// VisitReplace from level2 should count columns
	assert.True(t, v.ReplaceCount > 0)
}

// 3.9 — Self not set
func TestBaseVisitor_SelfNotSet_Panics(t *testing.T) {
	v := &BaseVisitor{} // Self is nil
	op := &LogicalOperator{Typ: LOT_Scan, Info: &ScanOpInfo{}}
	assert.Panics(t, func() {
		v.VisitOperator(op)
	})
}

// 3.10 — Read-only traversal
func TestBaseVisitor_ReadOnly(t *testing.T) {
	plan := buildTestPlan()
	before, err := ExplainLogicalPlan(plan)
	require.NoError(t, err)

	v := &BaseVisitor{}
	v.Self = v
	v.VisitOperator(plan)

	after, err := ExplainLogicalPlan(plan)
	require.NoError(t, err)
	assert.Equal(t, before, after)
}

// ============================================================
// Stage 3 extra: pointer correctness — verify **Expr modifies in place
// ============================================================

func TestEnumerateExpressions_PointerModify(t *testing.T) {
	original := mkConst(1)
	op := &LogicalOperator{
		Typ:     LOT_Filter,
		Filters: []*Expr{original},
	}
	replacement := mkConst(999)
	EnumerateExpressions(op, func(expr **Expr) {
		if (*expr).Typ == ET_Const {
			*expr = replacement
		}
	})
	assert.Same(t, replacement, op.Filters[0])
}

// ============================================================
// Stage 4: Integration tests — TPC-H
// ============================================================

func genLogicalPlan(t *testing.T, txn *storage.Txn, id int) *LogicalOperator {
	t.Helper()
	conf := loadTestConfig()
	stmts, err := genStmts(conf, id)
	require.NoError(t, err)
	builder := NewBuilder(txn)
	err = builder.buildSelect(stmts[0].GetStmt().GetSelectStmt(), builder.rootCtx, 0)
	require.NoError(t, err)
	lp, err := builder.CreatePlan(builder.rootCtx, nil)
	require.NoError(t, err)
	lp, err = builder.Optimize(builder.rootCtx, lp)
	require.NoError(t, err)
	return lp
}

// 4.1 — Traverse all 22 TPC-H queries, verify no panic
func TestVisitor_TPCH_TraverseAll(t *testing.T) {
	runQueryInTxn(t, func(txn *storage.Txn) {
		for qid := 1; qid <= 22; qid++ {
			lp := genLogicalPlan(t, txn, qid)
			cv := &countingVisitor{}
			cv.Self = cv
			cv.VisitOperator(lp)
			assert.True(t, cv.OpCount > 0, "q%d: should visit at least 1 operator", qid)
		}
	})
}

// 4.2 — Identity replace on TPC-H q1: verify no panic
func TestVisitor_TPCH_IdentityReplace(t *testing.T) {
	runQueryInTxn(t, func(txn *storage.Txn) {
		lp := genLogicalPlan(t, txn, 1)

		idv := &identityVisitor{}
		idv.Self = idv
		idv.VisitOperator(lp)
		// If we get here without panic, identity replace works
	})
}

type identityVisitor struct {
	BaseVisitor
}

func (v *identityVisitor) VisitReplace(expr *Expr) *Expr {
	return expr // identity: return same pointer
}
