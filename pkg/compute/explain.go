package compute

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/daviszhen/plan/pkg/common"
)

// ExplainWriter manages tree-shaped indented output (PostgreSQL EXPLAIN style).
type ExplainWriter struct {
	buf   strings.Builder
	level int
}

// Node writes a title line with "->  " prefix at level > 0.
func (w *ExplainWriter) Node(format string, args ...any) {
	line := fmt.Sprintf(format, args...)
	if w.level <= 0 {
		w.buf.WriteString(line)
	} else {
		pad := w.level*6 - 6
		w.buf.WriteString(strings.Repeat(" ", pad))
		w.buf.WriteString("->  ")
		w.buf.WriteString(line)
	}
	w.buf.WriteByte('\n')
}

// Detail writes a detail line indented under the current node.
func (w *ExplainWriter) Detail(format string, args ...any) {
	line := fmt.Sprintf(format, args...)
	indent := w.detailIndent()
	w.buf.WriteString(strings.Repeat(" ", indent))
	w.buf.WriteString(line)
	w.buf.WriteByte('\n')
}

func (w *ExplainWriter) Indent()  { w.level++ }
func (w *ExplainWriter) Dedent()  { w.level-- }
func (w *ExplainWriter) String() string { return strings.TrimRight(w.buf.String(), "\n") }

func (w *ExplainWriter) detailIndent() int {
	if w.level <= 0 {
		return 2
	}
	return w.level*6 + 2
}

// Explainable — operator Info structs implement this to output their details.
type Explainable interface {
	Explain(w *ExplainWriter)
}

// ExprFormatter — Expr Info structs implement this for inline expression formatting.
type ExprFormatter interface {
	FormatExpr(e *Expr, buf *bytes.Buffer)
}

// --- helpers ---

func fmtExprs(exprs []*Expr) string {
	buf := &bytes.Buffer{}
	opts := NewExplainOptions()
	for i, e := range exprs {
		if e == nil {
			continue
		}
		if i > 0 {
			buf.WriteString(", ")
		}
		explainExpr(e, opts, buf)
		if e.Alias != "" {
			fmt.Fprintf(buf, " as %v", e.Alias)
		}
		if e.Typ == ET_Orderby {
			if e.GetOrderByInfo().Desc {
				buf.WriteString(" desc")
			}
		}
	}
	return buf.String()
}

func fmtColumns(col2Idx map[string]int, typs []common.LType, cols []string) string {
	lines := make([]string, 0, len(cols))
	if len(col2Idx) > 0 {
		for i, col := range cols {
			idx := col2Idx[col]
			lines = append(lines, fmt.Sprintf("%v#%d::%v", col, i, typs[idx]))
		}
	} else {
		for i, col := range cols {
			if i >= len(typs) {
				lines = append(lines, fmt.Sprintf("%v#%d", col, i))
			} else {
				lines = append(lines, fmt.Sprintf("%v#%d::%v", col, i, typs[i]))
			}
		}
	}
	return strings.Join(lines, ", ")
}

func fmtSimpleColumns(cols []string) string {
	t := strings.Builder{}
	for i, col := range cols {
		if i > 0 {
			t.WriteString(", ")
		}
		t.WriteString(fmt.Sprintf("%v#%d", col, i))
	}
	return t.String()
}

// --- Logical plan explain ---

func ExplainLogicalPlan(root *LogicalOperator) (string, error) {
	w := &ExplainWriter{}
	explainLogicalNode(root, w)
	return w.String(), nil
}

func explainLogicalNode(lo *LogicalOperator, w *ExplainWriter) {
	if lo == nil {
		return
	}
	// title line
	w.Node("%s", logicalTitle(lo))

	// outputs, counts, colRefToPos
	logicalOutputs(lo, w)

	// type-specific details
	logicalDetails(lo, w)

	// children
	w.Indent()
	for _, child := range lo.Children {
		explainLogicalNode(child, w)
	}
	w.Dedent()
}

func logicalTitle(lo *LogicalOperator) string {
	buf := &strings.Builder{}
	buf.WriteString(lo.Typ.String())
	if lo.Typ == LOT_Scan {
		buf.WriteString(" on ")
		db := lo.getScanDatabase()
		tbl := lo.getScanTable()
		alias := lo.getScanAlias()
		if alias != "" && alias != tbl {
			fmt.Fprintf(buf, "%v.%v %v", db, tbl, alias)
		} else {
			fmt.Fprintf(buf, "%v.%v", db, tbl)
		}
	}
	fmt.Fprintf(buf, " (Id %d) ", lo.Index)
	return buf.String()
}

func logicalOutputs(lo *LogicalOperator, w *ExplainWriter) {
	if len(lo.Outputs) != 0 {
		w.Detail("outputs:%s", fmtExprs(lo.Outputs))
	}
	if len(lo.Counts) != 0 {
		buf := &strings.Builder{}
		j := 0
		for bind, i := range lo.Counts {
			if j > 0 {
				buf.WriteString(", ")
			}
			fmt.Fprintf(buf, "%v %v", bind, i)
			j++
		}
		w.Detail("counts:%s", buf.String())
	}
	if len(lo.ColRefToPos) != 0 {
		buf := &strings.Builder{}
		binds := lo.ColRefToPos.sortByColumnBind()
		j := 0
		for i, bind := range binds {
			if j > 0 {
				buf.WriteString(", ")
			}
			fmt.Fprintf(buf, "%v %v", bind, i)
			j++
		}
		w.Detail("colRefToPos:%s", buf.String())
	}
}

func logicalDetails(lo *LogicalOperator, w *ExplainWriter) {
	switch lo.Typ {
	case LOT_Project:
		w.Detail("exprs:%s", fmtExprs(lo.Projects))
	case LOT_Filter:
		w.Detail("exprs:%s", fmtExprs(lo.Filters))
	case LOT_Scan:
		db := lo.getScanDatabase()
		tbl := lo.getScanTable()
		alias := lo.getScanAlias()
		tableInfo := ""
		if alias != "" && alias != tbl {
			tableInfo = fmt.Sprintf("%v.%v %v", db, tbl, alias)
		} else {
			tableInfo = fmt.Sprintf("%v.%v", db, tbl)
		}
		w.Detail("table: %v", tableInfo)
		if len(lo.getScanColumns()) > 0 {
			w.Detail("columns:%s", fmtColumns(lo.getScanColName2Idx(), lo.getScanTypes(), lo.getScanColumns()))
		}
		w.Detail("filters:%s", fmtExprs(lo.Filters))
	case LOT_JOIN:
		w.Detail("type:%s", lo.getJoinTyp().String())
		w.Detail("on:%s", fmtExprs(lo.getOnConds()))
	case LOT_AggGroup:
		w.Detail("groupExprs[#%d]:%s", lo.Index, fmtExprs(lo.getGroupBys()))
		w.Detail("aggExprs[#%d]:%s", lo.getAggTag(), fmtExprs(lo.getAggs()))
		w.Detail("filters:%s", fmtExprs(lo.Filters))
	case LOT_Order:
		w.Detail("exprs:%s", fmtExprs(lo.getOrderBys()))
	case LOT_Limit:
		li := lo.Info.(*LimitOpInfo)
		w.Detail("limit:%s", fmtExprs([]*Expr{li.Limit}))
	case LOT_CreateSchema:
		cs := lo.Info.(*CreateSchemaOpInfo)
		w.Detail("CreateSchema: %v %v", cs.Database, cs.IfNotExists)
	case LOT_CreateTable:
		ct := lo.Info.(*CreateTableOpInfo)
		w.Detail("CreateTable: %v %v %v", ct.Database, ct.Table, ct.IfNotExists)
	}
}

// --- Physical plan explain ---

func ExplainPhysicalPlan(root *PhysicalOperator) (string, error) {
	w := &ExplainWriter{}
	explainPhysicalNode(root, w)
	return w.String(), nil
}

func explainPhysicalNode(po *PhysicalOperator, w *ExplainWriter) {
	if po == nil {
		return
	}
	// title line
	w.Node("%s", physicalTitle(po))

	// outputs
	if len(po.Outputs) != 0 {
		w.Detail("outputs:%s", fmtExprs(po.Outputs))
	}

	// type-specific details
	physicalDetails(po, w)

	// children
	w.Indent()
	for _, child := range po.Children {
		explainPhysicalNode(child, w)
	}
	w.Dedent()
}

func physicalTitle(po *PhysicalOperator) string {
	buf := &strings.Builder{}
	buf.WriteString(po.Typ.String())
	if po.Typ == POT_Scan {
		buf.WriteString(" on ")
		db := po.getScanDatabase()
		tbl := po.getScanTable()
		alias := po.getScanAlias()
		if alias != "" && alias != tbl {
			fmt.Fprintf(buf, "%v.%v %v", db, tbl, alias)
		} else {
			fmt.Fprintf(buf, "%v.%v", db, tbl)
		}
	}
	fmt.Fprintf(buf, " (Id %d ", po.Id)
	fmt.Fprintf(buf, "estCard %d)", po.estimatedCard)
	return buf.String()
}

func physicalDetails(po *PhysicalOperator, w *ExplainWriter) {
	switch po.Typ {
	case POT_Project:
		w.Detail("exprs:%s", fmtExprs(po.Projects))
	case POT_Filter:
		w.Detail("exprs:%s", fmtExprs(po.Filters))
	case POT_Scan:
		if po.getScanTyp() == ScanTypeTable && len(po.getScanColumns()) > 0 {
			w.Detail("columns:%s", fmtSimpleColumns(po.getScanColumns()))
		}
		w.Detail("filters:%s", fmtExprs(po.Filters))
	case POT_Join:
		w.Detail("type:%s", po.getJoinTyp().String())
		w.Detail("on:%s", fmtExprs(po.getOnConds()))
	case POT_Agg:
		w.Detail("groupExprs[#%d]:%s", po.Index, fmtExprs(po.getGroupBys()))
		w.Detail("aggExprs[#%d]:%s", po.getAggTag(), fmtExprs(po.getAggs()))
		w.Detail("filters:%s", fmtExprs(po.Filters))
	case POT_Order:
		w.Detail("exprs:%s", fmtExprs(po.getOrderBys()))
	case POT_Limit:
		w.Detail("limit:%s", fmtExprs([]*Expr{po.getLimitExpr()}))
	case POT_Stub:
		stubTable := ""
		if si, ok := po.Info.(*StubInfo); ok {
			stubTable = si.Table
		}
		w.Detail("Stub: %v %v", stubTable, po.ChunkCount)
	case POT_CreateSchema:
		cs := po.Info.(*CreateSchemaOpInfo)
		w.Detail("CreateSchema: %v %v", cs.Database, cs.IfNotExists)
	case POT_CreateTable:
		ct := po.Info.(*CreateTableOpInfo)
		w.Detail("CreateTable: %v %v %v", ct.Database, ct.Table, ct.IfNotExists)
	case POT_Insert:
		w.Detail("Insert: %v %v", po.getScanDatabase(), po.getScanTable())
	}
}
