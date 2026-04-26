package compute

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/daviszhen/plan/pkg/common"
	"github.com/stretchr/testify/require"
)

var updateGolden = os.Getenv("UPDATE_GOLDEN") == "1"

func goldenPath(name string) string {
	return filepath.Join("testdata", name+".golden")
}

func assertGolden(t *testing.T, name, actual string) {
	t.Helper()
	path := goldenPath(name)
	if updateGolden {
		err := os.WriteFile(path, []byte(actual), 0644)
		require.NoError(t, err)
		t.Logf("updated golden file: %s", path)
		return
	}
	expected, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		t.Fatalf("golden file %s not found, run with UPDATE_GOLDEN=1", path)
	}
	require.NoError(t, err)
	require.Equal(t, string(expected), actual, "differs from golden file %s", path)
}

func colExpr(table, name string, tag, pos uint64, typ common.LType) *Expr {
	return &Expr{Typ: ET_Column, BaseInfo: BaseInfo{Table: table, Name: name, ColRef: ColumnBind{tag, pos}}, DataTyp: typ}
}

func intConst(v int64) *Expr {
	return &Expr{Typ: ET_Const, ConstValue: NewIntegerConst(v), DataTyp: common.IntegerType()}
}

func strConst(v string) *Expr {
	return &Expr{Typ: ET_Const, ConstValue: NewStringConst(v), DataTyp: common.VarcharType()}
}

func cmpExpr(op string, left, right *Expr) *Expr {
	binder := FunctionBinder{}
	return binder.BindScalarFunc(op, []*Expr{left, right}, IsOperator(op))
}

func buildSimpleScanPlan() *LogicalOperator {
	scan := &LogicalOperator{
		Typ: LOT_Scan, Index: 1,
		Info: &ScanOpInfo{Database: "public", Table: "nation", Alias: "nation",
			Columns: []string{"n_nationkey", "n_name", "n_regionkey"}},
		Outputs: []*Expr{
			colExpr("nation", "n_nationkey", 1, 0, common.IntegerType()),
			colExpr("nation", "n_name", 1, 1, common.VarcharType()),
			colExpr("nation", "n_regionkey", 1, 2, common.IntegerType()),
		},
	}
	filter := &LogicalOperator{
		Typ: LOT_Filter, Index: 2, Children: []*LogicalOperator{scan},
		Filters: []*Expr{cmpExpr("=", colExpr("nation", "n_regionkey", 1, 2, common.IntegerType()), intConst(1))},
	}
	return &LogicalOperator{
		Typ: LOT_Project, Index: 3, Children: []*LogicalOperator{filter},
		Projects: []*Expr{colExpr("nation", "n_name", 1, 1, common.VarcharType())},
		Outputs:  []*Expr{colExpr("nation", "n_name", 1, 1, common.VarcharType())},
	}
}

func buildJoinPlan() *LogicalOperator {
	scanN := &LogicalOperator{Typ: LOT_Scan, Index: 1,
		Info: &ScanOpInfo{Database: "public", Table: "nation", Columns: []string{"n_nationkey", "n_name", "n_regionkey"}}}
	scanR := &LogicalOperator{Typ: LOT_Scan, Index: 2,
		Info: &ScanOpInfo{Database: "public", Table: "region", Columns: []string{"r_regionkey", "r_name"}}}
	join := &LogicalOperator{Typ: LOT_JOIN, Index: 3,
		Info:     &JoinOpInfo{JoinTyp: LOT_JoinTypeInner, OnConds: []*Expr{cmpExpr("=", colExpr("nation", "n_regionkey", 1, 2, common.IntegerType()), colExpr("region", "r_regionkey", 2, 0, common.IntegerType()))}},
		Children: []*LogicalOperator{scanN, scanR}}
	return &LogicalOperator{Typ: LOT_Project, Index: 4, Children: []*LogicalOperator{join},
		Projects: []*Expr{colExpr("nation", "n_name", 1, 1, common.VarcharType()), colExpr("region", "r_name", 2, 1, common.VarcharType())}}
}

func TestGolden_ExplainLogical_SimpleScan(t *testing.T) {
	result, err := ExplainLogicalPlan(buildSimpleScanPlan())
	require.NoError(t, err)
	assertGolden(t, "explain_logical_simple_scan", result)
}

func TestGolden_ExplainLogical_Join(t *testing.T) {
	result, err := ExplainLogicalPlan(buildJoinPlan())
	require.NoError(t, err)
	assertGolden(t, "explain_logical_join", result)
}

func TestGolden_FormatExpr_Column(t *testing.T) {
	assertGolden(t, "format_expr_column", colExpr("nation", "n_name", 1, 1, common.VarcharType()).String())
}

func TestGolden_FormatExpr_Const(t *testing.T) {
	cases := map[string]*Expr{
		"format_expr_const_int":      intConst(42),
		"format_expr_const_string":   strConst("hello"),
		"format_expr_const_null":     {Typ: ET_Const, ConstValue: NewNullConst(), DataTyp: common.IntegerType()},
		"format_expr_const_bool":     {Typ: ET_Const, ConstValue: NewBooleanConst(true), DataTyp: common.BooleanType()},
		"format_expr_const_date":     {Typ: ET_Const, ConstValue: NewDateConst("1998-12-01"), DataTyp: common.DateType()},
		"format_expr_const_interval": {Typ: ET_Const, ConstValue: NewIntervalConst(112, "day"), DataTyp: common.IntervalType()},
	}
	for name, e := range cases {
		t.Run(name, func(t *testing.T) { assertGolden(t, name, e.String()) })
	}
}

func TestGolden_FormatExpr_Compare(t *testing.T) {
	e := cmpExpr("=", colExpr("nation", "n_regionkey", 1, 2, common.IntegerType()), intConst(1))
	assertGolden(t, "format_expr_compare", e.String())
}
