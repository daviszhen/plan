// Copyright 2023-2024 daviszhen
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

package plan

import (
	"fmt"
	"strings"

	"github.com/xlab/treeprint"

	"github.com/daviszhen/plan/pkg/common"
	"github.com/daviszhen/plan/pkg/storage"
)

// WildcardMatch implements wildcard pattern match algorithm.
// pattern and target are ascii characters
// TODO: add \_ and \%
func WildcardMatch(pattern, target *common.String) bool {
	var p = 0
	var t = 0
	var positionOfPercentPlusOne int = -1
	var positionOfTargetEncounterPercent int = -1
	plen := pattern.Length()
	tlen := target.Length()
	pSlice := pattern.DataSlice()
	tSlice := target.DataSlice()
	for t < tlen {
		//%
		if p < plen && pSlice[p] == '%' {
			p++
			positionOfPercentPlusOne = p
			if p >= plen {
				//pattern end with %
				return true
			}
			//means % matches empty
			positionOfTargetEncounterPercent = t
		} else if p < plen && (pSlice[p] == '_' || pSlice[p] == tSlice[t]) { //match or _
			p++
			t++
		} else {
			if positionOfPercentPlusOne == -1 {
				//have not matched a %
				return false
			}
			if positionOfTargetEncounterPercent == -1 {
				return false
			}
			//backtrace to last % position + 1
			p = positionOfPercentPlusOne
			//means % matches multiple characters
			positionOfTargetEncounterPercent++
			t = positionOfTargetEncounterPercent
		}
	}
	//skip %
	for p < plen && pSlice[p] == '%' {
		p++
	}
	return p >= plen
}

var _ TypeOp[common.Decimal] = new(common.Decimal)

//lint:ignore U1000

//lint:ignore U1000

type DataType int

const (
	DataTypeInteger DataType = iota
	DataTypeVarchar
	DataTypeDecimal
	DataTypeDate
	DataTypeBool
	DataTypeInterval
	DataTypeFloat64
	DataTypeInvalid // used in binding process
)

var dataType2Str = map[DataType]string{
	DataTypeInteger: "int",
	DataTypeVarchar: "varchar",
	DataTypeDecimal: "decimal",
	DataTypeDate:    "date",
	DataTypeBool:    "bool",
	DataTypeInvalid: "invalid",
}

func (dt DataType) String() string {
	if s, ok := dataType2Str[dt]; ok {
		return s
	}
	return "invalid"
}

func findExpr(exprs []*Expr, fun func(expr *Expr) bool) []*Expr {
	ret := make([]*Expr, 0)
	for _, expr := range exprs {
		if fun != nil && fun(expr) {
			ret = append(ret, expr)
		}
	}
	return ret
}

func checkExprIsValid(root *LogicalOperator) {
	if root == nil {
		return
	}
	checkExprs(root.Projects...)
	checkExprs(root.Filters...)
	checkExprs(root.OnConds...)
	checkExprs(root.Aggs...)
	checkExprs(root.GroupBys...)
	checkExprs(root.OrderBys...)
	checkExprs(root.Limit)
	for _, child := range root.Children {
		checkExprIsValid(child)
	}
}

func checkExprs(e ...*Expr) {
	for _, expr := range e {
		if expr == nil {
			continue
		}
		if expr.Typ == ET_Func && expr.SubTyp == ET_Invalid {
			panic("xxx")
		}
		if expr.Typ == ET_Func && expr.SubTyp == ET_Between {
			if len(expr.Children) != 3 {
				panic("invalid between")
			}
		}
		if expr.Typ == ET_Func && expr.FunImpl == nil {
			panic("invalid function")
		}
		if expr.DataTyp.Id == common.LTID_INVALID {
			panic("invalid logical type")
		}
	}
}

type POT int

const (
	POT_Project      POT = 0
	POT_With         POT = 1
	POT_Filter       POT = 2
	POT_Agg          POT = 3
	POT_Join         POT = 4
	POT_Order        POT = 5
	POT_Limit        POT = 6
	POT_Scan         POT = 7
	POT_Stub         POT = 8 //test stub
	POT_CreateSchema POT = 9
	POT_CreateTable  POT = 10
	POT_Insert       POT = 11
)

var potToStr = map[POT]string{
	POT_Project:      "project",
	POT_With:         "with",
	POT_Filter:       "filter",
	POT_Agg:          "agg",
	POT_Join:         "join",
	POT_Order:        "order",
	POT_Limit:        "limit",
	POT_Scan:         "scan",
	POT_Stub:         "stub",
	POT_CreateSchema: "createSchema",
	POT_CreateTable:  "createTable",
	POT_Insert:       "insert",
}

func (t POT) String() string {
	if s, has := potToStr[t]; has {
		return s
	}
	panic(fmt.Sprintf("usp %d", t))
}

type PhysicalOperator struct {
	Typ POT
	Tag int //relationTag
	Id  int

	Index         uint64
	Index2        uint64
	Database      string
	Table         string // table
	Name          string // column
	Alias         string // alias
	JoinTyp       LOT_JoinType
	Outputs       []*Expr
	Columns       []string // name of project
	Projects      []*Expr
	Filters       []*Expr
	Aggs          []*Expr
	GroupBys      []*Expr
	OnConds       []*Expr
	OrderBys      []*Expr
	Limit         *Expr
	Offset        *Expr
	estimatedCard uint64
	ChunkCount    int //for stub
	IfNotExists   bool
	ColDefs       []*storage.ColumnDefinition //for create table
	Constraints   []*storage.Constraint       //for create table
	TableEnt      *storage.CatalogEntry
	ScanTyp       ScanType
	Types         []common.LType        //for insert ... values
	collection    *ColumnDataCollection //for insert ... values
	ColName2Idx   map[string]int
	InsertTypes   []common.LType //for insert ... values
	//column seq no in table -> column seq no in Insert
	ColumnIndexMap []int //for insert
	ScanInfo       *ScanInfo
	Children       []*PhysicalOperator
	ExecStats      ExecStats
}

func (po *PhysicalOperator) String() string {
	tree := treeprint.NewWithRoot("PhysicalPlan:")
	po.Print(tree)
	return tree.String()
}

func printPhyOutputs(tree treeprint.Tree, root *PhysicalOperator) {
	tree.AddMetaNode("Id", fmt.Sprintf("%d", root.Id))
	if len(root.Outputs) != 0 {
		node := tree.AddMetaBranch("outputs", "")
		listExprsToTree(node, root.Outputs)
	}
	tree.AddMetaNode("estCard", root.estimatedCard)
}

func (po *PhysicalOperator) Print(tree treeprint.Tree) {
	if po == nil {
		return
	}
	switch po.Typ {
	case POT_Project:
		tree = tree.AddBranch("Project:")
		printPhyOutputs(tree, po)
		tree.AddMetaNode("index", fmt.Sprintf("%d", po.Index))
		node := tree.AddMetaBranch("exprs", "")
		listExprsToTree(node, po.Projects)
	case POT_Filter:
		tree = tree.AddBranch("Filter:")
		printPhyOutputs(tree, po)
		node := tree.AddMetaBranch("exprs", "")
		listExprsToTree(node, po.Filters)
	case POT_Scan:
		tree = tree.AddBranch("Scan:")
		printPhyOutputs(tree, po)
		tree.AddMetaNode("index", fmt.Sprintf("%d", po.Index))
		tableInfo := ""
		if len(po.Alias) != 0 && po.Alias != po.Table {
			tableInfo = fmt.Sprintf("%v.%v %v", po.Database, po.Table, po.Alias)
		} else {
			tableInfo = fmt.Sprintf("%v.%v", po.Database, po.Table)
		}
		tree.AddMetaNode("table", tableInfo)
		if po.ScanTyp == ScanTypeTable {
			printColumns := func(cols []string) string {
				t := strings.Builder{}
				t.WriteByte('\n')
				for i, col := range cols {
					t.WriteString(fmt.Sprintf("col %d %v", i, col))
					t.WriteByte('\n')
				}
				return t.String()
			}
			if len(po.Columns) > 0 {
				tree.AddMetaNode("columns", printColumns(po.Columns))
			} else {
				panic("usp")
			}
		}

		node := tree.AddBranch("filters")
		listExprsToTree(node, po.Filters)
		//printStats := func(columns []string) string {
		//	sb := strings.Builder{}
		//	sb.WriteString(fmt.Sprintf("rowcount %v\n", po.Stats.RowCount))
		//	for colIdx, colName := range po.Columns {
		//		originIdx := catalogTable.Column2Idx[colName]
		//		sb.WriteString(fmt.Sprintf("col %v %v ", colIdx, colName))
		//		sb.WriteString(po.Stats.ColStats[originIdx].String())
		//		sb.WriteByte('\n')
		//	}
		//	return sb.String()
		//}
		//if len(po.Columns) > 0 {
		//	tree.AddMetaNode("stats", printStats(po.Columns))
		//} else {
		//	tree.AddMetaNode("stats", printStats(catalogTable.Columns))
		//}

	case POT_Join:
		tree = tree.AddBranch(fmt.Sprintf("Join (%v):", po.JoinTyp))
		printPhyOutputs(tree, po)
		tree.AddMetaNode("index", fmt.Sprintf("%d", po.Index))
		if len(po.OnConds) > 0 {
			node := tree.AddMetaBranch("On", "")
			listExprsToTree(node, po.OnConds)
		}
		//if po.Stats != nil {
		//	tree.AddMetaNode("Stats", po.Stats.String())
		//}
	case POT_Agg:
		tree = tree.AddBranch("Aggregate:")
		printPhyOutputs(tree, po)
		if len(po.GroupBys) > 0 {
			node := tree.AddBranch(fmt.Sprintf("groupExprs, index %d", po.Index))
			listExprsToTree(node, po.GroupBys)
		}
		if len(po.Aggs) > 0 {
			node := tree.AddBranch(fmt.Sprintf("aggExprs, index %d", po.Index2))
			listExprsToTree(node, po.Aggs)
		}
		if len(po.Filters) > 0 {
			node := tree.AddBranch("filters")
			listExprsToTree(node, po.Filters)
		}

	case POT_Order:
		tree = tree.AddBranch("Order:")
		printPhyOutputs(tree, po)
		node := tree.AddMetaBranch("exprs", "")
		listExprsToTree(node, po.OrderBys)
	case POT_Limit:
		tree = tree.AddBranch(fmt.Sprintf("Limit: %v", po.Limit.String()))
		printPhyOutputs(tree, po)
	case POT_Stub:
		tree = tree.AddBranch(fmt.Sprintf("Stub: %v %v", po.Table, po.ChunkCount))
		printPhyOutputs(tree, po)
	case POT_CreateSchema:
		tree = tree.AddBranch(fmt.Sprintf("CreateSchema: %v %v", po.Database, po.IfNotExists))
	case POT_CreateTable:
		tree = tree.AddBranch(fmt.Sprintf("CreateTable: %v %v %v", po.Database, po.Table, po.IfNotExists))
		node := tree.AddMetaBranch("colDefs", "")
		listColDefsToTree(node, po.ColDefs)
		consStr := make([]string, 0)
		for _, cons := range po.Constraints {
			consStr = append(consStr, cons.String())
		}
		tree.AddMetaNode("constraints", strings.Join(consStr, ","))
	case POT_Insert:
		tree = tree.AddBranch(fmt.Sprintf("Insert: %v %v", po.Database, po.Table))
	default:
		panic(fmt.Sprintf("usp %v", po.Typ))
	}
	tree.AddMetaNode("Exec Stats", po.ExecStats.String())

	for _, child := range po.Children {
		child.Print(tree)
	}
}

func collectFilterExprs(root *PhysicalOperator) []*Expr {
	if root == nil {
		return nil
	}
	ret := make([]*Expr, 0)
	ret = append(ret, root.Filters...)
	ret = append(ret, root.OnConds...)
	for _, child := range root.Children {
		ret = append(ret, collectFilterExprs(child)...)
	}
	return ret
}

type Catalog struct {
	tpch map[string]*CatalogTable
}

func (c *Catalog) Table(db, table string) (*CatalogTable, error) {
	if db == "tpch" {
		if c, ok := c.tpch[table]; ok {
			return c, nil
		} else {
			panic(fmt.Errorf("table %s in database %s does not exist", table, db))
		}
	} else {
		panic(fmt.Sprintf("database %s does not exist", db))
	}
}

type Stats struct {
	RowCount float64
	ColStats []*BaseStats
}

func (s *Stats) Copy() *Stats {
	ret := &Stats{
		RowCount: s.RowCount,
	}
	ret.ColStats = make([]*BaseStats, len(s.ColStats))
	for i, stat := range s.ColStats {
		ret.ColStats[i] = stat.Copy()
	}
	return ret
}

func (s *Stats) String() string {
	if s == nil {
		return ""
	}
	sb := strings.Builder{}
	sb.WriteString(fmt.Sprintf("rowcount %v\n", s.RowCount))
	for i, stat := range s.ColStats {
		sb.WriteString(fmt.Sprintf("col %v ", i))
		sb.WriteString(stat.String())
		sb.WriteByte('\n')
	}
	return sb.String()
}

func convertStats(tstats *storage.TableStats) *Stats {
	ret := &Stats{
		RowCount: float64(tstats.GetStats(0).Count()),
	}
	for _, cstats := range tstats.GetStats2() {
		ret.ColStats = append(ret.ColStats, convertStats2(cstats))
	}
	return ret
}

func convertStats2(cstats *storage.ColumnStats) *BaseStats {
	return &BaseStats{
		hasNull:       cstats.HasNull(),
		hasNoNull:     cstats.HasNoNull(),
		distinctCount: cstats.DistinctCount(),
	}
}

func convertStats3(bstats *storage.BaseStats) *BaseStats {
	return &BaseStats{
		hasNull:       bstats.HasNull(),
		hasNoNull:     bstats.HasNoNull(),
		distinctCount: bstats.DistinctCount(),
	}
}

type CatalogTable struct {
	Db         string
	Table      string
	Columns    []string
	Types      []common.LType
	PK         []int
	Column2Idx map[string]int
	Stats      *Stats
}

func splitExprByAnd(expr *Expr) []*Expr {
	if expr.Typ == ET_Func {
		if expr.SubTyp == ET_And {
			return append(splitExprByAnd(expr.Children[0]), splitExprByAnd(expr.Children[1])...)
		}
	}
	return []*Expr{expr.copy()}
}

func splitExprsByAnd(exprs []*Expr) []*Expr {
	ret := make([]*Expr, 0)
	for _, e := range exprs {
		if e == nil {
			continue
		}
		ret = append(ret, splitExprByAnd(e)...)
	}
	return ret
}

func splitExprByOr(expr *Expr) []*Expr {
	if expr.Typ == ET_Func {
		if expr.SubTyp == ET_Or {
			return append(splitExprByOr(expr.Children[0]), splitExprByOr(expr.Children[1])...)
		}
	}
	return []*Expr{expr.copy()}
}

func andExpr(a, b *Expr) *Expr {
	binder := FunctionBinder{}
	return binder.BindScalarFunc(ET_And.String(), []*Expr{a, b}, ET_And, ET_And.isOperator())
}

func combineExprsByAnd(exprs ...*Expr) *Expr {
	if len(exprs) == 1 {
		return exprs[0]
	} else if len(exprs) == 2 {
		return andExpr(exprs[0], exprs[1])
	} else {
		return andExpr(
			combineExprsByAnd(exprs[:len(exprs)-1]...),
			combineExprsByAnd(exprs[len(exprs)-1]))
	}
}

func orExpr(a, b *Expr) *Expr {
	binder := FunctionBinder{}
	return binder.BindScalarFunc(ET_Or.String(), []*Expr{a, b}, ET_Or, ET_Or.isOperator())
}

func combineExprsByOr(exprs ...*Expr) *Expr {
	if len(exprs) == 1 {
		return exprs[0]
	} else if len(exprs) == 2 {
		return orExpr(exprs[0], exprs[1])
	} else {
		return orExpr(
			combineExprsByOr(exprs[:len(exprs)-1]...),
			combineExprsByOr(exprs[len(exprs)-1]))
	}
}

// removeCorrExprs remove correlated columns from exprs
// , returns non-correlated exprs and correlated exprs.
func removeCorrExprs(exprs []*Expr) ([]*Expr, []*Expr) {
	nonCorrExprs := make([]*Expr, 0)
	corrExprs := make([]*Expr, 0)
	for _, expr := range exprs {
		newExpr, hasCorCol := deceaseDepth(expr)
		if hasCorCol {
			corrExprs = append(corrExprs, newExpr)
		} else {
			nonCorrExprs = append(nonCorrExprs, newExpr)
		}
	}
	return nonCorrExprs, corrExprs
}

// deceaseDepth decrease depth of the column
// , returns new column ref and returns it is correlated or not.
func deceaseDepth(expr *Expr) (*Expr, bool) {
	hasCorCol := false
	switch expr.Typ {
	case ET_Column:
		if expr.Depth > 0 {
			expr.Depth--
			return expr, expr.Depth > 0
		}
		return expr, false

	case ET_Func:
		switch expr.SubTyp {
		case ET_And, ET_Equal, ET_Like, ET_GreaterEqual, ET_Less, ET_NotEqual:
			left, leftHasCorr := deceaseDepth(expr.Children[0])
			hasCorCol = hasCorCol || leftHasCorr
			right, rightHasCorr := deceaseDepth(expr.Children[1])
			hasCorCol = hasCorCol || rightHasCorr
			return &Expr{
				Typ:      expr.Typ,
				SubTyp:   expr.SubTyp,
				Svalue:   expr.SubTyp.String(),
				DataTyp:  expr.DataTyp,
				Children: []*Expr{left, right},
				FunImpl:  expr.FunImpl,
			}, hasCorCol
		case ET_SubFunc:
			args := make([]*Expr, 0, len(expr.Children))
			for _, child := range expr.Children {
				newChild, yes := deceaseDepth(child)
				hasCorCol = hasCorCol || yes
				args = append(args, newChild)
			}
			return &Expr{
				Typ:      expr.Typ,
				SubTyp:   expr.SubTyp,
				Svalue:   expr.Svalue,
				DataTyp:  expr.DataTyp,
				Children: args,
				FunImpl:  expr.FunImpl,
			}, hasCorCol
		default:
			panic(fmt.Sprintf("usp %v", expr.SubTyp))
		}
	default:
		panic(fmt.Sprintf("usp %v", expr.Typ))
	}
}

func replaceColRef(e *Expr, bind, newBind ColumnBind) *Expr {
	if e == nil {
		return nil
	}
	switch e.Typ {
	case ET_Column:
		if bind == e.ColRef {
			e.ColRef = newBind
		}

	case ET_SConst, ET_IConst, ET_DateConst, ET_IntervalConst, ET_BConst, ET_FConst, ET_NConst, ET_DecConst:
	case ET_Func:
	case ET_Orderby:
	default:
		panic("usp")
	}
	for i, child := range e.Children {
		e.Children[i] = replaceColRef(child, bind, newBind)
	}
	return e
}

func replaceColRef2(e *Expr, colRefToPos ColumnBindPosMap, st SourceType) *Expr {
	if e == nil {
		return nil
	}
	switch e.Typ {
	case ET_Column:
		has, pos := colRefToPos.pos(e.ColRef)
		if has {
			e.ColRef[0] = uint64(st)
			e.ColRef[1] = uint64(pos)
		}

	case ET_SConst, ET_IConst, ET_DateConst, ET_IntervalConst, ET_BConst, ET_FConst, ET_NConst, ET_DecConst:
	case ET_Func:
	case ET_Orderby:
	default:
		panic("usp")
	}
	for i, child := range e.Children {
		e.Children[i] = replaceColRef2(child, colRefToPos, st)
	}
	return e
}

func replaceColRef3(es []*Expr, colRefToPos ColumnBindPosMap, st SourceType) {
	for _, e := range es {
		replaceColRef2(e, colRefToPos, st)
	}
}

func collectColRefs(e *Expr, set ColumnBindSet) {
	if e == nil {
		return
	}
	switch e.Typ {
	case ET_Column:
		set.insert(e.ColRef)

	case ET_Func:
	case ET_SConst, ET_IConst, ET_DateConst, ET_IntervalConst, ET_BConst, ET_FConst, ET_NConst, ET_DecConst:
	case ET_Orderby:
	default:
		panic("usp")
	}
	for _, child := range e.Children {
		collectColRefs(child, set)
	}
}

func collectColRefs2(set ColumnBindSet, exprs ...*Expr) {
	for _, expr := range exprs {
		collectColRefs(expr, set)
	}
}

func checkColRefPos(e *Expr, root *LogicalOperator) {
	if e == nil || root == nil {
		return
	}
	if e.Typ == ET_Column {
		if root.Typ == LOT_Scan {
			if !(e.ColRef.table() == root.Index && e.ColRef.column() < uint64(len(root.Columns))) {
				panic(fmt.Sprintf("no bind %v in scan %v", e.ColRef, root.Index))
			}
		} else if root.Typ == LOT_AggGroup {
			st := SourceType(e.ColRef.table())
			switch st {
			case ThisNode:
				if !(e.ColRef.table() == root.Index2 && e.ColRef.column() < uint64(len(root.Aggs))) {
					panic(fmt.Sprintf("no bind %v in scan %v", e.ColRef, root.Index))
				}
			case LeftChild:
				if len(root.Children) < 1 || root.Children[0] == nil {
					panic("no child")
				}
				binds := root.Children[0].ColRefToPos.sortByColumnBind()
				if e.ColRef.column() >= uint64(len(binds)) {
					panic(fmt.Sprintf("no bind %v in child", e.ColRef))
				}
			case RightChild:
				if len(root.Children) < 2 || root.Children[1] == nil {
					panic("no right child")
				}
				binds := root.Children[1].ColRefToPos.sortByColumnBind()
				if e.ColRef.column() >= uint64(len(binds)) {
					panic(fmt.Sprintf("no bind %v in right child", e.ColRef))
				}
			default:
				if !(e.ColRef.table() == root.Index2 && e.ColRef.column() < uint64(len(root.Aggs))) {
					panic(fmt.Sprintf("no bind %v in scan %v", e.ColRef, root.Index))
				}
			}
		} else {
			st := SourceType(e.ColRef.table())
			switch st {
			case ThisNode:
				panic(fmt.Sprintf("bind %v exists", e.ColRef))
			case LeftChild:
				if len(root.Children) < 1 || root.Children[0] == nil {
					panic("no child")
				}
				binds := root.Children[0].ColRefToPos.sortByColumnBind()
				if e.ColRef.column() >= uint64(len(binds)) {
					panic(fmt.Sprintf("no bind %v in child", e.ColRef))
				}
			case RightChild:
				if len(root.Children) < 2 || root.Children[1] == nil {
					panic("no right child")
				}
				binds := root.Children[1].ColRefToPos.sortByColumnBind()
				if e.ColRef.column() >= uint64(len(binds)) {
					panic(fmt.Sprintf("no bind %v in right child", e.ColRef))
				}
			default:
				panic(fmt.Sprintf("no source type %d", st))
			}
		}
	}
	for _, child := range e.Children {
		checkColRefPos(child, root)
	}
}

func checkColRefPosInExprs(es []*Expr, root *LogicalOperator) {
	for _, e := range es {
		checkColRefPos(e, root)
	}
}

func checkColRefPosInNode(root *LogicalOperator) {
	if root == nil {
		return
	}
	checkColRefPosInExprs(root.Projects, root)
	checkColRefPosInExprs(root.Filters, root)
	checkColRefPosInExprs(root.OnConds, root)
	checkColRefPosInExprs(root.Aggs, root)
	checkColRefPosInExprs(root.GroupBys, root)
	checkColRefPosInExprs(root.OrderBys, root)
	checkColRefPosInExprs([]*Expr{root.Limit}, root)
}
