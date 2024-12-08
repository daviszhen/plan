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

	"github.com/huandu/go-clone"
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

type LOT int

const (
	LOT_Project      LOT = 0
	LOT_Filter       LOT = 1
	LOT_Scan         LOT = 2
	LOT_JOIN         LOT = 3
	LOT_AggGroup     LOT = 4
	LOT_Order        LOT = 5
	LOT_Limit        LOT = 6
	LOT_CreateSchema LOT = 7
	LOT_CreateTable  LOT = 8
	LOT_Insert       LOT = 9
)

func (lt LOT) String() string {
	switch lt {
	case LOT_Project:
		return "Project"
	case LOT_Filter:
		return "Filter"
	case LOT_Scan:
		return "Scan"
	case LOT_JOIN:
		return "Join"
	case LOT_AggGroup:
		return "Aggregate"
	case LOT_Order:
		return "Order"
	case LOT_Limit:
		return "Limit"
	case LOT_CreateSchema:
		return "CreateSchema"
	case LOT_CreateTable:
		return "CreateTable"
	case LOT_Insert:
		return "Insert"
	default:
		panic(fmt.Sprintf("usp %d", lt))
	}
}

type LOT_JoinType int

const (
	LOT_JoinTypeCross LOT_JoinType = iota
	LOT_JoinTypeLeft
	LOT_JoinTypeInner
	LOT_JoinTypeSEMI
	LOT_JoinTypeANTI
	LOT_JoinTypeSINGLE
	LOT_JoinTypeMARK
	LOT_JoinTypeAntiMARK
	LOT_JoinTypeOUTER
)

func (lojt LOT_JoinType) String() string {
	switch lojt {
	case LOT_JoinTypeCross:
		return "cross"
	case LOT_JoinTypeLeft:
		return "left"
	case LOT_JoinTypeInner:
		return "inner"
	case LOT_JoinTypeMARK:
		return "mark"
	case LOT_JoinTypeAntiMARK:
		return "anti_mark"
	case LOT_JoinTypeSEMI:
		return "semi"
	case LOT_JoinTypeANTI:
		return "anti semi"
	default:
		panic(fmt.Sprintf("usp %d", lojt))
	}
}

type ScanType int

const (
	ScanTypeTable      ScanType = 0
	ScanTypeValuesList ScanType = 1
	ScanTypeCopyFrom   ScanType = 2
)

func (st ScanType) String() string {
	switch st {
	case ScanTypeTable:
		return "scan table"
	case ScanTypeValuesList:
		return "scan values list"
	case ScanTypeCopyFrom:
		return "scan copy from"
	default:
		panic("usp")
	}
}

type ScanOption struct {
	Kind string
	Opt  string
}

type ScanInfo struct {
	ReturnedTypes []common.LType
	Names         []string
	ColumnIds     []int
	FilePath      string
	Opts          []*ScanOption
	Format        string //for CopyFrom
}

type LogicalOperator struct {
	Typ              LOT
	Children         []*LogicalOperator
	Projects         []*Expr
	Index            uint64 //AggNode for groupTag. others in other Nodes
	Index2           uint64 //AggNode for aggTag
	Database         string
	Table            string       // table
	Alias            string       // alias
	Columns          []string     //needed column name for SCAN
	Filters          []*Expr      //for FILTER or AGG
	BelongCtx        *BindContext //for table or join
	JoinTyp          LOT_JoinType
	OnConds          []*Expr //for innor join
	Aggs             []*Expr
	GroupBys         []*Expr
	OrderBys         []*Expr
	Limit            *Expr
	Offset           *Expr
	Stats            *Stats
	hasEstimatedCard bool
	estimatedCard    uint64
	estimatedProps   *EstimatedProperties
	Outputs          []*Expr
	IfNotExists      bool
	ColDefs          []*storage.ColumnDefinition //for create table
	Constraints      []*storage.Constraint       //for create table
	TableEnt         *storage.CatalogEntry       //for insert
	TableIndex       int                         //for insert
	ExpectedTypes    []common.LType              //for insert
	IsValuesList     bool                        //for insert ... values
	ScanTyp          ScanType
	Types            []common.LType //for insert ... values
	Names            []string       //for insert ... values
	Values           [][]*Expr      //for insert ... values
	ColName2Idx      map[string]int
	//column seq no in table -> column seq no in Insert
	ColumnIndexMap []int //for insert
	ScanInfo       *ScanInfo
	Counts         ColumnBindCountMap `json:"-"`
	ColRefToPos    ColumnBindPosMap   `json:"-"`
}

func (lo *LogicalOperator) EstimatedCard(txn *storage.Txn) uint64 {
	if lo.Typ == LOT_Scan {
		{
			return lo.TableEnt.GetStats2(0).Count()
		}
		{
			//catalogTable, err := tpchCatalog().Table(lo.Database, lo.Table)
			//if err != nil {
			//	panic(err)
			//}
			//return uint64(catalogTable.Stats.RowCount)
		}
	}
	if lo.hasEstimatedCard {
		return lo.estimatedCard
	}
	maxCard := uint64(0)
	for _, child := range lo.Children {
		childCard := child.EstimatedCard(txn)
		maxCard = max(maxCard, childCard)
	}
	lo.hasEstimatedCard = true
	lo.estimatedCard = maxCard
	return lo.estimatedCard
}

func (lo *LogicalOperator) Print(tree treeprint.Tree) {
	if lo == nil {
		return
	}
	switch lo.Typ {
	case LOT_Project:
		tree = tree.AddBranch("Project:")
		printOutputs(tree, lo)
		tree.AddMetaNode("index", fmt.Sprintf("%d", lo.Index))
		node := tree.AddMetaBranch("exprs", "")
		listExprsToTree(node, lo.Projects)
	case LOT_Filter:
		tree = tree.AddBranch("Filter:")
		printOutputs(tree, lo)
		node := tree.AddMetaBranch("exprs", "")
		listExprsToTree(node, lo.Filters)
	case LOT_Scan:
		tree = tree.AddBranch("Scan:")
		printOutputs(tree, lo)
		tree.AddMetaNode("index", fmt.Sprintf("%d", lo.Index))
		tableInfo := ""
		if len(lo.Alias) != 0 && lo.Alias != lo.Table {
			tableInfo = fmt.Sprintf("%v.%v %v", lo.Database, lo.Table, lo.Alias)
		} else {
			tableInfo = fmt.Sprintf("%v.%v", lo.Database, lo.Table)
		}
		tree.AddMetaNode("table", tableInfo)

		printColumns := func(col2Idx map[string]int, typs []common.LType, cols []string) string {
			t := strings.Builder{}
			t.WriteByte('\n')
			for i, col := range cols {
				idx := col2Idx[col]
				t.WriteString(fmt.Sprintf("col %d %v %v", i, col, typs[idx]))
				t.WriteByte('\n')
			}
			return t.String()
		}
		//printColumns2 := func(cols []*Expr) string {
		//	t := strings.Builder{}
		//	t.WriteByte('\n')
		//	for _, col := range cols {
		//		t.WriteString(fmt.Sprintf("col %v %v %v", col.ColRef, col.Name, col.DataTyp))
		//		t.WriteByte('\n')
		//	}
		//	return t.String()
		//}
		if len(lo.Columns) > 0 {
			//tree.AddMetaNode("columns", printColumns2(lo.Outputs))
			tree.AddMetaNode("columns", printColumns(lo.ColName2Idx, lo.Types, lo.Columns))
		} else {
			panic("usp")
			//catalogTable, err := tpchCatalog().Table(lo.Database, lo.Table)
			//if err != nil {
			//	panic("no table")
			//}
			//tree.AddMetaNode("columns", printColumns(catalogTable.Columns))
		}
		node := tree.AddBranch("filters")
		listExprsToTree(node, lo.Filters)
		//printStats := func(columns []string) string {
		//	sb := strings.Builder{}
		//	sb.WriteString(fmt.Sprintf("rowcount %v\n", lo.Stats.RowCount))
		//	for colIdx, colName := range lo.Columns {
		//		originIdx := catalogTable.Column2Idx[colName]
		//		sb.WriteString(fmt.Sprintf("col %v %v ", colIdx, colName))
		//		sb.WriteString(lo.Stats.ColStats[originIdx].String())
		//		sb.WriteByte('\n')
		//	}
		//	return sb.String()
		//}
		//if len(lo.Columns) > 0 {
		//	tree.AddMetaNode("stats", printStats(lo.Columns))
		//} else {
		//	tree.AddMetaNode("stats", printStats(catalogTable.Columns))
		//}

	case LOT_JOIN:
		tree = tree.AddBranch(fmt.Sprintf("Join (%v):", lo.JoinTyp))
		printOutputs(tree, lo)
		tree.AddMetaNode("index", fmt.Sprintf("%d", lo.Index))
		if len(lo.OnConds) > 0 {
			node := tree.AddMetaBranch("On", "")
			listExprsToTree(node, lo.OnConds)
		}
		if lo.Stats != nil {
			tree.AddMetaNode("Stats", lo.Stats.String())
		}
	case LOT_AggGroup:
		tree = tree.AddBranch("Aggregate:")
		printOutputs(tree, lo)
		if len(lo.GroupBys) > 0 {
			node := tree.AddBranch(fmt.Sprintf("groupExprs, index %d", lo.Index))
			listExprsToTree(node, lo.GroupBys)
		}
		if len(lo.Aggs) > 0 {
			node := tree.AddBranch(fmt.Sprintf("aggExprs, index %d", lo.Index2))
			listExprsToTree(node, lo.Aggs)
		}
		if len(lo.Filters) > 0 {
			node := tree.AddBranch("filters")
			listExprsToTree(node, lo.Filters)
		}

	case LOT_Order:
		tree = tree.AddBranch("Order:")
		printOutputs(tree, lo)
		node := tree.AddMetaBranch("exprs", "")
		listExprsToTree(node, lo.OrderBys)
	case LOT_Limit:
		tree = tree.AddBranch(fmt.Sprintf("Limit: %v", lo.Limit.String()))
		printOutputs(tree, lo)
	case LOT_CreateSchema:
		tree = tree.AddBranch(fmt.Sprintf("CreateSchema: %v %v", lo.Database, lo.IfNotExists))
	case LOT_CreateTable:
		tree = tree.AddBranch(fmt.Sprintf("CreateTable: %v %v %v",
			lo.Database, lo.Table, lo.IfNotExists))
		node := tree.AddMetaBranch("colDefs", "")
		listColDefsToTree(node, lo.ColDefs)
		consStr := make([]string, 0)
		for _, cons := range lo.Constraints {
			consStr = append(consStr, cons.String())
		}
		tree.AddMetaNode("constraints", strings.Join(consStr, ","))
	default:
		panic(fmt.Sprintf("usp %v", lo.Typ))
	}

	for _, child := range lo.Children {
		child.Print(tree)
	}
}

func printOutputs(tree treeprint.Tree, root *LogicalOperator) {
	if len(root.Outputs) != 0 {
		node := tree.AddMetaBranch("outputs", "")
		listExprsToTree(node, root.Outputs)
	}

	if len(root.Counts) != 0 {
		node := tree.AddMetaBranch("counts", "")
		for bind, i := range root.Counts {
			node.AddNode(fmt.Sprintf("%v %v", bind, i))
		}
	}

	if len(root.ColRefToPos) != 0 {
		node := tree.AddMetaBranch("colRefToPos", "")
		binds := root.ColRefToPos.sortByColumnBind()
		for i, bind := range binds {
			node.AddNode(fmt.Sprintf("%v %v", bind, i))
		}
	}
}

func (lo *LogicalOperator) String() string {
	tree := treeprint.NewWithRoot("LogicalPlan:")
	lo.Print(tree)
	return tree.String()
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

type ET int

const (
	ET_Column     ET = iota //column
	ET_TABLE                //table
	ET_ValuesList           //for insert
	ET_Join                 //join
	ET_CTE

	ET_Func
	ET_Subquery

	ET_IConst    //integer
	ET_DecConst  //decimal
	ET_SConst    //string
	ET_FConst    //float
	ET_DateConst //date
	ET_IntervalConst
	ET_BConst // bool
	ET_NConst // null

	ET_Orderby
	ET_List
)

type ET_SubTyp int

const (
	//real function
	ET_Invalid ET_SubTyp = iota
	ET_SubFunc
	//operator
	ET_Add
	ET_Sub
	ET_Mul
	ET_Div
	ET_Equal
	ET_NotEqual
	ET_Greater
	ET_GreaterEqual
	ET_Less
	ET_LessEqual
	ET_And
	ET_Or
	ET_Not
	ET_Like
	ET_NotLike
	ET_Between
	ET_Case
	ET_CaseWhen
	ET_In
	ET_NotIn
	ET_Exists
	ET_NotExists
	ET_DateAdd
	ET_DateSub
	ET_Cast
	ET_Extract
	ET_Substring
)

func (et ET_SubTyp) String() string {
	switch et {
	case ET_Add:
		return "+"
	case ET_Sub:
		return "-"
	case ET_Mul:
		return "*"
	case ET_Div:
		return "/"
	case ET_Equal:
		return "="
	case ET_NotEqual:
		return "<>"
	case ET_Greater:
		return ">"
	case ET_GreaterEqual:
		return ">="
	case ET_Less:
		return "<"
	case ET_LessEqual:
		return "<="
	case ET_And:
		return "and"
	case ET_Or:
		return "or"
	case ET_Not:
		return "not"
	case ET_Like:
		return "like"
	case ET_NotLike:
		return "not like"
	case ET_Between:
		return "between"
	case ET_Case:
		return "case"
	case ET_In:
		return "in"
	case ET_NotIn:
		return "not in"
	case ET_Exists:
		return "exists"
	case ET_NotExists:
		return "not exists"
	case ET_DateAdd:
		return "date_add"
	case ET_DateSub:
		return "date_sub"
	case ET_Cast:
		return "cast"
	case ET_Extract:
		return "extract"
	case ET_Substring:
		return "substring"
	default:
		panic(fmt.Sprintf("usp %v", int(et)))
	}
}

func (et ET_SubTyp) isOperator() bool {
	switch et {
	case ET_Add:
		return true
	case ET_Sub:
		return true
	case ET_Mul:
		return true
	case ET_Div:
		return true
	case ET_Equal:
		return true
	case ET_NotEqual:
		return true
	case ET_Greater:
		return true
	case ET_GreaterEqual:
		return true
	case ET_Less:
		return true
	case ET_LessEqual:
		return true
	case ET_And:
		return true
	case ET_Or:
		return true
	case ET_Not:
		return true
	case ET_Like:
		return true
	case ET_NotLike:
		return true
	case ET_Between:
		return true
	case ET_Case:
		return true
	case ET_In:
		return true
	case ET_NotIn:
		return true
	case ET_Exists:
		return true
	case ET_NotExists:
		return true
	default:
		return false
	}
}

type ET_JoinType int

const (
	ET_JoinTypeCross ET_JoinType = iota
	ET_JoinTypeLeft
	ET_JoinTypeInner
)

type ET_SubqueryType int

const (
	ET_SubqueryTypeScalar ET_SubqueryType = iota
	ET_SubqueryTypeExists
	ET_SubqueryTypeNotExists
	ET_SubqueryTypeIn
	ET_SubqueryTypeNotIn
)

type Expr struct {
	Typ     ET
	SubTyp  ET_SubTyp
	DataTyp common.LType
	AggrTyp AggrType

	Children []*Expr

	Index       uint64
	Database    string
	Table       string     // table
	Name        string     // column
	ColRef      ColumnBind // relationTag, columnPos
	Depth       int        // > 0, correlated column
	Svalue      string
	Ivalue      int64
	Fvalue      float64
	Bvalue      bool
	Desc        bool        // in orderby
	JoinTyp     ET_JoinType // join
	Alias       string
	SubBuilder  *Builder     // builder for subquery
	SubCtx      *BindContext // context for subquery
	SubqueryTyp ET_SubqueryType
	CTEIndex    uint64

	BelongCtx   *BindContext // context for table and join
	On          *Expr        //JoinOn
	IsOperator  bool
	BindInfo    *FunctionData
	FunImpl     *FunctionV2
	Constraints []*storage.Constraint //for column def of create table
	//for insert ... values
	Types       []common.LType
	Names       []string
	Values      [][]*Expr
	ColName2Idx map[string]int
	TabEnt      *storage.CatalogEntry
}

func (e *Expr) equal(o *Expr) bool {
	if e == nil && o == nil {
		return true
	} else if e != nil && o != nil {
		if e.Typ != o.Typ {
			return false
		}
		if e.SubTyp != o.SubTyp {
			return false
		}
		if e.DataTyp != o.DataTyp {
			return false
		}
		if e.AggrTyp != o.AggrTyp {
			return false
		}
		if e.Index != o.Index {
			return false
		}
		if e.Database != o.Database {
			return false
		}
		if e.Table != o.Table {
			return false
		}
		if e.Name != o.Name {
			return false
		}
		if e.ColRef != o.ColRef {
			return false
		}
		if e.Depth != o.Depth {
			return false
		}
		if e.Svalue != o.Svalue {
			return false
		}
		if e.Ivalue != o.Ivalue {
			return false
		}
		if e.Fvalue != o.Fvalue {
			return false
		}
		if e.Bvalue != o.Bvalue {
			return false
		}
		if e.Desc != o.Desc {
			return false
		}
		if e.JoinTyp != o.JoinTyp {
			return false
		}
		if e.Alias != o.Alias {
			return false
		}
		if e.SubqueryTyp != o.SubqueryTyp {
			return false
		}
		if e.CTEIndex != o.CTEIndex {
			return false
		}
		if e.IsOperator != o.IsOperator {
			return false
		}
		if !e.On.equal(o.On) {
			return false
		}
		//children
		if len(e.Children) != len(o.Children) {
			return false
		}
		for i, child := range e.Children {
			if !child.equal(o.Children[i]) {
				return false
			}
		}
		return true
	} else {
		return false
	}
}

func (e *Expr) copy() *Expr {
	if e == nil {
		return nil
	}

	if e.Typ == ET_Func && e.FunImpl == nil {
		panic("invalid fun in copy")
	}

	ret := &Expr{
		Typ:         e.Typ,
		SubTyp:      e.SubTyp,
		DataTyp:     e.DataTyp,
		AggrTyp:     e.AggrTyp,
		Index:       e.Index,
		Database:    e.Database,
		Table:       e.Table,
		Name:        e.Name,
		ColRef:      e.ColRef,
		Depth:       e.Depth,
		Svalue:      e.Svalue,
		Ivalue:      e.Ivalue,
		Fvalue:      e.Fvalue,
		Bvalue:      e.Bvalue,
		Desc:        e.Desc,
		JoinTyp:     e.JoinTyp,
		Alias:       e.Alias,
		SubBuilder:  e.SubBuilder,
		SubCtx:      e.SubCtx,
		SubqueryTyp: e.SubqueryTyp,
		CTEIndex:    e.CTEIndex,
		BelongCtx:   e.BelongCtx,
		On:          e.On.copy(),
		IsOperator:  e.IsOperator,
		BindInfo:    e.BindInfo,
		FunImpl:     e.FunImpl,
	}
	for _, child := range e.Children {
		ret.Children = append(ret.Children, child.copy())
	}
	return ret
}

func copyExprs(exprs ...*Expr) []*Expr {
	ret := make([]*Expr, 0)
	for _, expr := range exprs {
		ret = append(ret, expr.copy())
	}
	return ret
}

func restoreExpr(e *Expr, index uint64, realExprs []*Expr) *Expr {
	if e == nil {
		return nil
	}
	switch e.Typ {
	case ET_Column:
		if index == e.ColRef[0] {
			e = realExprs[e.ColRef[1]]
		}
	case ET_SConst, ET_IConst, ET_DateConst, ET_IntervalConst, ET_BConst, ET_FConst, ET_NConst, ET_DecConst:
	case ET_Func:
	default:
		panic("usp")
	}
	for i, child := range e.Children {
		e.Children[i] = restoreExpr(child, index, realExprs)
	}
	return e
}

func referTo(e *Expr, index uint64) bool {
	if e == nil {
		return false
	}
	switch e.Typ {
	case ET_Column:
		return index == e.ColRef[0]
	case ET_SConst, ET_IConst, ET_DateConst, ET_FConst, ET_DecConst, ET_NConst, ET_BConst:

	case ET_Func:
	default:
		panic("usp")
	}
	for _, child := range e.Children {
		if referTo(child, index) {
			return true
		}
	}
	return false
}

func onlyReferTo(e *Expr, index uint64) bool {
	if e == nil {
		return false
	}
	switch e.Typ {
	case ET_Column:
		return index == e.ColRef[0]

	case ET_SConst, ET_IConst, ET_DateConst, ET_IntervalConst, ET_BConst, ET_FConst, ET_NConst, ET_DecConst:
		return true
	case ET_Func:
	default:
		panic("usp")
	}
	for _, child := range e.Children {
		if !onlyReferTo(child, index) {
			return false
		}
	}
	return true
}

func decideSide(e *Expr, leftTags, rightTags map[uint64]bool) int {
	var ret int
	switch e.Typ {
	case ET_Column:
		if _, has := leftTags[e.ColRef[0]]; has {
			ret |= LeftSide
		}
		if _, has := rightTags[e.ColRef[0]]; has {
			ret |= RightSide
		}
	case ET_SConst, ET_DateConst, ET_IConst, ET_IntervalConst, ET_BConst, ET_FConst, ET_NConst, ET_DecConst:
	case ET_Func:
	default:
		panic("usp")
	}
	for _, child := range e.Children {
		ret |= decideSide(child, leftTags, rightTags)
	}
	return ret
}

func copyExpr(e *Expr) *Expr {
	return clone.Clone(e).(*Expr)
}

func (e *Expr) Format(ctx *FormatCtx) {
	if e == nil {
		ctx.Write("")
		return
	}
	switch e.Typ {
	case ET_Column:
		//TODO:
		ctx.Writef("(%s.%s,%s,%v,%d)", e.Table, e.Name,
			e.DataTyp,
			e.ColRef, e.Depth)
	case ET_SConst:
		ctx.Writef("(%s,%s)", e.Svalue, e.DataTyp)
	case ET_IConst:
		ctx.Writef("(%d,%s)", e.Ivalue, e.DataTyp)
	case ET_DateConst:
		ctx.Writef("(%s,%s)", e.Svalue, e.DataTyp)
	case ET_IntervalConst:
		ctx.Writef("(%d %s,%s)", e.Ivalue, e.Svalue, e.DataTyp)
	case ET_BConst:
		ctx.Writef("(%v,%s)", e.Bvalue, e.DataTyp)
	case ET_FConst:
		ctx.Writef("(%v,%s)", e.Fvalue, e.DataTyp)
	case ET_DecConst:
		ctx.Writef("(%v,%s)", e.Svalue, e.DataTyp)
	case ET_TABLE:
		ctx.Writef("%s.%s", e.Database, e.Table)
	case ET_Join:
		e.Children[0].Format(ctx)
		typStr := ""
		switch e.JoinTyp {
		case ET_JoinTypeCross:
			typStr = "cross"
		case ET_JoinTypeLeft:
			typStr = "left"
		default:
			panic(fmt.Sprintf("usp join type %d", e.JoinTyp))
		}
		ctx.Writef(" %s ", typStr)
		e.Children[1].Format(ctx)

	case ET_Func:
		switch e.SubTyp {
		case ET_Invalid:
			panic("usp invalid expr")
		case ET_Between:
			e.Children[0].Format(ctx)
			ctx.Write(" between ")
			e.Children[1].Format(ctx)
			ctx.Write(" and ")
			e.Children[2].Format(ctx)
		case ET_Case:
			ctx.Write("case ")
			if e.Children[0] != nil {
				e.Children[0].Format(ctx)
				ctx.Writeln()
			}
			for i := 2; i < len(e.Children); i += 2 {
				ctx.Write(" when")
				e.Children[i].Format(ctx)
				ctx.Write(" then ")
				e.Children[i+1].Format(ctx)
				ctx.Writeln()
			}
			if e.Children[1] != nil {
				ctx.Write(" else ")
				e.Children[1].Format(ctx)
				ctx.Writeln()
			}
			ctx.Write("end")
		case ET_In, ET_NotIn:
			e.Children[0].Format(ctx)
			if e.SubTyp == ET_NotIn {
				ctx.Write(" not in ")
			} else {
				ctx.Write(" in ")
			}

			ctx.Write("(")
			for i := 1; i < len(e.Children); i++ {
				if i > 1 {
					ctx.Write(",")
				}
				e.Children[i].Format(ctx)
			}
			ctx.Write(")")
		case ET_Exists:
			ctx.Writef("exists(")
			e.Children[0].Format(ctx)
			ctx.Write(")")

		case ET_SubFunc:
			ctx.Writef("%s(", e.Svalue)
			for idx, child := range e.Children {
				if idx > 0 {
					ctx.Write(", ")
				}
				child.Format(ctx)
			}
			ctx.Write(")")
			ctx.Write("->")
			ctx.Writef("%s", e.DataTyp)
		default:
			//binary operator
			e.Children[0].Format(ctx)
			op := e.SubTyp.String()
			ctx.Writef(" %s ", op)
			e.Children[1].Format(ctx)
		}
	case ET_Subquery:
		ctx.Write("subquery(")
		ctx.AddOffset()
		// e.SubBuilder.Format(ctx)
		ctx.writeString(e.SubBuilder.String())
		ctx.RestoreOffset()
		ctx.Write(")")
	case ET_Orderby:
		e.Children[0].Format(ctx)
		if e.Desc {
			ctx.Write(" desc")
		}

	default:
		panic(fmt.Sprintf("usp expr type %d", e.Typ))
	}
}

func appendMeta(meta, s string) string {
	return fmt.Sprintf("%s %s", meta, s)
}

func (e *Expr) Print(tree treeprint.Tree, meta string) {
	if e == nil {
		return
	}
	head := appendMeta(meta, e.DataTyp.String())
	switch e.Typ {
	case ET_Column:
		if e.Depth != 0 {
			tree.AddMetaNode(head, fmt.Sprintf("(%s.%s,%v,%d)",
				e.Table, e.Name,
				e.ColRef, e.Depth))
		} else {
			tree.AddMetaNode(head, fmt.Sprintf("(%s.%s,%v)",
				e.Table, e.Name,
				e.ColRef))
		}

	case ET_SConst:
		tree.AddMetaNode(head, fmt.Sprintf("(%s)", e.Svalue))
	case ET_IConst:
		tree.AddMetaNode(head, fmt.Sprintf("(%d)", e.Ivalue))
	case ET_DateConst:
		tree.AddMetaNode(head, fmt.Sprintf("(%s)", e.Svalue))
	case ET_IntervalConst:
		tree.AddMetaNode(head, fmt.Sprintf("(%d %s)", e.Ivalue, e.Svalue))
	case ET_BConst:
		tree.AddMetaNode(head, fmt.Sprintf("(%v)", e.Bvalue))
	case ET_FConst:
		tree.AddMetaNode(head, fmt.Sprintf("(%v)", e.Fvalue))
	case ET_DecConst:
		tree.AddMetaNode(head, fmt.Sprintf("(%s %d %d)", e.Svalue, e.DataTyp.Width, e.DataTyp.Scale))
	case ET_TABLE:
		tree.AddNode(fmt.Sprintf("%s.%s", e.Database, e.Table))
	case ET_Join:
		typStr := ""
		switch e.JoinTyp {
		case ET_JoinTypeCross:
			typStr = "cross"
		case ET_JoinTypeLeft:
			typStr = "left"
		default:
			panic(fmt.Sprintf("usp join type %d", e.JoinTyp))
		}
		branch := tree.AddBranch(typStr)
		e.Children[0].Print(branch, "")
		e.Children[1].Print(branch, "")
	case ET_Func:
		var branch treeprint.Tree
		switch e.SubTyp {
		case ET_Invalid:
			panic("usp invalid expr")
		case ET_Between:
			branch = tree.AddMetaBranch(head, e.SubTyp.String())
			e.Children[0].Print(branch, "")
			e.Children[1].Print(branch, "")
			e.Children[2].Print(branch, "")
		case ET_Case:
			branch = tree.AddMetaBranch(head, e.SubTyp.String())
			if e.Children[0] != nil {
				e.Children[0].Print(branch, "")
			}
			when := branch.AddBranch("when")
			for i := 1; i < len(e.Children); i += 2 {
				e.Children[i].Print(when, "")
				e.Children[i+1].Print(when, "")
			}
			if e.Children[0] != nil {
				e.Children[0].Print(branch, "")
			}
		case ET_In, ET_NotIn:
			branch = tree.AddMetaBranch(head, e.SubTyp)
			for _, child := range e.Children {
				child.Print(branch, "")
			}
		case ET_Exists:
			branch = tree.AddMetaBranch(head, e.SubTyp)
			e.Children[0].Print(branch, "")
		case ET_SubFunc:
			dist := ""
			if e.AggrTyp == DISTINCT {
				dist = "(distinct)"
			}
			branch = tree.AddMetaBranch(head, fmt.Sprintf("%s %s", e.Svalue, dist))
			for _, child := range e.Children {
				child.Print(branch, "")
			}
		default:
			//binary operator
			branch = tree.AddMetaBranch(head, e.SubTyp)
			e.Children[0].Print(branch, "")
			e.Children[1].Print(branch, "")
		}
	case ET_Subquery:
		branch := tree.AddBranch("subquery(")
		e.SubBuilder.Print(branch)
		branch.AddNode(")")
	case ET_Orderby:
		e.Children[0].Print(tree, meta)

	default:
		panic(fmt.Sprintf("usp expr type %d", e.Typ))
	}
}

func (e *Expr) String() string {
	ctx := &FormatCtx{}
	e.Format(ctx)
	return ctx.String()
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
				//catalogTable, err := tpchCatalog().Table(po.Database, po.Table)
				//if err != nil {
				//	panic("no table")
				//}
				//tree.AddMetaNode("columns", printColumns(catalogTable.Columns))
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

func (catalog *CatalogTable) getStats(colId uint64) *BaseStats {
	return catalog.Stats.ColStats[colId]
}

func splitExprByAnd(expr *Expr) []*Expr {
	if expr.Typ == ET_Func {
		if expr.SubTyp == ET_And {
			return append(splitExprByAnd(expr.Children[0]), splitExprByAnd(expr.Children[1])...)
		}
		//else if expr.SubTyp == ET_In || expr.SubTyp == ET_NotIn {
		//	//ret := make([]*Expr, 0)
		//	//for i, child := range expr.Children {
		//	//	if i == 0 {
		//	//		continue
		//	//	}
		//	//	ret = append(ret, &Expr{
		//	//		Typ:      expr.Typ,
		//	//		SubTyp:   expr.SubTyp,
		//	//		Svalue:   expr.SubTyp.String(),
		//	//		FuncId:   expr.FuncId,
		//	//		Children: []*Expr{expr.Children[0], child},
		//	//	})
		//	//}
		//}
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
