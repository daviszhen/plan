// Copyright 2023-2024 daviszhen
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
//distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"fmt"
	"strings"

	"github.com/daviszhen/plan/pkg/common"
	"github.com/daviszhen/plan/pkg/storage"
	"github.com/huandu/go-clone"
	"github.com/xlab/treeprint"
)

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
	ColumnIndexMap   []int //for insert
	ScanInfo         *ScanInfo
	Counts           ColumnBindCountMap `json:"-"`
	ColRefToPos      ColumnBindPosMap   `json:"-"`
}

func (lo *LogicalOperator) EstimatedCard(txn *storage.Txn) uint64 {
	if lo.Typ == LOT_Scan {
		return lo.TableEnt.GetStats2(0).Count()
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
		if len(lo.Columns) > 0 {
			tree.AddMetaNode("columns", printColumns(lo.ColName2Idx, lo.Types, lo.Columns))
		} else {
			panic("usp")
		}
		node := tree.AddBranch("filters")
		listExprsToTree(node, lo.Filters)

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
