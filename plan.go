package main

import (
	"fmt"
	"strings"

	"github.com/huandu/go-clone"
	"github.com/xlab/treeprint"
)

type DataType int

const (
	DataTypeInteger = iota
	DataTypeVarchar
	DataTypeDecimal
	DataTypeDate
	DataTypeBool
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

type ExprDataType struct {
	Typ     DataType
	NotNull bool
	Width   uint64
	Scale   uint64
}

func (edt ExprDataType) String() string {
	null := "null"
	if edt.NotNull {
		null = "not null"
	}
	return fmt.Sprintf("<%s,%s,%d,%d>", edt.Typ, null, edt.Width, edt.Scale)
}

var InvalidExprDataType = ExprDataType{
	Typ: DataTypeInvalid,
}

type LOT int

const (
	LOT_Project = iota
	LOT_Filter
	LOT_Scan
	LOT_JOIN
	LOT_AggGroup
	LOT_Order
	LOT_Limit
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
	default:
		panic(fmt.Sprintf("usp %d", lojt))
	}
}

type LogicalOperator struct {
	Typ LOT

	Projects         []*Expr
	Index            uint64 //AggNode for groupTag. others in other Nodes
	Index2           uint64 //AggNode for aggTag
	Database         string
	Table            string       // table
	Columns          []string     //needed column name for SCAN
	Filters          []*Expr      //for FILTER or AGG
	BelongCtx        *BindContext //for table or join
	JoinTyp          LOT_JoinType
	OnConds          []*Expr //for innor join
	Aggs             []*Expr
	GroupBys         []*Expr
	OrderBys         []*Expr
	Limit            *Expr
	Stats            *Stats
	hasEstimatedCard bool
	estimatedCard    uint64
	estimatedProps   *EstimatedProperties

	Children []*LogicalOperator
}

func (lo *LogicalOperator) EstimatedCard() uint64 {
	if lo.Typ == LOT_Scan {
		catalogTable, err := tpchCatalog().Table(lo.Database, lo.Table)
		if err != nil {
			panic(err)
		}
		return uint64(catalogTable.Stats.RowCount)
	}
	if lo.hasEstimatedCard {
		return lo.estimatedCard
	}
	maxCard := uint64(0)
	for _, child := range lo.Children {
		childCard := child.EstimatedCard()
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

	bb := strings.Builder{}

	switch lo.Typ {
	case LOT_Project:
		tree = tree.AddBranch("Project:")
		tree.AddMetaNode("index", fmt.Sprintf("%d", lo.Index))
		tree.AddMetaNode("exprs", listExprs(&bb, lo.Projects).String())
	case LOT_Filter:
		tree = tree.AddBranch("Filter:")
		tree.AddMetaNode("exprs", listExprs(&bb, lo.Filters).String())
	case LOT_Scan:
		tree = tree.AddBranch("Scan:")
		tree.AddMetaNode("index", fmt.Sprintf("%d", lo.Index))
		tree.AddMetaNode("table", fmt.Sprintf("%v.%v", lo.Database, lo.Table))
		catalogTable, err := tpchCatalog().Table(lo.Database, lo.Table)
		if err != nil {
			panic("no table")
		}
		printColumns := func(cols []string) string {
			t := strings.Builder{}
			t.WriteByte('\n')
			for i, col := range cols {
				t.WriteString(fmt.Sprintf("col %d %v", i, col))
				t.WriteByte('\n')
			}
			return t.String()
		}
		if len(lo.Columns) > 0 {
			tree.AddMetaNode("columns", printColumns(lo.Columns))
		} else {
			tree.AddMetaNode("columns", printColumns(catalogTable.Columns))
		}
		tree.AddMetaNode("filters", listExprs(&bb, lo.Filters).String())
		printStats := func(columns []string) string {
			sb := strings.Builder{}
			sb.WriteString(fmt.Sprintf("rowcount %v\n", lo.Stats.RowCount))
			for colIdx, colName := range lo.Columns {
				originIdx := catalogTable.Column2Idx[colName]
				sb.WriteString(fmt.Sprintf("col %v %v ", colIdx, colName))
				sb.WriteString(lo.Stats.ColStats[originIdx].String())
				sb.WriteByte('\n')
			}
			return sb.String()
		}
		if len(lo.Columns) > 0 {
			tree.AddMetaNode("stats", printStats(lo.Columns))
		} else {
			tree.AddMetaNode("stats", printStats(catalogTable.Columns))
		}

	case LOT_JOIN:
		tree = tree.AddBranch(fmt.Sprintf("Join (%v):", lo.JoinTyp))
		if len(lo.OnConds) > 0 {
			tree.AddMetaNode("On", listExprs(&bb, lo.OnConds).String())
		}
		if lo.Stats != nil {
			tree.AddMetaNode("Stats", lo.Stats.String())
		}
	case LOT_AggGroup:
		tree = tree.AddBranch("Aggregate:")
		if len(lo.GroupBys) > 0 {
			bb.Reset()
			tree.AddMetaNode(fmt.Sprintf("groupExprs, index %d", lo.Index), listExprs(&bb, lo.GroupBys).String())
		}
		if len(lo.Aggs) > 0 {
			bb.Reset()
			tree.AddMetaNode(fmt.Sprintf("aggExprs, index %d", lo.Index2), listExprs(&bb, lo.Aggs).String())
		}
		if len(lo.Filters) > 0 {
			bb.Reset()
			tree.AddMetaNode("filters", listExprs(&bb, lo.Filters).String())
		}

	case LOT_Order:
		tree = tree.AddBranch("Order:")
		tree.AddMetaNode("exprs", listExprs(&bb, lo.OrderBys).String())
	case LOT_Limit:
		tree = tree.AddBranch(fmt.Sprintf("Limit: %v", lo.Limit.String()))
	default:
		panic(fmt.Sprintf("usp %v", lo.Typ))
	}

	for _, child := range lo.Children {
		child.Print(tree)
	}
}

func (lo *LogicalOperator) String() string {
	tree := treeprint.NewWithRoot("LogicalOperator:")
	lo.Print(tree)
	return tree.String()
}

type POT int

const (
	POT_Project = iota
	POT_With
	POT_Filter
	POT_Agg
	POT_Join
	POT_Order
	POT_Limit
	POT_Scan
)

type ET int

const (
	ET_Add = iota
	ET_Sub
	ET_Mul
	ET_Div
	ET_Equal
	ET_And
	ET_Or
	ET_Not
	ET_Like

	ET_Column //column
	ET_TABLE  //table
	ET_Join   //join

	ET_Func
	ET_Subquery

	ET_IConst //integer
	ET_SConst //string
	ET_FConst //float

	ET_Orderby
)

type ET_JoinType int

const (
	ET_JoinTypeCross = iota
	ET_JoinTypeLeft
	ET_JoinTypeInner
)

type Expr struct {
	Typ     ET
	DataTyp ExprDataType

	Index      uint64
	Database   string
	Table      string    // table
	Name       string    // column
	ColRef     [2]uint64 // relationTag, columnPos
	Depth      int       // > 0, correlated column
	Svalue     string
	Ivalue     int64
	Fvalue     float64
	Desc       bool        // in orderby
	JoinTyp    ET_JoinType // join
	Alias      string
	SubBuilder *Builder     // builder for subquery
	SubCtx     *BindContext // context for subquery
	FuncId     FuncId

	Children  []*Expr
	BelongCtx *BindContext // context for table and join
	On        *Expr        //JoinOn
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
	case ET_And, ET_Equal, ET_Like:
		for i, child := range e.Children {
			e.Children[i] = restoreExpr(child, index, realExprs)
		}
	case ET_Func:
		for i, child := range e.Children {
			e.Children[i] = restoreExpr(child, index, realExprs)
		}
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
	case ET_And, ET_Equal, ET_Like:
		for _, child := range e.Children {
			if referTo(child, index) {
				return true
			}
		}
	case ET_Func:
		for _, child := range e.Children {
			if referTo(child, index) {
				return true
			}
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
	case ET_And, ET_Equal, ET_Like:
		for _, child := range e.Children {
			if !onlyReferTo(child, index) {
				return false
			}
		}
	case ET_Func:
		for _, child := range e.Children {
			if !onlyReferTo(child, index) {
				return false
			}
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
	case ET_And, ET_Equal, ET_Like:
		for _, child := range e.Children {
			ret |= decideSide(child, leftTags, rightTags)
		}
	case ET_Func:
		for _, child := range e.Children {
			ret |= decideSide(child, leftTags, rightTags)
		}
	}
	return ret
}

func getTableNameFromExprs(exprs map[*Expr]bool) (string, string) {
	if len(exprs) == 0 {
		panic("must have exprs")
	}
	for e := range exprs {
		if e.Typ != ET_Column {
			panic("must be column ref")
		}
		return "", e.Table
	}
	return "", ""
}

/*
dir

	0 left
	1 right
*/
func collectRelation(e *Expr, dir int) (map[uint64]map[*Expr]bool, map[uint64]map[*Expr]bool) {
	left := make(map[uint64]map[*Expr]bool)
	right := make(map[uint64]map[*Expr]bool)
	switch e.Typ {
	case ET_Column:
		if dir <= 0 {
			set := left[e.ColRef[0]]
			if set == nil {
				set = make(map[*Expr]bool)
			}
			set[e] = true
			left[e.ColRef[0]] = set
		} else {
			set := right[e.ColRef[0]]
			if set == nil {
				set = make(map[*Expr]bool)
			}
			set[e] = true
			right[e.ColRef[0]] = set
		}
	case ET_And, ET_Equal, ET_Like:
		for i, child := range e.Children {
			newDir := 0
			if i > 0 {
				newDir = 1
			}
			retl, retr := collectRelation(child, newDir)
			if i == 0 {
				mergeMap(left, retl)
			} else {
				mergeMap(right, retr)
			}
		}
	case ET_Func:
		for _, child := range e.Children {
			retl, retr := collectRelation(child, dir)
			if dir <= 0 {
				mergeMap(left, retl)
			} else {
				mergeMap(right, retr)
			}
		}
	}
	return left, right
}

func mergeMap(a, b map[uint64]map[*Expr]bool) {
	for k, v := range b {
		set := a[k]
		if set == nil {
			a[k] = v
		} else {
			mergeSet(set, v)
		}
	}
}

func mergeSet(a, b map[*Expr]bool) {
	for k, v := range b {
		a[k] = v
	}
}

func printRelations(info string, maps map[uint64]map[*Expr]bool) {
	fmt.Println(info)
	for k, v := range maps {
		fmt.Printf("\trelation %d\n", k)
		for e := range v {
			fmt.Printf("\t\t%v\n", e)
		}
	}
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
		ctx.Writef("(%s.%s,%s,[%d,%d],%d)", e.Table, e.Name,
			e.DataTyp,
			e.ColRef[0], e.ColRef[1], e.Depth)
	case ET_SConst:
		ctx.Write(e.Svalue)
	case ET_IConst:
		ctx.Writef("%d", e.Ivalue)
	case ET_And, ET_Equal, ET_Like:
		e.Children[0].Format(ctx)
		op := ""
		switch e.Typ {
		case ET_And:
			op = "and"
		case ET_Equal:
			op = "="
		case ET_Like:
			op = "like"
		default:
			panic(fmt.Sprintf("usp binary expr type %d", e.Typ))
		}
		ctx.Writef(" %s ", op)
		e.Children[1].Format(ctx)
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
		ctx.Writef("%s_%d(", e.Svalue, e.FuncId)
		for idx, e := range e.Children {
			if idx > 0 {
				ctx.Write(",")
			}
			e.Format(ctx)
		}
		ctx.Write(")")
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

func (e *Expr) Print(tree treeprint.Tree) {
	if e == nil {
		return
	}
	switch e.Typ {
	case ET_Column:
		tree.AddNode(fmt.Sprintf("(%s.%s,%s,[%d,%d],%d)", e.Table, e.Name,
			e.DataTyp,
			e.ColRef[0], e.ColRef[1], e.Depth))
	case ET_SConst:
		tree.AddNode(e.Svalue)
	case ET_IConst:
		tree.AddNode(fmt.Sprintf("%d", e.Ivalue))
	case ET_And, ET_Equal, ET_Like:
		op := ""
		switch e.Typ {
		case ET_And:
			op = "and"
		case ET_Equal:
			op = "="
		case ET_Like:
			op = "like"
		default:
			panic(fmt.Sprintf("usp binary expr type %d", e.Typ))
		}
		sub := tree.AddBranch(op)
		e.Children[0].Print(sub)
		e.Children[1].Print(sub)
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
		sub := tree.AddBranch(typStr)
		e.Children[0].Print(sub)
		e.Children[1].Print(sub)
	case ET_Func:
		sub := tree.AddBranch(fmt.Sprintf("%s_%d(", e.Svalue, e.FuncId))
		for i, e := range e.Children {
			p := sub.AddMetaNode("param", fmt.Sprintf("%d", i))
			e.Print(p)
		}
		sub.AddNode(")")
	case ET_Subquery:
		sub := tree.AddBranch("subquery(")
		e.SubBuilder.Print(sub)
		sub.AddNode(")")
	case ET_Orderby:
		e.Children[0].Print(tree)
		if e.Desc {
			tree.AddNode(" desc")
		}
	default:
		panic(fmt.Sprintf("usp expr type %d", e.Typ))
	}
}

func (e *Expr) String() string {
	ctx := &FormatCtx{}
	e.Format(ctx)
	return ctx.String()
}

type TableDef struct {
	Db    string
	table string
	Alias string
}

type PhysicalOperator struct {
	Typ POT
	Tag int //relationTag

	Columns  []string // name of project
	Project  []*Expr
	Filter   []*Expr
	Agg      []*Expr
	JoinOn   []*Expr
	Order    []*Expr
	Limit    []*Expr
	Table    *TableDef
	Children []*PhysicalOperator
}

type Catalog struct {
	tpch map[string]*CatalogTable
}

func (c *Catalog) Table(db, table string) (*CatalogTable, error) {
	if db == "tpch" {
		if c, ok := c.tpch[table]; ok {
			return c, nil
		} else {
			fmt.Errorf("table %s in database %s does not exist", table, db)
		}
	} else {
		panic(fmt.Sprintf("database %s does not exist", db))
	}
	return nil, nil
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

type CatalogTable struct {
	Db         string
	Table      string
	Columns    []string
	Types      []ExprDataType
	PK         []int
	Column2Idx map[string]int
	Stats      *Stats
}

func (catalog *CatalogTable) getStats(colId uint64) *BaseStats {
	return catalog.Stats.ColStats[colId]
}

func splitExprByAnd(expr *Expr) []*Expr {
	if expr.Typ == ET_And {
		return append(splitExprByAnd(expr.Children[0]), splitExprByAnd(expr.Children[1])...)
	}
	return []*Expr{expr}
}

func splitExprsByAnd(exprs []*Expr) []*Expr {
	ret := make([]*Expr, 0)
	for _, e := range exprs {
		ret = append(ret, splitExprByAnd(e)...)
	}
	return ret
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
	case ET_And, ET_Equal, ET_Like:
		left, leftHasCorr := deceaseDepth(expr.Children[0])
		hasCorCol = hasCorCol || leftHasCorr
		right, rightHasCorr := deceaseDepth(expr.Children[1])
		hasCorCol = hasCorCol || rightHasCorr
		return &Expr{
			Typ:      expr.Typ,
			DataTyp:  expr.DataTyp,
			Children: []*Expr{left, right},
		}, hasCorCol
	case ET_Func:
		args := make([]*Expr, 0, len(expr.Children))
		for _, child := range expr.Children {
			newChild, yes := deceaseDepth(child)
			hasCorCol = hasCorCol || yes
			args = append(args, newChild)
		}
		return &Expr{
			Typ:      expr.Typ,
			Svalue:   expr.Svalue,
			FuncId:   expr.FuncId,
			DataTyp:  expr.DataTyp,
			Children: args,
		}, hasCorCol
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
	case ET_And, ET_Equal, ET_Like:
		for i, child := range e.Children {
			e.Children[i] = replaceColRef(child, bind, newBind)
		}
	case ET_Func:
		for i, child := range e.Children {
			e.Children[i] = replaceColRef(child, bind, newBind)
		}
	}
	return e
}

func collectColRefs(e *Expr, set ColumnBindSet) {
	if e == nil {
		return
	}
	switch e.Typ {
	case ET_Column:
		set.insert(e.ColRef)
	case ET_And, ET_Equal, ET_Like:
		for _, child := range e.Children {
			collectColRefs(child, set)
		}
	case ET_Func:
		for _, child := range e.Children {
			collectColRefs(child, set)
		}
	}
}
