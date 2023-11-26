package main

import "fmt"

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

	Projects  []*Expr
	Database  string
	Table     string // table
	Filters   []*Expr
	BelongCtx *BindContext //for table or join
	JoinTyp   LOT_JoinType
	OnConds   []*Expr //for innor join
	Aggs      []*Expr
	GroupBys  []*Expr
	OrderBys  []*Expr
	Limit     *Expr


	Children []*LogicalOperator
}

func (lo *LogicalOperator) Format(ctx *FormatCtx) {
	if lo == nil {
		ctx.Write("")
		return
	}

	switch lo.Typ {
	case LOT_Project:
		ctx.Write("Project: ")
		for i, project := range lo.Projects {
			if i > 0 {
				ctx.Write(",")
			}
			project.Format(ctx)
		}
		ctx.Writeln()

	case LOT_Filter:
		ctx.Write("Filter: ")
		for i, filter := range lo.Filters {
			if i > 0 {
				ctx.Write(",")
			}
			filter.Format(ctx)
		}
		ctx.Writeln()
	case LOT_Scan:
		ctx.Writefln("Scan: %v %v", lo.Database, lo.Table)
	case LOT_JOIN:
		ctx.Writefln("Join (%v): ", lo.JoinTyp)
		if len(lo.OnConds) > 0 {
			for i, on := range lo.OnConds {
				if i > 0 {
					ctx.Write(",")
				}
				on.Format(ctx)
			}
			ctx.Writeln()
		}
	case LOT_AggGroup:
		ctx.Write("Aggregate: ")
		if len(lo.GroupBys) > 0 {
			ctx.AddOffset()
			ctx.Write("GroupBy: ")
			for i, by := range lo.GroupBys {
				if i > 0 {
					ctx.Write(",")
				}
				by.Format(ctx)
			}
			ctx.Writeln()
			ctx.RestoreOffset()
		}
		if len(lo.Aggs) > 0 {
			ctx.AddOffset()
			ctx.Write("Agg: ")
			for i, agg := range lo.Aggs {
				if i > 0 {
					ctx.Write(",")
				}
				agg.Format(ctx)
			}
			ctx.Writeln()
			ctx.RestoreOffset()
		}
	case LOT_Order:
		ctx.Write("Order: ")
		for i, by := range lo.OrderBys {
			if i > 0 {
				ctx.Write(",")
			}
			by.Format(ctx)
		}
		ctx.Writeln()
	case LOT_Limit:
		ctx.Writefln("Limit: %v", lo.Limit.String())
	default:
		panic(fmt.Sprintf("usp %v", lo.Typ))
	}

	ctx.AddOffset()
	for _, child := range lo.Children {
		child.Format(ctx)
	}
	ctx.RestoreOffset()
}

func (lo *LogicalOperator) String() string {
	ctx := &FormatCtx{}
	lo.Format(ctx)
	return ctx.String()
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
)

type Expr struct {
	Typ     ET
	DataTyp ExprDataType

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
		e.SubBuilder.Format(ctx)
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

type CatalogTable struct {
	Db         string
	Table      string
	Columns    []string
	Types      []ExprDataType
	PK         []int
	Column2Idx map[string]int
}

func splitExprByAnd(expr *Expr) []*Expr {
	if expr.Typ == ET_And {
		return append(splitExprByAnd(expr.Children[0]), splitExprByAnd(expr.Children[1])...)
	}
	return []*Expr{expr}
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