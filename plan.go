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

type LOT_JoinType int

const (
	LOT_JoinTypeCross LOT_JoinType = iota
	LOT_JoinTypeLeft
)

type LogicalOperator struct {
	Typ LOT

	Database  string
	Table     string // table
	Filter    *Expr
	BelongCtx *BindContext //for table or join
	JoinTyp   LOT_JoinType

	Children []*LogicalOperator
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
