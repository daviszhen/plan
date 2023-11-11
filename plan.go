package main

type LOT int

const (
    LOT_Project = iota
    LOT_Filter
    LOT_Scan
    LOT_AggGroup
    LOT_Order
    LOT_Limit
)

type LogicalOperator struct {
    Typ LOT
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

    ET_Column
    ET_Join

    ET_Func
    ET_Subquery

    ET_IConst //integer
    ET_SConst //string
    ET_FConst //float
)

type DataType int

const (
    DataTypeInteger = iota
    DataTypeVarchar = iota
    DataTypeDecimal = iota
    DataTypeDate    = iota
)

type ExprDataType struct {
    Typ     DataType
    NotNull bool
    Width   uint64
    Scale   uint64
}

type ET_JoinType int

const (
    ET_JoinTypeCross = iota
    ET_JoinTypeLeft
)

type Expr struct {
    Typ     ET
    DataTyp DataType

    Name    string // column
    ColRef  [2]int // relationTag, columnPos
    Depth   int    // > 0, correlated column
    Svalue  string
    Ivalue  int64
    Fvalue  float64
    Desc    bool        // in orderby
    JoinTyp ET_JoinType // join
    Alias   string

    Children []*Expr
    On       *Expr //JoinOn
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

type CatalogTable struct {
    Db         string
    Table      string
    Columns    []string
    Types      []*ExprDataType
    PK         []int
    Column2Idx map[string]int
}
