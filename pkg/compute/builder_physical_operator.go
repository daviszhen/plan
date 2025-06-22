package compute

import (
	"fmt"

	"github.com/daviszhen/plan/pkg/common"
	"github.com/daviszhen/plan/pkg/storage"
)

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
	ret, err := ExplainPhysicalPlan(po)
	if err != nil {
		panic(err)
	}
	return ret
}
