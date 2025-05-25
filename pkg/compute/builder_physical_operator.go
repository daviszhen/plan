package compute

import (
	"fmt"
	"strings"

	"github.com/daviszhen/plan/pkg/common"
	"github.com/daviszhen/plan/pkg/storage"
	"github.com/xlab/treeprint"
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
