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
	return fmt.Sprintf("unknown_POT(%d)", t)
}

type PhysicalOperator struct {
	Typ POT
	Tag int //relationTag
	Id  int

	Index         uint64
	Outputs       []*Expr
	Projects      []*Expr
	Filters       []*Expr
	estimatedCard uint64
	ChunkCount    int                   //for stub
	collection    *ColumnDataCollection //for values list runtime
	Children      []*PhysicalOperator
	ExecStats     ExecStats

	// Info 存放算子类型特有数据
	Info any
}

// PhysicalOperator accessor methods

func (po *PhysicalOperator) getJoinTyp() LOT_JoinType {
	if ji, ok := po.Info.(*JoinOpInfo); ok { return ji.JoinTyp }
	return LOT_JoinTypeCross
}
func (po *PhysicalOperator) getOnConds() []*Expr {
	if ji, ok := po.Info.(*JoinOpInfo); ok { return ji.OnConds }
	return nil
}
func (po *PhysicalOperator) getAggs() []*Expr {
	if ai, ok := po.Info.(*AggOpInfo); ok { return ai.Aggs }
	return nil
}
func (po *PhysicalOperator) getGroupBys() []*Expr {
	if ai, ok := po.Info.(*AggOpInfo); ok { return ai.GroupBys }
	return nil
}
func (po *PhysicalOperator) getAggTag() uint64 {
	if ai, ok := po.Info.(*AggOpInfo); ok { return ai.AggTag }
	return 0
}
func (po *PhysicalOperator) getOrderBys() []*Expr {
	if oi, ok := po.Info.(*OrderOpInfo); ok { return oi.OrderBys }
	return nil
}
func (po *PhysicalOperator) getLimitExpr() *Expr {
	if li, ok := po.Info.(*LimitOpInfo); ok { return li.Limit }
	return nil
}
func (po *PhysicalOperator) getOffsetExpr() *Expr {
	if li, ok := po.Info.(*LimitOpInfo); ok { return li.Offset }
	return nil
}
func (po *PhysicalOperator) getScanDatabase() string {
	if si, ok := po.Info.(*ScanOpInfo); ok { return si.Database }
	return ""
}
func (po *PhysicalOperator) getScanTable() string {
	if si, ok := po.Info.(*ScanOpInfo); ok { return si.Table }
	return ""
}
func (po *PhysicalOperator) getScanAlias() string {
	if si, ok := po.Info.(*ScanOpInfo); ok { return si.Alias }
	return ""
}
func (po *PhysicalOperator) getScanColumns() []string {
	if si, ok := po.Info.(*ScanOpInfo); ok { return si.Columns }
	return nil
}
func (po *PhysicalOperator) getScanTyp() ScanType {
	if si, ok := po.Info.(*ScanOpInfo); ok { return si.ScanTyp }
	return ScanTypeTable
}
func (po *PhysicalOperator) getScanColName2Idx() map[string]int {
	if si, ok := po.Info.(*ScanOpInfo); ok { return si.ColName2Idx }
	return nil
}
func (po *PhysicalOperator) getScanTableEnt() *storage.CatalogEntry {
	if si, ok := po.Info.(*ScanOpInfo); ok { return si.TableEnt }
	return nil
}
func (po *PhysicalOperator) getScanConfig() *ScanInfo {
	if si, ok := po.Info.(*ScanOpInfo); ok { return si.ScanInfo }
	return nil
}
func (po *PhysicalOperator) getScanTypes() []common.LType {
	if si, ok := po.Info.(*ScanOpInfo); ok { return si.Types }
	return nil
}

func (po *PhysicalOperator) String() string {
	ret, err := ExplainPhysicalPlan(po)
	if err != nil {
		panic(err)
	}
	return ret
}
