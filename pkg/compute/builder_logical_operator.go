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

package compute

import (
	"fmt"

	"github.com/daviszhen/plan/pkg/common"
	"github.com/daviszhen/plan/pkg/storage"
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
		return fmt.Sprintf("unknown_LOT(%d)", lt)
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
		return fmt.Sprintf("unknown_JoinType(%d)", lojt)
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
		return fmt.Sprintf("unknown_ScanType(%d)", st)
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
	Index            uint64
	Filters          []*Expr
	BelongCtx        *BindContext
	Stats            *Stats
	hasEstimatedCard bool
	estimatedCard    uint64
	estimatedProps   *EstimatedProperties
	Outputs          []*Expr
	Counts           ColumnBindCountMap `json:"-"`
	ColRefToPos      ColumnBindPosMap   `json:"-"`

	// Info 存放算子类型特有数据
	Info any
}

func (lo *LogicalOperator) EstimatedCard(txn *storage.Txn) uint64 {
	if lo.Typ == LOT_Scan {
		return lo.getScanTableEnt().GetStats2(0).Count()
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

func (lo *LogicalOperator) String() string {
	ret, err := ExplainLogicalPlan(lo)
	if err != nil {
		panic(err)
	}
	return ret
}

func pushFilter(node *LogicalOperator, expr *Expr) *LogicalOperator {
	if node.Typ != LOT_Filter {
		filter := &LogicalOperator{Typ: LOT_Filter, Children: []*LogicalOperator{node}}
		node = filter
	}
	node.Filters = append(node.Filters, expr)
	return node
}

// getScanInfo returns ScanOpInfo from Info or nil
func (lo *LogicalOperator) getScanInfo() *ScanOpInfo {
	if si, ok := lo.Info.(*ScanOpInfo); ok {
		return si
	}
	return nil
}

// getScanDatabase returns Database from ScanOpInfo or embedded field
func (lo *LogicalOperator) getScanDatabase() string {
	if si, ok := lo.Info.(*ScanOpInfo); ok {
		return si.Database
	}
	return ""
}

// getScanTable returns Table from ScanOpInfo or embedded field
func (lo *LogicalOperator) getScanTable() string {
	if si, ok := lo.Info.(*ScanOpInfo); ok {
		return si.Table
	}
	return ""
}

// getScanAlias returns Alias from ScanOpInfo or embedded field
func (lo *LogicalOperator) getScanAlias() string {
	if si, ok := lo.Info.(*ScanOpInfo); ok {
		return si.Alias
	}
	return ""
}

// getScanTableEnt returns TableEnt from ScanOpInfo or embedded field
func (lo *LogicalOperator) getScanTableEnt() *storage.CatalogEntry {
	if si, ok := lo.Info.(*ScanOpInfo); ok {
		return si.TableEnt
	}
	return nil
}

// getScanColumns returns Columns from ScanOpInfo or embedded field
func (lo *LogicalOperator) getScanColumns() []string {
	if si, ok := lo.Info.(*ScanOpInfo); ok {
		return si.Columns
	}
	return nil
}

// getScanColName2Idx returns ColName2Idx from ScanOpInfo or embedded field
func (lo *LogicalOperator) getScanColName2Idx() map[string]int {
	if si, ok := lo.Info.(*ScanOpInfo); ok {
		return si.ColName2Idx
	}
	return nil
}

// getScanTyp returns ScanTyp from ScanOpInfo or embedded field
func (lo *LogicalOperator) getScanTyp() ScanType {
	if si, ok := lo.Info.(*ScanOpInfo); ok {
		return si.ScanTyp
	}
	return ScanTypeTable
}

// getScanConfig returns the ScanInfo config from ScanOpInfo or embedded field
func (lo *LogicalOperator) getScanConfig() *ScanInfo {
	if si, ok := lo.Info.(*ScanOpInfo); ok {
		return si.ScanInfo
	}
	return nil
}

// getScanTypes returns Types from ScanOpInfo or embedded field
func (lo *LogicalOperator) getScanTypes() []common.LType {
	if si, ok := lo.Info.(*ScanOpInfo); ok {
		return si.Types
	}
	return nil
}

// getScanValues returns Values from ScanOpInfo or embedded field
func (lo *LogicalOperator) getScanValues() [][]*Expr {
	if si, ok := lo.Info.(*ScanOpInfo); ok {
		return si.Values
	}
	return nil
}

// getOrderBys returns OrderBys from Info or embedded field
func (lo *LogicalOperator) getOrderBys() []*Expr {
	if si, ok := lo.Info.(*OrderOpInfo); ok {
		return si.OrderBys
	}
	return nil
}

// getJoinTyp returns JoinTyp from Info or embedded field
func (lo *LogicalOperator) getJoinTyp() LOT_JoinType {
	if si, ok := lo.Info.(*JoinOpInfo); ok {
		return si.JoinTyp
	}
	return LOT_JoinTypeCross
}

// getOnConds returns OnConds from Info or embedded field
func (lo *LogicalOperator) getOnConds() []*Expr {
	if si, ok := lo.Info.(*JoinOpInfo); ok {
		return si.OnConds
	}
	return nil
}

// setOnConds sets OnConds on Info or embedded field
func (lo *LogicalOperator) setOnConds(conds []*Expr) {
	if ji, ok := lo.Info.(*JoinOpInfo); ok {
		ji.OnConds = conds
		return
	}
	
}

// getAggs returns Aggs from Info or embedded field
func (lo *LogicalOperator) getAggs() []*Expr {
	if si, ok := lo.Info.(*AggOpInfo); ok {
		return si.Aggs
	}
	return nil
}

// getGroupBys returns GroupBys from Info or embedded field
func (lo *LogicalOperator) getGroupBys() []*Expr {
	if si, ok := lo.Info.(*AggOpInfo); ok {
		return si.GroupBys
	}
	return nil
}

// getAggTag returns AggTag (Index2) from Info or embedded field
func (lo *LogicalOperator) getAggTag() uint64 {
	if si, ok := lo.Info.(*AggOpInfo); ok {
		return si.AggTag
	}
	return 0
}

// getLimitInfo returns LimitOpInfo from Info
func (lo *LogicalOperator) getLimitInfo() *LimitOpInfo {
	if li, ok := lo.Info.(*LimitOpInfo); ok {
		return li
	}
	return nil
}
