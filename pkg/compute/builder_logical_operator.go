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
