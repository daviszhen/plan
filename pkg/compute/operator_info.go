package compute

import (
	"github.com/daviszhen/plan/pkg/common"
	"github.com/daviszhen/plan/pkg/storage"
)

// Operator Info structs — type-specific data for LogicalOperator/PhysicalOperator

type ScanOpInfo struct {
	Database    string
	Table       string
	Alias       string
	Columns     []string
	TableEnt    *storage.CatalogEntry
	ColName2Idx map[string]int
	ScanTyp     ScanType
	ScanInfo    *ScanInfo
	// values list fields
	Types  []common.LType
	Names  []string
	Values [][]*Expr
}

type JoinOpInfo struct {
	JoinTyp LOT_JoinType
	OnConds []*Expr
}

type AggOpInfo struct {
	Aggs     []*Expr
	GroupBys []*Expr
	AggTag   uint64 // 原 Index2
}

type OrderOpInfo struct {
	OrderBys []*Expr
}

type LimitOpInfo struct {
	Limit  *Expr
	Offset *Expr
}

type InsertOpInfo struct {
	TableEnt      *storage.CatalogEntry
	TableIndex    int
	ExpectedTypes []common.LType
	ColumnIndexMap []int
	IsValuesList  bool
}

type CreateTableOpInfo struct {
	Database    string
	Table       string
	IfNotExists bool
	ColDefs     []*storage.ColumnDefinition
	Constraints []*storage.Constraint
}

type CreateSchemaOpInfo struct {
	Database    string
	IfNotExists bool
}

// StubInfo holds test stub data
type StubInfo struct {
	Table string
}
