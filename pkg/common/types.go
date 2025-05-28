package common

import (
	"fmt"
	"unsafe"
)

var (
	BoolSize     int
	Int8Size     int
	Int16Size    int
	Int32Size    int
	Int64Size    int
	Int128Size   int
	IntervalSize int
	DateSize     int
	VarcharSize  int
	PointerSize  int
	DecimalSize  int
	Float32Size  int
)

func init() {
	b := false
	BoolSize = int(unsafe.Sizeof(b))
	i := int8(0)
	Int8Size = int(unsafe.Sizeof(i))
	Int16Size = Int8Size * 2
	Int32Size = Int8Size * 4
	Int64Size = Int8Size * 8
	Int128Size = int(unsafe.Sizeof(Hugeint{}))
	IntervalSize = int(unsafe.Sizeof(Interval{}))
	DateSize = int(unsafe.Sizeof(Date{}))
	VarcharSize = int(unsafe.Sizeof(String{}))
	PointerSize = int(unsafe.Sizeof(unsafe.Pointer(&b)))
	DecimalSize = int(unsafe.Sizeof(Decimal{}))
	f := float32(0)
	Float32Size = int(unsafe.Sizeof(f))
}

type AggrType int

const (
	NON_DISTINCT AggrType = iota
	DISTINCT
)

type SourceType int

const (
	ThisNode   SourceType = 0
	LeftChild  SourceType = -1 //also for one child
	RightChild SourceType = -2
)

type BindingType int

const (
	BT_BASE BindingType = iota
	BT_TABLE
	BT_DUMMY
	BT_CATALOG_ENTRY
	BT_Subquery
)

func (bt BindingType) String() string {
	switch bt {
	case BT_BASE:
		return "base"
	case BT_TABLE:
		return "table"
	case BT_DUMMY:
		return "dummy"
	case BT_CATALOG_ENTRY:
		return "catalog_entry"
	case BT_Subquery:
		return "subquery"
	default:
		panic(fmt.Sprintf("usp binding type %d", bt))
	}
}

type Expr interface{}
type Builder interface{}
type Binding interface {
	HasColumn(column string) int
	Type() BindingType
	Types() []LType
	Alias() string
	Index() uint64
}
type BindContext interface {
	GetMatchingBinding(table, column string) (Binding, int, error)
}
type ExprState interface{}
type AggrInputData interface{}
type AggrUnaryInput interface{}
type AggrFinalizeData interface{}
type LogicalOperator interface{}
type PhysicalOperator interface{}
type TupleDataLayout interface{}
