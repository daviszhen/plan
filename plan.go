package main

import (
	"fmt"
	"strings"

	"github.com/huandu/go-clone"
	"github.com/xlab/treeprint"
)

type PhyType int

const (
	BOOL PhyType = iota + 1
	UINT8
	INT8
	UINT16
	INT16
	UINT32
	INT32
	UINT64
	INT64
	FLOAT
	DOUBLE
	INTERVAL
	LIST
	STRUCT
	VARCHAR
	INT128
	UNKNOWN
	BIT
	INVALID
)

var pTypeToStr = map[PhyType]string{
	BOOL:     "BOOL",
	UINT8:    "UINT8",
	INT8:     "INT8",
	UINT16:   "UINT16",
	INT16:    "INT16",
	UINT32:   "UINT32",
	INT32:    "INT32",
	UINT64:   "UINT64",
	INT64:    "INT64",
	FLOAT:    "FLOAT",
	DOUBLE:   "DOUBLE",
	INTERVAL: "INTERVAL",
	LIST:     "LIST",
	STRUCT:   "STRUCT",
	VARCHAR:  "VARCHAR",
	INT128:   "INT128",
	UNKNOWN:  "UNKNOWN",
	BIT:      "BIT",
	INVALID:  "INVALID",
}

func (pt PhyType) String() string {
	if s, has := pTypeToStr[pt]; has {
		return s
	}
	panic(fmt.Sprintf("usp %d", pt))
}

type LTypeId int

const (
	LTID_INVALID LTypeId = iota
	LTID_NULL
	LTID_UNKNOWN
	LTID_ANY
	LTID_USER
	LTID_BOOLEAN
	LTID_TINYINT
	LTID_SMALLINT
	LTID_INTEGER
	LTID_BIGINT
	LTID_DATE
	LTID_TIME
	LTID_TIMESTAMP_SEC
	LTID_TIMESTAMP_MS
	LTID_TIMESTAMP
	LTID_TIMESTAMP_NS
	LTID_DECIMAL
	LTID_FLOAT
	LTID_DOUBLE
	LTID_CHAR
	LTID_VARCHAR
	LTID_BLOB
	LTID_INTERVAL
	LTID_UTINYINT
	LTID_USMALLINT
	LTID_UINTEGER
	LTID_UBIGINT
	LTID_TIMESTAMP_TZ
	LTID_TIME_TZ
	LTID_BIT
	LTID_HUGEINT
	LTID_POINTER
	LTID_VALIDITY
	LTID_UUID
	LTID_STRUCT
	LTID_LIST
	LTID_MAP
	LTID_TABLE
	LTID_ENUM
	LTID_AGGREGATE_STATE
	LTID_LAMBDA
	LTID_UNION
)

var lTypeIdToStr = map[LTypeId]string{
	LTID_INVALID:         "LTID_INVALID",
	LTID_NULL:            "LTID_NULL",
	LTID_UNKNOWN:         "LTID_UNKNOWN",
	LTID_ANY:             "LTID_ANY",
	LTID_USER:            "LTID_USER",
	LTID_BOOLEAN:         "LTID_BOOLEAN",
	LTID_TINYINT:         "LTID_TINYINT",
	LTID_SMALLINT:        "LTID_SMALLINT",
	LTID_INTEGER:         "LTID_INTEGER",
	LTID_BIGINT:          "LTID_BIGINT",
	LTID_DATE:            "LTID_DATE",
	LTID_TIME:            "LTID_TIME",
	LTID_TIMESTAMP_SEC:   "LTID_TIMESTAMP_SEC",
	LTID_TIMESTAMP_MS:    "LTID_TIMESTAMP_MS",
	LTID_TIMESTAMP:       "LTID_TIMESTAMP",
	LTID_TIMESTAMP_NS:    "LTID_TIMESTAMP_NS",
	LTID_DECIMAL:         "LTID_DECIMAL",
	LTID_FLOAT:           "LTID_FLOAT",
	LTID_DOUBLE:          "LTID_DOUBLE",
	LTID_CHAR:            "LTID_CHAR",
	LTID_VARCHAR:         "LTID_VARCHAR",
	LTID_BLOB:            "LTID_BLOB",
	LTID_INTERVAL:        "LTID_INTERVAL",
	LTID_UTINYINT:        "LTID_UTINYINT",
	LTID_USMALLINT:       "LTID_USMALLINT",
	LTID_UINTEGER:        "LTID_UINTEGER",
	LTID_UBIGINT:         "LTID_UBIGINT",
	LTID_TIMESTAMP_TZ:    "LTID_TIMESTAMP_TZ",
	LTID_TIME_TZ:         "LTID_TIME_TZ",
	LTID_BIT:             "LTID_BIT",
	LTID_HUGEINT:         "LTID_HUGEINT",
	LTID_POINTER:         "LTID_POINTER",
	LTID_VALIDITY:        "LTID_VALIDITY",
	LTID_UUID:            "LTID_UUID",
	LTID_STRUCT:          "LTID_STRUCT",
	LTID_LIST:            "LTID_LIST",
	LTID_MAP:             "LTID_MAP",
	LTID_TABLE:           "LTID_TABLE",
	LTID_ENUM:            "LTID_ENUM",
	LTID_AGGREGATE_STATE: "LTID_AGGREGATE_STATE",
	LTID_LAMBDA:          "LTID_LAMBDA",
	LTID_UNION:           "LTID_UNION",
}

func (id LTypeId) String() string {
	if s, has := lTypeIdToStr[id]; has {
		return s
	}
	panic(fmt.Sprintf("usp %d", id))
}

const (
	DecimalMaxWidthInt16  = 4
	DecimalMaxWidthInt32  = 9
	DecimalMaxWidthInt64  = 18
	DecimalMaxWidthInt128 = 38
	DecimalMaxWidth       = 38
)

type LType struct {
	id    LTypeId
	pTyp  PhyType
	width int
	scale int
}

func makeLType(id LTypeId) LType {
	ret := LType{id: id}
	ret.pTyp = ret.getInternalType()
	return ret
}

func invalidLType() LType {
	return makeLType(LTID_INVALID)
}

func decimal(width, scale int) LType {
	ret := makeLType(LTID_DECIMAL)
	ret.width = width
	ret.scale = scale
	return ret
}

func hugeint() LType {
	return makeLType(LTID_HUGEINT)
}

func bigint() LType {
	return makeLType(LTID_BIGINT)
}

func integer() LType {
	return makeLType(LTID_INTEGER)
}

func float() LType {
	return makeLType(LTID_FLOAT)
}

func smallint() LType {
	return makeLType(LTID_SMALLINT)
}

func varchar() LType {
	return makeLType(LTID_VARCHAR)
}
func varchar2(width int) LType {
	ret := makeLType(LTID_VARCHAR)
	ret.width = width
	return ret
}

func dateLTyp() LType {
	return makeLType(LTID_DATE)
}

func boolean() LType {
	return makeLType(LTID_BOOLEAN)
}

func intervalLType() LType {
	return makeLType(LTID_INTERVAL)
}

var numerics = map[LTypeId]int{
	LTID_TINYINT:   0,
	LTID_SMALLINT:  0,
	LTID_INTEGER:   0,
	LTID_BIGINT:    0,
	LTID_HUGEINT:   0,
	LTID_FLOAT:     0,
	LTID_DOUBLE:    0,
	LTID_DECIMAL:   0,
	LTID_UTINYINT:  0,
	LTID_USMALLINT: 0,
	LTID_UINTEGER:  0,
	LTID_UBIGINT:   0,
}

func (lt LType) isNumeric() bool {
	if _, has := numerics[lt.id]; has {
		return true
	}
	return false
}

func (lt LType) getDecimalSize() (bool, int, int) {
	switch lt.id {
	case LTID_NULL:
		return true, 0, 0
	case LTID_BOOLEAN:
		return true, 1, 0
	case LTID_TINYINT:
		// tinyint: [-127, 127] = DECIMAL(3,0)
		return true, 3, 0
	case LTID_SMALLINT:
		// smallint: [-32767, 32767] = DECIMAL(5,0)
		return true, 5, 0
	case LTID_INTEGER:
		// integer: [-2147483647, 2147483647] = DECIMAL(10,0)
		return true, 10, 0
	case LTID_BIGINT:
		// bigint: [-9223372036854775807, 9223372036854775807] = DECIMAL(19,0)
		return true, 19, 0
	case LTID_UTINYINT:
		// UInt8 — [0 : 255]
		return true, 3, 0
	case LTID_USMALLINT:
		// UInt16 — [0 : 65535]
		return true, 5, 0
	case LTID_UINTEGER:
		// UInt32 — [0 : 4294967295]
		return true, 10, 0
	case LTID_UBIGINT:
		// UInt64 — [0 : 18446744073709551615]
		return true, 20, 0
	case LTID_HUGEINT:
		// hugeint: max size decimal (38, 0)
		// note that a hugeint is not guaranteed to fit in this
		return true, 38, 0
	case LTID_DECIMAL:
		return true, lt.width, lt.scale
	default:
		return false, 0, 0
	}
}

func (lt LType) equal(o LType) bool {
	if lt.id != o.id {
		return false
	}
	switch lt.id {
	case LTID_DECIMAL:
		return lt.width == o.width && lt.scale == o.scale
	default:

	}
	return true
}

func (lt LType) getInternalType() PhyType {
	switch lt.id {
	case LTID_BOOLEAN:
		return BOOL
	case LTID_TINYINT:
		return INT8
	case LTID_UTINYINT:
		return UINT8
	case LTID_SMALLINT:
		return INT16
	case LTID_USMALLINT:
		return UINT16
	case LTID_NULL, LTID_DATE, LTID_INTEGER:
		return INT32
	case LTID_UINTEGER:
		return UINT32
	case LTID_BIGINT, LTID_TIME,
		LTID_TIMESTAMP, LTID_TIMESTAMP_SEC,
		LTID_TIMESTAMP_NS, LTID_TIMESTAMP_MS,
		LTID_TIME_TZ, LTID_TIMESTAMP_TZ:
		return INT64
	case LTID_UBIGINT:
		return UINT64
	case LTID_HUGEINT, LTID_UUID:
		return INT128
	case LTID_FLOAT:
		return FLOAT
	case LTID_DOUBLE:
		return DOUBLE
	case LTID_DECIMAL:
		if lt.width <= DecimalMaxWidthInt16 {
			return INT16
		} else if lt.width <= DecimalMaxWidthInt32 {
			return INT32
		} else if lt.width <= DecimalMaxWidthInt64 {
			return INT64
		} else if lt.width <= DecimalMaxWidthInt128 {
			return INT128
		} else {
			panic(fmt.Sprintf("usp decimal width %d", lt.width))
		}
	case LTID_VARCHAR, LTID_CHAR, LTID_BLOB, LTID_BIT:
		return VARCHAR
	case LTID_INTERVAL:
		return INTERVAL
	case LTID_UNION, LTID_STRUCT:
		return STRUCT
	case LTID_LIST, LTID_MAP:
		return LIST
	case LTID_POINTER:
		return UINT64
	case LTID_VALIDITY:
		return BIT
	case LTID_ENUM:
		{
			panic("usp enum")
		}
	case LTID_TABLE, LTID_LAMBDA, LTID_ANY, LTID_INVALID, LTID_UNKNOWN:
		return INVALID
	case LTID_USER:
		return UNKNOWN
	case LTID_AGGREGATE_STATE:
		return VARCHAR
	default:
		panic(fmt.Sprintf("usp logical type %d", lt))
	}
}

func (lt LType) String() string {
	return fmt.Sprintf("(%v %v %d %d)", lt.id, lt.pTyp, lt.width, lt.scale)
}

func targetTypeCost(typ LType) int64 {
	switch typ.id {
	case LTID_INTEGER:
		return 103
	case LTID_BIGINT:
		return 101
	case LTID_DOUBLE:
		return 102
	case LTID_HUGEINT:
		return 120
	case LTID_TIMESTAMP:
		return 120
	case LTID_VARCHAR:
		return 149
	case LTID_DECIMAL:
		return 104
	case LTID_STRUCT, LTID_MAP, LTID_LIST, LTID_UNION:
		return 160
	default:
		return 110
	}
	panic("can not be here")
}

var tinyintTo = map[LTypeId]int{
	LTID_SMALLINT: 0,
	LTID_INTEGER:  0,
	LTID_BIGINT:   0,
	LTID_HUGEINT:  0,
	LTID_FLOAT:    0,
	LTID_DOUBLE:   0,
	LTID_DECIMAL:  0,
}

func implicitCastTinyint(to LType) int64 {
	if _, has := tinyintTo[to.id]; has {
		return targetTypeCost(to)
	}
	return -1
}

var utinyintTo = map[LTypeId]int{
	LTID_USMALLINT: 0,
	LTID_UINTEGER:  0,
	LTID_UBIGINT:   0,
	LTID_SMALLINT:  0,
	LTID_INTEGER:   0,
	LTID_BIGINT:    0,
	LTID_HUGEINT:   0,
	LTID_FLOAT:     0,
	LTID_DOUBLE:    0,
	LTID_DECIMAL:   0,
}

func implicitCastUTinyint(to LType) int64 {
	if _, has := utinyintTo[to.id]; has {
		return targetTypeCost(to)
	}
	return -1
}

var smallintTo = map[LTypeId]int{
	LTID_INTEGER: 0,
	LTID_BIGINT:  0,
	LTID_HUGEINT: 0,
	LTID_FLOAT:   0,
	LTID_DOUBLE:  0,
	LTID_DECIMAL: 0,
}

func implicitCastSmallint(to LType) int64 {
	if _, has := smallintTo[to.id]; has {
		return targetTypeCost(to)
	}
	return -1
}

var usmallintTo = map[LTypeId]int{
	LTID_UINTEGER: 0,
	LTID_UBIGINT:  0,
	LTID_INTEGER:  0,
	LTID_BIGINT:   0,
	LTID_HUGEINT:  0,
	LTID_FLOAT:    0,
	LTID_DOUBLE:   0,
	LTID_DECIMAL:  0,
}

func implicitCastUSmallint(to LType) int64 {
	if _, has := usmallintTo[to.id]; has {
		return targetTypeCost(to)
	}
	return -1
}

var hugeintTo = map[LTypeId]int{
	LTID_FLOAT:   0,
	LTID_DOUBLE:  0,
	LTID_DECIMAL: 0,
}

func implicitCastHugeint(to LType) int64 {
	if _, has := hugeintTo[to.id]; has {
		return targetTypeCost(to)
	}
	return -1
}

var floatTo = map[LTypeId]int{
	LTID_DOUBLE: 0,
}

func implicitCastFloat(to LType) int64 {
	if _, has := floatTo[to.id]; has {
		return targetTypeCost(to)
	}
	return -1
}

var doubleTo = map[LTypeId]int{}

func implicitCastDouble(to LType) int64 {
	if _, has := doubleTo[to.id]; has {
		return targetTypeCost(to)
	}
	return -1
}

var dateTo = map[LTypeId]int{
	LTID_TIMESTAMP: 0,
}

func implicitCastDate(to LType) int64 {
	if _, has := dateTo[to.id]; has {
		return targetTypeCost(to)
	}
	return -1
}

var decimalTo = map[LTypeId]int{
	LTID_FLOAT:  0,
	LTID_DOUBLE: 0,
}

func implicitCastDecimal(to LType) int64 {
	if _, has := decimalTo[to.id]; has {
		return targetTypeCost(to)
	}
	return -1
}

var integerTo = map[LTypeId]int{
	LTID_BIGINT:  0,
	LTID_HUGEINT: 0,
	LTID_FLOAT:   0,
	LTID_DOUBLE:  0,
	LTID_DECIMAL: 0,
}

func implicitCastInteger(to LType) int64 {
	if _, has := integerTo[to.id]; has {
		return targetTypeCost(to)
	}
	return -1
}

var uintTo = map[LTypeId]int{
	LTID_UBIGINT: 0,
	LTID_BIGINT:  0,
	LTID_HUGEINT: 0,
	LTID_FLOAT:   0,
	LTID_DOUBLE:  0,
	LTID_DECIMAL: 0,
}

func implicitCastUInteger(to LType) int64 {
	if _, has := uintTo[to.id]; has {
		return targetTypeCost(to)
	}
	return -1
}

var bigintTo = map[LTypeId]int{
	LTID_FLOAT:   0,
	LTID_DOUBLE:  0,
	LTID_HUGEINT: 0,
	LTID_DECIMAL: 0,
}

func implicitCastBigint(to LType) int64 {
	if _, has := bigintTo[to.id]; has {
		return targetTypeCost(to)
	}
	return -1
}

var ubigintTo = map[LTypeId]int{
	LTID_FLOAT:   0,
	LTID_DOUBLE:  0,
	LTID_HUGEINT: 0,
	LTID_DECIMAL: 0,
}

func implicitCastUBigint(to LType) int64 {
	if _, has := ubigintTo[to.id]; has {
		return targetTypeCost(to)
	}
	return -1
}

func implicitCast(from, to LType) int64 {
	if from.id == LTID_NULL {
		//cast NULL to anything
		return targetTypeCost(to)
	}
	if from.id == LTID_UNKNOWN {
		//parameter expr cast no cost
		return 0
	}
	if to.id == LTID_ANY {
		//cast to any
		return 1
	}
	if from.id == to.id {
		//same. no cost
		return 0
	}
	if to.id == LTID_VARCHAR {
		//anything cast to varchar
		return targetTypeCost(to)
	}
	switch from.id {
	case LTID_TINYINT:
		return implicitCastTinyint(to)
	case LTID_SMALLINT:
		return implicitCastSmallint(to)
	case LTID_INTEGER:
		return implicitCastInteger(to)
	case LTID_BIGINT:
		return implicitCastBigint(to)
	case LTID_UTINYINT:
		return implicitCastUTinyint(to)
	case LTID_USMALLINT:
		return implicitCastUSmallint(to)
	case LTID_UINTEGER:
		return implicitCastUInteger(to)
	case LTID_UBIGINT:
		return implicitCastUBigint(to)
	case LTID_HUGEINT:
		return implicitCastHugeint(to)
	case LTID_FLOAT:
		return implicitCastFloat(to)
	case LTID_DOUBLE:
		return implicitCastDouble(to)
	case LTID_DATE:
		return implicitCastDate(to)
	case LTID_DECIMAL:
		return implicitCastDecimal(to)
	default:
		return -1
	}
	return -1
}

func decimalSizeCheck(left, right LType) LType {
	if left.id != LTID_DECIMAL && right.id != LTID_DECIMAL {
		panic("wrong type")
	}
	if left.id == right.id {
		panic("wrong type")
	}

	if left.id == LTID_DECIMAL {
		return decimalSizeCheck(right, left)
	}

	can, owith, _ := left.getDecimalSize()
	if !can {
		panic(fmt.Sprintf("to decimal failed. %v ", left))
	}
	ewidth := right.width - right.scale
	if owith > ewidth {
		newWidth := owith + right.scale
		newWidth = min(newWidth, DecimalMaxWidth)
		return decimal(newWidth, right.scale)
	}
	return right
}

func decideNumericType(left, right LType) LType {
	if left.id > right.id {
		return decideNumericType(right, left)
	}

	if implicitCast(left, right) >= 0 {
		if right.id == LTID_DECIMAL {
			return decimalSizeCheck(left, right)
		}
		return right
	}

	if implicitCast(right, left) >= 0 {
		if left.id == LTID_DECIMAL {
			return decimalSizeCheck(right, left)
		}
		return left
	}
	//types that can not be cast implicitly.
	//they are different.
	//left is signed and right is unsigned.
	//upcast
	if left.id == LTID_BIGINT || right.id == LTID_UBIGINT {
		return hugeint()
	}
	if left.id == LTID_INTEGER || right.id == LTID_UINTEGER {
		return bigint()
	}
	if left.id == LTID_SMALLINT || right.id == LTID_USMALLINT {
		return integer()
	}
	if left.id == LTID_TINYINT || right.id == LTID_UTINYINT {
		return smallint()
	}
	panic(fmt.Sprintf("imcompatible %v %v", left, right))
}

func MaxLType(left, right LType) LType {
	//digit type
	if left.id != right.id &&
		left.isNumeric() && right.isNumeric() {
		return decideNumericType(left, right)
	} else if left.id == LTID_UNKNOWN {
		return right
	} else if right.id == LTID_UNKNOWN {
		return left
	} else if left.id < right.id {
		if left.id == LTID_DATE && right.id == LTID_INTERVAL {
			return left
		} else if left.id == LTID_INTERVAL && right.id == LTID_DATE {
			return right
		}
		return right
	}
	if right.id < left.id {
		return left
	}
	id := left.id
	if id == LTID_ENUM {
		if left.equal(right) {
			return left
		} else {
			//enum cast to varchar
			return varchar()
		}
	}
	if id == LTID_VARCHAR {
		//no collation here
		return right
	}
	if id == LTID_DECIMAL {
		//decide the width & scal of the final deciaml
		leftNum := left.width - left.scale
		rightNum := right.width - right.scale
		num := max(leftNum, rightNum)
		scale := max(left.scale, right.scale)
		width := num + scale
		if width > DecimalMaxWidth {
			width = DecimalMaxWidth
			scale = width - num
		}
		return decimal(width, scale)
	}
	//same
	return left
}

type DataType int

const (
	DataTypeInteger DataType = iota
	DataTypeVarchar
	DataTypeDecimal
	DataTypeDate
	DataTypeBool
	DataTypeInterval
	DataTypeFloat64
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
	LTyp    LType
	NotNull bool
}

func (edt ExprDataType) equal(o ExprDataType) bool {
	if edt.NotNull != o.NotNull {
		return false
	}
	return edt.LTyp.equal(o.LTyp)
}

func (edt ExprDataType) include(o ExprDataType) bool {
	if !edt.LTyp.equal(o.LTyp) {
		if edt.LTyp.id != o.LTyp.id {
			return false
		}
		switch edt.LTyp.id {
		case LTID_DECIMAL:
			if implicitCast(o.LTyp, edt.LTyp) >= 0 {
				return true
			}
		}
		return false
	}
	if edt.NotNull {
		return o.NotNull
	} else {
		return true
	}
}

func (edt ExprDataType) String() string {
	null := "null"
	if edt.NotNull {
		null = "not null"
	}
	return fmt.Sprintf("{%v,%s}", edt.LTyp, null)
}

var InvalidExprDataType = ExprDataType{
	LTyp: invalidLType(),
}

type LOT int

const (
	LOT_Project  LOT = 0
	LOT_Filter       = 1
	LOT_Scan         = 2
	LOT_JOIN         = 3
	LOT_AggGroup     = 4
	LOT_Order        = 5
	LOT_Limit        = 6
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
	default:
		panic(fmt.Sprintf("usp %d", lojt))
	}
}

type LogicalOperator struct {
	Typ LOT

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
	Stats            *Stats
	hasEstimatedCard bool
	estimatedCard    uint64
	estimatedProps   *EstimatedProperties

	Children []*LogicalOperator
}

func (lo *LogicalOperator) EstimatedCard() uint64 {
	if lo.Typ == LOT_Scan {
		catalogTable, err := tpchCatalog().Table(lo.Database, lo.Table)
		if err != nil {
			panic(err)
		}
		return uint64(catalogTable.Stats.RowCount)
	}
	if lo.hasEstimatedCard {
		return lo.estimatedCard
	}
	maxCard := uint64(0)
	for _, child := range lo.Children {
		childCard := child.EstimatedCard()
		maxCard = max(maxCard, childCard)
	}
	lo.hasEstimatedCard = true
	lo.estimatedCard = maxCard
	return lo.estimatedCard
}

func (lo *LogicalOperator) Print(tree treeprint.Tree) {
	if lo == nil {
		return
	}
	switch lo.Typ {
	case LOT_Project:
		tree = tree.AddBranch("Project:")
		tree.AddMetaNode("index", fmt.Sprintf("%d", lo.Index))
		node := tree.AddMetaBranch("exprs", "")
		listExprsToTree(node, lo.Projects)
	case LOT_Filter:
		tree = tree.AddBranch("Filter:")
		node := tree.AddMetaBranch("exprs", "")
		listExprsToTree(node, lo.Filters)
	case LOT_Scan:
		tree = tree.AddBranch("Scan:")
		tree.AddMetaNode("index", fmt.Sprintf("%d", lo.Index))
		tableInfo := ""
		if len(lo.Alias) != 0 && lo.Alias != lo.Table {
			tableInfo = fmt.Sprintf("%v.%v %v", lo.Database, lo.Table, lo.Alias)
		} else {
			tableInfo = fmt.Sprintf("%v.%v", lo.Database, lo.Table)
		}
		tree.AddMetaNode("table", tableInfo)
		catalogTable, err := tpchCatalog().Table(lo.Database, lo.Table)
		if err != nil {
			panic("no table")
		}
		printColumns := func(cols []string) string {
			t := strings.Builder{}
			t.WriteByte('\n')
			for i, col := range cols {
				t.WriteString(fmt.Sprintf("col %d %v", i, col))
				t.WriteByte('\n')
			}
			return t.String()
		}
		if len(lo.Columns) > 0 {
			tree.AddMetaNode("columns", printColumns(lo.Columns))
		} else {
			tree.AddMetaNode("columns", printColumns(catalogTable.Columns))
		}
		node := tree.AddBranch("filters")
		listExprsToTree(node, lo.Filters)
		printStats := func(columns []string) string {
			sb := strings.Builder{}
			sb.WriteString(fmt.Sprintf("rowcount %v\n", lo.Stats.RowCount))
			for colIdx, colName := range lo.Columns {
				originIdx := catalogTable.Column2Idx[colName]
				sb.WriteString(fmt.Sprintf("col %v %v ", colIdx, colName))
				sb.WriteString(lo.Stats.ColStats[originIdx].String())
				sb.WriteByte('\n')
			}
			return sb.String()
		}
		if len(lo.Columns) > 0 {
			tree.AddMetaNode("stats", printStats(lo.Columns))
		} else {
			tree.AddMetaNode("stats", printStats(catalogTable.Columns))
		}

	case LOT_JOIN:
		tree = tree.AddBranch(fmt.Sprintf("Join (%v):", lo.JoinTyp))
		if len(lo.OnConds) > 0 {
			node := tree.AddMetaBranch("On", "")
			listExprsToTree(node, lo.OnConds)
		}
		if lo.Stats != nil {
			tree.AddMetaNode("Stats", lo.Stats.String())
		}
	case LOT_AggGroup:
		tree = tree.AddBranch("Aggregate:")
		if len(lo.GroupBys) > 0 {
			node := tree.AddBranch(fmt.Sprintf("groupExprs, index %d", lo.Index))
			listExprsToTree(node, lo.GroupBys)
		}
		if len(lo.Aggs) > 0 {
			node := tree.AddBranch(fmt.Sprintf("aggExprs, index %d", lo.Index2))
			listExprsToTree(node, lo.Aggs)
		}
		if len(lo.Filters) > 0 {
			node := tree.AddBranch("filters")
			listExprsToTree(node, lo.Filters)
		}

	case LOT_Order:
		tree = tree.AddBranch("Order:")
		node := tree.AddMetaBranch("exprs", "")
		listExprsToTree(node, lo.OrderBys)
	case LOT_Limit:
		tree = tree.AddBranch(fmt.Sprintf("Limit: %v", lo.Limit.String()))
	default:
		panic(fmt.Sprintf("usp %v", lo.Typ))
	}

	for _, child := range lo.Children {
		child.Print(tree)
	}
}

func (lo *LogicalOperator) String() string {
	tree := treeprint.NewWithRoot("LogicalOperator:")
	lo.Print(tree)
	return tree.String()
}

func checkExprIsValid(root *LogicalOperator) {
	if root == nil {
		return
	}
	checkExprs(root.Projects...)
	checkExprs(root.Filters...)
	checkExprs(root.OnConds...)
	checkExprs(root.Aggs...)
	checkExprs(root.GroupBys...)
	checkExprs(root.OrderBys...)
	checkExprs(root.Limit)
	for _, child := range root.Children {
		checkExprIsValid(child)
	}
}

func checkExprs(e ...*Expr) {
	for _, expr := range e {
		if expr == nil {
			continue
		}
		if expr.Typ == ET_Func && expr.SubTyp == ET_Invalid {
			panic("xxx")
		}
		if expr.Typ == ET_Func && expr.SubTyp == ET_Between {
			if len(expr.Children) != 3 {
				panic("invalid between")
			}
		}
		if expr.DataTyp.LTyp.id == LTID_INVALID {
			panic("invalid logical type")
		}
	}
}

type POT int

const (
	POT_Project POT = iota
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
	ET_Column ET = iota //column
	ET_TABLE            //table
	ET_Join             //join
	ET_CTE

	ET_Func
	ET_Subquery

	ET_IConst    //integer
	ET_SConst    //string
	ET_FConst    //float
	ET_DateConst //date
	ET_IntervalConst
	ET_BConst // bool

	ET_Orderby
)

type ET_SubTyp int

const (
	//real function
	ET_Invalid ET_SubTyp = iota
	ET_SubFunc
	//operator
	ET_Add
	ET_Sub
	ET_Mul
	ET_Div
	ET_Equal
	ET_NotEqual
	ET_Greater
	ET_GreaterEqual
	ET_Less
	ET_LessEqual
	ET_And
	ET_Or
	ET_Not
	ET_Like
	ET_NotLike
	ET_Between
	ET_Case
	ET_In
	ET_NotIn
	ET_Exists
	ET_NotExists
)

func (et ET_SubTyp) String() string {
	switch et {
	case ET_Add:
		return "+"
	case ET_Sub:
		return "-"
	case ET_Mul:
		return "*"
	case ET_Div:
		return "/"
	case ET_Equal:
		return "="
	case ET_NotEqual:
		return "<>"
	case ET_Greater:
		return ">"
	case ET_GreaterEqual:
		return ">="
	case ET_Less:
		return "<"
	case ET_LessEqual:
		return "<="
	case ET_And:
		return "and"
	case ET_Or:
		return "or"
	case ET_Not:
		return "not"
	case ET_Like:
		return "like"
	case ET_NotLike:
		return "not like"
	case ET_Between:
		return "between"
	case ET_Case:
		return "case"
	case ET_In:
		return "in"
	case ET_NotIn:
		return "not in"
	case ET_Exists:
		return "exists"
	case ET_NotExists:
		return "not exists"
	default:
		panic(fmt.Sprintf("usp %v", int(et)))
	}
}

type ET_JoinType int

const (
	ET_JoinTypeCross ET_JoinType = iota
	ET_JoinTypeLeft
	ET_JoinTypeInner
)

type ET_SubqueryType int

const (
	ET_SubqueryTypeScalar ET_SubqueryType = iota
	ET_SubqueryTypeExists
	ET_SubqueryTypeNotExists
)

type Expr struct {
	Typ     ET
	SubTyp  ET_SubTyp
	DataTyp ExprDataType

	Index       uint64
	Database    string
	Table       string    // table
	Name        string    // column
	ColRef      [2]uint64 // relationTag, columnPos
	Depth       int       // > 0, correlated column
	Svalue      string
	Ivalue      int64
	Fvalue      float64
	Bvalue      bool
	Desc        bool        // in orderby
	JoinTyp     ET_JoinType // join
	Alias       string
	SubBuilder  *Builder     // builder for subquery
	SubCtx      *BindContext // context for subquery
	FuncId      FuncId
	SubqueryTyp ET_SubqueryType
	Kase        *Expr
	When        []*Expr
	Els         *Expr
	CTEIndex    uint64

	Children  []*Expr
	BelongCtx *BindContext // context for table and join
	On        *Expr        //JoinOn
}

func restoreExpr(e *Expr, index uint64, realExprs []*Expr) *Expr {
	if e == nil {
		return nil
	}
	switch e.Typ {
	case ET_Column:
		if index == e.ColRef[0] {
			e = realExprs[e.ColRef[1]]
		}
	case ET_SConst, ET_IConst, ET_DateConst, ET_IntervalConst, ET_BConst, ET_FConst:
	case ET_Func:
	default:
		panic("usp")
	}
	for i, child := range e.Children {
		e.Children[i] = restoreExpr(child, index, realExprs)
	}
	return e
}

func referTo(e *Expr, index uint64) bool {
	if e == nil {
		return false
	}
	switch e.Typ {
	case ET_Column:
		return index == e.ColRef[0]
	case ET_SConst, ET_IConst, ET_DateConst, ET_FConst:

	case ET_Func:
	default:
		panic("usp")
	}
	for _, child := range e.Children {
		if referTo(child, index) {
			return true
		}
	}
	return false
}

func onlyReferTo(e *Expr, index uint64) bool {
	if e == nil {
		return false
	}
	switch e.Typ {
	case ET_Column:
		return index == e.ColRef[0]

	case ET_SConst, ET_IConst, ET_DateConst, ET_IntervalConst, ET_BConst, ET_FConst:
		return true
	case ET_Func:
	default:
		panic("usp")
	}
	for _, child := range e.Children {
		if !onlyReferTo(child, index) {
			return false
		}
	}
	return true
}

func decideSide(e *Expr, leftTags, rightTags map[uint64]bool) int {
	var ret int
	switch e.Typ {
	case ET_Column:
		if _, has := leftTags[e.ColRef[0]]; has {
			ret |= LeftSide
		}
		if _, has := rightTags[e.ColRef[0]]; has {
			ret |= RightSide
		}
	case ET_SConst, ET_DateConst, ET_IConst, ET_IntervalConst, ET_BConst, ET_FConst:
	case ET_Func:
	default:
		panic("usp")
	}
	for _, child := range e.Children {
		ret |= decideSide(child, leftTags, rightTags)
	}
	return ret
}

func getTableNameFromExprs(exprs map[*Expr]bool) (string, string) {
	if len(exprs) == 0 {
		panic("must have exprs")
	}
	for e := range exprs {
		if e.Typ != ET_Column {
			panic("must be column ref")
		}
		return "", e.Table
	}
	return "", ""
}

/*
dir

	0 left
	1 right
*/
//func collectRelation(e *Expr, dir int) (map[uint64]map[*Expr]bool, map[uint64]map[*Expr]bool) {
//	left := make(map[uint64]map[*Expr]bool)
//	right := make(map[uint64]map[*Expr]bool)
//	switch e.Typ {
//	case ET_Column:
//		if dir <= 0 {
//			set := left[e.ColRef[0]]
//			if set == nil {
//				set = make(map[*Expr]bool)
//			}
//			set[e] = true
//			left[e.ColRef[0]] = set
//		} else {
//			set := right[e.ColRef[0]]
//			if set == nil {
//				set = make(map[*Expr]bool)
//			}
//			set[e] = true
//			right[e.ColRef[0]] = set
//		}
//	case ET_And, ET_Equal, ET_Like, ET_GreaterEqual, ET_Less:
//		for i, child := range e.Children {
//			newDir := 0
//			if i > 0 {
//				newDir = 1
//			}
//			retl, retr := collectRelation(child, newDir)
//			if i == 0 {
//				mergeMap(left, retl)
//			} else {
//				mergeMap(right, retr)
//			}
//		}
//	case ET_Func:
//		for _, child := range e.Children {
//			retl, retr := collectRelation(child, dir)
//			if dir <= 0 {
//				mergeMap(left, retl)
//			} else {
//				mergeMap(right, retr)
//			}
//		}
//	}
//	return left, right
//}

func mergeMap(a, b map[uint64]map[*Expr]bool) {
	for k, v := range b {
		set := a[k]
		if set == nil {
			a[k] = v
		} else {
			mergeSet(set, v)
		}
	}
}

func mergeSet(a, b map[*Expr]bool) {
	for k, v := range b {
		a[k] = v
	}
}

func printRelations(info string, maps map[uint64]map[*Expr]bool) {
	fmt.Println(info)
	for k, v := range maps {
		fmt.Printf("\trelation %d\n", k)
		for e := range v {
			fmt.Printf("\t\t%v\n", e)
		}
	}
}

func copyExpr(e *Expr) *Expr {
	return clone.Clone(e).(*Expr)
}

func (e *Expr) Format(ctx *FormatCtx) {
	if e == nil {
		ctx.Write("")
		return
	}
	switch e.Typ {
	case ET_Column:
		//TODO:
		ctx.Writef("(%s.%s,%s,[%d,%d],%d)", e.Table, e.Name,
			e.DataTyp,
			e.ColRef[0], e.ColRef[1], e.Depth)
	case ET_SConst:
		ctx.Writef("(%s,%s)", e.Svalue, e.DataTyp)
	case ET_IConst:
		ctx.Writef("(%d,%s)", e.Ivalue, e.DataTyp)
	case ET_DateConst:
		ctx.Writef("(%s,%s)", e.Svalue, e.DataTyp)
	case ET_IntervalConst:
		ctx.Writef("(%d %s,%s)", e.Ivalue, e.Svalue, e.DataTyp)
	case ET_BConst:
		ctx.Writef("(%v,%s)", e.Bvalue, e.DataTyp)
	case ET_FConst:
		ctx.Writef("(%v,%s)", e.Fvalue, e.DataTyp)
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
		switch e.SubTyp {
		case ET_Invalid:
			panic("usp invalid expr")
		case ET_Between:
			e.Children[0].Format(ctx)
			ctx.Write(" between ")
			e.Children[1].Format(ctx)
			ctx.Write(" and ")
			e.Children[2].Format(ctx)
		case ET_Case:
			ctx.Write("case ")
			if e.Kase != nil {
				e.Kase.Format(ctx)
				ctx.Writeln()
			}
			for i := 0; i < len(e.When); i += 2 {
				ctx.Write(" when")
				e.When[i].Format(ctx)
				ctx.Write(" then ")
				e.When[i+1].Format(ctx)
				ctx.Writeln()
			}
			if e.Els != nil {
				ctx.Write(" else ")
				e.Els.Format(ctx)
				ctx.Writeln()
			}
			ctx.Write("end")
		case ET_In, ET_NotIn:
			e.Children[0].Format(ctx)
			if e.SubTyp == ET_NotIn {
				ctx.Write(" not in ")
			} else {
				ctx.Write(" in ")
			}

			ctx.Write("(")
			for i := 1; i < len(e.Children); i++ {
				if i > 1 {
					ctx.Write(",")
				}
				e.Format(ctx)
			}
			ctx.Write(")")
		case ET_Exists:
			ctx.Writef("exists(")
			e.Children[0].Format(ctx)
			ctx.Write(")")

		case ET_SubFunc:
			ctx.Writef("%s_%d(", e.Svalue, e.FuncId)
			for idx, e := range e.Children {
				if idx > 0 {
					ctx.Write(", ")
				}
				e.Format(ctx)
			}
			ctx.Write(")")
			ctx.Write("->")
			ctx.Writef("%s", e.DataTyp)
		default:
			//binary operator
			e.Children[0].Format(ctx)
			op := e.SubTyp.String()
			ctx.Writef(" %s ", op)
			e.Children[1].Format(ctx)
		}
	case ET_Subquery:
		ctx.Write("subquery(")
		ctx.AddOffset()
		// e.SubBuilder.Format(ctx)
		ctx.writeString(e.SubBuilder.String())
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

func appendMeta(meta, s string) string {
	return fmt.Sprintf("%s %s", meta, s)
}

func (e *Expr) Print(tree treeprint.Tree, meta string) {
	if e == nil {
		return
	}
	head := appendMeta(meta, e.DataTyp.String())
	switch e.Typ {
	case ET_Column:
		tree.AddMetaNode(head, fmt.Sprintf("(%s.%s,[%d,%d],%d)",
			e.Table, e.Name,
			e.ColRef[0], e.ColRef[1], e.Depth))
	case ET_SConst:
		tree.AddMetaNode(head, fmt.Sprintf("(%s)", e.Svalue))
	case ET_IConst:
		tree.AddMetaNode(head, fmt.Sprintf("(%d)", e.Ivalue))
	case ET_DateConst:
		tree.AddMetaNode(head, fmt.Sprintf("(%s)", e.Svalue))
	case ET_IntervalConst:
		tree.AddMetaNode(head, fmt.Sprintf("(%d %s)", e.Ivalue, e.Svalue))
	case ET_BConst:
		tree.AddMetaNode(head, fmt.Sprintf("(%v)", e.Bvalue))
	case ET_FConst:
		tree.AddMetaNode(head, fmt.Sprintf("(%v)", e.Fvalue))
	case ET_TABLE:
		tree.AddNode(fmt.Sprintf("%s.%s", e.Database, e.Table))
	case ET_Join:
		typStr := ""
		switch e.JoinTyp {
		case ET_JoinTypeCross:
			typStr = "cross"
		case ET_JoinTypeLeft:
			typStr = "left"
		default:
			panic(fmt.Sprintf("usp join type %d", e.JoinTyp))
		}
		branch := tree.AddBranch(typStr)
		e.Children[0].Print(branch, "")
		e.Children[1].Print(branch, "")
	case ET_Func:
		var branch treeprint.Tree
		switch e.SubTyp {
		case ET_Invalid:
			panic("usp invalid expr")
		case ET_Between:
			branch = tree.AddMetaBranch(head, fmt.Sprintf("%s", e.SubTyp))
			e.Children[0].Print(branch, "")
			e.Children[1].Print(branch, "")
			e.Children[2].Print(branch, "")
		case ET_Case:
			branch = tree.AddMetaBranch(head, fmt.Sprintf("%s", e.SubTyp))
			if e.Kase != nil {
				e.Kase.Print(branch, "")
			}
			when := branch.AddBranch("when")
			for i := 0; i < len(e.When); i += 2 {
				e.When[i].Print(when, "")
				e.When[i+1].Print(when, "")
			}
			if e.Els != nil {
				e.Els.Print(branch, "")
			}
		case ET_In, ET_NotIn:
			branch = tree.AddMetaBranch(head, fmt.Sprintf("%s", e.SubTyp))
			for _, e := range e.Children {
				e.Print(branch, "")
			}
		case ET_Exists:
			branch = tree.AddMetaBranch(head, fmt.Sprintf("%s", e.SubTyp))
			e.Children[0].Print(branch, "")
		case ET_SubFunc:
			branch = tree.AddMetaBranch(head, fmt.Sprintf("%s", e.Svalue))
			for _, e := range e.Children {
				e.Print(branch, "")
			}
		default:
			//binary operator
			branch = tree.AddMetaBranch(head, fmt.Sprintf("%s", e.SubTyp))
			e.Children[0].Print(branch, "")
			e.Children[1].Print(branch, "")
		}
	case ET_Subquery:
		branch := tree.AddBranch("subquery(")
		e.SubBuilder.Print(branch)
		branch.AddNode(")")
	case ET_Orderby:
		e.Children[0].Print(tree, meta)

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
			panic(fmt.Errorf("table %s in database %s does not exist", table, db))
		}
	} else {
		panic(fmt.Sprintf("database %s does not exist", db))
	}
	return nil, nil
}

type Stats struct {
	RowCount float64
	ColStats []*BaseStats
}

func (s *Stats) Copy() *Stats {
	ret := &Stats{
		RowCount: s.RowCount,
	}
	ret.ColStats = make([]*BaseStats, len(s.ColStats))
	for i, stat := range s.ColStats {
		ret.ColStats[i] = stat.Copy()
	}
	return ret
}

func (s *Stats) String() string {
	if s == nil {
		return ""
	}
	sb := strings.Builder{}
	sb.WriteString(fmt.Sprintf("rowcount %v\n", s.RowCount))
	for i, stat := range s.ColStats {
		sb.WriteString(fmt.Sprintf("col %v ", i))
		sb.WriteString(stat.String())
		sb.WriteByte('\n')
	}
	return sb.String()
}

type CatalogTable struct {
	Db         string
	Table      string
	Columns    []string
	Types      []ExprDataType
	PK         []int
	Column2Idx map[string]int
	Stats      *Stats
}

func (catalog *CatalogTable) getStats(colId uint64) *BaseStats {
	return catalog.Stats.ColStats[colId]
}

func splitExprByAnd(expr *Expr) []*Expr {
	if expr.Typ == ET_Func {
		if expr.SubTyp == ET_And {
			return append(splitExprByAnd(expr.Children[0]), splitExprByAnd(expr.Children[1])...)
		} else if expr.SubTyp == ET_In || expr.SubTyp == ET_NotIn {
			ret := make([]*Expr, 0)
			for i, child := range expr.Children {
				if i == 0 {
					continue
				}
				ret = append(ret, &Expr{
					Typ:      expr.Typ,
					SubTyp:   expr.SubTyp,
					Svalue:   expr.SubTyp.String(),
					Children: []*Expr{expr.Children[0], child},
				})
			}
		}
	}
	return []*Expr{expr}
}

func splitExprsByAnd(exprs []*Expr) []*Expr {
	ret := make([]*Expr, 0)
	for _, e := range exprs {
		if e == nil {
			continue
		}
		ret = append(ret, splitExprByAnd(e)...)
	}
	return ret
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

	case ET_Func:
		switch expr.SubTyp {
		case ET_And, ET_Equal, ET_Like, ET_GreaterEqual, ET_Less, ET_NotEqual:
			left, leftHasCorr := deceaseDepth(expr.Children[0])
			hasCorCol = hasCorCol || leftHasCorr
			right, rightHasCorr := deceaseDepth(expr.Children[1])
			hasCorCol = hasCorCol || rightHasCorr
			return &Expr{
				Typ:      expr.Typ,
				SubTyp:   expr.SubTyp,
				Svalue:   expr.SubTyp.String(),
				DataTyp:  expr.DataTyp,
				Children: []*Expr{left, right},
			}, hasCorCol
		case ET_SubFunc:
			args := make([]*Expr, 0, len(expr.Children))
			for _, child := range expr.Children {
				newChild, yes := deceaseDepth(child)
				hasCorCol = hasCorCol || yes
				args = append(args, newChild)
			}
			return &Expr{
				Typ:      expr.Typ,
				SubTyp:   expr.SubTyp,
				Svalue:   expr.Svalue,
				FuncId:   expr.FuncId,
				DataTyp:  expr.DataTyp,
				Children: args,
			}, hasCorCol
		default:
			panic(fmt.Sprintf("usp %v", expr.SubTyp))
		}
	default:
		panic(fmt.Sprintf("usp %v", expr.Typ))
	}
}

func replaceColRef(e *Expr, bind, newBind ColumnBind) *Expr {
	if e == nil {
		return nil
	}
	switch e.Typ {
	case ET_Column:
		if bind == e.ColRef {
			e.ColRef = newBind
		}

	case ET_SConst, ET_IConst, ET_DateConst, ET_IntervalConst, ET_BConst, ET_FConst:
	case ET_Func:
	case ET_Orderby:
	default:
		panic("usp")
	}
	for i, child := range e.Children {
		e.Children[i] = replaceColRef(child, bind, newBind)
	}
	return e
}

func collectColRefs(e *Expr, set ColumnBindSet) {
	if e == nil {
		return
	}
	switch e.Typ {
	case ET_Column:
		set.insert(e.ColRef)

	case ET_Func:
		switch e.SubTyp {
		case ET_Case:
			collectColRefs(e.Kase, set)
			for _, expr := range e.When {
				collectColRefs(expr, set)
			}
			collectColRefs(e.Els, set)
		default:
		}
	case ET_SConst, ET_IConst, ET_DateConst, ET_IntervalConst, ET_BConst, ET_FConst:
	case ET_Orderby:
	default:
		panic("usp")
	}
	for _, child := range e.Children {
		collectColRefs(child, set)
	}
}
