package common

import "fmt"

type PhyType int

const (
	NA       PhyType = 0
	BOOL     PhyType = 1
	UINT8    PhyType = 2
	INT8     PhyType = 3
	UINT16   PhyType = 4
	INT16    PhyType = 5
	UINT32   PhyType = 6
	INT32    PhyType = 7
	UINT64   PhyType = 8
	INT64    PhyType = 9
	FLOAT    PhyType = 11
	DOUBLE   PhyType = 12
	INTERVAL PhyType = 21
	LIST     PhyType = 23
	STRUCT   PhyType = 24
	VARCHAR  PhyType = 200
	INT128   PhyType = 204
	UNKNOWN  PhyType = 205
	BIT      PhyType = 206
	DATE     PhyType = 207
	POINTER  PhyType = 208
	DECIMAL  PhyType = 209

	INVALID PhyType = 255
)

var pTypeToStr = map[PhyType]string{
	NA:       "NA",
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
	DATE:     "DATE",
	POINTER:  "POINTER",
	DECIMAL:  "DECIMAL",
	INVALID:  "INVALID",
}

func (pt PhyType) String() string {
	if s, has := pTypeToStr[pt]; has {
		return s
	}
	panic(fmt.Sprintf("usp %d", pt))
}

func (pt PhyType) Size() int {
	switch pt {
	case BIT:
		return BoolSize
	case BOOL:
		return BoolSize
	case INT8:
		return Int8Size
	case INT16:
		return Int16Size
	case INT32:
		return Int32Size
	case INT64:
		return Int64Size
	case UINT8:
		return Int8Size
	case UINT16:
		return Int16Size
	case UINT32:
		return Int32Size
	case UINT64:
		return Int64Size
	case INT128:
		return Int128Size
	case FLOAT:
		return Float32Size
	case DOUBLE:
		return Int64Size
	case VARCHAR:
		return VarcharSize
	case INTERVAL:
		return IntervalSize
	case STRUCT:
	case UNKNOWN:
		return 0
	case LIST:
		panic("usp")
	case DATE:
		return DateSize
	case POINTER:
		return PointerSize
	case DECIMAL:
		return DecimalSize
	default:
		panic("usp")
	}
	panic("usp")
}

func (pt PhyType) IsConstant() bool {
	return pt >= BOOL && pt <= DOUBLE ||
		pt == INTERVAL ||
		pt == INT128 ||
		pt == DATE ||
		pt == POINTER ||
		pt == DECIMAL
}

func (pt PhyType) IsVarchar() bool {
	return pt == VARCHAR
}
