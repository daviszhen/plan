package common

import (
	"bytes"
	"fmt"
	"math"
	"time"
	"unsafe"

	decimal2 "github.com/govalues/decimal"

	"github.com/daviszhen/plan/pkg/util"
)

type LTypeId int

const (
	LTID_INVALID         LTypeId = 0
	LTID_NULL            LTypeId = 1
	LTID_UNKNOWN         LTypeId = 2
	LTID_ANY             LTypeId = 3
	LTID_USER            LTypeId = 4
	LTID_BOOLEAN         LTypeId = 10
	LTID_TINYINT         LTypeId = 11
	LTID_SMALLINT        LTypeId = 12
	LTID_INTEGER         LTypeId = 13
	LTID_BIGINT          LTypeId = 14
	LTID_DATE            LTypeId = 15
	LTID_TIME            LTypeId = 16
	LTID_TIMESTAMP_SEC   LTypeId = 17
	LTID_TIMESTAMP_MS    LTypeId = 18
	LTID_TIMESTAMP       LTypeId = 19
	LTID_TIMESTAMP_NS    LTypeId = 20
	LTID_DECIMAL         LTypeId = 21
	LTID_FLOAT           LTypeId = 22
	LTID_DOUBLE          LTypeId = 23
	LTID_CHAR            LTypeId = 24
	LTID_VARCHAR         LTypeId = 25
	LTID_BLOB            LTypeId = 26
	LTID_INTERVAL        LTypeId = 27
	LTID_UTINYINT        LTypeId = 28
	LTID_USMALLINT       LTypeId = 29
	LTID_UINTEGER        LTypeId = 30
	LTID_UBIGINT         LTypeId = 31
	LTID_TIMESTAMP_TZ    LTypeId = 32
	LTID_TIME_TZ         LTypeId = 34
	LTID_BIT             LTypeId = 36
	LTID_HUGEINT         LTypeId = 50
	LTID_POINTER         LTypeId = 51
	LTID_VALIDITY        LTypeId = 53
	LTID_UUID            LTypeId = 54
	LTID_STRUCT          LTypeId = 100
	LTID_LIST            LTypeId = 101
	LTID_MAP             LTypeId = 102
	LTID_TABLE           LTypeId = 103
	LTID_ENUM            LTypeId = 104
	LTID_AGGREGATE_STATE LTypeId = 105
	LTID_LAMBDA          LTypeId = 106
	LTID_UNION           LTypeId = 107
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

type Hugeint struct {
	Lower uint64
	Upper int64
}

func (h Hugeint) String() string {
	return fmt.Sprintf("[%d %d]", h.Upper, h.Lower)
}

func (h *Hugeint) Equal(o *Hugeint) bool {
	return h.Lower == o.Lower && h.Upper == o.Upper
}

func NegateHugeint(input *Hugeint, result *Hugeint) {
	if input.Upper == math.MinInt64 && input.Lower == 0 {
		panic("-hugeint overflow")
	}
	result.Lower = math.MaxUint64 - input.Lower + 1
	if input.Lower == 0 {
		result.Upper = -1 - input.Upper + 1
	} else {
		result.Upper = -1 - input.Upper
	}
}

// addInplace
// return
//
//	false : overflow
func AddInplace(lhs, rhs *Hugeint) bool {
	ladd := lhs.Lower + rhs.Lower
	overflow := int64(0)
	if ladd < lhs.Lower {
		overflow = 1
	}
	if rhs.Upper >= 0 {
		//rhs is positive
		if lhs.Upper > (math.MaxInt64 - rhs.Upper - overflow) {
			return false
		}
		lhs.Upper = lhs.Upper + overflow + rhs.Upper
	} else {
		//rhs is negative
		if lhs.Upper < (math.MinInt64 - rhs.Upper - overflow) {
			return false
		}
		lhs.Upper = lhs.Upper + (overflow + rhs.Upper)
	}
	lhs.Lower += rhs.Lower
	if lhs.Upper == math.MinInt64 && lhs.Lower == 0 {
		return false
	}
	return true
}

func (h *Hugeint) Add(lhs, rhs *Hugeint) {
	if !AddInplace(lhs, rhs) {
		panic("hugint and overflow")
	}
}
func (h *Hugeint) Mul(lhs, rhs *Hugeint) {}

func (h *Hugeint) Less(lhs, rhs *Hugeint) bool {
	panic("usp")
}
func (h *Hugeint) Greater(lhs, rhs *Hugeint) bool {
	panic("usp")
}

//lint:ignore U1000
type Interval struct {
	Months int32
	Days   int32
	Micros int32

	Unit string
	Year int32
}

func (i *Interval) Equal(o *Interval) bool {
	return i.Months == o.Months &&
		i.Days == o.Days &&
		i.Micros == o.Micros
}

func (i *Interval) Less(o *Interval) bool {
	panic("usp")
}

func (i *Interval) Milli() int64 {
	switch i.Unit {
	case "year":
		d := time.Date(
			int(1970+i.Year),
			1,
			1,
			0, 0, 0, 0, time.UTC)
		return d.UnixMilli()
	case "month":
		d := time.Date(
			1970,
			time.Month(1+i.Months),
			1,
			0, 0, 0, 0, time.UTC)
		return d.UnixMilli()
	case "day":
		d := time.Date(
			1970,
			1,
			int(1+i.Days),
			0, 0, 0, 0, time.UTC)
		return d.UnixMilli()
	default:
		panic("usp")
	}
}

//lint:ignore U1000
type Date struct {
	Year  int32
	Month int32
	Day   int32
}

func (d *Date) Equal(o *Date) bool {
	return d.Year == o.Year && d.Month == o.Month && d.Day == o.Day
}

func (d *Date) Less(o *Date) bool {
	d1 := d.ToDate()
	o1 := o.ToDate()
	return d1.Before(o1)
}

func (d *Date) ToDate() time.Time {
	return time.Date(int(d.Year), time.Month(d.Month), int(d.Day), 0, 0, 0, 0, time.UTC)
}

func (d *Date) AddInterval(rhs *Interval) Date {
	lhsD := d.ToDate()
	resD := lhsD.AddDate(int(rhs.Year), int(rhs.Months), int(rhs.Days))
	y, m, day := resD.Date()
	return Date{
		Year:  int32(y),
		Month: int32(m),
		Day:   int32(day),
	}
}

func (d *Date) SubInterval(rhs *Interval) Date {
	lhsD := d.ToDate()
	resD := lhsD.AddDate(int(-rhs.Year), int(-rhs.Months), int(-rhs.Days))
	y, m, day := resD.Date()
	return Date{
		Year:  int32(y),
		Month: int32(m),
		Day:   int32(day),
	}
}

type String struct {
	Len  int
	Data unsafe.Pointer
}

func (s *String) DataSlice() []byte {
	return util.PointerToSlice[byte](s.Data, s.Len)
}

func (s *String) DataPtr() unsafe.Pointer {
	return s.Data
}
func (s *String) String() string {
	t := s.DataSlice()
	return string(t)
}

func (s *String) Equal(o *String) bool {
	if s.Len != o.Len {
		return false
	}
	sSlice := util.PointerToSlice[byte](s.Data, s.Len)
	oSlice := util.PointerToSlice[byte](o.Data, o.Len)
	return bytes.Equal(sSlice, oSlice)
}

func (s *String) Less(o *String) bool {
	sSlice := util.PointerToSlice[byte](s.Data, s.Len)
	oSlice := util.PointerToSlice[byte](o.Data, o.Len)
	return bytes.Compare(sSlice, oSlice) < 0
}

func (s *String) Length() int {
	return s.Len
}

func (s String) NullLen() int {
	return 0
}

type Decimal struct {
	decimal2.Decimal
}

func (dec *Decimal) Equal(o *Decimal) bool {
	return dec.Decimal.Cmp(o.Decimal) == 0
}

func (dec *Decimal) String() string {
	return dec.Decimal.String()
}

func (dec *Decimal) Add(lhs *Decimal, rhs *Decimal) {
	res, err := lhs.Decimal.Add(rhs.Decimal)
	if err != nil {
		panic(err)
	}
	lhs.Decimal = res
}

func (dec *Decimal) Mul(lhs *Decimal, rhs *Decimal) {
	res, err := lhs.Decimal.Mul(rhs.Decimal)
	if err != nil {
		panic(err)
	}
	lhs.Decimal = res
}

func (dec *Decimal) Less(lhs, rhs *Decimal) bool {
	return lhs.Decimal.Cmp(rhs.Decimal) < 0
}

func (dec *Decimal) Greater(lhs, rhs *Decimal) bool {
	return lhs.Decimal.Cmp(rhs.Decimal) > 0
}

func NegateDecimal(input *Decimal, result *Decimal) {
	result.Decimal = input.Decimal.Neg()
}

type LType struct {
	Id    LTypeId
	PTyp  PhyType
	Width int
	Scale int
}

func Numeric() []LType {
	typs := []LTypeId{
		LTID_TINYINT, LTID_SMALLINT, LTID_INTEGER,
		LTID_BIGINT, LTID_HUGEINT, LTID_FLOAT,
		LTID_DOUBLE, LTID_DECIMAL, LTID_UTINYINT,
		LTID_USMALLINT, LTID_UINTEGER, LTID_UBIGINT,
	}
	ret := make([]LType, len(typs))
	for i, typ := range typs {
		ret[i].Id = typ
		ret[i].PTyp = ret[i].GetInternalType()
	}
	return ret
}

func (lt LType) Serialize(serial util.Serialize) error {
	err := util.Write[int](int(lt.Id), serial)
	if err != nil {
		return err
	}
	err = util.Write[int](lt.Width, serial)
	if err != nil {
		return err
	}
	err = util.Write[int](lt.Scale, serial)
	if err != nil {
		return err
	}
	return err
}

func DeserializeLType(deserial util.Deserialize) (LType, error) {
	id := 0
	width := 0
	scale := 0
	err := util.Read[int](&id, deserial)
	if err != nil {
		return LType{}, err
	}
	err = util.Read[int](&width, deserial)
	if err != nil {
		return LType{}, err
	}
	err = util.Read[int](&scale, deserial)
	if err != nil {
		return LType{}, err
	}

	ret := LType{
		Id:    LTypeId(id),
		Width: width,
		Scale: scale,
	}
	ret.PTyp = ret.GetInternalType()
	return ret, err
}

func MakeLType(id LTypeId) LType {
	ret := LType{Id: id}
	ret.PTyp = ret.GetInternalType()
	return ret
}

func Null() LType {
	return MakeLType(LTID_NULL)
}
func DecimalType(width, scale int) LType {
	ret := MakeLType(LTID_DECIMAL)
	ret.Width = width
	ret.Scale = scale
	return ret
}

func HugeintType() LType {
	return MakeLType(LTID_HUGEINT)
}

func BigintType() LType {
	return MakeLType(LTID_BIGINT)
}

func IntegerType() LType {
	return MakeLType(LTID_INTEGER)
}

func HashType() LType {
	return MakeLType(LTID_UBIGINT)
}

func FloatType() LType {
	return MakeLType(LTID_FLOAT)
}

func DoubleType() LType {
	return MakeLType(LTID_DOUBLE)
}

func TinyintType() LType {
	return MakeLType(LTID_TINYINT)
}

func SmallintType() LType {
	return MakeLType(LTID_SMALLINT)
}

func VarcharType() LType {
	return MakeLType(LTID_VARCHAR)
}
func VarcharType2(width int) LType {
	ret := MakeLType(LTID_VARCHAR)
	ret.Width = width
	return ret
}

func DateType() LType {
	return MakeLType(LTID_DATE)
}

func TimeType() LType {
	return MakeLType(LTID_TIME)
}

func TimestampType() LType {
	return MakeLType(LTID_TIMESTAMP)
}

func BooleanType() LType {
	return MakeLType(LTID_BOOLEAN)
}

func IntervalType() LType {
	return MakeLType(LTID_INTERVAL)
}

func PointerType() LType {
	return MakeLType(LTID_POINTER)
}

func UbigintType() LType {
	return MakeLType(LTID_UBIGINT)
}

func CopyLTypes(typs ...LType) []LType {
	ret := make([]LType, 0)
	ret = append(ret, typs...)
	return ret
}

var Numerics = map[LTypeId]int{
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

func (lt LType) IsDate() bool {
	return lt.Id == LTID_DATE
}

func (lt LType) IsInterval() bool {
	return lt.Id == LTID_INTERVAL
}

func (lt LType) IsNumeric() bool {
	if _, has := Numerics[lt.Id]; has {
		return true
	}
	return false
}

var Integrals = map[LTypeId]int{
	LTID_TINYINT:   0,
	LTID_SMALLINT:  0,
	LTID_INTEGER:   0,
	LTID_BIGINT:    0,
	LTID_UTINYINT:  0,
	LTID_USMALLINT: 0,
	LTID_UINTEGER:  0,
	LTID_UBIGINT:   0,
	LTID_HUGEINT:   0,
}

func (lt LType) IsIntegral() bool {
	if _, has := Integrals[lt.Id]; has {
		return true
	}
	return false
}

func (lt LType) IsPointer() bool {
	return lt.Id == LTID_POINTER
}

func (lt LType) GetDecimalSize() (bool, int, int) {
	switch lt.Id {
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
		return true, lt.Width, lt.Scale
	default:
		return false, 0, 0
	}
}

func (lt LType) Equal(o LType) bool {
	if lt.Id != o.Id {
		return false
	}
	switch lt.Id {
	case LTID_DECIMAL:
		return lt.Width == o.Width && lt.Scale == o.Scale
	default:

	}
	return true
}

func (lt LType) GetInternalType() PhyType {
	switch lt.Id {
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
	case LTID_NULL, LTID_INTEGER:
		return INT32
	case LTID_DATE:
		return DATE
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
		//if lt.Width <= DecimalMaxWidthInt16 {
		//	return INT16
		//} else if lt.Width <= DecimalMaxWidthInt32 {
		//	return INT32
		//} else if lt.Width <= DecimalMaxWidthInt64 {
		//	return INT64
		//} else if lt.Width <= DecimalMaxWidthInt128 {
		//	return INT128
		//} else {

		//}
		return DECIMAL
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
	//return fmt.Sprintf("(%v %v %d %d)", lt.Id, lt.PTyp, lt.Width, lt.Scale)
	if lt.Id == LTID_DECIMAL {
		return fmt.Sprintf("%v(%d,%d)", lt.PTyp, lt.Width, lt.Scale)
	}
	return fmt.Sprintf("%v", lt.PTyp)
}

func TargetTypeCost(typ LType) int64 {
	switch typ.Id {
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
}

var TinyintTo = map[LTypeId]int{
	LTID_SMALLINT: 0,
	LTID_INTEGER:  0,
	LTID_BIGINT:   0,
	LTID_HUGEINT:  0,
	LTID_FLOAT:    0,
	LTID_DOUBLE:   0,
	LTID_DECIMAL:  0,
}

func ImplicitCastTinyint(to LType) int64 {
	if _, has := TinyintTo[to.Id]; has {
		return TargetTypeCost(to)
	}
	return -1
}

var UtinyintTo = map[LTypeId]int{
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

func ImplicitCastUTinyint(to LType) int64 {
	if _, has := UtinyintTo[to.Id]; has {
		return TargetTypeCost(to)
	}
	return -1
}

var SmallintTo = map[LTypeId]int{
	LTID_INTEGER: 0,
	LTID_BIGINT:  0,
	LTID_HUGEINT: 0,
	LTID_FLOAT:   0,
	LTID_DOUBLE:  0,
	LTID_DECIMAL: 0,
}

func ImplicitCastSmallint(to LType) int64 {
	if _, has := SmallintTo[to.Id]; has {
		return TargetTypeCost(to)
	}
	return -1
}

var UsmallintTo = map[LTypeId]int{
	LTID_UINTEGER: 0,
	LTID_UBIGINT:  0,
	LTID_INTEGER:  0,
	LTID_BIGINT:   0,
	LTID_HUGEINT:  0,
	LTID_FLOAT:    0,
	LTID_DOUBLE:   0,
	LTID_DECIMAL:  0,
}

func ImplicitCastUSmallint(to LType) int64 {
	if _, has := UsmallintTo[to.Id]; has {
		return TargetTypeCost(to)
	}
	return -1
}

var HugeintTo = map[LTypeId]int{
	LTID_FLOAT:   0,
	LTID_DOUBLE:  0,
	LTID_DECIMAL: 0,
}

func ImplicitCastHugeint(to LType) int64 {
	if _, has := HugeintTo[to.Id]; has {
		return TargetTypeCost(to)
	}
	return -1
}

var FloatTo = map[LTypeId]int{
	LTID_DOUBLE: 0,
}

func ImplicitCastFloat(to LType) int64 {
	if _, has := FloatTo[to.Id]; has {
		return TargetTypeCost(to)
	}
	return -1
}

var DoubleTo = map[LTypeId]int{}

func ImplicitCastDouble(to LType) int64 {
	if _, has := DoubleTo[to.Id]; has {
		return TargetTypeCost(to)
	}
	return -1
}

var DateTo = map[LTypeId]int{
	LTID_TIMESTAMP: 0,
}

func ImplicitCastDate(to LType) int64 {
	if _, has := DateTo[to.Id]; has {
		return TargetTypeCost(to)
	}
	return -1
}

var DecimalTo = map[LTypeId]int{
	LTID_FLOAT:  0,
	LTID_DOUBLE: 0,
}

func ImplicitCastDecimal(to LType) int64 {
	if _, has := DecimalTo[to.Id]; has {
		return TargetTypeCost(to)
	}
	return -1
}

var IntegerTo = map[LTypeId]int{
	LTID_BIGINT:  0,
	LTID_HUGEINT: 0,
	LTID_FLOAT:   0,
	LTID_DOUBLE:  0,
	LTID_DECIMAL: 0,
}

func ImplicitCastInteger(to LType) int64 {
	if _, has := IntegerTo[to.Id]; has {
		return TargetTypeCost(to)
	}
	return -1
}

var UintTo = map[LTypeId]int{
	LTID_UBIGINT: 0,
	LTID_BIGINT:  0,
	LTID_HUGEINT: 0,
	LTID_FLOAT:   0,
	LTID_DOUBLE:  0,
	LTID_DECIMAL: 0,
}

func ImplicitCastUInteger(to LType) int64 {
	if _, has := UintTo[to.Id]; has {
		return TargetTypeCost(to)
	}
	return -1
}

var BigintTo = map[LTypeId]int{
	LTID_FLOAT:   0,
	LTID_DOUBLE:  0,
	LTID_HUGEINT: 0,
	LTID_DECIMAL: 0,
}

func ImplicitCastBigint(to LType) int64 {
	if _, has := BigintTo[to.Id]; has {
		return TargetTypeCost(to)
	}
	return -1
}

var UbigintTo = map[LTypeId]int{
	LTID_FLOAT:   0,
	LTID_DOUBLE:  0,
	LTID_HUGEINT: 0,
	LTID_DECIMAL: 0,
}

func ImplicitCastUBigint(to LType) int64 {
	if _, has := UbigintTo[to.Id]; has {
		return TargetTypeCost(to)
	}
	return -1
}

func ImplicitCast(from, to LType) int64 {
	if from.Id == LTID_NULL {
		//cast NULL to anything
		return TargetTypeCost(to)
	}
	if from.Id == LTID_UNKNOWN {
		//parameter expr cast no cost
		return 0
	}
	if to.Id == LTID_ANY {
		//cast to any
		return 1
	}
	if from.Id == to.Id {
		//same. no cost
		return 0
	}
	if to.Id == LTID_VARCHAR {
		//anything cast to varchar
		return TargetTypeCost(to)
	}
	switch from.Id {
	case LTID_TINYINT:
		return ImplicitCastTinyint(to)
	case LTID_SMALLINT:
		return ImplicitCastSmallint(to)
	case LTID_INTEGER:
		return ImplicitCastInteger(to)
	case LTID_BIGINT:
		return ImplicitCastBigint(to)
	case LTID_UTINYINT:
		return ImplicitCastUTinyint(to)
	case LTID_USMALLINT:
		return ImplicitCastUSmallint(to)
	case LTID_UINTEGER:
		return ImplicitCastUInteger(to)
	case LTID_UBIGINT:
		return ImplicitCastUBigint(to)
	case LTID_HUGEINT:
		return ImplicitCastHugeint(to)
	case LTID_FLOAT:
		return ImplicitCastFloat(to)
	case LTID_DOUBLE:
		return ImplicitCastDouble(to)
	case LTID_DATE:
		return ImplicitCastDate(to)
	case LTID_DECIMAL:
		return ImplicitCastDecimal(to)
	default:
		return -1
	}
}

func DecimalSizeCheck(left, right LType) LType {
	if left.Id != LTID_DECIMAL && right.Id != LTID_DECIMAL {
		panic("wrong type")
	}
	if left.Id == right.Id {
		panic("wrong type")
	}

	if left.Id == LTID_DECIMAL {
		return DecimalSizeCheck(right, left)
	}

	can, owith, _ := left.GetDecimalSize()
	if !can {
		panic(fmt.Sprintf("to decimal failed. %v ", left))
	}
	ewidth := right.Width - right.Scale
	if owith > ewidth {
		newWidth := owith + right.Scale
		newWidth = min(newWidth, DecimalMaxWidth)
		return DecimalType(newWidth, right.Scale)
	}
	return right
}

func DecideNumericType(left, right LType) LType {
	if left.Id > right.Id {
		return DecideNumericType(right, left)
	}

	if ImplicitCast(left, right) >= 0 {
		if right.Id == LTID_DECIMAL {
			return DecimalSizeCheck(left, right)
		}
		return right
	}

	if ImplicitCast(right, left) >= 0 {
		if left.Id == LTID_DECIMAL {
			return DecimalSizeCheck(right, left)
		}
		return left
	}
	//types that can not be cast implicitly.
	//they are different.
	//left is signed and right is unsigned.
	//upcast
	if left.Id == LTID_BIGINT || right.Id == LTID_UBIGINT {
		return HugeintType()
	}
	if left.Id == LTID_INTEGER || right.Id == LTID_UINTEGER {
		return BigintType()
	}
	if left.Id == LTID_SMALLINT || right.Id == LTID_USMALLINT {
		return IntegerType()
	}
	if left.Id == LTID_TINYINT || right.Id == LTID_UTINYINT {
		return SmallintType()
	}
	panic(fmt.Sprintf("imcompatible %v %v", left, right))
}

func MaxLType(left, right LType) LType {
	//digit type
	if left.Id != right.Id &&
		left.IsNumeric() && right.IsNumeric() {
		return DecideNumericType(left, right)
	} else if left.Id == LTID_UNKNOWN {
		return right
	} else if right.Id == LTID_UNKNOWN {
		return left
	} else if left.Id < right.Id {
		if left.Id == LTID_DATE && right.Id == LTID_INTERVAL {
			return left
		} else if left.Id == LTID_INTERVAL && right.Id == LTID_DATE {
			return right
		}
		return right
	}
	if right.Id < left.Id {
		return left
	}
	id := left.Id
	if id == LTID_ENUM {
		if left.Equal(right) {
			return left
		} else {
			//enum cast to varchar
			return VarcharType()
		}
	}
	if id == LTID_VARCHAR {
		//no collation here
		return right
	}
	if id == LTID_DECIMAL {
		//decide the Width & scal of the final deciaml
		leftNum := left.Width - left.Scale
		rightNum := right.Width - right.Scale
		num := max(leftNum, rightNum)
		scale := max(left.Scale, right.Scale)
		width := num + scale
		if width > DecimalMaxWidth {
			width = DecimalMaxWidth
			scale = width - num
		}
		return DecimalType(width, scale)
	}
	//same
	return left
}

func WriteString(val String, serial util.Serialize) error {
	err := util.Write[uint32](uint32(val.Length()), serial)
	if err != nil {
		return err
	}

	if val.Length() > 0 {
		err = serial.WriteData(val.DataSlice(), val.Length())
		if err != nil {
			return err
		}
	}
	return err
}

func ReadString(val *String, deserial util.Deserialize) error {
	var len uint32
	err := util.Read[uint32](&len, deserial)
	if err != nil {
		return err
	}
	if len > 0 {
		val.Data = util.CMalloc(int(len))
		val.Len = int(len)
		err = deserial.ReadData(val.DataSlice(), val.Length())
		if err != nil {
			return err
		}
	}
	return err
}

const (
	DecimalMaxWidthInt16  = 4
	DecimalMaxWidthInt32  = 9
	DecimalMaxWidthInt64  = 18
	DecimalMaxWidthInt128 = 38
	DecimalMaxWidth       = DecimalMaxWidthInt128
)
