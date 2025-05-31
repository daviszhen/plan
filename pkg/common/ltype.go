package common

import (
	"fmt"

	"github.com/daviszhen/plan/pkg/util"
)

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

const (
	DecimalMaxWidthInt16  = 4
	DecimalMaxWidthInt32  = 9
	DecimalMaxWidthInt64  = 18
	DecimalMaxWidthInt128 = 38
	DecimalMaxWidth       = DecimalMaxWidthInt128
)
