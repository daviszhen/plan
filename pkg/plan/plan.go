// Copyright 2023-2024 daviszhen
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"bytes"
	"fmt"
	"math"
	"math/rand/v2"
	"reflect"
	"strings"
	"time"
	"unsafe"

	dec "github.com/govalues/decimal"
	"github.com/huandu/go-clone"
	"github.com/xlab/treeprint"
)

type String struct {
	_len  int
	_data unsafe.Pointer
}

func (s *String) dataSlice() []byte {
	return pointerToSlice[byte](s._data, s._len)
}

func (s *String) data() unsafe.Pointer {
	return s._data
}
func (s *String) String() string {
	t := s.dataSlice()
	return string(t)
}

func (s *String) equal(o *String) bool {
	if s._len != o._len {
		return false
	}
	sSlice := pointerToSlice[byte](s._data, s._len)
	oSlice := pointerToSlice[byte](o._data, o._len)
	return bytes.Equal(sSlice, oSlice)
}

func (s *String) less(o *String) bool {
	sSlice := pointerToSlice[byte](s._data, s._len)
	oSlice := pointerToSlice[byte](o._data, o._len)
	return bytes.Compare(sSlice, oSlice) < 0
}

// WildcardMatch implements wildcard pattern match algorithm.
// pattern and target are ascii characters
// TODO: add \_ and \%
func WildcardMatch(pattern, target *String) bool {
	var p = 0
	var t = 0
	var positionOfPercentPlusOne int = -1
	var positionOfTargetEncounterPercent int = -1
	plen := pattern.len()
	tlen := target.len()
	pSlice := pattern.dataSlice()
	tSlice := target.dataSlice()
	for t < tlen {
		//%
		if p < plen && pSlice[p] == '%' {
			p++
			positionOfPercentPlusOne = p
			if p >= plen {
				//pattern end with %
				return true
			}
			//means % matches empty
			positionOfTargetEncounterPercent = t
		} else if p < plen && (pSlice[p] == '_' || pSlice[p] == tSlice[t]) { //match or _
			p++
			t++
		} else {
			if positionOfPercentPlusOne == -1 {
				//have not matched a %
				return false
			}
			if positionOfTargetEncounterPercent == -1 {
				return false
			}
			//backtrace to last % position + 1
			p = positionOfPercentPlusOne
			//means % matches multiple characters
			positionOfTargetEncounterPercent++
			t = positionOfTargetEncounterPercent
		}
	}
	//skip %
	for p < plen && pSlice[p] == '%' {
		p++
	}
	return p >= plen
}

func (s *String) len() int {
	return s._len
}

func (s String) nullLen() int {
	return 0
}

//lint:ignore U1000
type Date struct {
	_year  int32
	_month int32
	_day   int32
}

func (d *Date) equal(o *Date) bool {
	return d._year == o._year && d._month == o._month && d._day == o._day
}

func (d *Date) less(o *Date) bool {
	d1 := d.toDate()
	o1 := o.toDate()
	return d1.Before(o1)
}

func (d *Date) toDate() time.Time {
	return time.Date(int(d._year), time.Month(d._month), int(d._day), 0, 0, 0, 0, time.UTC)
}

func (d *Date) addInterval(rhs *Interval) Date {
	lhsD := d.toDate()
	resD := lhsD.AddDate(int(rhs._year), int(rhs._months), int(rhs._days))
	y, m, day := resD.Date()
	return Date{
		_year:  int32(y),
		_month: int32(m),
		_day:   int32(day),
	}
}

func (d *Date) subInterval(rhs *Interval) Date {
	lhsD := d.toDate()
	resD := lhsD.AddDate(int(-rhs._year), int(-rhs._months), int(-rhs._days))
	y, m, day := resD.Date()
	return Date{
		_year:  int32(y),
		_month: int32(m),
		_day:   int32(day),
	}
}

//lint:ignore U1000
type Interval struct {
	_months int32
	_days   int32
	_micros int32

	_unit string
	_year int32
}

func (i *Interval) equal(o *Interval) bool {
	return i._months == o._months &&
		i._days == o._days &&
		i._micros == o._micros
}

func (i *Interval) less(o *Interval) bool {
	panic("usp")
}

func (i *Interval) milli() int64 {
	switch i._unit {
	case "year":
		d := time.Date(
			int(1970+i._year),
			1,
			1,
			0, 0, 0, 0, time.UTC)
		return d.UnixMilli()
	case "month":
		d := time.Date(
			1970,
			time.Month(1+i._months),
			1,
			0, 0, 0, 0, time.UTC)
		return d.UnixMilli()
	case "day":
		d := time.Date(
			1970,
			1,
			int(1+i._days),
			0, 0, 0, 0, time.UTC)
		return d.UnixMilli()
	default:
		panic("usp")
	}
}

type Hugeint struct {
	_lower uint64
	_upper int64
}

func (h Hugeint) String() string {
	return fmt.Sprintf("[%d %d]", h._upper, h._lower)
}

// addInplace
// return
//
//	false : overflow
func addInplace(lhs, rhs *Hugeint) bool {
	ladd := lhs._lower + rhs._lower
	overflow := int64(0)
	if ladd < lhs._lower {
		overflow = 1
	}
	if rhs._upper >= 0 {
		//rhs is positive
		if lhs._upper > (math.MaxInt64 - rhs._upper - overflow) {
			return false
		}
		lhs._upper = lhs._upper + overflow + rhs._upper
	} else {
		//rhs is negative
		if lhs._upper < (math.MinInt64 - rhs._upper - overflow) {
			return false
		}
		lhs._upper = lhs._upper + (overflow + rhs._upper)
	}
	lhs._lower += rhs._lower
	if lhs._upper == math.MinInt64 && lhs._lower == 0 {
		return false
	}
	return true
}

func (h *Hugeint) Add(lhs, rhs *Hugeint) {
	if !addInplace(lhs, rhs) {
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

var _ TypeOp[Decimal] = new(Decimal)

type Decimal struct {
	dec.Decimal
}

func (dec *Decimal) equal(o *Decimal) bool {
	d, err := dec.Decimal.Sub(o.Decimal)
	if err != nil {
		panic(err)
	}
	return d.IsZero()
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
	d, err := lhs.Decimal.Sub(rhs.Decimal)
	if err != nil {
		panic("decimal sub failed")
	}
	return d.IsNeg()
}

func (dec *Decimal) Greater(lhs, rhs *Decimal) bool {
	d, err := lhs.Decimal.Sub(rhs.Decimal)
	if err != nil {
		panic("decimal sub failed")
	}
	return d.IsPos()
}

type ScatterOp[T any] interface {
	nullValue() T
	randValue() T
	store(src T, rowLoc unsafe.Pointer, offsetInRow int, heapLoc *unsafe.Pointer)
}

type int8ScatterOp struct {
}

func (scatter int8ScatterOp) nullValue() int8 {
	return 0
}

func (scatter int8ScatterOp) randValue() int8 {
	return int8(rand.Int32())
}

func (scatter int8ScatterOp) store(src int8, rowLoc unsafe.Pointer, offsetInRow int, heapLoc *unsafe.Pointer) {
	store[int8](src, pointerAdd(rowLoc, offsetInRow))
}

type int32ScatterOp struct {
}

func (scatter int32ScatterOp) nullValue() int32 {
	return 0
}

func (scatter int32ScatterOp) randValue() int32 {
	return rand.Int32()
}

func (scatter int32ScatterOp) store(src int32, rowLoc unsafe.Pointer, offsetInRow int, heapLoc *unsafe.Pointer) {
	store[int32](src, pointerAdd(rowLoc, offsetInRow))
}

type uint64ScatterOp struct {
}

func (scatter uint64ScatterOp) nullValue() uint64 {
	return 0
}

func (scatter uint64ScatterOp) randValue() uint64 {
	return rand.Uint64()
}

func (scatter uint64ScatterOp) store(src uint64, rowLoc unsafe.Pointer, offsetInRow int, heapLoc *unsafe.Pointer) {
	store[uint64](src, pointerAdd(rowLoc, offsetInRow))
}

type float64ScatterOp struct {
}

func (scatter float64ScatterOp) nullValue() float64 {
	return 0
}

func (scatter float64ScatterOp) randValue() float64 {
	return rand.Float64()
}
func (scatter float64ScatterOp) store(src float64, rowLoc unsafe.Pointer, offsetInRow int, heapLoc *unsafe.Pointer) {
	store[float64](src, pointerAdd(rowLoc, offsetInRow))
}

type stringScatterOp struct {
}

func (scatter stringScatterOp) nullValue() String {
	return String{_data: nil}
}

func (scatter stringScatterOp) randValue() String {
	start := 32
	end := 126
	l := int(rand.UintN(1024))
	if l == 0 {
		return String{}
	} else {
		data := cMalloc(l)
		ret := String{
			_data: data,
			_len:  l,
		}
		dSlice := ret.dataSlice()
		for i := 0; i < l; i++ {
			j := rand.UintN(uint(end-start) + 1)
			dSlice[i] = byte(j + ' ')
		}
		return ret
	}
}

func (scatter stringScatterOp) store(src String, rowLoc unsafe.Pointer, offsetInRow int, heapLoc *unsafe.Pointer) {
	if heapLoc == nil || *heapLoc == nil {
		panic("invalid heap location")
	}
	dst := pointerToSlice[byte](*heapLoc, src.len())
	srcSlice := pointerToSlice[byte](src.data(), src.len())
	copy(dst, srcSlice)
	newS := String{
		_data: *heapLoc,
		_len:  src.len(),
	}
	store[String](newS, pointerAdd(rowLoc, offsetInRow))
	assertFunc(newS.String() == src.String())
	*heapLoc = pointerAdd(*heapLoc, src.len())
}

type decimalScatterOp struct {
}

func (scatter decimalScatterOp) nullValue() Decimal {
	zero := dec.Zero
	return Decimal{zero}
}

func (scatter decimalScatterOp) randValue() Decimal {
	return Decimal{
		Decimal: dec.MustNew(rand.Int64N(1000000), 2),
	}
}

func (scatter decimalScatterOp) store(src Decimal, rowLoc unsafe.Pointer, offsetInRow int, heapLoc *unsafe.Pointer) {
	dst := src.Decimal
	store[Decimal](Decimal{dst}, pointerAdd(rowLoc, offsetInRow))
}

type dateScatterOp struct {
}

func (scatter dateScatterOp) nullValue() Date {
	return Date{_year: 1970, _month: 1, _day: 1}
}

func (scatter dateScatterOp) randValue() Date {
	now := time.Now().Unix()
	diff := rand.Int64N(24 * 3600)
	t := time.Unix(now+diff, 0)
	y, m, d := t.Date()
	return Date{
		_year:  int32(y),
		_month: int32(m),
		_day:   int32(d),
	}
}
func (scatter dateScatterOp) store(src Date, rowLoc unsafe.Pointer, offsetInRow int, heapLoc *unsafe.Pointer) {
	dst := src
	store[Date](dst, pointerAdd(rowLoc, offsetInRow))
}

type PhyType int

const (
	NA PhyType = iota
	BOOL
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
	BIT
	DATE
	POINTER
	DECIMAL
	UNKNOWN
	INVALID
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
	boolSize     int
	int8Size     int
	int16Size    int
	int32Size    int
	int64Size    int
	int128Size   int
	intervalSize int
	dateSize     int
	varcharSize  int
	pointerSize  int
	decimalSize  int
	float32Size  int
)

func init() {
	b := false
	boolSize = int(unsafe.Sizeof(b))
	i := int8(0)
	int8Size = int(unsafe.Sizeof(i))
	int16Size = int8Size * 2
	int32Size = int8Size * 4
	int64Size = int8Size * 8
	int128Size = int(unsafe.Sizeof(Hugeint{}))
	intervalSize = int(unsafe.Sizeof(Interval{}))
	dateSize = int(unsafe.Sizeof(Date{}))
	varcharSize = int(unsafe.Sizeof(String{}))
	pointerSize = int(unsafe.Sizeof(unsafe.Pointer(&b)))
	decimalSize = int(unsafe.Sizeof(Decimal{}))
	f := float32(0)
	float32Size = int(unsafe.Sizeof(f))
}

func (pt PhyType) size() int {
	switch pt {
	case BIT:
		panic("usp")
	case BOOL:
		return boolSize
	case INT8:
		return int8Size
	case INT16:
		return int16Size
	case INT32:
		return int32Size
	case INT64:
		return int64Size
	case UINT8:
		return int8Size
	case UINT16:
		return int16Size
	case UINT32:
		return int32Size
	case UINT64:
		return int64Size
	case INT128:
		return int128Size
	case FLOAT:
		return float32Size
	case DOUBLE:
		return int64Size
	case VARCHAR:
		return varcharSize
	case INTERVAL:
		return intervalSize
	case STRUCT:
	case UNKNOWN:
		return 0
	case LIST:
		panic("usp")
	case DATE:
		return dateSize
	case POINTER:
		return pointerSize
	case DECIMAL:
		return decimalSize
	default:
		panic("usp")
	}
	panic("usp")
}

func (pt PhyType) isConstant() bool {
	return pt >= BOOL && pt <= DOUBLE ||
		pt == INTERVAL ||
		pt == INT128 ||
		pt == DATE ||
		pt == POINTER ||
		pt == DECIMAL
}

func (pt PhyType) isVarchar() bool {
	return pt == VARCHAR
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

func (lt LType) serialize(serial Serialize) error {
	err := Write[int](int(lt.id), serial)
	if err != nil {
		return err
	}
	err = Write[int](lt.width, serial)
	if err != nil {
		return err
	}
	err = Write[int](lt.scale, serial)
	if err != nil {
		return err
	}
	return err
}

func deserializeLType(deserial Deserialize) (LType, error) {
	id := 0
	width := 0
	scale := 0
	err := Read[int](&id, deserial)
	if err != nil {
		return LType{}, err
	}
	err = Read[int](&width, deserial)
	if err != nil {
		return LType{}, err
	}
	err = Read[int](&scale, deserial)
	if err != nil {
		return LType{}, err
	}

	ret := LType{
		id:    LTypeId(id),
		width: width,
		scale: scale,
	}
	ret.pTyp = ret.getInternalType()
	return ret, err
}

func makeLType(id LTypeId) LType {
	ret := LType{id: id}
	ret.pTyp = ret.getInternalType()
	return ret
}

func null() LType {
	return makeLType(LTID_NULL)
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

func hashType() LType {
	return makeLType(LTID_UBIGINT)
}

func float() LType {
	return makeLType(LTID_FLOAT)
}

func double() LType {
	return makeLType(LTID_DOUBLE)
}

func tinyint() LType {
	return makeLType(LTID_TINYINT)
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

func pointerType() LType {
	return makeLType(LTID_POINTER)
}

func ubigintType() LType {
	return makeLType(LTID_UBIGINT)
}

func copyLTypes(typs ...LType) []LType {
	ret := make([]LType, 0)
	ret = append(ret, typs...)
	return ret
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

func (lt LType) isDate() bool {
	return lt.id == LTID_DATE
}

func (lt LType) isInterval() bool {
	return lt.id == LTID_INTERVAL
}

func (lt LType) isNumeric() bool {
	if _, has := numerics[lt.id]; has {
		return true
	}
	return false
}

func (lt LType) isPointer() bool {
	return lt.id == LTID_POINTER
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
		//if lt.width <= DecimalMaxWidthInt16 {
		//	return INT16
		//} else if lt.width <= DecimalMaxWidthInt32 {
		//	return INT32
		//} else if lt.width <= DecimalMaxWidthInt64 {
		//	return INT64
		//} else if lt.width <= DecimalMaxWidthInt128 {
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
	//return fmt.Sprintf("(%v %v %d %d)", lt.id, lt.pTyp, lt.width, lt.scale)
	return fmt.Sprintf("%v", lt.pTyp)
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
	//null := ""
	//if edt.NotNull {
	//	null = "not null"
	//}
	//return fmt.Sprintf("{%v,%s}", edt.LTyp, null)
	return fmt.Sprintf("%v", edt.LTyp)
}

var InvalidExprDataType = ExprDataType{
	LTyp: invalidLType(),
}

type LOT int

const (
	LOT_Project  LOT = 0
	LOT_Filter   LOT = 1
	LOT_Scan     LOT = 2
	LOT_JOIN     LOT = 3
	LOT_AggGroup LOT = 4
	LOT_Order    LOT = 5
	LOT_Limit    LOT = 6
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
	case LOT_JoinTypeSEMI:
		return "semi"
	case LOT_JoinTypeANTI:
		return "anti semi"
	default:
		panic(fmt.Sprintf("usp %d", lojt))
	}
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
	Counts           ColumnBindCountMap `json:"-"`
	ColRefToPos      ColumnBindPosMap   `json:"-"`
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
		printOutputs(tree, lo)
		tree.AddMetaNode("index", fmt.Sprintf("%d", lo.Index))
		node := tree.AddMetaBranch("exprs", "")
		listExprsToTree(node, lo.Projects)
	case LOT_Filter:
		tree = tree.AddBranch("Filter:")
		printOutputs(tree, lo)
		node := tree.AddMetaBranch("exprs", "")
		listExprsToTree(node, lo.Filters)
	case LOT_Scan:
		tree = tree.AddBranch("Scan:")
		printOutputs(tree, lo)
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
				idx := catalogTable.Column2Idx[col]
				t.WriteString(fmt.Sprintf("col %d %v %v", i, col, catalogTable.Types[idx]))
				t.WriteByte('\n')
			}
			return t.String()
		}
		//printColumns2 := func(cols []*Expr) string {
		//	t := strings.Builder{}
		//	t.WriteByte('\n')
		//	for _, col := range cols {
		//		t.WriteString(fmt.Sprintf("col %v %v %v", col.ColRef, col.Name, col.DataTyp))
		//		t.WriteByte('\n')
		//	}
		//	return t.String()
		//}
		if len(lo.Columns) > 0 {
			//tree.AddMetaNode("columns", printColumns2(lo.Outputs))
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
		printOutputs(tree, lo)
		tree.AddMetaNode("index", fmt.Sprintf("%d", lo.Index))
		if len(lo.OnConds) > 0 {
			node := tree.AddMetaBranch("On", "")
			listExprsToTree(node, lo.OnConds)
		}
		if lo.Stats != nil {
			tree.AddMetaNode("Stats", lo.Stats.String())
		}
	case LOT_AggGroup:
		tree = tree.AddBranch("Aggregate:")
		printOutputs(tree, lo)
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
		printOutputs(tree, lo)
		node := tree.AddMetaBranch("exprs", "")
		listExprsToTree(node, lo.OrderBys)
	case LOT_Limit:
		tree = tree.AddBranch(fmt.Sprintf("Limit: %v", lo.Limit.String()))
		printOutputs(tree, lo)
	default:
		panic(fmt.Sprintf("usp %v", lo.Typ))
	}

	for _, child := range lo.Children {
		child.Print(tree)
	}
}

func printOutputs(tree treeprint.Tree, root *LogicalOperator) {
	if len(root.Outputs) != 0 {
		node := tree.AddMetaBranch("outputs", "")
		listExprsToTree(node, root.Outputs)
	}

	if len(root.Counts) != 0 {
		node := tree.AddMetaBranch("counts", "")
		for bind, i := range root.Counts {
			node.AddNode(fmt.Sprintf("%v %v", bind, i))
		}
	}

	if len(root.ColRefToPos) != 0 {
		node := tree.AddMetaBranch("colRefToPos", "")
		binds := root.ColRefToPos.sortByColumnBind()
		for i, bind := range binds {
			node.AddNode(fmt.Sprintf("%v %v", bind, i))
		}
	}
}

func (lo *LogicalOperator) String() string {
	tree := treeprint.NewWithRoot("LogicalPlan:")
	lo.Print(tree)
	return tree.String()
}

func findExpr(exprs []*Expr, fun func(expr *Expr) bool) []*Expr {
	ret := make([]*Expr, 0)
	for _, expr := range exprs {
		if fun != nil && fun(expr) {
			ret = append(ret, expr)
		}
	}
	return ret
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
		if expr.Typ == ET_Func && expr.FuncId == INVALID_FUNC {
			panic("invalid function")
		}
		if expr.DataTyp.LTyp.id == LTID_INVALID {
			panic("invalid logical type")
		}
	}
}

type ET int

const (
	ET_Column ET = iota //column
	ET_TABLE            //table
	ET_Join             //join
	ET_CTE

	ET_Func
	ET_Subquery

	ET_IConst    //integer
	ET_DecConst  //decimal
	ET_SConst    //string
	ET_FConst    //float
	ET_DateConst //date
	ET_IntervalConst
	ET_BConst // bool
	ET_NConst // null

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
	ET_SubqueryTypeIn
	ET_SubqueryTypeNotIn
)

type Expr struct {
	Typ     ET
	SubTyp  ET_SubTyp
	DataTyp ExprDataType
	AggrTyp AggrType

	Children []*Expr

	Index       uint64
	Database    string
	Table       string     // table
	Name        string     // column
	ColRef      ColumnBind // relationTag, columnPos
	Depth       int        // > 0, correlated column
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
	CTEIndex    uint64

	BelongCtx *BindContext // context for table and join
	On        *Expr        //JoinOn
}

func (e *Expr) equal(o *Expr) bool {
	if e == nil && o == nil {
		return true
	} else if e != nil && o != nil {
		return reflect.DeepEqual(e, o)
	} else {
		return false
	}
}

func (e *Expr) copy() *Expr {
	if e == nil {
		return nil
	}

	if e.Typ == ET_Func && e.FuncId == INVALID_FUNC {
		panic("invalid fun in copy")
	}

	ret := &Expr{
		Typ:         e.Typ,
		SubTyp:      e.SubTyp,
		DataTyp:     e.DataTyp,
		AggrTyp:     e.AggrTyp,
		Index:       e.Index,
		Database:    e.Database,
		Table:       e.Table,
		Name:        e.Name,
		ColRef:      e.ColRef,
		Depth:       e.Depth,
		Svalue:      e.Svalue,
		Ivalue:      e.Ivalue,
		Fvalue:      e.Fvalue,
		Bvalue:      e.Bvalue,
		Desc:        e.Desc,
		JoinTyp:     e.JoinTyp,
		Alias:       e.Alias,
		SubBuilder:  e.SubBuilder,
		SubCtx:      e.SubCtx,
		FuncId:      e.FuncId,
		SubqueryTyp: e.SubqueryTyp,
		CTEIndex:    e.CTEIndex,
		BelongCtx:   e.BelongCtx,
		On:          e.On.copy(),
	}
	for _, child := range e.Children {
		ret.Children = append(ret.Children, child.copy())
	}
	return ret
}

func copyExprs(exprs ...*Expr) []*Expr {
	ret := make([]*Expr, 0)
	for _, expr := range exprs {
		ret = append(ret, expr.copy())
	}
	return ret
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
	case ET_SConst, ET_IConst, ET_DateConst, ET_IntervalConst, ET_BConst, ET_FConst, ET_NConst, ET_DecConst:
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
	case ET_SConst, ET_IConst, ET_DateConst, ET_FConst, ET_DecConst, ET_NConst, ET_BConst:

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

	case ET_SConst, ET_IConst, ET_DateConst, ET_IntervalConst, ET_BConst, ET_FConst, ET_NConst, ET_DecConst:
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
	case ET_SConst, ET_DateConst, ET_IConst, ET_IntervalConst, ET_BConst, ET_FConst, ET_NConst, ET_DecConst:
	case ET_Func:
	default:
		panic("usp")
	}
	for _, child := range e.Children {
		ret |= decideSide(child, leftTags, rightTags)
	}
	return ret
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
		ctx.Writef("(%s.%s,%s,%v,%d)", e.Table, e.Name,
			e.DataTyp,
			e.ColRef, e.Depth)
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
	case ET_DecConst:
		ctx.Writef("(%v,%s)", e.Svalue, e.DataTyp)
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
			if e.Children[0] != nil {
				e.Children[0].Format(ctx)
				ctx.Writeln()
			}
			for i := 2; i < len(e.Children); i += 2 {
				ctx.Write(" when")
				e.Children[i].Format(ctx)
				ctx.Write(" then ")
				e.Children[i+1].Format(ctx)
				ctx.Writeln()
			}
			if e.Children[1] != nil {
				ctx.Write(" else ")
				e.Children[1].Format(ctx)
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
				e.Children[i].Format(ctx)
			}
			ctx.Write(")")
		case ET_Exists:
			ctx.Writef("exists(")
			e.Children[0].Format(ctx)
			ctx.Write(")")

		case ET_SubFunc:
			ctx.Writef("%s_%d(", e.Svalue, e.FuncId)
			for idx, child := range e.Children {
				if idx > 0 {
					ctx.Write(", ")
				}
				child.Format(ctx)
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
		if e.Depth != 0 {
			tree.AddMetaNode(head, fmt.Sprintf("(%s.%s,%v,%d)",
				e.Table, e.Name,
				e.ColRef, e.Depth))
		} else {
			tree.AddMetaNode(head, fmt.Sprintf("(%s.%s,%v)",
				e.Table, e.Name,
				e.ColRef))
		}

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
	case ET_DecConst:
		tree.AddMetaNode(head, fmt.Sprintf("(%s %d %d)", e.Svalue, e.DataTyp.LTyp.width, e.DataTyp.LTyp.scale))
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
			branch = tree.AddMetaBranch(head, e.SubTyp.String())
			e.Children[0].Print(branch, "")
			e.Children[1].Print(branch, "")
			e.Children[2].Print(branch, "")
		case ET_Case:
			branch = tree.AddMetaBranch(head, e.SubTyp.String())
			if e.Children[0] != nil {
				e.Children[0].Print(branch, "")
			}
			when := branch.AddBranch("when")
			for i := 1; i < len(e.Children); i += 2 {
				e.Children[i].Print(when, "")
				e.Children[i+1].Print(when, "")
			}
			if e.Children[0] != nil {
				e.Children[0].Print(branch, "")
			}
		case ET_In, ET_NotIn:
			branch = tree.AddMetaBranch(head, e.SubTyp)
			for _, child := range e.Children {
				child.Print(branch, "")
			}
		case ET_Exists:
			branch = tree.AddMetaBranch(head, e.SubTyp)
			e.Children[0].Print(branch, "")
		case ET_SubFunc:
			branch = tree.AddMetaBranch(head, e.Svalue)
			for _, child := range e.Children {
				child.Print(branch, "")
			}
		default:
			//binary operator
			branch = tree.AddMetaBranch(head, e.SubTyp)
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
	POT_Stub //test stub
)

var potToStr = map[POT]string{
	POT_Project: "project",
	POT_With:    "with",
	POT_Filter:  "filter",
	POT_Agg:     "agg",
	POT_Join:    "join",
	POT_Order:   "order",
	POT_Limit:   "limit",
	POT_Scan:    "scan",
	POT_Stub:    "stub",
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

	Children []*PhysicalOperator
}

func (po *PhysicalOperator) String() string {
	tree := treeprint.NewWithRoot("PhysicalPlan:")
	po.Print(tree)
	return tree.String()
}

func printPhyOutputs(tree treeprint.Tree, root *PhysicalOperator) {
	tree.AddMetaNode("id", fmt.Sprintf("%d", root.Id))
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
		catalogTable, err := tpchCatalog().Table(po.Database, po.Table)
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
		if len(po.Columns) > 0 {
			tree.AddMetaNode("columns", printColumns(po.Columns))
		} else {
			tree.AddMetaNode("columns", printColumns(catalogTable.Columns))
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
	default:
		panic(fmt.Sprintf("usp %v", po.Typ))
	}

	for _, child := range po.Children {
		child.Print(tree)
	}
}

func collectFilterExprs(root *PhysicalOperator) []*Expr {
	if root == nil {
		return nil
	}
	ret := make([]*Expr, 0)
	ret = append(ret, root.Filters...)
	ret = append(ret, root.OnConds...)
	for _, child := range root.Children {
		ret = append(ret, collectFilterExprs(child)...)
	}
	return ret
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
		}
		//else if expr.SubTyp == ET_In || expr.SubTyp == ET_NotIn {
		//	//ret := make([]*Expr, 0)
		//	//for i, child := range expr.Children {
		//	//	if i == 0 {
		//	//		continue
		//	//	}
		//	//	ret = append(ret, &Expr{
		//	//		Typ:      expr.Typ,
		//	//		SubTyp:   expr.SubTyp,
		//	//		Svalue:   expr.SubTyp.String(),
		//	//		FuncId:   expr.FuncId,
		//	//		Children: []*Expr{expr.Children[0], child},
		//	//	})
		//	//}
		//}
	}
	return []*Expr{expr.copy()}
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

func splitExprByOr(expr *Expr) []*Expr {
	if expr.Typ == ET_Func {
		if expr.SubTyp == ET_Or {
			return append(splitExprByOr(expr.Children[0]), splitExprByOr(expr.Children[1])...)
		}
	}
	return []*Expr{expr.copy()}
}

func andExpr(a, b *Expr) *Expr {
	return &Expr{
		Typ:      ET_Func,
		SubTyp:   ET_And,
		FuncId:   AND,
		DataTyp:  ExprDataType{LTyp: boolean()},
		Children: []*Expr{a, b},
	}
}

func combineExprsByAnd(exprs ...*Expr) *Expr {
	if len(exprs) == 1 {
		return exprs[0]
	} else if len(exprs) == 2 {
		return andExpr(exprs[0], exprs[1])
	} else {
		return andExpr(
			combineExprsByAnd(exprs[:len(exprs)-1]...),
			combineExprsByAnd(exprs[len(exprs)-1]))
	}
}

func orExpr(a, b *Expr) *Expr {
	return &Expr{
		Typ:      ET_Func,
		SubTyp:   ET_Or,
		FuncId:   OR,
		DataTyp:  ExprDataType{LTyp: boolean()},
		Children: []*Expr{a, b},
	}
}

func combineExprsByOr(exprs ...*Expr) *Expr {
	if len(exprs) == 1 {
		return exprs[0]
	} else if len(exprs) == 2 {
		return orExpr(exprs[0], exprs[1])
	} else {
		return orExpr(
			combineExprsByOr(exprs[:len(exprs)-1]...),
			combineExprsByOr(exprs[len(exprs)-1]))
	}
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
				FuncId:   expr.FuncId,
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

	case ET_SConst, ET_IConst, ET_DateConst, ET_IntervalConst, ET_BConst, ET_FConst, ET_NConst, ET_DecConst:
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

func replaceColRef2(e *Expr, colRefToPos ColumnBindPosMap, st SourceType) *Expr {
	if e == nil {
		return nil
	}
	switch e.Typ {
	case ET_Column:
		has, pos := colRefToPos.pos(e.ColRef)
		if has {
			e.ColRef[0] = uint64(st)
			e.ColRef[1] = uint64(pos)
		}

	case ET_SConst, ET_IConst, ET_DateConst, ET_IntervalConst, ET_BConst, ET_FConst, ET_NConst, ET_DecConst:
	case ET_Func:
	case ET_Orderby:
	default:
		panic("usp")
	}
	for i, child := range e.Children {
		e.Children[i] = replaceColRef2(child, colRefToPos, st)
	}
	return e
}

func replaceColRef3(es []*Expr, colRefToPos ColumnBindPosMap, st SourceType) {
	for _, e := range es {
		replaceColRef2(e, colRefToPos, st)
	}
}

func collectColRefs(e *Expr, set ColumnBindSet) {
	if e == nil {
		return
	}
	switch e.Typ {
	case ET_Column:
		set.insert(e.ColRef)

	case ET_Func:
	case ET_SConst, ET_IConst, ET_DateConst, ET_IntervalConst, ET_BConst, ET_FConst, ET_NConst, ET_DecConst:
	case ET_Orderby:
	default:
		panic("usp")
	}
	for _, child := range e.Children {
		collectColRefs(child, set)
	}
}

func collectColRefs2(set ColumnBindSet, exprs ...*Expr) {
	for _, expr := range exprs {
		collectColRefs(expr, set)
	}
}

func checkColRefPos(e *Expr, root *LogicalOperator) {
	if e == nil || root == nil {
		return
	}
	if e.Typ == ET_Column {
		if root.Typ == LOT_Scan {
			if !(e.ColRef.table() == root.Index && e.ColRef.column() < uint64(len(root.Columns))) {
				panic(fmt.Sprintf("no bind %v in scan %v", e.ColRef, root.Index))
			}
		} else if root.Typ == LOT_AggGroup {
			st := SourceType(e.ColRef.table())
			switch st {
			case ThisNode:
				if !(e.ColRef.table() == root.Index2 && e.ColRef.column() < uint64(len(root.Aggs))) {
					panic(fmt.Sprintf("no bind %v in scan %v", e.ColRef, root.Index))
				}
			case LeftChild:
				if len(root.Children) < 1 || root.Children[0] == nil {
					panic("no child")
				}
				binds := root.Children[0].ColRefToPos.sortByColumnBind()
				if e.ColRef.column() >= uint64(len(binds)) {
					panic(fmt.Sprintf("no bind %v in child", e.ColRef))
				}
			case RightChild:
				if len(root.Children) < 2 || root.Children[1] == nil {
					panic("no right child")
				}
				binds := root.Children[1].ColRefToPos.sortByColumnBind()
				if e.ColRef.column() >= uint64(len(binds)) {
					panic(fmt.Sprintf("no bind %v in right child", e.ColRef))
				}
			default:
				if !(e.ColRef.table() == root.Index2 && e.ColRef.column() < uint64(len(root.Aggs))) {
					panic(fmt.Sprintf("no bind %v in scan %v", e.ColRef, root.Index))
				}
			}
		} else {
			st := SourceType(e.ColRef.table())
			switch st {
			case ThisNode:
				panic(fmt.Sprintf("bind %v exists", e.ColRef))
			case LeftChild:
				if len(root.Children) < 1 || root.Children[0] == nil {
					panic("no child")
				}
				binds := root.Children[0].ColRefToPos.sortByColumnBind()
				if e.ColRef.column() >= uint64(len(binds)) {
					panic(fmt.Sprintf("no bind %v in child", e.ColRef))
				}
			case RightChild:
				if len(root.Children) < 2 || root.Children[1] == nil {
					panic("no right child")
				}
				binds := root.Children[1].ColRefToPos.sortByColumnBind()
				if e.ColRef.column() >= uint64(len(binds)) {
					panic(fmt.Sprintf("no bind %v in right child", e.ColRef))
				}
			default:
				panic(fmt.Sprintf("no source type %d", st))
			}
		}
	}
	for _, child := range e.Children {
		checkColRefPos(child, root)
	}
}

func checkColRefPosInExprs(es []*Expr, root *LogicalOperator) {
	for _, e := range es {
		checkColRefPos(e, root)
	}
}

func checkColRefPosInNode(root *LogicalOperator) {
	if root == nil {
		return
	}
	checkColRefPosInExprs(root.Projects, root)
	checkColRefPosInExprs(root.Filters, root)
	checkColRefPosInExprs(root.OnConds, root)
	checkColRefPosInExprs(root.Aggs, root)
	checkColRefPosInExprs(root.GroupBys, root)
	checkColRefPosInExprs(root.OrderBys, root)
	checkColRefPosInExprs([]*Expr{root.Limit}, root)
}
