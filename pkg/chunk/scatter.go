package chunk

import (
	"math"
	"math/rand/v2"
	"time"
	"unsafe"

	"github.com/govalues/decimal"

	"github.com/daviszhen/plan/pkg/common"
	"github.com/daviszhen/plan/pkg/util"
)

type ScatterOp[T any] interface {
	NullValue() T
	RandValue() T
	Store(src T, rowLoc unsafe.Pointer, offsetInRow int, heapLoc *unsafe.Pointer)
}

type BoolScatterOp struct {
}

func (scatter BoolScatterOp) NullValue() bool {
	return false
}

func (scatter BoolScatterOp) RandValue() bool {
	return false
}

func (scatter BoolScatterOp) Store(src bool, rowLoc unsafe.Pointer, offsetInRow int, heapLoc *unsafe.Pointer) {
	util.Store[bool](src, util.PointerAdd(rowLoc, offsetInRow))
}

type Int8ScatterOp struct {
}

func (scatter Int8ScatterOp) NullValue() int8 {
	return 0
}

func (scatter Int8ScatterOp) RandValue() int8 {
	return int8(rand.Int32())
}

func (scatter Int8ScatterOp) Store(src int8, rowLoc unsafe.Pointer, offsetInRow int, heapLoc *unsafe.Pointer) {
	util.Store[int8](src, util.PointerAdd(rowLoc, offsetInRow))
}

type Int32ScatterOp struct {
}

func (scatter Int32ScatterOp) NullValue() int32 {
	return 0
}

func (scatter Int32ScatterOp) RandValue() int32 {
	return rand.Int32()
}

func (scatter Int32ScatterOp) Store(src int32, rowLoc unsafe.Pointer, offsetInRow int, heapLoc *unsafe.Pointer) {
	util.Store[int32](src, util.PointerAdd(rowLoc, offsetInRow))
}

type HugeintScatterOp struct {
}

func (scatter HugeintScatterOp) NullValue() common.Hugeint {
	return common.Hugeint{
		Lower: 0,
		Upper: math.MinInt64,
	}
}

func (scatter HugeintScatterOp) RandValue() common.Hugeint {
	return common.Hugeint{}
}

func (scatter HugeintScatterOp) Store(src common.Hugeint, rowLoc unsafe.Pointer, offsetInRow int, heapLoc *unsafe.Pointer) {
	util.Store[common.Hugeint](src, util.PointerAdd(rowLoc, offsetInRow))
}

type Uint64ScatterOp struct {
}

func (scatter Uint64ScatterOp) NullValue() uint64 {
	return 0
}

func (scatter Uint64ScatterOp) RandValue() uint64 {
	return rand.Uint64()
}

func (scatter Uint64ScatterOp) Store(src uint64, rowLoc unsafe.Pointer, offsetInRow int, heapLoc *unsafe.Pointer) {
	util.Store[uint64](src, util.PointerAdd(rowLoc, offsetInRow))
}

type Int64ScatterOp struct {
}

func (scatter Int64ScatterOp) NullValue() int64 {
	return 0
}

func (scatter Int64ScatterOp) RandValue() int64 {
	return rand.Int64()
}

func (scatter Int64ScatterOp) Store(src int64, rowLoc unsafe.Pointer, offsetInRow int, heapLoc *unsafe.Pointer) {
	util.Store[int64](src, util.PointerAdd(rowLoc, offsetInRow))
}

type Float64ScatterOp struct {
}

func (scatter Float64ScatterOp) NullValue() float64 {
	return 0
}

func (scatter Float64ScatterOp) RandValue() float64 {
	return rand.Float64()
}
func (scatter Float64ScatterOp) Store(src float64, rowLoc unsafe.Pointer, offsetInRow int, heapLoc *unsafe.Pointer) {
	util.Store[float64](src, util.PointerAdd(rowLoc, offsetInRow))
}

type StringScatterOp struct {
}

func (scatter StringScatterOp) NullValue() common.String {
	return common.String{Data: nil}
}

func (scatter StringScatterOp) RandValue() common.String {
	start := 32
	end := 126
	l := int(rand.UintN(1024))
	if l == 0 {
		return common.String{}
	} else {
		data := util.CMalloc(l)
		ret := common.String{
			Data: data,
			Len:  l,
		}
		dSlice := ret.DataSlice()
		for i := 0; i < l; i++ {
			j := rand.UintN(uint(end-start) + 1)
			dSlice[i] = byte(j + ' ')
		}
		return ret
	}
}

func (scatter StringScatterOp) Store(src common.String, rowLoc unsafe.Pointer, offsetInRow int, heapLoc *unsafe.Pointer) {
	if heapLoc == nil || *heapLoc == nil {
		panic("invalid heap location")
	}
	dst := util.PointerToSlice[byte](*heapLoc, src.Length())
	srcSlice := util.PointerToSlice[byte](src.DataPtr(), src.Length())
	copy(dst, srcSlice)
	newS := common.String{
		Data: *heapLoc,
		Len:  src.Length(),
	}
	util.Store[common.String](newS, util.PointerAdd(rowLoc, offsetInRow))
	util.AssertFunc(newS.String() == src.String())
	*heapLoc = util.PointerAdd(*heapLoc, src.Length())
}

type DecimalScatterOp struct {
}

func (scatter DecimalScatterOp) NullValue() common.Decimal {
	zero := decimal.Zero
	return common.Decimal{Decimal: zero}
}

func (scatter DecimalScatterOp) RandValue() common.Decimal {
	return common.Decimal{
		Decimal: decimal.MustNew(rand.Int64N(1000000), 2),
	}
}

func (scatter DecimalScatterOp) Store(src common.Decimal, rowLoc unsafe.Pointer, offsetInRow int, heapLoc *unsafe.Pointer) {
	dst := src.Decimal
	util.Store[common.Decimal](common.Decimal{Decimal: dst}, util.PointerAdd(rowLoc, offsetInRow))
}

type DateScatterOp struct {
}

func (scatter DateScatterOp) NullValue() common.Date {
	return common.Date{Year: 1970, Month: 1, Day: 1}
}

func (scatter DateScatterOp) RandValue() common.Date {
	now := time.Now().Unix()
	diff := rand.Int64N(24 * 3600)
	t := time.Unix(now+diff, 0)
	y, m, d := t.Date()
	return common.Date{
		Year:  int32(y),
		Month: int32(m),
		Day:   int32(d),
	}
}
func (scatter DateScatterOp) Store(src common.Date, rowLoc unsafe.Pointer, offsetInRow int, heapLoc *unsafe.Pointer) {
	dst := src
	util.Store[common.Date](dst, util.PointerAdd(rowLoc, offsetInRow))
}
