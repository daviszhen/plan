package compute

import (
	"unsafe"

	"github.com/daviszhen/plan/pkg/common"
	"github.com/daviszhen/plan/pkg/util"
)

type Encoder[T any] interface {
	EncodeData(unsafe.Pointer, *T)
	TypeSize() int
}

func BSWAP16(x uint16) uint16 {
	return ((x & 0xff00) >> 8) | ((x & 0x00ff) << 8)
}

func BSWAP32(x uint32) uint32 {
	return ((x & 0xff000000) >> 24) | ((x & 0x00ff0000) >> 8) |
		((x & 0x0000ff00) << 8) | ((x & 0x000000ff) << 24)

}

func BSWAP64(x uint64) uint64 {
	return ((x & 0xff00000000000000) >> 56) | ((x & 0x00ff000000000000) >> 40) |
		((x & 0x0000ff0000000000) >> 24) | ((x & 0x000000ff00000000) >> 8) |
		((x & 0x00000000ff000000) << 8) | ((x & 0x0000000000ff0000) << 24) |
		((x & 0x000000000000ff00) << 40) | ((x & 0x00000000000000ff) << 56)

}

func FlipSign(b uint8) uint8 {
	return b ^ 128
}

type int32Encoder struct {
}

func (i int32Encoder) EncodeData(ptr unsafe.Pointer, value *int32) {
	util.Store[uint32](BSWAP32(uint32(*value)), ptr)
	util.Store[uint8](FlipSign(util.Load[uint8](ptr)), ptr)
}

func (i int32Encoder) TypeSize() int {
	return 4
}

// actually it int64
type intEncoder struct {
}

func (i intEncoder) EncodeData(ptr unsafe.Pointer, value *int) {
	util.Store[uint64](BSWAP64(uint64(*value)), ptr)
	util.Store[uint8](FlipSign(util.Load[uint8](ptr)), ptr)
}

func (i intEncoder) TypeSize() int {
	return int(unsafe.Sizeof(int(0)))
}

type decimalEncoder struct {
}

func (decimalEncoder) EncodeData(ptr unsafe.Pointer, dec *common.Decimal) {
	whole, frac, ok := dec.Int64(2)
	util.AssertFunc(ok)
	encodeInt64(ptr, whole)
	encodeInt64(util.PointerAdd(ptr, common.Int64Size), frac)
}
func (decimalEncoder) TypeSize() int {
	return common.DecimalSize
}

type dateEncoder struct{}

func (dateEncoder) EncodeData(ptr unsafe.Pointer, d *common.Date) {
	encodeInt32(ptr, d.Year)
	encodeInt32(util.PointerAdd(ptr, common.Int32Size), d.Month)
	encodeInt32(util.PointerAdd(ptr, 2*common.Int32Size), d.Day)
}

func (dateEncoder) TypeSize() int {
	return common.DateSize
}

type hugeEncoder struct{}

func (hugeEncoder) EncodeData(ptr unsafe.Pointer, d *common.Hugeint) {
	encodeInt64(ptr, d.Upper)
	encodeUint64(util.PointerAdd(ptr, common.Int64Size), d.Lower)
}

func (hugeEncoder) TypeSize() int {
	return common.Int128Size
}

func encodeInt32(ptr unsafe.Pointer, value int32) {
	util.Store[uint32](BSWAP32(uint32(value)), ptr)
	util.Store[uint8](FlipSign(util.Load[uint8](ptr)), ptr)
}

//func encodeUint32(ptr unsafe.Pointer, value uint32) {
//	store[uint32](BSWAP32(value), ptr)
//}

func encodeInt64(ptr unsafe.Pointer, value int64) {
	util.Store[uint64](BSWAP64(uint64(value)), ptr)
	util.Store[uint8](FlipSign(util.Load[uint8](ptr)), ptr)
}

func encodeUint64(ptr unsafe.Pointer, value uint64) {
	util.Store[uint64](BSWAP64(value), ptr)
}
