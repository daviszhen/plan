package util

import (
	"unsafe"
)

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

func EncodeInt32(ptr unsafe.Pointer, value int32) {
	Store[uint32](BSWAP32(uint32(value)), ptr)
	Store[uint8](FlipSign(Load[uint8](ptr)), ptr)
}

func EncodeInt64(ptr unsafe.Pointer, value int64) {
	Store[uint64](BSWAP64(uint64(value)), ptr)
	Store[uint8](FlipSign(Load[uint8](ptr)), ptr)
}

func EncodeUint64(ptr unsafe.Pointer, value uint64) {
	Store[uint64](BSWAP64(value), ptr)
}

type Encoder[T any] interface {
	EncodeData(unsafe.Pointer, *T)
	TypeSize() int
}

type Int32Encoder struct {
}

func (i Int32Encoder) EncodeData(ptr unsafe.Pointer, value *int32) {
	Store[uint32](BSWAP32(uint32(*value)), ptr)
	Store[uint8](FlipSign(Load[uint8](ptr)), ptr)
}

func (i Int32Encoder) TypeSize() int {
	return 4
}

type Int64Encoder struct {
}

func (i Int64Encoder) EncodeData(ptr unsafe.Pointer, value *int64) {
	Store[uint64](BSWAP64(uint64(*value)), ptr)
	Store[uint8](FlipSign(Load[uint8](ptr)), ptr)
}

func (i Int64Encoder) TypeSize() int {
	return 8
}

type Uint64Encoder struct {
}

func (i Uint64Encoder) EncodeData(ptr unsafe.Pointer, value *uint64) {
	Store[uint64](BSWAP64(*value), ptr)
}

func (i Uint64Encoder) TypeSize() int {
	return 8
}
