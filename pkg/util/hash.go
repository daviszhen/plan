package util

import (
	"unsafe"
)

const (
	M    uint64 = 0xc6a4a7935bd1e995
	SEED uint64 = 0xe17a1465
	R    uint64 = 47
)

func HashBytes(ptr unsafe.Pointer, len uint64) uint64 {
	data64 := ptr
	h := SEED ^ (len * M)

	n_blocks := len / 8
	for i := uint64(0); i < n_blocks; i++ {
		k := Load[uint64](PointerAdd(data64, int(i*uint64(8))))
		k *= M
		k ^= k >> R
		k *= M

		h ^= k
		h *= M
	}
	data8 := PointerAdd(data64, int(n_blocks*uint64(8)))
	switch len & 7 {
	case 7:
		val := Load[byte](PointerAdd(data8, 6))
		h ^= uint64(val) << 48
		fallthrough
	case 6:
		val := Load[byte](PointerAdd(data8, 5))
		h ^= uint64(val) << 40
		fallthrough
	case 5:
		val := Load[byte](PointerAdd(data8, 4))
		h ^= uint64(val) << 32
		fallthrough
	case 4:
		val := Load[byte](PointerAdd(data8, 3))
		h ^= uint64(val) << 24
		fallthrough
	case 3:
		val := Load[byte](PointerAdd(data8, 2))
		h ^= uint64(val) << 16
		fallthrough
	case 2:
		val := Load[byte](PointerAdd(data8, 1))
		h ^= uint64(val) << 8
		fallthrough
	case 1:
		val := Load[byte](data8)
		h ^= uint64(val)
		h *= M
		fallthrough
	default:
		break
	}
	h ^= h >> R
	h *= M
	h ^= h >> R
	return h
}

func ChecksumU64(x uint64) uint64 {
	return x * 0xbf58476d1ce4e5b9
}

func Checksum(buffer unsafe.Pointer, sz uint64) uint64 {
	result := uint64(5381)
	l := sz / 8
	ptr := PointerToSlice[uint64](buffer, int(l))
	i := uint64(0)
	for i = 0; i < l; i++ {
		result ^= ChecksumU64(ptr[i])
	}
	rest := sz % 8
	if rest > 0 {
		result ^= HashBytes(
			PointerAdd(buffer, int(i*8)),
			rest)
	}
	return result
}
