package util

import (
	"bytes"
	"unsafe"
)

func Load[T any](ptr unsafe.Pointer) T {
	return *(*T)(ptr)
}

func Load2[T any](ptr unsafe.Pointer, offset int) T {
	return *(*T)(PointerAdd(ptr, offset))
}

func Store[T any](val T, ptr unsafe.Pointer) {
	*(*T)(ptr) = val
}

func Store2[T any](val T, ptr unsafe.Pointer, offset int) {
	*(*T)(PointerAdd(ptr, offset)) = val
}

func Memset(ptr unsafe.Pointer, val byte, size int) {
	for i := 0; i < size; i++ {
		Store[byte](val, PointerAdd(ptr, i))
	}
}

func Fill[T any](data []T, count int, val T) {
	for i := 0; i < count; i++ {
		data[i] = val
	}
}

func ToSlice[T any](data []byte, pSize int) []T {
	slen := len(data) / pSize
	return unsafe.Slice((*T)(unsafe.Pointer(unsafe.SliceData(data))), slen)
}

func BytesSliceToPointer(data []byte) unsafe.Pointer {
	return unsafe.Pointer(unsafe.SliceData(data))
}

func PointerAdd(base unsafe.Pointer, offset int) unsafe.Pointer {
	return unsafe.Add(base, offset)
}

func PointerLess(lhs, rhs unsafe.Pointer) bool {
	return uintptr(lhs) < uintptr(rhs)
}

func PointerLessEqual(lhs, rhs unsafe.Pointer) bool {
	return uintptr(lhs) <= uintptr(rhs)
}

func PointerSub(lhs, rhs unsafe.Pointer) int64 {
	a := uint64(uintptr(lhs))
	b := uint64(uintptr(rhs))
	//uint64
	ret0 := a - b
	ret := int64(ret0)
	if a < b {
		ret = -ret
	}
	return ret
}

func PointerToSlice[T any](base unsafe.Pointer, len int) []T {
	return unsafe.Slice((*T)(base), len)
}

func PointerCopy(dst, src unsafe.Pointer, len int) {
	dstSlice := PointerToSlice[byte](dst, len)
	srcSlice := PointerToSlice[byte](src, len)
	copy(dstSlice, srcSlice)
}

func PointerCopy2(dst unsafe.Pointer, src []byte, len int) {
	dstSlice := PointerToSlice[byte](dst, len)
	copy(dstSlice, src[:len])
}

func PointerValid(ptr unsafe.Pointer) bool {
	return uintptr(ptr) != 0
}

func PointerMemcmp(lAddr, rAddr unsafe.Pointer, len int) int {
	lSlice := PointerToSlice[byte](lAddr, len)
	rSlice := PointerToSlice[byte](rAddr, len)
	ret := bytes.Compare(lSlice, rSlice)
	return ret
}

func PointerMemcmp2(lAddr, rAddr unsafe.Pointer, len1, len2 int) int {
	lSlice := PointerToSlice[byte](lAddr, len1)
	rSlice := PointerToSlice[byte](rAddr, len2)
	ret := bytes.Compare(lSlice, rSlice)
	return ret
}

func InvertBits(base unsafe.Pointer, offset int) {
	ptr := PointerAdd(base, offset)
	b := Load[byte](ptr)
	b = ^b
	Store[byte](b, ptr)
}
