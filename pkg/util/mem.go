package util

import (
	"unsafe"
)

//#include <stdio.h>
//#include <stdlib.h>
//#include <string.h>
import "C"

func CMalloc(sz int) unsafe.Pointer {
	return C.malloc(C.size_t(sz))
}

func CFree(ptr unsafe.Pointer) {
	C.free(ptr)
}

func CRealloc(ptr unsafe.Pointer, sz int) unsafe.Pointer {
	return C.realloc(ptr, C.size_t(sz))
}

func CMemset(ptr unsafe.Pointer, val byte, sz int) {
	C.memset(ptr, C.int(val), C.size_t(sz))
}

func CMemmove(dst unsafe.Pointer, src unsafe.Pointer, sz int) {
	C.memmove(dst, src, C.size_t(sz))
}

func CMemcpy(dst unsafe.Pointer, src unsafe.Pointer, sz int) {
	C.memcpy(dst, src, C.size_t(sz))
}

type BytesAllocator interface {
	Alloc(sz int) []byte
	Free([]byte)
}

type DefaultAllocator struct {
}

func (alloc *DefaultAllocator) Alloc(sz int) []byte {
	return make([]byte, sz)
}

func (alloc *DefaultAllocator) Free(bytes []byte) {
}

var GAlloc BytesAllocator = &DefaultAllocator{}
