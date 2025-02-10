package util

import (
	"C"
	"unsafe"
)

//#include <stdio.h>
//#include <stdlib.h>
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
