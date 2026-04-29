package util

import (
	"unsafe"
)

//#include <stdio.h>
//#include <stdlib.h>
//#include <string.h>
//#cgo LDFLAGS: -ljemalloc
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

func CAlloc(sz int) []byte {
	if sz == 0 {
		return nil
	}
	ptr := CMalloc(sz)
	s := PointerToSlice[byte](ptr, sz)
	Memset(ptr, 0, sz)
	return s
}

func CFreeBytes(data []byte) {
	if len(data) == 0 {
		return
	}
	CFree(BytesSliceToPointer(data))
}

// CMemcmp calls libc memcmp directly on raw pointers. No slice header overhead.
func CMemcmp(a, b unsafe.Pointer, n int) int {
	return int(C.memcmp(a, b, C.size_t(n)))
}
