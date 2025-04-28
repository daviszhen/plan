package util

import (
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
)

func Test_cmemset(t *testing.T) {
	buf := make([]byte, 1024)
	CMemset(unsafe.Pointer(&buf[0]), 1, 1024)
	for i := 0; i < 1024; i++ {
		assert.Equal(t, byte(1), buf[i])
	}
	ptr := CMalloc(1024)
	defer CFree(ptr)
	CMemset(ptr, 1, 1024)
	for i := 0; i < 1024; i++ {
		assert.Equal(t,
			byte(1),
			*(*byte)(PointerAdd(ptr, i)))
	}
}
