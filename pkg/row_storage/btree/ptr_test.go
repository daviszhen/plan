package btree

import (
	"fmt"
	"sync/atomic"
	"testing"
	"unsafe"
)

type TPageHeader struct {
	stateAtomic      uint32 //atomic
	usageCountAtomic uint32 //atomic
	pageChangeCount  uint32
}
type TPage struct {
	Header TPageHeader
}

func TestPtr(t *testing.T) {
	buf := make([]byte, 1024)
	ptr := unsafe.Pointer(&buf[0])
	page_ptr := (*TPage)(ptr)
	atomic.StoreUint32(&page_ptr.Header.stateAtomic, 1)
	atomic.StoreUint32(&page_ptr.Header.usageCountAtomic, 2)
	page_ptr.Header.pageChangeCount = 3
	fmt.Println(*page_ptr)
}
