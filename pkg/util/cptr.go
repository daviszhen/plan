// Copyright 2023-2024 daviszhen
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package util

import "unsafe"

// CPtr is a unified handle for C-allocated (jemalloc) memory with ownership semantics.
// Zero value is safe. Destroy is idempotent.
type CPtr struct {
	ptr  unsafe.Pointer
	len  int
	owns bool
}

// NewCPtr allocates size bytes via CMalloc, zero-initializes, and returns an owning handle.
func NewCPtr(size int) CPtr {
	if size <= 0 {
		return CPtr{}
	}
	p := CMalloc(size)
	Memset(p, 0, size)
	return CPtr{ptr: p, len: size, owns: true}
}

// Borrow returns a non-owning handle to the same memory.
func (c CPtr) Borrow() CPtr {
	return CPtr{ptr: c.ptr, len: c.len, owns: false}
}

// Transfer moves ownership: the receiver becomes non-owner, the returned CPtr becomes owner.
func (c *CPtr) Transfer() CPtr {
	new := CPtr{ptr: c.ptr, len: c.len, owns: true}
	c.owns = false
	return new
}

// Destroy frees the memory if this handle is the owner. Idempotent.
func (c *CPtr) Destroy() {
	if c.owns && c.ptr != nil {
		CFree(c.ptr)
	}
	c.ptr = nil
	c.len = 0
	c.owns = false
}

// Realloc resizes the allocation (only valid for owner). Updates ptr and len in place.
func (c *CPtr) Realloc(newSize int) {
	if !c.owns {
		panic("CPtr.Realloc called on non-owner")
	}
	c.ptr = CRealloc(c.ptr, newSize)
	c.len = newSize
}

// Bytes returns a Go []byte view of the memory (zero-copy). Returns nil if ptr is nil.
func (c CPtr) Bytes() []byte {
	if c.ptr == nil || c.len == 0 {
		return nil
	}
	return PointerToSlice[byte](c.ptr, c.len)
}

// Ptr returns the raw pointer.
func (c CPtr) Ptr() unsafe.Pointer { return c.ptr }

// Len returns the allocation size in bytes.
func (c CPtr) Len() int { return c.len }

// IsOwner returns true if this handle owns the memory.
func (c CPtr) IsOwner() bool { return c.owns }

// IsNil returns true if the pointer is nil.
func (c CPtr) IsNil() bool { return c.ptr == nil }
