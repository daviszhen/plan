package util

import (
	"testing"
)

func TestCPtr_NewAndDestroy(t *testing.T) {
	c := NewCPtr(64)
	if c.IsNil() {
		t.Fatal("NewCPtr returned nil")
	}
	if c.Len() != 64 {
		t.Fatalf("Len: got %d, want 64", c.Len())
	}
	if !c.IsOwner() {
		t.Fatal("NewCPtr should be owner")
	}
	// verify zero-initialized
	for i, b := range c.Bytes() {
		if b != 0 {
			t.Fatalf("Bytes()[%d] = %d, want 0", i, b)
		}
	}
	c.Destroy()
	if !c.IsNil() {
		t.Fatal("after Destroy, should be nil")
	}
	if c.IsOwner() {
		t.Fatal("after Destroy, should not be owner")
	}
	if c.Len() != 0 {
		t.Fatal("after Destroy, Len should be 0")
	}
}

func TestCPtr_DestroyIdempotent(t *testing.T) {
	c := NewCPtr(32)
	c.Destroy()
	c.Destroy() // should not panic
	c.Destroy()
}

func TestCPtr_ZeroValue(t *testing.T) {
	var c CPtr
	if !c.IsNil() {
		t.Fatal("zero value should be nil")
	}
	if c.IsOwner() {
		t.Fatal("zero value should not be owner")
	}
	if c.Bytes() != nil {
		t.Fatal("zero value Bytes should be nil")
	}
	c.Destroy() // should not panic
}

func TestCPtr_Borrow(t *testing.T) {
	owner := NewCPtr(128)
	borrowed := owner.Borrow()

	if borrowed.IsOwner() {
		t.Fatal("Borrow should not be owner")
	}
	if borrowed.Ptr() != owner.Ptr() {
		t.Fatal("Borrow should share same pointer")
	}
	if borrowed.Len() != owner.Len() {
		t.Fatal("Borrow should share same len")
	}

	// Destroy borrowed — should NOT free memory
	borrowed.Destroy()
	if borrowed.IsNil() != true {
		t.Fatal("borrowed after Destroy should be nil")
	}

	// Owner should still be valid
	if owner.IsNil() {
		t.Fatal("owner should still be valid after borrowed.Destroy")
	}
	// Write to owner to verify memory is still accessible
	owner.Bytes()[0] = 0xAB
	if owner.Bytes()[0] != 0xAB {
		t.Fatal("owner memory should still be writable")
	}

	owner.Destroy()
}

func TestCPtr_Transfer(t *testing.T) {
	original := NewCPtr(256)
	original.Bytes()[0] = 0x42

	transferred := original.Transfer()

	// original should no longer be owner
	if original.IsOwner() {
		t.Fatal("after Transfer, original should not be owner")
	}
	// transferred should be owner
	if !transferred.IsOwner() {
		t.Fatal("after Transfer, new should be owner")
	}
	// same pointer
	if original.Ptr() != transferred.Ptr() {
		t.Fatal("Transfer should keep same pointer")
	}
	// data preserved
	if transferred.Bytes()[0] != 0x42 {
		t.Fatal("Transfer should preserve data")
	}

	// Destroy original (non-owner) — no-op
	original.Destroy()

	// transferred still valid
	if transferred.IsNil() {
		t.Fatal("transferred should still be valid")
	}
	if transferred.Bytes()[0] != 0x42 {
		t.Fatal("transferred data should be intact")
	}

	transferred.Destroy()
	if !transferred.IsNil() {
		t.Fatal("after Destroy, transferred should be nil")
	}
}

func TestCPtr_NewZeroSize(t *testing.T) {
	c := NewCPtr(0)
	if !c.IsNil() {
		t.Fatal("NewCPtr(0) should be nil")
	}
	if c.IsOwner() {
		t.Fatal("NewCPtr(0) should not be owner")
	}
	c.Destroy() // no panic
}
