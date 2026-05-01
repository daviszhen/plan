package util

import (
	"math"
	"testing"
	"unsafe"
)

func BenchmarkPointerMemcmp_8(b *testing.B) {
	benchMemcmp(b, 8)
}

func BenchmarkPointerMemcmp_32(b *testing.B) {
	benchMemcmp(b, 32)
}

func BenchmarkPointerMemcmp_128(b *testing.B) {
	benchMemcmp(b, 128)
}

func benchMemcmp(b *testing.B, size int) {
	buf1 := make([]byte, size)
	buf2 := make([]byte, size)
	for i := range buf1 {
		buf1[i] = byte(i)
		buf2[i] = byte(i)
	}
	p1 := unsafe.Pointer(&buf1[0])
	p2 := unsafe.Pointer(&buf2[0])
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		PointerMemcmp(p1, p2, size)
	}
}

func TestPointerMemcmp_Correctness(t *testing.T) {
	cases := []struct {
		a, b []byte
		want int
	}{
		{[]byte{}, []byte{}, 0},
		{[]byte{1}, []byte{1}, 0},
		{[]byte{1}, []byte{2}, -1},
		{[]byte{2}, []byte{1}, 1},
		{[]byte{1, 2, 3}, []byte{1, 2, 3}, 0},
		{[]byte{1, 2, 3}, []byte{1, 2, 4}, -1},
		{[]byte{1, 2, 4}, []byte{1, 2, 3}, 1},
		{[]byte{0, 0, 0, 0, 0, 0, 0, 1}, []byte{0, 0, 0, 0, 0, 0, 0, 2}, -1},
	}
	for _, tc := range cases {
		var p1, p2 unsafe.Pointer
		if len(tc.a) > 0 {
			p1 = unsafe.Pointer(&tc.a[0])
		}
		if len(tc.b) > 0 {
			p2 = unsafe.Pointer(&tc.b[0])
		}
		got := PointerMemcmp(p1, p2, len(tc.a))
		if got != tc.want {
			t.Errorf("PointerMemcmp(%v, %v) = %d, want %d", tc.a, tc.b, got, tc.want)
		}
	}
}

func TestPointerMemcmp2_Correctness(t *testing.T) {
	cases := []struct {
		a, b []byte
		want int
	}{
		{[]byte{1, 2}, []byte{1, 2, 3}, -1},
		{[]byte{1, 2, 3}, []byte{1, 2}, 1},
		{[]byte{1, 2, 3}, []byte{1, 2, 3}, 0},
		{[]byte{2}, []byte{1, 2, 3}, 1},
	}
	for _, tc := range cases {
		p1 := unsafe.Pointer(&tc.a[0])
		p2 := unsafe.Pointer(&tc.b[0])
		got := PointerMemcmp2(p1, p2, len(tc.a), len(tc.b))
		if got != tc.want {
			t.Errorf("PointerMemcmp2(%v, %v) = %d, want %d", tc.a, tc.b, got, tc.want)
		}
	}
}

func TestMemset(t *testing.T) {
	buf := make([]byte, 1024)
	Memset(unsafe.Pointer(&buf[0]), 0xAB, len(buf))
	for i, b := range buf {
		if b != 0xAB {
			t.Fatalf("Memset: buf[%d] = %x, want 0xAB", i, b)
		}
	}
	// size=0 should not panic
	Memset(unsafe.Pointer(&buf[0]), 0, 0)
	// size=1
	Memset(unsafe.Pointer(&buf[0]), 0xFF, 1)
	if buf[0] != 0xFF {
		t.Fatalf("Memset size=1 failed")
	}
}

func BenchmarkMemset_256(b *testing.B) {
	buf := make([]byte, 256)
	ptr := unsafe.Pointer(&buf[0])
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Memset(ptr, 0, 256)
	}
}

func BenchmarkMemset_4096(b *testing.B) {
	buf := make([]byte, 4096)
	ptr := unsafe.Pointer(&buf[0])
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Memset(ptr, 0, 4096)
	}
}

func TestFill(t *testing.T) {
	data := make([]uint64, 2048)
	Fill[uint64](data, 2048, 42)
	for i, v := range data {
		if v != 42 {
			t.Fatalf("Fill: data[%d] = %d, want 42", i, v)
		}
	}
	// count=0 should not panic
	Fill[uint64](data, 0, 0)
}

func BenchmarkFill_2048(b *testing.B) {
	data := make([]uint64, 2048)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Fill[uint64](data, 2048, 0)
	}
}

func TestInvertBits(t *testing.T) {
	buf := []byte{0x00, 0xFF, 0xA5}
	InvertBits(unsafe.Pointer(&buf[0]), 0)
	if buf[0] != 0xFF {
		t.Fatalf("InvertBits: got %x, want 0xFF", buf[0])
	}
	InvertBits(unsafe.Pointer(&buf[0]), 1)
	if buf[1] != 0x00 {
		t.Fatalf("InvertBits: got %x, want 0x00", buf[1])
	}
	InvertBits(unsafe.Pointer(&buf[0]), 2)
	if buf[2] != 0x5A {
		t.Fatalf("InvertBits: got %x, want 0x5A", buf[2])
	}
}

func TestPointerSub(t *testing.T) {
	buf := make([]byte, 100)
	base := unsafe.Pointer(&buf[0])
	end := unsafe.Pointer(&buf[99])
	if PointerSub(end, base) != 99 {
		t.Fatalf("PointerSub forward: got %d, want 99", PointerSub(end, base))
	}
	// When lhs < rhs, original semantics: negate the int64 of (lhs-rhs)
	// For nearby pointers this gives the positive absolute distance
	if PointerSub(base, end) != 99 {
		t.Fatalf("PointerSub backward: got %d, want 99", PointerSub(base, end))
	}
	// Edge case: MaxUint64 pointer (used in sort_test.go)
	p := unsafe.Pointer(&buf[0])
	maxPtr := unsafe.Pointer(uintptr(math.MaxUint64))
	if PointerSub(p, maxPtr) >= 0 {
		t.Fatalf("PointerSub with MaxUint64: expected negative, got %d", PointerSub(p, maxPtr))
	}
}
