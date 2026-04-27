package util

import (
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
