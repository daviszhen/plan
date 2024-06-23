package main

import (
	"fmt"
	"math"
	"math/rand"
	"sort"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_comp(t *testing.T) {
	type args struct {
		a      []int
		b      []int
		aVal   int
		bVal   int
		wanted bool
	}
	tests := []args{
		{
			a:      []int{0},
			b:      []int{0},
			aVal:   2,
			bVal:   1,
			wanted: false,
		},
		{
			a:      []int{0},
			b:      []int{0},
			aVal:   1,
			bVal:   2,
			wanted: true,
		},
		{
			a:      []int{0},
			b:      []int{0},
			aVal:   1,
			bVal:   1,
			wanted: false,
		},
		{
			a:      []int{0},
			b:      []int{0},
			aVal:   -2,
			bVal:   -1,
			wanted: true,
		},
		{
			a:      []int{0},
			b:      []int{0},
			aVal:   -1,
			bVal:   -2,
			wanted: false,
		},
		{
			a:      []int{0},
			b:      []int{0},
			aVal:   -1,
			bVal:   -1,
			wanted: false,
		},
	}
	for _, test := range tests {
		enc := intEncoder{}
		aPtr := unsafe.Pointer(&test.a[0])
		enc.EncodeData(aPtr, &test.aVal)
		bPtr := unsafe.Pointer(&test.b[0])
		enc.EncodeData(bPtr, &test.bVal)
		cPtr := unsafe.Pointer(uintptr(math.MaxUint64))
		require.True(t, pointerLess(aPtr, cPtr))
		require.True(t, pointerSub(bPtr, cPtr) < 0)
		require.True(t, pointerLess(bPtr, cPtr))
		require.True(t, pointerSub(bPtr, cPtr) < 0)
		ret := comp(
			unsafe.Pointer(&test.a[0]),
			unsafe.Pointer(&test.b[0]),
			&PDQConstants{
				_compSize: 4,
				_end:      unsafe.Pointer(uintptr(math.MaxUint64)),
			})
		if test.wanted {
			require.True(t, ret)
		} else {
			require.False(t, ret)
		}
	}
}

func Test_insertSort1(t *testing.T) {
	type kase struct {
		src []int
	}
	tests := []kase{
		{
			src: []int{10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0},
		},
		{
			src: []int{3, 2, 1, 10, 9, 8, 7, 6, 5, 4, 0},
		},
		{
			src: []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
		},
		{
			src: []int{0, 0, 0, 0, 0, 0},
		},
		{
			src: []int{math.MaxInt, math.MaxInt, math.MaxInt, math.MaxInt},
		},
	}
	for _, tt := range tests {
		data := make([]int, len(tt.src))
		entrySize := int(unsafe.Sizeof(data[0]))
		assert.NotEqual(t, entrySize, int(unsafe.Sizeof(int32(0))))
		enc := intEncoder{}
		for i, x := range tt.src {
			addr := unsafe.Pointer(&data[i])
			enc.EncodeData(addr, &x)
			//fmt.Printf("0x%p %d %d\n", addr, x, load[int](addr))
		}
		ret := sort.SliceIsSorted(data, func(i, j int) bool {
			return data[i] < data[j]
		})
		fmt.Println("sorted", ret, data)

		n := len(data)
		begin := NewPDQIterator(
			unsafe.Pointer(&data[0]),
			entrySize,
		)
		end := begin.plusCopy(n)
		//print
		for iter := begin.plusCopy(0); pdqIterNotEqaul(&iter, &end); iter.plus(1) {
			fmt.Printf("0x%p %d \n", iter.ptr(), load[int](iter.ptr()))
		}
		fmt.Println()
		constants := NewPDQConstants(
			entrySize,
			0,
			entrySize,
			end.ptr(),
		)

		insertSort(begin, &end, constants)
		ret = sort.SliceIsSorted(data, func(i, j int) bool {
			return data[i] < data[j]
		})
		require.True(t, ret)
	}
}

func Test_insertSort2(t *testing.T) {
	type kase struct {
		src []int32
	}
	tests := []kase{
		{
			src: []int32{10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0},
		},
		{
			src: []int32{3, 2, 1, 10, 9, 8, 7, 6, 5, 4, 0},
		},
		{
			src: []int32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
		},
		{
			src: []int32{0, 0, 0, 0, 0, 0},
		},
		{
			src: []int32{math.MaxInt32, math.MaxInt32, math.MaxInt32, math.MaxInt32},
		},
	}
	for _, tt := range tests {
		data := make([]int32, len(tt.src))
		entrySize := int(unsafe.Sizeof(data[0]))
		assert.Equal(t, entrySize, int(unsafe.Sizeof(int32(0))))
		enc := int32Encoder{}
		for i, x := range tt.src {
			addr := unsafe.Pointer(&data[i])
			enc.EncodeData(addr, &x)
			//fmt.Printf("0x%p %d %d\n", addr, x, load[int](addr))
		}
		ret := sort.SliceIsSorted(data, func(i, j int) bool {
			return data[i] < data[j]
		})
		fmt.Println("sorted", ret, data)

		n := len(data)
		begin := NewPDQIterator(
			unsafe.Pointer(&data[0]),
			entrySize,
		)
		end := begin.plusCopy(n)
		//print
		for iter := begin.plusCopy(0); pdqIterNotEqaul(&iter, &end); iter.plus(1) {
			fmt.Printf("0x%p %d \n", iter.ptr(), load[int32](iter.ptr()))
		}
		fmt.Println()
		constants := NewPDQConstants(
			entrySize,
			0,
			entrySize,
			end.ptr(),
		)

		insertSort(begin, &end, constants)
		ret = sort.SliceIsSorted(data, func(i, j int) bool {
			return data[i] < data[j]
		})
		require.True(t, ret)
	}
}

func Test_insertSort3(t *testing.T) {
	type kase struct {
		src []int
	}
	tests := []kase{
		{
			src: []int{0, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0},
		},
		{
			src: []int{0, 3, 2, 1, 10, 9, 8, 7, 6, 5, 4, 0},
		},
		{
			src: []int{0, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
		},
		{
			src: []int{0, 0, 0, 0, 0, 0, 0},
		},
		{
			src: []int{-2, math.MaxInt, math.MaxInt, math.MaxInt, math.MaxInt},
		},
	}
	for _, tt := range tests {
		data := make([]int, len(tt.src))
		entrySize := int(unsafe.Sizeof(data[0]))
		assert.NotEqual(t, entrySize, int(unsafe.Sizeof(int32(0))))
		enc := intEncoder{}
		for i, x := range tt.src {
			addr := unsafe.Pointer(&data[i])
			enc.EncodeData(addr, &x)
			//fmt.Printf("0x%p %d %d\n", addr, x, load[int](addr))
		}
		ret := sort.SliceIsSorted(data, func(i, j int) bool {
			return data[i] < data[j]
		})
		fmt.Println("sorted", ret, data)

		n := len(data)
		begin := NewPDQIterator(
			unsafe.Pointer(&data[1]),
			entrySize,
		)
		end := begin.plusCopy(n - 1)
		//print
		for iter := begin.plusCopy(0); pdqIterNotEqaul(&iter, &end); iter.plus(1) {
			fmt.Printf("0x%p %d \n", iter.ptr(), load[int](iter.ptr()))
		}
		fmt.Println()
		constants := NewPDQConstants(
			entrySize,
			0,
			entrySize,
			end.ptr(),
		)

		unguardedInsertSort(begin, &end, constants)
		fmt.Println(data)
		ret = sort.SliceIsSorted(data, func(i, j int) bool {
			return data[i] < data[j]
		})
		require.True(t, ret)
	}
}

func Test_insertSort4(t *testing.T) {
	type kase struct {
		src []int32
	}
	tests := []kase{
		{
			src: []int32{0, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0},
		},
		{
			src: []int32{0, 3, 2, 1, 10, 9, 8, 7, 6, 5, 4, 0},
		},
		{
			src: []int32{0, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
		},
		{
			src: []int32{0, 0, 0, 0, 0, 0, 0},
		},
		{
			src: []int32{-2, math.MaxInt32, math.MaxInt32, math.MaxInt32, math.MaxInt32},
		},
	}
	for _, tt := range tests {
		data := make([]int, len(tt.src))
		entrySize := int(unsafe.Sizeof(data[0]))
		assert.NotEqual(t, entrySize, int(unsafe.Sizeof(int32(0))))
		enc := int32Encoder{}
		for i, x := range tt.src {
			addr := unsafe.Pointer(&data[i])
			enc.EncodeData(addr, &x)
			//fmt.Printf("0x%p %d %d\n", addr, x, load[int](addr))
		}
		ret := sort.SliceIsSorted(data, func(i, j int) bool {
			return data[i] < data[j]
		})
		fmt.Println("sorted", ret, data)

		n := len(data)
		begin := NewPDQIterator(
			unsafe.Pointer(&data[1]),
			entrySize,
		)
		end := begin.plusCopy(n - 1)
		//print
		for iter := begin.plusCopy(0); pdqIterNotEqaul(&iter, &end); iter.plus(1) {
			fmt.Printf("0x%p %d \n", iter.ptr(), load[int](iter.ptr()))
		}
		fmt.Println()
		constants := NewPDQConstants(
			entrySize,
			0,
			entrySize,
			end.ptr(),
		)

		unguardedInsertSort(begin, &end, constants)
		fmt.Println(data)
		ret = sort.SliceIsSorted(data, func(i, j int) bool {
			return data[i] < data[j]
		})
		require.True(t, ret)
	}
}

func Test_pdqsort_branchless1(t *testing.T) {
	type kase struct {
		src []int
	}
	n := 2
	tests := []kase{
		{
			src: []int{-4596258735388573295, 6950034312330823304},
		},
		{
			src: []int{-7943962452447775168, -8627136175431519264},
		},
	}
	for _, tt := range tests {
		if len(tt.src) == 0 {
			tt.src = make([]int, n)
			for i := range tt.src {
				tt.src[i] = rand.Int()
			}
		}

		data := make([]int, len(tt.src))
		entrySize := int(unsafe.Sizeof(data[0]))
		assert.NotEqual(t, entrySize, int(unsafe.Sizeof(int32(0))))
		enc := intEncoder{}
		for i, x := range tt.src {
			addr := unsafe.Pointer(&data[i])
			enc.EncodeData(addr, &x)
			//fmt.Printf("0x%p %d %d\n", addr, x, load[int](addr))
		}
		ret := sort.SliceIsSorted(data, func(i, j int) bool {
			return data[i] < data[j]
		})
		fmt.Println("sorted", ret, data)

		n := len(data)
		begin := NewPDQIterator(
			unsafe.Pointer(&data[0]),
			entrySize,
		)
		end := begin.plusCopy(n)
		//print
		for iter := begin.plusCopy(0); pdqIterNotEqaul(&iter, &end); iter.plus(1) {
			fmt.Printf("0x%p %d \n", iter.ptr(), load[int](iter.ptr()))
		}
		fmt.Println()
		constants := NewPDQConstants(
			entrySize,
			0,
			entrySize,
			end.ptr(),
		)

		pdqsortBranchless(begin, &end, constants)
		ret = sort.SliceIsSorted(data, func(i, j int) bool {
			return data[i] < data[j]
		})
		fmt.Println(data)
		require.True(t, ret)
	}
}

func Test_rand(t *testing.T) {
	for i := 0; i < 1000; i++ {

	}
}
