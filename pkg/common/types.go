package common

import (
	"unsafe"
)

var (
	BoolSize     int
	Int8Size     int
	Int16Size    int
	Int32Size    int
	Int64Size    int
	Int128Size   int
	IntervalSize int
	DateSize     int
	VarcharSize  int
	PointerSize  int
	DecimalSize  int
	Float32Size  int
)

func init() {
	b := false
	BoolSize = int(unsafe.Sizeof(b))
	i := int8(0)
	Int8Size = int(unsafe.Sizeof(i))
	Int16Size = Int8Size * 2
	Int32Size = Int8Size * 4
	Int64Size = Int8Size * 8
	Int128Size = int(unsafe.Sizeof(Hugeint{}))
	IntervalSize = int(unsafe.Sizeof(Interval{}))
	DateSize = int(unsafe.Sizeof(Date{}))
	VarcharSize = int(unsafe.Sizeof(String{}))
	PointerSize = int(unsafe.Sizeof(unsafe.Pointer(&b)))
	DecimalSize = int(unsafe.Sizeof(Decimal{}))
	f := float32(0)
	Float32Size = int(unsafe.Sizeof(f))
}
