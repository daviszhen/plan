package main

import "fmt"

type PhysicalFormat int

const (
	PF_FLAT PhysicalFormat = iota
	PF_CONSTANT
	PF_DICT
)

func (f PhysicalFormat) String() string {
	switch f {
	case PF_FLAT:
		return "flat"
	case PF_CONSTANT:
		return "constant"
	case PF_DICT:
		return "dictionary"
	}
	panic(fmt.Sprintf("usp %d", f))
}

type BufferType int

const (
	BT_STANDARD BufferType = iota
	BT_DICT
	BT_CHILD
	BT_STRING
)

type Buffer struct {
	_bufTyp BufferType
	_data   []byte
}

type Vector struct {
	_phyFormat PhysicalFormat
	_typ       LType
	_data      []byte
	_bitmap    *Bitmap
	_buf       *Buffer
	_aux       *Buffer
}

type Bitmap struct {
	_bits []uint8
}

type SelectVector struct {
	_selVec []int
}

type Chunk struct {
	_data []Vector
}

type UnifiedVector struct {
}

func And(left, right, result *Vector, count int) {

}
