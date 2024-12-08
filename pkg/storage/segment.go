package storage

import (
	"sync/atomic"
)

type SegmentBase[T any] struct {
	_start IdxType
	_index IdxType
	_count atomic.Uint64
	_next  atomic.Pointer[T]
}

func (s *SegmentBase[T]) Next() *T {
	return s._next.Load()
}
