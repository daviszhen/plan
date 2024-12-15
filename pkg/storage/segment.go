package storage

import (
	"sync"
	"sync/atomic"

	"github.com/daviszhen/plan/pkg/util"
)

type SegmentBase[T any] struct {
	_start IdxType
	_index IdxType
	_count atomic.Uint64
	_next  atomic.Pointer[SegmentBase[T]]
}

func (s *SegmentBase[T]) Next() *SegmentBase[T] {
	return s._next.Load()
}

type SegmentNode[T any] struct {
	_rowStart IdxType
	_node     *T
}

type SegmentTree[T any] struct {
	_lock  sync.Mutex
	_nodes []SegmentNode[SegmentBase[T]]
}

func NewSegmentTree[T any]() *SegmentTree[T] {
	ret := &SegmentTree[T]{}
	return ret
}

func (tree *SegmentTree[T]) Lock() func() {
	tree._lock.Lock()
	return func() {
		tree._lock.Unlock()
	}
}

func (tree *SegmentTree[T]) IsEmpty() bool {
	tree._lock.Lock()
	defer tree._lock.Unlock()
	return len(tree._nodes) == 0
}

func (tree *SegmentTree[T]) AppendSegment(seg *SegmentBase[T]) {
	tree._lock.Lock()
	defer tree._lock.Unlock()
	if len(tree._nodes) != 0 {
		last := util.Back(tree._nodes)
		last._node._next.Store(seg)
	}
	seg._index = IdxType(len(tree._nodes))
	node := SegmentNode[SegmentBase[T]]{
		_rowStart: seg._start,
		_node:     seg,
	}
	tree._nodes = append(tree._nodes, node)
}

func (tree *SegmentTree[T]) GetLastSegment() *SegmentBase[T] {
	tree._lock.Lock()
	defer tree._lock.Unlock()
	return util.Back(tree._nodes)._node
}

func (tree *SegmentTree[T]) Reinitialize() {
	tree._lock.Lock()
	defer tree._lock.Unlock()
	if len(tree._nodes) == 0 {
		return
	}

}

func (tree *SegmentTree) GetRootSegment() *ColumnSegment {
	return tree._nodes.First().Value()._node
}

func (tree *SegmentTree) GetNextSegment(current *ColumnSegment) *ColumnSegment {
	return tree.GetSegmentByIndex(current._index + 1)
}

func (tree *SegmentTree) GetSegmentByIndex(idx IdxType) *ColumnSegment {
	util.AssertFunc(idx >= 0)
	temp := SegmentNode[ColumnSegment]{
		_rowStart: idx,
	}
	iter := tree._nodes.Find(temp)
	if iter.IsValid() {
		return iter.Value()._node
	} else {
		return nil
	}
}

func (tree *SegmentTree) GetSegment(rowNumber IdxType) *ColumnSegment {
	idx := tree.GetSegmentIdx(rowNumber)
	iter := tree._nodes.Find(SegmentNode[ColumnSegment]{
		_rowStart: idx,
	})
	return iter.Value()._node
}

func (tree *SegmentTree) GetSegmentIdx(rowNumber IdxType) IdxType {
	for iter := tree._nodes.Begin(); iter.IsValid(); iter.Next() {
		node := iter.Value()
		if node._rowStart <= rowNumber && rowNumber < node._rowStart+IdxType(node._node._count.Load()) {
			return node._rowStart
		}
	}
	panic("usp")
}
