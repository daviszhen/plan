package storage

import (
	"sync"
	"sync/atomic"

	"github.com/daviszhen/plan/pkg/util"
)

type SegmentBase[T any] interface {
	Start() IdxType
	Index() IdxType
	Count() uint64
	Next() SegmentBase[T]
	SetNext(SegmentBase[T])
	SetIndex(IdxType)
	SetCount(uint64)
	SetStart(IdxType)
	AddCount(uint64)
	SetValid(bool)
	IsValid() bool
	Load()
}

type SegmentBaseImpl[T any] struct {
	_start IdxType
	_index IdxType
	_count atomic.Uint64
	_next  atomic.Value
	_valid atomic.Bool
}

func (impl *SegmentBaseImpl[T]) IsValid() bool {
	return impl._valid.Load()
}

func (impl *SegmentBaseImpl[T]) SetValid(b bool) {
	impl._valid.Store(b)
}

func (impl *SegmentBaseImpl[T]) SetCount(cnt uint64) {
	impl._count.Store(cnt)
}

func (impl *SegmentBaseImpl[T]) AddCount(cnt uint64) {
	impl._count.Add(cnt)
}

func (impl *SegmentBaseImpl[T]) Start() IdxType {
	return impl._start
}

func (impl *SegmentBaseImpl[T]) SetStart(val IdxType) {
	impl._start = val
}

func (impl *SegmentBaseImpl[T]) Index() IdxType {
	return impl._index
}

func (impl *SegmentBaseImpl[T]) Count() uint64 {
	return impl._count.Load()
}

func (impl *SegmentBaseImpl[T]) Next() SegmentBase[T] {
	return impl._next.Load().(SegmentBase[T])
}

func (impl *SegmentBaseImpl[T]) SetNext(s SegmentBase[T]) {
	impl._next.Store(s)
}

func (impl *SegmentBaseImpl[T]) SetIndex(idxType IdxType) {
	impl._index = idxType
}

func (impl *SegmentBaseImpl[T]) Load() {

}

type SegmentNode[T any] struct {
	_rowStart IdxType
	_node     T
}

type SegmentTree[T any] struct {
	_lock  sync.Locker
	_nodes []*SegmentNode[SegmentBase[T]]
}

func NewSegmentTree[T any]() *SegmentTree[T] {
	ret := &SegmentTree[T]{
		_lock: util.NewReentryLock(),
	}
	return ret
}

func (tree *SegmentTree[T]) Lock() sync.Locker {
	tree._lock.Lock()
	return tree._lock
}

func (tree *SegmentTree[T]) IsEmpty(lock sync.Locker) bool {
	if lock != nil {
		lock.Lock()
		defer lock.Unlock()
	} else {
		tree._lock.Lock()
		defer tree._lock.Unlock()
	}
	return len(tree._nodes) == 0
}

func (tree *SegmentTree[T]) GetRootSegment(lock sync.Locker) SegmentBase[T] {
	if lock != nil {
		lock.Lock()
		defer lock.Unlock()
	} else {
		tree._lock.Lock()
		defer tree._lock.Unlock()
	}
	if len(tree._nodes) == 0 {
		return nil
	}
	return tree._nodes[0]._node
}

func (tree *SegmentTree[T]) MoveSegments(lock sync.Locker) []*SegmentNode[SegmentBase[T]] {
	if lock != nil {
		lock.Lock()
		defer lock.Unlock()
	} else {
		tree._lock.Lock()
		defer tree._lock.Unlock()
	}
	old := tree._nodes
	tree._nodes = []*SegmentNode[SegmentBase[T]]{}
	return old
}

func (tree *SegmentTree[T]) GetSegmentCount(lock sync.Locker) IdxType {
	if lock != nil {
		lock.Lock()
		defer lock.Unlock()
	} else {
		tree._lock.Lock()
		defer tree._lock.Unlock()
	}
	return IdxType(len(tree._nodes))
}

func (tree *SegmentTree[T]) GetSegmentByIndex(lock sync.Locker, idx IdxType) SegmentBase[T] {
	if lock != nil {
		lock.Lock()
		defer lock.Unlock()
	} else {
		tree._lock.Lock()
		defer tree._lock.Unlock()
	}
	util.AssertFunc(idx >= 0)
	if idx >= IdxType(len(tree._nodes)) {
		return nil
	}
	return tree._nodes[idx]._node
}

func (tree *SegmentTree[T]) GetNextSegment(lock sync.Locker, current SegmentBase[T]) SegmentBase[T] {
	if current == nil {
		return nil
	}
	if lock != nil {
		lock.Lock()
		defer lock.Unlock()
	} else {
		tree._lock.Lock()
		defer tree._lock.Unlock()
	}
	return tree.GetSegmentByIndex(lock, current.Index()+1)
}

func (tree *SegmentTree[T]) GetLastSegment(lock sync.Locker) SegmentBase[T] {
	if lock != nil {
		lock.Lock()
		defer lock.Unlock()
	} else {
		tree._lock.Lock()
		defer tree._lock.Unlock()
	}
	return util.Back(tree._nodes)._node
}

func (tree *SegmentTree[T]) GetSegment(lock sync.Locker, rowNumber IdxType) SegmentBase[T] {
	if lock != nil {
		lock.Lock()
		defer lock.Unlock()
	} else {
		tree._lock.Lock()
		defer tree._lock.Unlock()
	}
	idx := tree.GetSegmentIdx(lock, rowNumber)
	return tree._nodes[idx]._node
}

func (tree *SegmentTree[T]) AppendSegment(lock sync.Locker, seg SegmentBase[T]) {
	if lock != nil {
		lock.Lock()
		defer lock.Unlock()
	} else {
		tree._lock.Lock()
		defer tree._lock.Unlock()
	}
	if len(tree._nodes) != 0 {
		last := util.Back(tree._nodes)
		last._node.SetNext(seg)
	}
	seg.SetIndex(IdxType(len(tree._nodes)))
	node := &SegmentNode[SegmentBase[T]]{
		_rowStart: seg.Start(),
		_node:     seg,
	}
	tree._nodes = append(tree._nodes, node)
}

func (tree *SegmentTree[T]) HasSegment(lock sync.Locker, seg SegmentBase[T]) bool {
	if lock != nil {
		lock.Lock()
		defer lock.Unlock()
	} else {
		tree._lock.Lock()
		defer tree._lock.Unlock()
	}
	return seg.Index() < IdxType(len(tree._nodes)) &&
		tree._nodes[seg.Index()]._node == seg
}

func (tree *SegmentTree[T]) Replace(lock sync.Locker, other *SegmentTree[T]) {
	if lock != nil {
		lock.Lock()
		defer lock.Unlock()
	} else {
		tree._lock.Lock()
		defer tree._lock.Unlock()
	}
	tree._nodes = other._nodes
}

// EraseSegments erase Segments after segStart
func (tree *SegmentTree[T]) EraseSegments(lock sync.Locker, segStart IdxType) {
	if lock != nil {
		lock.Lock()
		defer lock.Unlock()
	} else {
		tree._lock.Lock()
		defer tree._lock.Unlock()
	}
	if segStart >= IdxType(len(tree._nodes)-1) {
		return
	}
	tree._nodes = tree._nodes[:segStart]
}

func (tree *SegmentTree[T]) Reinitialize(lock sync.Locker) {
	if lock != nil {
		lock.Lock()
		defer lock.Unlock()
	} else {
		tree._lock.Lock()
		defer tree._lock.Unlock()
	}
	if len(tree._nodes) == 0 {
		return
	}
	offset := tree._nodes[0]._node.Start()
	for i := 0; i < len(tree._nodes); i++ {
		n := tree._nodes[i]
		if n._node.Start() != offset {
			panic("not continuous")
		}
		n._rowStart = offset
		offset += IdxType(n._node.Count())
	}
}

func (tree *SegmentTree[T]) GetSegmentIdx(lock sync.Locker, rowNumber IdxType) IdxType {
	if lock != nil {
		lock.Lock()
		defer lock.Unlock()
	} else {
		tree._lock.Lock()
		defer tree._lock.Unlock()
	}
	if len(tree._nodes) == 0 {
		panic("not found 1")
	}
	for i, node := range tree._nodes {
		if rowNumber >= node._rowStart &&
			rowNumber < node._rowStart+IdxType(node._node.Count()) {
			return IdxType(i)
		}
	}
	panic("not found 2")
}

func (tree *SegmentTree[T]) LoadSegment() (SegmentBase[T], error) {
	panic("usp")
}
