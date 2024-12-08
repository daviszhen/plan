package util

import (
	"sync"
	"sync/atomic"

	"github.com/petermattis/goid"
)

type ReentryLock struct {
	mu    sync.Mutex
	cond  *sync.Cond
	owner atomic.Int64
	count atomic.Uint64
}

func NewReentryLock() *ReentryLock {
	lock := &ReentryLock{}
	lock.cond = sync.NewCond(&lock.mu)
	return lock
}

func (lock *ReentryLock) Lock() {
	rid := goid.Get()
	//fmt.Println("lock", rid)
	lock.mu.Lock()
	defer lock.mu.Unlock()
	if lock.owner.Load() == rid {
		lock.count.Add(1)
		return
	}
	for lock.owner.Load() != 0 {
		lock.cond.Wait()
	}
	lock.owner.Store(rid)
	lock.count.Store(1)
}

func (lock *ReentryLock) Unlock() {
	rid := goid.Get()
	//fmt.Println("unlock", rid)
	yes := false
	lock.mu.Lock()
	defer func() {
		lock.mu.Unlock()
		if yes {
			lock.cond.Signal()
		}
	}()

	if lock.count.Load() == 0 || lock.owner.Load() != rid {
		panic("unlock of unlocked mutex")
	}
	lock.count.Add(^uint64(0))
	if lock.count.Load() == 0 {
		lock.owner.Store(0)
		yes = true
	}
}

var _ sync.Locker = (*ReentryLock)(nil)
