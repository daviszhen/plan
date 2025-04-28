package btree

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/daviszhen/plan/pkg/util"
	"github.com/stretchr/testify/assert"
)

func TestGetMyLockedPageIndex(t *testing.T) {
	// 测试用例1：空列表
	t.Run("empty list", func(t *testing.T) {
		tPages := &ThreadPages{
			numberOfMyLockedPages: 0,
		}
		index := getMyLockedPageIndex(tPages, 1)
		assert.Equal(t, -1, index)
	})

	// 测试用例2：页面存在
	t.Run("page exists", func(t *testing.T) {
		tPages := &ThreadPages{
			numberOfMyLockedPages: 3,
		}
		tPages.myLockedPages[0] = MyLockedPage{blkno: 1}
		tPages.myLockedPages[1] = MyLockedPage{blkno: 2}
		tPages.myLockedPages[2] = MyLockedPage{blkno: 3}

		index := getMyLockedPageIndex(tPages, 2)
		assert.Equal(t, 1, index)
	})

	// 测试用例3：页面不存在
	t.Run("page not exists", func(t *testing.T) {
		tPages := &ThreadPages{
			numberOfMyLockedPages: 3,
		}
		tPages.myLockedPages[0] = MyLockedPage{blkno: 1}
		tPages.myLockedPages[1] = MyLockedPage{blkno: 2}
		tPages.myLockedPages[2] = MyLockedPage{blkno: 3}

		index := getMyLockedPageIndex(tPages, 4)
		assert.Equal(t, -1, index)
	})

	// 测试用例4：第一个页面
	t.Run("first page", func(t *testing.T) {
		tPages := &ThreadPages{
			numberOfMyLockedPages: 3,
		}
		tPages.myLockedPages[0] = MyLockedPage{blkno: 1}
		tPages.myLockedPages[1] = MyLockedPage{blkno: 2}
		tPages.myLockedPages[2] = MyLockedPage{blkno: 3}

		index := getMyLockedPageIndex(tPages, 1)
		assert.Equal(t, 0, index)
	})

	// 测试用例5：最后一个页面
	t.Run("last page", func(t *testing.T) {
		tPages := &ThreadPages{
			numberOfMyLockedPages: 3,
		}
		tPages.myLockedPages[0] = MyLockedPage{blkno: 1}
		tPages.myLockedPages[1] = MyLockedPage{blkno: 2}
		tPages.myLockedPages[2] = MyLockedPage{blkno: 3}

		index := getMyLockedPageIndex(tPages, 3)
		assert.Equal(t, 2, index)
	})

	// 测试用例6：部分填充的列表
	t.Run("partially filled list", func(t *testing.T) {
		tPages := &ThreadPages{
			numberOfMyLockedPages: 3,
		}
		tPages.myLockedPages[0] = MyLockedPage{blkno: 1}
		tPages.myLockedPages[1] = MyLockedPage{blkno: 2}
		tPages.myLockedPages[2] = MyLockedPage{blkno: 3}

		index := getMyLockedPageIndex(tPages, 2)
		assert.Equal(t, 1, index)
	})

	// 测试用例7：无效的 blkno
	t.Run("invalid blkno", func(t *testing.T) {
		tPages := &ThreadPages{
			numberOfMyLockedPages: 3,
		}
		tPages.myLockedPages[0] = MyLockedPage{blkno: 1}
		tPages.myLockedPages[1] = MyLockedPage{blkno: 2}
		tPages.myLockedPages[2] = MyLockedPage{blkno: 3}

		index := getMyLockedPageIndex(tPages, 0) // 假设 0 是无效的 blkno
		assert.Equal(t, -1, index)
	})

	// 测试用例8：并发安全
	t.Run("concurrent access", func(t *testing.T) {
		tPages := &ThreadPages{
			numberOfMyLockedPages: 8,
		}
		// 填充测试数据
		for i := 0; i < 8; i++ {
			tPages.myLockedPages[i] = MyLockedPage{blkno: Blkno(i + 1)}
		}

		// 并发访问测试
		done := make(chan bool)
		for i := 0; i < 10; i++ {
			go func() {
				for j := 0; j < 1000; j++ {
					index := getMyLockedPageIndex(tPages, Blkno(j%8+1))
					assert.True(t, index >= -1 && index < 8)
				}
				done <- true
			}()
		}

		// 等待所有 goroutine 完成
		for i := 0; i < 10; i++ {
			<-done
		}
	})
}

func BenchmarkGetMyLockedPageIndex(b *testing.B) {
	tPages := &ThreadPages{
		numberOfMyLockedPages: 8,
	}
	// 填充测试数据
	for i := 0; i < 8; i++ {
		tPages.myLockedPages[i] = MyLockedPage{blkno: Blkno(i + 1)}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		getMyLockedPageIndex(tPages, Blkno(i%8+1))
	}
}

func TestGetMyLockedPageIndexEdgeCases(t *testing.T) {
	// 测试最大 blkno
	t.Run("max blkno", func(t *testing.T) {
		tPages := &ThreadPages{
			numberOfMyLockedPages: 1,
		}
		tPages.myLockedPages[0] = MyLockedPage{blkno: ^Blkno(0)} // 最大可能的 blkno

		index := getMyLockedPageIndex(tPages, ^Blkno(0))
		assert.Equal(t, 0, index)
	})

	// 测试最小 blkno
	t.Run("min blkno", func(t *testing.T) {
		tPages := &ThreadPages{
			numberOfMyLockedPages: 1,
		}
		tPages.myLockedPages[0] = MyLockedPage{blkno: 1}

		index := getMyLockedPageIndex(tPages, 1)
		assert.Equal(t, 0, index)
	})

	// 测试重复的 blkno
	t.Run("duplicate blkno", func(t *testing.T) {
		tPages := &ThreadPages{
			numberOfMyLockedPages: 3,
		}
		tPages.myLockedPages[0] = MyLockedPage{blkno: 1}
		tPages.myLockedPages[1] = MyLockedPage{blkno: 1} // 重复的 blkno
		tPages.myLockedPages[2] = MyLockedPage{blkno: 2}

		index := getMyLockedPageIndex(tPages, 1)
		assert.Equal(t, 0, index) // 应该返回第一个匹配项
	})
}

// 注意：Go 1.18+ 支持模糊测试
func FuzzGetMyLockedPageIndex(f *testing.F) {
	// 添加种子值
	f.Add(uint64(1), 10)
	f.Add(uint64(100), 1000)

	f.Fuzz(func(t *testing.T, blkno uint64, size int) {
		if size <= 0 || size > 8 {
			t.Skip("size out of range")
		}

		tPages := &ThreadPages{
			numberOfMyLockedPages: size,
		}
		// 填充测试数据
		for i := 0; i < size; i++ {
			tPages.myLockedPages[i] = MyLockedPage{blkno: Blkno(i + 1)}
		}

		index := getMyLockedPageIndex(tPages, Blkno(blkno))
		assert.True(t, index >= -1 && index < size)
	})
}

func TestMyLockedPageAdd(t *testing.T) {
	// 测试用例1：正常添加页面
	t.Run("normal add", func(t *testing.T) {
		tPages := GetThreadPages()
		tPages.numberOfMyLockedPages = 0 // 重置页面数量
		myLockedPageAdd(1, 1)
		assert.Equal(t, 1, tPages.numberOfMyLockedPages)
		assert.Equal(t, Blkno(1), tPages.myLockedPages[0].blkno)
		assert.Equal(t, uint32(1), tPages.myLockedPages[0].state)
	})

	// 测试用例2：添加重复页面（应该失败）
	t.Run("duplicate page", func(t *testing.T) {
		tPages := GetThreadPages()
		tPages.numberOfMyLockedPages = 0 // 重置页面数量
		myLockedPageAdd(1, 1)
		assert.Panics(t, func() {
			myLockedPageAdd(1, 2) // 尝试添加重复页面
		})
	})

	// 测试用例3：添加超过最大限制的页面
	t.Run("exceed max pages", func(t *testing.T) {
		tPages := GetThreadPages()
		tPages.numberOfMyLockedPages = 0 // 重置页面数量
		// 添加最大数量的页面
		for i := 0; i < MaxPagesPerProcess; i++ {
			myLockedPageAdd(Blkno(i+1), uint32(i+1))
		}

		// 验证页面数量
		assert.Equal(t, MaxPagesPerProcess, tPages.numberOfMyLockedPages)

		// 尝试添加第9个页面
		assert.Panics(t, func() {
			myLockedPageAdd(Blkno(MaxPagesPerProcess+1), uint32(MaxPagesPerProcess+1))
		})
	})

	// 测试用例4：边界条件测试
	t.Run("boundary conditions", func(t *testing.T) {
		tPages := GetThreadPages()
		tPages.numberOfMyLockedPages = 0 // 重置页面数量
		// 测试添加 blkno 为 0 的页面
		myLockedPageAdd(0, 1)
		assert.Equal(t, Blkno(0), tPages.myLockedPages[0].blkno) // 验证可以添加 blkno 为 0 的页面

		// 测试添加 state 为 0 的页面
		myLockedPageAdd(1, 0)
		assert.Equal(t, uint32(0), tPages.myLockedPages[1].state)

		// 测试添加最大可能的 blkno
		myLockedPageAdd(^Blkno(0), 1)
		assert.Equal(t, ^Blkno(0), tPages.myLockedPages[2].blkno)
	})

	// 测试用例5：并发添加页面
	t.Run("concurrent add", func(t *testing.T) {
		tPages := GetThreadPages()
		tPages.numberOfMyLockedPages = 0 // 重置页面数量

		// 使用单个 goroutine 添加多个页面
		for i := 0; i < 8; i++ {
			blkno := Blkno(i + 1)
			myLockedPageAdd(blkno, uint32(blkno))
		}

		// 验证所有页面都被正确添加
		assert.Equal(t, 8, tPages.numberOfMyLockedPages)
		// 验证页面没有重复
		seen := make(map[Blkno]bool)
		for i := 0; i < tPages.numberOfMyLockedPages; i++ {
			blkno := tPages.myLockedPages[i].blkno
			assert.False(t, seen[blkno], "duplicate blkno found: %d", blkno)
			seen[blkno] = true
		}
	})
}

func TestMyLockedPageDel(t *testing.T) {
	// 测试用例1：正常删除页面
	t.Run("normal delete", func(t *testing.T) {
		tPages := GetThreadPages()
		tPages.numberOfMyLockedPages = 0 // 重置页面数量

		// 添加两个页面
		myLockedPageAdd(1, 1)
		myLockedPageAdd(2, 2)

		// 删除第一个页面
		state := myLockedPageDel(1)
		assert.Equal(t, uint32(1), state)                        // 验证返回的状态
		assert.Equal(t, 1, tPages.numberOfMyLockedPages)         // 验证页面数量减少
		assert.Equal(t, Blkno(2), tPages.myLockedPages[0].blkno) // 验证剩余页面正确
	})

	// 测试用例2：删除不存在的页面（应该失败）
	t.Run("delete non-existent page", func(t *testing.T) {
		tPages := GetThreadPages()
		tPages.numberOfMyLockedPages = 0 // 重置页面数量

		// 添加一个页面
		myLockedPageAdd(1, 1)

		// 尝试删除不存在的页面
		assert.Panics(t, func() {
			myLockedPageDel(2)
		})
	})

	// 测试用例3：删除最后一个页面
	t.Run("delete last page", func(t *testing.T) {
		tPages := GetThreadPages()
		tPages.numberOfMyLockedPages = 0 // 重置页面数量

		// 添加一个页面
		myLockedPageAdd(1, 1)

		// 删除页面
		state := myLockedPageDel(1)
		assert.Equal(t, uint32(1), state)                // 验证返回的状态
		assert.Equal(t, 0, tPages.numberOfMyLockedPages) // 验证页面数量为0
	})

	// 测试用例4：删除中间页面
	t.Run("delete middle page", func(t *testing.T) {
		tPages := GetThreadPages()
		tPages.numberOfMyLockedPages = 0 // 重置页面数量

		// 添加三个页面
		myLockedPageAdd(1, 1)
		myLockedPageAdd(2, 2)
		myLockedPageAdd(3, 3)

		// 删除中间页面
		state := myLockedPageDel(2)
		assert.Equal(t, uint32(2), state)                        // 验证返回的状态
		assert.Equal(t, 2, tPages.numberOfMyLockedPages)         // 验证页面数量减少
		assert.Equal(t, Blkno(1), tPages.myLockedPages[0].blkno) // 验证第一个页面还在
		assert.Equal(t, Blkno(3), tPages.myLockedPages[1].blkno) // 验证最后一个页面被移到中间位置
	})

	// 测试用例5：边界条件测试
	t.Run("boundary conditions", func(t *testing.T) {
		tPages := GetThreadPages()
		tPages.numberOfMyLockedPages = 0 // 重置页面数量

		// 添加边界值页面
		myLockedPageAdd(0, 0)         // blkno 为 0
		myLockedPageAdd(^Blkno(0), 1) // 最大 blkno

		// 删除 blkno 为 0 的页面
		state := myLockedPageDel(0)
		assert.Equal(t, uint32(0), state)
		assert.Equal(t, 1, tPages.numberOfMyLockedPages)
		assert.Equal(t, ^Blkno(0), tPages.myLockedPages[0].blkno)

		// 删除最大 blkno 的页面
		state = myLockedPageDel(^Blkno(0))
		assert.Equal(t, uint32(1), state)
		assert.Equal(t, 0, tPages.numberOfMyLockedPages)
	})

	// 测试用例6：重复删除同一个页面
	t.Run("delete same page twice", func(t *testing.T) {
		tPages := GetThreadPages()
		tPages.numberOfMyLockedPages = 0 // 重置页面数量

		// 添加一个页面
		myLockedPageAdd(1, 1)

		// 第一次删除
		state := myLockedPageDel(1)
		assert.Equal(t, uint32(1), state)
		assert.Equal(t, 0, tPages.numberOfMyLockedPages)

		// 第二次删除（应该失败）
		assert.Panics(t, func() {
			myLockedPageDel(1)
		})
	})
}

func TestMyLockedPageGetState(t *testing.T) {
	// 测试用例1：获取存在的页面的状态
	t.Run("get state of existing page", func(t *testing.T) {
		tPages := GetThreadPages()
		tPages.numberOfMyLockedPages = 0 // 重置页面数量

		// 添加页面
		myLockedPageAdd(1, 1)
		myLockedPageAdd(2, 2)

		// 获取状态
		state := myLockedPageGetState(1)
		assert.Equal(t, uint32(1), state)

		state = myLockedPageGetState(2)
		assert.Equal(t, uint32(2), state)
	})

	// 测试用例2：获取不存在的页面的状态（应该失败）
	t.Run("get state of non-existent page", func(t *testing.T) {
		tPages := GetThreadPages()
		tPages.numberOfMyLockedPages = 0 // 重置页面数量

		// 添加一个页面
		myLockedPageAdd(1, 1)

		// 尝试获取不存在的页面的状态
		assert.Panics(t, func() {
			myLockedPageGetState(2)
		})
	})

	// 测试用例3：获取已删除页面的状态（应该失败）
	t.Run("get state of deleted page", func(t *testing.T) {
		tPages := GetThreadPages()
		tPages.numberOfMyLockedPages = 0 // 重置页面数量

		// 添加并删除页面
		myLockedPageAdd(1, 1)
		myLockedPageDel(1)

		// 尝试获取已删除页面的状态
		assert.Panics(t, func() {
			myLockedPageGetState(1)
		})
	})

	// 测试用例4：边界条件测试
	t.Run("boundary conditions", func(t *testing.T) {
		tPages := GetThreadPages()
		tPages.numberOfMyLockedPages = 0 // 重置页面数量

		// 添加边界值页面
		myLockedPageAdd(0, 0)         // blkno 为 0
		myLockedPageAdd(^Blkno(0), 1) // 最大 blkno

		// 获取状态
		state := myLockedPageGetState(0)
		assert.Equal(t, uint32(0), state)

		state = myLockedPageGetState(^Blkno(0))
		assert.Equal(t, uint32(1), state)
	})

	// 测试用例5：获取多个页面的状态
	t.Run("get states of multiple pages", func(t *testing.T) {
		tPages := GetThreadPages()
		tPages.numberOfMyLockedPages = 0 // 重置页面数量

		// 添加多个页面
		for i := 0; i < 8; i++ {
			myLockedPageAdd(Blkno(i+1), uint32(i+1))
		}

		// 验证所有页面的状态
		for i := 0; i < 8; i++ {
			state := myLockedPageGetState(Blkno(i + 1))
			assert.Equal(t, uint32(i+1), state)
		}
	})
}

func TestHaveLockedPages(t *testing.T) {
	// 测试用例1：没有锁定的页面
	t.Run("no locked pages", func(t *testing.T) {
		tPages := GetThreadPages()
		tPages.numberOfMyLockedPages = 0 // 重置页面数量

		assert.False(t, haveLockedPages())
	})

	// 测试用例2：有一个锁定的页面
	t.Run("one locked page", func(t *testing.T) {
		tPages := GetThreadPages()
		tPages.numberOfMyLockedPages = 0 // 重置页面数量

		// 添加一个页面
		myLockedPageAdd(1, 1)

		assert.True(t, haveLockedPages())
	})

	// 测试用例3：有多个锁定的页面
	t.Run("multiple locked pages", func(t *testing.T) {
		tPages := GetThreadPages()
		tPages.numberOfMyLockedPages = 0 // 重置页面数量

		// 添加多个页面
		for i := 0; i < 8; i++ {
			myLockedPageAdd(Blkno(i+1), uint32(i+1))
		}

		assert.True(t, haveLockedPages())
	})

	// 测试用例4：添加后删除所有页面
	t.Run("add then delete all pages", func(t *testing.T) {
		tPages := GetThreadPages()
		tPages.numberOfMyLockedPages = 0 // 重置页面数量

		// 添加页面
		myLockedPageAdd(1, 1)
		myLockedPageAdd(2, 2)

		assert.True(t, haveLockedPages())

		// 删除所有页面
		myLockedPageDel(1)
		myLockedPageDel(2)

		assert.False(t, haveLockedPages())
	})

	// 测试用例5：边界条件测试
	t.Run("boundary conditions", func(t *testing.T) {
		tPages := GetThreadPages()
		tPages.numberOfMyLockedPages = 0 // 重置页面数量

		// 添加边界值页面
		myLockedPageAdd(0, 0) // blkno 为 0
		assert.True(t, haveLockedPages())

		// 删除页面
		myLockedPageDel(0)
		assert.False(t, haveLockedPages())

		// 添加最大 blkno 的页面
		myLockedPageAdd(^Blkno(0), 1)
		assert.True(t, haveLockedPages())
	})
}

func TestPageStateIsLocked(t *testing.T) {
	// 测试用例1：状态为0（未锁定）
	t.Run("state is 0", func(t *testing.T) {
		assert.False(t, PageStateIsLocked(0))
	})

	// 测试用例2：状态包含锁定标志
	t.Run("state has locked flag", func(t *testing.T) {
		assert.True(t, PageStateIsLocked(4)) // PAGE_STATE_LOCKED_FLAG = 4
	})

	// 测试用例3：状态包含锁定标志和其他标志
	t.Run("state has locked flag and other flags", func(t *testing.T) {
		state := uint32(4 | 1) // PAGE_STATE_LOCKED_FLAG | PAGE_STATE_HAS_WAITERS_FLAG
		assert.True(t, PageStateIsLocked(state))
	})

	// 测试用例4：状态只包含其他标志
	t.Run("state has only other flags", func(t *testing.T) {
		assert.False(t, PageStateIsLocked(1)) // PAGE_STATE_HAS_WAITERS_FLAG
	})

	// 测试用例5：边界条件测试
	t.Run("boundary conditions", func(t *testing.T) {
		// 测试最大可能的 state 值
		assert.False(t, PageStateIsLocked(^uint32(0) & ^uint32(4))) // 最大 state 值但不包含锁定标志
		assert.True(t, PageStateIsLocked(^uint32(0)))               // 最大 state 值
	})
}

func TestPageStateLock(t *testing.T) {
	// 测试用例1：状态为0
	t.Run("state is 0", func(t *testing.T) {
		state := PageStateLock(0)
		assert.Equal(t, uint32(4), state) // 0 | PAGE_STATE_LOCKED_FLAG = 4
	})

	// 测试用例2：状态已经包含锁定标志
	t.Run("state already has locked flag", func(t *testing.T) {
		state := PageStateLock(4) // PAGE_STATE_LOCKED_FLAG
		assert.Equal(t, uint32(4), state)
	})

	// 测试用例3：状态包含其他标志
	t.Run("state has other flags", func(t *testing.T) {
		state := PageStateLock(1)         // PAGE_STATE_HAS_WAITERS_FLAG
		assert.Equal(t, uint32(5), state) // 1 | PAGE_STATE_LOCKED_FLAG = 5
	})

	// 测试用例4：状态包含多个标志
	t.Run("state has multiple flags", func(t *testing.T) {
		state := PageStateLock(1 | 2)     // PAGE_STATE_HAS_WAITERS_FLAG | PAGE_STATE_LIST_LOCKED_FLAG
		assert.Equal(t, uint32(7), state) // (1 | 2) | PAGE_STATE_LOCKED_FLAG = 7
	})

	// 测试用例5：边界条件测试
	t.Run("boundary conditions", func(t *testing.T) {
		// 测试最大可能的 state 值
		state := PageStateLock(^uint32(0) & ^uint32(4)) // 最大 state 值但不包含锁定标志
		assert.Equal(t, ^uint32(0), state)              // 最大 state 值

		// 测试最大可能的 state 值已经包含锁定标志
		state = PageStateLock(^uint32(0))
		assert.Equal(t, ^uint32(0), state)
	})
}

func TestPageStateReadIsBlocked(t *testing.T) {
	// 测试用例1：状态为0（未禁止读取）
	t.Run("state is 0", func(t *testing.T) {
		assert.False(t, PageStateReadIsBlocked(0))
	})

	// 测试用例2：状态包含禁止读取标志
	t.Run("state has no read flag", func(t *testing.T) {
		assert.True(t, PageStateReadIsBlocked(8)) // PAGE_STATE_NO_READ_FLAG = 8
	})

	// 测试用例3：状态包含禁止读取标志和其他标志
	t.Run("state has no read flag and other flags", func(t *testing.T) {
		state := uint32(8 | 1) // PAGE_STATE_NO_READ_FLAG | PAGE_STATE_HAS_WAITERS_FLAG
		assert.True(t, PageStateReadIsBlocked(state))
	})

	// 测试用例4：状态只包含其他标志
	t.Run("state has only other flags", func(t *testing.T) {
		assert.False(t, PageStateReadIsBlocked(1)) // PAGE_STATE_HAS_WAITERS_FLAG
	})

	// 测试用例5：边界条件测试
	t.Run("boundary conditions", func(t *testing.T) {
		// 测试最大可能的 state 值
		assert.False(t, PageStateReadIsBlocked(^uint32(0) & ^uint32(8))) // 最大 state 值但不包含禁止读取标志
		assert.True(t, PageStateReadIsBlocked(^uint32(0)))               // 最大 state 值
	})
}

func TestPageStateBlockRead(t *testing.T) {
	// 测试用例1：状态为0
	t.Run("state is 0", func(t *testing.T) {
		state := PageStateBlockRead(0)
		assert.Equal(t, uint32(12), state) // 0 | PAGE_STATE_LOCKED_FLAG | PAGE_STATE_NO_READ_FLAG = 12
	})

	// 测试用例2：状态已经包含锁定和禁止读取标志
	t.Run("state already has locked and no read flags", func(t *testing.T) {
		state := PageStateBlockRead(12) // PAGE_STATE_LOCKED_FLAG | PAGE_STATE_NO_READ_FLAG
		assert.Equal(t, uint32(12), state)
	})

	// 测试用例3：状态只包含锁定标志
	t.Run("state has only locked flag", func(t *testing.T) {
		state := PageStateBlockRead(4)     // PAGE_STATE_LOCKED_FLAG
		assert.Equal(t, uint32(12), state) // 4 | PAGE_STATE_NO_READ_FLAG = 12
	})

	// 测试用例4：状态只包含禁止读取标志
	t.Run("state has only no read flag", func(t *testing.T) {
		state := PageStateBlockRead(8)     // PAGE_STATE_NO_READ_FLAG
		assert.Equal(t, uint32(12), state) // 8 | PAGE_STATE_LOCKED_FLAG = 12
	})

	// 测试用例5：状态包含其他标志
	t.Run("state has other flags", func(t *testing.T) {
		state := PageStateBlockRead(1)     // PAGE_STATE_HAS_WAITERS_FLAG
		assert.Equal(t, uint32(13), state) // 1 | PAGE_STATE_LOCKED_FLAG | PAGE_STATE_NO_READ_FLAG = 13
	})

	// 测试用例6：状态包含多个标志
	t.Run("state has multiple flags", func(t *testing.T) {
		state := PageStateBlockRead(1 | 2) // PAGE_STATE_HAS_WAITERS_FLAG | PAGE_STATE_LIST_LOCKED_FLAG
		assert.Equal(t, uint32(15), state) // (1 | 2) | PAGE_STATE_LOCKED_FLAG | PAGE_STATE_NO_READ_FLAG = 15
	})

	// 测试用例7：边界条件测试
	t.Run("boundary conditions", func(t *testing.T) {
		// 测试最大可能的 state 值
		state := PageStateBlockRead(^uint32(0) & ^uint32(12)) // 最大 state 值但不包含锁定和禁止读取标志
		assert.Equal(t, ^uint32(0), state)                    // 最大 state 值

		// 测试最大可能的 state 值已经包含锁定和禁止读取标志
		state = PageStateBlockRead(^uint32(0))
		assert.Equal(t, ^uint32(0), state)
	})
}

func TestPageIncUsageCount(t *testing.T) {
	// 测试用例1：正常增加使用计数
	t.Run("normal increment", func(t *testing.T) {
		// 分配页面
		InitPages(1)
		defer ClosePages()

		blkno := Blkno(0)
		ptr := GetInMemPage(blkno)
		header := GetPageHeader(ptr)

		// 设置初始使用计数
		header.usageCountAtomic = 0

		// 增加使用计数
		pageIncUsageCount(blkno, 0)

		// 验证使用计数已增加
		assert.Equal(t, uint32(1), header.usageCountAtomic)
	})

	// 测试用例2：并发增加使用计数
	t.Run("concurrent increment", func(t *testing.T) {
		// 分配页面
		InitPages(1)
		defer ClosePages()

		blkno := Blkno(0)
		ptr := GetInMemPage(blkno)
		header := GetPageHeader(ptr)

		// 设置初始使用计数
		header.usageCountAtomic = 0

		// 使用两个 goroutine 并发增加使用计数
		var wg sync.WaitGroup
		numGoroutines := 2
		wg.Add(numGoroutines)

		for i := 0; i < numGoroutines; i++ {
			go func() {
				defer wg.Done()
				pageIncUsageCount(blkno, atomic.LoadUint32(&header.usageCountAtomic))
			}()
		}

		wg.Wait()

		// 验证使用计数已正确增加
		assert.Equal(t, uint32(numGoroutines), header.usageCountAtomic)
	})

	// 测试用例3：边界条件测试
	t.Run("boundary conditions", func(t *testing.T) {
		// 分配页面
		InitPages(1)
		defer ClosePages()

		blkno := Blkno(0)
		ptr := GetInMemPage(blkno)
		header := GetPageHeader(ptr)

		// 测试最大可能的 usageCount
		header.usageCountAtomic = ^uint32(0) - 1
		pageIncUsageCount(blkno, ^uint32(0)-1)
		assert.Equal(t, ^uint32(0), header.usageCountAtomic)

		// 重置使用计数
		header.usageCountAtomic = 0
	})

	// 测试用例4：CAS 失败的情况
	t.Run("CAS failure", func(t *testing.T) {
		// 分配页面
		InitPages(1)
		defer ClosePages()

		blkno := Blkno(0)
		ptr := GetInMemPage(blkno)
		header := GetPageHeader(ptr)

		// 设置初始使用计数
		header.usageCountAtomic = 0

		// 尝试使用错误的值进行 CAS
		pageIncUsageCount(blkno, 1) // 预期会失败，因为当前值是 0

		// 验证使用计数没有改变
		assert.Equal(t, uint32(0), header.usageCountAtomic)
	})
}

func TestLockPage(t *testing.T) {
	// 测试用例1：正常锁定页面
	t.Run("normal lock", func(t *testing.T) {
		// 分配页面
		InitPages(1)
		defer ClosePages()

		blkno := Blkno(0)
		ptr := GetInMemPage(blkno)
		header := GetPageHeader(ptr)

		// 锁定页面
		lockPage(blkno)

		// 验证页面状态
		assert.True(t, PageStateIsLocked(atomic.LoadUint32(&header.stateAtomic)))
		assert.Equal(t, uint32(1), atomic.LoadUint32(&header.usageCountAtomic))

		// 验证页面已被添加到锁定列表
		tPages := GetThreadPages()
		assert.Equal(t, 1, tPages.numberOfMyLockedPages)
		assert.Equal(t, blkno, tPages.myLockedPages[0].blkno)
	})

	// 测试用例2：页面已经被锁定
	t.Run("page already locked", func(t *testing.T) {
		// 分配页面
		InitPages(1)
		defer ClosePages()

		blkno := Blkno(0)
		ptr := GetInMemPage(blkno)
		header := GetPageHeader(ptr)

		// 先锁定页面
		lockPage(blkno)

		// 尝试再次锁定页面
		done := make(chan bool)
		go func() {
			lockPage(blkno)
			done <- true
		}()

		// 等待一段时间后解锁页面
		time.Sleep(time.Millisecond * 50)
		atomic.StoreUint32(&header.stateAtomic, 0)
		desc := GetInMemPageDesc(blkno)
		desc.waitC <- struct{}{}

		// 等待第二个锁定完成
		<-done

		// 验证页面状态
		assert.True(t, PageStateIsLocked(atomic.LoadUint32(&header.stateAtomic)))
		assert.Equal(t, uint32(2), atomic.LoadUint32(&header.usageCountAtomic))
	})

	// 测试用例3：锁定超时
	t.Run("lock timeout", func(t *testing.T) {
		// 分配页面
		InitPages(1)
		defer ClosePages()

		blkno := Blkno(0)
		ptr := GetInMemPage(blkno)
		header := GetPageHeader(ptr)

		// 先锁定页面
		lockPage(blkno)

		// 尝试再次锁定页面
		done := make(chan bool)
		go func() {
			lockPage(blkno)
			done <- true
		}()

		// 等待超时
		select {
		case <-done:
			t.Fatal("lock should not succeed")
		case <-time.After(time.Millisecond * 150):
			// 验证页面状态
			assert.True(t, PageStateIsLocked(atomic.LoadUint32(&header.stateAtomic)))
			assert.Equal(t, uint32(2), atomic.LoadUint32(&header.usageCountAtomic))
		}

		// 释放锁
		atomic.StoreUint32(&header.stateAtomic, 0)
		desc := GetInMemPageDesc(blkno)
		desc.waitC <- struct{}{}

		// 等待第二个锁定完成
		<-done

		// 验证页面状态
		assert.True(t, PageStateIsLocked(atomic.LoadUint32(&header.stateAtomic)))
		assert.Equal(t, uint32(2), atomic.LoadUint32(&header.usageCountAtomic))
	})

	// 测试用例4：并发锁定页面
	t.Run("concurrent lock", func(t *testing.T) {
		// 分配页面
		InitPages(1)
		defer ClosePages()

		blkno := Blkno(0)
		ptr := GetInMemPage(blkno)
		header := GetPageHeader(ptr)
		desc := GetInMemPageDesc(blkno)

		// 使用两个 goroutine 并发锁定页面
		var wg sync.WaitGroup
		numGoroutines := 2
		wg.Add(numGoroutines)

		for i := 0; i < numGoroutines; i++ {
			go func() {
				defer wg.Done()
				lockPage(blkno)
				// 验证页面已被添加到锁定列表
				tPages := GetThreadPages()
				assert.Equal(t, 1, tPages.numberOfMyLockedPages)
				assert.Equal(t, blkno, tPages.myLockedPages[0].blkno)
				// 等待一段时间后释放锁
				time.Sleep(time.Millisecond * 50)
				atomic.StoreUint32(&header.stateAtomic, 0)
				fmt.Println("rt", i, "release lock")
				desc.waitC <- struct{}{}
				fmt.Println("rt", i, "release lock done")
			}()
		}

		wg.Wait()

		// 验证页面状态
		assert.False(t, PageStateIsLocked(atomic.LoadUint32(&header.stateAtomic)))
		assert.Equal(t, uint32(numGoroutines), atomic.LoadUint32(&header.usageCountAtomic))
	})

	// 测试用例4：边界条件测试
	t.Run("boundary conditions", func(t *testing.T) {
		// 分配页面
		InitPages(1)
		defer ClosePages()

		// 测试 blkno 为 0
		blkno := Blkno(0)
		ptr := GetInMemPage(blkno)
		header := GetPageHeader(ptr)

		lockPage(blkno)
		assert.True(t, PageStateIsLocked(atomic.LoadUint32(&header.stateAtomic)))
		assert.Equal(t, uint32(1), atomic.LoadUint32(&header.usageCountAtomic))

		// 测试最大可能的 blkno
		blkno = ^Blkno(0)
		assert.Panics(t, func() {
			lockPage(blkno)
		})
	})
}

func TestPageWaitForReadEnable(t *testing.T) {
	// 测试用例1：页面未被阻塞读取
	t.Run("page not blocked", func(t *testing.T) {
		// 分配页面
		InitPages(1)
		defer ClosePages()

		blkno := Blkno(0)
		ptr := GetInMemPage(blkno)
		header := GetPageHeader(ptr)

		// 设置页面状态为未阻塞
		atomic.StoreUint32(&header.stateAtomic, 0)

		// 等待读取启用
		pageWaitForReadEnable(blkno)

		// 验证页面状态
		assert.False(t, PageStateReadIsBlocked(atomic.LoadUint32(&header.stateAtomic)))
	})

	// 测试用例2：页面被阻塞读取，等待后解除阻塞
	t.Run("page blocked and unblocked", func(t *testing.T) {
		// 分配页面
		InitPages(1)
		defer ClosePages()

		blkno := Blkno(0)
		ptr := GetInMemPage(blkno)
		header := GetPageHeader(ptr)
		desc := GetInMemPageDesc(blkno)

		// 设置页面状态为阻塞读取
		atomic.StoreUint32(&header.stateAtomic, PageStateBlockRead(0))

		// 启动一个 goroutine 等待读取启用
		done := make(chan bool)
		go func() {
			pageWaitForReadEnable(blkno)
			done <- true
		}()

		// 等待一段时间后解除阻塞
		time.Sleep(time.Millisecond * 50)
		atomic.StoreUint32(&header.stateAtomic, 0)
		desc.waitC <- struct{}{}

		// 等待读取启用完成
		<-done

		// 验证页面状态
		assert.False(t, PageStateReadIsBlocked(atomic.LoadUint32(&header.stateAtomic)))
	})

	// 测试用例3：页面被阻塞读取，等待超时
	t.Run("page blocked and timeout", func(t *testing.T) {
		// 分配页面
		InitPages(1)
		defer ClosePages()

		blkno := Blkno(0)
		ptr := GetInMemPage(blkno)
		header := GetPageHeader(ptr)

		// 设置页面状态为阻塞读取
		atomic.StoreUint32(&header.stateAtomic, PageStateBlockRead(0))

		// 启动一个 goroutine 等待读取启用
		done := make(chan bool)
		go func() {
			pageWaitForReadEnable(blkno)
			done <- true
		}()

		// 等待超时
		select {
		case <-done:
			t.Fatal("wait should not complete")
		case <-time.After(time.Millisecond * 150):
			// 验证页面状态
			assert.True(t, PageStateReadIsBlocked(atomic.LoadUint32(&header.stateAtomic)))
		}

		// 解除阻塞
		atomic.StoreUint32(&header.stateAtomic, 0)
		desc := GetInMemPageDesc(blkno)
		desc.waitC <- struct{}{}

		// 等待读取启用完成
		<-done
		assert.False(t, PageStateReadIsBlocked(atomic.LoadUint32(&header.stateAtomic)))
	})

	// 测试用例4：边界条件测试
	t.Run("boundary conditions", func(t *testing.T) {
		// 分配页面
		InitPages(1)
		defer ClosePages()

		// 测试 blkno 为 0
		blkno := Blkno(0)
		ptr := GetInMemPage(blkno)
		header := GetPageHeader(ptr)

		// 设置页面状态为阻塞读取
		atomic.StoreUint32(&header.stateAtomic, PageStateBlockRead(0))

		// 测试最大可能的 blkno
		blkno = ^Blkno(0)
		assert.Panics(t, func() {
			pageWaitForReadEnable(blkno)
		})
	})
}

func TestPageWaitForChangeCount(t *testing.T) {
	// 测试用例1：页面状态立即变化
	t.Run("state changes immediately", func(t *testing.T) {
		// 分配页面
		InitPages(1)
		defer ClosePages()

		blkno := Blkno(0)
		ptr := GetInMemPage(blkno)
		header := GetPageHeader(ptr)
		desc := GetInMemPageDesc(blkno)

		// 设置初始状态
		initialState := uint32(0)
		atomic.StoreUint32(&header.stateAtomic, initialState)

		// 启动一个 goroutine 等待状态变化
		done := make(chan bool)
		var newState atomic.Uint32
		go func() {
			newState.Store(pageWaitForChangeCount(blkno, initialState))
			done <- true
		}()

		// 立即改变状态
		changedState := initialState | PageStateChangeCountMask
		atomic.StoreUint32(&header.stateAtomic, changedState)
		desc.waitC <- struct{}{}

		// 等待完成
		<-done

		// 验证新状态
		assert.Equal(t, changedState, newState.Load())
	})

	// 测试用例2：页面状态延迟变化
	t.Run("state changes after delay", func(t *testing.T) {
		// 分配页面
		InitPages(1)
		defer ClosePages()

		blkno := Blkno(0)
		ptr := GetInMemPage(blkno)
		header := GetPageHeader(ptr)
		desc := GetInMemPageDesc(blkno)

		// 设置初始状态
		initialState := uint32(0)
		atomic.StoreUint32(&header.stateAtomic, initialState)

		// 启动一个 goroutine 等待状态变化
		done := make(chan bool)
		var newState atomic.Uint32
		go func() {
			newState.Store(pageWaitForChangeCount(blkno, initialState))
			done <- true
		}()

		// 延迟改变状态
		time.Sleep(time.Millisecond * 50)
		changedState := initialState | PageStateChangeCountMask
		atomic.StoreUint32(&header.stateAtomic, changedState)
		desc.waitC <- struct{}{}

		// 等待完成
		<-done

		// 验证新状态
		assert.Equal(t, changedState, newState.Load())
	})

	// 测试用例3：页面状态超时
	t.Run("state timeout", func(t *testing.T) {
		// 分配页面
		InitPages(1)
		defer ClosePages()

		blkno := Blkno(0)
		ptr := GetInMemPage(blkno)
		header := GetPageHeader(ptr)
		desc := GetInMemPageDesc(blkno)

		// 设置初始状态
		initialState := uint32(0)
		atomic.StoreUint32(&header.stateAtomic, initialState)

		// 启动一个 goroutine 等待状态变化
		done := make(chan bool)
		var newState atomic.Uint32
		go func() {
			newState.Store(pageWaitForChangeCount(blkno, initialState))
			done <- true
		}()

		// 等待超时
		select {
		case <-done:
			t.Fatal("wait should not complete")
		case <-time.After(time.Millisecond * 150):
			// 验证状态未改变
			assert.Equal(t, initialState, atomic.LoadUint32(&header.stateAtomic))
		}

		// 改变状态
		changedState := initialState | PageStateChangeCountMask
		atomic.StoreUint32(&header.stateAtomic, changedState)
		desc.waitC <- struct{}{}

		// 等待完成
		<-done

		// 验证新状态
		assert.Equal(t, changedState, newState.Load())
	})

	// 测试用例4：边界条件测试
	t.Run("boundary conditions", func(t *testing.T) {
		// 分配页面
		InitPages(1)
		defer ClosePages()

		// 测试 blkno 为 0
		blkno := Blkno(0)
		ptr := GetInMemPage(blkno)
		header := GetPageHeader(ptr)
		desc := GetInMemPageDesc(blkno)

		// 设置初始状态
		initialState := uint32(0)
		atomic.StoreUint32(&header.stateAtomic, initialState)

		// 启动一个 goroutine 等待状态变化
		done := make(chan bool)
		var newState atomic.Uint32
		go func() {
			newState.Store(pageWaitForChangeCount(blkno, initialState))
			done <- true
		}()

		// 改变状态
		changedState := initialState | PageStateChangeCountMask
		atomic.StoreUint32(&header.stateAtomic, changedState)
		desc.waitC <- struct{}{}

		// 等待完成
		<-done

		// 验证新状态
		assert.Equal(t, changedState, newState.Load())

		// 测试最大可能的 blkno
		blkno = ^Blkno(0)
		assert.Panics(t, func() {
			pageWaitForChangeCount(blkno, initialState)
		})
	})
}

func TestUnlockPage(t *testing.T) {
	// 测试用例1：正常解锁页面（没有PageStateNoReadFlag）
	t.Run("normal unlock without no read flag", func(t *testing.T) {
		// 分配页面
		InitPages(1)
		defer ClosePages()

		blkno := Blkno(0)
		ptr := GetInMemPage(blkno)
		header := GetPageHeader(ptr)

		// 先锁定页面
		lockPage(blkno)
		initialState := atomic.LoadUint32(&header.stateAtomic)

		// 验证页面被锁定
		assert.True(t, PageStateIsLocked(atomic.LoadUint32(&header.stateAtomic)))

		// 解锁页面
		unlockPage(blkno)

		// 验证页面已解锁
		assert.False(t, PageStateIsLocked(atomic.LoadUint32(&header.stateAtomic)))

		// 提取并比较 changeCount
		value1 := initialState & PageStateChangeCountMask
		value2 := atomic.LoadUint32(&header.stateAtomic) & PageStateChangeCountMask
		fmt.Printf("Test case 1 - changeCount before: %d, after: %d\n", value1, value2)
		assert.Equal(t, value1, value2) // changeCount 不应该变化
	})

	// 测试用例2：解锁被禁止读取的页面（有PageStateNoReadFlag）
	t.Run("unlock blocked page with no read flag", func(t *testing.T) {
		// 分配页面
		InitPages(1)
		defer ClosePages()

		blkno := Blkno(0)
		ptr := GetInMemPage(blkno)
		header := GetPageHeader(ptr)

		// 设置页面状态为锁定和禁止读取
		atomic.StoreUint32(&header.stateAtomic, PageStateLockedFlag|PageStateNoReadFlag)
		myLockedPageAdd(blkno, PageStateLockedFlag|PageStateNoReadFlag)
		initialState := atomic.LoadUint32(&header.stateAtomic)

		// 获取初始 changeCount
		initialChangeCount := initialState & PageStateChangeCountMask

		// 解锁页面
		unlockPage(blkno)

		// 验证页面已解锁且不再禁止读取
		assert.False(t, PageStateIsLocked(atomic.LoadUint32(&header.stateAtomic)))
		assert.False(t, PageStateReadIsBlocked(atomic.LoadUint32(&header.stateAtomic)))

		// 获取解锁后的 changeCount
		finalState := atomic.LoadUint32(&header.stateAtomic)
		finalChangeCount := finalState & PageStateChangeCountMask

		// 计算预期的 changeCount 变化
		added := PageStateChangeCountOne - (initialState & (PageStateLockedFlag | PageStateNoReadFlag))
		added = added + initialState

		// 打印 changeCount 的变化
		fmt.Printf("Test case 2 - changeCount before: %d, added: %d, after: %d\n", initialChangeCount, added, finalChangeCount)

		// 验证 changeCount 增加了 1, 经过移位后变成16
		assert.Equal(t, (initialChangeCount+added)&PageStateChangeCountMask, finalChangeCount)
	})

	// 测试用例3：解锁不存在的页面
	t.Run("unlock non-existent page", func(t *testing.T) {
		// 分配页面
		InitPages(1)
		defer ClosePages()

		// 尝试解锁不存在的页面
		assert.Panics(t, func() {
			unlockPage(Blkno(1))
		})
	})

	// 测试用例4：边界条件测试
	t.Run("boundary conditions", func(t *testing.T) {
		// 分配页面
		InitPages(1)
		defer ClosePages()

		// 测试 blkno 为 0
		blkno := Blkno(0)
		ptr := GetInMemPage(blkno)
		header := GetPageHeader(ptr)

		// 锁定页面
		lockPage(blkno)
		initialState := atomic.LoadUint32(&header.stateAtomic)

		// 解锁页面
		unlockPage(blkno)

		// 验证页面已解锁
		assert.False(t, PageStateIsLocked(atomic.LoadUint32(&header.stateAtomic)))

		// 提取并比较 changeCount
		value1 := initialState & PageStateChangeCountMask
		value2 := atomic.LoadUint32(&header.stateAtomic) & PageStateChangeCountMask
		fmt.Printf("Test case 4 - changeCount before: %d, after: %d\n", value1, value2)
		assert.Equal(t, value1, value2) // changeCount 不应该变化

		// 测试最大可能的 blkno
		blkno = ^Blkno(0)
		assert.Panics(t, func() {
			unlockPage(blkno)
		})
	})

	// 测试用例5：加锁1次，解锁2次（没有PageStateNoReadFlag）
	t.Run("lock once, unlock twice without no read flag", func(t *testing.T) {
		// 分配页面
		InitPages(1)
		defer ClosePages()

		blkno := Blkno(0)
		ptr := GetInMemPage(blkno)
		header := GetPageHeader(ptr)

		// 锁定页面
		lockPage(blkno)
		initialState := atomic.LoadUint32(&header.stateAtomic)

		// 第一次解锁
		unlockPage(blkno)
		firstUnlockState := atomic.LoadUint32(&header.stateAtomic)

		// 验证第一次解锁后的状态
		assert.False(t, PageStateIsLocked(firstUnlockState))

		// 提取并比较第一次解锁的 changeCount
		value1 := initialState & PageStateChangeCountMask
		value2 := firstUnlockState & PageStateChangeCountMask
		fmt.Printf("Test case 5 - First unlock - changeCount before: %d, after: %d\n", value1, value2)
		assert.Equal(t, value1, value2) // changeCount 不应该变化

		// 第二次解锁（应该触发 panic）
		assert.Panics(t, func() {
			unlockPage(blkno)
		})

		// 验证最终状态与第一次解锁后的状态相同
		assert.Equal(t, firstUnlockState, atomic.LoadUint32(&header.stateAtomic))
	})

	// 测试用例6：只有PageStateNoReadFlag，没有PageStateLockedFlag
	t.Run("only no read flag without locked flag", func(t *testing.T) {
		// 分配页面
		InitPages(1)
		defer ClosePages()

		blkno := Blkno(0)
		ptr := GetInMemPage(blkno)
		header := GetPageHeader(ptr)

		// 设置页面状态为只有禁止读取标志
		atomic.StoreUint32(&header.stateAtomic, PageStateNoReadFlag)
		myLockedPageAdd(blkno, PageStateNoReadFlag)
		initialState := atomic.LoadUint32(&header.stateAtomic)

		// 获取初始 changeCount
		initialChangeCount := initialState & PageStateChangeCountMask

		// 解锁页面
		unlockPage(blkno)

		// 验证页面不再禁止读取
		assert.False(t, PageStateReadIsBlocked(atomic.LoadUint32(&header.stateAtomic)))

		// 获取解锁后的 changeCount
		finalState := atomic.LoadUint32(&header.stateAtomic)
		finalChangeCount := finalState & PageStateChangeCountMask

		// 计算预期的 changeCount 变化
		added := PageStateChangeCountOne - (initialState & (PageStateLockedFlag | PageStateNoReadFlag))
		added = added + initialState

		// 打印 changeCount 的变化
		fmt.Printf("Test case 6 - changeCount before: %d, added: %d, after: %d\n", initialChangeCount, added, finalChangeCount)

		// 验证 changeCount 的变化
		assert.Equal(t, (initialChangeCount+added)&PageStateChangeCountMask, finalChangeCount)
	})

	// 测试用例7：解锁未锁定的页面（应该触发panic）
	t.Run("unlock unlocked page", func(t *testing.T) {
		// 分配页面
		InitPages(1)
		defer ClosePages()

		blkno := Blkno(0)
		ptr := GetInMemPage(blkno)
		header := GetPageHeader(ptr)

		// 确保页面未锁定
		atomic.StoreUint32(&header.stateAtomic, 0)

		// 尝试解锁未锁定的页面（应该触发panic）
		assert.Panics(t, func() {
			unlockPage(blkno)
		})
	})

	// 测试用例8：解锁带有其他标志的页面
	t.Run("unlock page with other flags", func(t *testing.T) {
		// 分配页面
		InitPages(1)
		defer ClosePages()

		blkno := Blkno(0)
		ptr := GetInMemPage(blkno)
		header := GetPageHeader(ptr)

		// 设置页面状态为锁定和禁止读取
		atomic.StoreUint32(&header.stateAtomic, PageStateLockedFlag|PageStateNoReadFlag)
		myLockedPageAdd(blkno, PageStateLockedFlag|PageStateNoReadFlag)
		initialState := atomic.LoadUint32(&header.stateAtomic)

		// 解锁页面
		unlockPage(blkno)

		// 验证页面已解锁且不再禁止读取
		assert.False(t, PageStateIsLocked(atomic.LoadUint32(&header.stateAtomic)))
		assert.False(t, PageStateReadIsBlocked(atomic.LoadUint32(&header.stateAtomic)))

		// 提取并比较 changeCount
		value1 := initialState & PageStateChangeCountMask
		value2 := atomic.LoadUint32(&header.stateAtomic) & PageStateChangeCountMask
		fmt.Printf("Test case 8 - changeCount before: %d, after: %d\n", value1, value2)
		assert.NotEqual(t, value1, value2) // changeCount 应该变化，因为有 PageStateNoReadFlag
	})
}

func TestRelockPage(t *testing.T) {
	// 测试用例1：正常重新锁定页面
	t.Run("normal relock", func(t *testing.T) {
		// 分配页面
		InitPages(1)
		defer ClosePages()

		blkno := Blkno(0)
		ptr := GetInMemPage(blkno)
		header := GetPageHeader(ptr)
		desc := GetInMemPageDesc(blkno)

		// 先锁定页面
		lockPage(blkno)
		initialState := atomic.LoadUint32(&header.stateAtomic)
		initialUsageCount := atomic.LoadUint32(&header.usageCountAtomic)

		// 启动一个 goroutine 来改变状态
		done := make(chan bool)
		go func() {
			// 等待一段时间后改变状态
			time.Sleep(time.Millisecond * 50)
			// 设置新的状态，只修改高28位
			atomic.AddUint32(&header.stateAtomic, 1<<4)
			desc.waitC <- struct{}{}
			done <- true
		}()

		// 重新锁定页面
		relockPage(blkno)

		// 等待状态改变完成
		<-done

		// 验证页面状态
		assert.True(t, PageStateIsLocked(atomic.LoadUint32(&header.stateAtomic)))
		assert.Equal(t, initialUsageCount+2, atomic.LoadUint32(&header.usageCountAtomic))

		// 提取并比较 changeCount
		value1 := (initialState >> 4) & 0x0FFFFFFF
		value2 := (atomic.LoadUint32(&header.stateAtomic) >> 4) & 0x0FFFFFFF
		fmt.Printf("Test case 1 - changeCount before: %d, after: %d\n", value1, value2)
		assert.NotEqual(t, value1, value2) // changeCount 应该变化
		assert.Equal(t, value1+1, value2)
	})

	// 测试用例2：重新锁定带有 PageStateNoReadFlag 的页面
	t.Run("relock page with no read flag", func(t *testing.T) {
		// 分配页面
		InitPages(1)
		defer ClosePages()

		blkno := Blkno(0)
		ptr := GetInMemPage(blkno)
		header := GetPageHeader(ptr)
		desc := GetInMemPageDesc(blkno)

		// 设置页面状态为锁定和禁止读取
		atomic.StoreUint32(&header.stateAtomic, PageStateLockedFlag|PageStateNoReadFlag)
		myLockedPageAdd(blkno, PageStateLockedFlag|PageStateNoReadFlag)
		initialState := atomic.LoadUint32(&header.stateAtomic)
		initialUsageCount := atomic.LoadUint32(&header.usageCountAtomic)

		// 启动一个 goroutine 来改变状态
		done := make(chan bool)
		go func() {
			// 等待一段时间后改变状态
			time.Sleep(time.Millisecond * 50)
			// 设置新的状态，只修改高28位
			atomic.AddUint32(&header.stateAtomic, 1<<4)
			desc.waitC <- struct{}{}
			done <- true
		}()

		// 重新锁定页面
		relockPage(blkno)

		// 等待状态改变完成
		<-done

		// 验证页面状态
		assert.True(t, PageStateIsLocked(atomic.LoadUint32(&header.stateAtomic)))
		assert.False(t, PageStateReadIsBlocked(atomic.LoadUint32(&header.stateAtomic)))
		assert.Equal(t, initialUsageCount+2, atomic.LoadUint32(&header.usageCountAtomic))

		// 提取并比较 changeCount
		value1 := (initialState >> 4) & 0x0FFFFFFF
		value2 := (atomic.LoadUint32(&header.stateAtomic) >> 4) & 0x0FFFFFFF

		//此处是unlockPage增加的1
		added := PageStateChangeCountOne - (initialState & (PageStateLockedFlag | PageStateNoReadFlag))
		added = (added + initialState) >> 4

		fmt.Printf("Test case 2 - changeCount before: %d, after: %d\n", value1, value2)
		assert.NotEqual(t, value1, value2) // changeCount 应该变化
		assert.Equal(t, value1+added+1, value2)
	})

	// 测试用例3：边界条件测试
	t.Run("boundary conditions", func(t *testing.T) {
		// 分配页面
		InitPages(1)
		defer ClosePages()

		// 测试 blkno 为 0
		blkno := Blkno(0)
		ptr := GetInMemPage(blkno)
		header := GetPageHeader(ptr)
		desc := GetInMemPageDesc(blkno)

		// 锁定页面
		lockPage(blkno)
		initialState := atomic.LoadUint32(&header.stateAtomic)
		initialUsageCount := atomic.LoadUint32(&header.usageCountAtomic)

		// 启动一个 goroutine 来改变状态
		done := make(chan bool)
		go func() {
			// 等待一段时间后改变状态
			time.Sleep(time.Millisecond * 50)
			// 设置新的状态，只修改高28位
			atomic.AddUint32(&header.stateAtomic, 1<<4)
			desc.waitC <- struct{}{}
			done <- true
		}()

		// 重新锁定页面
		relockPage(blkno)

		// 等待状态改变完成
		<-done

		// 验证页面状态
		assert.True(t, PageStateIsLocked(atomic.LoadUint32(&header.stateAtomic)))
		assert.Equal(t, initialUsageCount+2, atomic.LoadUint32(&header.usageCountAtomic))

		// 提取并比较 changeCount
		value1 := (initialState >> 4) & 0x0FFFFFFF
		value2 := (atomic.LoadUint32(&header.stateAtomic) >> 4) & 0x0FFFFFFF

		fmt.Printf("Test case 3 - changeCount before: %d, after: %d\n", value1, value2)
		assert.NotEqual(t, value1, value2) // changeCount 应该变化
		//没有noReadFlag，所以增加1
		assert.Equal(t, value1+1, value2)

		// 测试最大可能的 blkno
		blkno = ^Blkno(0)
		assert.Panics(t, func() {
			relockPage(blkno)
		})
	})

	// 测试用例4：并发重新锁定测试
	t.Run("concurrent relock", func(t *testing.T) {
		// 分配页面
		InitPages(1)
		defer ClosePages()

		blkno := Blkno(0)
		ptr := GetInMemPage(blkno)
		header := GetPageHeader(ptr)
		desc := GetInMemPageDesc(blkno)

		// 使用两个 goroutine 并发重新锁定页面
		var wg sync.WaitGroup
		numGoroutines := 2
		wg.Add(numGoroutines)

		// 先锁定页面

		initialUsageCount := atomic.LoadUint32(&header.usageCountAtomic)

		// 启动一个 goroutine 来改变状态
		stateChangeDone := make(chan bool)
		go func() {
			// 等待一段时间后改变状态
			time.Sleep(time.Millisecond * 50)
			// 设置新的状态，只修改高28位
			atomic.AddUint32(&header.stateAtomic, uint32(numGoroutines)<<4)
			time.Sleep(time.Millisecond * 50)
			desc.waitC <- struct{}{}
			stateChangeDone <- true
		}()

		for i := 0; i < numGoroutines; i++ {
			go func() {
				defer wg.Done()
				lockPage(blkno)
				time.Sleep(50 * time.Millisecond)
				relockPage(blkno)
				// 等待一段时间后释放锁
				time.Sleep(time.Millisecond * 50)
				atomic.StoreUint32(&header.stateAtomic, 0)
				desc.waitC <- struct{}{}
			}()
		}

		// 等待状态改变完成
		<-stateChangeDone

		wg.Wait()

		// 验证页面状态
		assert.False(t, PageStateIsLocked(atomic.LoadUint32(&header.stateAtomic)))
		assert.Equal(t, initialUsageCount+uint32(numGoroutines)*3, atomic.LoadUint32(&header.usageCountAtomic))
	})
}

func TestTryLockPage(t *testing.T) {
	// 测试用例1：正常锁定未锁定的页面
	t.Run("normal lock", func(t *testing.T) {
		// 分配页面
		InitPages(1)
		defer ClosePages()

		blkno := Blkno(0)
		ptr := GetInMemPage(blkno)
		header := GetPageHeader(ptr)

		// 确保页面未锁定
		atomic.StoreUint32(&header.stateAtomic, 0)

		// 尝试锁定页面
		result := tryLockPage(blkno)

		// 验证结果
		assert.True(t, result)
		assert.True(t, PageStateIsLocked(atomic.LoadUint32(&header.stateAtomic)))
		assert.Equal(t, uint32(0), atomic.LoadUint32(&header.usageCountAtomic))

		// 验证页面已被添加到锁定列表
		tPages := GetThreadPages()
		assert.Equal(t, 1, tPages.numberOfMyLockedPages)
		assert.Equal(t, blkno, tPages.myLockedPages[0].blkno)
	})

	// 测试用例2：尝试锁定已锁定的页面
	t.Run("lock already locked page", func(t *testing.T) {
		// 分配页面
		InitPages(1)
		defer ClosePages()

		blkno := Blkno(0)
		ptr := GetInMemPage(blkno)
		header := GetPageHeader(ptr)

		// 先锁定页面
		atomic.StoreUint32(&header.stateAtomic, PageStateLockedFlag)
		myLockedPageAdd(blkno, PageStateLockedFlag)

		// 尝试再次锁定页面
		result := tryLockPage(blkno)

		// 验证结果
		assert.False(t, result)
		assert.True(t, PageStateIsLocked(atomic.LoadUint32(&header.stateAtomic)))
		assert.Equal(t, uint32(0), atomic.LoadUint32(&header.usageCountAtomic))
	})

	// 测试用例3：边界条件测试
	t.Run("boundary conditions", func(t *testing.T) {
		// 分配页面
		InitPages(1)
		defer ClosePages()

		// 测试 blkno 为 0
		blkno := Blkno(0)
		ptr := GetInMemPage(blkno)
		header := GetPageHeader(ptr)

		// 确保页面未锁定
		atomic.StoreUint32(&header.stateAtomic, 0)

		// 尝试锁定页面
		result := tryLockPage(blkno)
		assert.True(t, result)
		assert.True(t, PageStateIsLocked(atomic.LoadUint32(&header.stateAtomic)))

		// 测试最大可能的 blkno
		blkno = ^Blkno(0)
		assert.Panics(t, func() {
			tryLockPage(blkno)
		})
	})

	// 测试用例5：尝试锁定带有其他标志的页面
	t.Run("lock page with other flags", func(t *testing.T) {
		// 分配页面
		InitPages(1)
		defer ClosePages()

		blkno := Blkno(0)
		ptr := GetInMemPage(blkno)
		header := GetPageHeader(ptr)

		// 设置页面状态为带有其他标志
		atomic.StoreUint32(&header.stateAtomic, PageStateNoReadFlag)

		// 尝试锁定页面
		result := tryLockPage(blkno)

		// 验证结果
		assert.True(t, result)
		assert.True(t, PageStateIsLocked(atomic.LoadUint32(&header.stateAtomic)))
		assert.True(t, PageStateReadIsBlocked(atomic.LoadUint32(&header.stateAtomic)))
		assert.Equal(t, uint32(0), atomic.LoadUint32(&header.usageCountAtomic))
	})
}

func TestDeclarePageAsLocked(t *testing.T) {
	// 测试用例1：正常声明页面为锁定状态
	t.Run("normal declare", func(t *testing.T) {
		// 分配页面
		InitPages(1)
		defer ClosePages()

		blkno := Blkno(0)
		ptr := GetInMemPage(blkno)
		header := GetPageHeader(ptr)

		// 设置页面状态
		atomic.StoreUint32(&header.stateAtomic, PageStateLockedFlag)

		// 声明页面为锁定状态
		declarePageAsLocked(blkno)

		// 验证页面已被添加到锁定列表
		tPages := GetThreadPages()
		assert.Equal(t, 1, tPages.numberOfMyLockedPages)
		assert.Equal(t, blkno, tPages.myLockedPages[0].blkno)
		assert.Equal(t, uint32(PageStateLockedFlag), tPages.myLockedPages[0].state)
	})

	// 测试用例2：边界条件测试
	t.Run("boundary conditions", func(t *testing.T) {
		// 分配页面
		InitPages(1)
		defer ClosePages()

		// 测试 blkno 为 0
		blkno := Blkno(0)
		ptr := GetInMemPage(blkno)
		header := GetPageHeader(ptr)

		// 设置页面状态
		atomic.StoreUint32(&header.stateAtomic, PageStateLockedFlag)

		// 声明页面为锁定状态
		declarePageAsLocked(blkno)

		// 验证页面已被添加到锁定列表
		tPages := GetThreadPages()
		assert.Equal(t, 1, tPages.numberOfMyLockedPages)
		assert.Equal(t, blkno, tPages.myLockedPages[0].blkno)

		// 测试最大可能的 blkno
		blkno = ^Blkno(0)
		assert.Panics(t, func() {
			declarePageAsLocked(blkno)
		})
	})

	// 测试用例3：声明带有其他标志的页面
	t.Run("declare page with other flags", func(t *testing.T) {
		// 分配页面
		InitPages(1)
		defer ClosePages()

		blkno := Blkno(0)
		ptr := GetInMemPage(blkno)
		header := GetPageHeader(ptr)

		// 设置页面状态为带有其他标志
		atomic.StoreUint32(&header.stateAtomic, PageStateLockedFlag|PageStateNoReadFlag)

		// 声明页面为锁定状态
		declarePageAsLocked(blkno)

		// 验证页面已被添加到锁定列表，且状态包含所有标志
		tPages := GetThreadPages()
		assert.Equal(t, 1, tPages.numberOfMyLockedPages)
		assert.Equal(t, blkno, tPages.myLockedPages[0].blkno)
		assert.Equal(t, uint32(PageStateLockedFlag|PageStateNoReadFlag), tPages.myLockedPages[0].state)
	})

	// 测试用例4：重复声明同一个页面
	t.Run("declare same page twice", func(t *testing.T) {
		// 分配页面
		InitPages(1)
		defer ClosePages()

		blkno := Blkno(0)
		ptr := GetInMemPage(blkno)
		header := GetPageHeader(ptr)

		// 设置页面状态
		atomic.StoreUint32(&header.stateAtomic, PageStateLockedFlag)

		// 第一次声明
		declarePageAsLocked(blkno)

		// 第二次声明（应该触发 panic）
		assert.Panics(t, func() {
			declarePageAsLocked(blkno)
		})

		// 验证页面只被添加一次
		tPages := GetThreadPages()
		assert.Equal(t, 1, tPages.numberOfMyLockedPages)
	})
}

func TestPageIsLocked(t *testing.T) {
	// 测试用例1：页面未被锁定
	t.Run("page not locked", func(t *testing.T) {
		// 分配页面
		InitPages(1)
		defer ClosePages()

		blkno := Blkno(0)
		ptr := GetInMemPage(blkno)
		header := GetPageHeader(ptr)

		// 确保页面未锁定
		atomic.StoreUint32(&header.stateAtomic, 0)

		// 验证页面未被锁定
		assert.False(t, pageIsLocked(blkno))
	})

	// 测试用例2：页面被锁定
	t.Run("page is locked", func(t *testing.T) {
		// 分配页面
		InitPages(1)
		defer ClosePages()

		blkno := Blkno(0)
		ptr := GetInMemPage(blkno)
		header := GetPageHeader(ptr)

		// 锁定页面
		atomic.StoreUint32(&header.stateAtomic, PageStateLockedFlag)
		myLockedPageAdd(blkno, PageStateLockedFlag)

		// 验证页面被锁定
		assert.True(t, pageIsLocked(blkno))
	})

	// 测试用例3：边界条件测试
	t.Run("boundary conditions", func(t *testing.T) {
		// 分配页面
		InitPages(1)
		defer ClosePages()

		// 测试 blkno 为 0
		blkno := Blkno(0)
		ptr := GetInMemPage(blkno)
		header := GetPageHeader(ptr)

		// 锁定页面
		atomic.StoreUint32(&header.stateAtomic, PageStateLockedFlag)
		myLockedPageAdd(blkno, PageStateLockedFlag)

		// 验证页面被锁定
		assert.True(t, pageIsLocked(blkno))

		// 测试最大可能的 blkno
		blkno = ^Blkno(0)
		assert.False(t, pageIsLocked(blkno))
	})

	// 测试用例4：锁定列表为空
	t.Run("empty locked list", func(t *testing.T) {
		// 分配页面
		InitPages(1)
		defer ClosePages()

		blkno := Blkno(0)

		// 确保锁定列表为空
		tPages := GetThreadPages()
		tPages.numberOfMyLockedPages = 0

		// 验证页面未被锁定
		assert.False(t, pageIsLocked(blkno))
	})

	// 测试用例5：锁定列表中有多个页面
	t.Run("multiple locked pages", func(t *testing.T) {
		// 分配页面
		InitPages(2)
		defer ClosePages()

		blkno1 := Blkno(0)
		blkno2 := Blkno(1)
		ptr1 := GetInMemPage(blkno1)
		ptr2 := GetInMemPage(blkno2)
		header1 := GetPageHeader(ptr1)
		header2 := GetPageHeader(ptr2)

		// 锁定第一个页面
		atomic.StoreUint32(&header1.stateAtomic, PageStateLockedFlag)
		myLockedPageAdd(blkno1, PageStateLockedFlag)

		// 锁定第二个页面
		atomic.StoreUint32(&header2.stateAtomic, PageStateLockedFlag)
		myLockedPageAdd(blkno2, PageStateLockedFlag)

		// 验证两个页面都被锁定
		assert.True(t, pageIsLocked(blkno1))
		assert.True(t, pageIsLocked(blkno2))
	})

	// 测试用例6：锁定列表中有重复的页面
	t.Run("duplicate locked pages", func(t *testing.T) {
		// 分配页面
		InitPages(1)
		defer ClosePages()

		blkno := Blkno(0)
		ptr := GetInMemPage(blkno)
		header := GetPageHeader(ptr)

		// 锁定页面
		atomic.StoreUint32(&header.stateAtomic, PageStateLockedFlag)
		myLockedPageAdd(blkno, PageStateLockedFlag)

		// 验证页面被锁定
		assert.True(t, pageIsLocked(blkno))

		// 尝试再次添加相同的页面（应该触发 panic）
		assert.Panics(t, func() {
			myLockedPageAdd(blkno, PageStateLockedFlag)
		})
	})
}

func TestPageBlockReads(t *testing.T) {
	// 测试用例1：正常设置禁止读取标志
	t.Run("normal block reads", func(t *testing.T) {
		// 分配页面
		InitPages(1)
		defer ClosePages()

		blkno := Blkno(0)
		ptr := GetInMemPage(blkno)
		header := GetPageHeader(ptr)

		// 先锁定页面
		atomic.StoreUint32(&header.stateAtomic, PageStateLockedFlag)
		myLockedPageAdd(blkno, PageStateLockedFlag)

		// 设置禁止读取标志
		pageBlockReads(blkno)

		// 验证页面状态
		assert.True(t, PageStateIsLocked(atomic.LoadUint32(&header.stateAtomic)))
		assert.True(t, PageStateReadIsBlocked(atomic.LoadUint32(&header.stateAtomic)))

		// 验证锁定列表中的状态
		tPages := GetThreadPages()
		idx := getMyLockedPageIndex(tPages, blkno)
		assert.Equal(t, uint32(PageStateLockedFlag|PageStateNoReadFlag), tPages.myLockedPages[idx].state)
	})

	// 测试用例2：边界条件测试
	t.Run("boundary conditions", func(t *testing.T) {
		// 分配页面
		InitPages(1)
		defer ClosePages()

		// 测试 blkno 为 0
		blkno := Blkno(0)
		ptr := GetInMemPage(blkno)
		header := GetPageHeader(ptr)

		// 先锁定页面
		atomic.StoreUint32(&header.stateAtomic, PageStateLockedFlag)
		myLockedPageAdd(blkno, PageStateLockedFlag)

		// 设置禁止读取标志
		pageBlockReads(blkno)

		// 验证页面状态
		assert.True(t, PageStateIsLocked(atomic.LoadUint32(&header.stateAtomic)))
		assert.True(t, PageStateReadIsBlocked(atomic.LoadUint32(&header.stateAtomic)))

		// 测试最大可能的 blkno
		blkno = ^Blkno(0)
		assert.Panics(t, func() {
			pageBlockReads(blkno)
		})
	})

	// 测试用例3：页面未被锁定时设置禁止读取标志
	t.Run("block reads on unlocked page", func(t *testing.T) {
		// 分配页面
		InitPages(1)
		defer ClosePages()

		blkno := Blkno(0)
		ptr := GetInMemPage(blkno)
		header := GetPageHeader(ptr)

		// 确保页面未锁定
		atomic.StoreUint32(&header.stateAtomic, 0)

		// 尝试设置禁止读取标志（应该触发 panic）
		assert.Panics(t, func() {
			pageBlockReads(blkno)
		})
	})

	// 测试用例4：页面已经设置了禁止读取标志
	t.Run("page already blocked for reads", func(t *testing.T) {
		// 分配页面
		InitPages(1)
		defer ClosePages()

		blkno := Blkno(0)
		ptr := GetInMemPage(blkno)
		header := GetPageHeader(ptr)

		// 设置页面状态为锁定和禁止读取
		atomic.StoreUint32(&header.stateAtomic, PageStateLockedFlag|PageStateNoReadFlag)
		myLockedPageAdd(blkno, PageStateLockedFlag|PageStateNoReadFlag)

		// 再次设置禁止读取标志
		pageBlockReads(blkno)

		// 验证页面状态保持不变
		assert.True(t, PageStateIsLocked(atomic.LoadUint32(&header.stateAtomic)))
		assert.True(t, PageStateReadIsBlocked(atomic.LoadUint32(&header.stateAtomic)))

		// 验证锁定列表中的状态保持不变
		tPages := GetThreadPages()
		idx := getMyLockedPageIndex(tPages, blkno)
		assert.Equal(t, uint32(PageStateLockedFlag|PageStateNoReadFlag), tPages.myLockedPages[idx].state)
	})

	// 测试用例5：页面带有其他标志时设置禁止读取标志
	t.Run("block reads on page with other flags", func(t *testing.T) {
		// 分配页面
		InitPages(1)
		defer ClosePages()

		blkno := Blkno(0)
		ptr := GetInMemPage(blkno)
		header := GetPageHeader(ptr)

		// 设置页面状态为锁定和其他标志
		otherFlag := uint32(1) // 使用一个简单的位标志
		atomic.StoreUint32(&header.stateAtomic, PageStateLockedFlag|otherFlag)
		myLockedPageAdd(blkno, PageStateLockedFlag|otherFlag)

		// 设置禁止读取标志
		pageBlockReads(blkno)

		// 验证页面状态
		assert.True(t, PageStateIsLocked(atomic.LoadUint32(&header.stateAtomic)))
		assert.True(t, PageStateReadIsBlocked(atomic.LoadUint32(&header.stateAtomic)))
		assert.True(t, util.FlagIsSet(atomic.LoadUint32(&header.stateAtomic), otherFlag))

		// 验证锁定列表中的状态
		tPages := GetThreadPages()
		idx := getMyLockedPageIndex(tPages, blkno)
		assert.Equal(t, PageStateLockedFlag|PageStateNoReadFlag|otherFlag, tPages.myLockedPages[idx].state)
	})
}

func TestReleaseAllPageLocks(t *testing.T) {
	// 测试用例1：正常释放多个页面锁
	t.Run("normal release multiple locks", func(t *testing.T) {
		// 分配页面
		InitPages(3)
		defer ClosePages()

		// 锁定多个页面
		blkno1 := Blkno(0)
		blkno2 := Blkno(1)
		blkno3 := Blkno(2)

		// 锁定第一个页面
		lockPage(blkno1)
		ptr1 := GetInMemPage(blkno1)
		header1 := GetPageHeader(ptr1)

		// 锁定第二个页面
		lockPage(blkno2)
		ptr2 := GetInMemPage(blkno2)
		header2 := GetPageHeader(ptr2)

		// 锁定第三个页面
		lockPage(blkno3)
		ptr3 := GetInMemPage(blkno3)
		header3 := GetPageHeader(ptr3)

		// 验证所有页面都被锁定
		assert.True(t, PageStateIsLocked(atomic.LoadUint32(&header1.stateAtomic)))
		assert.True(t, PageStateIsLocked(atomic.LoadUint32(&header2.stateAtomic)))
		assert.True(t, PageStateIsLocked(atomic.LoadUint32(&header3.stateAtomic)))

		// 释放所有锁
		releaseAllPageLocks()

		// 验证所有页面都已解锁
		assert.False(t, PageStateIsLocked(atomic.LoadUint32(&header1.stateAtomic)))
		assert.False(t, PageStateIsLocked(atomic.LoadUint32(&header2.stateAtomic)))
		assert.False(t, PageStateIsLocked(atomic.LoadUint32(&header3.stateAtomic)))

		// 验证锁定列表为空
		tPages := GetThreadPages()
		assert.Equal(t, 0, tPages.numberOfMyLockedPages)
	})

	// 测试用例2：释放带有不同状态的页面锁
	t.Run("release locks with different states", func(t *testing.T) {
		// 分配页面
		InitPages(3)
		defer ClosePages()

		// 锁定多个页面
		blkno1 := Blkno(0)
		blkno2 := Blkno(1)
		blkno3 := Blkno(2)

		// 锁定第一个页面（普通锁定）
		lockPage(blkno1)
		ptr1 := GetInMemPage(blkno1)
		header1 := GetPageHeader(ptr1)

		// 锁定第二个页面（带禁止读取标志）
		lockPage(blkno2)
		ptr2 := GetInMemPage(blkno2)
		header2 := GetPageHeader(ptr2)
		pageBlockReads(blkno2)

		// 锁定第三个页面（带其他标志）
		lockPage(blkno3)
		ptr3 := GetInMemPage(blkno3)
		header3 := GetPageHeader(ptr3)
		otherFlag := uint32(1)
		atomic.StoreUint32(&header3.stateAtomic, PageStateLockedFlag|otherFlag)
		tPages := GetThreadPages()
		idx := getMyLockedPageIndex(tPages, blkno3)
		tPages.myLockedPages[idx].state |= otherFlag

		// 验证所有页面都被锁定
		assert.True(t, PageStateIsLocked(atomic.LoadUint32(&header1.stateAtomic)))
		assert.True(t, PageStateIsLocked(atomic.LoadUint32(&header2.stateAtomic)))
		assert.True(t, PageStateIsLocked(atomic.LoadUint32(&header3.stateAtomic)))
		assert.True(t, PageStateReadIsBlocked(atomic.LoadUint32(&header2.stateAtomic)))
		assert.True(t, util.FlagIsSet(atomic.LoadUint32(&header3.stateAtomic), otherFlag))

		// 释放所有锁
		releaseAllPageLocks()

		// 验证所有页面都已解锁
		assert.False(t, PageStateIsLocked(atomic.LoadUint32(&header1.stateAtomic)))
		assert.False(t, PageStateIsLocked(atomic.LoadUint32(&header2.stateAtomic)))
		assert.False(t, PageStateIsLocked(atomic.LoadUint32(&header3.stateAtomic)))
		assert.False(t, PageStateReadIsBlocked(atomic.LoadUint32(&header2.stateAtomic)))
		assert.True(t, util.FlagIsSet(atomic.LoadUint32(&header3.stateAtomic), otherFlag))

		// 验证锁定列表为空
		tPages = GetThreadPages()
		assert.Equal(t, 0, tPages.numberOfMyLockedPages)
	})

	// 测试用例3：没有锁定的页面
	t.Run("no locked pages", func(t *testing.T) {
		// 分配页面
		InitPages(1)
		defer ClosePages()

		// 确保没有锁定的页面
		tPages := GetThreadPages()
		assert.Equal(t, 0, tPages.numberOfMyLockedPages)

		// 释放所有锁（不应该有任何影响）
		releaseAllPageLocks()

		// 验证锁定列表仍然为空
		assert.Equal(t, 0, tPages.numberOfMyLockedPages)
	})

	// 测试用例4：并发释放锁
	t.Run("concurrent release", func(t *testing.T) {
		// 分配页面
		InitPages(2)
		defer ClosePages()

		// 锁定两个页面
		blkno1 := Blkno(0)
		blkno2 := Blkno(1)

		// 锁定第一个页面
		ptr1 := GetInMemPage(blkno1)
		header1 := GetPageHeader(ptr1)

		// 锁定第二个页面
		ptr2 := GetInMemPage(blkno2)
		header2 := GetPageHeader(ptr2)

		// 使用两个 goroutine 并发释放锁
		var wg sync.WaitGroup
		wg.Add(2)

		go func() {
			defer wg.Done()
			lockPage(blkno1)
			releaseAllPageLocks()
			assert.False(t, PageStateIsLocked(atomic.LoadUint32(&header1.stateAtomic)))
			tPages := GetThreadPages()
			assert.Equal(t, 0, tPages.numberOfMyLockedPages)
		}()

		go func() {
			defer wg.Done()
			lockPage(blkno2)
			releaseAllPageLocks()
			assert.False(t, PageStateIsLocked(atomic.LoadUint32(&header2.stateAtomic)))
			tPages := GetThreadPages()
			assert.Equal(t, 0, tPages.numberOfMyLockedPages)
		}()

		wg.Wait()
	})
}

func TestBtreeRegisterInProgressSplit(t *testing.T) {
	// 测试用例1：正常注册分裂操作
	t.Run("normal register split", func(t *testing.T) {
		// 分配页面
		InitPages(1)
		defer ClosePages()

		blkno := Blkno(0)
		tPages := GetThreadPages()

		// 注册分裂操作
		btreeRegisterInProgressSplit(blkno)

		// 验证分裂操作已注册
		assert.Equal(t, 1, tPages.numberOfMyInProgressSplitPages)
		assert.Equal(t, blkno, tPages.myInProgressSplitPages[0])
	})

	// 测试用例2：注册最大数量的分裂操作
	t.Run("register max number of splits", func(t *testing.T) {
		tPages := GetThreadPages()
		// 分配页面
		InitPages(len(tPages.myInProgressSplitPages))
		defer ClosePages()

		// 注册最大数量的分裂操作
		for i := 0; i < len(tPages.myInProgressSplitPages); i++ {
			btreeRegisterInProgressSplit(Blkno(i))
		}

		// 验证所有分裂操作都已注册
		assert.Equal(t, len(tPages.myInProgressSplitPages), tPages.numberOfMyInProgressSplitPages)
		for i := 0; i < len(tPages.myInProgressSplitPages); i++ {
			assert.Equal(t, Blkno(i), tPages.myInProgressSplitPages[i])
		}
	})

	// 测试用例3：超出最大数量限制
	t.Run("exceed max number of splits", func(t *testing.T) {
		tPages := GetThreadPages()
		// 分配页面
		InitPages(len(tPages.myInProgressSplitPages) + 1)
		defer ClosePages()

		// 注册最大数量的分裂操作
		for i := 0; i < len(tPages.myInProgressSplitPages); i++ {
			btreeRegisterInProgressSplit(Blkno(i))
		}

		// 尝试注册额外的分裂操作（应该触发 panic）
		assert.Panics(t, func() {
			btreeRegisterInProgressSplit(Blkno(len(tPages.myInProgressSplitPages)))
		})

		// 验证分裂操作数量没有变化
		assert.Equal(t, len(tPages.myInProgressSplitPages), tPages.numberOfMyInProgressSplitPages)
	})

	// 测试用例4：重复注册同一个页面
	t.Run("register same page twice", func(t *testing.T) {
		// 分配页面
		InitPages(1)
		defer ClosePages()

		blkno := Blkno(0)
		tPages := GetThreadPages()

		// 第一次注册
		btreeRegisterInProgressSplit(blkno)

		// 尝试再次注册同一个页面。没有检查不报错
		btreeRegisterInProgressSplit(blkno)

		// 验证分裂操作数量没有变化
		assert.Equal(t, 2, tPages.numberOfMyInProgressSplitPages)
		assert.Equal(t, blkno, tPages.myInProgressSplitPages[0])
		assert.Equal(t, blkno, tPages.myInProgressSplitPages[1])
	})

	// 测试用例5：边界条件测试
	t.Run("boundary conditions", func(t *testing.T) {
		// 分配页面
		InitPages(2)
		defer ClosePages()

		tPages := GetThreadPages()

		// 测试 blkno 为 0
		btreeRegisterInProgressSplit(Blkno(0))
		assert.Equal(t, 1, tPages.numberOfMyInProgressSplitPages)
		assert.Equal(t, Blkno(0), tPages.myInProgressSplitPages[0])

		// 测试最大可能的 blkno
		btreeRegisterInProgressSplit(^Blkno(0))
		assert.Equal(t, 2, tPages.numberOfMyInProgressSplitPages)
		assert.Equal(t, ^Blkno(0), tPages.myInProgressSplitPages[1])
	})
}

func TestBtreeUnregisterInProgressSplit(t *testing.T) {
	// 测试用例1：正常取消注册分裂操作
	t.Run("normal unregister split", func(t *testing.T) {
		// 分配页面
		InitPages(2)
		defer ClosePages()

		blkno1 := Blkno(0)
		blkno2 := Blkno(1)
		tPages := GetThreadPages()

		// 注册两个分裂操作
		btreeRegisterInProgressSplit(blkno1)
		btreeRegisterInProgressSplit(blkno2)

		// 取消注册第一个分裂操作
		btreeUnregisterInProgressSplit(blkno1)

		// 验证分裂操作已取消注册
		assert.Equal(t, 1, tPages.numberOfMyInProgressSplitPages)
		assert.Equal(t, blkno2, tPages.myInProgressSplitPages[0])
	})

	// 测试用例2：取消注册不存在的分裂操作
	t.Run("unregister non-existent split", func(t *testing.T) {
		// 分配页面
		InitPages(1)
		defer ClosePages()

		blkno := Blkno(0)
		tPages := GetThreadPages()

		// 注册一个分裂操作
		btreeRegisterInProgressSplit(blkno)

		// 尝试取消注册不存在的分裂操作（应该触发 panic）
		assert.Panics(t, func() {
			btreeUnregisterInProgressSplit(Blkno(1))
		})

		// 验证分裂操作数量没有变化
		assert.Equal(t, 1, tPages.numberOfMyInProgressSplitPages)
		assert.Equal(t, blkno, tPages.myInProgressSplitPages[0])
	})

	// 测试用例3：边界条件测试
	t.Run("boundary conditions", func(t *testing.T) {
		// 分配页面
		InitPages(2)
		defer ClosePages()

		tPages := GetThreadPages()

		// 测试 blkno 为 0
		btreeRegisterInProgressSplit(Blkno(0))
		btreeUnregisterInProgressSplit(Blkno(0))
		assert.Equal(t, 0, tPages.numberOfMyInProgressSplitPages)

		// 测试最大可能的 blkno
		btreeRegisterInProgressSplit(^Blkno(0))
		btreeUnregisterInProgressSplit(^Blkno(0))
		assert.Equal(t, 0, tPages.numberOfMyInProgressSplitPages)
	})

	// 测试用例4：并发取消注册测试
	t.Run("concurrent unregister", func(t *testing.T) {
		// 分配页面
		InitPages(2)
		defer ClosePages()

		blkno1 := Blkno(0)
		blkno2 := Blkno(1)
		tPages := GetThreadPages()

		// 使用两个 goroutine 并发取消注册
		var wg sync.WaitGroup
		wg.Add(2)

		go func() {
			defer wg.Done()
			btreeRegisterInProgressSplit(blkno1)
			time.Sleep(50 * time.Millisecond)
			btreeUnregisterInProgressSplit(blkno1)
		}()

		go func() {
			defer wg.Done()
			btreeRegisterInProgressSplit(blkno2)
			time.Sleep(50 * time.Millisecond)
			btreeUnregisterInProgressSplit(blkno2)
		}()

		wg.Wait()

		// 验证所有分裂操作都已取消注册
		assert.Equal(t, 0, tPages.numberOfMyInProgressSplitPages)
	})

	// 测试用例5：取消注册最后一个分裂操作
	t.Run("unregister last split", func(t *testing.T) {
		// 分配页面
		InitPages(1)
		defer ClosePages()

		blkno := Blkno(0)
		tPages := GetThreadPages()

		// 注册一个分裂操作
		btreeRegisterInProgressSplit(blkno)

		// 取消注册分裂操作
		btreeUnregisterInProgressSplit(blkno)

		// 验证分裂操作已取消注册
		assert.Equal(t, 0, tPages.numberOfMyInProgressSplitPages)
	})

	// 测试用例6：取消注册中间的分裂操作
	t.Run("unregister middle split", func(t *testing.T) {
		// 分配页面
		InitPages(3)
		defer ClosePages()

		blkno1 := Blkno(0)
		blkno2 := Blkno(1)
		blkno3 := Blkno(2)
		tPages := GetThreadPages()

		// 注册三个分裂操作
		btreeRegisterInProgressSplit(blkno1)
		btreeRegisterInProgressSplit(blkno2)
		btreeRegisterInProgressSplit(blkno3)

		// 取消注册中间的分裂操作
		btreeUnregisterInProgressSplit(blkno2)

		// 验证分裂操作已取消注册，且最后一个元素被移到中间位置
		assert.Equal(t, 2, tPages.numberOfMyInProgressSplitPages)
		assert.Equal(t, blkno1, tPages.myInProgressSplitPages[0])
		assert.Equal(t, blkno3, tPages.myInProgressSplitPages[1])
	})

	// 测试用例7：重复取消注册同一个分裂操作
	t.Run("unregister same split twice", func(t *testing.T) {
		// 分配页面
		InitPages(1)
		defer ClosePages()

		blkno := Blkno(0)
		tPages := GetThreadPages()

		// 注册一个分裂操作
		btreeRegisterInProgressSplit(blkno)

		// 第一次取消注册
		btreeUnregisterInProgressSplit(blkno)
		assert.Equal(t, 0, tPages.numberOfMyInProgressSplitPages)

		// 第二次取消注册（应该触发 panic）
		assert.Panics(t, func() {
			btreeUnregisterInProgressSplit(blkno)
		})

		// 验证分裂操作数量仍然为 0
		assert.Equal(t, 0, tPages.numberOfMyInProgressSplitPages)
	})
}

func TestBtreeSplitMarkFinished(t *testing.T) {
	// 测试用例1：成功完成分裂（使用锁）
	t.Run("successful split with lock", func(t *testing.T) {
		// 分配页面
		InitPages(2)
		defer ClosePages()

		leftBlkno := Blkno(0)
		rightBlkno := Blkno(1)

		// 设置初始状态
		ptr := GetInMemPage(leftBlkno)
		header := (*BTPageHeader)(ptr)
		header.rightLink = uint64(rightBlkno)
		// 注意：use_lock 为 true 时不应该设置 BROKEN_SPLIT 标志
		header.SetFlags(0)

		// 标记分裂完成
		btreeSplitMarkFinished(leftBlkno, true, true)

		// 验证结果
		assert.False(t, PageIs(ptr, BTREE_FLAG_BROKEN_SPLIT))
		assert.Equal(t, InvalidRightLink, header.rightLink)
	})

	// 测试用例2：分裂失败（使用锁）
	t.Run("failed split with lock", func(t *testing.T) {
		// 分配页面
		InitPages(2)
		defer ClosePages()

		leftBlkno := Blkno(0)
		rightBlkno := Blkno(1)

		// 设置初始状态
		ptr := GetInMemPage(leftBlkno)
		header := (*BTPageHeader)(ptr)
		header.rightLink = uint64(rightBlkno)
		// 注意：use_lock 为 true 时不应该设置 BROKEN_SPLIT 标志
		header.SetFlags(0)

		// 标记分裂失败
		btreeSplitMarkFinished(leftBlkno, true, false)

		// 验证结果
		assert.True(t, PageIs(ptr, BTREE_FLAG_BROKEN_SPLIT))
		assert.Equal(t, uint64(rightBlkno), header.rightLink)
	})

	// 测试用例3：成功完成分裂（不使用锁）
	t.Run("successful split without lock", func(t *testing.T) {
		// 分配页面
		InitPages(2)
		defer ClosePages()

		leftBlkno := Blkno(0)
		rightBlkno := Blkno(1)

		// 设置初始状态
		ptr := GetInMemPage(leftBlkno)
		header := (*BTPageHeader)(ptr)
		header.rightLink = uint64(rightBlkno)
		header.SetFlags(BTREE_FLAG_BROKEN_SPLIT)

		// 标记分裂完成
		btreeSplitMarkFinished(leftBlkno, false, true)

		// 验证结果
		assert.False(t, PageIs(ptr, BTREE_FLAG_BROKEN_SPLIT))
		assert.Equal(t, InvalidRightLink, header.rightLink)
	})

	// 测试用例4：分裂失败（不使用锁）
	t.Run("failed split without lock", func(t *testing.T) {
		// 分配页面
		InitPages(2)
		defer ClosePages()

		leftBlkno := Blkno(0)
		rightBlkno := Blkno(1)

		// 设置初始状态
		ptr := GetInMemPage(leftBlkno)
		header := (*BTPageHeader)(ptr)
		header.rightLink = uint64(rightBlkno)
		header.SetFlags(0)

		// 标记分裂失败
		btreeSplitMarkFinished(leftBlkno, false, false)

		// 验证结果
		assert.True(t, PageIs(ptr, BTREE_FLAG_BROKEN_SPLIT))
		assert.Equal(t, uint64(rightBlkno), header.rightLink)
	})

	// 测试用例5：已经有 BROKEN_SPLIT 标志的页面标记为失败
	t.Run("mark failed on already broken split page", func(t *testing.T) {
		// 分配页面
		InitPages(2)
		defer ClosePages()

		leftBlkno := Blkno(0)
		rightBlkno := Blkno(1)

		// 设置初始状态
		ptr := GetInMemPage(leftBlkno)
		header := (*BTPageHeader)(ptr)
		header.rightLink = uint64(rightBlkno)
		header.SetFlags(BTREE_FLAG_BROKEN_SPLIT)

		// 标记分裂失败（不使用锁）
		btreeSplitMarkFinished(leftBlkno, false, false)

		// 验证结果
		assert.True(t, PageIs(ptr, BTREE_FLAG_BROKEN_SPLIT))
		assert.Equal(t, uint64(rightBlkno), header.rightLink)
	})

	// 测试用例6：已经有 BROKEN_SPLIT 标志的页面标记为成功
	t.Run("mark success on broken split page", func(t *testing.T) {
		// 分配页面
		InitPages(2)
		defer ClosePages()

		leftBlkno := Blkno(0)
		rightBlkno := Blkno(1)

		// 设置初始状态
		ptr := GetInMemPage(leftBlkno)
		header := (*BTPageHeader)(ptr)
		header.rightLink = uint64(rightBlkno)
		header.SetFlags(BTREE_FLAG_BROKEN_SPLIT)

		// 标记分裂成功（不使用锁）
		btreeSplitMarkFinished(leftBlkno, false, true)

		// 验证结果
		assert.False(t, PageIs(ptr, BTREE_FLAG_BROKEN_SPLIT))
		assert.Equal(t, InvalidRightLink, header.rightLink)
	})
}

func TestBtreeMarkIncompleteSplits(t *testing.T) {
	// 测试用例1：没有未完成的分裂操作
	t.Run("no incomplete splits", func(t *testing.T) {
		// 分配页面
		InitPages(1)
		defer ClosePages()

		tPages := GetThreadPages()
		// 确保没有未完成的分裂操作
		tPages.numberOfMyInProgressSplitPages = 0

		// 调用函数
		btreeMarkIncompleteSplits()

		// 验证结果
		assert.Equal(t, 0, tPages.numberOfMyInProgressSplitPages)
	})

	// 测试用例2：有一个未完成的分裂操作
	t.Run("one incomplete split", func(t *testing.T) {
		// 分配页面
		InitPages(2)
		defer ClosePages()

		leftBlkno := Blkno(0)
		rightBlkno := Blkno(1)

		// 设置初始状态
		ptr := GetInMemPage(leftBlkno)
		header := (*BTPageHeader)(ptr)
		header.rightLink = uint64(rightBlkno)
		header.SetFlags(0)

		// 注册分裂操作
		tPages := GetThreadPages()
		btreeRegisterInProgressSplit(leftBlkno)

		// 调用函数
		btreeMarkIncompleteSplits()

		// 验证结果
		assert.Equal(t, 0, tPages.numberOfMyInProgressSplitPages)
		assert.True(t, PageIs(ptr, BTREE_FLAG_BROKEN_SPLIT))
		assert.Equal(t, uint64(rightBlkno), header.rightLink)
	})

	// 测试用例3：有多个未完成的分裂操作
	t.Run("multiple incomplete splits", func(t *testing.T) {
		// 分配页面
		InitPages(4)
		defer ClosePages()

		// 设置多个分裂操作
		blknos := []Blkno{0, 1, 2, 3}
		headers := make([]*BTPageHeader, len(blknos))

		// 初始化每个页面的状态
		for i, blkno := range blknos {
			ptr := GetInMemPage(blkno)
			header := (*BTPageHeader)(ptr)
			header.rightLink = uint64(blkno + 1)
			header.SetFlags(0)
			headers[i] = header
		}

		// 注册所有分裂操作
		tPages := GetThreadPages()
		for _, blkno := range blknos {
			btreeRegisterInProgressSplit(blkno)
		}

		// 调用函数
		btreeMarkIncompleteSplits()

		// 验证结果
		assert.Equal(t, 0, tPages.numberOfMyInProgressSplitPages)
		for i, header := range headers {
			assert.True(t, PageIs(unsafe.Pointer(header), BTREE_FLAG_BROKEN_SPLIT))
			assert.Equal(t, uint64(blknos[i]+1), header.rightLink)
		}
	})

	// 测试用例4：边界条件测试
	t.Run("boundary conditions", func(t *testing.T) {
		// 分配页面
		InitPages(2)
		defer ClosePages()

		// 测试最大可能的 blkno（gPageCount-1）
		blkno := Blkno(gPageCount - 1)
		ptr := GetInMemPage(blkno)
		header := (*BTPageHeader)(ptr)
		header.rightLink = uint64(0)
		header.SetFlags(0)

		// 注册分裂操作
		tPages := GetThreadPages()
		btreeRegisterInProgressSplit(blkno)

		// 调用函数
		btreeMarkIncompleteSplits()

		// 验证结果
		assert.Equal(t, 0, tPages.numberOfMyInProgressSplitPages)
		assert.True(t, PageIs(unsafe.Pointer(header), BTREE_FLAG_BROKEN_SPLIT))
		assert.Equal(t, uint64(0), header.rightLink)
	})

	// 测试用例5：并发测试
	t.Run("concurrent test", func(t *testing.T) {
		// 分配页面
		InitPages(4)
		defer ClosePages()

		// 设置多个分裂操作
		blknos := []Blkno{0, 1}
		headers := make([]*BTPageHeader, len(blknos))

		// 初始化每个页面的状态
		for i, blkno := range blknos {
			ptr := GetInMemPage(blkno)
			header := (*BTPageHeader)(ptr)
			header.rightLink = uint64(blkno + 1)
			header.SetFlags(0)
			headers[i] = header
		}

		// 使用多个 goroutine 并发注册和标记分裂操作
		var wg sync.WaitGroup
		numGoroutines := 2
		wg.Add(numGoroutines)

		for i := 0; i < numGoroutines; i++ {
			go func(idx int) {
				defer wg.Done()
				// 注册分裂操作
				btreeRegisterInProgressSplit(blknos[idx])
				// 等待一段时间
				time.Sleep(50 * time.Millisecond)
				// 标记未完成的分裂操作
				btreeMarkIncompleteSplits()
			}(i)
		}

		wg.Wait()

		// 验证结果
		tPages := GetThreadPages()
		assert.Equal(t, 0, tPages.numberOfMyInProgressSplitPages)
		for i, header := range headers {
			assert.True(t, PageIs(unsafe.Pointer(header), BTREE_FLAG_BROKEN_SPLIT))
			assert.Equal(t, uint64(blknos[i]+1), header.rightLink)
		}
	})
}

func TestPageChangeUsageCount(t *testing.T) {
	// 测试用例1：正常修改使用计数
	t.Run("normal change usage count", func(t *testing.T) {
		// 分配页面
		InitPages(1)
		defer ClosePages()

		blkno := Blkno(0)
		ptr := GetInMemPage(blkno)
		header := GetPageHeader(ptr)

		// 设置初始使用计数
		initialCount := uint32(5)
		atomic.StoreUint32(&header.usageCountAtomic, initialCount)

		// 修改使用计数
		newCount := uint32(10)
		pageChangeUsageCount(blkno, newCount)

		// 验证结果
		assert.Equal(t, newCount, atomic.LoadUint32(&header.usageCountAtomic))
	})

	// 测试用例2：边界值测试
	t.Run("boundary values", func(t *testing.T) {
		// 分配页面
		InitPages(1)
		defer ClosePages()

		blkno := Blkno(0)
		ptr := GetInMemPage(blkno)
		header := GetPageHeader(ptr)

		// 测试最小值和最大值
		testCases := []struct {
			name  string
			count uint32
		}{
			{"min value", 0},
			{"max value", ^uint32(0)},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				pageChangeUsageCount(blkno, tc.count)
				assert.Equal(t, tc.count, atomic.LoadUint32(&header.usageCountAtomic))
			})
		}
	})

	// 测试用例3：无效 blkno 测试
	t.Run("invalid blkno", func(t *testing.T) {
		// 分配页面
		InitPages(1)
		defer ClosePages()

		// 测试超出范围的 blkno
		invalidBlkno := Blkno(gPageCount)
		assert.Panics(t, func() {
			pageChangeUsageCount(invalidBlkno, 1)
		})
	})

	// 测试用例4：多次修改测试
	t.Run("multiple changes", func(t *testing.T) {
		// 分配页面
		InitPages(1)
		defer ClosePages()

		blkno := Blkno(0)
		ptr := GetInMemPage(blkno)
		header := GetPageHeader(ptr)

		// 进行多次修改
		changes := []uint32{1, 2, 3, 4, 5}
		for _, count := range changes {
			pageChangeUsageCount(blkno, count)
			assert.Equal(t, count, atomic.LoadUint32(&header.usageCountAtomic))
		}
	})
}
