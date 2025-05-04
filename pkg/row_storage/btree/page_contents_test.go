package btree

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_initNewBtreePage(t *testing.T) {
	type args struct {
		desc   *BTDesc
		blkno  Blkno
		flags  uint16
		level  uint16
		noLock bool
	}
	tests := []struct {
		name     string
		args     args
		prepare  func(arg *args)
		teardown func(arg *args)
		wantErr  assert.ErrorAssertionFunc
	}{
		{
			name: "test1",
			args: args{
				desc: &BTDesc{
					oids: RelOids{
						datoid:  1,
						reloid:  2,
						relnode: 3,
					},
					idxType: IndexRegular,
				},
				blkno:  0,
				flags:  BTREE_FLAG_LEAF,
				noLock: true,
			},
			prepare: func(arg *args) {
				InitPages(1)
			},
			teardown: func(arg *args) {
				ClosePages()
			},
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				arg := i[0].(*args)
				pagePtr := GetInMemPage(arg.blkno)
				pageHeader := GetPageHeader(pagePtr)
				assert.Equal(t, arg.desc.oids.datoid, Oid(1))
				assert.Equal(t, arg.desc.oids.reloid, Oid(2))
				assert.Equal(t, arg.desc.oids.relnode, Oid(3))
				assert.Equal(t, arg.desc.idxType, IndexType(IndexRegular))
				assert.Equal(t, arg.blkno, Blkno(0))
				assert.Equal(t, uint32(1), pageHeader.usageCountAtomic)
				pageDesc := GetInMemPageDesc(arg.blkno)
				assert.Equal(t, arg.desc.oids.datoid, pageDesc.oids.datoid)
				assert.Equal(t, arg.desc.oids.reloid, pageDesc.oids.reloid)
				assert.Equal(t, arg.desc.oids.relnode, pageDesc.oids.relnode)

				btPageHeader := (*BTPageHeader)(pagePtr)
				assert.Equal(t, uint16(BTREE_FLAG_LEAF), btPageHeader.GetFlags())
				assert.Equal(t, uint16(0), btPageHeader.GetField1())
				assert.Equal(t, uint16(0), btPageHeader.GetField2())
				assert.Equal(t, uint64(InvalidRightLink), btPageHeader.rightLink)
				assert.Equal(t, COMMITSEQNO_FROZEN, btPageHeader.csn)
				assert.Equal(t, InvalidUndoLocation, btPageHeader.undoLocation)
				assert.Equal(t, MaxOffsetNumber, btPageHeader.prevInsertOffset)
				return assert.NoError(t, err)
			},
		},
		{
			name: "test2",
			args: args{
				desc: &BTDesc{
					oids: RelOids{
						datoid:  1,
						reloid:  2,
						relnode: 3,
					},
					idxType: IndexRegular,
				},
				blkno:  0,
				flags:  BTREE_FLAG_LEFTMOST,
				noLock: true,
				level:  1,
			},
			prepare: func(arg *args) {
				InitPages(1)
			},
			teardown: func(arg *args) {
				ClosePages()
			},
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				arg := i[0].(*args)
				pagePtr := GetInMemPage(arg.blkno)
				pageHeader := GetPageHeader(pagePtr)
				assert.Equal(t, arg.desc.oids.datoid, Oid(1))
				assert.Equal(t, arg.desc.oids.reloid, Oid(2))
				assert.Equal(t, arg.desc.oids.relnode, Oid(3))
				assert.Equal(t, arg.desc.idxType, IndexType(IndexRegular))
				assert.Equal(t, arg.blkno, Blkno(0))
				assert.Equal(t, uint32(1), pageHeader.usageCountAtomic)
				pageDesc := GetInMemPageDesc(arg.blkno)
				assert.Equal(t, arg.desc.oids.datoid, pageDesc.oids.datoid)
				assert.Equal(t, arg.desc.oids.reloid, pageDesc.oids.reloid)
				assert.Equal(t, arg.desc.oids.relnode, pageDesc.oids.relnode)

				btPageHeader := (*BTPageHeader)(pagePtr)
				assert.Equal(t, uint16(BTREE_FLAG_LEFTMOST), btPageHeader.GetFlags())
				assert.Equal(t, uint16(1), btPageHeader.GetField1())
				assert.Equal(t, uint16(0), btPageHeader.GetField2())
				assert.Equal(t, uint64(InvalidRightLink), btPageHeader.rightLink)
				assert.Equal(t, COMMITSEQNO_FROZEN, btPageHeader.csn)
				assert.Equal(t, InvalidUndoLocation, btPageHeader.undoLocation)
				assert.Equal(t, MaxOffsetNumber, btPageHeader.prevInsertOffset)

				return assert.NoError(t, err)
			},
		},
	}
	for _, tt := range tests {
		if tt.prepare != nil {
			tt.prepare(&tt.args)
		}
		t.Run(tt.name,
			func(t *testing.T) {
				initNewBtreePage(tt.args.desc, tt.args.blkno, tt.args.flags, tt.args.level, tt.args.noLock)
				tt.wantErr(t, nil, &tt.args)
			})
		if tt.teardown != nil {
			tt.teardown(&tt.args)
		}
	}
}

func TestPageFlagsAndFields(t *testing.T) {
	// 测试用例1：页面标志测试
	t.Run("page flags", func(t *testing.T) {
		// 分配页面
		InitPages(1)
		defer ClosePages()

		blkno := Blkno(0)
		ptr := GetInMemPage(blkno)
		header := (*BTPageHeader)(ptr)

		// 测试设置和检查各种标志
		flags := []uint16{
			BTREE_FLAG_LEAF,
			BTREE_FLAG_RIGHTMOST,
			BTREE_FLAG_BROKEN_SPLIT,
		}

		for _, flag := range flags {
			t.Run(fmt.Sprintf("flag %d", flag), func(t *testing.T) {
				// 设置标志
				header.SetFlags(flag)
				// 验证标志已设置
				assert.True(t, PageIs(ptr, flag))
				// 清除标志
				header.SetFlags(0)
				// 验证标志已清除
				assert.False(t, PageIs(ptr, flag))
			})
		}
	})

	// 测试用例2：设置页面层级
	t.Run("set page level", func(t *testing.T) {
		// 分配页面
		InitPages(1)
		defer ClosePages()

		blkno := Blkno(0)
		ptr := GetInMemPage(blkno)
		header := (*BTPageHeader)(ptr)

		// 确保页面不是叶子节点
		header.SetFlags(0)

		// 测试设置不同的层级
		levels := []uint16{0, 1, 2, 3, 4}
		for _, level := range levels {
			t.Run(fmt.Sprintf("level %d", level), func(t *testing.T) {
				PageSetLevel(ptr, level)
				assert.Equal(t, level, header.GetField1())
			})
		}

		// 测试在叶子节点上设置层级（应该触发 panic）
		header.SetFlags(BTREE_FLAG_LEAF)
		assert.Panics(t, func() {
			PageSetLevel(ptr, 1)
		})
	})

	// 测试用例3：设置非叶子节点的磁盘数量
	t.Run("set non-leaf n on disk", func(t *testing.T) {
		// 分配页面
		InitPages(1)
		defer ClosePages()

		blkno := Blkno(0)
		ptr := GetInMemPage(blkno)
		header := (*BTPageHeader)(ptr)

		// 确保页面不是叶子节点
		header.SetFlags(0)

		// 测试设置不同的磁盘数量
		values := []uint16{0, 1, 2, 3, 4}
		for _, value := range values {
			t.Run(fmt.Sprintf("n on disk %d", value), func(t *testing.T) {
				PageSetNOnDisk(ptr, value)
				assert.Equal(t, value, header.GetField2())
			})
		}

		// 测试在叶子节点上设置磁盘数量（应该触发 panic）
		header.SetFlags(BTREE_FLAG_LEAF)
		assert.Panics(t, func() {
			PageSetNOnDisk(ptr, 1)
		})
	})

	// 测试用例4：设置叶子节点的空闲数量
	t.Run("set leaf n vacated", func(t *testing.T) {
		// 分配页面
		InitPages(1)
		defer ClosePages()

		blkno := Blkno(0)
		ptr := GetInMemPage(blkno)
		header := (*BTPageHeader)(ptr)

		// 确保页面是叶子节点
		header.SetFlags(BTREE_FLAG_LEAF)

		// 测试设置不同的空闲数量
		values := []uint16{0, 1, 2, 3, 4}
		for _, value := range values {
			t.Run(fmt.Sprintf("n vacated %d", value), func(t *testing.T) {
				PageSetNVacated(ptr, value)
				assert.Equal(t, value, header.GetField2())
			})
		}

		// 测试在非叶子节点上设置空闲数量（应该触发 panic）
		header.SetFlags(0)
		assert.Panics(t, func() {
			PageSetNVacated(ptr, 1)
		})
	})

	// 测试用例5：组合测试
	t.Run("combined tests", func(t *testing.T) {
		// 分配页面
		InitPages(1)
		defer ClosePages()

		blkno := Blkno(0)
		ptr := GetInMemPage(blkno)
		header := (*BTPageHeader)(ptr)

		// 测试非叶子节点
		t.Run("non-leaf page", func(t *testing.T) {
			// 设置非叶子节点标志
			header.SetFlags(0)
			assert.False(t, PageIs(ptr, BTREE_FLAG_LEAF))

			// 设置层级和磁盘数量
			PageSetLevel(ptr, 2)
			PageSetNOnDisk(ptr, 3)

			// 验证设置的值
			assert.Equal(t, uint16(2), header.GetField1())
			assert.Equal(t, uint16(3), header.GetField2())
		})

		// 测试叶子节点
		t.Run("leaf page", func(t *testing.T) {
			// 设置叶子节点标志
			header.SetFlags(BTREE_FLAG_LEAF)
			assert.True(t, PageIs(ptr, BTREE_FLAG_LEAF))

			// 设置空闲数量
			PageSetNVacated(ptr, 4)

			// 验证设置的值
			assert.Equal(t, uint16(4), header.GetField2())
		})
	})
}

func TestInitNewBtreePage(t *testing.T) {
	// 准备测试数据
	desc := &BTDesc{
		oids: RelOids{
			datoid:  1,
			reloid:  2,
			relnode: 3,
		},
		idxType: IndexRegular,
	}

	tests := []struct {
		name    string
		blkno   Blkno
		flags   uint16
		level   uint16
		noLock  bool
		prepare func()
		verify  func(t *testing.T, blkno Blkno)
	}{
		{
			name:   "leaf page with lock",
			blkno:  0,
			flags:  BTREE_FLAG_LEAF,
			level:  0,
			noLock: false,
			prepare: func() {
				InitPages(1)
			},
			verify: func(t *testing.T, blkno Blkno) {
				pagePtr := GetInMemPage(blkno)
				pageDesc := GetInMemPageDesc(blkno)
				header := (*BTPageHeader)(pagePtr)

				// 验证页面描述符
				assert.Equal(t, desc.oids.datoid, pageDesc.oids.datoid)
				assert.Equal(t, desc.oids.reloid, pageDesc.oids.reloid)
				assert.Equal(t, desc.oids.relnode, pageDesc.oids.relnode)
				assert.Equal(t, uint32(desc.idxType), pageDesc.GetType())

				// 验证页面头
				assert.Equal(t, BTREE_FLAG_LEAF, header.GetFlags())
				assert.Equal(t, uint16(0), header.GetField1()) // leaf page 的 field1 应该为 0
				assert.Equal(t, uint16(0), header.GetField2()) // leaf page 的 field2 应该为 0
				assert.Equal(t, InvalidRightLink, header.rightLink)
				assert.Equal(t, COMMITSEQNO_FROZEN, header.csn)
				assert.Equal(t, InvalidUndoLocation, header.undoLocation)
				assert.Equal(t, uint32(0), header.checkpointNum)
				assert.Equal(t, OffsetNumber(0), header.itemsCount)
				assert.Equal(t, MaxOffsetNumber, header.prevInsertOffset)
				assert.Equal(t, LocationIndex(0), header.maxKeyLen)
			},
		},
		{
			name:   "non-leaf page with lock",
			blkno:  0,
			flags:  0,
			level:  1,
			noLock: false,
			prepare: func() {
				InitPages(1)
			},
			verify: func(t *testing.T, blkno Blkno) {
				pagePtr := GetInMemPage(blkno)
				pageDesc := GetInMemPageDesc(blkno)
				header := (*BTPageHeader)(pagePtr)

				// 验证页面描述符
				assert.Equal(t, desc.oids.datoid, pageDesc.oids.datoid)
				assert.Equal(t, desc.oids.reloid, pageDesc.oids.reloid)
				assert.Equal(t, desc.oids.relnode, pageDesc.oids.relnode)
				assert.Equal(t, uint32(desc.idxType), pageDesc.GetType())

				// 验证页面头
				assert.Equal(t, uint16(0), header.GetFlags())
				assert.Equal(t, uint16(1), header.GetField1()) // non-leaf page 的 field1 应该为 level
				assert.Equal(t, uint16(0), header.GetField2()) // non-leaf page 的 field2 应该为 0
				assert.Equal(t, InvalidRightLink, header.rightLink)
				assert.Equal(t, COMMITSEQNO_FROZEN, header.csn)
				assert.Equal(t, InvalidUndoLocation, header.undoLocation)
				assert.Equal(t, uint32(0), header.checkpointNum)
				assert.Equal(t, OffsetNumber(0), header.itemsCount)
				assert.Equal(t, MaxOffsetNumber, header.prevInsertOffset)
				assert.Equal(t, LocationIndex(0), header.maxKeyLen)
			},
		},
		{
			name:   "leaf page without lock",
			blkno:  0,
			flags:  BTREE_FLAG_LEAF,
			level:  0,
			noLock: true,
			prepare: func() {
				InitPages(1)
			},
			verify: func(t *testing.T, blkno Blkno) {
				pagePtr := GetInMemPage(blkno)
				pageDesc := GetInMemPageDesc(blkno)
				header := (*BTPageHeader)(pagePtr)

				// 验证页面描述符
				assert.Equal(t, desc.oids.datoid, pageDesc.oids.datoid)
				assert.Equal(t, desc.oids.reloid, pageDesc.oids.reloid)
				assert.Equal(t, desc.oids.relnode, pageDesc.oids.relnode)
				assert.Equal(t, uint32(desc.idxType), pageDesc.GetType())

				// 验证页面头
				assert.Equal(t, BTREE_FLAG_LEAF, header.GetFlags())
				assert.Equal(t, uint16(0), header.GetField1())
				assert.Equal(t, uint16(0), header.GetField2())
				assert.Equal(t, InvalidRightLink, header.rightLink)
				assert.Equal(t, COMMITSEQNO_FROZEN, header.csn)
				assert.Equal(t, InvalidUndoLocation, header.undoLocation)
				assert.Equal(t, uint32(0), header.checkpointNum)
				assert.Equal(t, OffsetNumber(0), header.itemsCount)
				assert.Equal(t, MaxOffsetNumber, header.prevInsertOffset)
				assert.Equal(t, LocationIndex(0), header.maxKeyLen)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.prepare != nil {
				tt.prepare()
			}

			initNewBtreePage(desc, tt.blkno, tt.flags, tt.level, tt.noLock)

			if tt.verify != nil {
				tt.verify(t, tt.blkno)
			}

			ClosePages()
		})
	}
}
