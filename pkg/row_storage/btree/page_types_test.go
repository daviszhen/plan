package btree

import (
	"testing"
	"unsafe"

	"github.com/daviszhen/plan/pkg/util"
	"github.com/stretchr/testify/assert"
)

func TestBTPageHeaderFlags(t *testing.T) {
	// 创建一个测试用的页面
	page := make([]byte, 8192) // 标准页面大小
	header := (*BTPageHeader)(unsafe.Pointer(&page[0]))

	// 测试设置和获取标志
	tests := []struct {
		name  string
		flag  uint16
		value uint16
	}{
		{"BTREE_FLAG_LEFTMOST", BTREE_FLAG_LEFTMOST, BTREE_FLAG_LEFTMOST},
		{"BTREE_FLAG_RIGHTMOST", BTREE_FLAG_RIGHTMOST, BTREE_FLAG_RIGHTMOST},
		{"BTREE_FLAG_LEAF", BTREE_FLAG_LEAF, BTREE_FLAG_LEAF},
		{"BTREE_FLAG_BROKEN_SPLIT", BTREE_FLAG_BROKEN_SPLIT, BTREE_FLAG_BROKEN_SPLIT},
		{"BTREE_FLAG_PRE_CLEANUP", BTREE_FLAG_PRE_CLEANUP, BTREE_FLAG_PRE_CLEANUP},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 设置标志
			header.SetFlags(tt.flag)
			// 验证标志已设置
			assert.Equal(t, tt.value, header.GetFlags()&tt.flag)
			// 清除标志
			header.SetFlags(0)
			// 验证标志已清除
			assert.Equal(t, uint16(0), header.GetFlags()&tt.flag)
		})
	}
}

func TestBTPageHeaderField1(t *testing.T) {
	page := make([]byte, 8192)
	header := (*BTPageHeader)(unsafe.Pointer(&page[0]))

	// 测试设置和获取 Field1
	tests := []struct {
		name  string
		value uint16
	}{
		{"zero", 0},
		{"one", 1},
		{"max", 0x7FF}, // 最大有效值
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 设置值
			header.SetField1(tt.value)
			// 验证值
			assert.Equal(t, tt.value, header.GetField1())
		})
	}
}

func TestBTPageHeaderField2(t *testing.T) {
	page := make([]byte, 8192)
	header := (*BTPageHeader)(unsafe.Pointer(&page[0]))

	// 测试设置和获取 Field2
	tests := []struct {
		name  string
		value uint16
	}{
		{"zero", 0},
		{"one", 1},
		{"max", 0x7FFF}, // 最大有效值
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 设置值
			header.SetField2(tt.value)
			// 验证值
			assert.Equal(t, tt.value, header.GetField2())
		})
	}
}

func TestPageIs(t *testing.T) {
	page := make([]byte, 8192)
	header := (*BTPageHeader)(unsafe.Pointer(&page[0]))

	// 测试 PageIs 函数
	tests := []struct {
		name  string
		flag  uint16
		value bool
	}{
		{"BTREE_FLAG_LEFTMOST", BTREE_FLAG_LEFTMOST, true},
		{"BTREE_FLAG_RIGHTMOST", BTREE_FLAG_RIGHTMOST, true},
		{"BTREE_FLAG_LEAF", BTREE_FLAG_LEAF, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 设置标志
			header.SetFlags(tt.flag)
			// 验证 PageIs 函数
			assert.Equal(t, tt.value, PageIs(unsafe.Pointer(&page[0]), tt.flag))
			// 清除标志
			header.SetFlags(0)
			// 验证标志已清除
			assert.False(t, PageIs(unsafe.Pointer(&page[0]), tt.flag))
		})
	}
}

func TestRightLinkIsValid(t *testing.T) {
	tests := []struct {
		name      string
		rightLink uint64
		expected  bool
	}{
		{"valid", 12345, true},
		{"invalid", InvalidRightLink, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, RightLinkIsValid(tt.rightLink))
		})
	}
}

func TestPageDescFlagsAndType(t *testing.T) {
	desc := &PageDesc{}

	// 测试标志位和类型的组合操作
	tests := []struct {
		name     string
		flags    uint32
		typeVal  uint32
		expected uint32
	}{
		{
			name:     "set flags only",
			flags:    0xF,
			typeVal:  0,
			expected: 0xF,
		},
		{
			name:     "set type only",
			flags:    0,
			typeVal:  0xFFFFFFF,
			expected: 0xFFFFFFF0,
		},
		{
			name:     "set both",
			flags:    0xF,
			typeVal:  0xFFFFFFF,
			expected: 0xFFFFFFF0 | 0xF,
		},
		{
			name:     "set partial flags",
			flags:    0x5,
			typeVal:  0xFFFFFFF,
			expected: 0xFFFFFFF0 | 0x5,
		},
		{
			name:     "set partial type",
			flags:    0xF,
			typeVal:  0x1234567,
			expected: 0x12345670 | 0xF,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 重置描述符
			desc.flags_type = 0

			// 设置标志位
			desc.SetFlags(tt.flags)
			// 设置类型
			desc.SetType(tt.typeVal)

			// 验证标志位
			assert.Equal(t, tt.flags&0xF, desc.GetFlags())
			// 验证类型
			assert.Equal(t, tt.typeVal&0xFFFFFFF, desc.GetType())
			// 验证整体值
			assert.Equal(t, tt.expected, desc.flags_type)
		})
	}
}

func TestPageDescMultipleOperations(t *testing.T) {
	desc := &PageDesc{}

	// 测试多次操作
	t.Run("multiple operations", func(t *testing.T) {
		// 第一次设置
		desc.SetFlags(0x5)
		desc.SetType(0x1234567)
		assert.Equal(t, uint32(0x5), desc.GetFlags())
		assert.Equal(t, uint32(0x1234567), desc.GetType())

		// 第二次设置
		desc.SetFlags(0xA)
		desc.SetType(0x89ABCDE)
		assert.Equal(t, uint32(0xA), desc.GetFlags()) // 新实现会覆盖而不是或操作
		assert.Equal(t, uint32(0x89ABCDE), desc.GetType())

		// 第三次设置
		desc.SetFlags(0x3)
		desc.SetType(0x1111111)
		assert.Equal(t, uint32(0x3), desc.GetFlags()) // 新实现会覆盖而不是或操作
		assert.Equal(t, uint32(0x1111111), desc.GetType())
	})

	// 测试边界值
	t.Run("boundary values", func(t *testing.T) {
		desc.flags_type = 0

		// 测试最大标志位值
		desc.SetFlags(0xFFFFFFFF)
		assert.Equal(t, uint32(0xF), desc.GetFlags())

		// 测试最大类型值
		desc.SetType(0xFFFFFFFF)
		assert.Equal(t, uint32(0xFFFFFFF), desc.GetType())
	})

	// 测试清除操作
	t.Run("clear operations", func(t *testing.T) {
		// 设置初始值
		desc.SetFlags(0xF)
		desc.SetType(0xFFFFFFF)
		assert.Equal(t, uint32(0xF), desc.GetFlags())
		assert.Equal(t, uint32(0xFFFFFFF), desc.GetType())

		// 清除标志位
		desc.SetFlags(0)
		assert.Equal(t, uint32(0), desc.GetFlags())
		assert.Equal(t, uint32(0xFFFFFFF), desc.GetType())

		// 清除类型
		desc.SetType(0)
		assert.Equal(t, uint32(0), desc.GetFlags())
		assert.Equal(t, uint32(0), desc.GetType())
	})
}

func TestBTPageChunkDesc(t *testing.T) {
	desc := &BTPageChunkDesc{}

	// 测试单个字段设置和获取
	t.Run("single field operations", func(t *testing.T) {
		// 测试 shortLocation
		desc.SetShortLocation(0xABC)
		assert.Equal(t, uint32(0xABC), desc.GetShortLocation())

		// 测试 offset
		desc.SetOffset(0x3FF)
		assert.Equal(t, uint32(0x3FF), desc.GetOffset())

		// 测试 hikeyShortLocation
		desc.SetHikeyShortLocation(0xFF)
		assert.Equal(t, uint32(0xFF), desc.GetHikeyShortLocation())

		// 测试 hikeyFlags
		desc.SetHikeyFlags(0x3)
		assert.Equal(t, uint32(0x3), desc.GetHikeyFlags())
	})

	// 测试字段独立性
	t.Run("field independence", func(t *testing.T) {
		desc.fields = 0

		// 设置所有字段的初始值
		desc.SetShortLocation(0x123)
		desc.SetOffset(0x2AA)
		desc.SetHikeyShortLocation(0x55)
		desc.SetHikeyFlags(0x2)

		// 测试修改 shortLocation 不影响其他字段
		originalOffset := desc.GetOffset()
		originalHikeyShortLocation := desc.GetHikeyShortLocation()
		originalHikeyFlags := desc.GetHikeyFlags()

		desc.SetShortLocation(0x456)
		assert.Equal(t, uint32(0x456), desc.GetShortLocation())
		assert.Equal(t, originalOffset, desc.GetOffset())
		assert.Equal(t, originalHikeyShortLocation, desc.GetHikeyShortLocation())
		assert.Equal(t, originalHikeyFlags, desc.GetHikeyFlags())

		// 测试修改 offset 不影响其他字段
		originalShortLocation := desc.GetShortLocation()
		originalHikeyShortLocation = desc.GetHikeyShortLocation()
		originalHikeyFlags = desc.GetHikeyFlags()

		desc.SetOffset(0x1FF)
		assert.Equal(t, uint32(0x1FF), desc.GetOffset())
		assert.Equal(t, originalShortLocation, desc.GetShortLocation())
		assert.Equal(t, originalHikeyShortLocation, desc.GetHikeyShortLocation())
		assert.Equal(t, originalHikeyFlags, desc.GetHikeyFlags())

		// 测试修改 hikeyShortLocation 不影响其他字段
		originalShortLocation = desc.GetShortLocation()
		originalOffset = desc.GetOffset()
		originalHikeyFlags = desc.GetHikeyFlags()

		desc.SetHikeyShortLocation(0xAA)
		assert.Equal(t, uint32(0xAA), desc.GetHikeyShortLocation())
		assert.Equal(t, originalShortLocation, desc.GetShortLocation())
		assert.Equal(t, originalOffset, desc.GetOffset())
		assert.Equal(t, originalHikeyFlags, desc.GetHikeyFlags())

		// 测试修改 hikeyFlags 不影响其他字段
		originalShortLocation = desc.GetShortLocation()
		originalOffset = desc.GetOffset()
		originalHikeyShortLocation = desc.GetHikeyShortLocation()

		desc.SetHikeyFlags(0x1)
		assert.Equal(t, uint32(0x1), desc.GetHikeyFlags())
		assert.Equal(t, originalShortLocation, desc.GetShortLocation())
		assert.Equal(t, originalOffset, desc.GetOffset())
		assert.Equal(t, originalHikeyShortLocation, desc.GetHikeyShortLocation())
	})

	// 测试组合操作
	t.Run("combined operations", func(t *testing.T) {
		desc.fields = 0

		// 设置所有字段
		desc.SetShortLocation(0x123)
		desc.SetOffset(0x2AA)
		desc.SetHikeyShortLocation(0x55)
		desc.SetHikeyFlags(0x2)

		// 验证所有字段
		assert.Equal(t, uint32(0x123), desc.GetShortLocation())
		assert.Equal(t, uint32(0x2AA), desc.GetOffset())
		assert.Equal(t, uint32(0x55), desc.GetHikeyShortLocation())
		assert.Equal(t, uint32(0x2), desc.GetHikeyFlags())

		// 验证整体值
		expected := uint32(0x2)<<30 | uint32(0x55)<<22 | uint32(0x2AA)<<12 | uint32(0x123)
		assert.Equal(t, expected, desc.fields)
	})

	// 测试边界值
	t.Run("boundary values", func(t *testing.T) {
		desc.fields = 0

		// 测试最大有效值
		desc.SetShortLocation(0xFFF)
		desc.SetOffset(0x3FF)
		desc.SetHikeyShortLocation(0xFF)
		desc.SetHikeyFlags(0x3)

		assert.Equal(t, uint32(0xFFF), desc.GetShortLocation())
		assert.Equal(t, uint32(0x3FF), desc.GetOffset())
		assert.Equal(t, uint32(0xFF), desc.GetHikeyShortLocation())
		assert.Equal(t, uint32(0x3), desc.GetHikeyFlags())

		// 测试超出范围的值
		desc.SetShortLocation(0xFFFFFFFF)
		desc.SetOffset(0xFFFFFFFF)
		desc.SetHikeyShortLocation(0xFFFFFFFF)
		desc.SetHikeyFlags(0xFFFFFFFF)

		assert.Equal(t, uint32(0xFFF), desc.GetShortLocation())
		assert.Equal(t, uint32(0x3FF), desc.GetOffset())
		assert.Equal(t, uint32(0xFF), desc.GetHikeyShortLocation())
		assert.Equal(t, uint32(0x3), desc.GetHikeyFlags())
	})

	// 测试清除操作
	t.Run("clear operations", func(t *testing.T) {
		// 设置初始值
		desc.SetShortLocation(0xFFF)
		desc.SetOffset(0x3FF)
		desc.SetHikeyShortLocation(0xFF)
		desc.SetHikeyFlags(0x3)

		// 清除所有字段
		desc.SetShortLocation(0)
		desc.SetOffset(0)
		desc.SetHikeyShortLocation(0)
		desc.SetHikeyFlags(0)

		assert.Equal(t, uint32(0), desc.GetShortLocation())
		assert.Equal(t, uint32(0), desc.GetOffset())
		assert.Equal(t, uint32(0), desc.GetHikeyShortLocation())
		assert.Equal(t, uint32(0), desc.GetHikeyFlags())
	})
}

func TestPageDescFieldIndependence(t *testing.T) {
	desc := &PageDesc{}

	// 测试字段独立性
	t.Run("field independence", func(t *testing.T) {
		// 设置初始值
		desc.SetFlags(0xF)
		desc.SetType(0xFFFFFFF)

		// 测试修改 flags 不影响 type
		originalType := desc.GetType()
		desc.SetFlags(0x5)
		assert.Equal(t, uint32(0x5), desc.GetFlags())
		assert.Equal(t, originalType, desc.GetType())

		// 测试修改 type 不影响 flags
		originalFlags := desc.GetFlags()
		desc.SetType(0x1234567)
		assert.Equal(t, uint32(0x1234567), desc.GetType())
		assert.Equal(t, originalFlags, desc.GetFlags())
	})
}

func TestBTPageHeaderFieldIndependence(t *testing.T) {
	page := make([]byte, 8192)
	header := (*BTPageHeader)(unsafe.Pointer(&page[0]))

	// 测试字段独立性
	t.Run("field independence", func(t *testing.T) {
		// 设置初始值
		header.SetFlags(0x3F)
		header.SetField1(0x7FF)
		header.SetField2(0x7FFF)

		// 测试修改 flags 不影响其他字段
		originalField1 := header.GetField1()
		originalField2 := header.GetField2()
		header.SetFlags(0x1F)
		assert.Equal(t, uint16(0x1F), header.GetFlags())
		assert.Equal(t, originalField1, header.GetField1())
		assert.Equal(t, originalField2, header.GetField2())

		// 测试修改 field1 不影响其他字段
		originalFlags := header.GetFlags()
		originalField2 = header.GetField2()
		header.SetField1(0x3FF)
		assert.Equal(t, uint16(0x3FF), header.GetField1())
		assert.Equal(t, originalFlags, header.GetFlags())
		assert.Equal(t, originalField2, header.GetField2())

		// 测试修改 field2 不影响其他字段
		originalFlags = header.GetFlags()
		originalField1 = header.GetField1()
		header.SetField2(0x3FFF)
		assert.Equal(t, uint16(0x3FFF), header.GetField2())
		assert.Equal(t, originalFlags, header.GetFlags())
		assert.Equal(t, originalField1, header.GetField1())
	})
}

func TestItemOffsetAndFlags(t *testing.T) {
	tests := []struct {
		name     string
		item     LocationIndex
		offset   uint16
		flags    uint16
		setFlags uint16
	}{
		{
			name:     "zero values",
			item:     0,
			offset:   0,
			flags:    0,
			setFlags: 0,
		},
		{
			name:     "max offset",
			item:     0x3FFF,
			offset:   0x3FFF,
			flags:    0,
			setFlags: 0,
		},
		{
			name:     "with flags",
			item:     0x4000,
			offset:   0,
			flags:    1,
			setFlags: 1,
		},
		{
			name:     "max offset with flags",
			item:     0x7FFF,
			offset:   0x3FFF,
			flags:    1,
			setFlags: 1,
		},
		{
			name:     "set flags to zero",
			item:     0x4000,
			offset:   0,
			flags:    1,
			setFlags: 0,
		},
		{
			name:     "set flags to one",
			item:     0,
			offset:   0,
			flags:    0,
			setFlags: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 测试获取偏移量
			assert.Equal(t, tt.offset, ItemGetOffset(tt.item))

			// 测试获取标志位
			assert.Equal(t, tt.flags, ItemGetFlags(tt.item))

			// 测试设置标志位
			newItem := ItemSetFlags(tt.item, tt.setFlags)
			if tt.setFlags != 0 {
				assert.Equal(t, uint16(1), ItemGetFlags(newItem))
			} else {
				assert.Equal(t, uint16(0), ItemGetFlags(newItem))
			}
			// 验证设置标志位不影响偏移量
			assert.Equal(t, tt.offset, ItemGetOffset(newItem))
		})
	}

	// 测试边界值
	t.Run("boundary values", func(t *testing.T) {
		// 测试最大有效偏移量
		item := LocationIndex(0x3FFF)
		assert.Equal(t, uint16(0x3FFF), ItemGetOffset(item))
		assert.Equal(t, uint16(0), ItemGetFlags(item))

		// 测试带标志位的最大有效偏移量
		item = LocationIndex(0x7FFF)
		assert.Equal(t, uint16(0x3FFF), ItemGetOffset(item))
		assert.Equal(t, uint16(1), ItemGetFlags(item))

		// 测试设置标志位后的边界值
		item = ItemSetFlags(0x3FFF, 1)
		assert.Equal(t, uint16(0x3FFF), ItemGetOffset(item))
		assert.Equal(t, uint16(1), ItemGetFlags(item))

		// 测试清除标志位后的边界值
		item = ItemSetFlags(0x7FFF, 0)
		assert.Equal(t, uint16(0x3FFF), ItemGetOffset(item))
		assert.Equal(t, uint16(0), ItemGetFlags(item))
	})

	// 测试 offset 和 flags 的独立性
	t.Run("offset and flags independence", func(t *testing.T) {
		// 测试设置不同 offset 不影响 flags
		t.Run("set different offsets", func(t *testing.T) {
			// 初始值：offset = 0, flags = 1
			item := LocationIndex(0x4000)
			assert.Equal(t, uint16(0), ItemGetOffset(item))
			assert.Equal(t, uint16(1), ItemGetFlags(item))

			// 设置新的 offset，flags 应该保持不变
			item = (item & ^LocationIndex(0x3FFF)) | 0x1000 // 设置 offset 为 0x1000，保留 flags 位
			assert.Equal(t, uint16(0x1000), ItemGetOffset(item))
			assert.Equal(t, uint16(1), ItemGetFlags(item))

			// 设置另一个 offset，flags 应该保持不变
			item = (item & ^LocationIndex(0x3FFF)) | 0x2000 // 设置 offset 为 0x2000，保留 flags 位
			assert.Equal(t, uint16(0x2000), ItemGetOffset(item))
			assert.Equal(t, uint16(1), ItemGetFlags(item))
		})

		// 测试设置不同 flags 不影响 offset
		t.Run("set different flags", func(t *testing.T) {
			// 初始值：offset = 0x1000, flags = 0
			item := LocationIndex(0x1000)
			assert.Equal(t, uint16(0x1000), ItemGetOffset(item))
			assert.Equal(t, uint16(0), ItemGetFlags(item))

			// 设置 flags 为 1，offset 应该保持不变
			item = ItemSetFlags(item, 1)
			assert.Equal(t, uint16(0x1000), ItemGetOffset(item))
			assert.Equal(t, uint16(1), ItemGetFlags(item))

			// 设置 flags 为 0，offset 应该保持不变
			item = ItemSetFlags(item, 0)
			assert.Equal(t, uint16(0x1000), ItemGetOffset(item))
			assert.Equal(t, uint16(0), ItemGetFlags(item))
		})

		// 测试同时修改 offset 和 flags
		t.Run("modify both offset and flags", func(t *testing.T) {
			// 初始值：offset = 0x1000, flags = 0
			item := LocationIndex(0x1000)
			assert.Equal(t, uint16(0x1000), ItemGetOffset(item))
			assert.Equal(t, uint16(0), ItemGetFlags(item))

			// 修改 offset 和 flags
			item = ItemSetFlags(item, 1)                    // 设置 flags 为 1
			item = (item & ^LocationIndex(0x3FFF)) | 0x2000 // 设置 offset 为 0x2000，保留 flags 位
			assert.Equal(t, uint16(0x2000), ItemGetOffset(item))
			assert.Equal(t, uint16(1), ItemGetFlags(item))

			// 再次修改 offset 和 flags
			item = ItemSetFlags(item, 0)                    // 设置 flags 为 0
			item = (item & ^LocationIndex(0x3FFF)) | 0x3000 // 设置 offset 为 0x3000，保留 flags 位
			assert.Equal(t, uint16(0x3000), ItemGetOffset(item))
			assert.Equal(t, uint16(0), ItemGetFlags(item))
		})

		// 测试边界值的独立性
		t.Run("boundary independence", func(t *testing.T) {
			// 测试最大 offset 和 flags 的组合
			item := LocationIndex(0x3FFF) // 最大 offset
			item = ItemSetFlags(item, 1)  // 设置 flags 为 1
			assert.Equal(t, uint16(0x3FFF), ItemGetOffset(item))
			assert.Equal(t, uint16(1), ItemGetFlags(item))

			// 测试清除 flags 后 offset 保持不变
			item = ItemSetFlags(item, 0)
			assert.Equal(t, uint16(0x3FFF), ItemGetOffset(item))
			assert.Equal(t, uint16(0), ItemGetFlags(item))
		})
	})
}

func TestBTPageHikeysEnd(t *testing.T) {
	// 创建测试用的页面
	page := make([]byte, 8192)
	header := (*BTPageHeader)(unsafe.Pointer(&page[0]))
	desc := &BTDesc{}

	// 测试叶子节点的情况
	t.Run("leaf page", func(t *testing.T) {
		// 设置叶子节点标志
		header.SetFlags(BTREE_FLAG_LEAF)
		// 验证返回值
		assert.Equal(t, LocationIndex(256), BTPageHikeysEnd(desc, unsafe.Pointer(&page[0])))
	})

	// 测试非叶子节点的情况
	t.Run("non-leaf page", func(t *testing.T) {
		// 清除叶子节点标志
		header.SetFlags(0)
		// 验证返回值
		assert.Equal(t, LocationIndex(512), BTPageHikeysEnd(desc, unsafe.Pointer(&page[0])))
	})

	// 测试其他标志位组合的情况
	t.Run("other flag combinations", func(t *testing.T) {
		// 测试带有其他标志的叶子节点
		header.SetFlags(BTREE_FLAG_LEAF | BTREE_FLAG_LEFTMOST | BTREE_FLAG_RIGHTMOST)
		assert.Equal(t, LocationIndex(256), BTPageHikeysEnd(desc, unsafe.Pointer(&page[0])))

		// 测试带有其他标志的非叶子节点
		header.SetFlags(BTREE_FLAG_LEFTMOST | BTREE_FLAG_RIGHTMOST)
		assert.Equal(t, LocationIndex(512), BTPageHikeysEnd(desc, unsafe.Pointer(&page[0])))
	})

	// 测试边界情况
	t.Run("boundary cases", func(t *testing.T) {
		// 测试所有标志位都设置的情况
		header.SetFlags(0x3F) // 所有标志位都设置为1
		assert.Equal(t, LocationIndex(256), BTPageHikeysEnd(desc, unsafe.Pointer(&page[0])))

		// 测试所有标志位都清除的情况
		header.SetFlags(0)
		assert.Equal(t, LocationIndex(512), BTPageHikeysEnd(desc, unsafe.Pointer(&page[0])))
	})
}

func TestLocationGetShort(t *testing.T) {
	// 测试正常情况
	t.Run("normal cases", func(t *testing.T) {
		tests := []struct {
			name     string
			loc      uint32
			expected uint32
		}{
			{
				name:     "zero",
				loc:      0,
				expected: 0,
			},
			{
				name:     "simple value",
				loc:      0x4, // 0b100
				expected: 0x1, // 0b001
			},
			{
				name:     "larger value",
				loc:      0x100, // 0b100000000
				expected: 0x40,  // 0b01000000
			},
			{
				name:     "maximum value",
				loc:      0xFFFFFFFC, // 最大有效值，最后两位为0
				expected: 0x3FFFFFFF, // 右移2位后的值
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				assert.Equal(t, tt.expected, LocationGetShort(tt.loc))
			})
		}
	})

	// 测试边界情况
	t.Run("boundary cases", func(t *testing.T) {
		// 测试最后两位为0的最小值
		assert.Equal(t, uint32(0), LocationGetShort(0))

		// 测试最后两位为0的最大值
		assert.Equal(t, uint32(0x3FFFFFFF), LocationGetShort(0xFFFFFFFC))
	})

	// 测试无效输入
	t.Run("invalid input", func(t *testing.T) {
		// 测试最后一位为1的情况
		assert.Panics(t, func() {
			LocationGetShort(1)
		})

		// 测试最后两位为1的情况
		assert.Panics(t, func() {
			LocationGetShort(3)
		})

		// 测试中间值最后两位为1的情况
		assert.Panics(t, func() {
			LocationGetShort(0x1003)
		})

		// 测试最大值最后两位为1的情况
		assert.Panics(t, func() {
			LocationGetShort(0xFFFFFFFF)
		})
	})
}

func TestBTPageChunk_GetItem(t *testing.T) {
	// 分配一个页面空间
	page := make([]byte, BLOCK_SIZE)

	// 测试正常情况
	t.Run("normal cases", func(t *testing.T) {
		// 在页面中写入测试数据
		testData := []LocationIndex{100, 200, 300, 400}
		chunk := (*BTPageChunk)(unsafe.Pointer(&page[0]))

		// 写入测试数据
		for i, v := range testData {
			*(*LocationIndex)(util.PointerAdd(
				unsafe.Pointer(&chunk.items[0]),
				i*int(LocationIndexSize),
			)) = v
		}

		tests := []struct {
			name     string
			index    int
			expected LocationIndex
		}{
			{"first item", 0, 100},
			{"second item", 1, 200},
			{"third item", 2, 300},
			{"fourth item", 3, 400},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				got := chunk.GetItem(tt.index)
				if got != tt.expected {
					t.Errorf("GetItem(%d) = %v, want %v", tt.index, got, tt.expected)
				}
			})
		}
	})

	// 测试边界情况
	t.Run("boundary cases", func(t *testing.T) {
		chunk := (*BTPageChunk)(unsafe.Pointer(&page[0]))

		// 测试负索引
		t.Run("negative index", func(t *testing.T) {
			defer func() {
				if r := recover(); r == nil {
					t.Error("GetItem with negative index should panic")
				}
			}()
			chunk.GetItem(-1)
		})

		// 测试内存对齐
		t.Run("memory alignment", func(t *testing.T) {
			addr := uintptr(unsafe.Pointer(&chunk.items[0]))
			if addr%unsafe.Alignof(LocationIndex(0)) != 0 {
				t.Error("items array is not properly aligned")
			}
		})
	})
}
