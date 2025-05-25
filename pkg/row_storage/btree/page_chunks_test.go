package btree

import (
	"fmt"
	"testing"
	"unsafe"

	"github.com/daviszhen/plan/pkg/util"
	"github.com/stretchr/testify/assert"
)

func TestInitPageFirstChunk(t *testing.T) {
	// 创建测试用的页面和描述符
	page := make([]byte, 8192)
	header := (*BTPageHeader)(unsafe.Pointer(&page[0]))
	desc := &BTDesc{}

	// 测试叶子节点
	t.Run("leaf page", func(t *testing.T) {
		// 设置叶子节点标志
		header.SetFlags(BTREE_FLAG_LEAF)

		tests := []struct {
			name      string
			hikeySize LocationIndex
		}{
			{
				name:      "small hikey",
				hikeySize: 16, // 已对齐的小hikey
			},
			{
				name:      "medium hikey",
				hikeySize: 128, // 中等大小的hikey
			},
			{
				name:      "large hikey",
				hikeySize: 248, // 接近叶子节点限制的hikey
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				// 初始化第一个chunk
				initPageFirstChunk(desc, unsafe.Pointer(&page[0]), tt.hikeySize)

				// 验证基本字段
				assert.Equal(t, OffsetNumber(1), header.chunksCount)
				assert.Equal(t, OffsetNumber(0), header.itemsCount)

				// 计算预期的hikeysEnd
				expectedHikeysEnd := OffsetNumber(util.AlignValue(BT_PAGE_HEADER_SIZE, 8)) + OffsetNumber(tt.hikeySize)

				// 验证hikeysEnd
				assert.Equal(t, expectedHikeysEnd, header.hikeysEnd)

				// 对于叶子节点，BTPageHikeysEnd返回256
				assert.Equal(t, LocationIndex(256), BTPageHikeysEnd(desc, unsafe.Pointer(&page[0])))

				// 验证dataSize
				if header.hikeysEnd > 256 {
					assert.Equal(t, LocationIndex(header.hikeysEnd), header.dataSize)
				} else {
					assert.Equal(t, LocationIndex(256), header.dataSize)
				}

				// 验证chunk描述符
				assert.Equal(t, uint32(util.AlignValue(BT_PAGE_HEADER_SIZE, 8))>>2, header.chunksDesc[0].GetHikeyShortLocation())
				assert.Equal(t, uint32(header.dataSize)>>2, header.chunksDesc[0].GetShortLocation())
				assert.Equal(t, uint32(0), header.chunksDesc[0].GetOffset())
				assert.Equal(t, uint32(0), header.chunksDesc[0].GetHikeyFlags())
			})
		}
	})

	// 测试非叶子节点
	t.Run("non-leaf page", func(t *testing.T) {
		// 清除叶子节点标志
		header.SetFlags(0)

		tests := []struct {
			name      string
			hikeySize LocationIndex
		}{
			{
				name:      "small hikey",
				hikeySize: 16, // 已对齐的小hikey
			},
			{
				name:      "medium hikey",
				hikeySize: 256, // 中等大小的hikey
			},
			{
				name:      "large hikey",
				hikeySize: 504, // 接近非叶子节点限制的hikey
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				// 初始化第一个chunk
				initPageFirstChunk(desc, unsafe.Pointer(&page[0]), tt.hikeySize)

				// 验证基本字段
				assert.Equal(t, OffsetNumber(1), header.chunksCount)
				assert.Equal(t, OffsetNumber(0), header.itemsCount)

				// 计算预期的hikeysEnd
				expectedHikeysEnd := OffsetNumber(util.AlignValue(BT_PAGE_HEADER_SIZE, 8)) + OffsetNumber(tt.hikeySize)

				// 验证hikeysEnd
				assert.Equal(t, expectedHikeysEnd, header.hikeysEnd)

				// 对于非叶子节点，BTPageHikeysEnd返回512
				assert.Equal(t, LocationIndex(512), BTPageHikeysEnd(desc, unsafe.Pointer(&page[0])))

				// 验证dataSize
				if header.hikeysEnd > 512 {
					assert.Equal(t, LocationIndex(header.hikeysEnd), header.dataSize)
				} else {
					assert.Equal(t, LocationIndex(512), header.dataSize)
				}

				// 验证chunk描述符
				assert.Equal(t, uint32(util.AlignValue(BT_PAGE_HEADER_SIZE, 8))>>2, header.chunksDesc[0].GetHikeyShortLocation())
				assert.Equal(t, uint32(header.dataSize)>>2, header.chunksDesc[0].GetShortLocation())
				assert.Equal(t, uint32(0), header.chunksDesc[0].GetOffset())
				assert.Equal(t, uint32(0), header.chunksDesc[0].GetHikeyFlags())
			})
		}
	})

	// 测试未对齐的hikeySize
	t.Run("unaligned hikey size", func(t *testing.T) {
		unalignedSizes := []LocationIndex{15, 123, 255}
		for _, size := range unalignedSizes {
			t.Run(fmt.Sprintf("size %d", size), func(t *testing.T) {
				assert.Panics(t, func() {
					initPageFirstChunk(desc, unsafe.Pointer(&page[0]), size)
				})
			})
		}
	})

	// 测试 chunksDesc 地址对齐
	t.Run("chunksDesc address alignment", func(t *testing.T) {
		// 计算 chunksDesc 第一个元素的首地址
		chunksDescAddr := uintptr(unsafe.Pointer(&header.chunksDesc[0]))

		// 计算 page 首地址加上 chunkDesc 字段的偏移量
		expectedAddr := uintptr(unsafe.Pointer(&page[0])) + unsafe.Offsetof(header.chunksDesc)

		// 验证两个地址是否相同
		assert.Equal(t, expectedAddr, chunksDescAddr,
			fmt.Sprintf("chunksDesc first element address (0x%x) should be equal to page address plus offsetof chunkDesc (0x%x)",
				chunksDescAddr, expectedAddr))
	})
}

func TestPageChunkFillLocator(t *testing.T) {
	// 创建测试用的页面和描述符
	desc := &BTDesc{}
	InitPages(1)
	defer ClosePages()

	page := GetInMemPage(0)
	header := (*BTPageHeader)(page)

	// 初始化页面
	initNewBtreePage(desc, 0, 0, 0, true)

	// 测试用例
	tests := []struct {
		name         string
		chunkOffset  OffsetNumber
		chunksCount  OffsetNumber
		itemsCount   OffsetNumber
		chunkOffsets []uint32
		shortLocs    []uint32
		dataSize     LocationIndex
		expected     struct {
			itemsCount OffsetNumber
			chunkSize  LocationIndex
		}
	}{
		{
			name:        "middle chunk",
			chunkOffset: 1,
			chunksCount: 3,
			itemsCount:  30,
			chunkOffsets: []uint32{
				0,  // chunk 0 offset
				10, // chunk 1 offset
				20, // chunk 2 offset
			},
			shortLocs: []uint32{
				LocationGetShort(256), // chunk 0 location: 从 256 开始
				LocationGetShort(512), // chunk 1 location: 从 512 开始
				LocationGetShort(768), // chunk 2 location: 从 768 开始
			},
			dataSize: 1024,
			expected: struct {
				itemsCount OffsetNumber
				chunkSize  LocationIndex
			}{
				itemsCount: 10,  // 20 - 10
				chunkSize:  256, // 768 - 512
			},
		},
		{
			name:        "last chunk",
			chunkOffset: 2,
			chunksCount: 3,
			itemsCount:  30,
			chunkOffsets: []uint32{
				0,  // chunk 0 offset
				10, // chunk 1 offset
				20, // chunk 2 offset
			},
			shortLocs: []uint32{
				LocationGetShort(256), // chunk 0 location: 从 256 开始
				LocationGetShort(512), // chunk 1 location: 从 512 开始
				LocationGetShort(768), // chunk 2 location: 从 768 开始
			},
			dataSize: 1024,
			expected: struct {
				itemsCount OffsetNumber
				chunkSize  LocationIndex
			}{
				itemsCount: 10,  // 30 - 20
				chunkSize:  256, // 1024 - 768
			},
		},
		{
			name:        "last chunk boundary",
			chunkOffset: 0,
			chunksCount: 1,
			itemsCount:  1,
			chunkOffsets: []uint32{
				0, // chunk 0 offset
			},
			shortLocs: []uint32{
				LocationGetShort(256), // chunk 0 location: 从 256 开始
			},
			dataSize: 8192, // 刚好是页面大小
			expected: struct {
				itemsCount OffsetNumber
				chunkSize  LocationIndex
			}{
				itemsCount: 1,    // 只有一个 item
				chunkSize:  7936, // 8192 - 256
			},
		},
		{
			name:        "multiple items with correct alignment",
			chunkOffset: 0,
			chunksCount: 1,
			itemsCount:  3,
			chunkOffsets: []uint32{
				0, // chunk 0 offset
			},
			shortLocs: []uint32{
				LocationGetShort(256), // chunk 0 location: 从 256 开始
			},
			dataSize: 512, // 每个 item 占用 256 字节
			expected: struct {
				itemsCount OffsetNumber
				chunkSize  LocationIndex
			}{
				itemsCount: 3,   // 3 个 items
				chunkSize:  256, // 512 - 256
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 设置页面头信息
			header.chunksCount = tt.chunksCount
			header.itemsCount = tt.itemsCount
			header.dataSize = tt.dataSize

			// 设置chunk描述符
			for i := 0; i < len(tt.chunkOffsets); i++ {
				chunkDesc := header.GetChunkDesc(i)
				chunkDesc.SetOffset(tt.chunkOffsets[i])
				chunkDesc.SetShortLocation(tt.shortLocs[i])
			}

			// 填充 item 区域
			chunkDesc := header.GetChunkDesc(int(tt.chunkOffset))
			chunkStart := ShortGetLocation(chunkDesc.GetShortLocation())
			chunkEnd := chunkStart + uint32(tt.expected.chunkSize)

			// 用标记值填充 item 区域
			itemMarker := byte(0xAA)
			for i := chunkStart; i < chunkEnd; i++ {
				*(*byte)(util.PointerAdd(page, int(i))) = itemMarker
			}

			// 创建定位器
			locator := &BTPageItemLocator{}

			// 填充定位器
			pageChunkFillLocator(page, tt.chunkOffset, locator)

			// 验证结果
			assert.Equal(t, tt.chunkOffset, locator.chunkOffset)
			assert.Equal(t, tt.expected.itemsCount, locator.chunkItemsCount)
			assert.Equal(t, tt.expected.chunkSize, locator.chunkSize)
			assert.Equal(t, OffsetNumber(0), locator.itemOffset)

			// 验证chunk指针
			expectedChunkAddr := uintptr(page) +
				uintptr(ShortGetLocation(chunkDesc.GetShortLocation()))
			actualChunkAddr := uintptr(unsafe.Pointer(locator.chunk))
			assert.Equal(t, expectedChunkAddr, actualChunkAddr,
				fmt.Sprintf("chunk address mismatch: expected 0x%x, got 0x%x",
					expectedChunkAddr, actualChunkAddr))

			// 验证 item 区域与 chunkDesc 区域没有重叠
			chunkDescStart := uintptr(unsafe.Pointer(&header.chunksDesc[0]))
			chunkDescEnd := chunkDescStart + uintptr(tt.chunksCount)*unsafe.Sizeof(BTPageChunkDesc{})
			itemStart := uintptr(page) + uintptr(chunkStart)
			itemEnd := uintptr(page) + uintptr(chunkEnd)

			// 检查区域是否重叠
			assert.False(t, itemStart < chunkDescEnd && chunkDescStart < itemEnd,
				fmt.Sprintf("item region [0x%x, 0x%x] overlaps with chunkDesc region [0x%x, 0x%x]",
					itemStart, itemEnd, chunkDescStart, chunkDescEnd))

			// 验证 item 区域被正确填充
			for i := chunkStart; i < chunkEnd; i++ {
				value := *(*byte)(util.PointerAdd(page, int(i)))
				assert.Equal(t, itemMarker, value,
					fmt.Sprintf("item region at offset %d should be filled with 0x%x, got 0x%x",
						i, itemMarker, value))
			}
		})
	}
}

func TestPageItemFillLocator(t *testing.T) {
	// 创建测试用的页面和描述符
	desc := &BTDesc{}
	InitPages(1)
	defer ClosePages()

	page := GetInMemPage(0)
	header := (*BTPageHeader)(page)

	// 初始化页面
	initNewBtreePage(desc, 0, 0, 0, true)

	// 测试用例
	tests := []struct {
		name         string
		itemOffset   OffsetNumber
		chunksCount  OffsetNumber
		itemsCount   OffsetNumber
		chunkOffsets []uint32
		shortLocs    []uint32
		dataSize     LocationIndex
		expected     struct {
			chunkOffset OffsetNumber
			itemOffset  OffsetNumber
		}
	}{
		{
			name:        "first chunk first item",
			itemOffset:  0,
			chunksCount: 3,
			itemsCount:  30,
			chunkOffsets: []uint32{
				0,  // chunk 0 offset
				10, // chunk 1 offset
				20, // chunk 2 offset
			},
			shortLocs: []uint32{
				LocationGetShort(256), // chunk 0 location: 从 256 开始
				LocationGetShort(512), // chunk 1 location: 从 512 开始
				LocationGetShort(768), // chunk 2 location: 从 768 开始
			},
			dataSize: 1024,
			expected: struct {
				chunkOffset OffsetNumber
				itemOffset  OffsetNumber
			}{
				chunkOffset: 0,
				itemOffset:  0,
			},
		},
		{
			name:        "first chunk last item",
			itemOffset:  9,
			chunksCount: 3,
			itemsCount:  30,
			chunkOffsets: []uint32{
				0,  // chunk 0 offset
				10, // chunk 1 offset
				20, // chunk 2 offset
			},
			shortLocs: []uint32{
				LocationGetShort(256), // chunk 0 location: 从 256 开始
				LocationGetShort(512), // chunk 1 location: 从 512 开始
				LocationGetShort(768), // chunk 2 location: 从 768 开始
			},
			dataSize: 1024,
			expected: struct {
				chunkOffset OffsetNumber
				itemOffset  OffsetNumber
			}{
				chunkOffset: 0,
				itemOffset:  9,
			},
		},
		{
			name:        "middle chunk first item",
			itemOffset:  10,
			chunksCount: 3,
			itemsCount:  30,
			chunkOffsets: []uint32{
				0,  // chunk 0 offset
				10, // chunk 1 offset
				20, // chunk 2 offset
			},
			shortLocs: []uint32{
				LocationGetShort(256), // chunk 0 location: 从 256 开始
				LocationGetShort(512), // chunk 1 location: 从 512 开始
				LocationGetShort(768), // chunk 2 location: 从 768 开始
			},
			dataSize: 1024,
			expected: struct {
				chunkOffset OffsetNumber
				itemOffset  OffsetNumber
			}{
				chunkOffset: 1,
				itemOffset:  0,
			},
		},
		{
			name:        "last chunk last item",
			itemOffset:  29,
			chunksCount: 3,
			itemsCount:  30,
			chunkOffsets: []uint32{
				0,  // chunk 0 offset
				10, // chunk 1 offset
				20, // chunk 2 offset
			},
			shortLocs: []uint32{
				LocationGetShort(256), // chunk 0 location: 从 256 开始
				LocationGetShort(512), // chunk 1 location: 从 512 开始
				LocationGetShort(768), // chunk 2 location: 从 768 开始
			},
			dataSize: 1024,
			expected: struct {
				chunkOffset OffsetNumber
				itemOffset  OffsetNumber
			}{
				chunkOffset: 2,
				itemOffset:  9,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 设置页面头信息
			header.chunksCount = tt.chunksCount
			header.itemsCount = tt.itemsCount
			header.dataSize = tt.dataSize

			// 设置chunk描述符
			for i := 0; i < len(tt.chunkOffsets); i++ {
				chunkDesc := header.GetChunkDesc(i)
				chunkDesc.SetOffset(tt.chunkOffsets[i])
				chunkDesc.SetShortLocation(tt.shortLocs[i])
			}

			// 创建定位器
			locator := &BTPageItemLocator{}

			// 填充定位器
			pageItemFillLocator(page, tt.itemOffset, locator)

			// 验证结果
			assert.Equal(t, tt.expected.chunkOffset, locator.chunkOffset,
				fmt.Sprintf("chunk offset mismatch: expected %d, got %d",
					tt.expected.chunkOffset, locator.chunkOffset))
			assert.Equal(t, tt.expected.itemOffset, locator.itemOffset,
				fmt.Sprintf("item offset mismatch: expected %d, got %d",
					tt.expected.itemOffset, locator.itemOffset))

			// 验证 chunk 信息
			chunkDesc := header.GetChunkDesc(int(locator.chunkOffset))
			expectedChunkAddr := uintptr(page) +
				uintptr(ShortGetLocation(chunkDesc.GetShortLocation()))
			actualChunkAddr := uintptr(unsafe.Pointer(locator.chunk))
			assert.Equal(t, expectedChunkAddr, actualChunkAddr,
				fmt.Sprintf("chunk address mismatch: expected 0x%x, got 0x%x",
					expectedChunkAddr, actualChunkAddr))
		})
	}
}

func TestPageItemFillLocatorBackwards(t *testing.T) {
	// 创建测试用的页面和描述符
	desc := &BTDesc{}
	InitPages(1)
	defer ClosePages()

	page := GetInMemPage(0)
	header := (*BTPageHeader)(page)

	// 初始化页面
	initNewBtreePage(desc, 0, 0, 0, true)

	// 测试用例
	tests := []struct {
		name         string
		itemOffset   OffsetNumber
		chunksCount  OffsetNumber
		itemsCount   OffsetNumber
		chunkOffsets []uint32
		shortLocs    []uint32
		dataSize     LocationIndex
		expected     struct {
			chunkOffset OffsetNumber
			itemOffset  OffsetNumber
		}
	}{
		{
			name:        "last chunk last item",
			itemOffset:  29,
			chunksCount: 3,
			itemsCount:  30,
			chunkOffsets: []uint32{
				0,  // chunk 0 offset
				10, // chunk 1 offset
				20, // chunk 2 offset
			},
			shortLocs: []uint32{
				LocationGetShort(256), // chunk 0 location: 从 256 开始
				LocationGetShort(512), // chunk 1 location: 从 512 开始
				LocationGetShort(768), // chunk 2 location: 从 768 开始
			},
			dataSize: 1024,
			expected: struct {
				chunkOffset OffsetNumber
				itemOffset  OffsetNumber
			}{
				chunkOffset: 2,
				itemOffset:  9,
			},
		},
		{
			name:        "middle chunk first item",
			itemOffset:  10,
			chunksCount: 3,
			itemsCount:  30,
			chunkOffsets: []uint32{
				0,  // chunk 0 offset
				10, // chunk 1 offset
				20, // chunk 2 offset
			},
			shortLocs: []uint32{
				LocationGetShort(256), // chunk 0 location: 从 256 开始
				LocationGetShort(512), // chunk 1 location: 从 512 开始
				LocationGetShort(768), // chunk 2 location: 从 768 开始
			},
			dataSize: 1024,
			expected: struct {
				chunkOffset OffsetNumber
				itemOffset  OffsetNumber
			}{
				chunkOffset: 1,
				itemOffset:  0,
			},
		},
		{
			name:        "first chunk first item",
			itemOffset:  0,
			chunksCount: 3,
			itemsCount:  30,
			chunkOffsets: []uint32{
				0,  // chunk 0 offset
				10, // chunk 1 offset
				20, // chunk 2 offset
			},
			shortLocs: []uint32{
				LocationGetShort(256), // chunk 0 location: 从 256 开始
				LocationGetShort(512), // chunk 1 location: 从 512 开始
				LocationGetShort(768), // chunk 2 location: 从 768 开始
			},
			dataSize: 1024,
			expected: struct {
				chunkOffset OffsetNumber
				itemOffset  OffsetNumber
			}{
				chunkOffset: 0,
				itemOffset:  0,
			},
		},
		{
			name:        "single chunk last item",
			itemOffset:  9,
			chunksCount: 1,
			itemsCount:  10,
			chunkOffsets: []uint32{
				0, // chunk 0 offset
			},
			shortLocs: []uint32{
				LocationGetShort(256), // chunk 0 location: 从 256 开始
			},
			dataSize: 512,
			expected: struct {
				chunkOffset OffsetNumber
				itemOffset  OffsetNumber
			}{
				chunkOffset: 0,
				itemOffset:  9,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 设置页面头信息
			header.chunksCount = tt.chunksCount
			header.itemsCount = tt.itemsCount
			header.dataSize = tt.dataSize

			// 设置chunk描述符
			for i := 0; i < len(tt.chunkOffsets); i++ {
				chunkDesc := header.GetChunkDesc(i)
				chunkDesc.SetOffset(tt.chunkOffsets[i])
				chunkDesc.SetShortLocation(tt.shortLocs[i])
			}

			// 创建定位器
			locator := &BTPageItemLocator{}

			// 填充定位器
			pageItemFillLocatorBackwards(page, tt.itemOffset, locator)

			// 验证结果
			assert.Equal(t, tt.expected.chunkOffset, locator.chunkOffset,
				fmt.Sprintf("chunk offset mismatch: expected %d, got %d",
					tt.expected.chunkOffset, locator.chunkOffset))
			assert.Equal(t, tt.expected.itemOffset, locator.itemOffset,
				fmt.Sprintf("item offset mismatch: expected %d, got %d",
					tt.expected.itemOffset, locator.itemOffset))

			// 验证 chunk 信息
			chunkDesc := header.GetChunkDesc(int(locator.chunkOffset))
			expectedChunkAddr := uintptr(page) +
				uintptr(ShortGetLocation(chunkDesc.GetShortLocation()))
			actualChunkAddr := uintptr(unsafe.Pointer(locator.chunk))
			assert.Equal(t, expectedChunkAddr, actualChunkAddr,
				fmt.Sprintf("chunk address mismatch: expected 0x%x, got 0x%x",
					expectedChunkAddr, actualChunkAddr))
		})
	}
}

func TestPageLocatorNextChunk(t *testing.T) {
	// 创建测试用的页面和描述符
	desc := &BTDesc{}
	InitPages(1)
	defer ClosePages()

	// 测试移动到下一个chunk的情况
	t.Run("move to next chunk when item offset equals chunk items count", func(t *testing.T) {
		// 初始化页面
		page := GetInMemPage(0)
		header := (*BTPageHeader)(page)
		initNewBtreePage(desc, 0, 0, 0, true)

		// 设置页面头信息
		header.chunksCount = 3
		header.itemsCount = 30
		header.dataSize = 1024

		// 设置chunk描述符
		chunkOffsets := []uint32{
			0,  // chunk 0 offset
			10, // chunk 1 offset
			20, // chunk 2 offset
		}
		shortLocs := []uint32{
			LocationGetShort(256), // chunk 0 location: 从 256 开始
			LocationGetShort(512), // chunk 1 location: 从 512 开始
			LocationGetShort(768), // chunk 2 location: 从 768 开始
		}
		for i := 0; i < len(chunkOffsets); i++ {
			chunkDesc := header.GetChunkDesc(i)
			chunkDesc.SetOffset(chunkOffsets[i])
			chunkDesc.SetShortLocation(shortLocs[i])
		}

		// 创建并初始化定位器
		locator := &BTPageItemLocator{
			chunkOffset:     0,
			itemOffset:      10, // 等于第一个chunk的items数量
			chunkItemsCount: 10,
		}

		// 填充初始chunk信息
		pageChunkFillLocator(page, 0, locator)

		// 验证填充后的itemOffset
		assert.Equal(t, OffsetNumber(0), locator.itemOffset,
			"itemOffset should be 0 after pageChunkFillLocator")

		// 设置itemOffset为初始值，这样pageLocatorNextChunk才能正确判断是否移动到下一个chunk
		locator.itemOffset = 10

		// 尝试移动到下一个chunk
		success := pageLocatorNextChunk(page, locator)

		// 验证结果
		assert.True(t, success, "should move to next chunk")
		assert.Equal(t, OffsetNumber(1), locator.chunkOffset, "should move to next chunk")
		assert.Equal(t, OffsetNumber(0), locator.itemOffset, "itemOffset should be reset to 0")
		assert.Equal(t, OffsetNumber(10), locator.chunkItemsCount, "chunkItemsCount should remain unchanged")

		// 验证 chunk 信息
		chunkDesc := header.GetChunkDesc(1)
		expectedChunkAddr := uintptr(page) +
			uintptr(ShortGetLocation(chunkDesc.GetShortLocation()))
		actualChunkAddr := uintptr(unsafe.Pointer(locator.chunk))
		assert.Equal(t, expectedChunkAddr, actualChunkAddr,
			"chunk address mismatch")
	})

	// 测试移动到下一个chunk的情况（itemOffset大于chunkItemsCount）
	t.Run("move to next chunk when item offset greater than chunk items count", func(t *testing.T) {
		// 初始化页面
		page := GetInMemPage(0)
		header := (*BTPageHeader)(page)
		initNewBtreePage(desc, 0, 0, 0, true)

		// 设置页面头信息
		header.chunksCount = 3
		header.itemsCount = 30
		header.dataSize = 1024

		// 设置chunk描述符
		chunkOffsets := []uint32{
			0,  // chunk 0 offset
			10, // chunk 1 offset
			20, // chunk 2 offset
		}
		shortLocs := []uint32{
			LocationGetShort(256), // chunk 0 location: 从 256 开始
			LocationGetShort(512), // chunk 1 location: 从 512 开始
			LocationGetShort(768), // chunk 2 location: 从 768 开始
		}
		for i := 0; i < len(chunkOffsets); i++ {
			chunkDesc := header.GetChunkDesc(i)
			chunkDesc.SetOffset(chunkOffsets[i])
			chunkDesc.SetShortLocation(shortLocs[i])
		}

		// 创建并初始化定位器
		locator := &BTPageItemLocator{
			chunkOffset:     0,
			itemOffset:      15, // 大于第一个chunk的items数量
			chunkItemsCount: 10,
		}

		// 填充初始chunk信息
		pageChunkFillLocator(page, 0, locator)

		// 验证填充后的itemOffset
		assert.Equal(t, OffsetNumber(0), locator.itemOffset,
			"itemOffset should be 0 after pageChunkFillLocator")

		// 设置itemOffset为初始值，这样pageLocatorNextChunk才能正确判断是否移动到下一个chunk
		locator.itemOffset = 15

		// 尝试移动到下一个chunk
		success := pageLocatorNextChunk(page, locator)

		// 验证结果
		assert.True(t, success, "should move to next chunk")
		assert.Equal(t, OffsetNumber(1), locator.chunkOffset, "should move to next chunk")
		assert.Equal(t, OffsetNumber(0), locator.itemOffset, "itemOffset should be reset to 0")
		assert.Equal(t, OffsetNumber(10), locator.chunkItemsCount, "chunkItemsCount should remain unchanged")

		// 验证 chunk 信息
		chunkDesc := header.GetChunkDesc(1)
		expectedChunkAddr := uintptr(page) +
			uintptr(ShortGetLocation(chunkDesc.GetShortLocation()))
		actualChunkAddr := uintptr(unsafe.Pointer(locator.chunk))
		assert.Equal(t, expectedChunkAddr, actualChunkAddr,
			"chunk address mismatch")
	})

	// 测试在最后一个chunk时的情况
	t.Run("no next chunk when item offset equals chunk items count", func(t *testing.T) {
		// 初始化页面
		page := GetInMemPage(0)
		header := (*BTPageHeader)(page)
		initNewBtreePage(desc, 0, 0, 0, true)

		// 设置页面头信息
		header.chunksCount = 3
		header.itemsCount = 30
		header.dataSize = 1024

		// 设置chunk描述符
		chunkOffsets := []uint32{
			0,  // chunk 0 offset
			10, // chunk 1 offset
			20, // chunk 2 offset
		}
		shortLocs := []uint32{
			LocationGetShort(256), // chunk 0 location: 从 256 开始
			LocationGetShort(512), // chunk 1 location: 从 512 开始
			LocationGetShort(768), // chunk 2 location: 从 768 开始
		}
		for i := 0; i < len(chunkOffsets); i++ {
			chunkDesc := header.GetChunkDesc(i)
			chunkDesc.SetOffset(chunkOffsets[i])
			chunkDesc.SetShortLocation(shortLocs[i])
		}

		// 创建并初始化定位器
		locator := &BTPageItemLocator{
			chunkOffset:     2,  // 最后一个chunk
			itemOffset:      10, // 等于最后一个chunk的items数量
			chunkItemsCount: 10,
		}

		// 填充初始chunk信息
		pageChunkFillLocator(page, 2, locator)

		// 验证填充后的itemOffset
		assert.Equal(t, OffsetNumber(0), locator.itemOffset,
			"itemOffset should be 0 after pageChunkFillLocator")

		// 设置itemOffset为初始值，这样pageLocatorNextChunk才能正确判断是否移动到下一个chunk
		locator.itemOffset = 10

		// 尝试移动到下一个chunk
		success := pageLocatorNextChunk(page, locator)

		// 验证结果
		assert.False(t, success, "should not move to next chunk")
		assert.Equal(t, OffsetNumber(2), locator.chunkOffset, "should stay in current chunk")
		assert.Equal(t, OffsetNumber(10), locator.itemOffset, "itemOffset should remain unchanged")
		assert.Equal(t, OffsetNumber(10), locator.chunkItemsCount, "chunkItemsCount should remain unchanged")
	})

	// 测试保持在当前chunk的情况
	t.Run("stay in current chunk when item offset less than chunk items count", func(t *testing.T) {
		// 初始化页面
		page := GetInMemPage(0)
		header := (*BTPageHeader)(page)
		initNewBtreePage(desc, 0, 0, 0, true)

		// 设置页面头信息
		header.chunksCount = 3
		header.itemsCount = 30
		header.dataSize = 1024

		// 设置chunk描述符
		chunkOffsets := []uint32{
			0,  // chunk 0 offset
			10, // chunk 1 offset
			20, // chunk 2 offset
		}
		shortLocs := []uint32{
			LocationGetShort(256), // chunk 0 location: 从 256 开始
			LocationGetShort(512), // chunk 1 location: 从 512 开始
			LocationGetShort(768), // chunk 2 location: 从 768 开始
		}
		for i := 0; i < len(chunkOffsets); i++ {
			chunkDesc := header.GetChunkDesc(i)
			chunkDesc.SetOffset(chunkOffsets[i])
			chunkDesc.SetShortLocation(shortLocs[i])
		}

		// 创建并初始化定位器
		locator := &BTPageItemLocator{
			chunkOffset:     0,
			itemOffset:      5, // 小于第一个chunk的items数量
			chunkItemsCount: 10,
		}

		// 填充初始chunk信息
		pageChunkFillLocator(page, 0, locator)

		// 验证填充后的itemOffset
		assert.Equal(t, OffsetNumber(0), locator.itemOffset,
			"itemOffset should be 0 after pageChunkFillLocator")

		// 设置itemOffset为初始值，这样pageLocatorNextChunk才能正确判断是否移动到下一个chunk
		locator.itemOffset = 5

		// 尝试移动到下一个chunk
		success := pageLocatorNextChunk(page, locator)

		// 验证结果
		assert.True(t, success, "should stay in current chunk")
		assert.Equal(t, OffsetNumber(0), locator.chunkOffset, "should stay in current chunk")
		assert.Equal(t, OffsetNumber(5), locator.itemOffset, "itemOffset should be unchanged")
		assert.Equal(t, OffsetNumber(10), locator.chunkItemsCount, "chunkItemsCount should remain unchanged")

		// 验证 chunk 信息
		chunkDesc := header.GetChunkDesc(0)
		expectedChunkAddr := uintptr(page) +
			uintptr(ShortGetLocation(chunkDesc.GetShortLocation()))
		actualChunkAddr := uintptr(unsafe.Pointer(locator.chunk))
		assert.Equal(t, expectedChunkAddr, actualChunkAddr,
			"chunk address mismatch")
	})

	// 测试 chunkOffset 大于 chunksCount 的情况
	t.Run("chunk offset greater than chunks count", func(t *testing.T) {
		// 初始化页面
		page := GetInMemPage(0)
		header := (*BTPageHeader)(page)
		initNewBtreePage(desc, 0, 0, 0, true)

		// 设置页面头信息
		header.chunksCount = 3
		header.itemsCount = 30
		header.dataSize = 1024

		// 设置chunk描述符
		chunkOffsets := []uint32{
			0,  // chunk 0 offset
			10, // chunk 1 offset
			20, // chunk 2 offset
		}
		shortLocs := []uint32{
			LocationGetShort(256), // chunk 0 location: 从 256 开始
			LocationGetShort(512), // chunk 1 location: 从 512 开始
			LocationGetShort(768), // chunk 2 location: 从 768 开始
		}
		for i := 0; i < len(chunkOffsets); i++ {
			chunkDesc := header.GetChunkDesc(i)
			chunkDesc.SetOffset(chunkOffsets[i])
			chunkDesc.SetShortLocation(shortLocs[i])
		}

		// 创建并初始化定位器
		locator := &BTPageItemLocator{
			chunkOffset:     2, // 大于 chunksCount (3)
			itemOffset:      10,
			chunkItemsCount: 10,
		}

		// 填充初始chunk信息
		pageChunkFillLocator(page, 2, locator)

		// 验证填充后的itemOffset
		assert.Equal(t, OffsetNumber(0), locator.itemOffset,
			"itemOffset should be 0 after pageChunkFillLocator")

		// 设置itemOffset为初始值，这样pageLocatorNextChunk才能正确判断是否移动到下一个chunk
		locator.itemOffset = 10

		// 尝试移动到下一个chunk
		success := pageLocatorNextChunk(page, locator)

		// 验证结果
		assert.False(t, success, "should not move to next chunk when chunk offset is invalid")
		assert.Equal(t, OffsetNumber(2), locator.chunkOffset, "should stay in current chunk")
		assert.Equal(t, OffsetNumber(10), locator.itemOffset, "itemOffset should remain unchanged")
		assert.Equal(t, OffsetNumber(10), locator.chunkItemsCount, "chunkItemsCount should remain unchanged")
	})
}

func TestPageLocatorPrevChunk(t *testing.T) {
	// 创建测试用的页面和描述符
	desc := &BTDesc{}
	InitPages(1)
	defer ClosePages()

	// 测试从中间chunk移动到前一个chunk的情况
	t.Run("move to previous chunk from middle chunk", func(t *testing.T) {
		// 初始化页面
		page := GetInMemPage(0)
		header := (*BTPageHeader)(page)
		initNewBtreePage(desc, 0, 0, 0, true)

		// 设置页面头信息
		header.chunksCount = 3
		header.itemsCount = 30
		header.dataSize = 1024

		// 设置chunk描述符
		chunkOffsets := []uint32{
			0,  // chunk 0 offset
			10, // chunk 1 offset
			20, // chunk 2 offset
		}
		shortLocs := []uint32{
			LocationGetShort(256), // chunk 0 location: 从 256 开始
			LocationGetShort(512), // chunk 1 location: 从 512 开始
			LocationGetShort(768), // chunk 2 location: 从 768 开始
		}
		for i := 0; i < len(chunkOffsets); i++ {
			chunkDesc := header.GetChunkDesc(i)
			chunkDesc.SetOffset(chunkOffsets[i])
			chunkDesc.SetShortLocation(shortLocs[i])
		}

		// 创建并初始化定位器
		locator := &BTPageItemLocator{
			chunkOffset:     2, // 从最后一个chunk开始
			itemOffset:      5,
			chunkItemsCount: 10,
		}

		// 填充初始chunk信息
		pageChunkFillLocator(page, 2, locator)

		// 尝试移动到前一个chunk
		success := pageLocatorPrevChunk(page, locator)

		// 验证结果
		assert.True(t, success, "should move to previous chunk")
		assert.Equal(t, OffsetNumber(1), locator.chunkOffset, "should move to previous chunk")
		assert.Equal(t, OffsetNumber(9), locator.itemOffset, "itemOffset should be set to last item of previous chunk")
		assert.Equal(t, OffsetNumber(10), locator.chunkItemsCount, "chunkItemsCount should be set to previous chunk's items count")

		// 验证 chunk 信息
		chunkDesc := header.GetChunkDesc(1)
		expectedChunkAddr := uintptr(page) +
			uintptr(ShortGetLocation(chunkDesc.GetShortLocation()))
		actualChunkAddr := uintptr(unsafe.Pointer(locator.chunk))
		assert.Equal(t, expectedChunkAddr, actualChunkAddr,
			"chunk address mismatch")
	})

	// 测试从第一个chunk移动到前一个chunk的情况
	t.Run("no previous chunk when at first chunk", func(t *testing.T) {
		// 初始化页面
		page := GetInMemPage(0)
		header := (*BTPageHeader)(page)
		initNewBtreePage(desc, 0, 0, 0, true)

		// 设置页面头信息
		header.chunksCount = 3
		header.itemsCount = 30
		header.dataSize = 1024

		// 设置chunk描述符
		chunkOffsets := []uint32{
			0,  // chunk 0 offset
			10, // chunk 1 offset
			20, // chunk 2 offset
		}
		shortLocs := []uint32{
			LocationGetShort(256), // chunk 0 location: 从 256 开始
			LocationGetShort(512), // chunk 1 location: 从 512 开始
			LocationGetShort(768), // chunk 2 location: 从 768 开始
		}
		for i := 0; i < len(chunkOffsets); i++ {
			chunkDesc := header.GetChunkDesc(i)
			chunkDesc.SetOffset(chunkOffsets[i])
			chunkDesc.SetShortLocation(shortLocs[i])
		}

		// 创建并初始化定位器
		locator := &BTPageItemLocator{
			chunkOffset:     0, // 从第一个chunk开始
			itemOffset:      5,
			chunkItemsCount: 10,
		}

		// 填充初始chunk信息
		pageChunkFillLocator(page, 0, locator)

		// 尝试移动到前一个chunk
		success := pageLocatorPrevChunk(page, locator)

		// 验证结果
		assert.False(t, success, "should not move to previous chunk")
		assert.Equal(t, OffsetNumber(0), locator.chunkOffset, "should stay at first chunk")
		assert.Nil(t, locator.chunk, "chunk should be nil")
	})

	// 测试移动到空chunk的情况
	t.Run("skip empty chunk when moving to previous chunk", func(t *testing.T) {
		// 初始化页面
		page := GetInMemPage(0)
		header := (*BTPageHeader)(page)
		initNewBtreePage(desc, 0, 0, 0, true)

		// 设置页面头信息
		header.chunksCount = 3
		header.itemsCount = 30
		header.dataSize = 1024

		// 设置chunk描述符
		chunkOffsets := []uint32{
			0,  // chunk 0 offset
			10, // chunk 1 offset (空chunk)
			10, // chunk 2 offset
		}
		shortLocs := []uint32{
			LocationGetShort(256), // chunk 0 location: 从 256 开始
			LocationGetShort(512), // chunk 1 location: 从 512 开始
			LocationGetShort(768), // chunk 2 location: 从 768 开始
		}
		for i := 0; i < len(chunkOffsets); i++ {
			chunkDesc := header.GetChunkDesc(i)
			chunkDesc.SetOffset(chunkOffsets[i])
			chunkDesc.SetShortLocation(shortLocs[i])
		}

		// 创建并初始化定位器
		locator := &BTPageItemLocator{
			chunkOffset:     2, // 从最后一个chunk开始
			itemOffset:      5,
			chunkItemsCount: 10,
		}

		// 填充初始chunk信息
		pageChunkFillLocator(page, 2, locator)

		// 尝试移动到前一个chunk
		success := pageLocatorPrevChunk(page, locator)

		// 验证结果
		assert.True(t, success, "should move to previous non-empty chunk")
		assert.Equal(t, OffsetNumber(0), locator.chunkOffset, "should skip empty chunk and move to first chunk")
		assert.Equal(t, OffsetNumber(9), locator.itemOffset, "itemOffset should be set to last item of first chunk")
		assert.Equal(t, OffsetNumber(10), locator.chunkItemsCount, "chunkItemsCount should be set to first chunk's items count")

		// 验证 chunk 信息
		chunkDesc := header.GetChunkDesc(0)
		expectedChunkAddr := uintptr(page) +
			uintptr(ShortGetLocation(chunkDesc.GetShortLocation()))
		actualChunkAddr := uintptr(unsafe.Pointer(locator.chunk))
		assert.Equal(t, expectedChunkAddr, actualChunkAddr,
			"chunk address mismatch")
	})
}

func TestPageLocatorFitsNewItem(t *testing.T) {
	// 创建测试用的页面和描述符
	desc := &BTDesc{}
	InitPages(1)
	defer ClosePages()

	// 测试有足够空间的情况
	t.Run("has enough space for new item", func(t *testing.T) {
		// 初始化页面
		page := GetInMemPage(0)
		header := (*BTPageHeader)(page)
		initNewBtreePage(desc, 0, 0, 0, true)

		// 设置页面头信息
		header.chunksCount = 1
		header.itemsCount = 5
		header.dataSize = 256 // 设置较小的dataSize，确保有足够空间

		// 设置chunk描述符
		chunkOffsets := []uint32{
			0, // chunk 0 offset
		}
		shortLocs := []uint32{
			LocationGetShort(256), // chunk 0 location: 从 256 开始
		}
		for i := 0; i < len(chunkOffsets); i++ {
			chunkDesc := header.GetChunkDesc(i)
			chunkDesc.SetOffset(chunkOffsets[i])
			chunkDesc.SetShortLocation(shortLocs[i])
		}

		// 创建并初始化定位器
		locator := &BTPageItemLocator{
			chunkOffset:     0,
			itemOffset:      5,
			chunkItemsCount: 5,
		}

		// 填充初始chunk信息
		pageChunkFillLocator(page, 0, locator)

		// 测试小尺寸的item
		fits := pageLocatorFitsNewItem(page, locator, 16)
		assert.True(t, fits, "should have enough space for small item")

		// 测试中等尺寸的item
		fits = pageLocatorFitsNewItem(page, locator, 64)
		assert.True(t, fits, "should have enough space for medium item")
	})

	// 测试空间不足的情况
	t.Run("not enough space for new item", func(t *testing.T) {
		// 初始化页面
		page := GetInMemPage(0)
		header := (*BTPageHeader)(page)
		initNewBtreePage(desc, 0, 0, 0, true)

		// 设置页面头信息
		header.chunksCount = 1
		header.itemsCount = 5
		header.dataSize = 8192 // 设置较大的dataSize，模拟空间不足

		// 设置chunk描述符
		chunkOffsets := []uint32{
			0, // chunk 0 offset
		}
		shortLocs := []uint32{
			LocationGetShort(256), // chunk 0 location: 从 256 开始
		}
		for i := 0; i < len(chunkOffsets); i++ {
			chunkDesc := header.GetChunkDesc(i)
			chunkDesc.SetOffset(chunkOffsets[i])
			chunkDesc.SetShortLocation(shortLocs[i])
		}

		// 创建并初始化定位器
		locator := &BTPageItemLocator{
			chunkOffset:     0,
			itemOffset:      5,
			chunkItemsCount: 5,
		}

		// 填充初始chunk信息
		pageChunkFillLocator(page, 0, locator)

		// 测试大尺寸的item
		fits := pageLocatorFitsNewItem(page, locator, 4096)
		assert.False(t, fits, "should not have enough space for large item")
	})

	// 测试边界情况
	t.Run("boundary conditions", func(t *testing.T) {
		// 初始化页面
		page := GetInMemPage(0)
		header := (*BTPageHeader)(page)
		initNewBtreePage(desc, 0, 0, 0, true)

		// 设置页面头信息
		header.chunksCount = 1
		header.itemsCount = 0 // 空chunk
		header.dataSize = 256

		// 设置chunk描述符
		chunkOffsets := []uint32{
			0, // chunk 0 offset
		}
		shortLocs := []uint32{
			LocationGetShort(256), // chunk 0 location: 从 256 开始
		}
		for i := 0; i < len(chunkOffsets); i++ {
			chunkDesc := header.GetChunkDesc(i)
			chunkDesc.SetOffset(chunkOffsets[i])
			chunkDesc.SetShortLocation(shortLocs[i])
		}

		// 创建并初始化定位器
		locator := &BTPageItemLocator{
			chunkOffset:     0,
			itemOffset:      0,
			chunkItemsCount: 0,
		}

		// 填充初始chunk信息
		pageChunkFillLocator(page, 0, locator)

		// 测试空chunk添加第一个item
		fits := pageLocatorFitsNewItem(page, locator, 16)
		assert.True(t, fits, "should have enough space for first item in empty chunk")

		// 测试对齐边界
		fits = pageLocatorFitsNewItem(page, locator, 7) // 7 会被对齐到 8
		assert.True(t, fits, "should handle alignment correctly")
	})
}

func TestPageLocatorGetItemSize(t *testing.T) {
	t.Run("same chunk items", func(t *testing.T) {
		// 分配一个页面空间
		page := make([]byte, BLOCK_SIZE)
		header := (*BTPageHeader)(unsafe.Pointer(&page[0]))

		// 设置页面头部信息
		header.chunksCount = 1
		header.dataSize = 100

		// 创建第一个chunk
		chunk := (*BTPageChunk)(unsafe.Pointer(&page[BT_PAGE_HEADER_SIZE]))

		// 写入测试数据：两个相邻的item
		// item1: offset = 10
		// item2: offset = 30
		*(*LocationIndex)(util.PointerAdd(
			unsafe.Pointer(&chunk.items[0]),
			0*int(LocationIndexSize),
		)) = LocationIndex(10) // 第一个item的offset

		*(*LocationIndex)(util.PointerAdd(
			unsafe.Pointer(&chunk.items[0]),
			1*int(LocationIndexSize),
		)) = LocationIndex(30) // 第二个item的offset

		// 创建locator
		locator := &BTPageItemLocator{
			chunkOffset:     0,
			itemOffset:      0,
			chunkItemsCount: 2,
			chunk:           chunk,
		}

		// 测试获取第一个item的大小
		size := pageLocatorGetItemSize(unsafe.Pointer(&page[0]), locator)
		if size != 20 { // 30 - 10 = 20
			t.Errorf("pageLocatorGetItemSize() = %v, want %v", size, 20)
		}
	})

	t.Run("different chunks items", func(t *testing.T) {
		// 分配一个页面空间
		page := make([]byte, BLOCK_SIZE)
		header := (*BTPageHeader)(unsafe.Pointer(&page[0]))

		// 设置页面头部信息
		header.chunksCount = 2
		header.dataSize = 200

		// 设置第一个chunk的描述符
		chunk1Desc := header.GetChunkDesc(0)
		chunk1Desc.SetShortLocation(LocationGetShort(uint32(BT_PAGE_HEADER_SIZE)))

		// 设置第二个chunk的描述符
		chunk2Desc := header.GetChunkDesc(1)
		chunk2Desc.SetShortLocation(LocationGetShort(uint32(BT_PAGE_HEADER_SIZE + 100)))

		// 创建第一个chunk
		chunk1 := (*BTPageChunk)(unsafe.Pointer(&page[BT_PAGE_HEADER_SIZE]))

		// 写入测试数据
		// chunk1的最后一个item: offset = 80
		*(*LocationIndex)(util.PointerAdd(
			unsafe.Pointer(&chunk1.items[0]),
			0*int(LocationIndexSize),
		)) = LocationIndex(80)

		// 创建第二个chunk
		chunk2 := (*BTPageChunk)(unsafe.Pointer(&page[BT_PAGE_HEADER_SIZE+100]))

		// 写入测试数据
		// chunk2的第一个item: offset = 20
		*(*LocationIndex)(util.PointerAdd(
			unsafe.Pointer(&chunk2.items[0]),
			0*int(LocationIndexSize),
		)) = LocationIndex(20)

		// 创建locator
		locator := &BTPageItemLocator{
			chunkOffset:     0,
			itemOffset:      0,
			chunkItemsCount: 1,
			chunk:           chunk1,
		}

		// 测试获取最后一个item的大小
		size := pageLocatorGetItemSize(unsafe.Pointer(&page[0]), locator)
		expectedSize := LocationIndex(20) // 第二个chunk的第一个item的offset - (第一个chunk的起始位置 + 80)
		if size != expectedSize {
			t.Errorf("pageLocatorGetItemSize() = %v, want %v", size, expectedSize)
		}
	})

	t.Run("last chunk last item", func(t *testing.T) {
		// 分配一个页面空间
		page := make([]byte, BLOCK_SIZE)
		header := (*BTPageHeader)(unsafe.Pointer(&page[0]))

		// 设置页面头部信息
		header.chunksCount = 1
		header.dataSize = 180 // BT_PAGE_HEADER_SIZE + 80 + 20

		// 创建chunk
		chunk := (*BTPageChunk)(unsafe.Pointer(&page[BT_PAGE_HEADER_SIZE]))

		// 写入测试数据：最后一个item
		*(*LocationIndex)(util.PointerAdd(
			unsafe.Pointer(&chunk.items[0]),
			0*int(LocationIndexSize),
		)) = LocationIndex(80)

		// 创建locator
		locator := &BTPageItemLocator{
			chunkOffset:     0,
			itemOffset:      0,
			chunkItemsCount: 1,
			chunk:           chunk,
		}

		// 测试获取最后一个item的大小
		size := pageLocatorGetItemSize(unsafe.Pointer(&page[0]), locator)
		expectedSize := LocationIndex(36) // dataSize(180) - (chunk起始位置 + 80)
		if size != expectedSize {
			t.Errorf("pageLocatorGetItemSize() = %v, want %v", size, expectedSize)
		}
	})
}

func TestPageLocatorResizeItem(t *testing.T) {
	t.Run("increase item size", func(t *testing.T) {
		// 分配页面空间
		page := make([]byte, BLOCK_SIZE)
		header := (*BTPageHeader)(unsafe.Pointer(&page[0]))

		// 设置页面头部信息
		header.chunksCount = 1
		header.itemsCount = 2
		header.dataSize = 208 // BT_PAGE_HEADER_SIZE + 80 + 24 (8字节对齐)

		// 创建chunk
		chunk := (*BTPageChunk)(unsafe.Pointer(&page[BT_PAGE_HEADER_SIZE]))

		// 写入测试数据
		chunk.SetItem(0, LocationIndex(80))  // 第一个item的offset
		chunk.SetItem(1, LocationIndex(104)) // 第二个item的offset (80 + 24)

		// 创建locator
		locator := &BTPageItemLocator{
			chunkOffset:     0,
			itemOffset:      0,
			chunkItemsCount: 2,
			chunkSize:       104,
			chunk:           chunk,
		}

		// 增加第一个item的大小 (24 -> 48)
		pageLocatorResizeItem(unsafe.Pointer(&page[0]), locator, 48)

		// 验证结果
		assert.Equal(t, LocationIndex(232), header.dataSize)  // 184 + 48
		assert.Equal(t, LocationIndex(80), chunk.GetItem(0))  // 第一个item的offset不变
		assert.Equal(t, LocationIndex(128), chunk.GetItem(1)) // 第二个item的offset增加48
	})

	t.Run("no size change", func(t *testing.T) {
		// 分配页面空间
		page := make([]byte, BLOCK_SIZE)
		header := (*BTPageHeader)(unsafe.Pointer(&page[0]))

		// 设置页面头部信息
		header.chunksCount = 1
		header.itemsCount = 2
		header.dataSize = 184 // BT_PAGE_HEADER_SIZE + 80 + 24 (8字节对齐)

		// 创建chunk
		chunk := (*BTPageChunk)(unsafe.Pointer(&page[BT_PAGE_HEADER_SIZE]))

		// 写入测试数据
		chunk.SetItem(0, LocationIndex(80))  // 第一个item的offset
		chunk.SetItem(1, LocationIndex(104)) // 第二个item的offset (80 + 24)

		// 创建locator
		locator := &BTPageItemLocator{
			chunkOffset:     0,
			itemOffset:      0,
			chunkItemsCount: 2,
			chunkSize:       104,
			chunk:           chunk,
		}

		// 保持item大小不变
		pageLocatorResizeItem(unsafe.Pointer(&page[0]), locator, 24)

		// 验证结果
		assert.Equal(t, LocationIndex(184), header.dataSize)  // 保持不变
		assert.Equal(t, LocationIndex(80), chunk.GetItem(0))  // 保持不变
		assert.Equal(t, LocationIndex(104), chunk.GetItem(1)) // 保持不变
	})

	t.Run("multiple chunks", func(t *testing.T) {
		// 分配页面空间
		page := make([]byte, BLOCK_SIZE)
		header := (*BTPageHeader)(unsafe.Pointer(&page[0]))

		// 设置页面头部信息
		header.chunksCount = 2
		header.itemsCount = 3
		header.dataSize = 288 // BT_PAGE_HEADER_SIZE + 80 + 24 + 104 (8字节对齐)

		// 设置第一个chunk的描述符
		chunk1Desc := header.GetChunkDesc(0)
		chunk1Desc.SetShortLocation(LocationGetShort(uint32(BT_PAGE_HEADER_SIZE)))

		// 设置第二个chunk的描述符
		chunk2Desc := header.GetChunkDesc(1)
		chunk2Desc.SetShortLocation(LocationGetShort(uint32(BT_PAGE_HEADER_SIZE + 104)))

		// 创建第一个chunk
		chunk1 := (*BTPageChunk)(util.PointerAdd(
			unsafe.Pointer(&page[0]),
			int(ShortGetLocation(chunk1Desc.GetShortLocation()))))
		chunk1.SetItem(0, LocationIndex(80))  // 第一个item的offset
		chunk1.SetItem(1, LocationIndex(104)) // 第二个item的offset

		// 创建第二个chunk
		chunk2 := (*BTPageChunk)(util.PointerAdd(
			unsafe.Pointer(&page[0]),
			int(ShortGetLocation(chunk2Desc.GetShortLocation()))))
		fmt.Println(ShortGetLocation(chunk2Desc.GetShortLocation()))
		//184
		chunk2.SetItem(0, LocationIndex(ShortGetLocation(chunk2Desc.GetShortLocation()))) // 第三个item的offset
	})
}

func TestPageLocatorInsertItem(t *testing.T) {
	t.Run("insert item in empty chunk", func(t *testing.T) {
		// 分配页面空间
		page := make([]byte, BLOCK_SIZE)
		header := (*BTPageHeader)(unsafe.Pointer(&page[0]))

		// 设置页面头部信息
		header.chunksCount = 1
		header.itemsCount = 0
		header.dataSize = 80 // BT_PAGE_HEADER_SIZE

		// 创建chunk
		chunk := (*BTPageChunk)(unsafe.Pointer(&page[BT_PAGE_HEADER_SIZE]))

		// 创建locator
		locator := &BTPageItemLocator{
			chunkOffset:     0,
			itemOffset:      0,
			chunkItemsCount: 0,
			chunkSize:       0,
			chunk:           chunk,
		}

		// 插入一个24字节的item
		pageLocatorInsertItem(unsafe.Pointer(&page[0]), locator, 24)

		// 验证结果
		assert.Equal(t, OffsetNumber(1), header.itemsCount)
		assert.Equal(t, LocationIndex(112), header.dataSize) // 80 + 24+8
		assert.Equal(t, LocationIndex(8), chunk.GetItem(0))  // 第一个item的offset
	})

	t.Run("insert item between existing items", func(t *testing.T) {
		// 分配页面空间
		page := make([]byte, BLOCK_SIZE)
		header := (*BTPageHeader)(unsafe.Pointer(&page[0]))

		// 设置页面头部信息
		header.chunksCount = 1
		header.itemsCount = 2
		header.dataSize = 208 // BT_PAGE_HEADER_SIZE + 80 + 24 + 24

		// 创建chunk
		chunk := (*BTPageChunk)(unsafe.Pointer(&page[BT_PAGE_HEADER_SIZE]))

		// 写入测试数据
		chunk.SetItem(0, LocationIndex(80))  // 第一个item的offset
		chunk.SetItem(1, LocationIndex(104)) // 第二个item的offset

		// 创建locator
		locator := &BTPageItemLocator{
			chunkOffset:     0,
			itemOffset:      1, // 在第一个和第二个item之间插入
			chunkItemsCount: 2,
			chunkSize:       104,
			chunk:           chunk,
		}

		// 插入一个24字节的item
		pageLocatorInsertItem(unsafe.Pointer(&page[0]), locator, 24)

		// 验证结果
		//由于items array增加了一个元素。所有的offset都要加8
		assert.Equal(t, OffsetNumber(3), header.itemsCount)
		assert.Equal(t, LocationIndex(232), header.dataSize)  // 208 + 24 + 8
		assert.Equal(t, LocationIndex(80), chunk.GetItem(0))  // 第一个item的offset+8
		assert.Equal(t, LocationIndex(104), chunk.GetItem(1)) // 新插入的item的offset
		assert.Equal(t, LocationIndex(128), chunk.GetItem(2)) // 原来的第二个item的offset增加24
	})

	t.Run("insert item at end of chunk", func(t *testing.T) {
		// 分配页面空间
		page := make([]byte, BLOCK_SIZE)
		header := (*BTPageHeader)(unsafe.Pointer(&page[0]))

		// 设置页面头部信息
		header.chunksCount = 1
		header.itemsCount = 2
		header.dataSize = 208 // BT_PAGE_HEADER_SIZE + 80 + 24 + 24

		// 创建chunk
		chunk := (*BTPageChunk)(unsafe.Pointer(&page[BT_PAGE_HEADER_SIZE]))

		// 写入测试数据
		chunk.SetItem(0, LocationIndex(80))  // 第一个item的offset
		chunk.SetItem(1, LocationIndex(104)) // 第二个item的offset

		// 创建locator
		locator := &BTPageItemLocator{
			chunkOffset:     0,
			itemOffset:      2, // 在chunk末尾插入
			chunkItemsCount: 2,
			chunkSize:       128,
			chunk:           chunk,
		}

		// 插入一个24字节的item
		pageLocatorInsertItem(unsafe.Pointer(&page[0]), locator, 24)

		// 验证结果
		assert.Equal(t, OffsetNumber(3), header.itemsCount)
		assert.Equal(t, LocationIndex(232), header.dataSize)  // 208 + 24+8
		assert.Equal(t, LocationIndex(80), chunk.GetItem(0))  // 第一个item的offset+8
		assert.Equal(t, LocationIndex(104), chunk.GetItem(1)) // 第二个item的offset+8
		assert.Equal(t, LocationIndex(128), chunk.GetItem(2)) // 新插入的item的offset+
	})

	t.Run("insert item with multiple chunks", func(t *testing.T) {
		// 分配页面空间
		page := make([]byte, BLOCK_SIZE)
		header := (*BTPageHeader)(unsafe.Pointer(&page[0]))

		// 设置页面头部信息
		header.chunksCount = 2
		header.itemsCount = 3
		header.dataSize = 288 // BT_PAGE_HEADER_SIZE + 80 + 24 + 24 + 104

		// 设置第一个chunk的描述符
		chunk1Desc := header.GetChunkDesc(0)
		chunk1Desc.SetShortLocation(LocationGetShort(uint32(BT_PAGE_HEADER_SIZE)))

		// 设置第二个chunk的描述符
		chunk2Desc := header.GetChunkDesc(1)
		chunk2Desc.SetShortLocation(LocationGetShort(uint32(BT_PAGE_HEADER_SIZE + 104)))

		// 创建第一个chunk
		chunk1 := (*BTPageChunk)(util.PointerAdd(
			unsafe.Pointer(&page[0]),
			int(ShortGetLocation(chunk1Desc.GetShortLocation()))))
		chunk1.SetItem(0, LocationIndex(80))  // 第一个item的offset
		chunk1.SetItem(1, LocationIndex(104)) // 第二个item的offset

		// 创建第二个chunk
		chunk2 := (*BTPageChunk)(util.PointerAdd(
			unsafe.Pointer(&page[0]),
			int(ShortGetLocation(chunk2Desc.GetShortLocation()))))
		chunk2.SetItem(0, LocationIndex(0)) // 第三个item的offset

		chunk2Loc := int(ShortGetLocation(chunk2Desc.GetShortLocation()))

		// 创建locator
		locator := &BTPageItemLocator{
			chunkOffset:     0,
			itemOffset:      1, // 在第一个chunk的中间插入
			chunkItemsCount: 2,
			chunkSize:       184,
			chunk:           chunk1,
		}

		// 插入一个24字节的item
		pageLocatorInsertItem(unsafe.Pointer(&page[0]), locator, 24)

		// 验证结果
		assert.Equal(t, OffsetNumber(4), header.itemsCount)
		assert.Equal(t, LocationIndex(312), header.dataSize)   // 288 + 24+8
		assert.Equal(t, LocationIndex(80), chunk1.GetItem(0))  // 第一个item的offset不变
		assert.Equal(t, LocationIndex(104), chunk1.GetItem(1)) // 新插入的item的offset
		assert.Equal(t, LocationIndex(128), chunk1.GetItem(2)) // 原来的第二个item的offset增加24
		chunk2Loc2 := int(ShortGetLocation(chunk2Desc.GetShortLocation()))
		assert.Equal(t, chunk2Loc+24, chunk2Loc2)
		assert.Equal(t, LocationIndex(0), chunk2.GetItem(0)) // 第二个chunk的item的offset增加24
	})
}

func TestPageMergeChunks(t *testing.T) {
	t.Run("merge chunks with hikey", func(t *testing.T) {
		// 分配一个页面空间
		page := make([]byte, BLOCK_SIZE)

		// 设置测试数据
		chunksData := []ChunkData{
			{
				// 第一个chunk
				Items: []struct {
					Data []byte
				}{
					{
						Data: []byte("item1\000\000\000"), // 扩展到8字节
					},
					{
						Data: []byte("item2\000\000\000"), // 扩展到8字节
					},
				},
				Hikey: []byte{1, 2, 3, 4, 0, 0, 0, 0}, // 扩展到8字节
			},
			{
				// 第二个chunk
				Items: []struct {
					Data []byte
				}{
					{
						Data: []byte("item3\000\000\000"), // 扩展到8字节
					},
					{
						Data: []byte("item4\000\000\000"), // 扩展到8字节
					},
				},
				Hikey: []byte{5, 6, 7, 8, 0, 0, 0, 0}, // 扩展到8字节
			},
		}

		// 设置页面
		chunksInfo := SetupPageWithChunks(unsafe.Pointer(&page[0]), chunksData)
		fmt.Println(chunksInfo)

		// 打印合并前的页面信息
		fmt.Println("Before merge:")
		PrintPageInfo(unsafe.Pointer(&page[0]))

		// 执行merge操作
		pageMergeChunks(unsafe.Pointer(&page[0]), OffsetNumber(0))

		// 打印合并后的页面信息
		fmt.Println("After merge:")
		PrintPageInfo(unsafe.Pointer(&page[0]))

		// 验证merge后的结果
		header := (*BTPageHeader)(unsafe.Pointer(&page[0]))

		// 1. 验证chunksCount
		assert.Equal(t, OffsetNumber(1), header.chunksCount, "chunksCount should be 1 after merge")

		// 2. 验证itemsCount
		assert.Equal(t, OffsetNumber(4), header.itemsCount, "itemsCount should be 4 after merge")

		// 3. 验证hikey区域
		// 3.1 验证hikey区域大小
		// 获取chunkDesc字段相对于BTPageHeader的偏移量
		chunkDescOffset := uint32(BT_PAGE_HEADER_CHUNK_DESC_OFFSET)
		// 计算chunkDesc数组的大小(8字节对齐)
		chunkDescSize := util.AlignValue(uint32(BT_PAGE_CHUNK_DESC_SIZE)*uint32(header.chunksCount), 8)
		// hikey区域应该在header和chunkDesc之后
		expectedHikeysEnd := OffsetNumber(chunkDescOffset + chunkDescSize + 8) // 8是hikey的大小
		assert.Equal(t, expectedHikeysEnd, header.hikeysEnd, "hikeysEnd should be chunkDescOffset + chunkDescSize + hikeySize")

		// 3.2 验证hikey内容
		expectedHikey := []byte{5, 6, 7, 8, 0, 0, 0, 0} // 修改为chunk[1]的hikey,扩展到8字节
		actualHikey := page[chunkDescOffset+chunkDescSize : header.hikeysEnd]
		assert.Equal(t, expectedHikey, actualHikey, "hikey content should be preserved")

		// 4. 验证chunk的位置和内容
		// 4.1 验证第一个chunk的位置
		chunk1Desc := header.GetChunkDesc(0)
		expectedChunkLoc := chunkDescOffset + chunkDescSize + 16 // chunkDesc + hikey(对齐后)
		assert.Equal(t, LocationGetShort(expectedChunkLoc), chunk1Desc.GetShortLocation(), "chunk1 location should be correct")

		// 4.2 验证chunk的内容
		chunk1Loc := ShortGetLocation(chunk1Desc.GetShortLocation())
		chunk1 := (*BTPageChunk)(util.PointerAdd(unsafe.Pointer(&page[0]), int(chunk1Loc)))

		// 验证所有items
		expectedItems := []struct {
			offset uint32
			data   []byte
			size   int
		}{
			{8, []byte("item1\000\000\000"), 8},  // 修改为相对于chunk的offset
			{16, []byte("item2\000\000\000"), 8}, // 修改为相对于chunk的offset
			{24, []byte("item3\000\000\000"), 8}, // 修改为相对于chunk的offset
			{32, []byte("item4\000\000\000"), 8}, // 修改为相对于chunk的offset
		}

		for i, expected := range expectedItems {
			// 验证offset
			itemOffset := chunk1.GetItem(i)
			assert.Equal(t, LocationIndex(expected.offset), itemOffset, fmt.Sprintf("item %d offset should be correct", i))

			// 验证数据
			itemData := make([]byte, expected.size)
			util.CMemcpy(
				unsafe.Pointer(&itemData[0]),
				util.PointerAdd(unsafe.Pointer(chunk1), int(itemOffset)),
				expected.size,
			)
			assert.Equal(t, expected.data, itemData[:len(expected.data)], fmt.Sprintf("item %d data should be correct", i))
		}
	})
}

func TestPageLocatorDeleteItem(t *testing.T) {
	t.Run("delete middle item", func(t *testing.T) {
		// 分配一个页面空间
		page := make([]byte, BLOCK_SIZE)

		// 设置测试数据
		chunksData := []ChunkData{
			{
				// 第一个chunk
				Items: []struct {
					Data []byte
				}{
					{
						Data: []byte("item1\000\000\000"), // 扩展到8字节
					},
					{
						Data: []byte("item2\000\000\000"), // 扩展到8字节
					},
					{
						Data: []byte("item3\000\000\000"), // 扩展到8字节
					},
				},
				Hikey: []byte{1, 2, 3, 4, 0, 0, 0, 0}, // 扩展到8字节
			},
		}

		// 设置页面
		_ = SetupPageWithChunks(unsafe.Pointer(&page[0]), chunksData)

		// 打印删除前的页面信息
		fmt.Println("Before delete:")
		PrintPageInfo(unsafe.Pointer(&page[0]))

		// 获取header
		header := (*BTPageHeader)(unsafe.Pointer(&page[0]))

		// 创建locator并定位到第二个item
		locator := &BTPageItemLocator{
			chunkOffset:     0,
			itemOffset:      1, // 定位到第二个item
			chunkItemsCount: 3,
		}
		// 获取chunk指针
		chunk1Desc := header.GetChunkDesc(0)
		locator.chunk = (*BTPageChunk)(util.PointerAdd(
			unsafe.Pointer(&page[0]),
			int(ShortGetLocation(chunk1Desc.GetShortLocation()))))
		locator.chunkSize = LocationIndex(32) // 3个item,每个8字节

		// 执行删除操作
		pageLocatorDeleteItem(unsafe.Pointer(&page[0]), locator)

		// 打印删除后的页面信息
		fmt.Println("After delete:")
		PrintPageInfo(unsafe.Pointer(&page[0]))

		// 验证结果
		// 1. 验证itemsCount
		assert.Equal(t, OffsetNumber(2), header.itemsCount, "itemsCount should be 2 after delete")

		// 2. 验证chunk的内容
		chunk1 := (*BTPageChunk)(util.PointerAdd(
			unsafe.Pointer(&page[0]),
			int(ShortGetLocation(chunk1Desc.GetShortLocation()))))

		// 验证剩余items
		expectedItems := []struct {
			offset uint32
			data   []byte
		}{
			{8, []byte("item1\000\000\000")},  // 第一个item保持不变
			{16, []byte("item3\000\000\000")}, // 第三个item(现在是第二个)
		}

		for i, expected := range expectedItems {
			// 验证offset
			itemOffset := chunk1.GetItem(i)
			assert.Equal(t, LocationIndex(expected.offset), itemOffset, fmt.Sprintf("item %d offset should be correct", i))

			// 验证数据
			itemData := make([]byte, 8)
			util.CMemcpy(
				unsafe.Pointer(&itemData[0]),
				util.PointerAdd(unsafe.Pointer(chunk1), int(itemOffset)),
				8,
			)
			assert.Equal(t, expected.data, itemData[:len(expected.data)], fmt.Sprintf("item %d data should be correct", i))
		}
	})

	t.Run("delete last item", func(t *testing.T) {
		// 分配一个页面空间
		page := make([]byte, BLOCK_SIZE)

		// 设置测试数据
		chunksData := []ChunkData{
			{
				// 第一个chunk
				Items: []struct {
					Data []byte
				}{
					{
						Data: []byte("item1\000\000\000"), // 扩展到8字节
					},
					{
						Data: []byte("item2\000\000\000"), // 扩展到8字节
					},
				},
				Hikey: []byte{1, 2, 3, 4, 0, 0, 0, 0}, // 扩展到8字节
			},
		}

		// 设置页面
		_ = SetupPageWithChunks(unsafe.Pointer(&page[0]), chunksData)

		// 打印删除前的页面信息
		fmt.Println("Before delete:")
		PrintPageInfo(unsafe.Pointer(&page[0]))

		// 获取header
		header := (*BTPageHeader)(unsafe.Pointer(&page[0]))

		// 创建locator并定位到最后一个item
		locator := &BTPageItemLocator{
			chunkOffset:     0,
			itemOffset:      1, // 定位到最后一个item
			chunkItemsCount: 2,
		}
		// 获取chunk指针
		chunk1Desc := header.GetChunkDesc(0)
		locator.chunk = (*BTPageChunk)(util.PointerAdd(
			unsafe.Pointer(&page[0]),
			int(ShortGetLocation(chunk1Desc.GetShortLocation()))))
		locator.chunkSize = LocationIndex(24) // 2个item,每个8字节

		// 执行删除操作
		pageLocatorDeleteItem(unsafe.Pointer(&page[0]), locator)

		// 打印删除后的页面信息
		fmt.Println("After delete:")
		PrintPageInfo(unsafe.Pointer(&page[0]))

		// 验证结果
		// 1. 验证itemsCount
		assert.Equal(t, OffsetNumber(1), header.itemsCount, "itemsCount should be 1 after delete")

		// 2. 验证chunk的内容
		chunk1 := (*BTPageChunk)(util.PointerAdd(
			unsafe.Pointer(&page[0]),
			int(ShortGetLocation(chunk1Desc.GetShortLocation()))))

		// 验证剩余item
		expectedItem := struct {
			offset uint32
			data   []byte
		}{
			offset: 8,
			data:   []byte("item1\000\000\000"),
		}

		// 验证offset
		itemOffset := chunk1.GetItem(0)
		assert.Equal(t, LocationIndex(expectedItem.offset), itemOffset, "item offset should be correct")

		// 验证数据
		itemData := make([]byte, 8)
		util.CMemcpy(
			unsafe.Pointer(&itemData[0]),
			util.PointerAdd(unsafe.Pointer(chunk1), int(itemOffset)),
			8,
		)
		assert.Equal(t, expectedItem.data, itemData[:len(expectedItem.data)], "item data should be correct")
	})

	t.Run("delete last item and merge chunks", func(t *testing.T) {
		// 分配一个页面空间
		page := make([]byte, BLOCK_SIZE)

		// 设置测试数据
		chunksData := []ChunkData{
			{
				// 第一个chunk
				Items: []struct {
					Data []byte
				}{
					{
						Data: []byte("item1\000\000\000"), // 扩展到8字节
					},
				},
				Hikey: []byte{1, 2, 3, 4, 0, 0, 0, 0}, // 扩展到8字节
			},
			{
				// 第二个chunk
				Items: []struct {
					Data []byte
				}{
					{
						Data: []byte("item2\000\000\000"), // 扩展到8字节
					},
				},
				Hikey: []byte{5, 6, 7, 8, 0, 0, 0, 0}, // 扩展到8字节
			},
		}

		// 设置页面
		_ = SetupPageWithChunks(unsafe.Pointer(&page[0]), chunksData)

		// 打印删除前的页面信息
		fmt.Println("Before delete:")
		PrintPageInfo(unsafe.Pointer(&page[0]))

		// 获取header
		header := (*BTPageHeader)(unsafe.Pointer(&page[0]))

		// 创建locator并定位到第二个chunk的item
		locator := &BTPageItemLocator{
			chunkOffset:     1,
			itemOffset:      0, // 定位到第二个chunk的item
			chunkItemsCount: 1,
		}
		// 获取chunk指针
		chunk2Desc := header.GetChunkDesc(1)
		locator.chunk = (*BTPageChunk)(util.PointerAdd(
			unsafe.Pointer(&page[0]),
			int(ShortGetLocation(chunk2Desc.GetShortLocation()))))
		locator.chunkSize = LocationIndex(16) // 1个item,8字节

		// 执行删除操作
		pageLocatorDeleteItem(unsafe.Pointer(&page[0]), locator)

		// 打印删除后的页面信息
		fmt.Println("After delete:")
		PrintPageInfo(unsafe.Pointer(&page[0]))

		// 验证结果
		// 1. 验证chunksCount
		assert.Equal(t, OffsetNumber(1), header.chunksCount, "chunksCount should be 1 after merge")

		// 2. 验证itemsCount
		assert.Equal(t, OffsetNumber(1), header.itemsCount, "itemsCount should be 1 after delete")

		// 3. 验证chunk的内容
		chunk1Desc := header.GetChunkDesc(0)
		chunk1 := (*BTPageChunk)(util.PointerAdd(
			unsafe.Pointer(&page[0]),
			int(ShortGetLocation(chunk1Desc.GetShortLocation()))))

		// 验证剩余item
		expectedItem := struct {
			offset uint32
			data   []byte
		}{
			offset: 8,
			data:   []byte("item1\000\000\000"),
		}

		// 验证offset
		itemOffset := chunk1.GetItem(0)
		assert.Equal(t, LocationIndex(expectedItem.offset), itemOffset, "item offset should be correct")

		// 验证数据
		itemData := make([]byte, 8)
		util.CMemcpy(
			unsafe.Pointer(&itemData[0]),
			util.PointerAdd(unsafe.Pointer(chunk1), int(itemOffset)),
			8,
		)
		assert.Equal(t, expectedItem.data, itemData[:len(expectedItem.data)], "item data should be correct")
	})

	t.Run("delete last item and merge with next chunk", func(t *testing.T) {
		// 分配一个页面空间
		page := make([]byte, BLOCK_SIZE)

		// 设置测试数据
		chunksData := []ChunkData{
			{
				// 第一个chunk
				Items: []struct {
					Data []byte
				}{
					{
						Data: []byte("item1\000\000\000"), // 扩展到8字节
					},
				},
				Hikey: []byte{1, 2, 3, 4, 0, 0, 0, 0}, // 扩展到8字节
			},
			{
				// 第二个chunk
				Items: []struct {
					Data []byte
				}{
					{
						Data: []byte("item2\000\000\000"), // 扩展到8字节
					},
				},
				Hikey: []byte{5, 6, 7, 8, 0, 0, 0, 0}, // 扩展到8字节
			},
		}

		// 设置页面
		_ = SetupPageWithChunks(unsafe.Pointer(&page[0]), chunksData)

		// 打印删除前的页面信息
		fmt.Println("Before delete:")
		PrintPageInfo(unsafe.Pointer(&page[0]))

		// 获取header
		header := (*BTPageHeader)(unsafe.Pointer(&page[0]))

		// 创建locator并定位到第一个chunk的唯一item
		locator := &BTPageItemLocator{
			chunkOffset:     0,
			itemOffset:      0, // 定位到第一个chunk的唯一item
			chunkItemsCount: 1,
		}
		// 获取chunk指针
		chunk1Desc := header.GetChunkDesc(0)
		locator.chunk = (*BTPageChunk)(util.PointerAdd(
			unsafe.Pointer(&page[0]),
			int(ShortGetLocation(chunk1Desc.GetShortLocation()))))
		locator.chunkSize = LocationIndex(16) // 1个item,8字节

		// 执行删除操作
		pageLocatorDeleteItem(unsafe.Pointer(&page[0]), locator)

		// 打印删除后的页面信息
		fmt.Println("After delete:")
		PrintPageInfo(unsafe.Pointer(&page[0]))

		// 验证结果
		// 1. 验证chunksCount
		assert.Equal(t, OffsetNumber(1), header.chunksCount, "chunksCount should be 1 after merge")

		// 2. 验证itemsCount
		assert.Equal(t, OffsetNumber(1), header.itemsCount, "itemsCount should be 1 after delete")

		// 3. 验证chunk的内容
		chunk1 := (*BTPageChunk)(util.PointerAdd(
			unsafe.Pointer(&page[0]),
			int(ShortGetLocation(chunk1Desc.GetShortLocation()))))

		// 验证剩余item
		expectedItem := struct {
			offset uint32
			data   []byte
		}{
			offset: 8,
			data:   []byte("item2\000\000\000"),
		}

		// 验证offset
		itemOffset := chunk1.GetItem(0)
		assert.Equal(t, LocationIndex(expectedItem.offset), itemOffset, "item offset should be correct")

		// 验证数据
		itemData := make([]byte, 8)
		util.CMemcpy(
			unsafe.Pointer(&itemData[0]),
			util.PointerAdd(unsafe.Pointer(chunk1), int(itemOffset)),
			8,
		)
		assert.Equal(t, expectedItem.data, itemData[:len(expectedItem.data)], "item data should be correct")
	})
}

func TestPageSplitChunk(t *testing.T) {
	// 测试在chunk中间分割
	t.Run("split chunk in middle", func(t *testing.T) {
		// 分配一个页面空间
		page := make([]byte, BLOCK_SIZE)
		header := (*BTPageHeader)(unsafe.Pointer(&page[0]))

		// 初始化页面头部
		header.chunksCount = 1
		header.itemsCount = 3
		header.dataSize = 256 // 初始dataSize

		// 计算chunkDesc区域大小
		chunkDescSize := util.AlignValue(uint32(BT_PAGE_CHUNK_DESC_SIZE)*uint32(header.chunksCount), 8)
		chunkDescOffset := uint32(BT_PAGE_HEADER_CHUNK_DESC_OFFSET)

		// 设置hikey区域，预留足够空间
		hikeySize := LocationIndex(8)
		hikeyStart := chunkDescOffset + chunkDescSize
		header.hikeysEnd = OffsetNumber(hikeyStart + uint32(hikeySize)) // 额外添加64字节的缓冲区

		chunkStartLoc := 136

		// 设置第一个chunk的描述符
		chunk1Desc := header.GetChunkDesc(0)
		chunk1Desc.SetOffset(0)
		chunk1Desc.SetShortLocation(LocationGetShort(uint32(chunkStartLoc)))
		chunk1Desc.SetHikeyShortLocation(LocationGetShort(hikeyStart)) // 设置hikey的起始位置

		// 创建第一个chunk
		chunk1 := (*BTPageChunk)(util.PointerAdd(
			unsafe.Pointer(&page[0]),
			int(ShortGetLocation(chunk1Desc.GetShortLocation()))))

		// 写入测试数据
		chunk1.SetItem(0, LocationIndex(8))  // 第一个item的offset
		chunk1.SetItem(1, LocationIndex(16)) // 第二个item的offset
		chunk1.SetItem(2, LocationIndex(24)) // 第三个item的offset

		// 写入item数据
		util.CMemcpy(
			util.PointerAdd(unsafe.Pointer(chunk1), 8),
			unsafe.Pointer(&[]byte("item1\000\000\000")[0]),
			8,
		)
		util.CMemcpy(
			util.PointerAdd(unsafe.Pointer(chunk1), 16),
			unsafe.Pointer(&[]byte("item2\000\000\000")[0]),
			8,
		)
		util.CMemcpy(
			util.PointerAdd(unsafe.Pointer(chunk1), 24),
			unsafe.Pointer(&[]byte("item3\000\000\000")[0]),
			8,
		)

		// 写入hikey数据（只写hikeySize长度）
		util.CMemcpy(
			util.PointerAdd(unsafe.Pointer(&page[0]), int(hikeyStart)),
			unsafe.Pointer(&[]byte{1, 2, 3, 4, 0, 0, 0, 0}[0]),
			8,
		)

		// 打印分割前的页面信息
		fmt.Println("Before split:")
		PrintPageInfo(unsafe.Pointer(&page[0]))

		// 创建locator并定位到第二个item
		locator := &BTPageItemLocator{
			chunkOffset:     0,
			itemOffset:      1, // 定位到第二个item
			chunkItemsCount: 3,
			chunk:           chunk1,
			chunkSize:       LocationIndex(32), // 3个item,每个8字节
		}

		// 计算hikeyEnd参数,确保满足条件
		// 计算新的hikeyEnd，确保有足够空间容纳新的chunk desc和hikey
		hikeyEnd := LocationIndex(header.hikeysEnd) // 使用当前的hikeysEnd作为基准

		// 执行分割操作
		pageSplitChunk(unsafe.Pointer(&page[0]), locator, hikeyEnd+16, hikeySize)

		// 打印分割后的页面信息
		fmt.Println("After split:")
		PrintPageInfo(unsafe.Pointer(&page[0]))

		// 验证结果
		// 1. 验证chunksCount
		assert.Equal(t, OffsetNumber(2), header.chunksCount, "chunksCount should be 2 after split")

		// 2. 验证itemsCount
		assert.Equal(t, OffsetNumber(3), header.itemsCount, "itemsCount should remain unchanged")

		// 3. 验证第一个chunk的内容
		chunk1 = (*BTPageChunk)(util.PointerAdd(
			unsafe.Pointer(&page[0]),
			int(ShortGetLocation(chunk1Desc.GetShortLocation()))))
		chunk1ItemsCount := header.GetChunkDesc(1).GetOffset() - header.GetChunkDesc(0).GetOffset()
		assert.Equal(t, uint32(1), chunk1ItemsCount, "first chunk should have 1 item")
		assert.Equal(t, LocationIndex(8), chunk1.GetItem(0), "first chunk first item offset should be correct")
		itemData := make([]byte, 8)
		util.CMemcpy(
			unsafe.Pointer(&itemData[0]),
			util.PointerAdd(unsafe.Pointer(chunk1), int(chunk1.GetItem(0))),
			8,
		)
		assert.Equal(t, []byte("item1\000\000\000"), itemData, "first chunk first item data should be correct")

		// 4. 验证第二个chunk的内容
		chunk2Desc := header.GetChunkDesc(1)
		chunk2 := (*BTPageChunk)(util.PointerAdd(
			unsafe.Pointer(&page[0]),
			int(ShortGetLocation(chunk2Desc.GetShortLocation()))))
		chunk2ItemsCount := uint32(header.itemsCount) - header.GetChunkDesc(1).GetOffset()
		assert.Equal(t, uint32(2), chunk2ItemsCount, "second chunk should have 2 items")
		assert.Equal(t, LocationIndex(8), chunk2.GetItem(0), "second chunk first item offset should be correct")
		assert.Equal(t, LocationIndex(16), chunk2.GetItem(1), "second chunk second item offset should be correct")
		util.CMemcpy(
			unsafe.Pointer(&itemData[0]),
			util.PointerAdd(unsafe.Pointer(chunk2), int(chunk2.GetItem(0))),
			8,
		)
		assert.Equal(t, []byte("item2\000\000\000"), itemData, "second chunk first item data should be correct")
		util.CMemcpy(
			unsafe.Pointer(&itemData[0]),
			util.PointerAdd(unsafe.Pointer(chunk2), int(chunk2.GetItem(1))),
			8,
		)
		assert.Equal(t, []byte("item3\000\000\000"), itemData, "second chunk second item data should be correct")
	})

	// 测试在chunk开始位置分割
	t.Run("split chunk at start", func(t *testing.T) {
		// 分配一个页面空间
		page := make([]byte, BLOCK_SIZE)
		header := (*BTPageHeader)(unsafe.Pointer(&page[0]))

		// 初始化页面头部
		header.chunksCount = 1
		header.itemsCount = 2
		header.dataSize = 256 // 初始dataSize

		// 计算chunkDesc区域大小
		chunkDescSize := util.AlignValue(uint32(BT_PAGE_CHUNK_DESC_SIZE)*uint32(header.chunksCount), 8)
		chunkDescOffset := uint32(BT_PAGE_HEADER_CHUNK_DESC_OFFSET)

		// 设置hikey区域，预留足够空间
		hikeySize := LocationIndex(8)
		hikeyStart := chunkDescOffset + chunkDescSize
		header.hikeysEnd = OffsetNumber(hikeyStart + uint32(hikeySize)) // 移除额外的64字节缓冲区

		chunkStartLoc := 136 // 固定chunk起始位置

		// 设置第一个chunk的描述符
		chunk1Desc := header.GetChunkDesc(0)
		chunk1Desc.SetOffset(0)
		chunk1Desc.SetShortLocation(LocationGetShort(uint32(chunkStartLoc)))
		chunk1Desc.SetHikeyShortLocation(LocationGetShort(hikeyStart)) // 设置hikey的起始位置

		// 创建第一个chunk
		chunk1 := (*BTPageChunk)(util.PointerAdd(
			unsafe.Pointer(&page[0]),
			int(ShortGetLocation(chunk1Desc.GetShortLocation()))))

		// 写入测试数据
		chunk1.SetItem(0, LocationIndex(8))  // 第一个item的offset
		chunk1.SetItem(1, LocationIndex(16)) // 第二个item的offset

		// 写入item数据
		util.CMemcpy(
			util.PointerAdd(unsafe.Pointer(chunk1), 8),
			unsafe.Pointer(&[]byte("item1\000\000\000")[0]),
			8,
		)
		util.CMemcpy(
			util.PointerAdd(unsafe.Pointer(chunk1), 16),
			unsafe.Pointer(&[]byte("item2\000\000\000")[0]),
			8,
		)

		// 写入hikey数据
		util.CMemcpy(
			util.PointerAdd(unsafe.Pointer(&page[0]), int(hikeyStart)),
			unsafe.Pointer(&[]byte{1, 2, 3, 4, 0, 0, 0, 0}[0]),
			8,
		)

		// 打印分割前的页面信息
		fmt.Println("Before split:")
		PrintPageInfo(unsafe.Pointer(&page[0]))

		// 创建locator并定位到第一个item
		locator := &BTPageItemLocator{
			chunkOffset:     0,
			itemOffset:      0, // 定位到第一个item
			chunkItemsCount: 2,
			chunk:           chunk1,
			chunkSize:       LocationIndex(24), // 2个item,每个8字节
		}

		// 计算hikeyEnd参数,确保满足条件
		chunkDescSize = util.AlignValue(uint32(BT_PAGE_CHUNK_DESC_SIZE)*uint32(header.chunksCount+1), 8)
		// 计算新的hikeyEnd，确保有足够空间容纳新的chunk desc和hikey
		hikeyEnd := LocationIndex(header.hikeysEnd) // 使用当前的hikeysEnd作为基准

		// 执行分割操作
		pageSplitChunk(unsafe.Pointer(&page[0]), locator, hikeyEnd+16, hikeySize)

		// 打印分割后的页面信息
		fmt.Println("After split:")
		PrintPageInfo(unsafe.Pointer(&page[0]))

		// 验证结果
		// 1. 验证chunksCount
		assert.Equal(t, OffsetNumber(2), header.chunksCount, "chunksCount should be 2 after split")

		// 2. 验证itemsCount
		assert.Equal(t, OffsetNumber(2), header.itemsCount, "itemsCount should remain unchanged")

		// 3. 验证第一个chunk的内容
		chunk1 = (*BTPageChunk)(util.PointerAdd(
			unsafe.Pointer(&page[0]),
			int(ShortGetLocation(chunk1Desc.GetShortLocation()))))

		// 验证第一个chunk的items
		chunk1ItemsCount := header.GetChunkDesc(1).GetOffset() - header.GetChunkDesc(0).GetOffset()
		assert.Equal(t, uint32(0), chunk1ItemsCount, "first chunk should be empty")

		// 4. 验证第二个chunk的内容
		chunk2Desc := header.GetChunkDesc(1)
		chunk2 := (*BTPageChunk)(util.PointerAdd(
			unsafe.Pointer(&page[0]),
			int(ShortGetLocation(chunk2Desc.GetShortLocation()))))

		// 验证第二个chunk的items
		chunk2ItemsCount := uint32(header.itemsCount) - header.GetChunkDesc(1).GetOffset()
		assert.Equal(t, uint32(2), chunk2ItemsCount, "second chunk should have 2 items")
		assert.Equal(t, LocationIndex(8), chunk2.GetItem(0), "second chunk first item offset should be correct")
		assert.Equal(t, LocationIndex(16), chunk2.GetItem(1), "second chunk second item offset should be correct")

		// 验证第二个chunk的数据
		itemData := make([]byte, 8)
		util.CMemcpy(
			unsafe.Pointer(&itemData[0]),
			util.PointerAdd(unsafe.Pointer(chunk2), 8),
			8,
		)
		assert.Equal(t, []byte("item1\000\000\000"), itemData, "second chunk first item data should be correct")

		util.CMemcpy(
			unsafe.Pointer(&itemData[0]),
			util.PointerAdd(unsafe.Pointer(chunk2), 16),
			8,
		)
		assert.Equal(t, []byte("item2\000\000\000"), itemData, "second chunk second item data should be correct")
	})

	// 测试在chunk结束位置分割
	t.Run("split chunk at end", func(t *testing.T) {
		// 分配一个页面空间
		page := make([]byte, BLOCK_SIZE)
		header := (*BTPageHeader)(unsafe.Pointer(&page[0]))

		// 初始化页面头部
		header.chunksCount = 1
		header.itemsCount = 2
		header.dataSize = 256 // 初始dataSize

		// 计算chunkDesc区域大小
		chunkDescSize := util.AlignValue(uint32(BT_PAGE_CHUNK_DESC_SIZE)*uint32(header.chunksCount), 8)
		chunkDescOffset := uint32(BT_PAGE_HEADER_CHUNK_DESC_OFFSET)

		// 设置hikey区域，预留足够空间
		hikeySize := LocationIndex(8)
		hikeyStart := chunkDescOffset + chunkDescSize
		header.hikeysEnd = OffsetNumber(hikeyStart + uint32(hikeySize)) // 移除额外的64字节缓冲区

		chunkStartLoc := 136 // 固定chunk起始位置

		// 设置第一个chunk的描述符
		chunk1Desc := header.GetChunkDesc(0)
		chunk1Desc.SetOffset(0)
		chunk1Desc.SetShortLocation(LocationGetShort(uint32(chunkStartLoc)))
		chunk1Desc.SetHikeyShortLocation(LocationGetShort(hikeyStart)) // 设置hikey的起始位置

		// 创建第一个chunk
		chunk1 := (*BTPageChunk)(util.PointerAdd(
			unsafe.Pointer(&page[0]),
			int(ShortGetLocation(chunk1Desc.GetShortLocation()))))

		// 写入测试数据
		chunk1.SetItem(0, LocationIndex(8))  // 第一个item的offset
		chunk1.SetItem(1, LocationIndex(16)) // 第二个item的offset

		// 写入item数据
		util.CMemcpy(
			util.PointerAdd(unsafe.Pointer(chunk1), 8),
			unsafe.Pointer(&[]byte("item1\000\000\000")[0]),
			8,
		)
		util.CMemcpy(
			util.PointerAdd(unsafe.Pointer(chunk1), 16),
			unsafe.Pointer(&[]byte("item2\000\000\000")[0]),
			8,
		)

		// 写入hikey数据
		util.CMemcpy(
			util.PointerAdd(unsafe.Pointer(&page[0]), int(hikeyStart)),
			unsafe.Pointer(&[]byte{1, 2, 3, 4, 0, 0, 0, 0}[0]),
			8,
		)

		// 打印分割前的页面信息
		fmt.Println("Before split:")
		PrintPageInfo(unsafe.Pointer(&page[0]))

		// 创建locator并定位到最后一个item
		locator := &BTPageItemLocator{
			chunkOffset:     0,
			itemOffset:      1, // 定位到最后一个item
			chunkItemsCount: 2,
			chunk:           chunk1,
			chunkSize:       LocationIndex(24), // 2个item,每个8字节
		}

		// 计算hikeyEnd参数,确保满足条件
		chunkDescSize = util.AlignValue(uint32(BT_PAGE_CHUNK_DESC_SIZE)*uint32(header.chunksCount+1), 8)
		// 计算新的hikeyEnd，确保有足够空间容纳新的chunk desc和hikey
		hikeyEnd := LocationIndex(header.hikeysEnd) // 使用当前的hikeysEnd作为基准

		// 执行分割操作
		pageSplitChunk(unsafe.Pointer(&page[0]), locator, hikeyEnd+16, hikeySize)

		// 打印分割后的页面信息
		fmt.Println("After split:")
		PrintPageInfo(unsafe.Pointer(&page[0]))

		// 验证结果
		// 1. 验证chunksCount
		assert.Equal(t, OffsetNumber(2), header.chunksCount, "chunksCount should be 2 after split")

		// 2. 验证itemsCount
		assert.Equal(t, OffsetNumber(2), header.itemsCount, "itemsCount should remain unchanged")

		// 3. 验证第一个chunk的内容
		chunk1 = (*BTPageChunk)(util.PointerAdd(
			unsafe.Pointer(&page[0]),
			int(ShortGetLocation(chunk1Desc.GetShortLocation()))))

		// 验证第一个chunk的items
		chunk1ItemsCount := header.GetChunkDesc(1).GetOffset() - header.GetChunkDesc(0).GetOffset()
		assert.Equal(t, uint32(1), chunk1ItemsCount, "first chunk should have 1 item")

		// 验证第一个chunk的数据
		itemData := make([]byte, 8)
		util.CMemcpy(
			unsafe.Pointer(&itemData[0]),
			util.PointerAdd(unsafe.Pointer(chunk1), 8),
			8,
		)
		assert.Equal(t, []byte("item1\000\000\000"), itemData, "first chunk first item data should be correct")

		// 4. 验证第二个chunk的内容
		chunk2Desc := header.GetChunkDesc(1)
		chunk2 := (*BTPageChunk)(util.PointerAdd(
			unsafe.Pointer(&page[0]),
			int(ShortGetLocation(chunk2Desc.GetShortLocation()))))

		// 验证第二个chunk的items
		chunk2ItemsCount := uint32(header.itemsCount) - header.GetChunkDesc(1).GetOffset()
		assert.Equal(t, uint32(1), chunk2ItemsCount, "second chunk should have 1 item")

		// 验证第二个chunk的数据
		util.CMemcpy(
			unsafe.Pointer(&itemData[0]),
			util.PointerAdd(unsafe.Pointer(chunk2), 8),
			8,
		)
		assert.Equal(t, []byte("item2\000\000\000"), itemData, "second chunk first item data should be correct")
	})

	// 测试将一个chunk分成3个chunk
	t.Run("split chunk into three chunks", func(t *testing.T) {
		// 分配一个页面空间
		page := make([]byte, BLOCK_SIZE)
		header := (*BTPageHeader)(unsafe.Pointer(&page[0]))

		// 初始化页面头部
		header.chunksCount = 1
		header.itemsCount = 3
		header.dataSize = 256 // 初始dataSize

		// 计算chunkDesc区域大小
		chunkDescSize := util.AlignValue(uint32(BT_PAGE_CHUNK_DESC_SIZE)*uint32(header.chunksCount), 8)
		chunkDescOffset := uint32(BT_PAGE_HEADER_CHUNK_DESC_OFFSET)

		// 设置hikey区域，预留足够空间
		hikeySize := LocationIndex(8)
		hikeyStart := chunkDescOffset + chunkDescSize
		header.hikeysEnd = OffsetNumber(hikeyStart + uint32(hikeySize))

		chunkStartLoc := 136 // 固定chunk起始位置

		// 设置第一个chunk的描述符
		chunk1Desc := header.GetChunkDesc(0)
		chunk1Desc.SetOffset(0)
		chunk1Desc.SetShortLocation(LocationGetShort(uint32(chunkStartLoc)))
		chunk1Desc.SetHikeyShortLocation(LocationGetShort(hikeyStart))

		// 创建第一个chunk
		chunk1 := (*BTPageChunk)(util.PointerAdd(
			unsafe.Pointer(&page[0]),
			int(ShortGetLocation(chunk1Desc.GetShortLocation()))))

		// 写入测试数据
		chunk1.SetItem(0, LocationIndex(8))  // 第一个item的offset
		chunk1.SetItem(1, LocationIndex(16)) // 第二个item的offset
		chunk1.SetItem(2, LocationIndex(24)) // 第三个item的offset

		// 写入item数据
		util.CMemcpy(
			util.PointerAdd(unsafe.Pointer(chunk1), 8),
			unsafe.Pointer(&[]byte("item1\000\000\000")[0]),
			8,
		)
		util.CMemcpy(
			util.PointerAdd(unsafe.Pointer(chunk1), 16),
			unsafe.Pointer(&[]byte("item2\000\000\000")[0]),
			8,
		)
		util.CMemcpy(
			util.PointerAdd(unsafe.Pointer(chunk1), 24),
			unsafe.Pointer(&[]byte("item3\000\000\000")[0]),
			8,
		)

		// 写入hikey数据
		util.CMemcpy(
			util.PointerAdd(unsafe.Pointer(&page[0]), int(hikeyStart)),
			unsafe.Pointer(&[]byte{1, 2, 3, 4, 0, 0, 0, 0}[0]),
			8,
		)

		// 打印分割前的页面信息
		fmt.Println("Before split:")
		PrintPageInfo(unsafe.Pointer(&page[0]))

		// 创建定位器
		locator := &BTPageItemLocator{
			chunkOffset:     0,
			itemOffset:      1,
			chunkItemsCount: 3,
			chunk:           chunk1,
			chunkSize:       LocationIndex(32), // 3个item,每个8字节
		}

		// 计算hikeyEnd参数,确保满足条件
		hikeyEnd := LocationIndex(header.hikeysEnd) // 使用当前的hikeysEnd作为基准

		// 执行第一次分割
		pageSplitChunk(unsafe.Pointer(&page[0]), locator, hikeyEnd+16, hikeySize)

		// 打印第一次分割后的页面信息
		fmt.Println("After first split:")
		PrintPageInfo(unsafe.Pointer(&page[0]))

		// 验证第一次分割结果
		assert.Equal(t, OffsetNumber(2), header.chunksCount)
		assert.Equal(t, OffsetNumber(3), header.itemsCount)

		// 获取第一个chunk的信息
		chunk1 = (*BTPageChunk)(util.PointerAdd(
			unsafe.Pointer(&page[0]),
			int(ShortGetLocation(header.GetChunkDesc(0).GetShortLocation()))))

		chunk1ItemsCount := uint32(header.GetChunkDesc(1).GetOffset() - header.GetChunkDesc(0).GetOffset())
		assert.Equal(t, uint32(1), chunk1ItemsCount)

		// 获取第二个chunk的信息
		chunk2 := (*BTPageChunk)(util.PointerAdd(
			unsafe.Pointer(&page[0]),
			int(ShortGetLocation(header.GetChunkDesc(1).GetShortLocation()))))
		chunk2ItemsCount := uint32(OffsetNumber(uint32(header.itemsCount) - uint32(header.GetChunkDesc(1).GetOffset())))
		assert.Equal(t, uint32(2), chunk2ItemsCount)

		// 验证第一个chunk的数据
		item1Data := make([]byte, 8)
		util.CMemcpy(
			unsafe.Pointer(&item1Data[0]),
			util.PointerAdd(unsafe.Pointer(chunk1), int(chunk1.GetItem(0))),
			8,
		)
		assert.Equal(t, []byte("item1\000\000\000"), item1Data)

		// 验证第二个chunk的数据
		item2Data := make([]byte, 8)
		util.CMemcpy(
			unsafe.Pointer(&item2Data[0]),
			util.PointerAdd(unsafe.Pointer(chunk2), int(chunk2.GetItem(0))),
			8,
		)
		assert.Equal(t, []byte("item2\000\000\000"), item2Data)

		item3Data := make([]byte, 8)
		util.CMemcpy(
			unsafe.Pointer(&item3Data[0]),
			util.PointerAdd(unsafe.Pointer(chunk2), int(chunk2.GetItem(1))),
			8,
		)
		assert.Equal(t, []byte("item3\000\000\000"), item3Data)

		// 准备第二次分割
		locator = &BTPageItemLocator{
			chunkOffset:     1,
			itemOffset:      1,
			chunkItemsCount: 2,
			chunk:           chunk2,
			chunkSize:       LocationIndex(24), // 2个item,每个8字节
		}

		// 重新计算hikeyEnd参数
		hikeyEnd = LocationIndex(header.hikeysEnd) // 使用当前的hikeysEnd作为基准

		// 执行第二次分割
		pageSplitChunk(unsafe.Pointer(&page[0]), locator, hikeyEnd+16, hikeySize)

		// 打印第二次分割后的页面信息
		fmt.Println("After second split:")
		PrintPageInfo(unsafe.Pointer(&page[0]))

		// 验证第二次分割结果
		assert.Equal(t, OffsetNumber(3), header.chunksCount)
		assert.Equal(t, OffsetNumber(3), header.itemsCount)

		// 获取三个chunk的信息
		chunk1 = (*BTPageChunk)(util.PointerAdd(
			unsafe.Pointer(&page[0]),
			int(ShortGetLocation(header.GetChunkDesc(0).GetShortLocation()))))
		chunk2 = (*BTPageChunk)(util.PointerAdd(
			unsafe.Pointer(&page[0]),
			int(ShortGetLocation(header.GetChunkDesc(1).GetShortLocation()))))
		chunk3 := (*BTPageChunk)(util.PointerAdd(
			unsafe.Pointer(&page[0]),
			int(ShortGetLocation(header.GetChunkDesc(2).GetShortLocation()))))

		// 验证每个chunk的items数量
		chunk1ItemsCount = uint32(header.GetChunkDesc(1).GetOffset() - header.GetChunkDesc(0).GetOffset())
		chunk2ItemsCount = uint32(header.GetChunkDesc(2).GetOffset() - header.GetChunkDesc(1).GetOffset())
		chunk3ItemsCount := uint32(OffsetNumber(uint32(header.itemsCount) - uint32(header.GetChunkDesc(2).GetOffset())))
		assert.Equal(t, uint32(1), chunk1ItemsCount)
		assert.Equal(t, uint32(1), chunk2ItemsCount)
		assert.Equal(t, uint32(1), chunk3ItemsCount)

		// 验证每个chunk的数据
		util.CMemcpy(
			unsafe.Pointer(&item1Data[0]),
			util.PointerAdd(unsafe.Pointer(chunk1), int(chunk1.GetItem(0))),
			8,
		)
		assert.Equal(t, []byte("item1\000\000\000"), item1Data)

		util.CMemcpy(
			unsafe.Pointer(&item2Data[0]),
			util.PointerAdd(unsafe.Pointer(chunk2), int(chunk2.GetItem(0))),
			8,
		)
		assert.Equal(t, []byte("item2\000\000\000"), item2Data)

		util.CMemcpy(
			unsafe.Pointer(&item3Data[0]),
			util.PointerAdd(unsafe.Pointer(chunk3), int(chunk3.GetItem(0))),
			8,
		)
		assert.Equal(t, []byte("item3\000\000\000"), item3Data)
	})

	// 测试对空chunk进行分割
	t.Run("split empty chunk", func(t *testing.T) {
		// 分配一个页面空间
		page := make([]byte, BLOCK_SIZE)
		header := (*BTPageHeader)(unsafe.Pointer(&page[0]))

		// 初始化页面头部
		header.chunksCount = 1
		header.itemsCount = 0
		header.dataSize = 256 // 初始dataSize

		// 计算chunkDesc区域大小
		chunkDescSize := util.AlignValue(uint32(BT_PAGE_CHUNK_DESC_SIZE)*uint32(header.chunksCount), 8)
		chunkDescOffset := uint32(BT_PAGE_HEADER_CHUNK_DESC_OFFSET)

		// 设置hikey区域，预留足够空间
		hikeySize := LocationIndex(8)
		hikeyStart := chunkDescOffset + chunkDescSize
		header.hikeysEnd = OffsetNumber(hikeyStart + uint32(hikeySize))

		chunkStartLoc := 136 // 固定chunk起始位置

		// 设置第一个chunk的描述符
		chunk1Desc := header.GetChunkDesc(0)
		chunk1Desc.SetOffset(0)
		chunk1Desc.SetShortLocation(LocationGetShort(uint32(chunkStartLoc)))
		chunk1Desc.SetHikeyShortLocation(LocationGetShort(hikeyStart))

		// 创建第一个chunk
		chunk1 := (*BTPageChunk)(util.PointerAdd(
			unsafe.Pointer(&page[0]),
			int(ShortGetLocation(chunk1Desc.GetShortLocation()))))

		// 写入hikey数据
		util.CMemcpy(
			util.PointerAdd(unsafe.Pointer(&page[0]), int(hikeyStart)),
			unsafe.Pointer(&[]byte{1, 2, 3, 4, 0, 0, 0, 0}[0]),
			8,
		)

		// 打印分割前的页面信息
		fmt.Println("Before split:")
		PrintPageInfo(unsafe.Pointer(&page[0]))

		// 创建locator并定位到第一个item
		locator := &BTPageItemLocator{
			chunkOffset:     0,
			itemOffset:      0,
			chunkItemsCount: 0,
			chunk:           chunk1,
			chunkSize:       LocationIndex(0), // 空chunk
		}

		// 计算hikeyEnd参数,确保满足条件
		chunkDescSize = util.AlignValue(uint32(BT_PAGE_CHUNK_DESC_SIZE)*uint32(header.chunksCount+1), 8)
		// 计算新的hikeyEnd，确保有足够空间容纳新的chunk desc和hikey
		hikeyEnd := LocationIndex(header.hikeysEnd) // 使用当前的hikeysEnd作为基准

		// 执行分割操作
		pageSplitChunk(unsafe.Pointer(&page[0]), locator, hikeyEnd+16, hikeySize)

		// 打印分割后的页面信息
		fmt.Println("After split:")
		PrintPageInfo(unsafe.Pointer(&page[0]))

		// 验证结果
		// 1. 验证chunksCount
		assert.Equal(t, OffsetNumber(2), header.chunksCount, "chunksCount should be 2 after split")

		// 2. 验证itemsCount
		assert.Equal(t, OffsetNumber(0), header.itemsCount, "itemsCount should remain 0")

		// 3. 验证第一个chunk的内容
		chunk1 = (*BTPageChunk)(util.PointerAdd(
			unsafe.Pointer(&page[0]),
			int(ShortGetLocation(header.GetChunkDesc(0).GetShortLocation()))))

		// 验证第一个chunk的items
		chunk1ItemsCount := header.GetChunkDesc(1).GetOffset() - header.GetChunkDesc(0).GetOffset()
		assert.Equal(t, uint32(0), chunk1ItemsCount, "first chunk should remain empty")

		// 4. 验证第二个chunk的内容
		chunk2Desc := header.GetChunkDesc(1)

		// 验证第二个chunk的items
		chunk2ItemsCount := uint32(header.itemsCount) - header.GetChunkDesc(1).GetOffset()
		assert.Equal(t, uint32(0), chunk2ItemsCount, "second chunk should also be empty")

		// 验证第二个chunk的起始位置是否正确
		expectedChunk2Loc := chunkStartLoc // chunkDesc + hikey
		assert.Equal(t, LocationGetShort(uint32(expectedChunk2Loc)), chunk2Desc.GetShortLocation(), "chunk2 location should be correct")

		// 5. 验证hikeysEnd
		expectedHikeysEnd := OffsetNumber(chunkDescOffset + chunkDescSize + uint32(hikeySize)*2) // 两个chunk的hikey
		assert.Equal(t, expectedHikeysEnd, header.hikeysEnd, "hikeysEnd should be chunkDescOffset + chunkDescSize + 2*hikeySize")
	})
}

func TestPageSplitChunkIfNeeded(t *testing.T) {
	t.Run("basic split test", func(t *testing.T) {
		// Create a test page
		page := make([]byte, BLOCK_SIZE)
		pagePtr := unsafe.Pointer(&page[0])

		// Initialize page header
		header := (*BTPageHeader)(pagePtr)
		header.chunksCount = 1
		header.itemsCount = 10
		header.maxKeyLen = 16

		MaxhikeysEnd := uint32(BTPageHikeysEnd(nil, pagePtr))

		// 预留 hikey 区域空间
		hikeySize := LocationIndex(16) // 修改为 16 字节
		// 计算 chunk 描述符数组的大小（8字节对齐）
		chunkDescSize := util.AlignValue(uint32(BT_PAGE_CHUNK_DESC_SIZE)*uint32(header.chunksCount), 8)
		// hikey 区域应该在 chunk 描述符数组之后
		header.hikeysEnd = OffsetNumber(BT_PAGE_HEADER_CHUNK_DESC_OFFSET) + OffsetNumber(chunkDescSize) + OffsetNumber(hikeySize)

		// 设置 data 区域大小，确保有足够空间
		header.dataSize = LocationIndex(BT_PAGE_HEADER_SIZE) + LocationIndex(100)

		// Create mock BTreeOps
		mockOps := &MockBTreeOps{
			lenFunc: func(desc *BTDesc, tuple Tuple, lenType LengthType) int {
				return 8 // Mock key length
			},
			tupleMakeKeyFunc: func(desc *BTDesc, tuple Tuple, data unsafe.Pointer, keepVersion bool, allocated *bool) Tuple {
				return Tuple{
					data:        data,
					formatFlags: tuple.formatFlags,
				}
			},
		}

		// Create BTDesc
		desc := &BTDesc{
			ops: mockOps,
		}

		// Create a chunk with some items
		chunkOffset := OffsetNumber(0)
		chunkItemsCount := OffsetNumber(10)
		chunkSize := LocationIndex(1000) // 增大 chunk 大小，使 condLeft 变大

		// Initialize chunk descriptor
		chunkDesc := header.GetChunkDesc(int(chunkOffset))
		chunkDesc.SetOffset(0)
		chunkDesc.SetHikeyFlags(1) // 设置 hikeyFlags

		// 设置 hikey 区域位置 - 在 chunk 描述符之后，8字节对齐
		hikeyAreaStart := util.AlignValue(BT_PAGE_HEADER_CHUNK_DESC_OFFSET+uint32(int(header.chunksCount)*BT_PAGE_CHUNK_DESC_SIZE), 8)
		chunkDesc.SetHikeyShortLocation(LocationGetShort(hikeyAreaStart))

		// 设置 chunk 区域位置
		chunkAreaStart := util.AlignValue(MaxhikeysEnd, 8) // 每个 hikey 16 字节
		chunkDesc.SetShortLocation(LocationGetShort(chunkAreaStart))

		// 初始化 chunk items offset
		chunk := (*BTPageChunk)(util.PointerAdd(pagePtr,
			int(ShortGetLocation(chunkDesc.GetShortLocation()))))
		fmt.Println("\nItem offsets before split:")
		itemStart := LocationIndex(int(util.AlignValue(uint32(LocationIndexSize)*uint32(chunkItemsCount), 8)))
		for i := 0; i < int(chunkItemsCount); i++ {
			// 每个 item 的 offset 从 16 开始，步长为 16
			itemOffset := itemStart + LocationIndex(16*i)
			item := ItemSetFlags(itemOffset, 1)
			chunk.SetItem(i, item)
			item = chunk.GetItem(i)
			fmt.Printf("Item %d item: %d offset: %d flags: %d \n",
				i,
				item,
				ItemGetOffset(item),
				ItemGetFlags(item),
			)
		}

		// 设置数据区域起始位置，8字节对齐
		dataStart := unsafe.Pointer(chunk)
		// 计算每个 item 的实际数据位置
		itemDatas := make([][]byte, int(chunkItemsCount))
		for i := 0; i < int(chunkItemsCount); i++ {
			itemOffset := ItemGetOffset(chunk.GetItem(i))
			// 为每个 item 设置唯一的数据，每个 item 16 字节
			itemData := make([]byte, 16)
			for j := 0; j < 16; j++ {
				itemData[j] = byte(i*10 + j) // 每个 item 的数据从 i*10 开始
			}
			util.CMemcpy(
				util.PointerAdd(dataStart, int(itemOffset)),
				unsafe.Pointer(&itemData[0]),
				16,
			)
			itemDatas[i] = itemData
		}

		// 设置初始 hikey 数据
		initialHikeyData := make([]byte, 16) // hikey 也使用 16 字节
		for i := 0; i < 16; i++ {
			initialHikeyData[i] = byte(100 + i) // 从 100 开始的数据
		}
		util.CMemcpy(
			util.PointerAdd(pagePtr, int(hikeyAreaStart)),
			unsafe.Pointer(&initialHikeyData[0]),
			int(hikeySize),
		)

		// 设置 header 字段
		header.chunksCount = 1
		header.itemsCount = chunkItemsCount
		header.dataSize = LocationIndex(chunkAreaStart) + chunkSize
		header.maxKeyLen = 16 // 设置较小的 maxKeyLen，使 condRight 变小

		// Create locator
		locator := &BTPageItemLocator{
			chunkOffset:     chunkOffset,
			chunkItemsCount: chunkItemsCount,
			chunkSize:       chunkSize,
			itemOffset:      0,
			chunk:           chunk,
		}

		// 调用前打印页面信息
		fmt.Println("Before pageSplitChunkIfNeeded:")
		PrintPageInfo(pagePtr)

		// Test the function
		pageSplitChunkIfNeeded(desc, pagePtr, locator)

		// 调用后打印页面信息
		fmt.Println("After pageSplitChunkIfNeeded:")
		PrintPageInfo(pagePtr)

		// Verify the results
		// 1. Check if the chunk was split
		if header.chunksCount != 2 {
			t.Errorf("Expected chunksCount to be 2, got %d", header.chunksCount)
		}

		// 2. Check if the hikey was set correctly for first chunk
		newChunkDesc := header.GetChunkDesc(int(chunkOffset))
		hikeyLocation := ShortGetLocation(newChunkDesc.GetHikeyShortLocation())
		if hikeyLocation == 0 {
			t.Error("Expected hikey location to be set")
		}

		// 3. Check if the hikey flags were set for first chunk
		hikeyFlags := newChunkDesc.GetHikeyFlags()
		if hikeyFlags == 0 {
			t.Error("Expected hikey flags to be set")
		}

		// 4. Verify the first chunk hikey data was copied correctly
		hikeyData := (*[1 << 30]byte)(util.PointerAdd(pagePtr, int(hikeyLocation)))[:16] // 修改为 16 字节
		for i := 0; i < 8; i++ {
			if hikeyData[i] != itemDatas[8][i+8] {
				t.Errorf("Expected first chunk hikey data[%d] to be %d, got %d", i, 100+i, hikeyData[i])
			}
		}

		// 5. Check the second chunk
		secondChunkDesc := header.GetChunkDesc(int(chunkOffset + 1))
		secondHikeyLocation := ShortGetLocation(secondChunkDesc.GetHikeyShortLocation())
		if secondHikeyLocation == 0 {
			t.Error("Expected second chunk hikey location to be set")
		}

		// 6. Check if the second chunk hikey flags were set
		secondHikeyFlags := secondChunkDesc.GetHikeyFlags()
		if secondHikeyFlags == 0 {
			t.Error("Expected second chunk hikey flags to be set")
		}

		// 7. Verify the second chunk hikey data was copied correctly
		secondHikeyData := (*[1 << 30]byte)(util.PointerAdd(pagePtr, int(secondHikeyLocation)))[:16] // 修改为 16 字节
		for i := 0; i < 8; i++ {
			if secondHikeyData[i] != initialHikeyData[i] { // 第二个 chunk 的 hikey 数据从 200 开始
				t.Errorf("Expected second chunk hikey data[%d] to be %d, got %d", i, 200+i, secondHikeyData[i])
			}
		}

		// 8. Verify the items data in both chunks
		firstChunk := (*BTPageChunk)(util.PointerAdd(pagePtr, int(ShortGetLocation(newChunkDesc.GetShortLocation()))))
		secondChunk := (*BTPageChunk)(util.PointerAdd(pagePtr, int(ShortGetLocation(secondChunkDesc.GetShortLocation()))))

		// 验证第一个 chunk 的 items
		firstChunkItemCount := chunkItemsCount - locator.chunkItemsCount
		firstChunkDataStart := util.PointerAdd(unsafe.Pointer(firstChunk),
			int(util.AlignValue(uint32(LocationIndexSize)*uint32(firstChunkItemCount), 8)))
		for i := 0; i < int(firstChunkItemCount); i++ {
			itemData := make([]byte, 16)
			util.CMemcpy(
				unsafe.Pointer(&itemData[0]),
				util.PointerAdd(firstChunkDataStart, i*16),
				16,
			)
			for j := 0; j < 16; j++ {
				if itemData[j] != byte(i*10+j) {
					t.Errorf("Expected first chunk item[%d] data[%d] to be %d, got %d", i, j, i*10+j, itemData[j])
				}
			}
		}

		// 验证第二个 chunk 的 items
		secondChunkItemCount := locator.chunkItemsCount
		secondChunkDataStart := util.PointerAdd(unsafe.Pointer(secondChunk),
			int(util.AlignValue(uint32(LocationIndexSize)*uint32(secondChunkItemCount), 8)))
		for i := 0; i < int(secondChunkItemCount); i++ {
			itemData := make([]byte, 16)
			util.CMemcpy(
				unsafe.Pointer(&itemData[0]),
				util.PointerAdd(secondChunkDataStart, i*16),
				16,
			)
			for j := 0; j < 16; j++ {
				if itemData[j] != byte((i+int(firstChunkItemCount))*10+j) {
					t.Errorf("Expected second chunk item[%d] data[%d] to be %d, got %d", i, j, (i+int(chunkItemsCount/2))*10+j, itemData[j])
				}
			}
		}

		// 打印调试信息
		fmt.Printf("First chunk hikey location: %d\n", hikeyLocation)
		fmt.Printf("Second chunk hikey location: %d\n", secondHikeyLocation)
		fmt.Printf("First chunk data start: %d\n", uintptr(firstChunkDataStart)-uintptr(pagePtr))
		fmt.Printf("Second chunk data start: %d\n", uintptr(secondChunkDataStart)-uintptr(pagePtr))

		// 验证第二个 chunk 的 items
		secondChunk = (*BTPageChunk)(util.PointerAdd(pagePtr, int(ShortGetLocation(secondChunkDesc.GetShortLocation()))))
		fmt.Println("\nItem offsets after split:")
		fmt.Println("First chunk items:")
		for i := 0; i < int(chunkItemsCount/2); i++ {
			fmt.Printf("Item %d offset: %d\n", i, firstChunk.GetItem(i))
		}
		fmt.Println("Second chunk items:")
		for i := 0; i < int(chunkItemsCount/2); i++ {
			fmt.Printf("Item %d offset: %d\n", i, secondChunk.GetItem(i))
		}
	})

	t.Run("split test with different layout", func(t *testing.T) {
		// Create a test page
		page := make([]byte, BLOCK_SIZE)
		pagePtr := unsafe.Pointer(&page[0])

		// Initialize page header
		header := (*BTPageHeader)(pagePtr)
		header.chunksCount = 1
		header.itemsCount = 8 // 使用 8 个 items
		header.maxKeyLen = 32 // 使用更大的 maxKeyLen

		MaxhikeysEnd := uint32(BTPageHikeysEnd(nil, pagePtr))

		// 预留 hikey 区域空间
		hikeySize := LocationIndex(32) // 使用 32 字节的 hikey
		chunkDescSize := util.AlignValue(uint32(BT_PAGE_CHUNK_DESC_SIZE)*uint32(header.chunksCount), 8)
		header.hikeysEnd = OffsetNumber(BT_PAGE_HEADER_CHUNK_DESC_OFFSET) + OffsetNumber(chunkDescSize) + OffsetNumber(hikeySize)

		// 设置 data 区域大小
		header.dataSize = LocationIndex(BT_PAGE_HEADER_SIZE) + LocationIndex(200)

		// Create mock BTreeOps
		mockOps := &MockBTreeOps{
			lenFunc: func(desc *BTDesc, tuple Tuple, lenType LengthType) int {
				return 16 // 使用更大的 key length
			},
			tupleMakeKeyFunc: func(desc *BTDesc, tuple Tuple, data unsafe.Pointer, keepVersion bool, allocated *bool) Tuple {
				return Tuple{
					data:        data,
					formatFlags: tuple.formatFlags,
				}
			},
		}

		desc := &BTDesc{
			ops: mockOps,
		}

		// Create a chunk with some items
		chunkOffset := OffsetNumber(0)
		chunkItemsCount := OffsetNumber(8)
		chunkSize := LocationIndex(1200) // 减小 chunk 大小，使 condLeft 变小

		// Initialize chunk descriptor
		chunkDesc := header.GetChunkDesc(int(chunkOffset))
		chunkDesc.SetOffset(0)
		chunkDesc.SetHikeyFlags(1)

		// 设置 hikey 区域位置
		hikeyAreaStart := util.AlignValue(BT_PAGE_HEADER_CHUNK_DESC_OFFSET+uint32(int(header.chunksCount)*BT_PAGE_CHUNK_DESC_SIZE), 8)
		chunkDesc.SetHikeyShortLocation(LocationGetShort(hikeyAreaStart))

		// 设置 chunk 区域位置
		chunkAreaStart := util.AlignValue(MaxhikeysEnd, 8)
		chunkDesc.SetShortLocation(LocationGetShort(chunkAreaStart))

		// 初始化 chunk items offset
		chunk := (*BTPageChunk)(util.PointerAdd(pagePtr, int(ShortGetLocation(chunkDesc.GetShortLocation()))))
		fmt.Println("\nItem offsets before split:")
		itemStart := LocationIndex(int(util.AlignValue(uint32(LocationIndexSize)*uint32(chunkItemsCount), 8)))
		for i := 0; i < int(chunkItemsCount); i++ {
			// 每个 item 的 offset 从 itemStart 开始，步长为 32
			itemOffset := itemStart + LocationIndex(32*i)
			item := ItemSetFlags(itemOffset, 1)
			chunk.SetItem(i, item)
			item = chunk.GetItem(i)
			fmt.Printf("Item %d item: %d offset: %d flags: %d \n",
				i,
				item,
				ItemGetOffset(item),
				ItemGetFlags(item),
			)
		}

		// 设置数据区域
		dataStart := unsafe.Pointer(chunk)
		itemDatas := make([][]byte, int(chunkItemsCount))
		for i := 0; i < int(chunkItemsCount); i++ {
			itemOffset := ItemGetOffset(chunk.GetItem(i))
			// 为每个 item 设置唯一的数据，每个 item 32 字节
			itemData := make([]byte, 32)
			for j := 0; j < 32; j++ {
				itemData[j] = byte(i*20 + j) // 使用不同的数据模式
			}
			util.CMemcpy(
				util.PointerAdd(dataStart, int(itemOffset)),
				unsafe.Pointer(&itemData[0]),
				32,
			)
			itemDatas[i] = itemData
		}

		// 设置初始 hikey 数据
		initialHikeyData := make([]byte, 32)
		for i := 0; i < 32; i++ {
			initialHikeyData[i] = byte(200 + i)
		}
		util.CMemcpy(
			util.PointerAdd(pagePtr, int(hikeyAreaStart)),
			unsafe.Pointer(&initialHikeyData[0]),
			int(hikeySize),
		)

		// 设置 header 字段
		header.chunksCount = 1
		header.itemsCount = chunkItemsCount
		header.dataSize = LocationIndex(chunkAreaStart) + chunkSize
		header.maxKeyLen = 32 // 设置较小的 maxKeyLen，使 condRight 变小

		// Create locator
		locator := &BTPageItemLocator{
			chunkOffset:     chunkOffset,
			chunkItemsCount: chunkItemsCount,
			chunkSize:       chunkSize,
			itemOffset:      0,
			chunk:           chunk,
		}

		// 调用前打印页面信息
		fmt.Println("Before pageSplitChunkIfNeeded:")
		PrintPageInfo(pagePtr)

		// Test the function
		pageSplitChunkIfNeeded(desc, pagePtr, locator)

		// 调用后打印页面信息
		fmt.Println("After pageSplitChunkIfNeeded:")
		PrintPageInfo(pagePtr)

		// 验证结果
		if header.chunksCount != 2 {
			t.Errorf("Expected chunksCount to be 2, got %d", header.chunksCount)
		}

		// 验证第一个 chunk
		newChunkDesc := header.GetChunkDesc(int(chunkOffset))
		hikeyLocation := ShortGetLocation(newChunkDesc.GetHikeyShortLocation())
		if hikeyLocation == 0 {
			t.Error("Expected hikey location to be set")
		}

		// 验证第一个 chunk 的 hikey 数据
		hikeyData := (*[1 << 30]byte)(util.PointerAdd(pagePtr, int(hikeyLocation)))[:32]
		for i := 0; i < 16; i++ {
			if hikeyData[i] != itemDatas[6][i+8] {
				t.Errorf("Expected first chunk hikey data[%d] to be %d, got %d", i, itemDatas[6][i+16], hikeyData[i])
			}
		}

		// 验证第二个 chunk
		secondChunkDesc := header.GetChunkDesc(int(chunkOffset + 1))
		secondHikeyLocation := ShortGetLocation(secondChunkDesc.GetHikeyShortLocation())
		if secondHikeyLocation == 0 {
			t.Error("Expected second chunk hikey location to be set")
		}

		// 验证第二个 chunk 的 hikey 数据
		secondHikeyData := (*[1 << 30]byte)(util.PointerAdd(pagePtr, int(secondHikeyLocation)))[:32]
		for i := 0; i < 32; i++ {
			if secondHikeyData[i] != initialHikeyData[i] {
				t.Errorf("Expected second chunk hikey data[%d] to be %d, got %d", i, initialHikeyData[i], secondHikeyData[i])
			}
		}

		// 验证 items 数据
		firstChunk := (*BTPageChunk)(util.PointerAdd(pagePtr, int(ShortGetLocation(newChunkDesc.GetShortLocation()))))
		secondChunk := (*BTPageChunk)(util.PointerAdd(pagePtr, int(ShortGetLocation(secondChunkDesc.GetShortLocation()))))

		// 验证第一个 chunk 的 items
		firstChunkItemCount := chunkItemsCount - locator.chunkItemsCount
		firstChunkDataStart := util.PointerAdd(unsafe.Pointer(firstChunk),
			int(util.AlignValue(uint32(LocationIndexSize)*uint32(firstChunkItemCount), 8)))
		for i := 0; i < int(firstChunkItemCount); i++ {
			itemData := make([]byte, 32)
			util.CMemcpy(
				unsafe.Pointer(&itemData[0]),
				util.PointerAdd(firstChunkDataStart, i*32),
				32,
			)
			for j := 0; j < 32; j++ {
				if itemData[j] != byte(i*20+j) {
					t.Errorf("Expected first chunk item[%d] data[%d] to be %d, got %d", i, j, i*20+j, itemData[j])
				}
			}
		}

		// 验证第二个 chunk 的 items
		secondChunkItemCount := locator.chunkItemsCount
		secondChunkDataStart := util.PointerAdd(unsafe.Pointer(secondChunk),
			int(util.AlignValue(uint32(LocationIndexSize)*uint32(secondChunkItemCount), 8)))
		for i := 0; i < int(secondChunkItemCount); i++ {
			itemData := make([]byte, 32)
			util.CMemcpy(
				unsafe.Pointer(&itemData[0]),
				util.PointerAdd(secondChunkDataStart, i*32),
				32,
			)
			for j := 0; j < 32; j++ {
				if itemData[j] != byte((i+int(firstChunkItemCount))*20+j) {
					t.Errorf("Expected second chunk item[%d] data[%d] to be %d, got %d", i, j, (i+int(firstChunkItemCount))*20+j, itemData[j])
				}
			}
		}
	})
}

func TestItemGetKeySize(t *testing.T) {
	t.Run("leaf page item", func(t *testing.T) {
		// 创建 mock BTreeOps
		mockOps := &MockBTreeOps{
			lenFunc: func(desc *BTDesc, tuple Tuple, lenType LengthType) int {
				return 16 // 模拟 key 长度为 16
			},
		}

		// 创建 BTDesc
		desc := &BTDesc{
			ops: mockOps,
		}

		// 创建 item 数据
		itemData := make([]byte, 32)
		for i := 0; i < 32; i++ {
			itemData[i] = byte(i)
		}

		// 测试 newItem 为 true 的情况
		item := &BTPageItem{
			data:    unsafe.Pointer(&itemData[0]),
			flags:   1,
			newItem: true,
		}

		// 验证 newItem 为 true 时的 key size
		keySize := itemGetKeySize(desc, true, item)
		expectedSize := LocationIndex(util.AlignValue(16, 8)) // 16 字节对齐到 8
		if keySize != expectedSize {
			t.Errorf("Expected key size %d for new item, got %d", expectedSize, keySize)
		}

		// 测试 newItem 为 false 的情况
		item.newItem = false
		keySize = itemGetKeySize(desc, true, item)
		expectedSize = LocationIndex(util.AlignValue(16, 8)) // 16 字节对齐到 8
		if keySize != expectedSize {
			t.Errorf("Expected key size %d for existing item, got %d", expectedSize, keySize)
		}
	})

	t.Run("non-leaf page item", func(t *testing.T) {
		// 创建 mock BTreeOps
		mockOps := &MockBTreeOps{
			lenFunc: func(desc *BTDesc, tuple Tuple, lenType LengthType) int {
				return 24 // 模拟 key 长度为 24
			},
		}

		// 创建 BTDesc
		desc := &BTDesc{
			ops: mockOps,
		}

		// 创建 item 数据
		itemData := make([]byte, 32)
		for i := 0; i < 32; i++ {
			itemData[i] = byte(i)
		}

		// 测试 non-leaf item
		item := &BTPageItem{
			data:    unsafe.Pointer(&itemData[0]),
			flags:   1,
			newItem: false, // non-leaf 必须是 false
		}

		// 验证 non-leaf item 的 key size
		keySize := itemGetKeySize(desc, false, item)
		expectedSize := LocationIndex(util.AlignValue(24, 8)) // 24 字节对齐到 8
		if keySize != expectedSize {
			t.Errorf("Expected key size %d for non-leaf item, got %d", expectedSize, keySize)
		}

		// 验证 newItem 为 true 时会 panic
		item.newItem = true
		defer func() {
			if r := recover(); r == nil {
				t.Error("Expected panic for non-leaf item with newItem=true")
			}
		}()
		itemGetKeySize(desc, false, item)
	})
}

// 添加辅助函数
func page_get_hikey(page unsafe.Pointer, chunkIndex int) Tuple {
	header := (*BTPageHeader)(page)
	chunkDesc := header.GetChunkDesc(chunkIndex)
	return Tuple{
		formatFlags: uint8(chunkDesc.GetHikeyFlags()),
		data: unsafe.Pointer(uintptr(page) +
			uintptr(ShortGetLocation(chunkDesc.GetHikeyShortLocation()))),
	}
}

func TestBTPageReorg(t *testing.T) {
	t.Run("basic split", func(t *testing.T) {
		// 创建测试页面
		page := make([]byte, BLOCK_SIZE)
		pagePtr := unsafe.Pointer(&page[0])

		// 构造测试数据：只有一个chunk
		itemCount := 8
		itemSize := 32
		items := make([]struct{ Data []byte }, itemCount)
		itemDataList := make([][]byte, itemCount)
		headLen := int(BT_LEAF_TUPHDR_SIZE)
		for i := 0; i < itemCount; i++ {
			itemData := make([]byte, itemSize)
			// 填充 head 区域
			for j := 0; j < headLen; j++ {
				itemData[j] = 0xA0 + byte(i)
			}
			// 填充 payload 区域
			for j := 0; j < itemSize-headLen; j++ {
				itemData[headLen+j] = 0xC0 + byte(i*itemSize+j)
			}
			items[i] = struct{ Data []byte }{Data: itemData}
			itemDataList[i] = itemData
		}
		lasthikey := make([]byte, itemSize-headLen)
		for i := 0; i < itemSize-headLen; i++ {
			lasthikey[i] = byte(200 + i)
		}
		chunksData := []ChunkData{
			{Items: items},
		}

		// 初始化页面
		SetupPageWithChunksV2(pagePtr, chunksData, BTREE_FLAG_LEAF, lasthikey)
		header := (*BTPageHeader)(pagePtr)

		// 验证hikey内容
		chunkDesc := header.GetChunkDesc(0)
		hikeyLoc := ShortGetLocation(chunkDesc.GetHikeyShortLocation())
		hikeyData := util.PointerToSlice[byte](util.PointerAdd(pagePtr, int(hikeyLoc)), itemSize-headLen)
		assert.Equal(t, lasthikey, hikeyData[:len(lasthikey)])
		fmt.Printf("hikeyData: % x\n", hikeyData)

		// 验证chunk和item
		assert.Equal(t, OffsetNumber(1), header.chunksCount)
		assert.Equal(t, OffsetNumber(itemCount), header.itemsCount)
		chunk := (*BTPageChunk)(util.PointerAdd(pagePtr, int(ShortGetLocation(chunkDesc.GetShortLocation()))))
		for i := 0; i < itemCount; i++ {
			item := chunk.GetItem(i)
			itemLoc := ItemGetOffset(item)
			itemData := (*[32]byte)(util.PointerAdd(unsafe.Pointer(chunk), int(itemLoc)))[:]
			assert.Equal(t, itemDataList[i][headLen:], itemData[headLen:])
			// flags
			assert.Equal(t, uint16(0), ItemGetFlags(item))
		}

		fmt.Println("before split")
		PrintPageInfo(pagePtr)

		// 构造 desc
		mockOps := &MockBTreeOps{
			lenFunc: func(desc *BTDesc, tuple Tuple, lenType LengthType) int {
				return 16
			},
			tupleMakeKeyFunc: func(desc *BTDesc, tuple Tuple, data unsafe.Pointer, keepVersion bool, allocated *bool) Tuple {
				util.PointerCopy(data, tuple.data, itemSize-headLen)
				return Tuple{
					data:        data,
					formatFlags: tuple.formatFlags,
				}
			},
		}
		desc := &BTDesc{ops: mockOps}

		// 构造 pageItems 数组
		var pageItems [BT_PAGE_MAX_CHUNK_ITEMS]BTPageItem
		for i := 0; i < itemCount; i++ {
			item := chunk.GetItem(i)
			itemLoc := ItemGetOffset(item)
			pageItems[i] = BTPageItem{
				data:    util.PointerAdd(unsafe.Pointer(chunk), int(itemLoc)),
				flags:   uint8(ItemGetFlags(item)),
				size:    LocationIndex(itemSize),
				newItem: false,
			}
		}

		// hikey
		lasthikey2 := make([]byte, itemSize-headLen)
		for i := 0; i < itemSize-headLen; i++ {
			lasthikey2[i] = byte(300 + i)
		}
		hikey := Tuple{
			data:        unsafe.Pointer(&lasthikey2[0]),
			formatFlags: 1,
		}

		hikeyData2 := util.PointerToSlice[byte](hikey.data, itemSize-headLen)
		fmt.Printf("hikeyData2: % x\n", hikeyData2)
		// 执行重组
		var newLoc BTPageItemLocator
		BTPageReorg(desc, pagePtr, pageItems, OffsetNumber(itemCount), LocationIndex(itemSize-headLen), hikey, &newLoc)

		fmt.Println("after split")
		PrintPageInfo(pagePtr)

		// 验证
		if header.chunksCount <= 1 {
			t.Errorf("Expected chunks count > 1, got %d", header.chunksCount)
		}

		// 验证每个 chunk 的 items 数量和值
		totalItems := 0
		itemIndex := 0
		for i := 0; i < int(header.chunksCount); i++ {
			chunkDesc := header.GetChunkDesc(i)
			chunkItemsCount := OffsetNumber(0)
			if i < int(header.chunksCount)-1 {
				nextChunkDesc := header.GetChunkDesc(i + 1)
				chunkItemsCount = OffsetNumber(nextChunkDesc.GetOffset() - chunkDesc.GetOffset())
			} else {
				chunkItemsCount = OffsetNumber(uint32(header.itemsCount) - chunkDesc.GetOffset())
			}
			totalItems += int(chunkItemsCount)

			// 验证 chunk 的 hikey
			if i < int(header.chunksCount)-1 {
				hikeyLoc := ShortGetLocation(chunkDesc.GetHikeyShortLocation())
				if hikeyLoc == 0 {
					t.Errorf("Chunk %d has invalid hikey location", i)
				}

				// 验证 hikey flags - 非最后一个 chunk 的 hikey flags 应该来自 item flags
				if chunkDesc.GetHikeyFlags() != 0 {
					t.Errorf("Chunk %d hikey flags = %d, want 1", i, chunkDesc.GetHikeyFlags())
				}

				// 获取并验证 hikey 值
				chunkHikey := page_get_hikey(pagePtr, i)
				if chunkHikey.data == nil {
					t.Errorf("Chunk %d has nil hikey data", i)
				} else {
					// 验证 hikey 值
					nextChunkDesc := header.GetChunkDesc(i + 1)
					nextChunk := (*BTPageChunk)(util.PointerAdd(pagePtr, int(ShortGetLocation(nextChunkDesc.GetShortLocation()))))
					nextItem := nextChunk.GetItem(0)
					nextItemLoc := ItemGetOffset(nextItem)
					headLen := int(BT_LEAF_TUPHDR_SIZE)
					itemData := (*[32]byte)(util.PointerAdd(unsafe.Pointer(nextChunk), int(nextItemLoc)))[:]
					expectedHikey := itemData[headLen:itemSize]
					hikeyBytes := (*[32]byte)(chunkHikey.data)[:itemSize-headLen]
					for j := 0; j < itemSize-headLen; j++ {
						if hikeyBytes[j] != expectedHikey[j] {
							t.Errorf("Chunk %d hikey[%d] = %d, want %d", i, j, hikeyBytes[j], expectedHikey[j])
						}
					}
				}
			}

			// 验证 chunk 中的 items
			chunk := (*BTPageChunk)(util.PointerAdd(pagePtr, int(ShortGetLocation(chunkDesc.GetShortLocation()))))
			for j := 0; j < int(chunkItemsCount); j++ {
				item := chunk.GetItem(j)
				itemLoc := ItemGetOffset(item)
				itemData := (*[32]byte)(util.PointerAdd(unsafe.Pointer(chunk), int(itemLoc)))[:]

				// 验证 item 值
				expectedData := itemDataList[itemIndex]
				for k := 0; k < 32; k++ {
					if itemData[k] != expectedData[k] {
						t.Errorf("Item %d[%d] = %d, want %d", itemIndex, k, itemData[k], expectedData[k])
					}
				}

				// 验证 item flags 和 size
				if pageItems[itemIndex].flags != 0 {
					t.Errorf("Item %d flags = %d, want 1", itemIndex, pageItems[itemIndex].flags)
				}
				if pageItems[itemIndex].size != 32 {
					t.Errorf("Item %d size = %d, want 32", itemIndex, pageItems[itemIndex].size)
				}

				itemIndex++
			}
		}

		if totalItems != 8 {
			t.Errorf("Expected total items 8, got %d", totalItems)
		}

		// 验证最后一个 chunk 的 hikey - 非 rightmost page 的最后一个 chunk 的 hikey 来自参数
		lastChunkDesc := header.GetChunkDesc(int(header.chunksCount - 1))
		if lastChunkDesc.GetHikeyFlags() != 1 {
			t.Errorf("Expected last chunk hikey flags 1, got %d", lastChunkDesc.GetHikeyFlags())
		}

		// 验证最后一个chunk的hikey等于输入的lasthikey
		lastChunkIdx := int(header.chunksCount - 1)
		hikeyLoc = ShortGetLocation(lastChunkDesc.GetHikeyShortLocation())
		if hikeyLoc == 0 {
			t.Errorf("Last chunk has invalid hikey location")
		}
		chunkHikey := page_get_hikey(pagePtr, lastChunkIdx)
		if chunkHikey.data == nil {
			t.Errorf("Last chunk has nil hikey data")
		} else {
			hikeyBytes := (*[32]byte)(chunkHikey.data)[:itemSize-headLen]
			for j := 0; j < itemSize-headLen; j++ {
				if hikeyBytes[j] != lasthikey2[j] {
					t.Errorf("Last chunk hikey[%d] = %d, want %d", j, hikeyBytes[j], lasthikey2[j])
				}
			}
		}
	})

	t.Run("rightmost page", func(t *testing.T) {
		// 创建测试数据
		page := make([]byte, BLOCK_SIZE)
		pagePtr := unsafe.Pointer(&page[0])

		// 创建 mock BTreeOps
		mockOps := &MockBTreeOps{
			lenFunc: func(desc *BTDesc, tuple Tuple, lenType LengthType) int {
				return 16
			},
			tupleMakeKeyFunc: func(desc *BTDesc, tuple Tuple, data unsafe.Pointer, keepVersion bool, allocated *bool) Tuple {
				// 如果 data 不为空，直接使用 data
				if data != nil {
					return Tuple{
						data:        data,
						formatFlags: tuple.formatFlags,
					}
				}
				// 否则创建新的数据
				newData := make([]byte, 16)
				copy(newData, (*[16]byte)(tuple.data)[:])
				*allocated = true
				return Tuple{
					data:        unsafe.Pointer(&newData[0]),
					formatFlags: tuple.formatFlags,
				}
			},
		}

		// 创建 BTDesc
		desc := &BTDesc{
			ops: mockOps,
		}

		// 初始化页面
		header := (*BTPageHeader)(pagePtr)
		header.SetFlags(BTREE_FLAG_LEAF | BTREE_FLAG_RIGHTMOST)
		header.chunksCount = 1
		header.itemsCount = 8
		header.dataSize = 400

		// 创建测试 items
		var items [BT_PAGE_MAX_CHUNK_ITEMS]BTPageItem
		itemDataList := make([][]byte, 8) // 保存原始数据用于后续验证
		for i := 0; i < 8; i++ {
			itemData := make([]byte, 32)
			for j := 0; j < 32; j++ {
				itemData[j] = byte(i*20 + j)
			}
			itemDataList[i] = itemData
			items[i] = BTPageItem{
				data:    unsafe.Pointer(&itemData[0]),
				flags:   1,
				size:    LocationIndex(32),
				newItem: false,
			}
		}

		// 创建 hikey - rightmost page 的 hikey 应该是空的
		hikey := Tuple{
			data:        nil,
			formatFlags: 0,
		}

		// 执行重组
		var newLoc BTPageItemLocator
		BTPageReorg(desc, pagePtr, items, 8, 0, hikey, &newLoc)

		// 验证结果
		if header.chunksCount <= 1 {
			t.Errorf("Expected chunks count > 1, got %d", header.chunksCount)
		}

		// 验证每个 chunk 的 items 数量和值
		totalItems := 0
		itemIndex := 0
		for i := 0; i < int(header.chunksCount); i++ {
			chunkDesc := header.GetChunkDesc(i)
			chunkItemsCount := OffsetNumber(0)
			if i < int(header.chunksCount)-1 {
				nextChunkDesc := header.GetChunkDesc(i + 1)
				chunkItemsCount = OffsetNumber(nextChunkDesc.GetOffset() - chunkDesc.GetOffset())
			} else {
				chunkItemsCount = OffsetNumber(uint32(header.itemsCount) - chunkDesc.GetOffset())
			}
			totalItems += int(chunkItemsCount)

			// 验证 chunk 的 hikey
			if i < int(header.chunksCount)-1 {
				hikeyLoc := ShortGetLocation(chunkDesc.GetHikeyShortLocation())
				if hikeyLoc == 0 {
					t.Errorf("Chunk %d has invalid hikey location", i)
				}

				// 验证 hikey flags - 非最后一个 chunk 的 hikey flags 应该来自 item flags
				if chunkDesc.GetHikeyFlags() != 1 {
					t.Errorf("Chunk %d hikey flags = %d, want 1", i, chunkDesc.GetHikeyFlags())
				}
			}

			// 验证 chunk 中的 items
			chunk := (*BTPageChunk)(util.PointerAdd(pagePtr, int(ShortGetLocation(chunkDesc.GetShortLocation()))))
			for j := 0; j < int(chunkItemsCount); j++ {
				item := chunk.GetItem(j)
				itemLoc := ItemGetOffset(item)
				itemData := (*[32]byte)(util.PointerAdd(unsafe.Pointer(chunk), int(itemLoc)))[:]

				// 验证 item 值
				expectedData := itemDataList[itemIndex]
				for k := 0; k < 32; k++ {
					if itemData[k] != expectedData[k] {
						t.Errorf("Item %d[%d] = %d, want %d", itemIndex, k, itemData[k], expectedData[k])
					}
				}

				// 验证 item flags 和 size
				pageItem := items[itemIndex]
				if pageItem.flags != 1 {
					t.Errorf("Item %d flags = %d, want 1", itemIndex, pageItem.flags)
				}
				if pageItem.size != 32 {
					t.Errorf("Item %d size = %d, want 32", itemIndex, pageItem.size)
				}

				itemIndex++
			}
		}

		if totalItems != 8 {
			t.Errorf("Expected total items 8, got %d", totalItems)
		}

		// 验证最后一个 chunk 的 hikey - rightmost page 的最后一个 chunk 的 hikey flags 应该是 0
		lastChunkDesc := header.GetChunkDesc(int(header.chunksCount - 1))
		if lastChunkDesc.GetHikeyFlags() != 0 {
			t.Errorf("Expected last chunk hikey flags 0, got %d", lastChunkDesc.GetHikeyFlags())
		}
	})
}

func TestBTPageReorgBasic(t *testing.T) {
	headLen := int(BT_LEAF_TUPHDR_SIZE)
	itemCount := 8
	itemSize := 32
	page := make([]byte, BLOCK_SIZE)
	pagePtr := unsafe.Pointer(&page[0])

	// 构造 items
	items := make([]struct{ Data []byte }, itemCount)
	for i := 0; i < itemCount; i++ {
		itemData := make([]byte, itemSize)
		for j := 0; j < headLen; j++ {
			itemData[j] = 0xA0 + byte(i)
		}
		for j := 0; j < itemSize-headLen; j++ {
			itemData[headLen+j] = 0xC0 + byte(i*(itemSize-headLen)+j)
		}
		items[i] = struct{ Data []byte }{Data: itemData}
	}

	// 构造 lasthikey
	lasthikey := make([]byte, itemSize-headLen)
	for i := 0; i < itemSize-headLen; i++ {
		lasthikey[i] = byte(200 + i)
	}

	// 构造 pageItems
	var pageItems [BT_PAGE_MAX_CHUNK_ITEMS]BTPageItem
	header := (*BTPageHeader)(pagePtr)
	header.SetFlags(BTREE_FLAG_LEAF)
	for i := 0; i < itemCount; i++ {
		item := items[i].Data
		pageItems[i] = BTPageItem{
			data:    unsafe.Pointer(&item[0]),
			flags:   1,
			size:    LocationIndex(itemSize),
			newItem: false,
		}
	}

	// 构造 desc
	mockOps := &MockBTreeOps{
		lenFunc: func(desc *BTDesc, tuple Tuple, lenType LengthType) int {
			return int(itemSize - headLen)
		},
		tupleMakeKeyFunc: func(desc *BTDesc, tuple Tuple, data unsafe.Pointer, keepVersion bool, allocated *bool) Tuple {
			return Tuple{
				data:        data,
				formatFlags: tuple.formatFlags,
			}
		},
	}
	desc := &BTDesc{ops: mockOps}

	// 构造 hikey
	hikey := Tuple{
		data:        unsafe.Pointer(&lasthikey[0]),
		formatFlags: 1,
	}

	// 执行重组
	var newLoc BTPageItemLocator
	BTPageReorg(desc, pagePtr, pageItems, OffsetNumber(itemCount), LocationIndex(itemSize-headLen), hikey, &newLoc)

	header = (*BTPageHeader)(pagePtr)
	if header.chunksCount <= 1 {
		t.Errorf("Expected chunks count > 1, got %d", header.chunksCount)
	}

	// 验证最后一个chunk的hikey等于lasthikey
	lastChunkIdx := int(header.chunksCount - 1)
	lastChunkDesc := header.GetChunkDesc(lastChunkIdx)
	hikeyLoc := ShortGetLocation(lastChunkDesc.GetHikeyShortLocation())
	if hikeyLoc == 0 {
		t.Errorf("Last chunk has invalid hikey location")
	}
	chunkHikey := page_get_hikey(pagePtr, lastChunkIdx)
	if chunkHikey.data == nil {
		t.Errorf("Last chunk has nil hikey data")
	} else {
		hikeyBytes := (*[32]byte)(chunkHikey.data)[:itemSize-headLen]
		for j := 0; j < itemSize-headLen; j++ {
			if hikeyBytes[j] != lasthikey[j] {
				t.Errorf("Last chunk hikey[%d] = %d, want %d", j, hikeyBytes[j], lasthikey[j])
			}
		}
	}
}

func TestPartialLoadChunk(t *testing.T) {
	// 创建测试用的页面和描述符
	InitPages(2) // 需要两个页面：一个作为源页面，一个作为目标页面
	defer ClosePages()

	srcPage := GetInMemPage(0)
	dstPage := GetInMemPage(1)

	// 准备源页面测试数据
	srcChunksData := []ChunkData{
		{
			Items: []struct {
				Data []byte
			}{
				{Data: []byte("src_chunk0_item1_data_that_is_longer_than_header")},
				{Data: []byte("src_chunk0_item2_data_that_is_longer_than_header")},
			},
		},
		{
			Items: []struct {
				Data []byte
			}{
				{Data: []byte("src_chunk1_item1_data_that_is_longer_than_header")},
				{Data: []byte("src_chunk1_item2_data_that_is_longer_than_header")},
			},
		},
	}

	// 设置源页面
	SetupPageWithChunksV2(srcPage, srcChunksData, BTREE_FLAG_LEAF, []byte("src_last_hikey_data_that_is_longer_than_header"))

	// 准备目标页面测试数据（使用不同的数据）
	dstChunksData := []ChunkData{
		{
			Items: []struct {
				Data []byte
			}{
				{Data: []byte("dst_chunk0_item1_data_that_is_longer_than_header")},
				{Data: []byte("dst_chunk0_item2_data_that_is_longer_than_header")},
			},
		},
		{
			Items: []struct {
				Data []byte
			}{
				{Data: []byte("dst_chunk1_item1_data_that_is_longer_than_header")},
				{Data: []byte("dst_chunk1_item2_data_that_is_longer_than_header")},
			},
		},
	}

	// 设置目标页面
	SetupPageWithChunksV2(dstPage, dstChunksData, BTREE_FLAG_LEAF, []byte("dst_last_hikey_data_that_is_longer_than_header"))

	// 创建PartialPageState
	partial := &PartialPageState{
		src:           srcPage,
		isPartial:     true,
		chunkIsLoaded: [BT_PAGE_MAX_CHUNKS]bool{},
	}

	t.Run("already loaded chunk", func(t *testing.T) {
		// 重置状态
		partial.isPartial = true
		for i := range partial.chunkIsLoaded {
			partial.chunkIsLoaded[i] = false
		}

		// 设置已加载状态
		partial.chunkIsLoaded[0] = true

		// 执行测试
		got := partialLoadChunk(partial, dstPage, 0)

		// 验证结果
		assert.True(t, got)
		// 验证数据没有被改变（因为chunk已经加载）
		dstHeader := (*BTPageHeader)(dstPage)
		chunkBegin := LocationIndex(ShortGetLocation(dstHeader.GetChunkDesc(0).GetShortLocation()))
		chunkEnd := LocationIndex(ShortGetLocation(dstHeader.GetChunkDesc(1).GetShortLocation()))
		dstData := util.PointerAdd(dstPage, int(chunkBegin))
		dstBytes := (*[1 << 30]byte)(dstData)[:chunkEnd-chunkBegin]
		assert.Contains(t, string(dstBytes), "dst_chunk0")
	})

	t.Run("not partial page", func(t *testing.T) {
		// 重置状态
		partial.isPartial = true
		for i := range partial.chunkIsLoaded {
			partial.chunkIsLoaded[i] = false
		}

		// 设置非部分页面状态
		partial.isPartial = false

		// 执行测试
		got := partialLoadChunk(partial, dstPage, 0)

		// 验证结果
		assert.True(t, got)
		// 验证数据没有被改变（因为不是部分页面）
		dstHeader := (*BTPageHeader)(dstPage)
		chunkBegin := LocationIndex(ShortGetLocation(dstHeader.GetChunkDesc(0).GetShortLocation()))
		chunkEnd := LocationIndex(ShortGetLocation(dstHeader.GetChunkDesc(1).GetShortLocation()))
		dstData := util.PointerAdd(dstPage, int(chunkBegin))
		dstBytes := (*[1 << 30]byte)(dstData)[:chunkEnd-chunkBegin]
		assert.Contains(t, string(dstBytes), "dst_chunk0")
	})

	t.Run("successful load first chunk", func(t *testing.T) {
		// 重置状态
		partial.isPartial = true
		for i := range partial.chunkIsLoaded {
			partial.chunkIsLoaded[i] = false
		}

		// 执行测试
		got := partialLoadChunk(partial, dstPage, 0)

		// 验证结果
		assert.True(t, got)

		// 验证数据是否正确复制
		dstHeader := (*BTPageHeader)(dstPage)
		chunkBegin := LocationIndex(ShortGetLocation(dstHeader.GetChunkDesc(0).GetShortLocation()))
		chunkEnd := LocationIndex(ShortGetLocation(dstHeader.GetChunkDesc(1).GetShortLocation()))

		// 比较源页面和目标页面的数据
		srcData := util.PointerAdd(srcPage, int(chunkBegin))
		dstData := util.PointerAdd(dstPage, int(chunkBegin))
		dataLen := int(chunkEnd - chunkBegin)
		srcBytes := (*[1 << 30]byte)(srcData)[:dataLen]
		dstBytes := (*[1 << 30]byte)(dstData)[:dataLen]
		assert.Equal(t, srcBytes, dstBytes)
		// 验证数据确实被改变了
		assert.Contains(t, string(dstBytes), "src_chunk0")
	})

	t.Run("successful load last chunk", func(t *testing.T) {

		// 创建PartialPageState
		partial := &PartialPageState{
			src:           srcPage,
			isPartial:     true,
			chunkIsLoaded: [BT_PAGE_MAX_CHUNKS]bool{},
		}
		// 执行测试
		got := partialLoadChunk(partial, dstPage, 1)

		// 验证结果
		assert.True(t, got)

		// 验证数据是否正确复制
		dstHeader := (*BTPageHeader)(dstPage)
		chunkBegin := LocationIndex(ShortGetLocation(dstHeader.GetChunkDesc(1).GetShortLocation()))
		chunkEnd := dstHeader.dataSize

		// 比较源页面和目标页面的数据
		srcData := util.PointerAdd(srcPage, int(chunkBegin))
		dstData := util.PointerAdd(dstPage, int(chunkBegin))
		dataLen := int(chunkEnd - chunkBegin)
		srcBytes := (*[1 << 30]byte)(srcData)[:dataLen]
		dstBytes := (*[1 << 30]byte)(dstData)[:dataLen]
		assert.Equal(t, srcBytes, dstBytes)
		// 验证数据确实被改变了
		assert.Contains(t, string(dstBytes), "src_chunk1")
	})
}
