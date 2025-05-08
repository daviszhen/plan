package btree

import (
	"fmt"
	"strings"
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

// setupPageWithChunks 是一个helper函数,用于设置测试页面
// 参数:
// - page: 页面指针
// - chunksData: 每个chunk的配置,包含items数据和hikey
// 返回:
// - 各个chunk的描述符位置,用于验证
type ChunkData struct {
	Items []struct {
		Offset uint32
		Data   []byte
	}
	Hikey []byte
}

type SetupResult struct {
	ChunkLoc      uint32
	HikeyLoc      uint32
	ItemsCount    int
	OriginalItems []struct {
		Offset uint32
		Data   []byte
	}
}

func (r SetupResult) String() string {
	var sb strings.Builder

	sb.WriteString(fmt.Sprintf("Chunk Location: %d\n", r.ChunkLoc))
	sb.WriteString(fmt.Sprintf("Hikey Location: %d\n", r.HikeyLoc))
	sb.WriteString(fmt.Sprintf("Items Count: %d\n", r.ItemsCount))

	sb.WriteString("Original Items:\n")
	for i, item := range r.OriginalItems {
		sb.WriteString(fmt.Sprintf("  Item %d:\n", i))
		sb.WriteString(fmt.Sprintf("    Offset: %d\n", item.Offset))
		sb.WriteString(fmt.Sprintf("    Data: % x\n", item.Data))
	}

	return sb.String()
}

func setupPageWithChunks(page unsafe.Pointer, chunksData []ChunkData) []SetupResult {
	header := (*BTPageHeader)(page)

	// 设置基本页面信息
	header.chunksCount = OffsetNumber(len(chunksData))
	header.itemsCount = 0 // 会在添加items时累加

	// 获取chunkDesc字段相对于BTPageHeader的偏移量
	chunkDescOffset := uint32(unsafe.Offsetof(header.chunksDesc))

	// 计算chunkDesc数组的大小(8字节对齐)
	chunkDescSize := util.AlignValue(uint32(unsafe.Sizeof(BTPageChunkDesc{}))*uint32(len(chunksData)), 8)

	// 计算所有hikey的总大小
	totalHikeySize := uint32(0)
	for _, cd := range chunksData {
		if len(cd.Hikey) > 0 {
			totalHikeySize += uint32(util.AlignValue(uint32(len(cd.Hikey)), 8))
		}
	}

	// 确保hikey区域也是8字节对齐的
	totalHikeySize = util.AlignValue(totalHikeySize, 8)

	// 设置hikeysEnd
	header.hikeysEnd = OffsetNumber(chunkDescOffset + chunkDescSize + totalHikeySize)

	// 跟踪当前位置
	currentHikeyOffset := chunkDescOffset + chunkDescSize
	currentChunkOffset := chunkDescOffset + chunkDescSize + totalHikeySize
	currentItemOffset := uint32(0)

	// 保存chunk信息用于返回
	result := make([]SetupResult, len(chunksData))

	// 处理每个chunk
	for i, cd := range chunksData {
		// 设置chunk描述符
		chunkDesc := header.GetChunkDesc(i)
		chunkDesc.SetShortLocation(LocationGetShort(currentChunkOffset))
		chunkDesc.SetOffset(currentItemOffset)

		if len(cd.Hikey) > 0 {
			chunkDesc.SetHikeyShortLocation(LocationGetShort(currentHikeyOffset))
			// 复制hikey数据
			util.CMemcpy(
				util.PointerAdd(page, int(currentHikeyOffset)),
				unsafe.Pointer(&cd.Hikey[0]),
				len(cd.Hikey),
			)
			// 如果需要填充以对齐到8字节
			hikeyAlignedSize := util.AlignValue(uint32(len(cd.Hikey)), 8)
			if hikeyAlignedSize > uint32(len(cd.Hikey)) {
				util.CMemset(
					util.PointerAdd(page, int(currentHikeyOffset)+len(cd.Hikey)),
					0,
					int(hikeyAlignedSize-uint32(len(cd.Hikey))),
				)
			}
			currentHikeyOffset += hikeyAlignedSize
		}

		// 创建chunk
		chunk := (*BTPageChunk)(util.PointerAdd(page, int(currentChunkOffset)))

		// 保存原始信息用于返回
		result[i].ChunkLoc = currentChunkOffset
		result[i].HikeyLoc = currentHikeyOffset
		result[i].ItemsCount = len(cd.Items)
		result[i].OriginalItems = make([]struct {
			Offset uint32
			Data   []byte
		}, len(cd.Items))

		// 设置items
		for j, item := range cd.Items {
			chunk.SetItem(j, LocationIndex(item.Offset))
			if len(item.Data) > 0 {
				util.CMemcpy(
					util.PointerAdd(unsafe.Pointer(chunk), int(item.Offset)),
					unsafe.Pointer(&item.Data[0]),
					len(item.Data),
				)
			}
			result[i].OriginalItems[j] = struct {
				Offset uint32
				Data   []byte
			}{
				Offset: item.Offset,
				Data:   item.Data,
			}
		}

		// 更新计数
		header.itemsCount += OffsetNumber(len(cd.Items))
		currentItemOffset += uint32(len(cd.Items))

		// 计算下一个chunk的位置
		// chunk size = items数组大小(对齐到8字节) + 最后一个item的offset + 最后一个item的大小
		lastItemOffset := uint32(0)
		lastItemSize := uint32(0)
		if len(cd.Items) > 0 {
			lastItem := cd.Items[len(cd.Items)-1]
			lastItemOffset = lastItem.Offset
			lastItemSize = uint32(len(lastItem.Data))
		}
		// 确保整个chunk size是8字节对齐的
		itemsArraySize := util.AlignValue(uint32(LocationIndexSize)*uint32(len(cd.Items)), 8)
		chunkSize := util.AlignValue(itemsArraySize+lastItemOffset+lastItemSize, 8)
		currentChunkOffset += chunkSize
	}

	// 设置最终的dataSize
	header.dataSize = LocationIndex(currentChunkOffset)

	return result
}

// printPageInfo 打印page的详细信息
func printPageInfo(page unsafe.Pointer) {
	header := (*BTPageHeader)(page)

	// 打印页面头部信息
	fmt.Printf("\n=== Page Header Info ===\n")
	fmt.Printf("chunksCount: %d\n", header.chunksCount)
	fmt.Printf("itemsCount: %d\n", header.itemsCount)
	fmt.Printf("hikeysEnd: %d\n", header.hikeysEnd)
	fmt.Printf("dataSize: %d\n", header.dataSize)

	// 打印chunkDesc数组信息
	fmt.Printf("\n=== ChunkDesc Array Info ===\n")
	for i := 0; i < int(header.chunksCount); i++ {
		chunkDesc := header.GetChunkDesc(i)
		fmt.Printf("ChunkDesc %d:\n", i)
		fmt.Printf("  Offset: %d\n", chunkDesc.GetOffset())
		fmt.Printf("  ShortLocation: %d (actual: %d)\n", chunkDesc.GetShortLocation(), ShortGetLocation(chunkDesc.GetShortLocation()))
		fmt.Printf("  HikeyShortLocation: %d (actual: %d)\n", chunkDesc.GetHikeyShortLocation(), ShortGetLocation(chunkDesc.GetHikeyShortLocation()))
		fmt.Printf("  HikeyFlags: %d\n", chunkDesc.GetHikeyFlags())
	}

	// 打印hikey区域信息
	fmt.Printf("\n=== Hikey Area Info ===\n")
	for i := 0; i < int(header.chunksCount); i++ {
		chunkDesc := header.GetChunkDesc(i)
		hikeyLoc := ShortGetLocation(chunkDesc.GetHikeyShortLocation())
		if hikeyLoc > 0 {
			fmt.Printf("Chunk %d Hikey at offset %d:\n", i, hikeyLoc)
			// 打印hikey数据(十六进制格式)
			hikeyData := (*[1 << 30]byte)(util.PointerAdd(page, int(hikeyLoc)))[:8]
			fmt.Printf("  Data: % x\n", hikeyData)
		}
	}

	// 打印chunk信息
	fmt.Printf("\n=== Chunks Info ===\n")
	for i := 0; i < int(header.chunksCount); i++ {
		chunkDesc := header.GetChunkDesc(i)
		chunkLoc := ShortGetLocation(chunkDesc.GetShortLocation())
		chunkOffset := chunkDesc.GetOffset()

		fmt.Printf("Chunk %d:\n", i)
		fmt.Printf("  Location: %d\n", chunkLoc)
		fmt.Printf("  Offset: %d\n", chunkOffset)

		// 获取chunk
		chunk := (*BTPageChunk)(util.PointerAdd(page, int(chunkLoc)))

		// 计算这个chunk的items数量
		itemsCount := 0
		if i < int(header.chunksCount)-1 {
			nextChunkDesc := header.GetChunkDesc(i + 1)
			itemsCount = int(nextChunkDesc.GetOffset() - chunkOffset)
		} else {
			itemsCount = int(uint32(header.itemsCount) - chunkOffset)
		}

		fmt.Printf("  Items Count: %d\n", itemsCount)

		// 打印每个item的信息
		for j := 0; j < itemsCount; j++ {
			itemOffset := chunk.GetItem(j)
			fmt.Printf("  Item %d:\n", j)
			fmt.Printf("    Offset: %d\n", itemOffset)

			// 获取item数据
			itemData := (*[1 << 30]byte)(util.PointerAdd(unsafe.Pointer(chunk), int(itemOffset)))

			// 计算item大小
			itemSize := 0
			if j < itemsCount-1 {
				nextItemOffset := chunk.GetItem(j + 1)
				itemSize = int(nextItemOffset - itemOffset)
			} else if i < int(header.chunksCount)-1 {
				nextChunkDesc := header.GetChunkDesc(i + 1)
				nextChunkLoc := ShortGetLocation(nextChunkDesc.GetShortLocation())
				itemSize = int(nextChunkLoc - (chunkLoc + uint32(itemOffset)))
			} else {
				itemSize = int(header.dataSize - LocationIndex(chunkLoc+uint32(itemOffset)))
			}

			// 打印item数据(十六进制格式)
			fmt.Printf("    Size: %d\n", itemSize)
			fmt.Printf("    Data: % x\n", itemData[:itemSize])
		}
	}
	fmt.Printf("\n")
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
					Offset uint32
					Data   []byte
				}{
					{
						Offset: 80,
						Data:   []byte("item1"),
					},
					{
						Offset: 100,
						Data:   []byte("item2"),
					},
				},
				Hikey: []byte{1, 2, 3, 4},
			},
			{
				// 第二个chunk
				Items: []struct {
					Offset uint32
					Data   []byte
				}{
					{
						Offset: 20,
						Data:   []byte("item3"),
					},
					{
						Offset: 40,
						Data:   []byte("item4"),
					},
				},
				Hikey: []byte{5, 6, 7, 8},
			},
		}

		// 设置页面
		chunksInfo := setupPageWithChunks(unsafe.Pointer(&page[0]), chunksData)
		for i, info := range chunksInfo {
			fmt.Printf("Chunk %d:\n%s\n", i, info)
		}

		// 打印合并前的页面信息
		fmt.Println("Before merge:")
		printPageInfo(unsafe.Pointer(&page[0]))

		// 执行merge操作
		pageMergeChunks(unsafe.Pointer(&page[0]), OffsetNumber(0))

		// 打印合并后的页面信息
		fmt.Println("After merge:")
		printPageInfo(unsafe.Pointer(&page[0]))

		// 验证merge后的结果
		header := (*BTPageHeader)(unsafe.Pointer(&page[0]))

		// 1. 验证chunksCount
		assert.Equal(t, OffsetNumber(1), header.chunksCount, "chunksCount should be 1 after merge")

		// 2. 验证itemsCount
		assert.Equal(t, OffsetNumber(4), header.itemsCount, "itemsCount should be 4 after merge")

		// 3. 验证hikey区域
		// 3.1 验证hikey区域大小
		// 获取chunkDesc字段相对于BTPageHeader的偏移量
		chunkDescOffset := uint32(unsafe.Offsetof(header.chunksDesc))
		// 计算chunkDesc数组的大小(8字节对齐)
		chunkDescSize := util.AlignValue(uint32(unsafe.Sizeof(BTPageChunkDesc{}))*uint32(header.chunksCount), 8)
		// hikey区域应该在header和chunkDesc之后
		expectedHikeysEnd := OffsetNumber(chunkDescOffset + chunkDescSize + util.AlignValue(uint32(4), 8)) // 4是hikey的大小
		assert.Equal(t, expectedHikeysEnd, header.hikeysEnd, "hikeysEnd should be chunkDescOffset + chunkDescSize + hikeySize")

		// 3.2 验证hikey内容
		expectedHikey := []byte{5, 6, 7, 8} // 修改为chunk[1]的hikey
		actualHikey := page[chunkDescOffset+chunkDescSize : header.hikeysEnd]
		assert.Equal(t, expectedHikey, actualHikey, "hikey content should be preserved")

		// 4. 验证chunk的位置和内容
		// 4.1 验证第一个chunk的位置
		chunk1Desc := header.GetChunkDesc(0)
		expectedChunkLoc := chunkDescOffset + chunkDescSize + util.AlignValue(uint32(4), 8) // chunkDesc + hikey(对齐后)
		assert.Equal(t, LocationGetShort(expectedChunkLoc), chunk1Desc.GetShortLocation(), "chunk1 location should be correct")

		// 4.2 验证chunk的内容
		chunk1 := (*BTPageChunk)(util.PointerAdd(unsafe.Pointer(&page[0]), int(expectedChunkLoc)))

		// 验证所有items
		expectedItems := []struct {
			offset uint32
			data   []byte
			size   int
		}{
			{80, []byte("item1"), 20},
			{100, []byte("item2"), 20},
			{20, []byte("item3"), 20},
			{40, []byte("item4"), 20},
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
