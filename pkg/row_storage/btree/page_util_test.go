package btree

import (
	"testing"
	"unsafe"

	"github.com/daviszhen/plan/pkg/util"
	"github.com/stretchr/testify/assert"
)

func TestSetupPageWithChunksV2(t *testing.T) {
	t.Run("leaf page basic", func(t *testing.T) {
		// 创建测试页面
		page := make([]byte, BLOCK_SIZE)
		pagePtr := unsafe.Pointer(&page[0])

		// 构造测试数据
		chunksData := []ChunkData{
			{
				Items: []struct{ Data []byte }{
					{Data: make([]byte, BT_LEAF_TUPHDR_SIZE+32)}, // 头部+32字节数据
					{Data: make([]byte, BT_LEAF_TUPHDR_SIZE+32)},
				},
			},
			{
				Items: []struct{ Data []byte }{
					{Data: make([]byte, BT_LEAF_TUPHDR_SIZE+32)},
					{Data: make([]byte, BT_LEAF_TUPHDR_SIZE+32)},
				},
			},
		}

		// 填充测试数据
		for i := range chunksData {
			for j := range chunksData[i].Items {
				data := chunksData[i].Items[j].Data
				// 填充头部
				for k := uint32(0); k < BT_LEAF_TUPHDR_SIZE; k++ {
					data[k] = byte(k)
				}
				// 填充数据部分
				for k := BT_LEAF_TUPHDR_SIZE; k < uint32(len(data)); k++ {
					data[k] = byte(uint32(i)*100 + uint32(j)*10 + k)
				}
			}
		}

		// 最后一个chunk的hikey
		lasthikey := make([]byte, 32)
		for i := range lasthikey {
			lasthikey[i] = byte(i + 200)
		}

		// 调用SetupPageWithChunksV2
		SetupPageWithChunksV2(pagePtr, chunksData, BTREE_FLAG_LEAF, lasthikey)

		// 验证结果
		header := (*BTPageHeader)(pagePtr)
		assert.Equal(t, OffsetNumber(2), header.chunksCount)
		assert.Equal(t, OffsetNumber(4), header.itemsCount)
		assert.True(t, (header.GetFlags()&BTREE_FLAG_LEAF) != 0)

		// 验证第一个chunk的hikey是第二个chunk第一个item的数据部分
		chunk0 := header.GetChunkDesc(0)
		hikeyLoc := ShortGetLocation(chunk0.GetHikeyShortLocation())
		hikeyData := util.PointerToSlice[byte](util.PointerAdd(pagePtr, int(hikeyLoc)), 32)
		expectedHikey := chunksData[1].Items[0].Data[BT_LEAF_TUPHDR_SIZE:]
		assert.Equal(t, expectedHikey, hikeyData[:len(expectedHikey)])

		// 验证第二个chunk的hikey是lasthikey
		chunk1 := header.GetChunkDesc(1)
		hikeyLoc = ShortGetLocation(chunk1.GetHikeyShortLocation())
		hikeyData = util.PointerToSlice[byte](util.PointerAdd(pagePtr, int(hikeyLoc)), 32)
		assert.Equal(t, lasthikey, hikeyData[:len(lasthikey)])
	})

	t.Run("non-leaf page basic", func(t *testing.T) {
		// 创建测试页面
		page := make([]byte, BLOCK_SIZE)
		pagePtr := unsafe.Pointer(&page[0])

		// 构造测试数据
		chunksData := []ChunkData{
			{
				Items: []struct{ Data []byte }{
					{Data: make([]byte, BT_NON_LEAF_TUPHDR_SIZE+32)},
					{Data: make([]byte, BT_NON_LEAF_TUPHDR_SIZE+32)},
				},
			},
			{
				Items: []struct{ Data []byte }{
					{Data: make([]byte, BT_NON_LEAF_TUPHDR_SIZE+32)},
					{Data: make([]byte, BT_NON_LEAF_TUPHDR_SIZE+32)},
				},
			},
		}

		// 填充测试数据
		for i := range chunksData {
			for j := range chunksData[i].Items {
				data := chunksData[i].Items[j].Data
				// 填充头部
				for k := uint32(0); k < BT_NON_LEAF_TUPHDR_SIZE; k++ {
					data[k] = byte(k)
				}
				// 填充数据部分
				for k := BT_NON_LEAF_TUPHDR_SIZE; k < uint32(len(data)); k++ {
					data[k] = byte(uint32(i)*100 + uint32(j)*10 + k)
				}
			}
		}

		// 最后一个chunk的hikey
		lasthikey := make([]byte, 32)
		for i := range lasthikey {
			lasthikey[i] = byte(i + 200)
		}

		// 调用SetupPageWithChunksV2
		SetupPageWithChunksV2(pagePtr, chunksData, 0, lasthikey)

		// 验证结果
		header := (*BTPageHeader)(pagePtr)
		assert.Equal(t, OffsetNumber(2), header.chunksCount)
		assert.Equal(t, OffsetNumber(4), header.itemsCount)
		assert.False(t, (header.GetFlags()&BTREE_FLAG_LEAF) != 0)

		// 验证第一个chunk的hikey是第二个chunk第一个item的数据部分
		chunk0 := header.GetChunkDesc(0)
		hikeyLoc := ShortGetLocation(chunk0.GetHikeyShortLocation())
		hikeyData := util.PointerToSlice[byte](util.PointerAdd(pagePtr, int(hikeyLoc)), 32)
		expectedHikey := chunksData[1].Items[0].Data[BT_NON_LEAF_TUPHDR_SIZE:]
		assert.Equal(t, expectedHikey, hikeyData[:len(expectedHikey)])

		// 验证第二个chunk的hikey是lasthikey
		chunk1 := header.GetChunkDesc(1)
		hikeyLoc = ShortGetLocation(chunk1.GetHikeyShortLocation())
		hikeyData = util.PointerToSlice[byte](util.PointerAdd(pagePtr, int(hikeyLoc)), 32)
		assert.Equal(t, lasthikey, hikeyData[:len(lasthikey)])
	})

	t.Run("error cases", func(t *testing.T) {
		page := make([]byte, BLOCK_SIZE)
		pagePtr := unsafe.Pointer(&page[0])

		t.Run("empty next chunk items", func(t *testing.T) {
			chunksData := []ChunkData{
				{
					Items: []struct{ Data []byte }{
						{Data: make([]byte, BT_LEAF_TUPHDR_SIZE+32)},
					},
				},
				{
					Items: []struct{ Data []byte }{}, // 空items
				},
			}
			lasthikey := make([]byte, 32)

			assert.Panics(t, func() {
				SetupPageWithChunksV2(pagePtr, chunksData, BTREE_FLAG_LEAF, lasthikey)
			})
		})

		t.Run("item data too short", func(t *testing.T) {
			chunksData := []ChunkData{
				{
					Items: []struct{ Data []byte }{
						{Data: make([]byte, BT_LEAF_TUPHDR_SIZE+32)},
					},
				},
				{
					Items: []struct{ Data []byte }{
						{Data: make([]byte, BT_LEAF_TUPHDR_SIZE)}, // 只有头部，没有数据
					},
				},
			}
			lasthikey := make([]byte, 32)

			assert.Panics(t, func() {
				SetupPageWithChunksV2(pagePtr, chunksData, BTREE_FLAG_LEAF, lasthikey)
			})
		})
	})
}
