package btree

import (
	"fmt"
	"strings"
	"unsafe"

	"github.com/daviszhen/plan/pkg/util"
)

// setupPageWithChunks 是一个helper函数,用于设置测试页面
// 参数:
// - page: 页面指针
// - chunksData: 每个chunk的配置,包含items数据和hikey
// 返回:
// - 各个chunk的描述符位置,用于验证
type ChunkData struct {
	Items []struct {
		Data []byte
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

type SetupResults []SetupResult

func (r SetupResults) String() string {
	var sb strings.Builder
	sb.WriteString("Setup Results:\n")
	for i, result := range r {
		sb.WriteString(fmt.Sprintf("Chunk %d:\n%s\n", i, result))
	}
	return sb.String()
}

func SetupPageWithChunks(page unsafe.Pointer, chunksData []ChunkData) SetupResults {
	header := (*BTPageHeader)(page)

	// 设置基本页面信息
	header.chunksCount = OffsetNumber(len(chunksData))
	header.itemsCount = 0 // 会在添加items时累加

	// 获取chunkDesc字段相对于BTPageHeader的偏移量
	chunkDescOffset := util.AlignValue(BT_PAGE_HEADER_CHUNK_DESC_OFFSET, 8)

	// 计算chunkDesc数组的大小(8字节对齐)
	chunkDescSize := util.AlignValue(uint32(BT_PAGE_CHUNK_DESC_SIZE)*uint32(len(chunksData)), 8)

	// 计算所有hikey的总大小
	totalHikeySize := uint32(0)
	for _, cd := range chunksData {
		rawLen := uint32(len(cd.Hikey))
		if rawLen > 0 {
			alignLen := util.AlignValue(rawLen, 8)
			util.AssertFunc(alignLen == rawLen)
			totalHikeySize += alignLen
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
		chunkDesc.SetHikeyShortLocation(LocationGetShort(currentHikeyOffset))
		chunkDesc.SetShortLocation(LocationGetShort(currentChunkOffset))
		chunkDesc.SetOffset(currentItemOffset)
		rawLen := uint32(len(cd.Hikey))
		if rawLen > 0 {
			// 复制hikey数据
			util.CMemcpy(
				util.PointerAdd(page, int(currentHikeyOffset)),
				unsafe.Pointer(&cd.Hikey[0]),
				int(rawLen),
			)
			// 如果需要填充以对齐到8字节
			hikeyAlignedSize := util.AlignValue(rawLen, 8)
			if hikeyAlignedSize > rawLen {
				util.CMemset(
					util.PointerAdd(page, int(currentHikeyOffset+rawLen)),
					0,
					int(hikeyAlignedSize-rawLen),
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

		itemArrSize := util.AlignValue(uint32(LocationIndexSize)*uint32(len(cd.Items)), 8)
		//relative to chunk start
		relativeItemPosition := itemArrSize
		// 设置items
		for j, item := range cd.Items {
			itemSize := uint32(len(item.Data))
			itemSize = util.AlignValue(itemSize, 8)
			// 计算相对于chunk的offset
			realOffset := currentChunkOffset + relativeItemPosition
			chunk.SetItem(j, LocationIndex(relativeItemPosition))
			if itemSize > 0 {
				dst := util.PointerAdd(unsafe.Pointer(chunk), int(relativeItemPosition))
				util.CMemcpy(
					dst,
					unsafe.Pointer(&item.Data[0]),
					int(itemSize),
				)
				//read it back and print it
				data := util.PointerToSlice[byte](
					dst,
					int(itemSize),
				)
				fmt.Printf("Fill Chunk %d ChunkOffset %d Item %d RealOffset %d RelativePosition %d addr:0x%x, Data: % x\n",
					i,
					currentChunkOffset,
					j,
					realOffset,
					relativeItemPosition,
					dst,
					data)
			}
			result[i].OriginalItems[j] = struct {
				Offset uint32
				Data   []byte
			}{
				Offset: relativeItemPosition,
				Data:   item.Data,
			}
			relativeItemPosition += itemSize
		}

		// 更新计数
		header.itemsCount += OffsetNumber(len(cd.Items))
		currentItemOffset += uint32(len(cd.Items))

		// 计算下一个chunk的位置
		currentChunkOffset += util.AlignValue(relativeItemPosition, 8)
		currentChunkOffset = util.AlignValue(currentChunkOffset, 8)
	}

	// 设置最终的dataSize
	header.dataSize = LocationIndex(currentChunkOffset)

	return result
}

// PrintPageInfo 打印page的详细信息
func PrintPageInfo(page unsafe.Pointer) {
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
			// 获取hikey长度
			var hikeyLen int
			if i < int(header.chunksCount)-1 {
				nextChunkDesc := header.GetChunkDesc(i + 1)
				hikeyLen = int(ShortGetLocation(nextChunkDesc.GetHikeyShortLocation()) - hikeyLoc)
			} else {
				hikeyLen = int(header.hikeysEnd) - int(hikeyLoc)
			}
			if hikeyLen > 0 {
				hikeyData := (*[1 << 20]byte)(util.PointerAdd(page, int(hikeyLoc)))[:hikeyLen]
				fmt.Printf("  Length: %d\n", hikeyLen)
				if hikeyLen > 32 {
					fmt.Printf("  Data: % x ... (%d more bytes)\n", hikeyData[:32], hikeyLen-32)
				} else {
					fmt.Printf("  Data: % x\n", hikeyData)
				}
			} else {
				fmt.Printf("  Length: 0\n")
			}
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
			item := chunk.GetItem(j)
			itemOffset := ItemGetOffset(item)
			itemFlags := ItemGetFlags(item)
			fmt.Printf("  Item %d:\n", j)
			fmt.Printf("    RawItem: %d\n", item)
			fmt.Printf("    Offset: %d\n", itemOffset)
			fmt.Printf("    Flags: %d\n", itemFlags)

			// 获取item数据
			itemPtr := util.PointerAdd(unsafe.Pointer(chunk), int(itemOffset))

			// 计算item大小
			itemSize := 0
			if j < itemsCount-1 {
				nextItemOffset := ItemGetOffset(chunk.GetItem(j + 1))
				itemSize = int(nextItemOffset - itemOffset)
			} else if i < int(header.chunksCount)-1 {
				nextChunkDesc := header.GetChunkDesc(i + 1)
				nextChunkLoc := ShortGetLocation(nextChunkDesc.GetShortLocation())
				itemSize = int(nextChunkLoc - (chunkLoc + uint32(itemOffset)))
			} else {
				itemSize = int(header.dataSize - LocationIndex(chunkLoc+uint32(itemOffset)))
			}
			itemData := util.PointerToSlice[byte](itemPtr, itemSize)

			// 打印item数据(十六进制格式)
			fmt.Printf("    Size: %d\n", itemSize)
			if itemSize > 64 {
				fmt.Printf("    Data: % x ... (%d more bytes)\n", itemData[:64], itemSize-64)
			} else {
				fmt.Printf("    Data: % x\n", itemData)
			}
		}
	}
	fmt.Printf("\n")
}

// 新版：hikey内容自动用下一个chunk的第一个item，最后一个chunk用lasthikey
func SetupPageWithChunksV2(
	page unsafe.Pointer,
	chunksData []ChunkData,
	flags uint16,
	lasthikey []byte,
) SetupResults {
	header := (*BTPageHeader)(page)
	header.SetFlags(flags)
	isLeaf := (flags & BTREE_FLAG_LEAF) != 0

	header.chunksCount = OffsetNumber(len(chunksData))
	header.itemsCount = 0

	chunkDescOffset := util.AlignValue(BT_PAGE_HEADER_CHUNK_DESC_OFFSET, 8)
	chunkDescSize := util.AlignValue(uint32(BT_PAGE_CHUNK_DESC_SIZE)*uint32(len(chunksData)), 8)

	// 计算所有hikey的总大小
	hikeyContents := make([][]byte, len(chunksData))
	for i := 0; i < len(chunksData); i++ {
		var hikey []byte
		if i < len(chunksData)-1 {
			if len(chunksData[i+1].Items) == 0 {
				panic(fmt.Sprintf("Chunk %d has no items, cannot use as hikey", i+1))
			}
			itemData := chunksData[i+1].Items[0].Data
			headLen := BT_LEAF_TUPHDR_SIZE
			if !isLeaf {
				headLen = BT_NON_LEAF_TUPHDR_SIZE
			}
			if uint32(len(itemData)) <= headLen {
				panic(fmt.Sprintf("Item data length %d is not greater than head length %d", len(itemData), headLen))
			}
			hikey = itemData[headLen:]
		} else {
			hikey = lasthikey
		}
		alignLen := util.AlignValue(uint32(len(hikey)), 8)
		hikeyAligned := make([]byte, alignLen)
		copy(hikeyAligned, hikey)
		hikeyContents[i] = hikeyAligned
	}

	totalHikeySize := uint32(0)
	for _, hk := range hikeyContents {
		totalHikeySize += uint32(len(hk))
	}
	totalHikeySize = util.AlignValue(totalHikeySize, 8)

	var maxHikeyEnd uint32
	if isLeaf {
		maxHikeyEnd = 256
	} else {
		maxHikeyEnd = 512
	}

	hikeysEnd := chunkDescOffset + chunkDescSize + totalHikeySize
	if hikeysEnd <= maxHikeyEnd {
		header.hikeysEnd = OffsetNumber(hikeysEnd)
	} else {
		panic(fmt.Sprintf("hikeysEnd %d exceeds maxHikeyEnd %d", hikeysEnd, maxHikeyEnd))
	}

	currentHikeyOffset := chunkDescOffset + chunkDescSize
	currentChunkOffset := maxHikeyEnd
	currentItemOffset := uint32(0)

	result := make([]SetupResult, len(chunksData))

	for i, cd := range chunksData {
		chunkDesc := header.GetChunkDesc(i)
		chunkDesc.SetHikeyShortLocation(LocationGetShort(currentHikeyOffset))
		chunkDesc.SetShortLocation(LocationGetShort(currentChunkOffset))
		chunkDesc.SetOffset(currentItemOffset)

		hk := hikeyContents[i]
		if len(hk) > 0 {
			util.CMemcpy(
				util.PointerAdd(page, int(currentHikeyOffset)),
				unsafe.Pointer(&hk[0]),
				len(hk),
			)
			currentHikeyOffset += uint32(len(hk))
		}

		chunk := (*BTPageChunk)(util.PointerAdd(page, int(currentChunkOffset)))

		result[i].ChunkLoc = currentChunkOffset
		result[i].HikeyLoc = currentHikeyOffset
		result[i].ItemsCount = len(cd.Items)
		result[i].OriginalItems = make([]struct {
			Offset uint32
			Data   []byte
		}, len(cd.Items))

		itemArrSize := util.AlignValue(uint32(LocationIndexSize)*uint32(len(cd.Items)), 8)
		relativeItemPosition := itemArrSize
		for j, item := range cd.Items {
			itemSize := uint32(len(item.Data))
			itemSize = util.AlignValue(itemSize, 8)
			chunk.SetItem(j, LocationIndex(relativeItemPosition))
			if itemSize > 0 {
				dst := util.PointerAdd(unsafe.Pointer(chunk), int(relativeItemPosition))
				util.CMemcpy(
					dst,
					unsafe.Pointer(&item.Data[0]),
					int(itemSize),
				)
			}
			result[i].OriginalItems[j] = struct {
				Offset uint32
				Data   []byte
			}{
				Offset: relativeItemPosition,
				Data:   item.Data,
			}
			relativeItemPosition += itemSize
		}

		header.itemsCount += OffsetNumber(len(cd.Items))
		currentItemOffset += uint32(len(cd.Items))
		currentChunkOffset += util.AlignValue(relativeItemPosition, 8)
		currentChunkOffset = util.AlignValue(currentChunkOffset, 8)
	}

	header.dataSize = LocationIndex(currentChunkOffset)
	return result
}
