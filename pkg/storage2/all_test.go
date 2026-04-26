package storage2

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/daviszhen/plan/pkg/chunk"
	"github.com/daviszhen/plan/pkg/common"
	"github.com/daviszhen/plan/pkg/util"
)

func TestAll(t *testing.T) {
	basePath := t.TempDir()
	ctx := context.Background()
	handler := NewLocalRenameCommitHandler()

	// 1. 创建初始 manifest（版本 0）
	m0 := NewManifest(0)
	m0.Fragments = []*DataFragment{}
	m0.NextRowId = 1
	if err := handler.Commit(ctx, basePath, 0, m0); err != nil {
		panic(err)
	}

	// 2. 写入数据文件（使用 pkg/chunk）
	dataPath := filepath.Join(basePath, "data", "0.dat")
	numRows := 100

	typs := []common.LType{
		common.MakeLType(common.LTID_INTEGER),
		common.MakeLType(common.LTID_BIGINT),
	}
	c := &chunk.Chunk{}
	c.Init(typs, util.DefaultVectorSize)
	c.SetCard(numRows)
	for i := 0; i < numRows; i++ {
		c.Data[0].SetValue(i, &chunk.Value{Typ: typs[0], I64: int64(i)})
		c.Data[1].SetValue(i, &chunk.Value{Typ: typs[1], I64: int64(i * 100)})
	}
	if err := WriteChunkToFile(dataPath, c); err != nil {
		panic(err)
	}

	// 3. 创建数据文件元数据
	dataFile := NewDataFile(
		"data/0.dat",
		[]int32{0, 1},
		1, 0,
	)

	// 4. 创建片段
	fragment := NewDataFragmentWithRows(0, uint64(numRows), []*DataFile{dataFile})

	// 5. 创建并提交 Append 事务
	txn := NewTransactionAppend(0, "first-append", []*DataFragment{fragment})
	if err := CommitTransaction(ctx, basePath, handler, txn); err != nil {
		panic(err)
	}

	// 6. 验证结果
	latest, _ := handler.ResolveLatestVersion(ctx, basePath)
	fmt.Printf("Latest version: %d\n", latest)

	m1, _ := LoadManifest(ctx, basePath, handler, 1)
	fmt.Printf("Fragments in v1: %d\n", len(m1.Fragments))

	// 7. 读取并验证写入的数据（使用 pkg/chunk）
	got, err := ReadChunkFromFile(dataPath)
	if err != nil {
		t.Fatalf("Failed to open data file: %v", err)
	}

	fmt.Printf("\n=== 数据文件信息 ===\n")
	fmt.Printf("行数: %d\n", got.Card())
	fmt.Printf("列数: %d\n", got.ColumnCount())

	fmt.Printf("\n=== 第一列数据 (int32) ===\n")
	fmt.Printf("前 10 个值: ")
	for i := 0; i < 10 && i < numRows; i++ {
		val := got.Data[0].GetValue(i)
		fmt.Printf("%d ", val.I64)
	}
	fmt.Printf("\n后 10 个值: ")
	for i := 90; i < numRows; i++ {
		val := got.Data[0].GetValue(i)
		fmt.Printf("%d ", val.I64)
	}
	fmt.Printf("\n")

	fmt.Printf("\n=== 第二列数据 (int64) ===\n")
	fmt.Printf("前 10 个值: ")
	for i := 0; i < 10 && i < numRows; i++ {
		val := got.Data[1].GetValue(i)
		fmt.Printf("%d ", val.I64)
	}
	fmt.Printf("\n后 10 个值: ")
	for i := 90; i < numRows; i++ {
		val := got.Data[1].GetValue(i)
		fmt.Printf("%d ", val.I64)
	}
	fmt.Printf("\n")

	// 验证数据正确性
	fmt.Printf("\n=== 数据验证 ===\n")
	allCorrect := true
	for i := 0; i < numRows; i++ {
		v0 := got.Data[0].GetValue(i)
		v1 := got.Data[1].GetValue(i)
		if v0.I64 != int64(i) || v1.I64 != int64(i*100) {
			fmt.Printf("数据不匹配: row %d, col0=%d (期望 %d), col1=%d (期望 %d)\n", i, v0.I64, i, v1.I64, i*100)
			allCorrect = false
		}
	}
	if allCorrect {
		fmt.Printf("✓ 所有数据验证通过！\n")
	}
}
