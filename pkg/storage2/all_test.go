package storage2

import (
	"context"
	"encoding/binary"
	"fmt"
	"path/filepath"
	"testing"
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

    // 2. 写入数据文件
    dataPath := filepath.Join(basePath, "data", "0.dat")
    numRows := uint64(100)
    numCols := uint32(2)

    writer, err := CreateDataFile(dataPath, numRows, numCols)
    if err != nil {
        panic(err)
    }

    // 写入列数据
    col0 := make([]byte, 100*4) // int32 列
    col1 := make([]byte, 100*8) // int64 列
    for i := 0; i < 100; i++ {
        binary.LittleEndian.PutUint32(col0[i*4:(i+1)*4], uint32(i))
        binary.LittleEndian.PutUint64(col1[i*8:(i+1)*8], uint64(i*100))
    }

    writer.WriteColumn(col0)
    writer.WriteColumn(col1)
    writer.Close()

    // 3. 创建数据文件元数据
    dataFile := NewDataFile(
        "data/0.dat",
        []int32{0, 1},
        1, 0,
    )

    // 4. 创建片段
    fragment := NewDataFragmentWithRows(0, numRows, []*DataFile{dataFile})

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

    // 7. 读取并验证写入的数据
    reader, err := OpenDataFile(dataPath)
    if err != nil {
        t.Fatalf("Failed to open data file: %v", err)
    }
    defer func() {
        // DataFileReader 不需要显式关闭，但我们可以输出文件信息
    }()

    fmt.Printf("\n=== 数据文件信息 ===\n")
    fmt.Printf("行数: %d\n", reader.NumRows())
    fmt.Printf("列数: %d\n", reader.NumColumns())
    fmt.Printf("文件大小: %d 字节\n", reader.FileSize())

    // 读取第一列（int32）
    col0Data, err := reader.ReadColumn(0)
    if err != nil {
        t.Fatalf("Failed to read column 0: %v", err)
    }
    fmt.Printf("\n=== 第一列数据 (int32) ===\n")
    fmt.Printf("前 10 个值: ")
    for i := 0; i < 10 && i < 100; i++ {
        val := binary.LittleEndian.Uint32(col0Data[i*4 : (i+1)*4])
        fmt.Printf("%d ", val)
    }
    fmt.Printf("\n后 10 个值: ")
    for i := 90; i < 100; i++ {
        val := binary.LittleEndian.Uint32(col0Data[i*4 : (i+1)*4])
        fmt.Printf("%d ", val)
    }
    fmt.Printf("\n")

    // 读取第二列（int64）
    col1Data, err := reader.ReadColumn(1)
    if err != nil {
        t.Fatalf("Failed to read column 1: %v", err)
    }
    fmt.Printf("\n=== 第二列数据 (int64) ===\n")
    fmt.Printf("前 10 个值: ")
    for i := 0; i < 10 && i < 100; i++ {
        val := binary.LittleEndian.Uint64(col1Data[i*8 : (i+1)*8])
        fmt.Printf("%d ", val)
    }
    fmt.Printf("\n后 10 个值: ")
    for i := 90; i < 100; i++ {
        val := binary.LittleEndian.Uint64(col1Data[i*8 : (i+1)*8])
        fmt.Printf("%d ", val)
    }
    fmt.Printf("\n")

    // 验证数据正确性
    fmt.Printf("\n=== 数据验证 ===\n")
    allCorrect := true
    for i := 0; i < 100; i++ {
        val0 := binary.LittleEndian.Uint32(col0Data[i*4 : (i+1)*4])
        val1 := binary.LittleEndian.Uint64(col1Data[i*8 : (i+1)*8])
        if val0 != uint32(i) || val1 != uint64(i*100) {
            fmt.Printf("数据不匹配: row %d, col0=%d (期望 %d), col1=%d (期望 %d)\n", i, val0, i, val1, i*100)
            allCorrect = false
        }
    }
    if allCorrect {
        fmt.Printf("✓ 所有数据验证通过！\n")
    }
}