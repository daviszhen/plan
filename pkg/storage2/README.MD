# Storage2 使用教程

Storage2 是一个版本化的表存储引擎，与 Lance 的元数据模型对齐。本教程介绍如何使用 storage2 进行数据读写操作。

## 目录

- [数据文件读写](#数据文件读写)
- [版本和 Manifest](#版本和-manifest)
- [事务操作](#事务操作)
- [完整示例](#完整示例)

## 数据文件读写

数据文件列存使用 **pkg/chunk**，类型与序列化与 `chunk.Chunk`、`pkg/common.LType` 一致。

### 写入数据文件

使用 `WriteChunkToFile` 将 `chunk.Chunk` 写入文件：

```go
import (
    "github.com/daviszhen/plan/pkg/chunk"
    "github.com/daviszhen/plan/pkg/common"
    "github.com/daviszhen/plan/pkg/storage2"
    "github.com/daviszhen/plan/pkg/util"
)

typs := []common.LType{
    common.MakeLType(common.LTID_INTEGER),
    common.MakeLType(common.LTID_BIGINT),
}
c := &chunk.Chunk{}
c.Init(typs, util.DefaultVectorSize)
c.SetCard(10)
for i := 0; i < 10; i++ {
    c.Data[0].SetValue(i, &chunk.Value{Typ: typs[0], I64: int64(i)})
    c.Data[1].SetValue(i, &chunk.Value{Typ: typs[1], I64: int64(i * 100)})
}
err := storage2.WriteChunkToFile("data/0.dat", c)
```

### 读取数据文件

使用 `ReadChunkFromFile` 从文件读回为 Chunk：

```go
c, err := storage2.ReadChunkFromFile(path)
if err != nil {
    return err
}
numRows := c.Card()
numCols := c.ColumnCount()
for i := 0; i < numRows; i++ {
    for j := 0; j < numCols; j++ {
        val := c.Data[j].GetValue(i)
        // 使用 val.I64, val.F64, val.Str 等
    }
}
```

## 版本和 Manifest

### 创建初始 Manifest

```go
// 创建版本 0 的 manifest
manifest := storage2.NewManifest(0)
manifest.Fragments = []*storage2.DataFragment{}
manifest.NextRowId = 1
```

### 提交 Manifest

使用 `CommitHandler` 提交新版本：

```go
import "context"

ctx := context.Background()
handler := storage2.NewLocalRenameCommitHandler()

// 提交版本 0
if err := handler.Commit(ctx, basePath, 0, manifest); err != nil {
    return err
}

// 获取最新版本
latestVersion, err := handler.ResolveLatestVersion(ctx, basePath)
if err != nil {
    return err
}
```

### 加载 Manifest

```go
// 加载指定版本的 manifest
manifest, err := storage2.LoadManifest(ctx, basePath, handler, version)
if err != nil {
    return err
}
```

## 事务操作

Storage2 支持三种事务操作：Append（追加）、Delete（删除）、Overwrite（覆盖）。

### Append 操作

追加新的数据片段：

```go
// 创建数据文件
dataFile := storage2.NewDataFile(
    "data/1.dat",           // 路径
    []int32{0, 1},          // 字段 ID
    1,                      // 主版本号
    0,                      // 次版本号
)

// 创建片段
fragment := storage2.NewDataFragment(0, []*storage2.DataFile{dataFile})

// 创建 Append 事务
readVersion := uint64(0)
uuid := "append-txn-001"
txn := storage2.NewTransactionAppend(readVersion, uuid, []*storage2.DataFragment{fragment})

// 提交事务
if err := storage2.CommitTransaction(ctx, basePath, handler, txn); err != nil {
    if err == storage2.ErrConflict {
        // 处理冲突：有其他事务已提交
        return fmt.Errorf("transaction conflict")
    }
    return err
}
```

### Delete 操作

删除满足条件的数据：

```go
txn := storage2.NewTransactionDelete(
    readVersion,
    "delete-txn-001",
    []*storage2.DataFragment{},  // 更新的片段（如果有）
    []uint64{0, 1},               // 要删除的片段 ID
    "id > 100",                   // 删除谓词
)

if err := storage2.CommitTransaction(ctx, basePath, handler, txn); err != nil {
    return err
}
```

### Overwrite 操作

覆盖整个表（创建或替换）：

```go
txn := storage2.NewTransactionOverwrite(
    readVersion,
    "overwrite-txn-001",
    []*storage2.DataFragment{fragment},  // 新片段
    []*storage2pb.Field{},                // Schema（可选）
    map[string]string{},                 // 配置（可选）
)

if err := storage2.CommitTransaction(ctx, basePath, handler, txn); err != nil {
    return err
}
```

## 完整示例

以下是一个完整的示例，展示如何创建数据集、写入数据并提交事务：

```go
package main

import (
    "context"
    "fmt"
    "path/filepath"

    "github.com/daviszhen/plan/pkg/chunk"
    "github.com/daviszhen/plan/pkg/common"
    "github.com/daviszhen/plan/pkg/storage2"
    "github.com/daviszhen/plan/pkg/util"
)

func main() {
    basePath := "/tmp/my_dataset"
    ctx := context.Background()
    handler := storage2.NewLocalRenameCommitHandler()

    // 1. 创建初始 manifest（版本 0）
    m0 := storage2.NewManifest(0)
    m0.Fragments = []*storage2.DataFragment{}
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
    if err := storage2.WriteChunkToFile(dataPath, c); err != nil {
        panic(err)
    }

    // 3. 创建数据文件元数据
    dataFile := storage2.NewDataFile(
        "data/0.dat",
        []int32{0, 1},
        1, 0,
    )

    // 4. 创建片段
    fragment := storage2.NewDataFragmentWithRows(0, uint64(numRows), []*storage2.DataFile{dataFile})

    // 5. 创建并提交 Append 事务
    txn := storage2.NewTransactionAppend(0, "first-append", []*storage2.DataFragment{fragment})
    if err := storage2.CommitTransaction(ctx, basePath, handler, txn); err != nil {
        panic(err)
    }

    // 6. 验证结果
    latest, _ := handler.ResolveLatestVersion(ctx, basePath)
    fmt.Printf("Latest version: %d\n", latest)

    m1, _ := storage2.LoadManifest(ctx, basePath, handler, 1)
    fmt.Printf("Fragments in v1: %d\n", len(m1.Fragments))
}
```

## 注意事项

1. **事务冲突**：如果多个事务基于相同的 `readVersion` 提交，可能会发生冲突。使用 `ErrConflict` 检查并重试。

2. **数据文件格式**：Storage2 使用 pkg/chunk 列存格式（chunk.Chunk 序列化），与 plan 执行器、算子统一。

3. **路径约定**：
   - Manifest 文件：`_versions/{version}.manifest`
   - 事务文件：`_transactions/{read_version}-{uuid}.txn`
   - 数据文件：自定义路径（通常放在 `data/` 目录下）

4. **版本管理**：每次成功提交事务会创建新版本，版本号递增。

## 更多信息

- 查看 `doc.go` 了解包的设计和与 Lance 的差异
- 查看测试文件了解更多使用示例
- 参考 `proto/` 目录了解 Protobuf 定义
