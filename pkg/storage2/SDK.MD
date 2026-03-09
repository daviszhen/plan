# Storage2 Golang SDK 设计文档

## 概述

Storage2 Golang SDK 是一个高级 API 封装，为 storage2 存储引擎提供易用的 Go 语言接口。设计参考了 Lance Java SDK 的架构模式，同时遵循 Go 语言的惯用法和最佳实践。

### 设计原则

1. **简洁性**：提供直观、易用的 API，隐藏底层复杂性
2. **类型安全**：充分利用 Go 的类型系统，减少运行时错误
3. **Builder 模式**：使用函数式选项模式（Functional Options）进行配置
4. **资源管理**：遵循 Go 的惯用法，使用 `defer` 和 `Close()` 管理资源
5. **错误处理**：明确的错误返回，符合 Go 的错误处理规范

### 核心概念

- **Dataset（数据集）**：版本化的表，包含多个 Fragment（片段）
- **Fragment（片段）**：逻辑数据块，包含一个或多个 DataFile（数据文件）
- **DataFile（数据文件）**：物理存储文件，使用 S2DF 列式格式
- **Manifest（清单）**：版本快照，记录 Schema、Fragments、配置等
- **Transaction（事务）**：原子操作，支持 Append、Delete、Overwrite
- **Version（版本）**：每次提交创建新版本，支持时间旅行查询

## 核心 API 设计

### Dataset 接口

`Dataset` 是 SDK 的核心接口，提供数据集的所有操作。

```go
type Dataset interface {
    // 关闭数据集，释放资源
    Close() error
    
    // 获取当前版本号
    Version() uint64
    
    // 获取最新版本号
    LatestVersion() (uint64, error)
    
    // 获取行数
    CountRows() (uint64, error)
    
    // 获取 Schema（未来扩展）
    // Schema() (*Schema, error)
    
    // 追加数据
    Append(ctx context.Context, fragments []*DataFragment) error
    
    // 删除数据
    Delete(ctx context.Context, predicate string) error
    
    // 覆盖数据（创建或替换表）
    Overwrite(ctx context.Context, fragments []*DataFragment) error
    
    // 读取数据（未来扩展）
    // Scanner() *Scanner
}
```

### 打开数据集

使用 Builder 模式打开数据集：

```go
// 从路径打开数据集
dataset, err := OpenDataset(ctx, basePath).
    WithVersion(version).        // 可选：指定版本，默认最新
    WithCommitHandler(handler).   // 可选：自定义 CommitHandler
    Build()

defer dataset.Close()
```

### 创建数据集

创建空数据集或从数据创建：

```go
// 创建空数据集
dataset, err := CreateDataset(ctx, basePath, schema).
    WithCommitHandler(handler).
    Build()

// 从数据创建数据集（未来扩展）
// dataset, err := WriteDataset(ctx, basePath).
//     WithData(reader).
//     WithSchema(schema).
//     Build()
```

## 配置选项

### ReadOptions（读取选项）

```go
type ReadOptions struct {
    Version  *uint64              // 指定版本号，nil 表示最新版本
    BlockSize *int                // I/O 块大小（字节）
}

// 使用函数式选项模式
func WithVersion(v uint64) ReadOption {
    return func(opts *ReadOptions) {
        opts.Version = &v
    }
}

func WithBlockSize(size int) ReadOption {
    return func(opts *ReadOptions) {
        opts.BlockSize = &size
    }
}
```

### WriteOptions（写入选项）

```go
type WriteOptions struct {
    MaxRowsPerFile  *uint64       // 每个文件最大行数
    MaxBytesPerFile *uint64       // 每个文件最大字节数
    Mode            WriteMode     // 写入模式：CREATE, APPEND, OVERWRITE
}

type WriteMode int

const (
    WriteModeCreate WriteMode = iota
    WriteModeAppend
    WriteModeOverwrite
)
```

## 写入操作

### Append（追加数据）

```go
// 创建数据文件
dataFile := NewDataFile(
    "data/1.dat",           // 相对路径
    []int32{0, 1},          // 字段 ID
    1,                      // 主版本号
    0,                      // 次版本号
)

// 创建 Fragment
fragment := NewDataFragmentWithRows(
    0,                      // Fragment ID（会自动分配）
    100,                    // 物理行数
    []*DataFile{dataFile},  // 数据文件列表
)

// 追加到数据集
err := dataset.Append(ctx, []*DataFragment{fragment})
if err != nil {
    if err == ErrConflict {
        // 处理事务冲突
    }
    return err
}
```

### Delete（删除数据）

```go
// 删除满足条件的数据
err := dataset.Delete(ctx, "id > 100")
if err != nil {
    return err
}
```

### Overwrite（覆盖数据）

```go
// 覆盖整个数据集
err := dataset.Overwrite(ctx, []*DataFragment{fragment})
if err != nil {
    return err
}
```

## 数据文件操作

### 写入数据文件

SDK 提供高级封装，简化数据文件写入：

```go
// 方式1：直接写入列数据
writer, err := CreateDataFileWriter(path, numRows, numCols)
if err != nil {
    return err
}
defer writer.Close()

// 写入各列数据
for i := 0; i < numCols; i++ {
    colData := prepareColumnData(i) // 准备列数据
    if err := writer.WriteColumn(colData); err != nil {
        return err
    }
}

// 方式2：使用高级 API（未来扩展）
// writer := NewColumnWriter(path, schema)
// writer.WriteBatch(batch)
// writer.Close()
```

### 读取数据文件

```go
// 打开数据文件
reader, err := OpenDataFile(path)
if err != nil {
    return err
}

// 获取元数据
numRows := reader.NumRows()
numCols := reader.NumColumns()

// 读取列数据
for i := uint32(0); i < numCols; i++ {
    colData, err := reader.ReadColumn(i)
    if err != nil {
        return err
    }
    // 处理列数据
    processColumn(colData)
}
```

## 完整示例

### 示例1：创建数据集并写入数据

```go
package main

import (
    "context"
    "encoding/binary"
    "fmt"
    "log"
    
    "github.com/daviszhen/plan/pkg/storage2"
)

func main() {
    ctx := context.Background()
    basePath := "/tmp/my_dataset"
    
    // 1. 创建空数据集
    handler := storage2.NewLocalRenameCommitHandler()
    dataset, err := CreateDataset(ctx, basePath).
        WithCommitHandler(handler).
        Build()
    if err != nil {
        log.Fatal(err)
    }
    defer dataset.Close()
    
    // 2. 准备数据文件
    dataPath := "data/0.dat"
    numRows := uint64(100)
    numCols := uint32(2)
    
    writer, err := storage2.CreateDataFile(
        storage2.JoinPath(basePath, dataPath),
        numRows,
        numCols,
    )
    if err != nil {
        log.Fatal(err)
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
    
    // 3. 创建 Fragment 并追加
    dataFile := storage2.NewDataFile(
        dataPath,
        []int32{0, 1},
        1, 0,
    )
    
    fragment := storage2.NewDataFragmentWithRows(
        0,
        numRows,
        []*storage2.DataFile{dataFile},
    )
    
    if err := dataset.Append(ctx, []*storage2.DataFragment{fragment}); err != nil {
        log.Fatal(err)
    }
    
    // 4. 验证
    version, _ := dataset.Version()
    count, _ := dataset.CountRows()
    fmt.Printf("Version: %d, Rows: %d\n", version, count)
}
```

### 示例2：读取数据集

```go
func readDataset() {
    ctx := context.Background()
    basePath := "/tmp/my_dataset"
    
    // 打开数据集（指定版本）
    dataset, err := OpenDataset(ctx, basePath).
        WithVersion(1).  // 读取版本 1
        Build()
    if err != nil {
        log.Fatal(err)
    }
    defer dataset.Close()
    
    // 获取信息
    version := dataset.Version()
    count, _ := dataset.CountRows()
    fmt.Printf("Version: %d, Rows: %d\n", version, count)
    
    // 读取数据文件（通过 Manifest 获取 Fragment 信息）
    // 未来扩展：提供 Scanner API 直接读取数据
}
```

### 示例3：删除数据

```go
func deleteData() {
    ctx := context.Background()
    basePath := "/tmp/my_dataset"
    
    dataset, err := OpenDataset(ctx, basePath).Build()
    if err != nil {
        log.Fatal(err)
    }
    defer dataset.Close()
    
    // 删除满足条件的数据
    if err := dataset.Delete(ctx, "id > 50"); err != nil {
        log.Fatal(err)
    }
    
    count, _ := dataset.CountRows()
    fmt.Printf("Rows after delete: %d\n", count)
}
```

## 实现细节

### Dataset 实现

```go
type datasetImpl struct {
    basePath     string
    handler      CommitHandler
    currentManifest *Manifest
    version      uint64
    closed       bool
}

func (d *datasetImpl) Close() error {
    if d.closed {
        return nil
    }
    d.closed = true
    // 清理资源
    return nil
}

func (d *datasetImpl) Version() uint64 {
    return d.version
}

func (d *datasetImpl) LatestVersion() (uint64, error) {
    return d.handler.ResolveLatestVersion(context.Background(), d.basePath)
}

func (d *datasetImpl) CountRows() (uint64, error) {
    if d.currentManifest == nil {
        return 0, nil
    }
    var total uint64
    for _, frag := range d.currentManifest.Fragments {
        total += frag.PhysicalRows
    }
    return total, nil
}

func (d *datasetImpl) Append(ctx context.Context, fragments []*DataFragment) error {
    readVersion := d.version
    uuid := generateUUID()
    
    txn := NewTransactionAppend(readVersion, uuid, fragments)
    if err := CommitTransaction(ctx, d.basePath, d.handler, txn); err != nil {
        return err
    }
    
    // 重新加载 manifest
    latest, _ := d.handler.ResolveLatestVersion(ctx, d.basePath)
    manifest, err := LoadManifest(ctx, d.basePath, d.handler, latest)
    if err != nil {
        return err
    }
    
    d.currentManifest = manifest
    d.version = latest
    return nil
}

func (d *datasetImpl) Delete(ctx context.Context, predicate string) error {
    readVersion := d.version
    uuid := generateUUID()
    
    // 简化实现：删除所有片段（实际应该根据 predicate 过滤）
    var deletedIds []uint64
    for _, frag := range d.currentManifest.Fragments {
        deletedIds = append(deletedIds, frag.Id)
    }
    
    txn := NewTransactionDelete(readVersion, uuid, nil, deletedIds, predicate)
    if err := CommitTransaction(ctx, d.basePath, d.handler, txn); err != nil {
        return err
    }
    
    latest, _ := d.handler.ResolveLatestVersion(ctx, d.basePath)
    manifest, err := LoadManifest(ctx, d.basePath, d.handler, latest)
    if err != nil {
        return err
    }
    
    d.currentManifest = manifest
    d.version = latest
    return nil
}

func (d *datasetImpl) Overwrite(ctx context.Context, fragments []*DataFragment) error {
    readVersion := d.version
    uuid := generateUUID()
    
    txn := NewTransactionOverwrite(readVersion, uuid, fragments, nil, nil)
    if err := CommitTransaction(ctx, d.basePath, d.handler, txn); err != nil {
        return err
    }
    
    latest, _ := d.handler.ResolveLatestVersion(ctx, d.basePath)
    manifest, err := LoadManifest(ctx, d.basePath, d.handler, latest)
    if err != nil {
        return err
    }
    
    d.currentManifest = manifest
    d.version = latest
    return nil
}
```

### Builder 实现

```go
type datasetBuilder struct {
    basePath     string
    version      *uint64
    handler      CommitHandler
    readOptions  *ReadOptions
}

func OpenDataset(ctx context.Context, basePath string) *datasetBuilder {
    return &datasetBuilder{
        basePath: basePath,
        handler: NewLocalRenameCommitHandler(),
    }
}

func (b *datasetBuilder) WithVersion(v uint64) *datasetBuilder {
    b.version = &v
    return b
}

func (b *datasetBuilder) WithCommitHandler(handler CommitHandler) *datasetBuilder {
    b.handler = handler
    return b
}

func (b *datasetBuilder) Build() (Dataset, error) {
    var version uint64
    if b.version != nil {
        version = *b.version
    } else {
        var err error
        version, err = b.handler.ResolveLatestVersion(context.Background(), b.basePath)
        if err != nil {
            return nil, err
        }
    }
    
    manifest, err := LoadManifest(context.Background(), b.basePath, b.handler, version)
    if err != nil {
        return nil, err
    }
    
    return &datasetImpl{
        basePath:        b.basePath,
        handler:         b.handler,
        currentManifest: manifest,
        version:         version,
    }, nil
}

func CreateDataset(ctx context.Context, basePath string) *datasetBuilder {
    return &datasetBuilder{
        basePath: basePath,
        handler: NewLocalRenameCommitHandler(),
    }
}

func (b *datasetBuilder) Build() (Dataset, error) {
    // 创建初始 manifest
    m0 := NewManifest(0)
    m0.Fragments = []*DataFragment{}
    m0.NextRowId = 1
    
    if err := b.handler.Commit(context.Background(), b.basePath, 0, m0); err != nil {
        return nil, err
    }
    
    return &datasetImpl{
        basePath:        b.basePath,
        handler:         b.handler,
        currentManifest: m0,
        version:         0,
    }, nil
}
```

## 高级功能接口设计

### 1. Schema 支持

#### Schema 类型定义

```go
// Schema 表示数据集的表结构
type Schema struct {
    Fields []Field
    Metadata map[string]string  // 可选的元数据
}

// Field 表示一个字段
type Field struct {
    Name     string              // 字段名
    Type     FieldType           // 字段类型
    Nullable bool                // 是否可为空
    Metadata map[string]string   // 字段元数据
}

// FieldType 表示字段类型
type FieldType interface {
    fieldType()
}

// 基本类型
type Int32Type struct{}
type Int64Type struct{}
type Float32Type struct{}
type Float64Type struct{}
type StringType struct{}
type BytesType struct{}

// 复合类型
type ListType struct {
    ElementType FieldType
}

type StructType struct {
    Fields []Field
}

// 实现 FieldType 接口
func (Int32Type) fieldType() {}
func (Int64Type) fieldType() {}
func (Float32Type) fieldType() {}
func (Float64Type) fieldType() {}
func (StringType) fieldType() {}
func (BytesType) fieldType() {}
func (ListType) fieldType() {}
func (StructType) fieldType() {}
```

#### Schema 操作方法

```go
// Dataset 接口扩展
type Dataset interface {
    // ... 现有方法 ...
    
    // 获取当前 Schema
    Schema() (*Schema, error)
    
    // 添加新列（通过表达式或 Schema）
    AddColumns(ctx context.Context, schema *Schema) error
    AddColumnsByExpressions(ctx context.Context, expressions map[string]string) error
    
    // 修改列
    AlterColumns(ctx context.Context, alterations []ColumnAlteration) error
    
    // 删除列
    DropColumns(ctx context.Context, columnNames []string) error
}

// ColumnAlteration 表示列修改操作
type ColumnAlteration struct {
    Name        string            // 列名
    NewName     *string           // 新列名（重命名）
    NewType     FieldType         // 新类型（类型转换）
    Nullable    *bool             // 是否可为空
}

// 使用示例
func schemaExample() {
    dataset, _ := OpenDataset(ctx, basePath).Build()
    defer dataset.Close()
    
    // 获取 Schema
    schema, _ := dataset.Schema()
    fmt.Printf("Schema has %d fields\n", len(schema.Fields))
    
    // 添加新列
    newSchema := &Schema{
        Fields: []Field{
            {Name: "age", Type: Int32Type{}, Nullable: true},
            {Name: "email", Type: StringType{}, Nullable: false},
        },
    }
    dataset.AddColumns(ctx, newSchema)
    
    // 通过表达式添加列
    expressions := map[string]string{
        "double_id": "id * 2",
        "full_name": "first_name || ' ' || last_name",
    }
    dataset.AddColumnsByExpressions(ctx, expressions)
    
    // 修改列
    alterations := []ColumnAlteration{
        {
            Name: "name",
            NewName: stringPtr("full_name"),
            Nullable: boolPtr(true),
        },
    }
    dataset.AlterColumns(ctx, alterations)
    
    // 删除列
    dataset.DropColumns(ctx, []string{"old_column"})
}

func stringPtr(s string) *string { return &s }
func boolPtr(b bool) *bool { return &b }
```

### 高级功能实现优先级

以上高级功能的实现建议按以下优先级进行：

1. **Schema 支持**（P0）
   - 基础功能，其他功能依赖
   - 需要扩展 Manifest 和 Transaction 支持 Schema 操作
   - 实现 AddColumns、AlterColumns、DropColumns

2. **Scanner API**（P0）
   - 核心读取功能，用户最常用
   - 需要实现数据文件读取和过滤逻辑
   - 支持流式处理，内存效率高

3. **随机访问**（P1）
   - 重要功能，支持点查询
   - 需要实现 Fragment 定位和行索引映射
   - 可优化为批量预取和并行读取

4. **索引支持**（P2）
   - 高级功能，提升查询性能
   - 需要扩展 Transaction 支持 CreateIndex 操作
   - 向量索引需要额外的数学库支持

## 未来扩展

### 1. Schema 支持

```go
type Schema struct {
    Fields []Field
}

type Field struct {
    Name string
    Type FieldType
    Nullable bool
}

// 数据集 Schema 操作
func (d *datasetImpl) Schema() (*Schema, error) { ... }
func (d *datasetImpl) AddColumns(schema *Schema) error { ... }
func (d *datasetImpl) AlterColumns(alterations []ColumnAlteration) error { ... }
func (d *datasetImpl) DropColumns(columnNames []string) error { ... }
```

### 2. Scanner API（数据扫描）

#### Scanner 接口定义

```go
// Scanner 提供流式数据扫描接口
type Scanner interface {
    // Next 移动到下一行，返回 false 表示没有更多数据
    Next() bool
    
    // Scan 扫描当前行的数据到提供的变量中
    Scan(dest ...interface{}) error
    
    // GetRow 获取当前行作为 Record（结构化数据）
    GetRow() (*Record, error)
    
    // Close 关闭 Scanner，释放资源
    Close() error
    
    // Err 返回扫描过程中的错误
    Err() error
}

// Record 表示一行数据
type Record struct {
    Values map[string]interface{}
}

// ScannerBuilder 用于构建 Scanner
type ScannerBuilder struct {
    dataset  Dataset
    filter   string              // 过滤条件（SQL WHERE 子句）
    columns  []string            // 要读取的列（nil 表示所有列）
    limit    *int                // 限制返回行数
    offset   *int                // 偏移量
    batchSize int                // 批处理大小
}

// 使用示例
func scannerExample() {
    dataset, _ := OpenDataset(ctx, basePath).Build()
    defer dataset.Close()
    
    // 创建 Scanner
    scanner := dataset.Scanner().
        WithFilter("id > 100 AND age < 50").
        WithColumns("id", "name", "age").
        WithLimit(1000).
        WithBatchSize(100).
        Build()
    defer scanner.Close()
    
    // 方式1：使用 Scan
    for scanner.Next() {
        var id int32
        var name string
        var age int32
        if err := scanner.Scan(&id, &name, &age); err != nil {
            log.Fatal(err)
        }
        fmt.Printf("ID: %d, Name: %s, Age: %d\n", id, name, age)
    }
    
    if err := scanner.Err(); err != nil {
        log.Fatal(err)
    }
    
    // 方式2：使用 GetRow
    scanner2 := dataset.Scanner().
        WithFilter("status = 'active'").
        Build()
    defer scanner2.Close()
    
    for scanner2.Next() {
        row, _ := scanner2.GetRow()
        fmt.Printf("Row: %+v\n", row.Values)
    }
}
```

#### Scanner 实现细节

```go
// scannerImpl 实现 Scanner 接口
type scannerImpl struct {
    dataset     Dataset
    filter      string
    columns     []string
    limit       *int
    offset      *int
    batchSize   int
    
    currentBatch []*Record
    currentIdx   int
    hasMore      bool
    err          error
}

func (s *scannerImpl) Next() bool {
    if s.err != nil {
        return false
    }
    
    // 如果当前批次已读完，加载下一批次
    if s.currentIdx >= len(s.currentBatch) {
        if !s.hasMore {
            return false
        }
        if err := s.loadNextBatch(); err != nil {
            s.err = err
            return false
        }
    }
    
    s.currentIdx++
    return s.currentIdx <= len(s.currentBatch)
}

func (s *scannerImpl) Scan(dest ...interface{}) error {
    if s.currentIdx == 0 || s.currentIdx > len(s.currentBatch) {
        return fmt.Errorf("no current row")
    }
    
    row := s.currentBatch[s.currentIdx-1]
    // 根据列顺序填充 dest
    // 实现细节：根据 Schema 和列类型进行类型转换
    return s.scanRow(row, dest)
}

func (s *scannerImpl) GetRow() (*Record, error) {
    if s.currentIdx == 0 || s.currentIdx > len(s.currentBatch) {
        return nil, fmt.Errorf("no current row")
    }
    return s.currentBatch[s.currentIdx-1], nil
}

func (s *scannerImpl) loadNextBatch() error {
    // 从数据文件加载下一批数据
    // 实现细节：读取 Fragment，应用过滤条件，转换为 Record
    return nil
}
```

#### Dataset 接口扩展

```go
// Dataset 接口添加 Scanner 方法
type Dataset interface {
    // ... 现有方法 ...
    
    // Scanner 创建 ScannerBuilder
    Scanner() *ScannerBuilder
}

// ScannerBuilder 方法
func (b *ScannerBuilder) WithFilter(filter string) *ScannerBuilder {
    b.filter = filter
    return b
}

func (b *ScannerBuilder) WithColumns(columns ...string) *ScannerBuilder {
    b.columns = columns
    return b
}

func (b *ScannerBuilder) WithLimit(limit int) *ScannerBuilder {
    b.limit = &limit
    return b
}

func (b *ScannerBuilder) WithOffset(offset int) *ScannerBuilder {
    b.offset = &offset
    return b
}

func (b *ScannerBuilder) WithBatchSize(size int) *ScannerBuilder {
    b.batchSize = size
    return b
}

func (b *ScannerBuilder) Build() Scanner {
    return &scannerImpl{
        dataset:   b.dataset,
        filter:    b.filter,
        columns:   b.columns,
        limit:     b.limit,
        offset:    b.offset,
        batchSize: b.batchSize,
        hasMore:   true,
    }
}
```

### 3. 随机访问

#### 随机访问接口

```go
// RecordBatch 表示一批记录
type RecordBatch struct {
    Records []*Record
    Schema  *Schema
}

// Dataset 接口扩展
type Dataset interface {
    // ... 现有方法 ...
    
    // Take 根据行索引随机访问数据
    Take(ctx context.Context, indices []uint64, columns []string) (*RecordBatch, error)
    
    // TakeByRowId 根据行 ID 访问数据（如果启用了稳定行 ID）
    TakeByRowId(ctx context.Context, rowIds []uint64, columns []string) (*RecordBatch, error)
    
    // GetRow 获取单行数据
    GetRow(ctx context.Context, index uint64, columns []string) (*Record, error)
}

// 使用示例
func randomAccessExample() {
    dataset, _ := OpenDataset(ctx, basePath).Build()
    defer dataset.Close()
    
    // 方式1：根据行索引访问
    indices := []uint64{0, 5, 10, 100}
    columns := []string{"id", "name", "age"}
    
    batch, err := dataset.Take(ctx, indices, columns)
    if err != nil {
        log.Fatal(err)
    }
    
    for i, record := range batch.Records {
        fmt.Printf("Row %d: %+v\n", indices[i], record.Values)
    }
    
    // 方式2：根据行 ID 访问（稳定行 ID）
    rowIds := []uint64{1001, 1002, 1003}
    batch2, err := dataset.TakeByRowId(ctx, rowIds, columns)
    if err != nil {
        log.Fatal(err)
    }
    
    // 方式3：获取单行
    row, err := dataset.GetRow(ctx, 42, []string{"id", "name"})
    if err != nil {
        log.Fatal(err)
    }
    fmt.Printf("Row 42: %+v\n", row.Values)
}
```

#### 随机访问实现

```go
// takeImpl 实现随机访问逻辑
func (d *datasetImpl) Take(ctx context.Context, indices []uint64, columns []string) (*RecordBatch, error) {
    if d.currentManifest == nil {
        return nil, fmt.Errorf("dataset not initialized")
    }
    
    // 1. 根据索引确定所属的 Fragment
    fragmentMap := make(map[uint64][]uint64) // fragmentId -> indices
    rowOffset := uint64(0)
    
    for _, frag := range d.currentManifest.Fragments {
        fragStart := rowOffset
        fragEnd := rowOffset + frag.PhysicalRows
        
        for _, idx := range indices {
            if idx >= fragStart && idx < fragEnd {
                localIdx := idx - fragStart
                fragmentMap[frag.Id] = append(fragmentMap[frag.Id], localIdx)
            }
        }
        
        rowOffset = fragEnd
    }
    
    // 2. 从各个 Fragment 读取数据
    var allRecords []*Record
    for fragId, localIndices := range fragmentMap {
        frag := d.findFragment(fragId)
        if frag == nil {
            continue
        }
        
        records, err := d.readFragmentRows(ctx, frag, localIndices, columns)
        if err != nil {
            return nil, err
        }
        allRecords = append(allRecords, records...)
    }
    
    // 3. 按原始索引顺序排序
    sortedRecords := d.sortRecordsByIndices(allRecords, indices)
    
    return &RecordBatch{
        Records: sortedRecords,
        Schema:  d.buildSchemaForColumns(columns),
    }, nil
}

func (d *datasetImpl) readFragmentRows(ctx context.Context, frag *DataFragment, indices []uint64, columns []string) ([]*Record, error) {
    // 从 Fragment 的数据文件读取指定行的数据
    // 实现细节：
    // 1. 打开数据文件
    // 2. 根据列类型和索引计算偏移量
    // 3. 读取数据并转换为 Record
    return nil, nil
}
```

#### 性能优化

```go
// 批量随机访问优化
type RandomAccessOptions struct {
    PrefetchSize    int      // 预取大小
    UseCache        bool     // 是否使用缓存
    ParallelReads   bool     // 是否并行读取多个 Fragment
}

func (d *datasetImpl) TakeWithOptions(
    ctx context.Context,
    indices []uint64,
    columns []string,
    opts *RandomAccessOptions,
) (*RecordBatch, error) {
    // 使用选项优化随机访问性能
    // - 预取相邻数据
    // - 使用缓存减少 I/O
    // - 并行读取多个 Fragment
    return nil, nil
}
```

### 4. 索引支持

#### 索引类型定义

```go
// IndexType 表示索引类型
type IndexType int

const (
    IndexTypeBTree IndexType = iota      // B-Tree 索引（标量）
    IndexTypeInverted                    // 倒排索引（全文搜索）
    IndexTypeVectorIVF                   // IVF 向量索引
    IndexTypeVectorHNSW                  // HNSW 向量索引
    IndexTypeBitmap                     // 位图索引
    IndexTypeZoneMap                    // Zone Map 索引
)

// IndexParams 索引参数接口
type IndexParams interface {
    indexParams()
}

// BTreeIndexParams B-Tree 索引参数
type BTreeIndexParams struct {
    Columns []string
}

func (BTreeIndexParams) indexParams() {}

// InvertedIndexParams 倒排索引参数
type InvertedIndexParams struct {
    Columns []string
    NGramSize *int  // N-gram 大小（用于文本索引）
}

func (InvertedIndexParams) indexParams() {}

// VectorIndexParams 向量索引参数
type VectorIndexParams struct {
    Column      string
    IndexType   IndexType  // IVF 或 HNSW
    Dimension   int        // 向量维度
    Metric      VectorMetric  // 距离度量
}

type VectorMetric int

const (
    VectorMetricL2 VectorMetric = iota
    VectorMetricCosine
    VectorMetricDot
)

// IVF 特定参数
type IVFParams struct {
    NumPartitions int  // 分区数量
    NumSubVectors int  // PQ 子向量数量（可选）
}

// HNSW 特定参数
type HNSWParams struct {
    M              int  // 每个节点的连接数
    EfConstruction int // 构建时的搜索范围
    EfSearch       int // 搜索时的搜索范围
}

func (VectorIndexParams) indexParams() {}
```

#### 索引操作方法

```go
// Dataset 接口扩展
type Dataset interface {
    // ... 现有方法 ...
    
    // CreateIndex 创建索引
    CreateIndex(ctx context.Context, params IndexParams) error
    
    // ListIndexes 列出所有索引
    ListIndexes(ctx context.Context) ([]*IndexDescription, error)
    
    // DropIndex 删除索引
    DropIndex(ctx context.Context, indexName string) error
    
    // VectorSearch 向量相似度搜索
    VectorSearch(ctx context.Context, query []float32, column string, limit int) (*RecordBatch, error)
    
    // Search 标量搜索（使用索引加速）
    Search(ctx context.Context, predicate string, limit int) (*RecordBatch, error)
}

// IndexDescription 索引描述
type IndexDescription struct {
    Name    string
    Type    IndexType
    Columns []string
    Status  IndexStatus
}

type IndexStatus int

const (
    IndexStatusBuilding IndexStatus = iota
    IndexStatusReady
    IndexStatusFailed
)

// 使用示例
func indexExample() {
    dataset, _ := OpenDataset(ctx, basePath).Build()
    defer dataset.Close()
    
    // 1. 创建 B-Tree 索引
    btreeParams := BTreeIndexParams{
        Columns: []string{"id", "age"},
    }
    if err := dataset.CreateIndex(ctx, btreeParams); err != nil {
        log.Fatal(err)
    }
    
    // 2. 创建倒排索引（全文搜索）
    invertedParams := InvertedIndexParams{
        Columns:  []string{"description", "title"},
        NGramSize: intPtr(3),
    }
    if err := dataset.CreateIndex(ctx, invertedParams); err != nil {
        log.Fatal(err)
    }
    
    // 3. 创建向量索引
    vectorParams := VectorIndexParams{
        Column:    "embedding",
        IndexType: IndexTypeVectorIVF,
        Dimension: 128,
        Metric:    VectorMetricCosine,
    }
    if err := dataset.CreateIndex(ctx, vectorParams); err != nil {
        log.Fatal(err)
    }
    
    // 4. 列出所有索引
    indexes, _ := dataset.ListIndexes(ctx)
    for _, idx := range indexes {
        fmt.Printf("Index: %s, Type: %v, Status: %v\n", 
            idx.Name, idx.Type, idx.Status)
    }
    
    // 5. 向量搜索
    queryVector := make([]float32, 128)
    // ... 填充查询向量 ...
    results, err := dataset.VectorSearch(ctx, queryVector, "embedding", 10)
    if err != nil {
        log.Fatal(err)
    }
    fmt.Printf("Found %d similar vectors\n", len(results.Records))
    
    // 6. 使用索引加速搜索
    results2, err := dataset.Search(ctx, "age > 30 AND age < 50", 100)
    if err != nil {
        log.Fatal(err)
    }
    
    // 7. 删除索引
    dataset.DropIndex(ctx, "my_index")
}

func intPtr(i int) *int { return &i }
```

#### 索引实现细节

```go
// createIndexImpl 实现索引创建
func (d *datasetImpl) CreateIndex(ctx context.Context, params IndexParams) error {
    // 1. 验证参数
    if err := d.validateIndexParams(params); err != nil {
        return err
    }
    
    // 2. 创建索引事务
    readVersion := d.version
    uuid := generateUUID()
    
    // 3. 构建索引操作（未来扩展 Transaction 支持 CreateIndex）
    // 目前需要通过 Overwrite 或 Append 操作间接实现
    // 或者扩展 Transaction 类型
    
    // 4. 提交事务
    // 5. 更新 manifest
    return nil
}

// vectorSearchImpl 实现向量搜索
func (d *datasetImpl) VectorSearch(
    ctx context.Context,
    query []float32,
    column string,
    limit int,
) (*RecordBatch, error) {
    // 1. 查找向量索引
    index := d.findVectorIndex(column)
    if index == nil {
        return nil, fmt.Errorf("no vector index found for column: %s", column)
    }
    
    // 2. 根据索引类型执行搜索
    switch index.Type {
    case IndexTypeVectorIVF:
        return d.ivfSearch(ctx, query, index, limit)
    case IndexTypeVectorHNSW:
        return d.hnswSearch(ctx, query, index, limit)
    default:
        return nil, fmt.Errorf("unsupported vector index type: %v", index.Type)
    }
}

// ivfSearch IVF 索引搜索
func (d *datasetImpl) ivfSearch(
    ctx context.Context,
    query []float32,
    index *IndexDescription,
    limit int,
) (*RecordBatch, error) {
    // 1. 加载索引文件
    // 2. 计算查询向量与各分区的距离
    // 3. 选择最相关的分区
    // 4. 在分区内搜索最相似的向量
    // 5. 返回 top-k 结果
    return nil, nil
}
```

#### 索引构建选项

```go
// IndexBuildOptions 索引构建选项
type IndexBuildOptions struct {
    NumThreads    int      // 构建线程数
    ProgressCallback func(progress float64)  // 进度回调
    CancelChan    <-chan struct{}  // 取消通道
}

func (d *datasetImpl) CreateIndexWithOptions(
    ctx context.Context,
    params IndexParams,
    opts *IndexBuildOptions,
) error {
    // 使用选项控制索引构建过程
    return nil
}
```

### 5. 对象存储支持

```go
// S3 支持
s3Handler := NewS3CommitHandler(bucket, region, credentials)

dataset, err := OpenDataset(ctx, "s3://bucket/path").
    WithCommitHandler(s3Handler).
    Build()
```

## 与底层 API 的关系

SDK 是对底层 `storage2` 包的封装：

- **底层 API**：提供基础功能（`CommitTransaction`, `LoadManifest`, `DataFileWriter/Reader` 等）
- **SDK API**：提供高级抽象（`Dataset`, `Scanner`, `Builder` 等）

SDK 内部调用底层 API，但提供更友好的接口和更好的错误处理。

## 总结

Storage2 Golang SDK 设计遵循以下原则：

1. **渐进式开发**：先实现核心功能（Dataset、Append、Delete、Overwrite），再扩展高级特性
2. **向后兼容**：底层 API 保持不变，SDK 作为可选的高级封装
3. **Go 惯用法**：使用接口、函数式选项、明确的错误处理
4. **参考 Lance Java SDK**：保持 API 设计的一致性，便于用户迁移和理解

通过 SDK，用户可以更简单地使用 storage2，而无需深入了解底层的事务、冲突检测、版本管理等细节。
```
```
```
