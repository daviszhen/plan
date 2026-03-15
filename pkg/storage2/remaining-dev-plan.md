# 剩余未开发功能开发计划

基于代码库 TODO/FIXME 分析，识别出以下未完成功能。

---

## 概览：TODO 清单

| 优先级 | 功能模块 | 文件位置 | 预估代码量 |
|--------|----------|----------|------------|
| **P1** | LZ4/Zstd 压缩 | `lance_v2_column.go:578-600` | ~200 行 |
| **P1** | 索引构建持久化 | `index_transaction.go:357,379` | ~400 行 |
| **P2** | 并行 I/O | `io_ext.go:296,319` | ~300 行 |
| **P3** | 远程压缩 Worker | `compaction_worker.go:415` | ~800 行 |
| **P4** | Update ZoneMap 优化 | `update.go:226` | ~100 行 |
| **P4** | V1→V2 格式迁移 | `table_format.go:240` | ~300 行 |
| **P5** | DynamoDB 外部锁 | (新功能) | ~400 行 |

---

## P1-1: LZ4/Zstd 压缩库集成

### 现状

`lance_v2_column.go:578-600` 有 4 个桩函数，直接返回原始数据：

```go
func compressLZ4(data []byte) ([]byte, error) {
    // TODO: Implement with lz4 library
    return data, nil  // 未压缩
}

func decompressLZ4(data []byte, uncompressedSize int) ([]byte, error) {
    // TODO: Implement with lz4 library
    return data, nil
}

func compressZstd(data []byte) ([]byte, error) {
    // TODO: Implement with zstd library
    return data, nil
}

func decompressZstd(data []byte, uncompressedSize int) ([]byte, error) {
    // TODO: Implement with zstd library
    return data, nil
}
```

### 重要性

- **数据压缩** 是 V2 文件格式的核心特性
- 直接影响存储成本和 I/O 性能
- Lance Rust 默认使用 Zstd 压缩

### 推荐库

| 库 | 特点 | go get |
|----|------|--------|
| `github.com/pierrec/lz4/v4` | 纯 Go，无 CGO | `go get github.com/pierrec/lz4/v4` |
| `github.com/klauspost/compress/zstd` | 纯 Go，高性能 | `go get github.com/klauspost/compress/zstd` |

### 实现步骤

**Step 1**: 添加依赖

```bash
go get github.com/pierrec/lz4/v4
go get github.com/klauspost/compress/zstd
```

**Step 2**: 实现 LZ4 压缩/解压

```go
import "github.com/pierrec/lz4/v4"

func compressLZ4(data []byte) ([]byte, error) {
    // 预分配缓冲区
    maxSize := lz4.CompressBlockBound(len(data))
    compressed := make([]byte, maxSize)
    
    n, err := lz4.CompressBlock(data, compressed, nil)
    if err != nil {
        return nil, fmt.Errorf("lz4 compress: %w", err)
    }
    
    // 如果压缩后更大，返回原始数据
    if n == 0 || n >= len(data) {
        return data, nil
    }
    
    return compressed[:n], nil
}

func decompressLZ4(data []byte, uncompressedSize int) ([]byte, error) {
    decompressed := make([]byte, uncompressedSize)
    n, err := lz4.UncompressBlock(data, decompressed)
    if err != nil {
        return nil, fmt.Errorf("lz4 decompress: %w", err)
    }
    return decompressed[:n], nil
}
```

**Step 3**: 实现 Zstd 压缩/解压

```go
import "github.com/klauspost/compress/zstd"

var (
    zstdEncoder *zstd.Encoder
    zstdDecoder *zstd.Decoder
    zstdOnce    sync.Once
)

func initZstd() {
    zstdOnce.Do(func() {
        var err error
        zstdEncoder, err = zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
        if err != nil {
            panic(err)
        }
        zstdDecoder, err = zstd.NewReader(nil)
        if err != nil {
            panic(err)
        }
    })
}

func compressZstd(data []byte) ([]byte, error) {
    initZstd()
    return zstdEncoder.EncodeAll(data, nil), nil
}

func decompressZstd(data []byte, uncompressedSize int) ([]byte, error) {
    initZstd()
    return zstdDecoder.DecodeAll(data, nil)
}
```

**Step 4**: 添加压缩级别配置

```go
type CompressionConfig struct {
    Algorithm CompressionType  // None, LZ4, Zstd
    Level     int              // Zstd: 1-22, default 3
}

func compressWithConfig(data []byte, cfg CompressionConfig) ([]byte, error) {
    switch cfg.Algorithm {
    case CompressionNone:
        return data, nil
    case CompressionLZ4:
        return compressLZ4(data)
    case CompressionZstd:
        return compressZstdWithLevel(data, cfg.Level)
    default:
        return nil, fmt.Errorf("unknown compression: %v", cfg.Algorithm)
    }
}
```

**Step 5**: 测试

```go
func TestCompressLZ4Roundtrip(t *testing.T) {
    original := []byte("test data " + strings.Repeat("a", 10000))
    
    compressed, err := compressLZ4(original)
    if err != nil {
        t.Fatal(err)
    }
    
    decompressed, err := decompressLZ4(compressed, len(original))
    if err != nil {
        t.Fatal(err)
    }
    
    if !bytes.Equal(original, decompressed) {
        t.Error("roundtrip failed")
    }
}
```

### 预估工作量

- 代码实现: ~150 行
- 测试代码: ~100 行
- 修改文件: `lance_v2_column.go`, `go.mod`

---

## P1-2: 索引构建持久化

### 现状

`index_transaction.go:357,379` 中 `buildScalarIndex` 和 `buildVectorIndex` 只创建元数据，实际构建是 TODO：

```go
func (b *IndexBuilder) buildScalarIndex(...) (*storage2pb.IndexMetadata, error) {
    metadata := &storage2pb.IndexMetadata{
        Name:   op.IndexName,
        Fields: make([]int32, len(op.ColumnIndices)),
    }
    for i, idx := range op.ColumnIndices {
        metadata.Fields[i] = int32(idx)
    }
    // TODO: Actually build and persist the index data
    return metadata, nil
}
```

### 设计方案

```
{basePath}/_indices/{index_name}/
  ├── metadata.pb        # IndexMetadata (protobuf)
  ├── data.bin           # 序列化的索引数据
  └── stats.json         # IndexStatistics
```

### 实现步骤

**Step 1**: 定义索引序列化接口

```go
// index.go

// Serializable 索引序列化接口
type SerializableIndex interface {
    Index
    Marshal() ([]byte, error)
    Unmarshal(data []byte) error
}
```

**Step 2**: 为各索引类型实现序列化

```go
// btree_index.go

func (idx *BTreeIndex) Marshal() ([]byte, error) {
    var buf bytes.Buffer
    enc := gob.NewEncoder(&buf)
    
    // 序列化节点结构
    if err := enc.Encode(idx.root); err != nil {
        return nil, err
    }
    if err := enc.Encode(idx.stats); err != nil {
        return nil, err
    }
    
    return buf.Bytes(), nil
}

func (idx *BTreeIndex) Unmarshal(data []byte) error {
    dec := gob.NewDecoder(bytes.NewReader(data))
    
    if err := dec.Decode(&idx.root); err != nil {
        return err
    }
    return dec.Decode(&idx.stats)
}
```

**Step 3**: 实现 LocalIndexStore

```go
// index_store.go (新文件)

type LocalIndexStore struct {
    basePath string
    store    ObjectStoreExt
}

func (s *LocalIndexStore) WriteIndex(ctx context.Context, name string, idx SerializableIndex) error {
    indexDir := filepath.Join(s.basePath, "_indices", name)
    
    // 创建目录
    if err := os.MkdirAll(indexDir, 0755); err != nil {
        return err
    }
    
    // 序列化索引数据
    data, err := idx.Marshal()
    if err != nil {
        return fmt.Errorf("marshal index: %w", err)
    }
    
    // 写入数据文件
    dataPath := filepath.Join(indexDir, "data.bin")
    if err := s.store.Write(dataPath, data); err != nil {
        return err
    }
    
    // 写入统计信息
    stats := idx.Stats()
    statsData, _ := json.Marshal(stats)
    statsPath := filepath.Join(indexDir, "stats.json")
    return s.store.Write(statsPath, statsData)
}

func (s *LocalIndexStore) ReadIndex(ctx context.Context, name string, idx SerializableIndex) error {
    dataPath := filepath.Join(s.basePath, "_indices", name, "data.bin")
    
    data, err := s.store.ReadRange(ctx, dataPath, ReadOptions{})
    if err != nil {
        return err
    }
    
    return idx.Unmarshal(data)
}

func (s *LocalIndexStore) DeleteIndex(ctx context.Context, name string) error {
    indexDir := filepath.Join(s.basePath, "_indices", name)
    return os.RemoveAll(indexDir)
}

func (s *LocalIndexStore) ListIndexes(ctx context.Context) ([]string, error) {
    indicesDir := filepath.Join(s.basePath, "_indices")
    entries, err := os.ReadDir(indicesDir)
    if err != nil {
        if os.IsNotExist(err) {
            return nil, nil
        }
        return nil, err
    }
    
    var names []string
    for _, e := range entries {
        if e.IsDir() {
            names = append(names, e.Name())
        }
    }
    return names, nil
}
```

**Step 4**: 集成到 IndexBuilder

```go
// index_transaction.go

func (b *IndexBuilder) buildScalarIndex(...) (*storage2pb.IndexMetadata, error) {
    // 1. 创建元数据
    metadata := &storage2pb.IndexMetadata{...}
    
    // 2. 扫描数据构建索引
    var idx Index
    switch op.IndexType {
    case IndexTypeBTree:
        idx = NewBTreeIndex(op.IndexName, op.ColumnIndices[0])
    case IndexTypeBitmap:
        idx = NewBitmapIndex(op.IndexName, op.ColumnIndices[0])
    case IndexTypeZoneMap:
        idx = NewZoneMapIndex(op.IndexName)
    case IndexTypeBloomFilter:
        idx = NewBloomFilterIndex(op.IndexName, op.ColumnIndices[0], 0.01)
    }
    
    // 3. 遍历所有 fragment 构建索引
    for _, frag := range manifest.Fragments {
        chunks, err := ScanChunks(ctx, b.store, frag, nil)
        if err != nil {
            return nil, err
        }
        for _, chunk := range chunks {
            if err := idx.Build(chunk); err != nil {
                return nil, err
            }
        }
    }
    
    // 4. 持久化索引
    indexStore := NewLocalIndexStore(b.basePath, b.store)
    if err := indexStore.WriteIndex(ctx, op.IndexName, idx.(SerializableIndex)); err != nil {
        return nil, err
    }
    
    return metadata, nil
}
```

### 预估工作量

- `index_store.go` (新文件): ~200 行
- 各索引类型 Marshal/Unmarshal: ~200 行 (4 种索引 x 50 行)
- 测试: ~150 行
- 修改文件: `index_transaction.go`, `btree_index.go`, `bitmap_index.go`, `zonemap_index.go`, `bloomfilter_index.go`

---

## P2: 并行 I/O 实现

### 现状

`io_ext.go:296,319` 有 ParallelReader 和 ParallelWriter，但实际未实现并行：

```go
func (r *ParallelReader) Read(ctx context.Context, path string) ([]byte, error) {
    // For large files, read in parallel chunks
    // TODO: Implement parallel reading
    return r.store.ReadRange(ctx, path, ReadOptions{})
}
```

### 设计方案

```
文件大小: 100MB
分块大小: 8MB
并行度: 4

Block-0 ──┬──> goroutine-1 ──┐
Block-1 ──┼──> goroutine-2 ──┼──> 合并 -> 完整数据
Block-2 ──┼──> goroutine-3 ──┤
...       └──> goroutine-4 ──┘
```

### 实现步骤

**Step 1**: 实现并行读取

```go
// io_ext.go

func (r *ParallelReader) Read(ctx context.Context, path string) ([]byte, error) {
    // 获取文件大小
    size, err := r.store.Size(ctx, path)
    if err != nil {
        return nil, err
    }
    
    // 小文件直接读取
    if size < r.scheduler.IOBufferSize {
        return r.store.ReadRange(ctx, path, ReadOptions{})
    }
    
    // 计算分块
    chunkSize := r.scheduler.IOBufferSize
    numChunks := (size + chunkSize - 1) / chunkSize
    
    // 并行读取
    result := make([]byte, size)
    var wg sync.WaitGroup
    errChan := make(chan error, numChunks)
    
    sem := make(chan struct{}, r.scheduler.MaxConcurrency)
    
    for i := int64(0); i < numChunks; i++ {
        wg.Add(1)
        go func(idx int64) {
            defer wg.Done()
            
            sem <- struct{}{}        // 获取信号量
            defer func() { <-sem }() // 释放信号量
            
            offset := idx * chunkSize
            length := chunkSize
            if offset+length > size {
                length = size - offset
            }
            
            data, err := r.store.ReadRange(ctx, path, ReadOptions{
                Offset: offset,
                Length: length,
            })
            if err != nil {
                errChan <- err
                return
            }
            
            copy(result[offset:], data)
        }(i)
    }
    
    wg.Wait()
    close(errChan)
    
    // 检查错误
    for err := range errChan {
        if err != nil {
            return nil, err
        }
    }
    
    return result, nil
}
```

**Step 2**: 实现并行写入 (使用 multipart upload)

```go
func (w *ParallelWriter) Write(ctx context.Context, path string, data []byte) error {
    // 小文件直接写入
    if int64(len(data)) < w.scheduler.IOBufferSize {
        return w.store.Write(path, data)
    }
    
    // 检查是否支持 multipart
    if mp, ok := w.store.(MultipartUploader); ok {
        return w.writeMultipart(ctx, path, data, mp)
    }
    
    // 降级为普通写入
    return w.store.Write(path, data)
}

func (w *ParallelWriter) writeMultipart(ctx context.Context, path string, data []byte, mp MultipartUploader) error {
    uploadID, err := mp.CreateMultipartUpload(ctx, path)
    if err != nil {
        return err
    }
    
    chunkSize := w.scheduler.IOBufferSize
    numParts := (int64(len(data)) + chunkSize - 1) / chunkSize
    
    parts := make([]CompletedPart, numParts)
    var wg sync.WaitGroup
    errChan := make(chan error, numParts)
    
    sem := make(chan struct{}, w.scheduler.MaxConcurrency)
    
    for i := int64(0); i < numParts; i++ {
        wg.Add(1)
        go func(partNum int64) {
            defer wg.Done()
            
            sem <- struct{}{}
            defer func() { <-sem }()
            
            offset := partNum * chunkSize
            end := offset + chunkSize
            if end > int64(len(data)) {
                end = int64(len(data))
            }
            
            etag, err := mp.UploadPart(ctx, path, uploadID, int(partNum+1), data[offset:end])
            if err != nil {
                errChan <- err
                return
            }
            
            parts[partNum] = CompletedPart{
                PartNumber: int(partNum + 1),
                ETag:       etag,
            }
        }(i)
    }
    
    wg.Wait()
    close(errChan)
    
    for err := range errChan {
        if err != nil {
            _ = mp.AbortMultipartUpload(ctx, path, uploadID)
            return err
        }
    }
    
    return mp.CompleteMultipartUpload(ctx, path, uploadID, parts)
}
```

### 预估工作量

- 代码实现: ~250 行
- 测试: ~100 行
- 修改文件: `io_ext.go`, `io_ext_test.go`

---

## P3: 远程压缩 Worker

### 现状

`compaction_worker.go:415` 是占位实现：

```go
func (w *RemoteCompactionWorker) Execute(...) (*CompactionResult, error) {
    return nil, fmt.Errorf("remote worker not implemented")
}
```

### 设计方案

使用 gRPC 实现远程 Worker 通信：

```
┌─────────────────┐        gRPC        ┌─────────────────┐
│   Coordinator   │  ──────────────>   │  Remote Worker  │
│                 │  <──────────────   │                 │
│ - 分发任务       │   CompactionTask   │ - 执行压缩      │
│ - 收集结果       │   CompactionResult │ - 返回新 frag   │
└─────────────────┘                    └─────────────────┘
```

### 实现步骤

**Step 1**: 定义 Proto

```protobuf
// pkg/storage2/proto/compaction.proto

syntax = "proto3";

package storage2;

service CompactionWorker {
    rpc Execute(CompactionRequest) returns (CompactionResponse);
    rpc Ping(PingRequest) returns (PingResponse);
}

message CompactionRequest {
    string task_id = 1;
    string dataset_uri = 2;
    repeated uint32 fragment_ids = 3;
    CompactionOptions options = 4;
}

message CompactionOptions {
    uint64 target_rows_per_fragment = 1;
    uint64 max_rows_per_group = 2;
    bool materialize_deletions = 3;
}

message CompactionResponse {
    string task_id = 1;
    repeated DataFragment new_fragments = 2;
    repeated uint32 rewritten_fragment_ids = 3;
    string error = 4;
}

message PingRequest {}
message PingResponse {
    bool healthy = 1;
}
```

**Step 2**: 实现 gRPC 服务端

```go
// compaction_worker_server.go (新文件)

type CompactionWorkerServer struct {
    storage2pb.UnimplementedCompactionWorkerServer
    executor *LocalCompactionWorker
}

func (s *CompactionWorkerServer) Execute(ctx context.Context, req *storage2pb.CompactionRequest) (*storage2pb.CompactionResponse, error) {
    // 1. 打开远程数据集
    store, err := NewObjectStoreFromURI(req.DatasetUri)
    if err != nil {
        return nil, err
    }
    
    // 2. 加载 manifest
    manifest, err := LoadLatestManifest(ctx, store, req.DatasetUri)
    if err != nil {
        return nil, err
    }
    
    // 3. 找到要压缩的 fragments
    var fragments []*DataFragment
    fragMap := make(map[uint32]*DataFragment)
    for _, frag := range manifest.Fragments {
        fragMap[frag.Id] = frag
    }
    for _, id := range req.FragmentIds {
        if frag, ok := fragMap[id]; ok {
            fragments = append(fragments, frag)
        }
    }
    
    // 4. 执行压缩
    task := &CompactionTask{
        Fragments: fragments,
        Options:   convertOptions(req.Options),
    }
    
    result, err := s.executor.Execute(ctx, task, nil)
    if err != nil {
        return &storage2pb.CompactionResponse{
            TaskId: req.TaskId,
            Error:  err.Error(),
        }, nil
    }
    
    // 5. 返回结果
    return &storage2pb.CompactionResponse{
        TaskId:               req.TaskId,
        NewFragments:         result.NewFragments,
        RewrittenFragmentIds: req.FragmentIds,
    }, nil
}
```

**Step 3**: 实现 gRPC 客户端

```go
// compaction_worker.go

type RemoteCompactionWorker struct {
    config RemoteWorkerConfig
    conn   *grpc.ClientConn
    client storage2pb.CompactionWorkerClient
}

func NewRemoteCompactionWorker(config RemoteWorkerConfig) (*RemoteCompactionWorker, error) {
    conn, err := grpc.Dial(config.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
    if err != nil {
        return nil, err
    }
    
    return &RemoteCompactionWorker{
        config: config,
        conn:   conn,
        client: storage2pb.NewCompactionWorkerClient(conn),
    }, nil
}

func (w *RemoteCompactionWorker) Execute(ctx context.Context, task *CompactionTask, cfg *WorkerConfig) (*CompactionResult, error) {
    req := &storage2pb.CompactionRequest{
        TaskId:      task.ID,
        DatasetUri:  task.DatasetURI,
        FragmentIds: extractFragmentIDs(task.Fragments),
        Options:     convertToProtoOptions(task.Options),
    }
    
    resp, err := w.client.Execute(ctx, req)
    if err != nil {
        return nil, fmt.Errorf("remote execute: %w", err)
    }
    
    if resp.Error != "" {
        return nil, fmt.Errorf("remote error: %s", resp.Error)
    }
    
    return &CompactionResult{
        NewFragments:         resp.NewFragments,
        RewrittenFragmentIDs: resp.RewrittenFragmentIds,
    }, nil
}

func (w *RemoteCompactionWorker) Close() error {
    if w.conn != nil {
        return w.conn.Close()
    }
    return nil
}
```

**Step 4**: 更新 Coordinator 支持远程 Worker

```go
// compaction_coordinator.go

type WorkerPool struct {
    local   []*LocalCompactionWorker
    remote  []*RemoteCompactionWorker
    taskCh  chan *CompactionTask
}

func (c *CompactionCoordinator) DispatchToWorkers(ctx context.Context, tasks []*CompactionTask) error {
    // 优先使用本地 worker，溢出到远程
    for i, task := range tasks {
        if i < len(c.pool.local) {
            go c.pool.local[i].Execute(ctx, task, c.config)
        } else {
            remoteIdx := (i - len(c.pool.local)) % len(c.pool.remote)
            go c.pool.remote[remoteIdx].Execute(ctx, task, c.config)
        }
    }
    return nil
}
```

### 预估工作量

- `compaction.proto`: ~50 行
- `compaction_worker_server.go` (新文件): ~200 行
- `compaction_worker.go` 修改: ~200 行
- `compaction_coordinator.go` 修改: ~100 行
- 测试: ~150 行
- 修改文件: `compaction_worker.go`, `compaction_coordinator.go`, 新增 2 个文件

---

## P4-1: Update 使用 ZoneMap 优化

### 现状

`update.go:226` TODO 注释：

```go
func (p *UpdatePlanner) selectFragmentsToUpdate(...) ([]*DataFragment, error) {
    // TODO: Use ZoneMap statistics for range pruning
    return manifest.Fragments, nil  // 当前返回所有 fragment
}
```

### 实现方案

```go
func (p *UpdatePlanner) selectFragmentsToUpdate(manifest *Manifest, predicate *UpdatePredicate) ([]*DataFragment, error) {
    if predicate == nil || predicate.ColumnIndex < 0 {
        return manifest.Fragments, nil
    }
    
    var selected []*DataFragment
    
    for _, frag := range manifest.Fragments {
        // 检查 fragment 级别的 ZoneMap
        if frag.ZoneMapStats != nil {
            colStats := frag.ZoneMapStats[predicate.ColumnIndex]
            if colStats != nil {
                // 根据谓词类型裁剪
                switch predicate.Op {
                case OpEqual:
                    if !zoneMapMightContain(colStats, predicate.Value) {
                        continue // 跳过不可能包含目标值的 fragment
                    }
                case OpGreaterThan:
                    if compareValues(colStats.Max, predicate.Value) <= 0 {
                        continue // max <= value，跳过
                    }
                case OpLessThan:
                    if compareValues(colStats.Min, predicate.Value) >= 0 {
                        continue // min >= value，跳过
                    }
                case OpRange:
                    if compareValues(colStats.Max, predicate.Min) < 0 ||
                       compareValues(colStats.Min, predicate.Max) > 0 {
                        continue // 范围不重叠，跳过
                    }
                }
            }
        }
        selected = append(selected, frag)
    }
    
    return selected, nil
}

func zoneMapMightContain(stats *ZoneMapColumnStats, value interface{}) bool {
    return compareValues(stats.Min, value) <= 0 && compareValues(stats.Max, value) >= 0
}
```

### 预估工作量

- 代码实现: ~80 行
- 测试: ~50 行
- 修改文件: `update.go`, `update_test.go`

---

## P4-2: V1 到 V2 格式迁移

### 现状

`table_format.go:240`:

```go
func MigrateV1ToV2(ctx context.Context, basePath string) error {
    // TODO: Implement migration from V1 to V2
    return fmt.Errorf("migration not implemented")
}
```

### 实现方案

```go
func MigrateV1ToV2(ctx context.Context, basePath string) error {
    store := NewLocalObjectStore(basePath)
    
    // 1. 检测当前版本
    format, err := DetectTableFormat(ctx, store, basePath)
    if err != nil {
        return err
    }
    
    if format.Version == FormatV2 {
        return nil // 已经是 V2
    }
    
    // 2. 加载 V1 manifest
    v1Manifest, err := loadV1Manifest(ctx, store, basePath)
    if err != nil {
        return err
    }
    
    // 3. 转换为 V2 格式
    v2Manifest := &Manifest{
        Version:          1,
        Schema:           v1Manifest.Schema,
        Fragments:        make([]*DataFragment, 0, len(v1Manifest.Fragments)),
        WriterVersion:    writerVersion(FormatV2),
        ReaderFeatureFlags: 0,
        WriterFeatureFlags: 0,
    }
    
    // 4. 转换每个 fragment
    for _, v1Frag := range v1Manifest.Fragments {
        v2Frag := &DataFragment{
            Id:             v1Frag.Id,
            Files:          convertFiles(v1Frag.Files),
            PhysicalRows:   v1Frag.PhysicalRows,
            DeletionFile:   nil,
            RowIdSequence:  generateRowIdSequence(v1Frag),
        }
        v2Manifest.Fragments = append(v2Manifest.Fragments, v2Frag)
    }
    
    // 5. 写入 V2 manifest
    handler := NewLocalRenameCommitHandler(basePath)
    return writeV2Manifest(ctx, store, handler, basePath, v2Manifest)
}
```

### 预估工作量

- 代码实现: ~250 行
- 测试: ~100 行
- 修改文件: `table_format.go`, `table_format_test.go`

---

## P5: DynamoDB 外部锁 (可选)

### 背景

Lance Rust 支持使用 DynamoDB 作为外部锁服务，实现跨节点的强一致性 commit。当前 Go 实现使用 S3 ETag 乐观锁。

### 何时需要

- 多节点同时写入同一数据集
- 需要强一致性保证
- S3 ETag 乐观锁冲突率过高

### 设计方案

```go
// dynamodb_lock.go (新文件)

type DynamoDBLock struct {
    client    *dynamodb.Client
    tableName string
    ttl       time.Duration
}

type LockItem struct {
    DatasetURI string    `dynamodbav:"pk"`
    LockID     string    `dynamodbav:"lock_id"`
    Owner      string    `dynamodbav:"owner"`
    ExpiresAt  int64     `dynamodbav:"expires_at"`
    Version    int64     `dynamodbav:"version"`
}

func (l *DynamoDBLock) Acquire(ctx context.Context, datasetURI string, owner string) (*LockItem, error) {
    item := LockItem{
        DatasetURI: datasetURI,
        LockID:     uuid.New().String(),
        Owner:      owner,
        ExpiresAt:  time.Now().Add(l.ttl).Unix(),
        Version:    1,
    }
    
    _, err := l.client.PutItem(ctx, &dynamodb.PutItemInput{
        TableName:           &l.tableName,
        Item:                marshalItem(item),
        ConditionExpression: aws.String("attribute_not_exists(pk) OR expires_at < :now"),
        ExpressionAttributeValues: map[string]types.AttributeValue{
            ":now": &types.AttributeValueMemberN{Value: strconv.FormatInt(time.Now().Unix(), 10)},
        },
    })
    
    if err != nil {
        var cce *types.ConditionalCheckFailedException
        if errors.As(err, &cce) {
            return nil, ErrLockHeld
        }
        return nil, err
    }
    
    return &item, nil
}

func (l *DynamoDBLock) Release(ctx context.Context, item *LockItem) error {
    _, err := l.client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
        TableName: &l.tableName,
        Key: map[string]types.AttributeValue{
            "pk": &types.AttributeValueMemberS{Value: item.DatasetURI},
        },
        ConditionExpression: aws.String("lock_id = :lid"),
        ExpressionAttributeValues: map[string]types.AttributeValue{
            ":lid": &types.AttributeValueMemberS{Value: item.LockID},
        },
    })
    return err
}
```

### 预估工作量

- `dynamodb_lock.go` (新文件): ~300 行
- 集成到 CommitHandler: ~100 行
- 测试: ~100 行

---

## 执行优先级总览

```
P1: 核心功能补全 (必须)
├── P1-1: LZ4/Zstd 压缩 (~200 行) ← 开始
└── P1-2: 索引构建持久化 (~400 行)

P2: I/O 性能优化
└── P2-1: 并行 I/O (~300 行)

P3: 分布式能力
└── P3-1: 远程压缩 Worker (~800 行)

P4: 优化与兼容性
├── P4-1: Update ZoneMap 优化 (~100 行)
└── P4-2: V1→V2 迁移 (~300 行)

P5: 可选增强
└── P5-1: DynamoDB 外部锁 (~400 行)
```

---

## 依赖关系图

```
P1-1 (压缩) ─────────────────────────────> 无依赖
P1-2 (索引持久化) ───────────────────────> 无依赖

P2-1 (并行 I/O) ─────────────────────────> 无依赖

P3-1 (远程 Worker) ──────────────────────> P1-1 (可选，压缩后再传输)

P4-1 (ZoneMap 优化) ─────────────────────> 无依赖
P4-2 (V1→V2 迁移) ───────────────────────> 无依赖

P5-1 (DynamoDB 锁) ──────────────────────> 无依赖
```

---

## 总预估代码量

| 阶段 | 新增文件 | 修改文件 | 代码行数 |
|------|----------|----------|----------|
| P1-1 | 0 | 2 | ~200 |
| P1-2 | 1 | 5 | ~550 |
| P2-1 | 0 | 2 | ~350 |
| P3-1 | 3 | 2 | ~700 |
| P4-1 | 0 | 2 | ~130 |
| P4-2 | 0 | 2 | ~350 |
| P5-1 | 1 | 2 | ~500 |
| **合计** | **5** | **~17** | **~2780** |
