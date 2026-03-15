# Lance Rust vs Go storage2 功能一对一对比

## 1. 概述

本文档详细对比 Lance Rust 代码库 (`/Users/pengzhen/Documents/GitHub/lance/rust`) 与 Go `pkg/storage2` 模块的功能对应关系。

**更新日期**: 2026-03-15

### 1.1 架构对比

```
Lance Rust (多 crate 工作区)                Go storage2 (单模块)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

lance::Dataset (高层 API)                   sdk/dataset.go: Dataset 接口
│                                           │
├─ lance::dataset::Scanner                  sdk/scanner.go: ScannerBuilder
├─ lance::dataset::Transaction              pkg/storage2/transaction.go
├─ lance::dataset::FileFragment             pkg/storage2/fragment.go
│
lance-table (表格式层)                      pkg/storage2/table_format.go
├─ format::Manifest                         pkg/storage2/manifest.go
├─ format::Fragment                         pkg/storage2/proto/table.pb.go
├─ io::commit::CommitHandler                pkg/storage2/commit.go
├─ rowids::RowIdSequence                    pkg/storage2/rowids.go
│
lance-io (I/O 层)                           pkg/storage2/io.go, io_ext.go
├─ object_store::ObjectStore                ObjectStore / ObjectStoreExt
├─ scheduler (I/O 调度)                     pkg/storage2/lance_table_io.go
│
lance-index (索引层)                        pkg/storage2/index.go
├─ scalar::BTreeIndex                       pkg/storage2/btree_index.go
├─ scalar::RTreeIndex                       pkg/storage2/rtree_index.go
├─ vector::IVFIndex                         pkg/storage2/ivf_index.go
├─ vector::HNSWIndex                        pkg/storage2/hnsw_index.go
│
lance-encoding (编解码层)                   pkg/storage2/lance_encoder.go
├─ v2 file format                           pkg/storage2/lance_v2_*.go
│
lance::optimize (优化层)                    pkg/storage2/compaction_*.go
├─ distributed compaction                   compaction_planner/worker/coordinator
```

## 2. 核心功能模块对比

### 2.1 Dataset (数据集)

| 功能 | Lance Rust | Go storage2 | 状态 |
|------|------------|-------------|------|
| **打开数据集** | `Dataset::open(uri)` | `Dataset.Open(basePath)` | 已实现 |
| **写入数据** | `Dataset::write()` | `Dataset.Append()` | 已实现 |
| **扫描数据** | `Dataset::scan()` → Scanner | `Dataset.Scanner()` | 已实现 |
| **统计行数** | `Dataset::count_rows()` | `Dataset.CountRows()` | 已实现 |
| **带过滤统计** | `Dataset::count_rows(filter)` | `Dataset.CountRowsWithFilter()` | 已实现 |
| **Take 操作** | `Dataset::take()` | `TakeRows()` / `TakeRowsProjected()` | 已实现 |
| **删除数据** | `Dataset::delete(predicate)` | `Dataset.Delete()` | 已实现 |
| **更新数据** | `Dataset::update()` | `Dataset.Update()` | 已实现 |
| **合并数据** | `Dataset::merge()` | `Dataset.Merge()` | 已实现 |
| **删除列** | `Dataset::drop_columns()` | `Dataset.DropColumns()` | 已实现 |
| **添加列** | `Dataset::add_columns()` | `Dataset.AddColumns()` | 已实现 |
| **修改列** | `Dataset::alter_columns()` | `Dataset.AlterColumns()` | 已实现 |
| **切换版本** | `Dataset::checkout_version()` | `CheckoutVersion()` | 已实现 |
| **标签管理** | `Dataset::create_tag/delete_tag` | `Dataset.CreateTag()` | 已实现 |
| **恢复版本** | `Dataset::restore()` | `Restore()` | 已实现 |
| **浅克隆** | `Dataset::shallow_clone()` | `Dataset.ShallowClone()` | 已实现 |
| **压缩** | `Dataset::compact()` | `Dataset.Compact()` | 已实现 |
| **分布式压缩** | `Dataset::compact_distributed()` | `Dataset.DistributedCompact()` | 已实现 |
| **数据大小** | `Dataset::data_size()` | `Dataset.DataSize()` | 已实现 |

### 2.2 Scanner (扫描器)

| 功能 | Lance Rust | Go storage2 | 状态 |
|------|------------|-------------|------|
| **创建扫描器** | `Scanner::new(dataset)` | `Dataset.Scanner()` | 已实现 |
| **列投影** | `Scanner::project(columns)` | `ScannerBuilder.WithColumns()` | 已实现 |
| **过滤条件** | `Scanner::filter(expr)` | `ScannerBuilder.WithFilter()` | 已实现 |
| **Limit/Offset** | `Scanner::limit(n, offset)` | `ScannerBuilder.WithLimit/WithOffset()` | 已实现 |
| **KNN 搜索** | `Scanner::nearest()` | `KnnSearch()` / `KnnSearchWithIndex` | 已实现 |
| **包含 RowID** | `Scanner::with_row_id()` | `ScannerBuilder.WithRowId()` | 已实现 |
| **有序扫描** | `Scanner::scan_in_order()` | `ScannerBuilder.ScanInOrder()` | 已实现 |
| **批大小** | `Scanner::batch_size()` | `ScannerBuilder.WithBatchSize()` | 已实现 |
| **使用索引** | `Scanner::use_index()` | `ScannerBuilder.UseIndex()` | 已实现 |

### 2.3 Transaction (事务)

| 操作类型 | Lance Rust | Go storage2 | 状态 |
|----------|------------|-------------|------|
| **Append** | `Operation::Append` | `NewTransactionAppend()` | 已实现 |
| **Delete** | `Operation::Delete` | `NewTransactionDelete()` | 已实现 |
| **Overwrite** | `Operation::Overwrite` | `NewTransactionOverwrite()` | 已实现 |
| **Project** | `Operation::Project` | `NewTransactionProject()` | 已实现 |
| **Merge** | `Operation::Merge` | `NewTransactionMerge()` | 已实现 |
| **Clone** | `Operation::Clone` | `NewTransactionClone()` | 已实现 |
| **Rewrite** | `Operation::Rewrite` | `NewTransactionRewrite()` | 已实现 |
| **Restore** | `Operation::Restore` | `Restore()` 函数 | 已实现 |
| **Update** | `Operation::Update` | `update.go` | 已实现 |
| **CreateIndex** | `Operation::CreateIndex` | `index_transaction.go` | 已实现 |
| **DataReplacement** | `Operation::DataReplacement` | `data_replacement.go` | 已实现 |
| **UpdateConfig** | `Operation::UpdateConfig` | `build_manifest.go` | 已实现 |
| **UpdateMemWalState** | `Operation::UpdateMemWalState` | `NewTransactionUpdateMemWalState()` | 已实现 |
| **冲突检测** | Transaction conflict matrix | `CheckConflict()` | 已实现 |
| **提交事务** | `commit_transaction()` | `CommitTransaction()` | 已实现 |

### 2.4 Manifest (版本快照)

| 功能 | Lance Rust | Go storage2 | 状态 |
|------|------------|-------------|------|
| **结构定义** | `Manifest` struct | `Manifest = storage2pb.Manifest` | 已实现 |
| **序列化** | `Manifest::try_from(pb)` | `MarshalManifest()` | 已实现 |
| **反序列化** | `Manifest::into()` | `UnmarshalManifest()` | 已实现 |
| **版本号** | `manifest.version: u64` | `manifest.Version` | 已实现 |
| **Schema** | `manifest.schema: Schema` | `manifest.Fields` | 已实现 |
| **Fragments** | `manifest.fragments: Vec<Fragment>` | `manifest.Fragments` | 已实现 |
| **索引元数据** | `manifest.indices` | 已支持 | 已实现 |
| **特性标志** | `reader/writer_feature_flags: u64` | `ReaderFeatureFlags` / `WriterFeatureFlags` | 已实现 |
| **最大 Fragment ID** | `max_fragment_id: Option<u32>` | `MaxFragmentId` | 已实现 |
| **下一 RowID** | `next_row_id: u64` | `NextRowId` | 已实现 |
| **配置** | `config: HashMap` | `Config` | 已实现 |

### 2.5 CommitHandler (提交处理器)

| 功能 | Lance Rust | Go storage2 | 状态 |
|------|------------|-------------|------|
| **Trait/接口** | `CommitHandler` trait | `CommitHandler` interface | 已实现 |
| **获取最新版本** | `get_latest_version()` | `ResolveLatestVersion()` | 已实现 |
| **解析版本路径** | `resolve_version_location()` | `ResolveVersion()` | 已实现 |
| **提交 Manifest** | `commit()` | `Commit()` | 已实现 |
| **本地重命名** | `RenameCommitHandler` | `LocalRenameCommitHandler` | 已实现 |
| **S3 提交** | `ExternalManifestCommitHandler` | `S3CommitHandlerV2` | 已实现 |
| **GCS 提交** | `ExternalManifestCommitHandler` | `GSCommitHandler` | 已实现 |
| **Azure 提交** | `ExternalManifestCommitHandler` | `AZCommitHandler` | 已实现 |
| **乐观锁** | DynamoDB 外部锁 | S3 ETag 检查 | 已实现 |
| **重试机制** | 内置重试 | `CommitTransactionWithRetry()` | 已实现 |

### 2.6 ObjectStore (存储抽象)

| 功能 | Lance Rust | Go storage2 | 状态 |
|------|------------|-------------|------|
| **基础接口** | `ObjectStore` struct | `ObjectStore` interface | 已实现 |
| **扩展接口** | `ObjectStoreExt` trait | `ObjectStoreExt` interface | 已实现 |
| **读取文件** | `open(path)` → Reader | `Read(path)` | 已实现 |
| **写入文件** | `create(path)` → Writer | `Write(path, data)` | 已实现 |
| **范围读取** | `get_range()` | `ReadRange(ctx, path, opts)` | 已实现 |
| **流式读取** | `ObjectReader` | `ReadStream()` | 已实现 |
| **流式写入** | `ObjectWriter` | `WriteStream()` | 已实现 |
| **复制文件** | `copy(from, to)` | `Copy(ctx, src, dst)` | 已实现 |
| **重命名** | - | `Rename(ctx, src, dst)` | 已实现 |
| **删除文件** | `delete(path)` | `Delete(ctx, path)` | 已实现 |
| **检查存在** | `exists(path)` | `Exists(ctx, path)` | 已实现 |
| **获取大小** | `size(path)` | `GetSize(ctx, path)` | 已实现 |
| **获取 ETag** | - | `GetETag(ctx, path)` | 已实现 |
| **列出目录** | `read_dir()` / `list()` | `List(dir)` | 已实现 |
| **本地存储** | `ObjectStore::local()` | `LocalObjectStore` | 已实现 |
| **内存存储** | `ObjectStore::memory()` | `MemoryObjectStore` | 已实现 |
| **S3 存储** | `s3` provider | `S3ObjectStore` | 已实现 |
| **GCS 存储** | `gcs` provider | `GSObjectStore` | 已实现 |
| **Azure 存储** | `azure` provider | `AZObjectStore` | 已实现 |
| **URI 解析** | `ObjectStore::from_uri()` | `ParseURI()` / `StoreFactory` | 已实现 |

### 2.7 RowIdSequence (行 ID 序列)

| 功能 | Lance Rust | Go storage2 | 状态 |
|------|------------|-------------|------|
| **结构定义** | `RowIdSequence` | `RowIdSequence` | 已实现 |
| **Range 段** | `Range` | `RangeSegment` | 已实现 |
| **RangeWithHoles** | `RangeWithHoles` | `RangeWithHolesSegment` | 已实现 |
| **RangeWithBitmap** | `RangeWithBitmap` | `RangeWithBitmapSegment` | 已实现 |
| **SortedArray** | `SortedArray` | `SortedArraySegment` | 已实现 |
| **Array** | `Array` | `ArraySegment` | 已实现 |
| **Protobuf 解析** | `RowIdSequence::decode()` | `ParseRowIdSequence()` | 已实现 |
| **长度获取** | `sequence.len()` | `sequence.Len()` | 已实现 |
| **索引访问** | `sequence.get(i)` | `sequence.Get(i)` | 已实现 |
| **包含检查** | `sequence.contains(val)` | `sequence.Contains(val)` | 已实现 |
| **位置查找** | `sequence.position(val)` | `sequence.Position(val)` | 已实现 |
| **合并序列** | `sequence.extend(other)` | `sequence.Extend(other)` | 已实现 |
| **删除行ID** | `sequence.delete(ids)` | `sequence.Delete(ids)` | 已实现 |
| **切片** | `sequence.slice(off, len)` | `sequence.Slice(off, len)` | 已实现 |
| **自动编码选择** | `U64Segment::from_iter()` | `NewU64SegmentFromSlice()` | 已实现 |

### 2.8 索引 (Index)

| 功能 | Lance Rust | Go storage2 | 状态 |
|------|------------|-------------|------|
| **索引 Trait** | `Index` trait | `Index` interface | 已实现 |
| **标量索引** | `ScalarIndex` trait | `ScalarIndexImpl` interface | 已实现 |
| **向量索引** | `VectorIndex` trait | `VectorIndexImpl` interface | 已实现 |
| **全文索引** | `InvertedIndex` trait | `InvertedIndexImpl` interface | 已实现 |
| **B-Tree 索引** | `btree.rs` | `BTreeIndex` | 已实现 |
| **IVF 索引** | `ivf/` | `IVFIndex` | 已实现 |
| **HNSW 索引** | `hnsw/` | `HNSWIndex` | 已实现 |
| **Bitmap 索引** | `bitmap.rs` | `BitmapIndex` | 已实现 |
| **ZoneMap 索引** | `zonemap.rs` | `ZoneMapIndex` | 已实现 |
| **BloomFilter** | `bloomfilter.rs` | `BloomFilterIndex` | 已实现 |
| **R-Tree 索引** | `rtree.rs` | `RTreeIndex` | 已实现 |
| **索引管理器** | `DatasetIndexExt` | `IndexManager` | 已实现 |
| **创建索引** | `create_index()` | `CreateScalarIndex()` / `CreateVectorIndex()` | 已实现 |
| **删除索引** | `drop_index()` | `DropIndex()` | 已实现 |
| **索引统计** | `index_statistics()` | `GetIndexStatistics()` | 已实现 |
| **索引优化** | `optimize_indices()` | `OptimizeIndex()` | 已实现 |
| **索引描述** | `describe_indices()` | `DescribeIndex()` | 已实现 |
| **索引选择器** | `IndexSelector` | `IndexSelector` | 已实现 |
| **索引持久化** | `IndexStore` | `IndexStore` / `LocalIndexStore` | 已实现 |

### 2.9 I/O 调度与优化

| 功能 | Lance Rust | Go storage2 | 状态 |
|------|------------|-------------|------|
| **I/O 调度器** | `scheduler.rs` | `ConcurrentIOScheduler` | 已实现 |
| **信号量控制** | 内部实现 | `Semaphore` | 已实现 |
| **并发读取限制** | `io_parallelism` | `MaxConcurrentReads` | 已实现 |
| **并发写入限制** | - | `MaxConcurrentWrites` | 已实现 |
| **并行多文件读取** | `read_all()` | `ParallelMultiFileReader` | 已实现 |
| **分块并行读取** | - | `ChunkedParallelReader` | 已实现 |
| **重试包装器** | 内置重试 | `RetryableObjectStore` | 已实现 |
| **指数退避** | 内置 | `RetryConfig.BackoffFactor` | 已实现 |
| **I/O 统计** | `IOTracker` | `IOStatsCollector` | 已实现 |

### 2.10 V2 Manifest 命名

| 功能 | Lance Rust | Go storage2 | 状态 |
|------|------------|-------------|------|
| **V2 命名方案** | `ManifestNamingScheme::V2` | `ManifestNamingV2` | 已实现 |
| **版本号反转** | `MAX - version` | `ManifestNamingV2Max - version` | 已实现 |
| **文件名生成** | `manifest_path_v2()` | `ManifestPathV2()` | 已实现 |
| **版本解析** | `parse_version_v2()` | `ParseVersionV2()` | 已实现 |
| **方案检测** | - | `DetectNamingScheme()` | 已实现 |
| **V2 提交处理器** | - | `V2CommitHandler` | 已实现 |

### 2.11 表验证

| 功能 | Lance Rust | Go storage2 | 状态 |
|------|------------|-------------|------|
| **Manifest 验证** | `validate_manifest()` | `ValidateManifest()` | 已实现 |
| **文件存在验证** | - | `ValidateTableFiles()` | 已实现 |
| **Fragment ID 唯一性** | 内置检查 | 已验证 | 已实现 |
| **max_fragment_id 一致性** | 内置检查 | 已验证 | 已实现 |
| **删除文件一致性** | 内置检查 | 已验证 | 已实现 |

### 2.12 版本控制 (Refs)

| 功能 | Lance Rust | Go storage2 | 状态 |
|------|------------|-------------|------|
| **标签管理** | `TagManager` | `Tags` | 已实现 |
| **分支管理** | `BranchManager` | `Branches` | 已实现 |
| **创建标签** | `create_tag()` | `Tags.Create()` | 已实现 |
| **删除标签** | `delete_tag()` | `Tags.Delete()` | 已实现 |
| **获取标签** | `get_tag()` | `Tags.Get()` | 已实现 |
| **列出标签** | `list_tags()` | `Tags.List()` | 已实现 |
| **创建分支** | `create_branch()` | `Branches.Create()` | 已实现 |
| **删除分支** | `delete_branch()` | `Branches.Delete()` | 已实现 |
| **列出分支** | `list_branches()` | `Branches.List()` | 已实现 |
| **标签验证** | - | `CheckValidTag()` | 已实现 |
| **分支验证** | - | `CheckValidBranch()` | 已实现 |

### 2.13 编码层 (Encoding)

| 功能 | Lance Rust | Go storage2 | 状态 |
|------|------------|-------------|------|
| **Plain 编码** | `PlainEncoder` | `PlainEncoder` | 已实现 |
| **BitPack 编码** | `BitPackEncoder` | `BitPackEncoder` | 已实现 |
| **RLE 编码** | `RLEEncoder` | `RLEEncoder` | 已实现 |
| **Dict 编码** | `DictEncoder` | `DictEncoder` | 已实现 |
| **VarBinary 编码** | `VarBinaryEncoder` | `VarBinaryEncoder` | 已实现 |
| **StringDict 编码** | `StringDictEncoder` | `StringDictEncoder` | 已实现 |
| **逻辑编码器** | `LogicalEncoder` | `LogicalColumnEncoder` | 已实现 |
| **逻辑解码器** | `LogicalDecoder` | `LogicalColumnDecoder` | 已实现 |
| **编码选择** | 自动选择 | `SelectIntEncoding()` / `SelectStringEncoding()` | 已实现 |
| **编码调度器** | - | `EncodingScheduler` | 已实现 |
| **并行编码** | - | `EncodeChunk()` / `EncodeBatch()` | 已实现 |

### 2.14 V2 文件格式

| 功能 | Lance Rust | Go storage2 | 状态 |
|------|------------|-------------|------|
| **文件魔数** | `LNC2` | `V2Magic = "LNC2"` | 已实现 |
| **文件头** | `V2Header` | `V2FileHeader` (32 bytes) | 已实现 |
| **页头** | `PageHeader` | `V2PageHeader` (24 bytes) | 已实现 |
| **页索引** | `PageIndex` | `V2PageIndex` / `V2PageIndexEntry` | 已实现 |
| **压缩类型** | LZ4/Zstd/Snappy | `V2CompressionType` | 已实现 |
| **列写入器** | `ColumnWriter` | `V2ColumnWriter` | 已实现 |
| **列读取器** | `ColumnReader` | `V2ColumnReader` | 已实现 |
| **文件写入器** | `FileWriter` | `V2FileWriter` | 已实现 |
| **文件读取器** | `FileReader` | `V2FileReader` | 已实现 |
| **嵌套类型** | Struct/List/Map | `V2StructWriter` (定义层级) | 已实现 |
| **校验和** | CRC32 | `ComputeChecksum()` / `VerifyChecksum()` | 已实现 |
| **LZ4 压缩** | 完整实现 | 桩实现 | 部分实现 |
| **Zstd 压缩** | 完整实现 | 桩实现 | 部分实现 |

### 2.15 分布式压缩 (Distributed Compaction)

| 功能 | Lance Rust | Go storage2 | 状态 |
|------|------------|-------------|------|
| **压缩规划器** | `CompactionPlanner` | `CompactionPlanner` | 已实现 |
| **压缩策略-大小** | Size-based | `StrategySize` | 已实现 |
| **压缩策略-计数** | Count-based | `StrategyCount` | 已实现 |
| **压缩策略-混合** | Hybrid | `StrategyHybrid` | 已实现 |
| **压缩策略-装箱** | Bin-packing | `StrategyBinPacking` | 已实现 |
| **压缩任务** | `CompactionTask` | `CompactionTask` | 已实现 |
| **压缩计划** | `CompactionPlan` | `CompactionPlan` | 已实现 |
| **Worker 接口** | `CompactionWorker` | `CompactionWorker` interface | 已实现 |
| **本地 Worker** | Local execution | `LocalCompactionWorker` | 已实现 |
| **Worker 池** | Worker pool | `WorkerPool` | 已实现 |
| **协调器** | `CompactionCoordinator` | `CompactionCoordinator` | 已实现 |
| **并行执行** | Parallel execution | `executeParallel()` | 已实现 |
| **冲突重试** | Conflict retry | `RetryOnConflict` | 已实现 |
| **进度跟踪** | Progress tracking | `ProgressTracker` | 已实现 |
| **指标收集** | Metrics collection | `MetricsCollector` | 已实现 |
| **压缩统计** | Compaction stats | `CompactionStats` | 已实现 |
| **行 ID 保留** | Row ID preservation | `PreserveRowIds` option | 已实现 |

## 3. 功能实现状态汇总

### 3.1 已实现功能

#### 核心功能
- [x] Dataset 打开、写入、扫描、更新、删除
- [x] Scanner 投影、过滤、Limit、KNN、RowID、有序扫描、索引使用
- [x] Transaction (全部 13 种操作类型)
- [x] CommitHandler (本地、S3、GCS、Azure)
- [x] Manifest 序列化/反序列化
- [x] ObjectStore (本地、内存、S3、GCS、Azure)
- [x] RowIdSequence (5 种段类型)
- [x] 冲突检测 (完整冲突矩阵)

#### I/O 层
- [x] IO 调度器与信号量控制
- [x] 并行多文件读取
- [x] 分块并行读取
- [x] 重试包装器与指数退避
- [x] V2 Manifest 命名
- [x] 表验证

#### 索引
- [x] B-Tree 索引
- [x] IVF 向量索引
- [x] HNSW 向量索引
- [x] Bitmap 索引
- [x] ZoneMap 索引
- [x] BloomFilter 索引
- [x] R-Tree 空间索引
- [x] 索引管理器
- [x] 索引统计与描述
- [x] 索引选择器

#### 版本控制
- [x] 标签管理 (Tags)
- [x] 分支管理 (Branches)
- [x] 版本恢复 (Restore)

#### 编码层
- [x] 物理编码器 (Plain, BitPack, RLE, Dict, VarBinary, StringDict)
- [x] 逻辑编码/解码器
- [x] 编码调度器与并行编码
- [x] V2 文件格式 (Header, Pages, Footer, Index)
- [x] V2 列写入/读取器
- [x] 嵌套类型支持 (Struct with definition levels)
- [x] 校验和 (CRC32)

#### 分布式压缩
- [x] 压缩规划器 (4 种策略)
- [x] 压缩任务与计划
- [x] Worker 接口与本地实现
- [x] Worker 池
- [x] 压缩协调器
- [x] 并行执行
- [x] 冲突重试
- [x] 进度跟踪与指标收集

### 3.2 部分实现功能

| 功能 | 状态 | 说明 |
|------|------|------|
| **LZ4 压缩** | 桩实现 | 接口已定义，需引入 lz4 库 |
| **Zstd 压缩** | 桩实现 | 接口已定义，需引入 zstd 库 |
| **远程 Worker** | 预留 | 本地 Worker 完成，RPC 接口预留 |

### 3.3 与 Lance Rust 的主要差异

| 差异点 | Lance Rust | Go storage2 | 说明 |
|--------|------------|-------------|------|
| **语言** | Rust (async/await) | Go (sync + goroutine) | 不同的并发模型 |
| **数据格式** | Arrow | chunk.Chunk/Vector | 自定义格式 |
| **压缩库** | 完整集成 lz4/zstd | 桩实现 | 可后续添加 |
| **S3 锁机制** | DynamoDB | ETag 乐观锁 | 语义等价 |

## 4. 代码行数对比

| 项目 | Lance Rust | Go storage2 + SDK | 比例 |
|------|------------|-------------------|------|
| **总计** | **~300,000 行** | **~56,000 行** | **~5:1** |

### 各模块详细对比

| 模块 | Lance Rust | Go storage2 | 说明 |
|------|------------|-------------|------|
| Dataset/Scanner | ~50,000 行 | ~7,000 行 | SDK 层完整 |
| Transaction | ~15,000 行 | ~3,000 行 | 全部 13 种操作 |
| Manifest/Table | ~20,000 行 | ~3,500 行 | 包含 protobuf 生成代码 |
| IO/Commit | ~30,000 行 | ~10,000 行 | 调度器+重试+云存储 |
| Index | ~80,000 行 | ~8,000 行 | 7 种索引类型 |
| Encoding | ~60,000 行 | ~8,000 行 | 含 v2 文件格式 |
| Compaction | ~10,000 行 | ~2,000 行 | 分布式压缩完整 |
| 测试代码 | ~100,000+ 行 | ~20,000 行 | 全面测试覆盖 |
| 其他 | ~35,000 行 | ~4,500 行 | 工具、示例等 |

## 5. 关键差异说明

### 5.1 架构差异

1. **Crate 结构 vs 单模块**
   - Rust: 多 crate 工作区 (lance, lance-table, lance-io, lance-index, lance-encoding)
   - Go: 单模块 pkg/storage2 + sdk 层

2. **异步模型**
   - Rust: 全异步 (async/await)
   - Go: 同步为主，部分使用 goroutine

3. **数据格式**
   - Rust: Arrow 格式
   - Go: 自定义 Chunk/Vector 格式

### 5.2 功能差异

1. **压缩支持**
   - Rust: 完整的 LZ4/Zstd/Snappy 实现
   - Go: 接口定义完成，实际压缩桩实现

2. **CommitHandler**
   - Rust: ExternalManifestCommitHandler 使用 DynamoDB
   - Go: S3CommitHandler 使用乐观锁 + ETag

## 6. 对应文件速查表

| Lance Rust 文件 | Go storage2 文件 | 功能 |
|-----------------|------------------|------|
| `lance/src/dataset.rs` | `sdk/dataset.go` | Dataset API |
| `lance/src/dataset/scanner.rs` | `sdk/scanner.go` | Scanner |
| `lance/src/dataset/transaction.rs` | `pkg/storage2/transaction.go` | Transaction |
| `lance-table/src/format/manifest.rs` | `pkg/storage2/manifest.go` | Manifest |
| `lance-table/src/io/commit.rs` | `pkg/storage2/commit.go` | CommitHandler |
| `lance-table/src/rowids.rs` | `pkg/storage2/rowids.go` | RowIdSequence |
| `lance-io/src/object_store.rs` | `pkg/storage2/io.go`, `io_ext.go` | ObjectStore |
| `lance-io/src/scheduler.rs` | `pkg/storage2/lance_table_io.go` | IO Scheduler |
| `lance-index/src/traits.rs` | `pkg/storage2/index.go` | Index traits |
| `lance-index/src/scalar/btree.rs` | `pkg/storage2/btree_index.go` | B-Tree index |
| `lance-index/src/scalar/rtree.rs` | `pkg/storage2/rtree_index.go` | R-Tree index |
| `lance-index/src/vector/ivf/` | `pkg/storage2/ivf_index.go` | IVF index |
| `lance-index/src/vector/hnsw/` | `pkg/storage2/hnsw_index.go` | HNSW index |
| `lance/src/dataset/optimize.rs` | `pkg/storage2/compaction_*.go` | Compaction |
| `lance-encoding/src/` | `pkg/storage2/lance_v2_*.go` | V2 Encoding |

## 7. 实现一致性检查报告（Lance Rust 对照）

### 7.1 完全一致的实现

| 模块 | 一致性说明 |
|------|-------------|
| **RowIdSequence** | 段类型与 Rust 一致：Range、RangeWithHoles、RangeWithBitmap、SortedArray、Array；API：Len/Get/Contains/Position/Iter/Extend/Delete/Slice；自动编码选择 `NewU64SegmentFromSlice` 对应 Rust `U64Segment::from_iter`；解析使用 Lance 兼容的 protobuf 结构。 |
| **Manifest 命名 V2** | `ManifestPathV2` 使用 `%020d` 零填充 + `ManifestNamingV2Max - version` 与 Rust `ManifestNamingScheme::V2` 完全一致；`ParseVersionV2`、`DetectNamingScheme`、`V2CommitHandler` 已实现。 |
| **CommitHandler** | 接口语义一致：ResolveLatestVersion、ResolveVersion、Commit；本地用 temp file + rename；S3 用乐观锁（ETag），Rust 可选 DynamoDB，Go 未实现外部锁。 |
| **冲突检测** | 完整实现 Lance 冲突矩阵（7x7），包括所有非对称组合；fragment 重叠检测与 Rust 一致。 |
| **ObjectStore** | 读写、范围读、流、Copy/Delete/Exists/List/GetSize 等与 Rust ObjectStoreExt 对应；本地/内存/S3/GCS/Azure 均有实现。 |
| **V2 文件格式** | 文件魔数 `LNC2`、Header 32 bytes、Page Header 24 bytes、压缩类型枚举与 Lance 规范一致。 |

### 7.2 冲突矩阵与 Lance 完全一致

1. **Overwrite 冲突语义**：
   - 当**当前事务**是 Overwrite 时与已提交的 Append/Delete/Merge/Rewrite 等**不冲突**（不依赖先前状态）。
   - 当**已提交事务**是 Overwrite（或 Restore）时后续操作**冲突**。
   - 新增 `OpRestore`、`OpRewrite`、`OpMerge`、`OpProject` 操作类型识别。

2. **非对称冲突矩阵**——以下 7 处已修正为与 Lance `conflict_resolver.rs` 完全一致：
   - Project(当前) vs Append(已提交) = 兼容
   - Project(当前) vs Delete(已提交) = 兼容
   - Project(当前) vs Rewrite(已提交) = 兼容
   - CreateIndex(当前) vs Project(已提交) = 兼容
   - Rewrite(当前) vs CreateIndex(已提交) = 冲突
   - Merge(当前) vs CreateIndex(已提交) = 冲突
   - Merge(当前) vs Rewrite(已提交) = 兼容

3. **Fragment 重叠检测**：Delete↔Rewrite、Rewrite↔Rewrite 按 fragment ID 交集判断。

4. **CheckConflict 重构**：采用 per-operation-type 分发（`checkAppendConflict`、`checkDeleteConflict` 等），每个函数只处理一行矩阵，消除对称假设。

- **测试**：`TestConflictMatrix_*`（7 个矩阵行测试）、`TestConflictMatrix_AsymmetricPairs`（关键非对称用例）、Fragment 重叠测试均通过。

---

## 8. 总结

### 8.1 功能覆盖评估

Go `pkg/storage2` 实现了 Lance Rust **核心功能**的约 **85-90%**：

| 类别 | 覆盖度 | 说明 |
|------|--------|------|
| 存储抽象层 | 95% | 完整的 ObjectStore + 4 种后端 |
| 事务系统 | 100% | 13 种操作全部实现，冲突矩阵与 Lance 完全一致 |
| 版本控制 | 95% | Manifest + Tags/Branches + Restore |
| I/O 优化 | 85% | Scheduler + Retry + V2 命名 |
| 索引 | 90% | 7 种索引类型全覆盖 |
| 编码层 | 75% | 完整框架，压缩库待集成 |
| 分布式压缩 | 90% | 规划器+Worker+协调器完整 |
| 查询执行 | 85% | Scanner 高级功能完整 |

### 8.2 与上次对比的改进

| 新增功能 | 文件 | 代码量 |
|----------|------|--------|
| 分布式压缩规划器 | `compaction_planner.go` | ~520 行 |
| 压缩 Worker | `compaction_worker.go` | ~420 行 |
| 压缩协调器 | `compaction_coordinator.go` | ~490 行 |
| V2 文件格式 | `lance_v2_format.go` | ~350 行 |
| V2 列写入/读取 | `lance_v2_column.go` | ~560 行 |
| V2 文件写入/读取 | `lance_v2_file.go` | ~600 行 |
| R-Tree 索引 | `rtree_index.go` | ~570 行 |
| Scanner 高级功能 | `scanner.go` | WithRowId, UseIndex, ScanInOrder |
| Dataset 更多操作 | `dataset.go` | Update, Merge, Restore, Tags |
| UpdateMemWalState | `transaction.go` | 事务操作 |

### 8.3 剩余差距

| 功能 | 状态 | 优先级 |
|------|------|--------|
| LZ4/Zstd 压缩库集成 | 桩实现 | 中 |
| 远程 RPC Worker | 预留 | 低 |
| DynamoDB 外部锁 | 未实现 | 低 |

### 8.4 适用场景

Go `pkg/storage2` 适用于：
- TPC-H 查询执行
- 基础数据分析
- 需要与 Go 生态集成的场景
- 中等规模向量搜索
- 云原生数据湖存储

Lance Rust 适用于：
- 生产级向量数据库
- 需要完整压缩支持的场景
- 高性能数据分析
- 复杂查询优化需求
- 大规模分布式部署
