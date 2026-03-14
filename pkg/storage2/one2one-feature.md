# Lance Rust vs Go storage2 功能一对一对比

## 1. 概述

本文档详细对比 Lance Rust 代码库 (`/Users/pengzhen/Documents/GitHub/lance/rust`) 与 Go `pkg/storage2` 模块的功能对应关系。

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
├─ vector::IVFIndex                         pkg/storage2/ivf_index.go
├─ vector::HNSWIndex                        pkg/storage2/hnsw_index.go
│
lance-encoding (编解码层)                   pkg/storage2/lance_encoder.go
```

## 2. 核心功能模块对比

### 2.1 Dataset (数据集)

| 功能 | Lance Rust | Go storage2 | 状态 |
|------|------------|-------------|------|
| **打开数据集** | `Dataset::open(uri)` | `Dataset.Open(basePath)` | 已实现 |
| **写入数据** | `Dataset::write()` | `Dataset.Append()` | 已实现 |
| **扫描数据** | `Dataset::scan()` → Scanner | `Dataset.Scanner()` | 已实现 |
| **统计行数** | `Dataset::count_rows()` | `Dataset.CountRows()` | 已实现 |
| **Take 操作** | `Dataset::take()` | `TakeRows()` / `TakeRowsProjected()` | 已实现 |
| **删除数据** | `Dataset::delete(predicate)` | `Dataset.Delete()` | 已实现 |
| **更新数据** | `Dataset::update()` | 未实现 | 未实现 |
| **合并数据** | `Dataset::merge()` | `Dataset.Merge()` | 已实现 |
| **删除列** | `Dataset::drop_columns()` | `Dataset.DropColumns()` | 已实现 |
| **添加列** | `Dataset::add_columns()` | 未实现 | 未实现 |
| **切换版本** | `Dataset::checkout_version()` | `CheckoutVersion()` | 已实现 |
| **标签管理** | `Dataset::create_tag/delete_tag` | `Dataset.CreateTag()` | 已实现 |
| **恢复版本** | `Dataset::restore()` | `Restore()` | 已实现 |

### 2.2 Scanner (扫描器)

| 功能 | Lance Rust | Go storage2 | 状态 |
|------|------------|-------------|------|
| **创建扫描器** | `Scanner::new(dataset)` | `Dataset.Scanner()` | 已实现 |
| **列投影** | `Scanner::project(columns)` | `ScannerBuilder.Project()` | 已实现 |
| **过滤条件** | `Scanner::filter(expr)` | `ScannerBuilder.Filter()` | 已实现 |
| **Limit/Offset** | `Scanner::limit(n, offset)` | `ScannerBuilder.Limit()` | 已实现 |
| **KNN 搜索** | `Scanner::nearest()` | `KnnSearch()` / `KnnSearchWithIndex` | 已实现 |
| **包含 RowID** | `Scanner::with_row_id()` | 未实现 | 未实现 |
| **有序扫描** | `Scanner::scan_in_order()` | 未实现 | 未实现 |
| **批大小** | `Scanner::batch_size()` | 未实现 | 未实现 |
| **使用索引** | `Scanner::use_index()` | 未实现 | 未实现 |

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
| **Update** | `Operation::Update` | 未实现 | 未实现 |
| **CreateIndex** | `Operation::CreateIndex` | 未实现 | 未实现 |
| **DataReplacement** | `Operation::DataReplacement` | 未实现 | 未实现 |
| **UpdateConfig** | `Operation::UpdateConfig` | 未实现 | 未实现 |
| **冲突检测** | Transaction conflict matrix | `DetectConflict()` | 已实现 |
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
| **索引元数据** | `manifest.indices` | 基础支持 | 部分实现 |
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
| **索引管理器** | `DatasetIndexExt` | `IndexManager` | 已实现 |
| **创建索引** | `create_index()` | `CreateScalarIndex()` / `CreateVectorIndex()` | 框架实现 |
| **删除索引** | `drop_index()` | `DropIndex()` | 已实现 |
| **索引统计** | `index_statistics()` | `GetIndexStatistics()` | 已实现 |
| **索引优化** | `optimize_indices()` | `OptimizeIndex()` | 已实现 |
| **索引描述** | `describe_indices()` | `DescribeIndex()` | 已实现 |
| **Bitmap 索引** | `bitmap.rs` | 未实现 | 未实现 |
| **ZoneMap 索引** | `zonemap.rs` | 未实现 | 未实现 |
| **BloomFilter** | `bloomfilter.rs` | 未实现 | 未实现 |
| **R-Tree 索引** | `rtree.rs` | 未实现 | 未实现 |

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

## 3. 功能实现状态汇总

### 3.1 已实现功能

#### 核心功能
- [x] Dataset 打开、写入、扫描
- [x] Scanner 投影、过滤、Limit、KNN
- [x] Transaction (Append, Delete, Overwrite, Project, Merge, Clone, Rewrite, Restore)
- [x] CommitHandler (本地、S3、GCS、Azure)
- [x] Manifest 序列化/反序列化
- [x] ObjectStore (本地、内存、S3、GCS、Azure)
- [x] RowIdSequence (5 种段类型)
- [x] 冲突检测

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
- [x] 索引管理器
- [x] 索引统计与描述

#### 版本控制
- [x] 标签管理 (Tags)
- [x] 分支管理 (Branches)
- [x] 版本恢复 (Restore)

### 3.2 未实现功能

| 功能 | 说明 | 优先级 |
|------|------|--------|
| **Update 操作** | 行级更新 | P2 |
| **CreateIndex 事务** | 索引创建作为事务 | P2 |
| **DataReplacement** | 数据替换操作 | P3 |
| **UpdateConfig** | 配置更新操作 | P3 |
| **Bitmap 索引** | 位图索引 | P3 |
| **ZoneMap 索引** | 区域映射索引 | P3 |
| **BloomFilter 索引** | 布隆过滤器索引 | P3 |
| **R-Tree 索引** | 空间索引 | P3 |
| **分布式压缩** | 分布式 Compaction | P3 |
| **UpdateMemWalState** | 内存 WAL 状态更新 | P4 |

### 3.3 部分实现功能

| 功能 | 状态 | 说明 |
|------|------|------|
| **标量索引创建** | 框架 | 有接口，完整实现待完善 |
| **向量索引创建** | 框架 | 有接口，完整实现待完善 |
| **索引存储** | 框架 | `IndexStore` 接口待完整实现 |

## 4. 代码行数对比

| 项目 | Lance Rust | Go storage2 | 比例 |
|------|------------|-------------|------|
| **总计** | **~300,000 行** | **~28,000 行** | **~10:1** |

### 各模块详细对比

| 模块 | Lance Rust | Go storage2 | 说明 |
|------|------------|-------------|------|
| Dataset/Scanner | ~50,000 行 | ~5,000 行 | Rust 包含更多执行优化 |
| Transaction | ~15,000 行 | ~1,500 行 | Rust 支持更多操作类型 |
| Manifest/Table | ~20,000 行 | ~3,000 行 | 包含 protobuf 生成代码 |
| IO/Commit | ~30,000 行 | ~8,000 行 | Rust 有更复杂的调度器 |
| Index | ~80,000 行 | ~5,000 行 | Rust 有更多索引类型 |
| Encoding | ~60,000 行 | ~3,000 行 | Rust 有完整的 v2 编码 |
| 测试代码 | ~100,000+ 行 | ~15,000 行 | Rust 测试更全面 |
| 其他 | ~45,000 行 | ~2,500 行 | 工具、示例等 |

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

1. **事务类型**
   - Rust 支持更多操作类型 (Update, CreateIndex, DataReplacement, UpdateConfig)
   - Go 实现了核心操作类型

2. **索引支持**
   - Rust: 完整的标量和向量索引生态
   - Go: 基础索引框架，B-Tree、IVF、HNSW 实现

3. **CommitHandler**
   - Rust: ExternalManifestCommitHandler 使用 DynamoDB
   - Go: S3CommitHandler 使用乐观锁 + ETag

4. **编码**
   - Rust: 完整的 v2 文件格式编码
   - Go: 基础编码支持，使用 protobuf 元数据

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
| `lance-index/src/vector/ivf/` | `pkg/storage2/ivf_index.go` | IVF index |
| `lance-index/src/vector/hnsw/` | `pkg/storage2/hnsw_index.go` | HNSW index |
| `lance/src/dataset/optimize.rs` | `sdk/dataset.go: Compact` | Compaction |
| `lance/src/dataset/take.rs` | `pkg/storage2/scanner.go` | Take operation |
| `lance/src/dataset/fragment.rs` | `pkg/storage2/fragment.go` | Fragment |

## 7. 总结

### 7.1 代码量与功能实现的关系

Go `pkg/storage2` (~2.8万行) 与 Lance Rust (~30万行) 的代码量比约为 **1:10**，但这不直接等同于功能比例：

**Go 代码更精简的原因：**
1. **语言特性**：Go 语法更简洁，错误处理更直接
2. **异步模型**：Go 使用同步代码 + goroutine，Rust 需要 async/await 状态机
3. **功能范围**：Go 实现了核心功能子集，Rust 包含更多高级特性
4. **测试覆盖**：Rust 包含大量测试代码 (~10万行)
5. **编码复杂度**：Rust 有完整的 v2 文件格式编码，Go 使用 protobuf 元数据

### 7.2 功能覆盖评估

Go `pkg/storage2` 实现了 Lance Rust **核心功能**的约 **60-70%**：

| 类别 | 覆盖度 | 说明 |
|------|--------|------|
| 存储抽象层 | 90% | 完整的 ObjectStore + 4 种后端 |
| 事务系统 | 70% | 8 种核心操作，缺少 Update/CreateIndex |
| 版本控制 | 85% | Manifest + Tags/Branches + Restore |
| I/O 优化 | 75% | Scheduler + Retry + V2 命名 |
| 索引 | 40% | B-Tree/IVF/HNSW 基础实现 |
| 编码层 | 30% | 基础编码，远不及 Rust 完整 |
| 查询执行 | 50% | Scanner 基础功能 |

### 7.3 主要差距

| 功能 | Rust 复杂度 | Go 状态 |
|------|-------------|---------|
| Update 操作 | 高 | 未实现 |
| CreateIndex 事务 | 高 | 未实现 |
| Bitmap/ZoneMap/BloomFilter 索引 | 中 | 未实现 |
| 完整的 v2 编码 | 很高 | 基础实现 |
| 分布式压缩 | 高 | 未实现 |
| 复杂查询优化 | 很高 | 未实现 |

### 7.4 适用场景

Go `pkg/storage2` 适用于：
- TPC-H 查询执行
- 基础数据分析
- 需要与 Go 生态集成的场景

Lance Rust 适用于：
- 生产级向量数据库
- 需要完整索引支持的场景
- 高性能数据分析
- 复杂查询优化需求
