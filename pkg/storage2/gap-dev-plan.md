# Storage2 功能缺口开发计划

基于 `one2one-feature.md` 功能对比分析，结合代码实际状态编写。

---

## 前置：修正文档 one2one-feature.md

文档中 3 处声称"未实现"但实际已实现，需先修正：

| 条目 | 文档声称 | 实际状态 |
|------|----------|----------|
| Dataset.AddColumns() | 未实现 | `sdk/dataset.go:641` 已实现 + 测试 |
| Scanner.BatchSize() | 未实现 | `sdk/scanner.go:83` WithBatchSize 已实现 |
| UpdateConfig 事务 | 未实现 | `build_manifest.go:148` 已实现 + 4 个测试 |

---

## P0: SDK 层 API 暴露

**目标**: 将已有的底层实现暴露为 Dataset 接口方法。底层代码完整，仅需编写薄封装层。

### P0-1: Dataset.Update()

**底层现状**: `pkg/storage2/update.go` 已有完整实现：
- `UpdatePlanner` — 计划更新操作（策略选择: RewriteRows / RewriteColumns）
- `UpdateExecutor` — 执行更新（逐 chunk 扫描、修改、写回）
- `UpdatePredicate` / `ColumnUpdate` / `UpdateOperation` — 完整的参数类型
- `BuildManifest` 已支持 Update 操作类型

**需实现**:

```go
// sdk/dataset.go — Dataset 接口新增
Update(ctx context.Context, predicate string, updates map[string]interface{}) (*UpdateResult, error)
```

**实现步骤**:

1. 在 `Dataset` 接口中添加 `Update` 方法签名
2. 在 `datasetImpl` 中实现：
   - 获取当前 manifest
   - 创建 `storage2.UpdatePlanner`
   - 将 `map[string]interface{}` 转为 `[]ColumnUpdate`（列名 → 列索引映射）
   - 调用 `PlanUpdate()` + `ExecuteUpdate()`
   - 通过 `CommitTransaction()` 提交 Update 事务
   - 刷新 `currentManifest` 和 `version`
3. 编写测试：
   - `TestDatasetUpdate` — 基本更新
   - `TestDatasetUpdateWithPredicate` — 带条件更新
   - `TestDatasetUpdateNoMatch` — 无匹配行
   - `TestDatasetUpdateMultipleColumns` — 多列更新

**依赖**: 无新依赖

---

### P0-2: Dataset.Merge()

**底层现状**: `storage2.NewTransactionMerge()` 已实现，SDK 内部在 `AlterColumns` 和 `AddColumns` 中使用。

**需实现**:

```go
// sdk/dataset.go — Dataset 接口新增
Merge(ctx context.Context, otherFragments []*DataFragment, newSchema []*storage2pb.Field) error
```

**实现步骤**:

1. 在 `Dataset` 接口中添加 `Merge` 方法
2. 在 `datasetImpl` 中实现：
   - 获取 readVersion
   - 调用 `storage2.NewTransactionMerge(readVersion, uuid, otherFragments, newSchema, nil)`
   - `CommitTransaction` 提交
   - `refreshState` 刷新
3. 编写测试：
   - `TestDatasetMerge` — 基本合并
   - `TestDatasetMergeSchemaChange` — 带 schema 变更

**依赖**: 无新依赖

---

### P0-3: Dataset.Restore()

**底层现状**: `pkg/storage2/refs.go:387` 已有 `Restore()` 函数，可恢复到指定版本。

**需实现**:

```go
// sdk/dataset.go — Dataset 接口新增
Restore(ctx context.Context, version uint64) error
CheckoutVersion(ctx context.Context, version uint64) error
```

**实现步骤**:

1. `Restore` — 调用 `storage2.Restore()`，创建新版本指向旧版本的 manifest，刷新状态
2. `CheckoutVersion` — 只读切换：加载指定版本的 manifest，更新 `currentManifest` 和 `version`
3. 编写测试：
   - `TestDatasetRestore` — 恢复到旧版本
   - `TestDatasetCheckoutVersion` — 切换版本读取

**依赖**: 无新依赖

---

### P0-4: Dataset Tag/Branch 管理

**底层现状**: `pkg/storage2/refs.go` 已有完整的 `Tags` 和 `Branches` 实现（Create/Delete/Get/List/Update）。

**需实现**:

```go
// sdk/dataset.go — Dataset 接口新增
CreateTag(ctx context.Context, tag string, version uint64) error
DeleteTag(ctx context.Context, tag string) error
ListTags(ctx context.Context) (map[string]uint64, error)
```

**实现步骤**:

1. 在 `datasetImpl` 中创建 `Refs` 实例（使用已有的 `basePath`、`handler`、`LocalObjectStore`）
2. 代理调用 `refs.Tags().Create/Delete/List`
3. `ListTags` 返回简化的 `map[tag]version`
4. 编写测试：
   - `TestDatasetCreateTag` — 创建标签
   - `TestDatasetDeleteTag` — 删除标签
   - `TestDatasetListTags` — 列出标签

**依赖**: 需在 `datasetImpl` 中缓存 `*storage2.Refs` 实例

---

## P1: Scanner 行 ID 支持 + 索引持久化

### P1-1: Scanner.WithRowId()

**重要性**: 行 ID 是 update/delete-by-rowid 工作流的基础。Lance Rust 的 Scanner 可以在结果中附带 RowID，用于后续精确操作。

**设计方案**:

```go
// sdk/scanner.go — ScannerBuilder 新增
func (b *ScannerBuilder) WithRowId() *ScannerBuilder

// 扫描结果中每行附带 RowID
type Record struct {
    Values map[string]interface{}
    RowID  *uint64  // 新增：行 ID（nil 表示未请求）
}
```

**实现步骤**:

1. `ScannerBuilder` 新增 `includeRowId bool` 字段
2. `scannerImpl.loadNextBatch` 中：
   - 利用 `storage2.ScanChunks` 返回的 chunk，结合 fragment 的 RowIdSequence 计算全局行 ID
   - 每个 fragment 有自身的 ID 和行范围，全局行 ID = `(fragmentID << 32) | rowOffset`（Lance 标准编码）
3. 将行 ID 存入 `Record.RowID`
4. 编写测试：
   - `TestScannerWithRowId` — 验证行 ID 在结果中存在
   - `TestScannerWithRowIdAfterDelete` — 删除后行 ID 不连续

**依赖**: 需了解 `ScanChunks` 返回的 chunk 与 fragment 的映射关系

---

### P1-2: 索引持久化 (IndexStore 实现)

**现状**: `LocalIndexStore` 接口已定义（`index.go:526-569`），但所有方法都是 `TODO`，索引仅存在于内存中。

**设计方案**:

索引文件布局：
```
{basePath}/_indices/{index_name}/
  ├── metadata.json     # IndexMetadata
  ├── data.bin          # 序列化的索引数据
  └── stats.json        # IndexStatistics
```

**实现步骤**:

1. **`LocalIndexStore.WriteIndex`** — 序列化索引数据写入 `_indices/{name}/data.bin`
2. **`LocalIndexStore.ReadIndex`** — 从文件加载索引数据
3. **`LocalIndexStore.DeleteIndex`** — 删除索引目录
4. **`LocalIndexStore.ListIndexes`** — 列出 `_indices/` 下的目录
5. **索引序列化接口** — 为每种索引类型添加 `Marshal/Unmarshal`：
   - `BTreeIndex` — JSON 序列化节点结构
   - `BitmapIndex` — 二进制序列化 bitmap words
   - `ZoneMapIndex` — JSON 序列化 min/max 统计
   - `BloomFilterIndex` — 二进制序列化 filter bits
6. **`IndexManager` 集成** — 创建/加载索引时自动调用 IndexStore
7. 编写测试：
   - `TestLocalIndexStoreRoundtrip` — 写入+读取
   - `TestIndexManagerPersistence` — 创建索引 → 重新加载 → 验证

**依赖**: 各索引类型需实现序列化接口

---

### P1-3: Manifest 索引元数据集成

**现状**: proto 中有 `IndexMetadata` 字段，Go 结构体存在，但 Manifest 创建/加载时未与 `IndexManager` 关联。

**实现步骤**:

1. `BuildManifest` 在处理 `CreateIndex` 事务时，将 `IndexMetadata` 写入新 Manifest
2. 加载 Manifest 时，从 `manifest.Indices` 恢复 `IndexManager` 状态
3. `DropIndex` 事务从 Manifest 中移除对应元数据
4. 编写测试：
   - `TestManifestIndexMetadataRoundtrip` — 索引元数据序列化往返

**依赖**: P1-2 完成后

---

## P2: Scanner 高级功能

### P2-1: Scanner.UseIndex()

**设计方案**:

```go
// sdk/scanner.go
func (b *ScannerBuilder) UseIndex(indexName string) *ScannerBuilder
```

**实现步骤**:

1. `ScannerBuilder` 新增 `indexHint string` 字段
2. `scannerImpl.loadNextBatch` 中：
   - 如果设置了 indexHint 且 filter 存在，使用 `IndexSelector`（已有 `index_selector.go`）查询索引
   - 索引返回匹配的 fragment ID 列表，仅扫描这些 fragment
   - fallback: 索引不可用时退化为全表扫描
3. 集成已有的 `selectForEquality` / `selectForRange`（`index_selector.go`）
4. 编写测试：
   - `TestScannerUseIndex` — 指定索引加速扫描
   - `TestScannerUseIndexFallback` — 索引不可用时退化

**依赖**: P1-2 索引持久化

---

### P2-2: Scanner.ScanInOrder()

**设计方案**:

```go
// sdk/scanner.go
func (b *ScannerBuilder) ScanInOrder(column string, ascending bool) *ScannerBuilder
```

**实现步骤**:

1. `ScannerBuilder` 新增 `orderBy string` + `ascending bool`
2. 实现方案 A（简单版）：扫描完毕后内存排序
3. 实现方案 B（优化版）：
   - 如果列有 ZoneMapIndex，按 min 值排序 fragment 读取顺序
   - 如果列有 BTreeIndex，使用索引迭代器有序扫描
4. 编写测试：
   - `TestScannerScanInOrder` — 验证结果有序
   - `TestScannerScanInOrderDesc` — 降序

**依赖**: 基础版无依赖，优化版依赖 P1-2

---

## P3: 新索引类型

### P3-1: R-Tree 索引

**Lance 对应**: `lance-index/src/scalar/rtree.rs`

**用途**: 多维空间查询（地理位置 bounding box、2D/3D 范围查询）

**设计方案**:

```go
// pkg/storage2/rtree_index.go

type RTreeIndex struct {
    name      string
    dimension int       // 2 或 3
    root      *rTreeNode
    stats     IndexStats
}

// 节点类型
type rTreeNode struct {
    mbr      BoundingBox       // 最小包围矩形
    children []*rTreeNode      // 内部节点
    entries  []rTreeEntry      // 叶节点
    isLeaf   bool
}

type BoundingBox struct {
    Min []float64  // [x_min, y_min, ...]
    Max []float64  // [x_max, y_max, ...]
}
```

**实现步骤**:

1. 定义 R-Tree 结构和 BoundingBox 类型
2. 实现核心算法：
   - `Insert(rowID uint64, point []float64)` — 插入点（自动分裂）
   - `Search(bbox BoundingBox) []uint64` — 范围查询
   - `NearestNeighbor(point []float64, k int) []uint64` — 最近邻
3. 节点分裂策略：使用 R*-tree 的 forced reinsert
4. 集成到 `IndexManager`：`CreateRTreeIndex(ctx, name, columnIdx, dimension)`
5. 编写测试：
   - 2D 点插入和范围查询
   - 3D 扩展
   - 大量数据性能验证

**依赖**: 无

---

### P3-2: UpdateMemWalState 事务

**Lance 对应**: `Operation::UpdateMemWalState`

**用途**: 内存 WAL（Write-Ahead Log）状态管理，用于记录 generation 合并状态。

**现状**: proto 定义已有 (`Transaction_UpdateMemWalState`)，包含 `MergedGenerations` 字段。

**实现步骤**:

1. 在 `transaction.go` 中添加 `NewTransactionUpdateMemWalState()` 构造器
2. 在 `build_manifest.go` 的 `BuildManifest` switch 中添加处理分支
3. 实现 manifest 中 WAL 状态的更新逻辑
4. 在 `conflict.go` 中添加 UpdateMemWalState 的冲突检测规则
5. 编写测试

**依赖**: 无

---

## P4: 架构级增强

### P4-1: 分布式压缩

**Lance 对应**: `test_compact_distributed` — 多节点并行 compaction

**现状**: `sdk/dataset.go` 有单节点 `Compact()`/`CompactWithOptions()`，使用本地 Rewrite 事务。

**设计方案**:

```
                     Coordinator
                    ┌──────────┐
                    │ Partition │
                    │ Fragments │
                    └─────┬────┘
              ┌───────────┼───────────┐
              ▼           ▼           ▼
          Worker-1    Worker-2    Worker-3
         Compact(F0,F1) Compact(F2,F3) Compact(F4,F5)
              │           │           │
              ▼           ▼           ▼
         NewFragment  NewFragment  NewFragment
              └───────────┼───────────┘
                          ▼
                    Commit (single Rewrite txn)
```

**实现步骤**:

1. **分区策略** (`compaction_planner.go`)：
   - 按 fragment 大小分组
   - 按 fragment 间数据重叠度分组
   - 按目标大小阈值分组
2. **Worker 接口** (`compaction_worker.go`)：
   - `CompactFragments(ctx, fragments []DataFragment) ([]DataFragment, error)`
   - 本地实现 + 远程 RPC 实现（预留）
3. **Coordinator** (`compaction_coordinator.go`)：
   - 分发任务到 worker
   - 收集结果
   - 构造 Rewrite 事务并提交
4. 编写测试：
   - 多 goroutine 模拟分布式 worker
   - 冲突场景：compaction 过程中有新写入

**依赖**: 无新外部依赖，可基于 goroutine 实现本地并行版本

---

### P4-2: 完整 v2 文件编码层

**现状**: Go 有基础编码器（Plain/BitPack/RLE/Dict/VarBinary），约 3000 行。Lance Rust 编码层约 6 万行。

**主要差距**:

| 特性 | Lance Rust | Go storage2 | 差距 |
|------|------------|-------------|------|
| 列页式布局 | 完整 page 格式 | 无 page 概念 | 大 |
| 嵌套类型编码 | Struct/List/Map | 仅扁平列 | 大 |
| NULL bitmap | 独立 validity buffer | 编码器内嵌 | 中 |
| 压缩管道 | 编码→压缩→写文件 | 编码→内存字节 | 中 |
| Mini-block | 自适应 mini-block 大小 | 无 | 大 |
| 统计收集 | 编码时自动收集 | 手动调用 | 小 |

**实现步骤**:

1. **Page 格式定义** — 对齐 Lance v2 file format spec
2. **ColumnWriter/ColumnReader** — 流式列写入/读取
3. **嵌套类型支持** — Struct (definition/repetition levels)
4. **压缩集成** — zstd/lz4 压缩管道
5. **文件格式** — Header + Column Pages + Footer + Index
6. 编写测试：
   - 各编码类型 roundtrip
   - 混合编码 chunk 读写
   - 与 Lance Rust 生成的文件交叉验证

**依赖**: 需引入压缩库 (zstd)

---

## 执行优先级总览

```
P0: SDK API 暴露（底层已有，编写薄封装）
├── P0-1: Dataset.Update()
├── P0-2: Dataset.Merge()
├── P0-3: Dataset.Restore() / CheckoutVersion()
└── P0-4: Dataset.CreateTag/DeleteTag/ListTags

P1: Scanner 行 ID + 索引持久化
├── P1-1: Scanner.WithRowId()
├── P1-2: IndexStore 持久化实现
└── P1-3: Manifest 索引元数据集成

P2: Scanner 高级功能
├── P2-1: Scanner.UseIndex()
└── P2-2: Scanner.ScanInOrder()

P3: 新功能模块
├── P3-1: R-Tree 索引
└── P3-2: UpdateMemWalState 事务

P4: 架构级增强
├── P4-1: 分布式压缩
└── P4-2: 完整 v2 文件编码层
```

---

## 依赖关系图

```
P0-1 (Update)  ─────────────────────────────────┐
P0-2 (Merge)                                     │ (无互相依赖，可并行)
P0-3 (Restore)                                   │
P0-4 (Tags)    ─────────────────────────────────┘
                          │
                          ▼
P1-1 (WithRowId)  ←──── P0 完成后
P1-2 (IndexStore)  ←──── 无依赖，可与 P1-1 并行
P1-3 (Manifest索引) ←── P1-2
                          │
                          ▼
P2-1 (UseIndex) ←──────── P1-2 + P1-3
P2-2 (ScanInOrder) ←───── 无强依赖（基础版）/ P1-2（优化版）
                          │
                          ▼
P3-1 (R-Tree)     ←───── 无依赖
P3-2 (MemWalState) ←──── 无依赖
                          │
                          ▼
P4-1 (分布式压缩) ←────── P0-2 (Compact 基础)
P4-2 (v2 编码)   ←────── 无依赖，但工作量最大
```

---

## 文件变更预估

| 阶段 | 新增文件 | 修改文件 | 预估新增代码量 |
|------|---------|---------|-------------|
| P0 | 0 | `sdk/dataset.go`, `sdk/dataset_test.go` | ~300 行 |
| P1-1 | 0 | `sdk/scanner.go`, `sdk/scanner_test.go` | ~150 行 |
| P1-2 | 0 | `pkg/storage2/index.go`, 各索引文件 | ~500 行 |
| P1-3 | 0 | `pkg/storage2/build_manifest.go`, `index.go` | ~200 行 |
| P2 | 0 | `sdk/scanner.go` | ~200 行 |
| P3-1 | `pkg/storage2/rtree_index.go` | `index.go` | ~800 行 |
| P3-2 | 0 | `transaction.go`, `build_manifest.go`, `conflict.go` | ~150 行 |
| P4-1 | `pkg/storage2/compaction_*.go` (3 文件) | `sdk/dataset.go` | ~1500 行 |
| P4-2 | `pkg/storage2/lance_v2_*.go` (5+ 文件) | `lance_encoder.go` | ~5000 行 |
| **合计** | ~9 个新文件 | ~15 个修改文件 | **~8800 行** |
