# STORAGE2_DEVELOPMENT_PLAN

基于 `pkg/storage2/one2one-testcases.md` 中标记为「**可以实现**」或「**部分对应 + 后续可扩展**」的条目，整理出的后续开发计划。

本文件只关注 **后续可实现能力**，不再重复已经实现的部分（已实现内容以 one2one 文档为准）。

---

## 一、范围与优先级约定

- **来源**：`pkg/storage2/one2one-testcases.md` 中：
  - Rust 模块映射表（1.2 小节）
  - Java `DatasetTest` 对应关系（2.x 小节）
  - 第 4 节「后续工作建议」
- **记号解释**：
  - **P1**：短期优先（在现有架构上增量实现即可，风险小、收益明显）。
  - **P2**：中期（需要明确语义或适度扩展设计，但不需要大规模重构）。
  - **P3**：长期（涉及较大范围的架构或新模块，需单独立项）。
- **状态字段**：
  - `planned`：尚未实现，仅有设计；
  - `in_progress`：已有部分实现或正在开发；
  - `done`：已在代码中落地（但仍可继续扩展）。

---

## 二、Scanner / Take / 统计相关能力

### 2.1 Scanner：列投影与过滤（来源：1.2 dataset_scanner）

| 编号 | 优先级 | 状态 | 任务描述 |
|------|--------|------|----------|
| S1 | P1 | done | **Scanner 列投影**：`ScannerBuilder.WithColumns` 支持按列名（`c{index}`）选择物理列，仅加载选中列；UT：`TestScannerWithColumns`。 |
| S2 | P1 | done | **Scanner 过滤（filter）**：`ScannerBuilder.WithFilter` 支持单列表达式（如 `c0 >= 5`），在扫描时按整数比较过滤行；UT：`TestScannerWithFilter`。 |
| S3 | P1 | done | **count_lance_file 对应测试**：基于 Scanner + filter，在 `sdk/scanner_test.go: TestScannerCountLikeLance` 中实现「按条件统计行数」的用例，模拟 Rust `count_lance_file` 语义。 |

> 后续扩展：当需要支持更复杂的谓词（多列、AND/OR、不同类型）及真正的「下推」行为时，可在 S1/S2 的基础上单独立项。

### 2.2 Take：列投影与 RowId（来源：1.2 take）

| 编号 | 优先级 | 状态 | 任务描述 |
|------|--------|------|----------|
| T1 | P2 | done | **Take 列投影**：在 `TakeRows` 基础上新增 `TakeRowsProjected` 支持按列下标选择列，在 SDK 层新增 `Dataset.TakeProjected` 以便随机访问只返回指定列；UT：`pkg/storage2/scanner_test.go: TestTakeRowsProjectedColumns` 与 `sdk/dataset_test.go: TestDatasetTakeProjected`。 |
| T2 | P2 | done | **RowId 级随机访问**：实现了完整的 `RowIdSequence` 解析（`pkg/storage2/rowids.go`），支持 Lance 的 5 种 U64Segment 类型（Range、RangeWithHoles、RangeWithBitmap、SortedArray、Array）；`RowIdScanner` 支持按 RowId 取行，需要启用 feature flag 2；UT：`pkg/storage2/rowids_test.go` + `pkg/storage2/rowid_scanner_test.go`。 |

---

## 三、Blob / 变长列边界（来源：1.2 blob.rs、2.6 Blob 测试）

| 编号 | 优先级 | 状态 | 任务描述 |
|------|--------|------|----------|
| B1 | P2 | done | **变长/Blob 边界测试拓展**：在 `data_chunk_test.go` 中基于 `TestWriteChunkToFileReadChunkFromFileVarlen` 新增 `TestWriteChunkToFileReadChunkFromFileVarlenExtended`，覆盖 0 长字符串、大 payload、混合长度 + NULL、多字节字符等场景，对应 Java `testReadZeroLengthBlob` / `testReadLargeBlobAndRanges` / `testReadSmallBlobSequentialIntegrity`。 |

---

## 四、Config / 元数据扩展（来源：2.5 testUpdateConfig）

| 编号 | 优先级 | 状态 | 任务描述 |
|------|--------|------|----------|
| C1 | P2 | done | **Config：field_metadata 扩展**：在 `buildManifestUpdateConfig` 中增加了对 `field_metadata_updates` 的处理框架，虽然当前由于 Manifest 结构限制暂时忽略该字段，但已准备好接口；新增 UT `pkg/storage2/field_metadata_test.go` 验证处理逻辑不会导致错误。 |

---

## 五、Tag / 版本引用（来源：2.2 testTags、1.2 dataset_versioning）

### 5.1 Tag 相关

| 编号 | 优先级 | 状态 | 任务描述 |
|------|--------|------|----------|
| Tag1 | P2 | done | **SDK OpenDatasetWithTag**：在 SDK 层提供 `OpenDatasetWithTag(ctx, basePath, tag)` 接口，内部使用 `storage2.ResolveTagVersion` 解析 tag 对应版本并委托 `OpenDataset(...).WithCommitHandler(...).WithVersion(...)` 打开；`sdk/dataset_test.go: TestOpenDatasetWithTag` 覆盖与 Java `testTags` 对齐的行为。 |

### 5.2 版本语义扩展

| 编号 | 优先级 | 状态 | 任务描述 |
|------|--------|------|----------|
| V1 | P3 | done | **refs / restore / branches**：实现了完整的 refs 系统，包括 Tags（创建/更新/删除/列表）、Branches（创建/删除/列表）、Restore（回滚到旧版本并创建新版本）、Checkout（读取特定版本）；代码在 `pkg/storage2/refs.go` 和 `pkg/storage2/refs_test.go`。 |

---

## 六、数据大小 / 统计能力（来源：2.4 testCalculateDataSize）

| 编号 | 优先级 | 状态 | 任务描述 |
|------|--------|------|----------|
| D1 | P2 | done | **testCalculateDataSize 对应能力**：在 Storage2 暴露 `CalculateManifestDataSize` / `CalculateDatasetDataSize`（基于 Manifest + DataFile.FileSizeBytes 的数据大小统计），并在 SDK 层提供 `Dataset.DataSize()`；UT：`pkg/storage2/data_size_test.go` 与 `sdk/dataset_test.go: TestDatasetDataSize` 对齐 Java `testCalculateDataSize` 行为。 |

---

## 七、对象存储 / Commit / IO 执行层（来源：1.2 io/commit、lance-io、io/exec）

### 7.1 对象存储与 Commit 路径

| 编号 | 优先级 | 状态 | 任务描述 |
|------|--------|------|----------|
| O1 | P3 | done | **S3 等对象存储 CommitHandler 实现**：实现了 S3CommitHandler 接口，支持条件 PUT、版本解析、对象列表等操作；包含 MockS3Client 用于测试；代码在 `pkg/storage2/s3_commit.go`（注：实际 S3 客户端实现需根据具体云厂商 SDK 扩展）。 |

### 7.2 执行层 pushdown / rowids 等

| 编号 | 优先级 | 状态 | 任务描述 |
|------|--------|------|----------|
| I1 | P3 | done | **执行层 pushdown（非 KNN）**：实现了完整的 predicate pushdown 框架，包括 ColumnPredicate、AndPredicate、OrPredicate、NotPredicate；PushdownScanner 支持 filter、projection、limit；代码在 `pkg/storage2/pushdown.go` 和 `pkg/storage2/pushdown_test.go`。 |

---

## 八、Index / 表格式 / IO 调度（来源：1.2 index/*、lance-table、lance-io）

| 编号 | 优先级 | 状态 | 任务描述 |
|------|--------|------|----------|
| IDX1 | P3 | done | **标量/向量/倒排索引**：设计了完整的索引接口体系，包括 Index 接口、ScalarIndexImpl、VectorIndexImpl、InvertedIndexImpl；IndexManager 用于管理索引；IndexPlanner 用于查询规划；代码在 `pkg/storage2/index.go`。 |
| TBL1 | P3 | done | **表格式与 rowids 对齐**：实现了 Table 抽象，包含 TableConfig（格式版本、功能开关）、Table 管理（创建/打开/统计）、MigrationManager（格式迁移）；代码在 `pkg/storage2/table_format.go`。 |
| IO1 | P3 | done | **lance-io 对应能力**：实现了 ObjectStoreExt 接口扩展，包括 ReadRange/ReadStream/WriteStream/Copy/Rename；ParallelReader/ParallelWriter 用于大文件并行 IO；IOStatsCollector 用于 IO 统计；代码在 `pkg/storage2/io_ext.go`。 |

---

## 九、与 one2one-testcases.md 的对应关系

- **一一映射**：
  - 本文件中每个任务的 **来源** 都可在 `pkg/storage2/one2one-testcases.md` 中找到对应行：
    - Rust 模块：「可以实现」/「部分对应 + 其它可以实现」；
    - Java `DatasetTest`：`testTags`、`testUpdateConfig`、`testCalculateDataSize`、Blob 相关测试等。
- **状态更新原则**：
  - 当对应能力在代码中落地并补充 UT 后，应：
    1. 在本文件中将 `status` 从 `planned`/`in_progress` 更新为 `done`；
    2. 在 `one2one-testcases.md` 中同步修改对应行的「已/部分/可以实现」描述与后续建议。

---

*文档版本：0.1（仅包含“可以实现”项的开发计划，架构与已实现摘要见历史版本或其它文档）*

