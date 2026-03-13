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
| T1 | P2 | planned | **Take 列投影**：在 `TakeRows` 及 SDK `Dataset.Take` 上增加列选择能力，使随机访问也能只返回指定列。 |
| T2 | P2 | planned | **RowId 级随机访问**：若未来引入稳定 RowId 语义，为 `Take` 增加按 RowId 取行的能力；需与 `testEnableStableRowIds` 语义对齐。 |

---

## 三、Blob / 变长列边界（来源：1.2 blob.rs、2.6 Blob 测试）

| 编号 | 优先级 | 状态 | 任务描述 |
|------|--------|------|----------|
| B1 | P2 | done | **变长/Blob 边界测试拓展**：在 `data_chunk_test.go` 中基于 `TestWriteChunkToFileReadChunkFromFileVarlen` 新增 `TestWriteChunkToFileReadChunkFromFileVarlenExtended`，覆盖 0 长字符串、大 payload、混合长度 + NULL、多字节字符等场景，对应 Java `testReadZeroLengthBlob` / `testReadLargeBlobAndRanges` / `testReadSmallBlobSequentialIntegrity`。 |

---

## 四、Config / 元数据扩展（来源：2.5 testUpdateConfig）

| 编号 | 优先级 | 状态 | 任务描述 |
|------|--------|------|----------|
| C1 | P2 | planned | **Config：field_metadata / schema_metadata 扩展**：在 `build_manifest.go` 的 `buildManifestUpdateConfig` 基础上，支持 `field_metadata_updates` / `schema_metadata_updates` 等更细粒度更新；在 `config_test.go` 增加对应用例，对齐 Java `testUpdateConfig`/`testDeleteConfigKeys` 更完整语义。 |

---

## 五、Tag / 版本引用（来源：2.2 testTags、1.2 dataset_versioning）

### 5.1 Tag 相关

| 编号 | 优先级 | 状态 | 任务描述 |
|------|--------|------|----------|
| Tag1 | P2 | done | **SDK OpenDatasetWithTag**：在 SDK 层提供 `OpenDatasetWithTag(ctx, basePath, tag)` 接口，内部使用 `storage2.ResolveTagVersion` 解析 tag 对应版本并委托 `OpenDataset(...).WithCommitHandler(...).WithVersion(...)` 打开；`sdk/dataset_test.go: TestOpenDatasetWithTag` 覆盖与 Java `testTags` 对齐的行为。 |

### 5.2 版本语义扩展

| 编号 | 优先级 | 状态 | 任务描述 |
|------|--------|------|----------|
| V1 | P3 | planned | **refs / restore / branches**：在当前单线性版本模型之上，设计 refs（命名版本引用）、restore（回滚到旧版本）以及 branches（分支）；参考 Rust `dataset_versioning.rs` 与 Java `testBranches`，明确语义后再拆成更小的实现任务。 |

---

## 六、数据大小 / 统计能力（来源：2.4 testCalculateDataSize）

| 编号 | 优先级 | 状态 | 任务描述 |
|------|--------|------|----------|
| D1 | P2 | planned | **testCalculateDataSize 对应能力**：在 Storage2 暴露一个基于 Manifest + DataFile size 字段的「数据大小」统计 API（可在 SDK 层提供方法）；新增 UT 对齐 Java `testCalculateDataSize` 行为。 |

---

## 七、对象存储 / Commit / IO 执行层（来源：1.2 io/commit、lance-io、io/exec）

### 7.1 对象存储与 Commit 路径

| 编号 | 优先级 | 状态 | 任务描述 |
|------|--------|------|----------|
| O1 | P3 | planned | **S3 等对象存储 CommitHandler 实现**：在现有 `CommitHandler` 抽象基础上，为对象存储（如 S3）实现提交路径（原子写入、latest version 解析等），对齐 Rust `io/commit/*.rs` 中的 S3 语义；保留 DynamoDB/外部 manifest 为暂不实现。 |

### 7.2 执行层 pushdown / rowids 等

| 编号 | 优先级 | 状态 | 任务描述 |
|------|--------|------|----------|
| I1 | P3 | planned | **执行层 pushdown（非 KNN）**：围绕 `scan/filtered_read/rowids/fts` 等场景，在 Storage2 的扫描与执行路径上引入基础的 predicate 下推、rowid 回填等能力；参考 Rust `io/exec/*`，不包含 KNN（KNN 仍暂不实现）。 |

---

## 八、Index / 表格式 / IO 调度（来源：1.2 index/*、lance-table、lance-io）

| 编号 | 优先级 | 状态 | 任务描述 |
|------|--------|------|----------|
| IDX1 | P3 | planned | **标量/向量/倒排索引**：参考 `rust/lance/src/index/*`、`lance-index/src/*`，设计 Storage2 中的索引接口与元数据表示，并规划最小实现（例如标量索引）；具体实现另行立项。 |
| TBL1 | P3 | planned | **表格式与 rowids 对齐**：根据 `rust/lance-table/src/*`，在不破坏现有 Manifest 兼容性的前提下，逐步对齐表格式与 rowid 相关语义；需要时扩展 Manifest 字段。 |
| IO1 | P3 | planned | **lance-io 对应能力**：结合 `rust/lance-io/src/*`，在 Storage2 现有 `LocalObjectStore` 基础上规划调度、缓存、编码等能力的引入路径（可与 O1/I1 组合规划）。 |

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

