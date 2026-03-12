# Storage2 vs Lance 测试用例一一对应设计

本文档根据 Lance 的测试用例（尤其是 `java/src/test/java/org/lance/DatasetTest.java` 与 `rust/lance-file/src/testing.rs`），为 Storage2（`pkg/storage2` + `sdk/`）设计对应的测试用例，便于对齐行为并发现差异。

设计原则：

- **按场景一一对应**：每个 Lance 测试方法 / 场景，在 Storage2 中都有一个对应的 Go 测试（已实现或计划）。
- **优先覆盖元数据与事务**：Manifest / Transaction / Commit / Conflict / Version / 路径 等，与 Storage2 现有实现严格对齐。
- **数据文件统一使用 `pkg/chunk`**：Lance Arrow 文件读写由 `rust/lance-file` 测试覆盖；Storage2 侧用 `data_chunk_test.go` 和集成测试替代。
- **暂不覆盖的高级特性**（索引、Compaction、分支、Clone、Schema 变更等）在表中标记为 “暂不实现”，后续如实现再补充测试。

---

## 1. Rust `lance-file` 测试辅助与 Storage2 对应

`rust/lance-file/src/testing.rs` 主要提供文件级别的测试辅助，用于 Lance Arrow 文件读写与扫描测试：

- `FsFixture`：临时目录 + 本地 `ObjectStore` + `ScanScheduler`
- `write_lance_file`：将 Arrow `RecordBatchReader` 写为 Lance 文件，并记录 schema / data / field_id_mapping
- `read_lance_file`：按 Filter / Decoder 读取 Lance 文件并校验 metadata
- `count_lance_file`：统计行数

Storage2 对应关系（更偏元数据 + Chunk 数据文件）：

| Rust 辅助/场景 | 覆盖能力简述 | Storage2 已有/计划测试 | 说明 |
|----------------|--------------|------------------------|------|
| `FsFixture` + `ObjectStore::local()` | 本地对象存储 + 调度器 | `io_test.go: TestLocalObjectStore*` | Storage2 的 `LocalObjectStore` 覆盖本地对象存储的读写/列目录/自动建目录等能力。 |
| `write_lance_file` / `read_lance_file` | Arrow `RecordBatch` → Lance 文件 → 过滤读取 | `data_chunk_test.go: TestWriteChunkToFile*`, `all_test.go: TestAll` | Storage2 不实现 Arrow 文件，而是 Chunk 文件；对应测试验证 `WriteChunkToFile` / `ReadChunkFromFile` 与数据正确性。 |
| `count_lance_file` | 按过滤表达式统计行数 | （未来）`scanner_test.go` | 需在 Storage2 引入扫描/过滤 API 后，对应增加基于 Chunk 的行计数测试。当前仅在 `all_test.go` 中做简单读取校验。 |

---

## 1.1 Rust Lance（`rust/`）测试用例覆盖范围说明（重要）

结论：**否**，目前本文档**没有**记录 `lance/rust/` 下“所有测试 case”，也不可能在 Storage2 现阶段为其提供一一对应的 Go 测试实现。

原因：

- Lance Rust 侧包含大量测试，覆盖 **Arrow/Lance 文件格式、扫描执行引擎、Schema 演进、行号与稳定 row id、对象存储（S3/DDB）、Compaction、Index（标量/向量/倒排）、DataFusion、Namespace** 等；而 Storage2 当前仅实现元数据与事务提交（Append/Delete/Overwrite/Conflict/Rebase）+ `pkg/chunk` 数据文件读写 + 一个精简 SDK。
- 因此本文档将 Rust 侧测试按“**模块/能力簇**”补充记录，并标注 Storage2 是否已有对应测试/是否暂不实现。等 Storage2 功能落地后，再逐步把“暂不实现”的条目拆成更细的一一对应用例。

### 1.2 Rust 侧主要测试模块（按文件/目录）与 Storage2 对应情况

下面列出本次扫描到的 Rust 测试入口（`#[test]` / `tokio::test` / `rstest`）所覆盖的主要模块。**这些模块当前大多未在本文档记录**，现补充如下：

| Rust 测试模块（示例路径） | 关注点 | Storage2 对应情况 |
|---|---|---|
| `rust/lance/src/dataset/tests/dataset_versioning.rs` | Dataset 版本/checkout/refs 等 | **部分对应**：SDK 级 `TestDatasetVersioning`、`TestCheckoutVersion`；更多语义（refs/restore/branches）**暂不实现** |
| `rust/lance/src/dataset/tests/dataset_scanner.rs`、`rust/lance/src/dataset/scanner.rs` | 扫描、投影、过滤、batch 读取 | **暂不实现**（需新增 Scanner API + 基于 Chunk 的扫描执行） |
| `rust/lance/src/dataset/take.rs`、`rust/lance/src/io/exec/take.rs` | Take / 随机访问 | **暂不实现**（需 row→fragment/chunk 映射 + Take API；可复用 `fragment_offsets.go`） |
| `rust/lance/src/dataset/schema_evolution.rs`、`dataset_io.rs` | Schema 演进、读写兼容 | **暂不实现**（需 Schema API + Manifest/schema 更新） |
| `rust/lance/src/dataset/blob.rs` | Blob/变长列读写边界 | **部分对应**：`data_chunk_test.go: TestWriteChunkToFileReadChunkFromFileVarlen`（变长字符串边界）；后续可扩展到 BLOB/更大 payload | |
| `rust/lance/src/io/commit/*.rs`（含 s3/dynamodb/external manifest） | 提交协议、对象存储一致性、外部 manifest | **部分对应**：`commit.go`/`commit_txn.go`/冲突矩阵；对象存储/S3/DDB **暂不实现** |
| `rust/lance/src/index/*`、`rust/lance-index/src/*` | 标量/向量/倒排索引、统计、优化 | **暂不实现** |
| `rust/lance/src/io/exec/*`（scan/filtered_read/rowids/knn/fts 等） | 执行层 pushdown、rowid、knn、全文等 | **暂不实现** |
| `rust/lance-table/src/*` | 表格式/manifest/rowids | **部分对应**：Manifest/Transaction proto 结构对齐；其余 **暂不实现** |
| `rust/lance-io/src/*` | object_store/scheduler/encodings | **部分对应**：`LocalObjectStore` 的最小读写/list/mkdir；调度与编码 **暂不实现** |
| `rust/lance-encoding/src/*` | 编解码、统计、压缩 | **暂不实现** |
| `rust/lance-datafusion/src/*` | SQL / DataFusion 互操作 | **暂不实现** |

---

## 2. Lance Java `DatasetTest` 与 Storage2 对应测试设计

Java 侧 `DatasetTest` 是 Lance Dataset 的端到端测试主力，覆盖 Dataset 创建、版本管理、Tag、Schema 变更、Compaction、分支、索引等。

Storage2 当前仅实现：Manifest / Transaction / Commit / Conflict / Path 约定 / DataFile+Fragment / Chunk 数据文件、以及 `sdk.Dataset` 的 Append/Delete/Overwrite/Version/CountRows。以下按 `DatasetTest` 中每个 `test*` 方法设计对应测试。

### 2.1 基础创建/打开与路径合法性

| Lance 测试 | 场景描述 | Storage2 对应测试（建议文件/函数） | 说明 |
|------------|----------|-------------------------------------|------|
| `testWriteStreamAndOpenPath` | 通过 Arrow 流写入数据集，再从路径打开并校验 | **已有**：`sdk/dataset_test.go: TestCreateAndOpenDataset` | Storage2 使用 Chunk+`WriteChunkToFile` 写数据文件，通过 `sdk.CreateDataset`+`Append`、`sdk.OpenDataset` 校验版本和行数，相当于 Lance 的“写入+打开”。 |
| `testCreateEmptyDataset` | 创建空数据集，不写入数据即可关闭 | **已有**：`sdk/dataset_test.go: TestCreateAndOpenDataset`（前半段） | 已覆盖创建空数据集、`Version()/CountRows()` 行为。 |
| `testCreateDirNotExist` | 在不存在目录下创建数据集 | **已有**：`sdk/dataset_test.go: TestCreateAndOpenDataset` 使用 `t.TempDir()`；Storage2 Commit 会自动创建 `_versions/` 等目录 | 行为等价：在空目录下 `CreateDataset` 成功。 |
| `testOpenInvalidPath` | 打开非法路径应失败 | **已有**：`sdk/dataset_test.go: TestOpenInvalidPath` | 使用不存在或未初始化的 `basePath` 调用 `sdk.OpenDataset(...).Build()`，期望返回错误。 |
| `testDatasetUri` | Dataset URI（路径格式）行为 | **暂不实现** | Storage2 目前只支持本地路径，不暴露 URI 解析层，留待将来支持对象存储 URI 时补充。 |
| `testOpenNonExist` | 打开不存在的数据集路径 | **已有**：`sdk/dataset_test.go: TestOpenNonExist` | 空目录下 `OpenDataset(...).Build()` 返回错误。 |
| `testOpenSerializedManifest` | 使用已存在 Manifest 文件打开数据集 | **已有**：`comparison_test.go: TestLoadManifestFixture` + `sdk/dataset_test.go: TestOpenExistingManifestDataset` | 底层 API 写入 manifest 后，SDK 打开并校验版本与行数。 |
| `testCreateExist` | 在已存在数据集目录上再次创建的行为 | **已有**：`sdk/dataset_test.go: TestCreateOnExistingDir` | 再次 `CreateDataset.Build()` 得到版本 0 空数据集（覆盖/重置）。 |

### 2.2 版本、时间旅行与版本列表

| Lance 测试 | 场景描述 | Storage2 对应测试 | 说明 |
|------------|----------|-------------------|------|
| `testDatasetVersion` | 版本号递增、`latestVersion`、版本时间戳、按版本打开 | **已有**：`commit_test.go`、`commit_txn_test.go`、`sdk/dataset_test.go: TestDatasetVersioning` | Storage2 不维护时间戳；版本号与 latestVersion、`WithVersion` 已覆盖。 |
| `testDatasetCheckoutVersion` | checkout 到旧版本再读 | **已有**：`sdk/dataset_test.go: TestCheckoutVersion` | 通过 `OpenDataset(...).WithVersion(v)` 打开旧版本，校验 `CountRows()` 随版本变化。 |
| `testDatasetRestore` | Restore 版本 | **暂不实现** | Storage2 未提供 Restore/Undo 语义。 |
| `testTags` | Tag（命名版本）相关操作 | **暂不实现/后续** | Manifest 中已有 Tag 字段设计（见开发计划），但实现/测试尚未完成，未来可在 `version_test.go` 或单独 `tags_test.go` 中增加与 Lance 对齐的 Tag 行为测试。 |
| `testBranches` | 分支管理（类似 Git Branch） | **暂不实现** | Storage2 当前版本模型为单线性版本号，无分支；保留为未来扩展。 |

### 2.3 Schema / 列操作

这些测试主要验证 Lance 的 Schema 管理与列操作；Storage2 目前只维护 Fragment 中的字段 ID，不提供完整 Schema/列操作 API。

| Lance 测试 | 场景描述 | Storage2 对应测试 | 说明 |
|------------|----------|-------------------|------|
| `testGetSchemaWithClosedDataset` | 关闭后获取 Schema 行为 | **暂不实现** | SDK 还未暴露 `Schema()` 方法。 |
| `testDropColumns` / `testAlterColumns` / `testAddColumnBySqlExpressions` / `testAddColumnsByStream` / `testAddColumnByFieldsOrSchema` / `testDropPath` | 列删除/修改/新增（包括 SQL 表达式） | **暂不实现** | Storage2 目前没有列级 schema 变更逻辑，也无对应 Manifest 字段更新；待将来实现 Schema 管理后参考这些测试设计对等用例。 |
| `testGetLanceSchema` / `testReplaceSchemaMetadata` / `testReplaceFieldConfig` | 获取/更新 Lance Schema 和字段配置 | **暂不实现** | 同上，属于 Schema API 范畴。 |

### 2.4 行级操作与统计

| Lance 测试 | 场景描述 | Storage2 对应测试 | 说明 |
|------------|----------|-------------------|------|
| `testTake` | 按行号随机访问 | **已有**：`scanner_test.go: TestTakeRowsSingleFragment` / `TestTakeRowsMultiFragment` | 通过 `TakeRows` + `ComputeFragmentOffsets` 支持单/多 fragment 的随机访问（基础整型列）。 |
| `testCountRows` | 按条件计数 | **已有基础**：`sdk/dataset_test.go: TestCreateAndOpenDataset` / `TestDeleteAndOverwrite` | 当前只测试全表 `CountRows()`；未来扩展带谓词的计数时可对齐。 |
| `testCalculateDataSize` | 计算数据大小 | **暂不实现** | Storage2 未暴露数据大小统计 API，后续可用 Manifest + DataFile 的 size 字段实现。 |
| `testDeleteRows` | 逻辑删除行 | **已有**：`comparison_test.go: TestOperationBehaviorDelete`、`sdk/dataset_test.go: TestDelete` | Manifest 层 Delete + SDK 端到端 Delete（当前按 Fragment 粒度）。 |

### 2.5 配置和元数据

| Lance 测试 | 场景描述 | Storage2 对应测试 | 说明 |
|------------|----------|-------------------|------|
| `testUpdateConfig` / `testDeleteConfigKeys` | 更新/删除表级配置 | **暂不实现** | Storage2 Proto 中已有 `config` 字段，但当前 BuildManifest/Commit 尚未提供高层配置 API；后续可在 `config_test.go` 中增加。 |
| `testReadTransaction` | 读取事务文件列表 | **已有**：`txn_file_test.go: TestWriteTransactionFile`、`TestParseTransactionFilename`、`TestLoadTransactionsAfter` | 事务文件读写、命名解析、按版本列举已提交事务。 |
| `testCommitTransactionDetachedTrue` / `testCommitTransactionDetachedTrueOnV1ManifestThrowsUnsupported` | Detached Transaction Commit 行为 | **暂不实现** | Storage2 目前只实现简单的 Append/Delete/Overwrite 事务提交模型。 |
| `testEnableStableRowIds` | 启用稳定 RowId | **暂不实现** | Storage2 尚未支持稳定行 ID；未来实现后需参考该测试设计行 ID 稳定性的用例。 |

### 2.6 Compaction / Clone / 索引

这些测试覆盖 Lance 的高级功能，当前不在 Storage2 范围内。

| Lance 测试 | 场景描述 | Storage2 对应测试 | 说明 |
|------------|----------|-------------------|------|
| `testCompact` / `testCompactWithDeletions` / `testCompactWithMaxBytesAndBatchSize` / `testMultipleCompactions` / `testCompactWithAllOptions` | Compaction 行为与参数 | **暂不实现** | Storage2 还未实现 Compaction；后续如增加 Compaction，可直接对照这些测试设计 P0 用例。 |
| `testShallowClone` | 浅克隆数据集 | **暂不实现** | Storage2 未实现 Clone。 |
| `testOptimizingIndices` / `testIndexStatistics` / `testDescribeIndicesByName` | 索引优化与统计 | **暂不实现** | Storage2 尚未接入 Lance Index 模块。 |
| `testReadZeroLengthBlob` / `testReadLargeBlobAndRanges` / `testReadSmallBlobSequentialIntegrity` | Blob 列读写边界 | **部分对应**：`data_chunk_test.go: TestWriteChunkToFileReadChunkFromFileVarlen` 覆盖空字符串/多字节/较长字符串；后续可在此基础上增加更大 payload 与 BLOB 类型测试。 |

---

## 3. Storage2 现有测试与 Lance 场景的对应关系（摘要）

为便于整体把握，列出 Storage2 已有测试与 Lance 行为的对齐关系：

| Storage2 测试 | 覆盖内容 | 对应 Lance 场景 |
|---------------|----------|------------------|
| `version_test.go` | `ManifestPath` / `TransactionPath` / `ParseVersion` | 路径约定 & 版本解析（`DatasetTest` 中版本路径相关逻辑 + 文档约定） |
| `manifest_test.go: TestManifestRoundTrip` | Manifest Proto round-trip + DataFragment/DataFile 结构 | Dataset 版本元数据正确序列化/反序列化 |
| `manifest_test.go: TestBuildManifest*` / `comparison_test.go: TestOperationBehavior*` | Append / Overwrite / Delete 对 Manifest 的影响 | `Dataset` Append/Delete/Overwrite 后 Fragment 列表和版本号的变化 |
| `txn_file_test.go` / `transaction_test.go` | Transaction Proto & `.txn` 文件 round-trip；`TestLoadTransactionsAfter` 按版本列举已提交事务 | `testReadTransaction` 场景 |
| `commit_test.go: TestLocalRenameCommit` | CommitHandler 的 `Commit/ResolveLatestVersion/ResolveVersion` 行为 | Dataset 不同版本 Manifest 的生成与加载 |
| `commit_txn_test.go: TestCommitTransaction*` | Append/Overwrite 事务提交 + 冲突检测 | `Dataset` 多事务提交与冲突语义 |
| `conflict_test.go` / `comparison_test.go: TestConflict*` / `TestRebaseResultMatchesOrder` | 事务冲突矩阵与 Rebase 行为 | 事务并发与重放的行为与 Lance 设计一致性 |
| `fragment_offsets_test.go` | Fragment -> 行号偏移映射 | Dataset 级随机访问（`testTake` 等）的基础 |
| `io_test.go` | LocalObjectStore 读写/列目录/自动建目录 | Rust `ObjectStore::local()` 行为 |
| `data_chunk_test.go` | Chunk 文件写入/读取/缺失文件错误；变长字符串往返校验 | Rust lance-file 文件级测试（按 Storage2 Chunk 格式重现）+ 变长列边界（字符串） |
| `scanner_test.go` | ScanChunks（全表扫描，支持空表）；TakeRows（单/多 fragment 随机访问） | Rust `dataset_scanner` / `take` 测试的最小子集（无 filter/投影，仅按行号访问） |
| `all_test.go` | 从 Manifest 0 开始，写入 Chunk、创建 DataFile/Fragment、Append 事务并读回 Chunk 校验数据 | `testWriteStreamAndOpenPath` + `testCountRows` 的简单端到端版本 |
| `sdk/dataset_test.go` | `CreateDataset` / `OpenDataset` / Append / Delete / Overwrite / Version / CountRows；TestOpenInvalidPath / TestOpenNonExist / TestOpenExistingManifestDataset / TestCreateOnExistingDir / TestCheckoutVersion / TestDelete | `DatasetTest` 中创建/打开/版本/行数/删除等核心场景 |

---

## 4. 后续工作建议

- **P0（已完成）**：SDK 层版本与错误路径测试已实现（`TestDatasetVersioning`、`TestCheckoutVersion`、`TestOpenInvalidPath`、`TestOpenNonExist`、`TestOpenExistingManifestDataset`、`TestCreateOnExistingDir`、`TestDelete`）；事务列表测试已实现（`txn_file_test.go: TestLoadTransactionsAfter`）。
- **P1：设计 Scanner/Take 测试**：在 Storage2 中引入 Scanner/Take API 后，对齐 `testTake`、`testCountRows` 的行为。
- **P2：Schema/Config/Compaction/Index 等高级特性**：当对应功能在 Storage2 实现后，直接参考上表中的 `DatasetTest` 用例，为每个功能补充一一对应的 Go 测试。

