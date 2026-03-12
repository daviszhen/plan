# Storage2 开发计划

本文档基于对 [Lance](https://github.com/lancedb/lance) 存储与事务结构的分析，在 plan 项目 `pkg/storage2` 下设计新的存储引擎开发计划，借鉴 Lance 的版本化 Manifest、Fragment/DataFile 分层与乐观并发提交模型。

---

## 一、Lance 存储与事务结构摘要

### 1.1 整体层次（Lance）

```
Dataset (表/数据集)
  └── Manifest (版本快照)
       └── Fragments (逻辑数据块)
            └── DataFiles (物理文件引用)
                 └── 物理文件 (列式、Page、ColumnMetadata、Footer)
```

- **读路径**：打开数据集 → 解析版本（CommitHandler）→ 加载 Manifest → 基于 fragments/schema 读数据。
- **写路径**：基于某版本构造 Transaction → 写事务文件 → 构建新 Manifest → 通过 CommitHandler 原子提交；冲突则 Rebase 重试。

### 1.2 核心数据结构（Lance）

| 概念 | 职责 |
|------|------|
| **Manifest** | 版本快照：schema、version、fragments、timestamp、config、transaction_file、next_row_id、data_storage_format 等。 |
| **Fragment** | 逻辑数据块：id、files (DataFile 列表)、deletion_file、row_id_meta、physical_rows。 |
| **DataFile** | 物理文件元数据：path、fields、column_indices、file_major/minor_version、file_size_bytes、base_id。 |
| **Transaction** | 写事务描述：read_version、uuid、operation（Append/Delete/Overwrite/Update/...）、tag、transaction_properties。 |

### 1.3 事务与提交（Lance）

- **乐观并发**：通过 `read_version` 与“新 manifest 仅写一次”保证一致性。
- **事务文件**：`_transactions/{read_version}-{uuid}.txn`（Proto 序列化），再写新 manifest。
- **CommitHandler**：抽象“如何原子写入新版本”（如 ConditionalPut、Rename、Unsafe），不同存储不同实现。
- **冲突与 Rebase**：Serializable 隔离；加载 read_version 之后已提交事务，用 TransactionRebase 做 operation 级冲突检测；可兼容则合并删除/更新并更新 read_version 后重试，不可兼容则返回 CommitConflict。

### 1.4 参考文档与代码位置（Lance）

- 事务与读写交互：`TRANSACTION_ARCHITECTURE.md`（项目内）
- 文件写入层次：`FILEWRITER_ARCHITECTURE.md`（项目内）
- Manifest：`rust/lance-table/src/format/manifest.rs`
- Fragment/DataFile：`rust/lance-table/src/format/fragment.rs`
- Transaction/Operation：`rust/lance/src/dataset/transaction.rs`
- 提交与冲突：`rust/lance/src/io/commit.rs`、`rust/lance-table/src/io/commit.rs`（CommitHandler）、`conflict_resolver.rs`

---

## 二、设计原则（借鉴 Lance）

1. **版本即 Manifest**：每个可见版本对应一份 Manifest；读总是基于某一版本快照。
2. **逻辑/物理分离**：Fragment 为逻辑块，DataFile 为物理文件引用，便于多文件、多格式扩展。
3. **写为增量操作**：Transaction 仅描述“相对 read_version 的变更”（Append/Delete/Update/Overwrite 等），便于冲突检测与 Rebase。
4. **提交可插拔**：CommitHandler 抽象，本地 FS、S3、分布式锁等可不同实现。
5. **无写锁读**：读不写任何事务文件；写通过版本号与原子提交保证可见性。

---

## 三、独立性说明

**Storage2 元数据层独立于 pkg/storage 等模块；数据文件列存使用 pkg/chunk。**

- **元数据层**（Manifest、Fragment、Transaction、CommitHandler、冲突检测等）：不依赖 `pkg/storage`、`pkg/catalog`、`pkg/compute` 等，仅使用 Go 标准库及本包（含 proto 生成代码）。
- **数据文件列存**：**直接使用 pkg/chunk**（及 pkg/common 类型体系），数据文件的读写、类型、序列化与 `chunk.Chunk`、`chunk.Vector`、`common.LType` 等一致，便于与 plan 执行器、算子统一。
- 暂不考虑与 pkg/storage、pkg/catalog 的对接；与 pkg/chunk 的对接通过“列存使用 pkg/chunk”完成。

---

## 四、Storage2 包结构建议（pkg/storage2）

```
pkg/storage2/
├── proto/           # 从 Lance 引入的 proto 定义及生成的 Go 代码（table.proto、transaction.proto 等）
├── manifest.go      # Manifest 结构体与序列化（版本、schema、fragments、config 等，基于 proto）
├── fragment.go      # Fragment、DataFile、DeletionFile（逻辑块与物理文件引用，基于 proto）
├── transaction.go   # Transaction、Operation 类型（Append/Delete/Overwrite/Update/...，基于 proto）
├── commit.go        # CommitHandler 接口与默认实现（本地 Rename、ConditionalPut 风格）
├── conflict.go      # 冲突检测与 Rebase（可选，与 Operation 对应）
├── version.go       # 版本解析与 _versions 路径约定
├── io.go            # Manifest/Fragment 的读写与 ObjectStore 抽象（可选）
└── doc.go           # 包文档与设计说明
```

序列化方案：

- **采用 Proto**，**直接使用 Lance 的 proto 定义**（如 `table.proto`、`transaction.proto` 等）。将 Lance 仓库中的 proto 文件引入 plan 项目（复制到 `pkg/storage2/proto` 或通过 submodule 引用），用 `protoc` 生成 Go 结构体；Manifest、Fragment、DataFile、Transaction 的读写与 Lance 使用同一套消息格式，便于后续与 Lance 的对比测试（格式一致可直接做字节级或反序列化后逐字段比对）。

### 4.1 数据文件格式（不采用 Arrow，使用 pkg/chunk）

Lance 的**数据文件**（.lance）内部使用 **Arrow 列存**（RecordBatch、列式编码等）。Storage2 **不采用 Arrow**，仅在**元数据层**与 Lance 对齐（Manifest/Fragment/DataFile 的 Proto）；**数据文件本体**采用列存，且**直接使用 pkg/chunk** 的类型与序列化，与 plan 的 compute、catalog 等模块统一。

**建议**：

1. **明确分工**  
   - **元数据**：Manifest、Fragment、DataFile、Transaction 继续使用 Lance 的 Proto，DataFile 只描述“路径、字段 id、大小、版本”等，**不约定**文件内部布局。  
   - **数据文件**：列存格式**使用 pkg/chunk**：类型采用 `pkg/common` 的 LType/LTypeId，读写采用 `chunk.Chunk`、`chunk.Vector` 及 chunk 的物理格式/序列化（如 `vector_serialize`、`UnifiedFormat` 等），数据文件即“持久化后的 Chunk”，无需额外转换层。

2. **数据文件格式**  
   - **列存**，且**使用 pkg/chunk**：列式布局、类型与 `pkg/chunk` / `pkg/common` 一致，写入时由 Chunk 序列化到文件，读取时反序列化为 Chunk，与执行器、算子直接对接。

3. **与对比测试的关系**  
   - 对比测试只针对**元数据与操作行为**（Manifest、Transaction、Append/Delete/Overwrite 等结果）。  
   - **数据文件内容**不参与与 Lance 的字节级对比；仅保证 DataFile 的 path/fields/size 等与 Manifest 一致。

4. **文档化**  
   - 在“与 Lance 的已知差异”中写明：**数据文件内部格式：Lance 使用 Arrow 列存，Storage2 使用 pkg/chunk 列存格式（非 Arrow）**，避免与 Lance 数据文件混用或误判为兼容。

---

## 五、开发阶段规划

### Phase 1：Manifest 与 Fragment 模型（核心数据结构）

- **目标**：在 Go 中定义并序列化 Manifest、Fragment、DataFile，与 Lance 概念对齐。
- **产出**：
  - `manifest.go`：Manifest 结构体（Version、Schema 引用、Fragments、Timestamp、Config、NextRowID、StorageFormat 等）。
  - `fragment.go`：Fragment（Id、Files、DeletionFile、PhysicalRows、RowIDMeta）、DataFile（Path、Fields、ColumnIndices、Version、Size、BaseID）。
  - 序列化格式：**Proto**，使用 Lance 的 proto 文件（如 table.proto）生成的 Go 类型。
- **验收**：可构造 Manifest、Fragment、DataFile，并完成 Proto 序列化/反序列化；与 Lance 的二进制格式兼容。

### Phase 2：Transaction 与 Operation

- **目标**：定义写事务与操作类型，支持“基于 read_version 的增量描述”。
- **产出**：
  - `transaction.go`：Transaction（ReadVersion、UUID、Operation、Tag、Properties）、Operation 枚举/和类型（Append、Delete、Overwrite、Update、CreateIndex、Rewrite、Merge、UpdateConfig 等，可先实现子集）。
  - 从 Transaction 计算“新 Manifest”的逻辑（BuildManifest）：根据 Operation 合并 fragments、递增 version、更新 next_row_id 等。
- **验收**：给定当前 Manifest + Transaction，能生成下一版本 Manifest；与 Phase 1 的 Manifest/Fragment 一致。

### Phase 3：CommitHandler 与版本路径

- **目标**：抽象“原子提交新版本”，并约定版本目录与命名。
- **产出**：
  - `commit.go`：CommitHandler 接口（ResolveLatestVersion、ResolveVersion、Commit(manifest, writer)）、默认实现（如 LocalRenameCommit：写临时文件再 Rename；或 ConditionalPut 风格）。
  - `version.go`：版本目录约定（如 `_versions/{version}.manifest`）、Manifest 命名与解析、Latest 指针（可选）。
- **验收**：能通过 CommitHandler 提交新 Manifest；能解析并加载指定版本或最新版本的 Manifest。

### Phase 4：事务文件与冲突检测（可选）

- **目标**：写事务文件、加载“read_version 之后”的已提交事务，并做简单冲突检测。
- **产出**：
  - 事务文件路径约定：`_transactions/{read_version}-{uuid}.txn`；内容为 Transaction 的 Proto 序列化（与 Lance 的 transaction.proto 一致）。
  - `conflict.go`：ConflictChecker 或 Rebase 逻辑（基于 Operation 与 modified_fragment_ids）；可先支持 Append 与 Append 兼容、Append 与 Delete 兼容等规则。
- **验收**：并发写入时能检测冲突或成功 Rebase 后重试提交。

### Phase 5：扩展与优化（后续）

- 多 BasePath、分支（branch）、Tag 等（若需要）。
- 删除向量（DeletionFile）的读写与合并。
- 与 ObjectStore 抽象对接（本地、S3、GCS 等），统一 IO 接口。
- 性能：Manifest 缓存、fragment 偏移索引（类似 Lance 的 fragment_offsets）以加速按范围查找。

**Phase 5 已实现**：`fragment_offsets.go`（ComputeFragmentOffsets、FragmentsByOffsetRange）；BuildManifest Overwrite 支持 InitialBases → next.BasePaths，Commit 时保留 Transaction.Tag → next.Tag；`fragment.go` 中 NewBasePath、NewDeletionFile、NewDataFragmentWithRows；`io.go` 中 ObjectStore 接口与 LocalObjectStore 实现；单测覆盖上述逻辑及 LocalObjectStore。

### Phase 6：数据文件格式（使用 pkg/chunk）

- **目标**：实现列存数据文件读写，**使用 pkg/chunk** 的类型与序列化（Chunk/Vector、LType、chunk 的物理格式或序列化接口），数据文件即持久化的 Chunk。
- **产出**：数据文件读写层依赖 `pkg/chunk`、`pkg/common`；Writer 接收 `chunk.Chunk` 或按列写入时使用 chunk 的序列化；Reader 返回 `chunk.Chunk` 或可填充 Chunk 的列数据；类型与 `common.LTypeId` 一致，无需自建 ColumnTypeID。

**Phase 6 当前实现**：数据文件仅使用 pkg/chunk。`data_chunk.go` 提供 `WriteChunkToFile`、`ReadChunkFromFile`，基于 `chunk.Chunk` 的序列化/反序列化（util.Serialize/Deserialize），无独立 S2DF 格式。

---

## 六、开发任务拆解

以下将各 Phase 拆解为可执行任务，便于排期与验收。任务编号与 Phase 对应（如 1.x 属于 Phase 1）。

### 6.1 前置：Proto 与工程骨架（Phase 0）

| 任务 ID | 任务描述 | 产出 | 依赖 |
|---------|----------|------|------|
| 0.1 | 从 Lance 仓库定位并列出所需 proto 文件（如 table.proto、transaction.proto 及依赖） | 文档：proto 文件清单及路径 | - |
| 0.2 | 在 plan 中创建 `pkg/storage2/proto`，引入 Lance 的 proto 文件（复制或 submodule） | 目录下存在与 Lance 一致的 .proto 文件 | 0.1 |
| 0.3 | 配置 protoc 与 Go 插件，编写生成脚本/Makefile，生成 Go 代码到 `pkg/storage2/proto` 或子包 | 可执行脚本；生成的 .pb.go 可编译通过 | 0.2 |
| 0.4 | 初始化 `pkg/storage2` 包骨架：doc.go、go.mod；元数据层不依赖其它 pkg，数据文件列存可依赖 pkg/chunk、pkg/common | 可 `go build ./pkg/storage2/...` | 0.3 |

### 6.2 Phase 1：Manifest 与 Fragment 模型

| 任务 ID | 任务描述 | 产出 | 依赖 |
|---------|----------|------|------|
| 1.1 | 基于生成的 Proto 定义 Go 侧 Manifest 的构造与访问 API（或直接使用 pb 类型 + 薄封装） | manifest.go：NewManifest、字段访问、与 pb 互转 | 0.4 |
| 1.2 | 实现 Manifest 的 Proto 序列化/反序列化（Marshal/Unmarshal） | 可写字节流、可从字节流还原 | 1.1 |
| 1.3 | 基于 Proto 定义 Fragment、DataFile、DeletionFile 的构造与访问 | fragment.go：Fragment/DataFile/DeletionFile 构造与 pb 互转 | 0.4 |
| 1.4 | 实现 Fragment/DataFile 的 Proto 序列化/反序列化（若为 Manifest 子结构则随 Manifest 一起） | 与 Manifest 序列化一致，可往返 | 1.2, 1.3 |
| 1.5 | 编写 Phase 1 单测：构造最小 Manifest、单 Fragment、DataFile，序列化后反序列化断言一致；可选：与 Lance 生成的样本字节比对 | 单测通过；可选 golden 文件 | 1.4 |

### 6.3 Phase 2：Transaction 与 Operation

| 任务 ID | 任务描述 | 产出 | 依赖 |
|---------|----------|------|------|
| 2.1 | 基于 transaction.proto 定义 Transaction 及各 Operation 类型的构造与访问 | transaction.go：Transaction、Operation 枚举/结构（Append/Delete/Overwrite/Update 等子集） | 0.4 |
| 2.2 | 实现 Transaction 的 Proto 序列化/反序列化（.txn 文件格式） | 可写/读 _transactions/*.txn | 2.1 |
| 2.3 | 实现 BuildManifest：输入当前 Manifest + Transaction，按 Operation 类型计算新 Manifest（fragments 合并、version 递增、next_row_id 等） | BuildManifest 函数；覆盖 Append、Delete、Overwrite、Update（先子集） | 1.1, 1.3, 2.1 |
| 2.4 | 为每种已实现的 Operation 编写单测：给定 Manifest + Transaction，断言结果 Manifest 的 fragments、version、next_row_id 等符合预期 | 单测通过 | 2.3 |
| 2.5 | 可选：实现 CreateIndex、Rewrite、Merge、UpdateConfig 等更多 Operation 的 BuildManifest 逻辑 | 扩展 Operation 支持 | 2.3 |

### 6.4 Phase 3：CommitHandler 与版本路径

| 任务 ID | 任务描述 | 产出 | 依赖 |
|---------|----------|------|------|
| 3.1 | 定义版本路径约定：_versions/{version}.manifest、_transactions/{read_version}-{uuid}.txn；实现 version.go（路径拼接、解析 version 号） | version.go：ManifestPath、ParseVersion、事务文件路径等 | - |
| 3.2 | 定义 CommitHandler 接口：ResolveLatestVersion、ResolveVersion、Commit(ctx, manifest, writer) 等 | commit.go：CommitHandler 接口 | - |
| 3.3 | 实现本地默认 CommitHandler（如 RenameCommit：写临时文件再 Rename；或 ConditionalPut 风格） | 至少一种 CommitHandler 实现 | 3.1, 3.2, 1.2 |
| 3.4 | 实现“解析并加载指定版本 / 最新版本”的 Manifest 读取（基于 CommitHandler + version.go） | 可从存储加载某版本或 latest | 3.1, 3.2, 1.2 |
| 3.5 | 单测：提交新 Manifest 后能通过 ResolveVersion 加载；多次提交同一 version 仅一次成功（原子性） | 单测通过 | 3.3, 3.4 |

### 6.5 Phase 4：事务文件与冲突检测（可选）

| 任务 ID | 任务描述 | 产出 | 依赖 |
|---------|----------|------|------|
| 4.1 | 实现写事务文件：在提交前将 Transaction 写入 _transactions/{read_version}-{uuid}.txn | 提交流程中写入 .txn 文件 | 2.2, 3.1 |
| 4.2 | 实现“加载 read_version 之后已提交事务”：列出 _transactions 或从各版本 Manifest 的 transaction_file 读取并排序 | 可返回 [](version, Transaction) | 2.2, 3.1, 3.4 |
| 4.3 | 实现 ConflictChecker / Rebase：根据 Lance 冲突矩阵判断当前 Transaction 与已提交事务是否冲突；可兼容时合并（Rebase）并更新 read_version | conflict.go：CheckConflict、Rebase（或等价 API） | 2.1, 4.2 |
| 4.4 | 提交流程集成：先写 .txn → 冲突检测 → 若冲突则 Rebase 重试或返回错误；若通过则 BuildManifest 并 Commit | 完整提交流程支持冲突与重试 | 4.1, 4.2, 4.3, 3.3 |
| 4.5 | 单测：成对操作 (A,B) 验证冲突/兼容与 Rebase 后结果与“先 A 后 B”一致 | 冲突与 Rebase 单测通过 | 4.3, 4.4 |

### 6.6 对比测试任务（对应第六节方案）

| 任务 ID | 任务描述 | 产出 | 依赖 |
|---------|----------|------|------|
| T.1 | 建立 testdata/：最小 Manifest、单 Fragment、典型 Transaction 的 .manifest/.txn fixture（可从 Lance 拷贝或由 Storage2 生成） | testdata/ 下 fixture 文件 | 1.4, 2.2 |
| T.2 | 格式对比：Storage2 解析 Lance 生成的 .manifest/.txn，断言关键字段一致；Storage2 生成的 .manifest/.txn 与 Lance 逻辑等价（结构或字节，视 proto 一致性） | 格式对比单测/脚本 | 1.4, 2.2, T.1 |
| T.3 | 版本与路径：单测验证写出路径为 _versions/{v}.manifest、_transactions/{rv}-{uuid}.txn | 路径约定单测 | 3.1, 4.1 |
| T.4 | 操作行为对比：Append/Delete/Overwrite/Update 各写用例，相同输入下对比结果 Manifest（fragments、version、next_row_id）与预期或与 Lance 行为描述一致 | 行为对比单测 | 2.3, 3.4, 七、对比测试方案 |
| T.5 | 冲突与 Rebase：按冲突矩阵构造 (A,B) 用例，验证冲突时返回、兼容时 Rebase 后结果正确 | 冲突/Rebase 对比单测 | 4.3, 4.4 |
| T.6 | 差异文档：在 doc.go 或本文档中列出与 Lance 的已知差异（若有） | 已知差异说明 | 全 Phase |

**对比测试已实现**：testdata/ 与 TestGenerateFixtures；comparison_test.go 中 T.2 格式对比（LoadManifestFixture、LoadTransactionFixture、ManifestRoundTripViaFixture）、T.3 路径约定（TestPathConvention）、T.4 操作行为（TestOperationBehaviorAppend/Overwrite/Delete）、T.5 冲突与 Rebase（TestConflictAppendAppendCompatible、TestConflictAppendOverwriteConflict、TestRebaseResultMatchesOrder）；doc.go 中已知差异说明。

### 6.7 任务依赖关系简图

```
0.1 → 0.2 → 0.3 → 0.4
         ↓
1.1 → 1.2   1.3 → 1.4 → 1.5
  \    /      \    /
   \  /        \  /
    ↓           ↓
2.1 → 2.2   2.3 ← 2.4   2.5(可选)
  \    |     |
   \   |     ↓
    \  |   3.1 → 3.2 → 3.3 → 3.4 → 3.5
     \ |     ↑
      \|    /
       ↓   /
4.1 → 4.2 → 4.3 → 4.4 → 4.5(可选)
              ↑
T.1 → T.2   T.3  T.4   T.5   T.6

6.x Phase 6（数据文件）：6.1 格式定义 → 6.2 Writer → 6.3 Reader → 6.4 单测
```

### 6.8 Phase 6：数据文件格式（使用 pkg/chunk）

| 任务 ID | 任务描述 | 产出 | 依赖 |
|---------|----------|------|------|
| 6.1 | 列存格式使用 pkg/chunk：类型 common.LType；文件为 chunk 序列化（util.Serialize/Deserialize） | 数据文件即持久化 Chunk，无独立格式定义 | pkg/chunk, pkg/common, pkg/util |
| 6.2 | 实现 WriteChunkToFile：将 chunk.Chunk 序列化写至文件 | data_chunk.go | 6.1 |
| 6.3 | 实现 ReadChunkFromFile：从文件反序列化为 chunk.Chunk | data_chunk.go | 6.1 |
| 6.4 | 单测：Chunk 写入后读回校验、空 Chunk、缺失文件 | data_chunk_test.go | 6.2, 6.3 |

---

## 七、与 Lance 的对比测试方案

在保持 Storage2 独立实现的前提下，通过**格式对比**与**操作行为对比**验证与 Lance 设计的一致性，便于回归与兼容性评估。

### 7.1 格式对比（Format）

| 对比项 | 内容 | 方法 |
|--------|------|------|
| **Manifest 结构** | 字段语义一一对应：version、schema、fragments、timestamp、config、next_row_id、transaction_file、data_storage_format 等。 | Storage2 与 Lance **共用同一 Proto 定义**，同一逻辑内容序列化后可直接**字节级比对**或反序列化后逐字段比对。 |
| **Fragment / DataFile** | id、files 列表、deletion_file、physical_rows、row_id_meta；DataFile 的 path、fields、column_indices、version、size、base_id。 | 同 Proto，构造等价 Fragment/DataFile 后可直接比较序列化结果或反序列化结构。 |
| **Transaction / Operation** | read_version、uuid、operation 类型及 payload（如 Append 的 fragments、Delete 的 updated_fragment_ids 等）。 | 同 Proto，同一操作生成的 Transaction 可与 Lance 写出的事务文件做字节级或结构对比。 |
| **版本与路径约定** | `_versions/{version}.manifest`、`_transactions/{read_version}-{uuid}.txn`。 | 测试用例约定相同目录与命名规则，验证 Storage2 写出路径与 Lance 约定一致（或文档明确差异）。 |
| **序列化一致性** | Lance 与 Storage2 均使用同一套 Proto。 | 对比测试可直接用 Lance 生成的文件由 Storage2 解析，或 Storage2 生成的文件由 Lance 解析，验证双向兼容。 |

**落地方式**：因采用 Lance 的 Proto，可在 plan 侧维护**测试 fixture**（最小 Manifest、单 Fragment、典型 Transaction 的 `.manifest` / `.txn` 文件），部分可直接从 Lance 仓库或 Lance 运行结果拷贝；Go 测试中解析后与 Storage2 生成结果做**字节级或结构比对**，无需额外映射层。

### 7.2 操作行为对比（Operation Behavior）

| 对比项 | 预期一致的行为 | 验证方式 |
|--------|----------------|----------|
| **Append** | 新 fragments 追加到当前 fragment 列表；version 递增；next_row_id 按 Lance 规则更新（若实现）。 | 相同初始 Manifest + 相同 Append 操作 → 比较结果 Manifest 的 fragments 列表、version、next_row_id。 |
| **Delete** | 指定 fragments 被标记删除或替换为 updated_fragments；deleted_fragment_ids 正确；version 递增。 | 相同初始状态 + 相同删除条件 → 比较结果 Manifest 的 fragments、deletion 信息。 |
| **Overwrite** | 全量替换为新 schema + 新 fragments；version 可重置或按策略递增。 | 相同输入 → 比较结果 Manifest 的 schema、fragments、version。 |
| **Update** | 按 Lance 的 Update 语义：removed_fragment_ids、updated_fragments、new_fragments、fields_modified 等一致。 | 相同初始状态 + 相同更新内容 → 比较结果 Manifest。 |
| **Commit 原子性** | 新版本仅写入一次；并发提交时仅一方成功或按 CommitHandler 语义。 | 单机测试：多次 Commit 同一 version 仅一次成功；并发测试：两写同一 read_version，结果符合冲突/重试预期。 |
| **冲突与 Rebase** | 与 Lance 的冲突矩阵一致：如 Append 与 Append 兼容、Append 与 Overwrite 冲突等；可兼容时 Rebase 后结果与“顺序执行”语义一致。 | 构造冲突表驱动的用例：成对操作 (A, B)，验证是否冲突、Rebase 后提交结果是否与“先 A 后 B”一致。 |

**冲突矩阵参考**（与 Lance 保持一致）：  
Append↔Append 兼容；Append↔Delete 兼容；Append↔Overwrite 冲突；Delete/Update 与影响同一 fragment 的其它 Delete/Update 冲突；Overwrite 与其它写冲突等（详见 Lance `TRANSACTION_ARCHITECTURE.md` 冲突表）。

### 7.3 测试实施建议

- **Fixture 与 Golden**：在 `pkg/storage2` 或独立测试目录下提供 `testdata/`，存放最小 Manifest/Fragment/Transaction 的 **Proto 二进制快照**（.manifest、.txn），可直接使用 Lance 生成的文件或由 Storage2 生成后与 Lance 交叉验证，用于回归。
- **单测**：每个 Phase 的验收测试中，对核心结构（Manifest、Fragment、Transaction）增加“与 Lance 语义对齐”的断言（字段存在性、取值范围、关系不变量）。
- **行为测试**：实现 CommitHandler 与 BuildManifest 后，按 6.2 表格逐项写用例：给定输入状态 + 操作，对比输出 Manifest（或导出结构）与预期。
- **差异文档**：若某处有意与 Lance 不同（如字段省略、命名、路径），在本文档或 `doc.go` 中单独列出“与 Lance 的已知差异”，避免误判为缺陷。必列项：**数据文件内部格式**（Lance 使用 Arrow 列存，Storage2 使用 pkg/chunk 列存格式，见 4.1）。

### 7.4 可近期补齐的能力（建议优先级）

> 目标：把 Lance（Java/Rust）中“高频核心能力”的测试缺口，收敛到 Storage2 可实现的最小闭环；优先补齐不需要大规模重构的数据读取路径与边界用例。

> 特别说明：以下能力**暂不纳入当前规划**，在 `one2one-testcases.md` 中继续标记为“暂不实现”：  
> - Schema 演进与列级操作（`schema_evolution.rs`、列增删改等）  
> - 依赖 DynamoDB / 外部 manifest 的提交路径（如 Rust `dynamodb.rs` / `external_manifest.rs`）  
> - KNN/向量检索高阶实现（Rust `io/exec/knn.rs` 等）  
> - 独立 Lance encoding 层（`lance-encoding` crate，全套物理/逻辑编码与压缩）  
> - Lance-DataFusion 集成（SQL / DataFusion 互操作相关模块）  
> 这些能力涉及较大范围的架构与依赖引入，将在 Storage2 完成核心功能与基础 API 后再单独规划。

#### P0 / P1：现在就能加（改动范围可控）

- **变长类型 / Blob 边界测试（仅测试，先不改 API）**
  - **对应 Lance**：`DatasetTest` 中的 blob/range/integrity 类测试；Rust 侧 `dataset/blob.rs` 等。
  - **落地方式**：在 `pkg/storage2/data_chunk_test.go` 增加用例，覆盖：
    - 0 长度 bytes/string、非常大 payload（接近单 chunk 上限）、混合长度、包含 NULL
    - 顺序完整性（写入后逐行逐列读回比对）
  - **收益**：不引入新接口即可快速提升文件读写稳健性。

- **最小 Scanner（Chunk 扫描，先不做 filter/pushdown）**
  - **对应 Lance**：Rust `dataset_scanner` / `scan`，Java `Scanner` 概念。
  - **落地方式（建议最小接口）**：
    - 遍历 `Manifest.Fragments` → 遍历每个 `DataFile.Path` → `ReadChunkFromFile` → 返回 `[]*chunk.Chunk` 或迭代器/chan
  - **测试建议**：新增 `scanner_test.go`（或 `sdk/scanner_test.go`），验证：
    - 多 fragment / 多 datafile 扫描顺序一致
    - 空数据集扫描结果为空

- **最小 Take（按全局行号随机访问）**
  - **对应 Lance**：Java `testTake`、Rust `dataset/take.rs`。
  - **落地方式**：
    - 使用 `ComputeFragmentOffsets` / `FragmentsByOffsetRange` 将全局 row index 映射到 fragment
    - 读取对应 chunk 后在列向量上 `GetValue` 拼装结果（先支持基础类型）
  - **测试建议**：新增 `take_test.go`，覆盖：
    - 跨 fragment 的 indices
    - 重复 indices、乱序 indices

#### P2：可以做但需要先明确语义（避免过早设计失误）

- **Config 更新（UpdateConfig / DeleteConfigKeys）**
  - **对应 Lance**：Java `testUpdateConfig` / `testDeleteConfigKeys`。
  - **当前进度**：在 `BuildManifest` 中支持 `Transaction_UpdateConfig`，使用 deprecated 字段 `upsert_values` / `delete_keys` 更新 Manifest.Config；`config_test.go` 覆盖增删改行为。
  - **后续拓展**：可逐步接入新的 `config_updates` / `table_metadata_updates` / `schema_metadata_updates` / `field_metadata_updates`，并在测试中对齐 Lance 的更完整语义。

- **Tags（命名版本）增强**
  - **对应 Lance**：Java `testTags`、Rust refs/版本引用。
  - **当前进度**：Storage2 已实现基于 Manifest.Tag 的 tag 列举与解析：  
    - `ListTags(ctx, basePath, handler)`：扫描 0..latest 版本的 Manifest，返回 tag → 版本列表。  
    - `ResolveTagVersion(ctx, basePath, handler, tag)`：返回给定 tag 对应的最新版本（若不存在则返回 `(0,false)`）。  
    - 对应单测：`tags_test.go: TestListTagsAndResolveTagVersion`。
  - **后续拓展**：可在 SDK 层增加基于 tag 打开版本的辅助方法（如 `OpenDatasetWithTag`），并在 `one2one-testcases.md` 中对齐更多 Tag 相关用例。

---

## 八、依赖与约束

- **语言与仓库**：Go，位于 `plan` 仓库 `pkg/storage2`。
- **依赖**：**元数据层**：除 Go 标准库外，序列化采用 **Proto**（Lance 的 `table.proto`、`transaction.proto` 等），不依赖 `pkg/storage`、`pkg/compute` 等。**数据文件列存**：**使用 pkg/chunk**（及 pkg/common），类型与序列化与 chunk 一致。
- **与 Lance 的差异**：Lance 为 Arrow 列式与 Rust 生态。Storage2 **元数据**与 Lance 使用同一套 Proto；**数据文件**不采用 Arrow，**使用 pkg/chunk 列存格式**（见 4.1），与 plan 执行器、算子统一。

---

## 九、文档与参考

- Lance 事务框架：`/path/to/lance/TRANSACTION_ARCHITECTURE.md`
- Lance 文件写入层次：`/path/to/lance/FILEWRITER_ARCHITECTURE.md`
- Lance 代码：`rust/lance-table/src/format/`（manifest、fragment、transaction）、`rust/lance/src/dataset/transaction.rs`、`rust/lance/src/io/commit.rs`

---

*文档版本：1.2*  
*序列化采用 Proto，直接使用 Lance 的 proto 文件，便于与 Lance 的对比测试。*
