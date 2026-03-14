# STORAGE2 后续开发计划（扩展阶段）

基于 `pkg/storage2/one2one-testcases.md` 中标记为「暂不实现」的功能，制定后续开发计划。

---

## 一、总体目标与阶段划分

### 目标
在现有 P1/P2/P3 任务全部完成后的基础上，进一步扩展 Storage2 功能，逐步对齐 Lance 的完整能力。

### 阶段划分
- **Phase 1**：Schema 基础（3-4周）
- **Phase 2**：Compaction 数据治理（2-3周）  
- **Phase 3**：索引查询性能（4-6周）
- **Phase 4**：高级功能（持续迭代）

---

## 二、Phase 1: Schema 基础 API 实现

### 1.1 Schema 读取与暴露

| 编号 | 优先级 | 状态 | 任务描述 |
|------|--------|------|----------|
| SCH1 | P1 | ✅ completed | **GetSchema API**：在 SDK 层暴露 `Dataset.Schema()` 方法，返回当前版本的 schema 结构；对应 Java `testGetSchemaWithClosedDataset` 和 `testGetLanceSchema`。 |
| SCH2 | P1 | ✅ completed | **Schema 元数据读取**：实现 `GetSchemaMetadata()` 方法，支持读取 schema 级别的元数据；对应 Java `testReplaceSchemaMetadata`。 |
| SCH3 | P1 | ✅ completed | **字段配置读取**：实现 `GetFieldConfig()` 方法，支持读取字段级别的配置；对应 Java `testReplaceFieldConfig`。 |

### 1.2 Schema 验证与兼容性

| 编号 | 优先级 | 状态 | 任务描述 |
|------|--------|------|----------|
| SCH4 | P2 | ✅ completed | **Schema 兼容性检查**：实现 schema 版本间的兼容性检查工具，确保读写兼容；为后续 schema 演进做准备。 |
| SCH5 | P2 | ✅ completed | **字段类型验证**：实现字段类型的验证逻辑，防止不兼容的类型转换。 |

---

## 三、Phase 2: Compaction 数据治理

### 2.1 基础 Compaction 实现

| 编号 | 优先级 | 状态 | 任务描述 |
|------|--------|------|----------|
| CMP1 | P1 | ✅ completed | **基础 Compaction**：实现 `Compact()` 方法，支持合并小 fragment、清理删除数据；对应 Java `testCompact`。 |
| CMP2 | P1 | ✅ completed | **带删除的 Compaction**：支持在 Compaction 过程中清理已删除的行；对应 Java `testCompactWithDeletions`。 |
| CMP3 | P1 | ✅ completed | **参数化 Compaction**：支持 max_bytes、batch_size 等参数控制；对应 Java `testCompactWithMaxBytesAndBatchSize`。 |

### 2.2 高级 Compaction 策略

| 编号 | 优先级 | 状态 | 任务描述 |
|------|--------|------|----------|
| CMP4 | P2 | ✅ completed | **多次 Compaction**：支持连续多次 Compaction 操作；对应 Java `testMultipleCompactions`。 |
| CMP5 | P2 | ✅ completed | **完整 Compaction 选项**：实现所有 Compaction 选项的支持；对应 Java `testCompactWithAllOptions`。 |

---

## 四、Phase 3: 索引查询性能优化

### 3.1 标量索引实现

| 编号 | 优先级 | 状态 | 任务描述 |
|------|--------|------|----------|
| IDX2 | P1 | ✅ completed | **B-tree 标量索引**：实现基础的 B-tree 索引，支持等值查询和范围查询；对应 Lance `index/scalar`。 |
| IDX3 | P1 | ✅ completed | **标量索引统计**：实现索引统计信息收集；对应 Java `testIndexStatistics`。 |

### 3.2 向量索引实现

| 编号 | 优先级 | 状态 | 任务描述 |
|------|--------|------|----------|
| IDX4 | P1 | ✅ completed | **IVF 向量索引**：实现 IVF（Inverted File）向量索引；对应 Lance `index/vector`。 |
| IDX5 | P1 | ✅ completed | **HNSW 向量索引**：实现 HNSW（Hierarchical Navigable Small World）向量索引。 |
| IDX6 | P1 | ✅ completed | **向量索引优化**：实现索引优化和重建功能；对应 Java `testOptimizingIndices`。 |

### 3.3 索引管理

| 编号 | 优先级 | 状态 | 任务描述 |
|------|--------|------|----------|
| IDX7 | P2 | ✅ completed | **索引描述**：实现索引描述功能，支持按名称查询索引信息；对应 Java `testDescribeIndicesByName`。 |

---

## 五、Phase 4: 高级功能扩展

### 4.1 Schema 演进

| 编号 | 优先级 | 状态 | 任务描述 |
|------|--------|------|----------|
| SE1 | P2 | ✅ completed | **列删除**：实现 `DropColumns()` 方法；对应 Java `testDropColumns`。 |
| SE2 | P2 | ✅ completed | **列修改**：实现 `AlterColumns()` 方法；对应 Java `testAlterColumns`。 |
| SE3 | P2 | ✅ completed | **列新增**：实现多种方式的列新增（SQL 表达式、流式、字段定义等）；对应 Java `testAddColumnBySqlExpressions` / `testAddColumnsByStream` / `testAddColumnByFieldsOrSchema`。 |
| SE4 | P2 | ✅ completed | **路径删除**：实现 `DropPath()` 方法；对应 Java `testDropPath`。 |

### 4.2 数据集克隆

| 编号 | 优先级 | 状态 | 任务描述 |
|------|--------|------|----------|
| CLN1 | P2 | ✅ completed | **浅克隆**：实现 `ShallowClone()` 方法；对应 Java `testShallowClone`。 |

### 4.3 版本管理与分支

| 编号 | 优先级 | 状态 | 任务描述 |
|------|--------|------|----------|
| RST1 | P2 | ✅ completed | **版本恢复**：实现 `Restore()` 方法，支持将数据集恢复到指定版本；对应 Java `testDatasetRestore`。 |
| BRH1 | P2 | ✅ completed | **分支管理**：实现完整的分支 CRUD 操作（Create/Get/List/Delete）；对应 Java `testBranches`。 |
| TAG1 | P2 | ✅ completed | **Tag 打开数据集**：实现 `OpenDatasetWithTag()` 方法，支持按 tag 名称打开数据集。 |

### 4.4 数据统计与查询增强

| 编号 | 优先级 | 状态 | 任务描述 |
|------|--------|------|----------|
| DSZ1 | P2 | ✅ completed | **数据大小统计**：实现 `DataSize()` 方法，基于 Manifest 元数据计算数据大小；对应 Java `testCalculateDataSize`。 |
| SCN1 | P1 | ✅ completed | **Scanner 过滤**：实现 `Scanner.WithFilter()` 方法，支持基础过滤表达式；对应 Rust scanner filter。 |
| SCN2 | P1 | ✅ completed | **Scanner 列投影**：实现 `Scanner.WithColumns()` 方法，支持列选择；对应 Rust scanner projection。 |
| TAK1 | P2 | ✅ completed | **Take 列投影**：实现 `TakeProjected()` 方法，支持随机访问时指定列；对应 Rust take projection。 |

### 4.5 URI 与对象存储

| 编号 | 优先级 | 状态 | 任务描述 |
|------|--------|------|----------|
| URI1 | P3 | planned | **Dataset URI 支持**：支持 s3://, gs:// 等对象存储 URI；对应 Java `testDatasetUri`。 |

### 4.6 Detached Transaction

| 编号 | 优先级 | 状态 | 任务描述 |
|------|--------|------|----------|
| DT1 | P3 | planned | **Detached Transaction**：实现分离式事务提交模型；对应 Java `testCommitTransactionDetachedTrue`。 |

### 4.7 编解码与压缩

| 编号 | 优先级 | 状态 | 任务描述 |
|------|--------|------|----------|
| ENC1 | P3 | planned | **Lance 编码格式**：实现 Lance 的编码格式支持；对应 `rust/lance-encoding`。 |
| CMP6 | P3 | planned | **数据压缩**：实现数据压缩算法支持。 |

### 4.8 DataFusion 集成

| 编号 | 优先级 | 状态 | 任务描述 |
|------|--------|------|----------|
| DF1 | P3 | planned | **SQL 查询支持**：集成 DataFusion，支持 SQL 查询；对应 `rust/lance-datafusion`。 |

---

## 六、实施建议

### 6.1 开发节奏
- 每个 Phase 预计 2-6 周完成
- 采用敏捷开发模式，每 1-2 周发布小版本
- 每个任务完成后立即补充对应测试

### 6.2 测试策略
- 每个新功能必须有对应的单元测试
- 与 Lance 对齐的集成测试
- 性能基准测试（特别是索引和 Compaction）

### 6.3 兼容性保证
- 保持向前兼容
- 提供迁移工具
- 渐进式功能启用

---

*文档版本：0.3（扩展阶段规划 - 更新已完成项）*
*更新时间：2026-03-14*