# STORAGE2 开发计划 Phase 6：测试对齐与功能完善

本文档规划 Storage2 下一阶段的开发工作，基于 `one2one-testcases.md` 分析，聚焦于测试对齐和功能完善。

---

## 一、开发背景

Phase 5 已完成：
- ✅ 对象存储支持（S3、本地）
- ✅ 查询能力增强（带谓词 CountRows、复合 Filter）
- ✅ Detached Transaction
- ✅ Lance 编码格式
- ✅ KNN 向量搜索

Phase 6 目标：
1. 对齐 Lance 测试用例，确保功能正确性
2. 完善已有功能的测试覆盖
3. 增强稳定 RowId 支持

---

## 二、Phase 6.1：Detached Transaction 测试对齐

### 2.1 背景

Phase 5.3 已实现 Detached Transaction 功能，但测试用例未与 Lance Java `DatasetTest` 完全对齐。

### 2.2 任务分解

| 编号 | 优先级 | 任务描述 |
|------|--------|----------|
| DTX-T1 | P1 | **测试 Detached Transaction 提交流程**：验证 `CreateDetached` + `CommitDetached` 完整流程 |
| DTX-T2 | P1 | **测试 Detached Transaction 状态查询**：验证 `GetDetachedStatus` 返回正确状态 |
| DTX-T3 | P1 | **测试 Detached Transaction 超时清理**：验证过期事务自动清理 |
| DTX-T4 | P2 | **测试 Detached Transaction 并发冲突**：多个 Detached Transaction 并发提交场景 |
| DTX-T5 | P2 | **对齐 Lance Java `testCommitTransactionDetachedTrue`**：确保行为一致 |

### 2.3 验收标准

- [ ] 所有 Detached Transaction 测试通过
- [ ] 测试覆盖率达到 80% 以上
- [ ] 与 Lance Java 行为对齐

---

## 三、Phase 6.2：带谓词计数测试对齐

### 3.1 背景

Phase 5.2 已实现带谓词的 `CountRows`，但缺少与 Lance `count_lance_file` 对应的测试用例。

### 3.2 任务分解

| 编号 | 优先级 | 任务描述 |
|------|--------|----------|
| CNT-T1 | P1 | **测试基础计数**：全表 `CountRows()` 正确性 |
| CNT-T2 | P1 | **测试带谓词计数**：`CountRowsWithFilter("c0 > 10")` 正确性 |
| CNT-T3 | P1 | **测试复合谓词计数**：`CountRowsWithFilter("c0 > 10 AND c1 < 100")` 正确性 |
| CNT-T4 | P2 | **测试空结果计数**：谓词无匹配时返回 0 |
| CNT-T5 | P2 | **测试统计信息加速**：利用 fragment 级统计信息快速估算 |

### 3.3 验收标准

- [ ] 所有计数测试通过
- [ ] 支持 AND/OR/NOT 复合谓词
- [ ] 与 Lance `count_lance_file` 行为对齐

---

## 四、Phase 6.3：稳定 RowId 完善

### 4.1 背景

`rowid_scanner.go` 已实现 RowId 级随机访问框架，但 RowId 序列解析未完全实现，功能受限。

### 4.2 任务分解

| 编号 | 优先级 | 任务描述 |
|------|--------|----------|
| RID1 | P1 | **完善 RowIdSequence 解析**：支持所有五种 U64Segment 格式（Range/RangeWithHoles/RangeWithBitmap/SortedArray/Array） |
| RID2 | P1 | **启用 Feature Flag 2**：在 Manifest 中正确设置 `writer_feature_flags` |
| RID3 | P1 | **测试稳定 RowId 读写**：验证写入后 RowId 稳定可读 |
| RID4 | P2 | **测试 RowId 随机访问**：验证 `TakeRows` 使用 RowId 正确访问 |
| RID5 | P2 | **对齐 Lance Java `testEnableStableRowIds`**：确保行为一致 |

### 4.3 验收标准

- [ ] 所有 RowIdSegment 格式解析正确
- [ ] Feature Flag 2 正确启用
- [ ] 稳定 RowId 读写测试通过

---

## 五、Phase 6.4：S3 提交协议增强

### 5.1 背景

Phase 5.1 已实现 S3ObjectStore，但 S3 提交协议可能需要增强以支持更强的一致性保证。

### 5.2 任务分解

| 编号 | 优先级 | 任务描述 |
|------|--------|----------|
| S3-C1 | P2 | **S3 乐观锁实现**：版本号检测冲突，写入临时文件后原子提交 |
| S3-C2 | P2 | **S3 冲突重试机制**：检测冲突后自动重试（适用于低并发场景） |
| S3-C3 | P3 | **外部协调服务接口**：预留 DynamoDB 作为锁服务的接口（暂不实现） |
| S3-C4 | P2 | **S3 提交测试**：使用 MinIO/LocalStack 进行本地 S3 兼容测试 |

### 5.3 验收标准

- [ ] S3 提交协议支持乐观锁
- [ ] 冲突检测与重试机制工作正常
- [ ] MinIO 集成测试通过

---

## 六、Phase 6.5：lance-table/lance-io 对齐

### 6.1 背景

逐步对齐 Rust `lance-table` 和 `lance-io` 实现，增强 Storage2 的兼容性。

### 6.2 任务分解

| 编号 | 优先级 | 任务描述 |
|------|--------|----------|
| TBL1 | P3 | **Table 格式扩展**：对齐 `lance-table/src/format.rs` 的表格式 |
| TBL2 | P3 | **MigrationManager 完善**：支持更多迁移类型 |
| IO1 | P3 | **ParallelReader 增强**：并行读取优化 |
| IO2 | P3 | **IOStats 统计**：IO 性能统计收集 |

### 6.3 验收标准

- [ ] Table 格式与 Lance 兼容
- [ ] IO 性能可观测

---

## 七、实施优先级

### 推荐顺序

1. **Phase 6.1 Detached Transaction 测试对齐** - P1，功能已实现，只需补充测试
2. **Phase 6.2 带谓词计数测试对齐** - P1，功能已实现，只需补充测试
3. **Phase 6.3 稳定 RowId 完善** - P1，框架已有，需完善实现
4. **Phase 6.4 S3 提交协议增强** - P2，增强生产环境可靠性
5. **Phase 6.5 lance-table/lance-io 对齐** - P3，逐步对齐

### 里程碑规划

| 里程碑 | 包含功能 | 验收标准 |
|--------|----------|----------|
| M6.1 | DTX-T1~5, CNT-T1~5 | 测试覆盖完整，与 Lance 行为对齐 |
| M6.2 | RID1~5 | 稳定 RowId 功能完整可用 |
| M6.3 | S3-C1~4 | S3 提交协议生产可用 |

---

## 八、测试策略

### 8.1 测试对齐原则

- 每个 Lance Java `DatasetTest` 方法都有对应的 Go 测试
- 每个 Rust `lance-file` 测试场景都有对应的 Storage2 测试
- 测试命名遵循 `Test<功能>_<场景>` 格式

### 8.2 测试覆盖要求

- 单元测试：核心逻辑覆盖率 > 80%
- 集成测试：端到端场景全覆盖
- 边界测试：空值、边界值、错误路径

---

*文档版本：1.0*
*创建时间：2026-03-14*
