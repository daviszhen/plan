# STORAGE2 开发计划 Phase 5：高价值功能扩展

本文档规划 Storage2 下一阶段的高价值功能开发，基于对 `one2one-testcases.md` 的分析，聚焦于对实际生产使用影响最大的功能。

---

## 一、功能价值评估

### 高价值（生产必备/高频使用）

| 功能 | 价值分析 | 当前状态 |
|------|----------|----------|
| **S3/对象存储支持** | 生产环境几乎必备，当前只支持本地路径，严重限制部署场景 | 未实现 |
| **带谓词的 CountRows** | 查询场景基础统计需求，当前只支持全表 count | 未实现 |
| **Scanner 复合 Filter** | 当前只支持单列整数比较，实用性受限 | 部分实现 |

### 中价值（特定场景需要）

| 功能 | 价值分析 | 当前状态 |
|------|----------|----------|
| **Detached Transaction** | 高并发写入场景需要 | 未实现 |
| **Lance 编码格式** | 与 Lance 生态互操作需要 | 未实现 |
| **KNN 向量搜索** | AI/向量数据库场景核心能力 | 未实现 |

---

## 二、Phase 5.1：对象存储支持

### 2.1 设计目标

- 支持 `s3://`, `gs://`, `az://` 等 URI 格式
- 与现有 `CommitHandler` 接口兼容
- 支持认证配置（静态凭证、IAM 角色等）

### 2.2 任务分解

| 编号 | 优先级 | 依赖 | 任务描述 |
|------|--------|------|----------|
| OBJ1 | P0 | - | **URI 解析器**：实现 `ParseURI(uri string)` 函数，解析协议、bucket、path 等 |
| OBJ2 | P0 | OBJ1 | **ObjectStore 接口抽象**：定义统一的 `ObjectStore` 接口，封装读写/列举操作 |
| OBJ3 | P0 | OBJ2 | **LocalObjectStore 重构**：将现有本地存储实现适配到新接口 |
| OBJ4 | P1 | OBJ2 | **S3ObjectStore 实现**：基于 AWS SDK 实现 S3 存储，支持标准/兼容 S3 的服务 |
| OBJ5 | P2 | OBJ2 | **GSObjectStore 实现**：基于 GCS SDK 实现谷歌云存储 |
| OBJ6 | P2 | OBJ2 | **AZObjectStore 实现**：基于 Azure SDK 实现 Azure Blob 存储 |
| OBJ7 | P1 | OBJ4 | **S3 CommitHandler**：实现 S3 兼容的提交协议，处理一致性问题 |
| OBJ8 | P1 | OBJ7 | **S3 认证配置**：支持静态凭证、IAM 角色、临时凭证等 |
| OBJ9 | P2 | OBJ4-6 | **SDK 层 URI 支持**：`OpenDataset("s3://bucket/path")` 自动识别协议 |

### 2.3 接口设计草案

```go
// ObjectStore 统一对象存储接口
type ObjectStore interface {
    // 基础读写
    Get(ctx context.Context, path string) ([]byte, error)
    Put(ctx context.Context, path string, data []byte) error
    Delete(ctx context.Context, path string) error
    
    // 目录操作
    List(ctx context.Context, prefix string) ([]string, error)
    Exists(ctx context.Context, path string) (bool, error)
    
    // 元数据
    Size(ctx context.Context, path string) (int64, error)
}

// URI 解析结果
type ParsedURI struct {
    Scheme   string // "file", "s3", "gs", "az"
    Bucket   string // bucket name (empty for local)
    Path     string // object path
    Region   string // region (optional)
    Endpoint string // custom endpoint (optional)
}

// 存储配置
type StorageConfig struct {
    URI         ParsedURI
    Credentials interface{} // scheme-specific credentials
}
```

### 2.4 S3 提交协议考虑

S3 不支持原子重命名，需要采用以下策略之一：

1. **乐观锁 + 版本号**：写入临时文件，通过版本号检测冲突
2. **外部协调服务**：使用 DynamoDB 作为锁服务（Lance 默认方案）
3. **简单重试**：检测冲突后重试（适用于低并发场景）

---

## 三、Phase 5.2：查询能力增强

### 3.1 带谓词的 CountRows

| 编号 | 优先级 | 任务描述 |
|------|--------|----------|
| CNT1 | P1 | **CountRows(predicate string)**：SDK 层支持带过滤条件的行计数 |
| CNT2 | P1 | **谓词下推优化**：利用 fragment 级统计信息快速估算 |
| CNT3 | P2 | **索引加速计数**：利用 B-tree 索引加速等值条件计数 |

### 3.2 Scanner 复合 Filter 增强

当前 Scanner filter 只支持 `"cN op int"` 格式的单列整数比较，需要增强：

| 编号 | 优先级 | 任务描述 |
|------|--------|----------|
| FLT1 | P1 | **AND/OR/NOT 组合**：支持 `"c0 > 10 AND c1 < 100"` 等复合表达式 |
| FLT2 | P1 | **字符串比较**：支持 `"name = 'foo'"` 和 `"name LIKE 'bar%'"` |
| FLT3 | P1 | **NULL 判断**：支持 `"c0 IS NULL"` 和 `"c1 IS NOT NULL"` |
| FLT4 | P2 | **IN 表达式**：支持 `"id IN (1, 2, 3)"` |
| FLT5 | P2 | **索引加速过滤**：利用 B-tree 索引加速等值和范围过滤 |
| FLT6 | P3 | **表达式解析器**：实现完整的 SQL-like 表达式解析（可复用现有 pushdown） |

### 3.3 接口设计草案

```go
// 增强 Scanner 接口
type ScannerBuilder struct {
    // ... existing fields ...
    filter string // 支持复合表达式
}

// CountRows 增强
func (d *datasetImpl) CountRowsWithFilter(predicate string) (uint64, error)

// 示例用法
count, _ := ds.CountRowsWithFilter("age > 18 AND status = 'active'")

scanner := ds.Scanner().
    WithFilter("name LIKE 'A%' AND created_at > '2024-01-01'").
    WithColumns("id", "name", "email").
    Build()
```

---

## 四、Phase 5.3：Detached Transaction

### 4.1 背景

当前事务模型要求写入者保持连接状态直到提交完成。Detached Transaction 允许：
- 创建事务后断开连接
- 稍后检查事务状态
- 支持长时间运行的写入操作

### 4.2 任务分解

| 编号 | 优先级 | 任务描述 |
|------|--------|----------|
| DTX1 | P1 | **事务状态存储**：设计事务状态持久化格式 |
| DTX2 | P1 | **CreateDetached API**：创建分离式事务，返回事务 ID |
| DTX3 | P1 | **CommitDetached API**：通过事务 ID 提交 |
| DTX4 | P1 | **GetTransactionStatus API**：查询事务状态 |
| DTX5 | P2 | **事务超时与清理**：自动清理过期未提交事务 |

---

## 五、Phase 5.4：Lance 编码格式（可选）

### 5.1 背景

如需与 Lance Python/Rust 生态互通数据，需要实现 Lance 文件格式读写。

### 5.2 任务分解

| 编号 | 优先级 | 任务描述 |
|------|--------|----------|
| ENC1 | P2 | **Lance 文件格式研究**：分析 Lance Arrow 文件格式规范 |
| ENC2 | P2 | **Lance 文件写入器**：实现 Chunk -> Lance 文件转换 |
| ENC3 | P2 | **Lance 文件读取器**：实现 Lance 文件 -> Chunk 转换 |
| ENC4 | P3 | **Schema 映射**：处理 Storage2 Schema 与 Lance Schema 差异 |

---

## 六、Phase 5.5：KNN 向量搜索（可选）

### 6.1 背景

当前已实现 IVF/HNSW 向量索引，但执行层缺少 KNN 查询能力。

### 6.2 任务分解

| 编号 | 优先级 | 任务描述 |
|------|--------|----------|
| KNN1 | P2 | **KNN 查询接口**：定义 `SearchNearest(vector, k)` API |
| KNN2 | P2 | **IVF KNN 实现**：利用 IVF 索引实现近似最近邻搜索 |
| KNN3 | P2 | **HNSW KNN 实现**：利用 HNSW 索引实现高效最近邻搜索 |
| KNN4 | P3 | **距离函数**：支持 L2、Cosine、IP 等距离度量 |

---

## 七、实施优先级建议

### 推荐顺序

1. **Phase 5.1 对象存储支持** - 最高优先级，解锁生产部署
2. **Phase 5.2 查询能力增强** - 高优先级，提升实用价值
3. **Phase 5.3 Detached Transaction** - 中优先级，高并发场景需要
4. **Phase 5.4 Lance 编码格式** - 按需实现，取决于生态互通需求
5. **Phase 5.5 KNN 向量搜索** - 按需实现，取决于 AI 应用场景

### 里程碑规划

| 里程碑 | 包含功能 | 验收标准 |
|--------|----------|----------|
| M5.1 | OBJ1-4, OBJ7-9 | 可通过 `s3://` URI 读写数据集 |
| M5.2 | CNT1-3, FLT1-6 | Scanner 支持复合过滤，CountRows 支持谓词 |
| M5.3 | DTX1-5 | 支持 Detached Transaction 完整流程 |

---

## 八、测试策略

### 8.1 对象存储测试

- 使用 MinIO / LocalStack 进行本地 S3 兼容测试
- Mock 测试覆盖各种错误场景
- 集成测试验证端到端流程

### 8.2 查询增强测试

- 表达式解析单元测试
- 边界条件覆盖（空结果、全匹配、类型错误等）
- 性能基准测试

### 8.3 事务测试

- 并发提交冲突测试
- 超时清理测试
- 故障恢复测试

---

*文档版本：1.0*
*创建时间：2026-03-14*
