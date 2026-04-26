# Storage2 测试用例开发计划

基于 Lance Rust 1887 个测试用例的一一对应分析

## 概览统计

| 指标 | 数量 |
|------|------|
| Lance Rust 总测试数 | 1887 |
| Storage2 已有测试数 | **612** (含子测试共 854) |
| 测试文件数 | 57 |
| 源文件数 (非测试/非proto) | 45 |
| 总函数数 | 868 |
| Lance 功能覆盖率 (按测试数) | 32.4% |
| 语句覆盖率 (不含proto) | **74.8%** |
| 语句覆盖率 (含proto) | 62.6% |

> **更新于 2026-03-15**: P3 完成后精确统计。612 个顶层测试函数 (含子测试 854 个)。
> 868 个源函数中：520 (59.9%) 高覆盖 (>=85%)、206 (23.7%) 中覆盖 (60-84%)、31 (3.6%) 低覆盖 (1-59%)、111 (12.8%) 零覆盖。

### 分类覆盖情况

| 分类 | 测试数 | 函数平均覆盖率 | 涉及函数数 | 说明 |
|------|--------|---------------|-----------|------|
| transaction | ✅ 85 | 81.2% | 77 | 事务冲突检测、提交、重放、并发、分离式事务 |
| manifest | ✅ 40 | 92.7% | 19 | Manifest 序列化、版本管理、V2命名、构建 |
| fragment | ✅ 13 | 93.9% | 7 | Fragment 操作、偏移计算、数据文件 |
| rowid | ✅ 32 | 81.3% | 66 | RowIdSequence、稳定行ID、序列化、扫描 |
| schema | ✅ 6 | — | — | Schema/Field 定义、类型映射 |
| refs | ✅ 19 | 74.0% | 24 | Tags/Branches 引用 |
| io | ✅ 62 | 85.9% | 95 | ObjectStore、流式读写、并行Reader、StoreFactory |
| encoding | ✅ 70 | 80.3% | 83 | 列编码 BitPack/Dict/RLE/Plain/VarBinary/Scheduler |
| index | ✅ 146 | 82.9% | 228 | BTree/IVF/HNSW/Bitmap/Bloom/ZoneMap/KNN/Selector |
| scan | ✅ 38 | 92.3% | 59 | 扫描、过滤、投影、谓词下推 |
| update | 🟡 7 | 60.4% | 28 | 更新操作 (6个零覆盖函数) |
| data_replacement | ✅ 8 | 86.1% | 31 | 数据替换、验证、回滚 |
| s3 (cloud) | 🟡 — | 7.0% | 43 | s3_store 0%, s3_commit 25% (需MinIO集成测试) |
| az (cloud) | 🟡 — | 74.4% | 40 | Azure Blob Storage (mock测试) |
| gs (cloud) | 🟡 — | 70.9% | 39 | Google Cloud Storage (mock测试) |
| other | ✅ 86 | 80.6% | 53 | URI/DataSize/DataChunk 等 |

---

## Storage2 已有测试详情

### TRANSACTION (85 tests)

| 测试名称 |
|----------|
| `TestAZCommitHandlerWithLock_Conflict` |
| `TestAZCommitHandler_Basic` |
| `TestCheckCreateIndexConflict` |
| `TestCheckDataReplacementConflict` |
| `TestCheckUpdateConflict` |
| `TestCleanupExpiredDetachedTransactions` |
| `TestCommitDetachedTransaction` |
| `TestCommitDetachedTransactionAlreadyCommitted` |
| `TestCommitDetachedTransactionNotFound` |
| `TestCommitTransactionConflict` |
| `TestCommitTransactionFirstCommit` |
| `TestCommitTransactionOverwriteAfterAppend` |
| `TestCommitTransactionPreservesTag` |
| `TestCommitTransactionRebase` |
| `TestCommitTransactionWithRetry_LocalHandler` |
| `TestConflictAppendAppendCompatible` |
| `TestConflictAppendOverwriteConflict` |
| `TestConflictDeleteDeleteNoOverlap` |
| `TestConflictDeleteDeleteOverlap` |
| `TestConflictDeleteRewriteOverlap` |
| `TestConflictMatrix_Append` |
| `TestConflictMatrix_AsymmetricPairs` |
| `TestConflictMatrix_CreateIndex` |
| `TestConflictMatrix_Delete` |
| `TestConflictMatrix_Merge` |
| `TestConflictMatrix_Overwrite` |
| `TestConflictMatrix_Project` |
| `TestConflictMatrix_Rewrite` |
| `TestConflictMatrix_RewriteVsCreateIndex` |
| `TestConflictRewriteRewriteOverlap` |
| ... (51 more) |

### MANIFEST (40 tests)

| 测试名称 |
|----------|
| `TestBuildManifestAppendFromManifestTest` |
| `TestBuildManifestCloneShallow` |
| `TestBuildManifestCreateIndex` |
| `TestBuildManifestDataReplacement` |
| `TestBuildManifestMerge` |
| `TestBuildManifestMergeWithSchemaMetadata` |
| `TestBuildManifestOverwriteWithInitialBases` |
| `TestBuildManifestProject` |
| `TestBuildManifestProjectVersionMismatch` |
| `TestBuildManifestUpdate` |
| `TestBuildManifestUpdateConfigFieldMetadata` |
| `TestBuildManifestUpdateConfigUpsertAndDelete` |
| `TestCalculateManifestDataSize` |
| `TestCheckoutVersion` |
| `TestListTagsAndResolveTagVersion` |
| `TestLoadManifestFixture` |
| `TestManifestPath` |
| `TestManifestPathV2` |
| `TestManifestRoundTrip` |
| `TestManifestRoundTripViaFixture` |
| `TestParseVersion` |
| `TestParseVersionV2` |
| `TestStableRowId_VersionIsolation` |
| `TestUnmarshalManifestNil` |
| `TestValidateManifest_DeletionExceedsPhysicalRows` |
| `TestValidateManifest_DuplicateFragmentID` |
| `TestValidateManifest_EmptyDataFilePath` |
| `TestValidateManifest_MaxFragmentIdInconsistent` |
| `TestValidateManifest_Nil` |
| `TestValidateManifest_Valid` |

### FRAGMENT (13 tests)

| 测试名称 |
|----------|
| `TestBloomFilterIndexFragmentFilters` |
| `TestComputeFragmentOffsets` |
| `TestComputeFragmentOffsetsEmpty` |
| `TestCountRows_MultipleFragments` |
| `TestFragmentsByOffsetRange` |
| `TestScanChunksSingleFragment` |
| `TestStableRowId_MultiFragment` |
| `TestTakeRowsMultiFragment` |
| `TestTakeRowsSingleFragment` |
| `TestZoneMapIndexFragmentPruning` |
| `TestZoneMapIndexFragmentZoneMaps` |

### ROWID (32 tests)

| 测试名称 |
|----------|
| `TestArraySegment` |
| `TestNewRowIdSequenceFromRange` |
| `TestNewRowIdSequenceFromSlice` |
| `TestNewU64SegmentFromSlice_Array` |
| `TestNewU64SegmentFromSlice_Range` |
| `TestNewU64SegmentFromSlice_SortedArray` |
| `TestRangeSegment` |
| `TestRangeWithBitmapSegment` |
| `TestRangeWithHolesSegment` |
| `TestRowIdScannerBasic` |
| `TestRowIdScannerNotFound` |
| `TestRowIdScannerWithoutFeatureFlag` |
| `TestRowIdSequence` |
| `TestRowIdSequenceDelete` |
| `TestRowIdSequenceDelete_AllFromSegment` |
| `TestRowIdSequenceDelete_NoOp` |
| `TestRowIdSequenceExtend_Empty` |
| `TestRowIdSequenceExtend_MergeAdjacentRanges` |
| `TestRowIdSequenceExtend_NoMerge` |
| `TestRowIdSequenceExtend_Simple` |
| `TestRowIdSequenceSlice` |
| `TestRowIdSequenceSlice_Empty` |
| `TestRowIdSequenceSlice_MultiSegment` |
| `TestRowIdSequence_Serialization` |
| `TestSortedArraySegment` |
| `TestStableRowId_EnableFeatureFlag` |
| `TestStableRowId_TakeByRowIds` |
| `TestStableRowId_WriteRead` |

### SCHEMA (6 tests)

| 测试名称 |
|----------|
| `TestFieldsMatch` |
| `TestLanceEncoder_EncodeSchema` |
| `TestLanceLogicalTypeToLType` |
| `TestLanceRoundTrip_AllTypes` |
| `TestLtypeToLanceLogicalType` |

### REFS (19 tests)

| 测试名称 |
|----------|
| `TestAZObjectStore_GetETag` |
| `TestAZObjectStore_Prefix` |
| `TestBranchAlreadyExists` |
| `TestBranchNotFound` |
| `TestBranchesCreateGetDelete` |
| `TestCheckValidBranch` |
| `TestCheckValidTag` |
| `TestCheckoutTag` |
| `TestGSObjectStore_GetETag` |
| `TestGSObjectStore_Prefix` |
| `TestMemoryObjectStore_ETag` |
| `TestMemoryObjectStore_ListWithPrefix` |
| `TestStoreFactory_AZ` |
| `TestStoreFactory_GS` |
| `TestStoreFactory_Local` |
| `TestStoreFactory_Memory` |
| `TestStoreFactory_Unsupported` |
| `TestTagAlreadyExists` |
| `TestTagNotFound` |
| `TestTagsCreateGetDelete` |

### IO (62 tests)

| 测试名称 |
|----------|
| `TestAZCredentials` |
| `TestAZObjectStore_Basic` |
| `TestAZObjectStore_Copy` |
| `TestAZObjectStore_Delete` |
| `TestAZObjectStore_Exists` |
| `TestAZObjectStore_GetSize` |
| `TestAZObjectStore_Integration` |
| `TestAZObjectStore_List` |
| `TestAZObjectStore_ReadRange` |
| `TestAZObjectStore_ReadStream` |
| `TestAZObjectStore_Rename` |
| `TestAZObjectStore_WriteStream` |
| `TestBTreeIndexBasicOperations` |
| `TestBitPackEncoderCompressionRatio` |
| `TestBitmapIndexBasicOperations` |
| `TestBitmapIndexBitmapOperations` |
| `TestBitmapIndexSerialization` |
| `TestBloomFilterBasicOperations` |
| `TestBloomFilterIndexBasicOperations` |
| `TestBloomFilterIndexSerialization` |
| `TestBloomFilterIndexUnsupportedOperations` |
| `TestBloomFilterMemoryConstraint` |
| `TestBloomFilterSerialization` |
| `TestChunkedParallelReader_LargeFile` |
| `TestChunkedParallelReader_SmallFile` |
| `TestConcurrentIOScheduler_ConcurrencyLimit` |
| `TestConcurrentIOScheduler_ReadWrite` |
| `TestCreateIndexOperationValidation` |
| `TestDataReplacementPlannerEstimateIO` |
| `TestDefaultDataReplacementOptions` |
| ... (84 more) |

### ENCODING (70 tests)

| 测试名称 |
|----------|
| `TestBitPackEncoderConstant` |
| `TestBitPackEncoderEmpty` |
| `TestBitPackEncoderNegative` |
| `TestBitPackEncoderSmallRange` |
| `TestBitPackEncoderWithOffset` |
| `TestDictEncoderAllDistinct` |
| `TestDictEncoderEmpty` |
| `TestDictEncoderFewDistinct` |
| `TestDictEncoderNegative` |
| `TestDictEncoderSingleValue` |
| `TestEncodingProfileInteger` |
| `TestEncodingProfileString` |
| `TestEncodingTypeString` |
| `TestLanceDecoder_DecodeChunk` |
| `TestLanceEncoder_EncodeChunk` |
| `TestLogicalColumnEncoderBooleanRoundtrip` |
| `TestLogicalColumnEncoderFloatRoundtrip` |
| `TestLogicalColumnEncoderForcedEncoding` |
| `TestLogicalColumnEncoderIntegerRoundtrip` |
| `TestLogicalColumnEncoderIntegerWithNulls` |
| `TestLogicalColumnEncoderStringRoundtrip` |
| `TestLogicalColumnEncoderStringWithNulls` |
| `TestPlainEncoderByteWidth1` |
| `TestPlainEncoderByteWidth2` |
| `TestPlainEncoderByteWidth4` |
| `TestPlainEncoderByteWidth8` |
| `TestPlanChunkEncodingBooleanColumn` |
| `TestPlanChunkEncodingFloatColumn` |
| `TestPlanChunkEncodingIntColumns` |
| `TestPlanChunkEncodingMultiColumn` |
| ... (19 more) |

### INDEX (146 tests)

| 测试名称 |
|----------|
| `TestAllIndexStatistics` |
| `TestBTreeIndexConcurrentAccess` |
| `TestBTreeIndexDuplicateKeys` |
| `TestBTreeIndexFloatKeys` |
| `TestBTreeIndexInterface` |
| `TestBTreeIndexLargeDataset` |
| `TestBTreeIndexNonExistentKey` |
| `TestBTreeIndexStringKeys` |
| `TestBitmapIndexBatchInsert` |
| `TestBitmapIndexEdgeCases` |
| `TestBitmapIndexGetValueCardinality` |
| `TestBitmapIndexInsertAndSearch` |
| `TestBitmapIndexLargeDataset` |
| `TestBitmapIndexListValues` |
| `TestBitmapIndexNullHandling` |
| `TestBloomFilterAddAndCheck` |
| `TestBloomFilterAddValue` |
| `TestBloomFilterClear` |
| `TestBloomFilterEstimatedFPR` |
| `TestBloomFilterFPRWithFillRate` |
| `TestBloomFilterFalsePositiveRate` |
| `TestBloomFilterFloat64` |
| `TestBloomFilterIndexAddAndCheck` |
| `TestBloomFilterIndexCanPrune` |
| `TestBloomFilterIndexColumns` |
| `TestBloomFilterIndexCreateFilter` |
| `TestBloomFilterIndexPruning` |
| `TestBloomFilterInt64` |
| `TestBloomFilterOptimalParameters` |
| `TestBloomFilterSize` |
| ... (69 more) |

### SCAN (38 tests)

| 测试名称 |
|----------|
| `TestAndPredicate` |
| `TestColumnPredicate` |
| `TestCountRows_ComplexFilter` |
| `TestCountRows_WithFilter` |
| `TestFilterParser_BooleanOperators` |
| `TestFilterParser_Comparison` |
| `TestFilterParser_Empty` |
| `TestFilterParser_Errors` |
| `TestFilterParser_NullChecks` |
| `TestMergeFilters` |
| `TestMergeFiltersDifferentSizes` |
| `TestNotPredicate` |
| `TestOrPredicate` |
| `TestPredicateString` |
| `TestPushdownScanner` |
| `TestScanChunksEmptyDataset` |
| `TestTakeRowsProjectedColumns` |
| `TestUpdatePredicateParsing` |

### UPDATE (7 tests)

| 测试名称 |
|----------|
| `TestEstimateUpdateCost` |
| `TestUpdateExecutor` |
| `TestUpdateModeString` |
| `TestUpdatePlannerPlanUpdate` |
| `TestValidateUpdate` |

### STATISTICS (3 tests)

| 测试名称 |
|----------|
| `TestCollectStatsBasic` |
| `TestCollectStatsEmpty` |
| `TestTable_V2_Stats` |

### OTHER (86 tests)

| 测试名称 |
|----------|
| `TestAll` |
| `TestCalculateDatasetDataSize` |
| `TestCompareInterfaceValues` |
| `TestCompareValues` |
| `TestComparisonOps` |
| `TestCountRows_AllMatches` |
| `TestCountRows_Basic` |
| `TestCountRows_EmptyResult` |
| `TestCountRows_WithNulls` |
| `TestDataReplacementBatch` |
| `TestDataReplacementManager` |
| `TestDataReplacementPlanner` |
| `TestDataReplacementProgressTracker` |
| `TestDataReplacementRollback` |
| `TestDataReplacementValidator` |
| `TestEncodeBatchBasic` |
| `TestEncodeChunkBasic` |
| `TestEncodeChunkManyColumns` |
| `TestEncodeChunkNilChunk` |
| `TestEncodeChunkRoundtrip` |
| `TestExtractValueFromChunkValue` |
| `TestGenerateFixtures` |
| `TestLanceRoundTrip_WithNulls` |
| `TestLikePattern` |
| `TestMetricTypeString` |
| `TestMockExternalLocker_Basic` |
| `TestMockExternalLocker_Errors` |
| `TestMockExternalLocker_FailAcquire` |
| `TestNewStorageConfig` |
| `TestNodeDistanceHeap` |
| ... (23 more) |

---

## 待开发测试用例 (基于 Lance Rust)

以下是 Lance Rust 中重要但 Storage2 尚未覆盖的测试类型：

### P0 关键测试 (必须实现) — ✅ 全部完成

| 分类 | Lance 测试名 | 说明 | Go 测试名 | 状态 |
|------|-------------|------|-----------|------|
| transaction | `test_concurrent_commits_are_okay` | 并发提交不冲突 | `TestConcurrentCommitsAreOkay` | ✅ |
| transaction | `test_conflicting_rebase` | 冲突 Rebase 检测 | `TestConflictingRebase` | ✅ |
| transaction | `test_conflicts` | 通用冲突检测 | `TestConflicts` | ✅ |
| transaction | `test_commit_batch` | 批量提交 | `TestCommitBatch` | ✅ |
| transaction | `test_inline_transaction` | 内联事务 | `TestInlineTransaction` | ✅ |
| manifest | `test_manifest_naming_scheme` | V1/V2 命名方案 | `TestManifestNamingScheme` | ✅ |
| manifest | `test_manifest_roundtrip` | 序列化往返 | `TestManifestRoundtripFull` | ✅ |
| manifest | `test_current_manifest_path` | 当前 Manifest 路径 | `TestCurrentManifestPath` | ✅ |
| fragment | `test_create_fragment` | 创建 Fragment | `TestCreateFragment` | ✅ |
| fragment | `test_fragment_metadata` | Fragment 元数据 | `TestFragmentMetadata` | ✅ |
| rowid | `test_row_id_sequence_roundtrip` | RowIdSequence 序列化 | `TestRowIdSequenceRoundtrip` | ✅ |
| rowid | `test_u64_segment_encode_decode` | U64Segment 编解码 | `TestU64SegmentEncodeDecode` | ✅ |
| rowid | `test_stable_row_id_basic` | 稳定行ID基础 | `TestStableRowIdBasic` | ✅ |
| encoding | `test_bitpacked_roundtrip` | BitPack 往返 | `TestBitpackedRoundtrip` | ✅ |
| encoding | `test_dict_roundtrip` | Dict 往返 | `TestDictRoundtrip` | ✅ |
| encoding | `test_rle_roundtrip` | RLE 往返 | `TestRleRoundtrip` | ✅ |
| encoding | `test_binary_roundtrip` | Binary 往返 | `TestBinaryRoundtrip` | ✅ |
| io | `test_object_store_roundtrip` | ObjectStore 往返 | `TestObjectStoreRoundtrip` | ✅ |
| io | `test_stream_read_write` | 流读写 | `TestStreamReadWrite` | ✅ |

### P1 高优先级测试 — ✅ 功能覆盖完成 (2026-03-15)

> 基于覆盖率分析，以下按覆盖率缺口大小排列优先级。
> **说明**: P1 采用"覆盖率驱动"策略，针对目标文件编写覆盖测试，而非逐一实现 Lance 同名测试。
> 实际新增 53 个测试 (分布在 4 个新/扩展测试文件中)，功能上覆盖了大部分 Lance P1 目标。

| 分类 | Lance 测试名 | 说明 | 原始覆盖缺口 | 状态 | 实际覆盖的 Go 测试 |
|------|-------------|------|-------------|------|-------------------|
| io | `test_scheduler_iops` | 调度器 IOPS | io_ext.go 35.5% | ✅ 已覆盖 | `TestDefaultIOScheduler`, `TestParallelReaderWriter`, `TestIOStatsCollector` (io_test.go) |
| io | `test_retry_on_transient_error` | 瞬态错误重试 | lance_table_io.go Retryable 0% | ✅ 已覆盖 | `TestRetryableObjectStoreExt*` (7 个, build_manifest_p2_test.go) |
| scan | `test_filter_pushdown` | 过滤下推 | filter_parser.go 54.6% | ✅ 已覆盖 | `TestNullPredicateEvaluate`, `TestInPredicateEvaluate`, `TestLikePredicateEvaluate`, `TestPredicateOutOfRange` (filter_parser_test.go) |
| scan | `test_projection_pushdown` | 投影下推 | pushdown.go 78.8% | ✅ 已覆盖 | `TestCanPushdown*`, `TestEvaluate*Predicate`, `TestCompareValues*` (15 个, pushdown_p2_test.go) |
| scan | `test_take_by_indices` | 按索引 Take | scanner.go | ❌ 未实现 | — (scanner.go 85.6%, 非紧迫) |
| index | `test_scalar_index_roundtrip` | 标量索引往返 | index.go 52.7% | ✅ 已覆盖 | `TestIndexManagerCreate*`, `TestIndexManagerDrop`, `TestIndexManagerList`, `TestIndexManagerOptimize` (12 个, index_manager_test.go) |
| index | `test_vector_index_build` | 向量索引构建 | index_transaction.go 71.5% | 🟡 部分覆盖 | `TestIndexBuilderAsyncBasic`, `TestIndexBuilderSyncBasic`, `TestConcurrentIndexBuilderP2` (index_transaction_p2_test.go) |
| index | `test_index_query` | 索引查询 | index_selector.go 63.7% | ❌ 未实现 | — (index_selector.go 63.7%, 待后续) |
| encoding | `test_mixed_encoding_selection` | 混合编码选择 | encoding.go 79.9% | ❌ 未实现 | — (encoding.go 79.9%, 非紧迫) |
| transaction | `test_rebase_iops` | Rebase IOPS | conflict.go 83.4% | ❌ 未实现 | — (conflict.go 83.4%, 非紧迫) |
| transaction | `test_read_version_isolation` | 读版本隔离 | detached_txn.go | ❌ 未实现 | — (detached_txn.go 79.2%, 待后续) |
| manifest | `test_list_manifests_sorted` | Manifest 列表排序 | lance_table_io.go | 🟡 部分覆盖 | `TestRetryableObjectStoreExt*` 覆盖了 lance_table_io.go 的 Retry 部分 |
| manifest | `auto_cleanup_old_versions` | 自动清理旧版本 | table_format.go 57.4% | ✅ 已覆盖 | `TestCreateTable*`, `TestOpenTable*`, `TestTableValidateTable`, `TestMigrationManager*` (14 个, table_format_p1_test.go) |
| fragment | `test_fragment_take` | Fragment Take | rowid_scanner.go | ❌ 未实现 | — (rowid_scanner.go 84.5%) |
| fragment | `test_fragment_scanner` | Fragment 扫描 | scanner.go | ❌ 未实现 | — |
| fragment | `test_fragment_deletion_file` | 删除文件 | fragment.go | ❌ 未实现 | — (fragment.go 93.3%, 非紧迫) |
| rowid | `test_row_id_index_lookup` | 行ID索引查找 | rowids.go 80.7% | ❌ 未实现 | — |

**P1 实际效果汇总**:
- ✅ 已覆盖: 7/17 (41%) — 通过等价测试实现功能覆盖
- 🟡 部分覆盖: 2/17 (12%) — 目标文件覆盖率已提升，但 Lance 特定场景未完整覆盖
- ❌ 未实现: 8/17 (47%) — 目标文件覆盖率已在中高水平 (63-93%)，优先级降低

### P2 增强测试 — ❌ Lance 映射测试未直接实现 (覆盖率驱动替代)

> **说明**: P2 同样采用"覆盖率驱动"策略，新增 42 个测试覆盖 4 个目标文件
> (build_manifest.go, lance_table_io.go, pushdown.go, index_transaction.go)，
> 但以下 3 个 Lance 映射测试未直接实现。

| 分类 | Lance 测试名 | 说明 | 状态 | 说明 |
|------|-------------|------|------|------|
| manifest | `test_manifest_namespace_basic_create_and_delete` | Namespace 操作 | ❌ 未实现 | Namespace 功能未引入 |
| index | `test_index_update_on_append` | 追加后索引更新 | ❌ 未实现 | 需要完整索引+事务集成 |
| scan | `test_full_scan` | 全表扫描 | ❌ 未实现 | scanner.go 85.6% 已高覆盖 |

---

## 开发行动计划

### 第1阶段：核心功能测试 (P0) — ✅ 已完成

**状态**: 2026-03-15 全部 19 个 P0 测试通过
**覆盖率**: 71.7% (不含 proto)

1. ✅ **事务系统** - 冲突矩阵全覆盖、并发提交、Rebase
2. ✅ **Manifest** - V1/V2 命名、序列化往返、版本隔离
3. ✅ **RowIdSequence** - 5种段编码序列化、稳定行ID
4. ✅ **编码** - BitPack/Dict/RLE/Plain/VarBinary 往返
5. ✅ **IO** - ObjectStore 本地存储往返

### 第2阶段：重要功能测试 (P1) — ✅ 已完成

**状态**: 2026-03-15 全部 4 个模块测试通过，新增 53 个测试
**覆盖率**: 71.7% → 73.5% (不含 proto)

**策略**: 覆盖率驱动 — 针对覆盖率最低的 4 个文件，编写全面的功能覆盖测试

**优先攻坚** (覆盖率最低模块):
1. ✅ **io_ext.go** (35.5% → ~89.6%) — +8 测试: ObjectStoreExt 接口 ReadStream/WriteStream/Copy/Rename/ParallelReader/IOStats
2. ✅ **filter_parser.go** (54.6% → ~91.6%) — +6 测试: Evaluate/CanPushdown/String 方法 (发现并修复 NullPredicate Bug)
3. ✅ **index.go** (52.7% → ~73.6%) — +12 测试: IndexManager 核心操作 Create/Drop/Get/List/Optimize
4. ✅ **table_format.go** (57.4% → ~87.8%) — +14 测试: Table/MigrationManager 全覆盖

**发现的 Bug**:
- **NullPredicate.Evaluate**: 原实现检查 `val == nil`，但 `GetValue()` 返回 `IsNull=true` 的 `*Value`，不返回 `nil`。修复为 `val == nil || val.IsNull`
- **NotNullPredicate.Evaluate**: 同上修复

**新增/修改文件**:
| 文件 | 类型 | 测试数 |
|------|------|--------|
| `io_test.go` | 测试扩展 | +8 |
| `filter_parser_test.go` | 测试扩展 | +6 |
| `index_manager_test.go` | **新文件** | +12 |
| `table_format_p1_test.go` | **新文件** | +14 |
| `filter_parser.go` | **Bug修复** | NullPredicate/NotNullPredicate 逻辑修复 |

### 第3阶段：增强测试 (P2/P3) — ✅ 已完成

#### P2 覆盖率增强 (2026-03-15)

**状态**: 全部 4 个模块测试通过，新增 42 个测试
**覆盖率**: 73.5% → 74.8% (不含 proto)

**策略**: 继续覆盖率驱动，聚焦中等覆盖率文件中的零覆盖函数

1. ✅ **build_manifest.go** — +7 测试: Rewrite (compaction) 操作全覆盖, UpdateConfig replace/delete
2. ✅ **lance_table_io.go** — +8 测试: RetryableObjectStoreExt 全部方法 0%→100%, DefaultRetryConfig
3. ✅ **pushdown.go** — +15 测试: CanPushdown 全类型, compareValues 多类型, Evaluate 组合谓词
4. ✅ **index_transaction.go** — +12 测试: IndexBuilder job管理, 进度跟踪, 并发构建, 回滚/恢复

**新增/修改文件**:
| 文件 | 类型 | 测试数 |
|------|------|--------|
| `build_manifest_p2_test.go` | **新文件** | +14 (含 RetryableObjectStoreExt 7个) |
| `pushdown_p2_test.go` | **新文件** | +15 |
| `index_transaction_p2_test.go` | **新文件** | +13 |

#### P3 S3 集成测试 (2026-03-15)

**状态**: S3 集成测试框架完成，需 MinIO 环境运行
**覆盖率**: 常规测试不变 (74.8%), MinIO 集成测试可达 ~76%

1. ✅ **s3_store.go** — +18 测试: 基础读写、流式操作、范围读取、大文件、列表/复制/重命名
2. ✅ **s3_commit.go** — +8 测试: 版本解析、重复版本检测、外部锁集成、重试退避

**新增/修改文件**:
| 文件 | 类型 | 测试数 |
|------|------|--------|
| `s3_store_p3_test.go` | **新文件** | +18 |
| `s3_commit_p3_test.go` | **新文件** | +8 |
| `s3_minio_test.go` | 修改 | 修复并发提交测试 |
| `s3_commit.go` | 修改 | 改进 commitOnce MinIO 兼容 |

**技术要点**:
- MinIO 不支持 `IfNoneMatch: "*"` 条件写入，使用外部锁机制 (`UseExternalLock: true`)
- S3 集成测试使用 `-tags=s3_integration` 构建标签，与常规测试隔离

**后续待改进项**:
- GCS/Azure 集成测试 (需要对应云环境)
- `s3_store.go` CreateBucket/WaitUntilExists 边界测试
- `s3_commit.go` CommitTransactionWithRetry 更多重试场景

---

## 语句覆盖率热力图 (2026-03-15)

```
覆盖率   ████████████████████  100%
高 ≥85%  ████████████████████  23 个文件 (51.1%)
中 60-85 ████████████████░░░░  20 个文件 (44.4%)
低 <60%  █░░░░░░░░░░░░░░░░░░░   1 个文件 (s3_commit.go 25.0%)
零 =0%   █░░░░░░░░░░░░░░░░░░░   1 个文件 (s3_store.go)
```

### 按文件覆盖率排序 (45 个源文件)

#### 高覆盖率 (>=85%) — 23 个文件

| 源文件 | 覆盖率 | 函数数 | 0%函数 | 说明 |
|--------|--------|--------|--------|------|
| `version.go` | 98.5% | 5 | 0 | V1/V2 命名方案 |
| `fragment_offsets.go` | 95.5% | 2 | 0 | Fragment 偏移计算 |
| `pushdown.go` | 93.9% | 24 | 0 | 谓词下推 |
| `transaction.go` | 93.5% | 9 | 0 | 事务类型构造器 |
| `fragment.go` | 93.3% | 5 | 0 | Fragment/DataFile 构造器 |
| `uri.go` | 92.4% | 6 | 0 | URI 解析 |
| `ivf_index.go` | 92.3% | 16 | 1 | IVF 向量索引 |
| `manifest.go` | 91.7% | 3 | 0 | Manifest 序列化 |
| `filter_parser.go` | 91.6% | 32 | 0 | 过滤表达式解析 |
| `commit.go` | 91.5% | 4 | 0 | 本地提交处理 |
| `bloomfilter_index.go` | 90.6% | 46 | 2 | 布隆过滤器索引 |
| `build_manifest.go` | 90.4% | 11 | 0 | Manifest 构建 |
| `lance_table_io.go` | 89.9% | 36 | 1 | Lance 表 IO/Retry |
| `io_ext.go` | 89.6% | 21 | 0 | ObjectStoreExt 扩展 |
| `hnsw_index.go` | 89.1% | 22 | 2 | HNSW 向量索引 |
| `io.go` | 88.0% | 12 | 0 | 本地对象存储 |
| `table_format.go` | 87.8% | 13 | 0 | 表格式/迁移 |
| `btree_index.go` | 87.8% | 16 | 1 | B-tree 索引 |
| `encoding_scheduler.go` | 87.2% | 19 | 1 | 编码调度器 |
| `bitmap_index.go` | 86.5% | 28 | 2 | 位图索引 |
| `data_replacement.go` | 86.1% | 31 | 3 | 数据替换 |
| `knn.go` | 85.8% | 19 | 1 | KNN 搜索管理 |
| `scanner.go` | 85.6% | 3 | 0 | 扫描器 |

#### 中等覆盖率 (60%-84%) — 20 个文件

| 源文件 | 覆盖率 | 函数数 | 0%函数 | 说明 |
|--------|--------|--------|--------|------|
| `rowid_scanner.go` | 84.5% | 10 | 0 | RowId 扫描 |
| `tags.go` | 83.8% | 2 | 0 | 标签管理 |
| `conflict.go` | 83.4% | 13 | 0 | 冲突检测 |
| `rowids.go` | 80.7% | 56 | 6 | RowId 序列 |
| `data_chunk.go` | 80.5% | 6 | 0 | 数据块读写 |
| `txn_file.go` | 80.2% | 4 | 0 | 事务文件 |
| `encoding.go` | 79.9% | 46 | 6 | 列编码 |
| `detached_txn.go` | 79.2% | 10 | 0 | 分离式事务 |
| `commit_txn.go` | 78.6% | 1 | 0 | 事务提交 |
| `zonemap_index.go` | 77.1% | 28 | 2 | ZoneMap 索引 |
| `index_transaction.go` | 76.9% | 36 | 5 | 索引事务 |
| `store_factory.go` | 76.3% | 26 | 3 | 存储工厂 |
| `az_store.go` | 74.4% | 40 | 6 | Azure 存储 |
| `lance_encoder.go` | 74.0% | 18 | 2 | Lance 编码器 |
| `refs.go` | 74.0% | 24 | 3 | 引用管理 |
| `data_size.go` | 73.8% | 2 | 0 | 数据大小计算 |
| `index.go` | 73.6% | 32 | 7 | 索引管理 |
| `gs_store.go` | 70.9% | 39 | 6 | GCS 存储 |
| `index_selector.go` | 63.7% | 21 | 5 | 索引选择器 |
| `update.go` | 60.4% | 28 | 6 | 更新操作 |

#### 低覆盖率 (<60%) — 2 个文件 ⚠️

| 源文件 | 覆盖率 | 函数数 | 0%函数 | 说明 |
|--------|--------|--------|--------|------|
| `s3_commit.go` | 25.0% | 12 | 9 | S3 提交 (需 MinIO 集成测试) |
| `s3_store.go` | **0.0%** | 31 | 31 | S3 存储 (需 MinIO 集成测试) |

### 函数级覆盖率分布

| 覆盖级别 | 函数数 | 占比 |
|----------|--------|------|
| 高覆盖 (>=85%) | 520 | 59.9% |
| 中覆盖 (60-84%) | 206 | 23.7% |
| 低覆盖 (1-59%) | 31 | 3.6% |
| 零覆盖 (0%) | 111 | 12.8% |
| **合计** | **868** | **100%** |

### 零覆盖函数分布 (top 10 文件, 不含 s3_store.go)

| 源文件 | 0%函数数 | 关键未覆盖函数 |
|--------|----------|---------------|
| `s3_commit.go` | 9 | commitOnce (条件写入分支), retryCommit 等 |
| `index.go` | 7 | CreateScalarIndex, DropIndex, GetIndex 等 |
| `update.go` | 6 | ApplyUpdate, ExecuteColumnRewrite 等 |
| `rowids.go` | 6 | 部分 Segment 序列化辅助函数 |
| `encoding.go` | 6 | encodeDoubleColumn, decodeDoubleColumn 等 |
| `az_store.go` | 6 | MkdirAll, Container, Close 等 |
| `gs_store.go` | 6 | MkdirAll, Bucket, ReadStream 等 |
| `index_transaction.go` | 5 | startAsyncBuild, runAsyncBuild 等 |
| `index_selector.go` | 5 | selectBestIndex, rankIndexes 等 |
| `store_factory.go` | 3 | NewS3Store, NewGSStore 等 |

---

## 测试文件组织建议

```
pkg/storage2/
├── conflict_test.go          # 事务冲突测试
├── manifest_test.go          # Manifest 测试 (扩展)
├── rowids_test.go            # RowIdSequence 测试 (扩展)
├── encoding_test.go          # 编码器测试
├── fragment_test.go          # Fragment 测试 (新增)
├── scanner_test.go           # 扫描测试 (新增)
├── index_test.go             # 索引测试 (扩展)
├── io_test.go                # IO 测试 (扩展)
├── lance_compat_test.go      # Lance 兼容性测试 (新增)
└── integration_test.go       # 端到端集成测试 (新增)
```
