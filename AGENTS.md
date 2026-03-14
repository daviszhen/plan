# AGENTS.md

This file provides guidance to Qoder (qoder.com) when working with code in this repository.

## Build and Test Commands

```bash
# Build binaries
make plandb          # Build psql-compatible server (cmd/main/main.go)
make tester          # Build non-interactive tester (cmd/tester/main.go)

# Run tests
go test ./...                              # Run all tests
go test ./pkg/storage2/...                 # Run storage2 tests
go test ./sdk/...                          # Run SDK tests
go test -run TestName ./pkg/storage2/...   # Run specific test

# Code quality
make fmt             # Format code with gofmt
make static-check    # Run golangci-lint + license-eye header check
```

## Architecture Overview

Single-node transactional database kernel designed for TPC-H query execution. Two separate storage backends coexist.

### Dual Storage Engine Architecture

```
         cmd/ (plandb, tester)
                 │
                 ▼
         pkg/compute/            ← Query execution (SQL → plan → execute)
                 │
                 ▼
         pkg/storage/            ← Original block-based engine (WAL, checkpoint)
                                   Uses global singletons: GCatalog, GStorageMgr, GTxnMgr


         sdk/                    ← High-level Dataset API
                 │
                 ▼
         pkg/storage2/           ← Versioned engine (Lance-compatible metadata)
                 │
          ┌──────┴──────┐
          ▼              ▼
    pkg/chunk/    pkg/storage2/proto/
    (columnar)    (protobuf metadata)
          │
          ▼
    pkg/common/
    (LType, PhyType)
```

**Critical distinction:** `pkg/compute/` uses `pkg/storage/` (original engine). `sdk/` uses `pkg/storage2/` (versioned engine). They are independent subsystems.

### pkg/storage2/ - Versioned Storage Engine

Aligned with the Lance columnar format metadata model. Key interfaces:

- **`ObjectStore` / `ObjectStoreExt`** (`io.go`) - Storage abstraction for local/S3/GCS/Azure. URI schemes: `file:///`, `s3://`, `gs://`, `az://`, `mem://`
- **`CommitHandler`** (`commit.go`) - Transaction commit protocol. `LocalRenameCommitHandler` for local (atomic rename), `S3CommitHandler` for S3 (optimistic locking with ETags since S3 lacks atomic rename)
- **Manifest** (`manifest.go`) - Version snapshot: schema, fragments, config, feature flags
- **Transaction** (`transaction.go`) - Atomic operations: Append, Delete, Overwrite, Project (drop columns), Merge, Clone, Rewrite (compaction)
- **Feature flags** - `ReaderFeatureFlags` / `WriterFeatureFlags` on Manifest: bit 1 = deletion files, bit 2 = stable row IDs
- **RowIdSequence** - Protobuf-encoded row ID mapping in DataFragment, five segment types: Range, RangeWithHoles, RangeWithBitmap, SortedArray, Array

**Transaction Flow:**
1. Create transaction (Append/Delete/Overwrite/etc.)
2. `CommitTransaction()` checks conflicts via `read_version`
3. On conflict, returns `ErrConflict` for retry
4. Success writes new Manifest with incremented version number

**Index System:**
- **BitmapIndex** (`bitmap_index.go`) - 64-bit word-compressed bitmap for low-cardinality columns, supports AND/OR/NOT operations
- **ZoneMapIndex** (`zonemap_index.go`) - Column-level and fragment-level min/max statistics for range query pruning
- **BloomFilterIndex** (`bloomfilter_index.go`) - Probabilistic membership testing with configurable false positive rate
- **BTreeIndex** (`btree_index.go`) - B-tree index for scalar columns
- **IVFIndex** (`ivf_index.go`) - Inverted File vector index for ANN search
- **HNSWIndex** (`hnsw_index.go`) - Hierarchical Navigable Small World graph for ANN search
- **IndexSelector** (`index_selector.go`) - Cost-based index selection for query optimization
- **IndexManager** (`index.go`) - Index lifecycle management (create, drop, optimize, rebuild)

**Cloud Storage:**
- **S3Store** (`s3_store.go`) - S3 backend with multipart upload support
- **GSStore** (`gs_store.go`) - Google Cloud Storage backend
- **AZStore** (`az_store.go`) - Azure Blob Storage backend

**Encoding Framework:**
- **Physical Encoders** (`encoding.go`):
  - `PlainEncoder` - Fixed-width native binary encoding (1, 2, 4, 8 bytes)
  - `BitPackEncoder` - Minimum-bits packing with min/max offset, effective for small value ranges
  - `RLEEncoder` - Run-length encoding for repeated values, stores (count, value) pairs
  - `DictEncoder` - Dictionary encoding for integer columns with few distinct values
  - `VarBinaryEncoder` - Variable-length binary with offset/data layout for strings/blobs
  - `StringDictEncoder` - Dictionary encoding for string columns
- **Logical Encoder/Decoder** - `LogicalColumnEncoder` and `LogicalColumnDecoder` provide type-aware encoding with automatic encoding selection
- **Encoding Selection** - `AnalyzeIntColumn`, `AnalyzeStringColumn`, `SelectIntEncoding`, `SelectStringEncoding` choose optimal encoding based on column statistics
- **EncodingProfile** - Measures compression ratios across different encodings for a column
- **Encoding Scheduler** (`encoding_scheduler.go`):
  - `EncodingScheduler` - Parallel, memory-bounded column encoding with semaphore-based concurrency control
  - `EncodingSchedulerConfig` - Configurable `MaxConcurrency`, `MemoryBudget`, `AutoSelectEncoding`
  - `EncodingTask` / `EncodingResult` - Task and result types for individual column encoding jobs
  - `EncodingProgress` - Atomic progress tracking (completed columns, bytes encoded, fraction)
  - `EncodeChunk` - Convenience method to encode all columns of a `chunk.Chunk` in parallel
  - `EncodeBatch` - Sequential chunk encoding with per-chunk parallel column encoding
  - `PlanChunkEncoding` - Preview encoding decisions per column without encoding
  - `CollectStats` - Aggregate encoding statistics from results
  - Memory back-pressure via `memoryTracker` with configurable budget and `sync.Cond` blocking

**Advanced Transactions (Phase 9):**
- **Update Operation** (`update.go`) - Row-level update transactions with predicate filtering:
  - `UpdatePlanner` - Plans update operations with cost-based strategy selection (REWRITE_ROWS vs REWRITE_COLUMNS)
  - `UpdateExecutor` - Applies updates to chunks with row-level granularity
  - `UpdatePredicate` - Filter expression support for targeting specific rows
  - Conflict detection via `CheckUpdateConflict()`
- **CreateIndex Transaction** (`index_transaction.go`) - Transactional index creation:
  - `IndexBuilder` - Sync/async index building with job management
  - `IndexBuildJob` - Tracks ongoing index builds with cancellation support
  - `IndexBuildProgressTracker` - Atomic progress tracking for long-running builds
  - `ConcurrentIndexBuilder` - Semaphore-based concurrent index building
  - `IndexRollback` - Rollback support for failed index operations
- **DataReplacement Operation** (`data_replacement.go`) - File-level data replacement:
  - `DataReplacementPlanner` - Plans atomic data replacement operations
  - `DataReplacementValidator` - Validates replacement data (checksum, row count, schema)
  - `DataReplacementManager` - Orchestrates replacement with atomic commit
  - Batch processing support via `DataReplacementBatch`

### sdk/ - High-Level SDK

`Dataset` interface (`dataset.go`) wraps storage2 with: Append, Delete, Overwrite, Scanner, Take, DropColumns, ShallowClone, Compact, detached transactions, KNN vector search.

`ScannerBuilder` (`scanner.go`) supports filter expressions: `"c0 > 10 AND c1 < 100"`, `"name LIKE 'foo%'"`, `"id IN (1, 2, 3)"`, `"c0 IS NULL"`. Column names use `"c0"`, `"c1"` format (zero-indexed).

### pkg/compute/ - Query Execution Pipeline

SQL → AST (pg_query_go) → logical plan (Builder) → optimization (column pruning, join ordering, cost estimation) → physical plan → execution (Runner produces Chunks).

Key operator types: Scan, Project, Join (cross/hash/collection), Aggregate, Filter, Order, Limit.

### pkg/chunk/ - Columnar Data Format

`Chunk` holds columnar `Vector`s. Four physical formats: PF_CONST, PF_FLAT, PF_DICT, PF_SEQUENCE. Custom serialization (not Arrow):
```
[uint32: row count] [uint32: col count] [LType per col] [Vector data per col]
```

### pkg/common/ - Type System

`LType` (logical type) maps to `PhyType` (physical type). Key type IDs: LTID_BOOLEAN(10), LTID_TINYINT(11), LTID_SMALLINT(12), LTID_INTEGER(13), LTID_BIGINT(14), LTID_DATE(15), LTID_DECIMAL(21), LTID_FLOAT(22), LTID_DOUBLE(23), LTID_VARCHAR(25), LTID_HUGEINT(50), LTID_STRUCT(100), LTID_LIST(101).

### Proto Files

Located in `pkg/storage2/proto/` - copied from Lance for metadata compatibility:
- `file.proto` - Field, Schema, FileDescriptor
- `table.proto` - Manifest, DataFragment, DataFile, DeletionFile
- `transaction.proto` - Transaction and operations

Regenerate: `cd pkg/storage2/proto && make`

## Gotchas and Known Limitations

- **Chunk serialization does not support LTID_DOUBLE** - Use LTID_INTEGER or LTID_FLOAT instead in tests. LTID_DOUBLE causes a panic in `chunk.Serialize()`.
- **S3 has no atomic rename** - S3CommitHandler uses optimistic locking (copy + ETag check) instead of rename. Race conditions are handled via conflict detection.
- **Scanner column names are positional** - Filters use `"c0"`, `"c1"`, etc. (zero-indexed), not the schema field names.
- **Global singletons in pkg/storage/** - `GCatalog`, `GStorageMgr`, `GTxnMgr`, `GBufferMgr` are initialized at startup. This is the original engine only.
- **RowIdSequence protobuf encoding** - Requires an outer `RowIdSequence` wrapper message around the `U64Segment`. Missing the wrapper causes "expected length-delimited" parse errors.
- **String memory in chunk** - Uses C-allocated memory (`util.CMalloc`) requiring proper lifecycle management.

## Testing Conventions

- Tests are co-located with source (`*_test.go`)
- Use `t.TempDir()` for temporary directories (auto-cleaned)
- Use `context.Background()` for test contexts
- Use `NewLocalRenameCommitHandler()` for local storage2 tests
- Assertions use `t.Fatalf()` / `t.Errorf()` (not testify in storage2 tests)
- `util.DefaultVectorSize` (2048) is the standard chunk capacity for tests

## Development Notes

- Go 1.24 with toolchain go1.24.4
- Apache 2.0 license header required on all `.go`, `.proto`, `.c`, `.h` files - checked by `license-eye`
- Error handling uses custom error types in `pkg/util/`
- TPC-H 1G is the primary benchmark - all 22 queries pass
