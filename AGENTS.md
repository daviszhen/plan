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
go test -race ./pkg/storage2/...           # Run with race detector
go test -coverprofile=coverage.out ./pkg/storage2/...  # Generate coverage

# Code quality
make fmt             # Format code with gofmt
make static-check    # Run golangci-lint + license-eye header check
```

**CGO required**: The project uses CGO for memory management (`pkg/util/mem.go`, `pkg/storage/mem_buffer.go`). Ensure `CGO_ENABLED=1` (default on most systems).

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
- **`CommitHandler`** (`commit.go`) - Transaction commit protocol. `LocalRenameCommitHandler` for local (atomic rename), `S3CommitHandler` for S3 (optimistic locking with ETags)
- **Manifest** (`manifest.go`) - Version snapshot: schema, fragments, config, feature flags
- **Transaction** (`transaction.go`) - Atomic operations: Append, Delete, Overwrite, Project, Merge, Clone, Rewrite (compaction)
- **Feature flags** - `ReaderFeatureFlags` / `WriterFeatureFlags` on Manifest: bit 1 = deletion files, bit 2 = stable row IDs
- **RowIdSequence** - Protobuf-encoded row ID mapping in DataFragment, five segment types: Range, RangeWithHoles, RangeWithBitmap, SortedArray, Array

**Transaction Flow:**
1. Create transaction (Append/Delete/Overwrite/etc.)
2. `CommitTransaction()` checks conflicts via `read_version`
3. On conflict, returns `ErrConflict` for retry
4. Success writes new Manifest with incremented version number

**Index System** (`index.go`, `index_selector.go`): Bitmap, ZoneMap, BloomFilter, BTree (scalar); IVF, HNSW, IVF-HNSW (vector); FTS with WAND algorithm (full-text). `IndexSelector` does cost-based index selection. `IndexManager` handles lifecycle.

**Encoding Framework** (`encoding.go`): Physical encoders (Plain, BitPack, RLE, Dict, VarBinary, StringDict) with automatic encoding selection via `AnalyzeIntColumn`/`AnalyzeStringColumn`. `EncodingScheduler` (`encoding_scheduler.go`) provides parallel, memory-bounded column encoding.

**Lance V2 File Format** (`lance_v2_format.go`, `lance_v2_column.go`, `lance_v2_file.go`): Column-oriented binary format with `V2FileWriter`/`V2FileReader` for reading and writing lance v2 data files.

**Compaction** (`compaction_worker.go`, `compaction_coordinator.go`, `compaction_planner.go`): Worker executes individual compaction tasks, Coordinator orchestrates end-to-end compaction, Planner selects fragments using bin-packing strategies.

**Advanced Transactions**: `UpdatePlanner`/`UpdateExecutor` (`update.go`) for row-level updates with predicate filtering; `IndexBuilder`/`ConcurrentIndexBuilder` (`index_transaction.go`) for transactional index creation; `DataReplacementManager` (`data_replacement.go`) for atomic file-level data replacement.

**Cloud Storage**: S3 (`s3_store.go`), GCS (`gs_store.go`), Azure (`az_store.go`) backends. S3 commit uses optimistic locking via copy + ETag since S3 lacks atomic rename.

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
- **V2 file format field sizes** - Footer is 48 bytes (not 52). `V2ColumnWriter.appendNotNull()` requires special handling for the first value (zero-length previous entry). `V2ColumnReader.ReadVector()` must respect Seek offset.

## Testing Conventions

- Tests are co-located with source (`*_test.go`)
- Use `t.TempDir()` for temporary directories (auto-cleaned)
- Use `context.Background()` for test contexts
- Use `NewLocalRenameCommitHandler()` for local storage2 tests
- **pkg/storage2/ tests**: Use `t.Fatalf()` / `t.Errorf()` (stdlib only, no testify)
- **sdk/ tests**: Some files use `github.com/stretchr/testify` (e.g., `knn_test.go`, phase6 tests)
- `util.DefaultVectorSize` (2048) is the standard chunk capacity for tests
- **golangci-lint excludes test files** from `stylecheck`, `govet`, and `unused` linters (see `.golangci.yml`)

## Development Notes

- Go 1.24 with toolchain go1.24.4
- Apache 2.0 license header required on all `.go`, `.proto`, `.c`, `.h` files - checked by `license-eye` (copyright-owner: daviszhen)
- Error handling uses custom error types in `pkg/util/`
- TPC-H 1G is the primary benchmark - all 22 queries pass
