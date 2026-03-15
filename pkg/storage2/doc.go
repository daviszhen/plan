// Package storage2 implements a versioned table storage engine aligned with
// Lance's metadata model (Manifest, Fragment, DataFile, Transaction) for
// comparison testing and future integration.
//
// The metadata layer is independent of pkg/storage and pkg/compute; serialization
// uses the same Proto as Lance (table.proto, transaction.proto). Data file
// columnar format uses pkg/chunk: WriteChunkToFile and ReadChunkFromFile use
// chunk.Chunk.Serialize/Deserialize, so files are persisted Chunks (pkg/common
// types, chunk layout).
//
// See STORAGE2_DEVELOPMENT_PLAN.md for the full design and task breakdown.
//
// Known differences from Lance:
//   - Data file format: Lance uses Arrow columnar; Storage2 uses pkg/chunk
//     columnar format (chunk.Serialize/Deserialize).
//   - Metadata (Manifest, Fragment, Transaction) uses the same Proto as Lance;
//     path convention: _versions/{v}.manifest, _transactions/{rv}-{uuid}.txn.
//   - Not all Lance operations are implemented (e.g. CreateIndex, Rewrite, Merge,
//     UpdateConfig); Append, Delete, Overwrite and conflict/rebase semantics are aligned.
package storage2
