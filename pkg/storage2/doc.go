// Package storage2 implements a versioned table storage engine aligned with
// Lance's metadata model (Manifest, Fragment, DataFile, Transaction) for
// comparison testing and future integration.
//
// It is independent of pkg/storage and other plan packages. Serialization
// uses the same Proto definitions as Lance (table.proto, transaction.proto);
// data files use a columnar format compatible with pkg/chunk, not Arrow.
//
// See STORAGE2_DEVELOPMENT_PLAN.md for the full design and task breakdown.
//
// Known differences from Lance:
//   - Data file format: Lance uses Arrow columnar (e.g. file2); Storage2 does not
//     implement Arrow and targets a columnar format compatible with plan's pkg/chunk.
//   - Metadata (Manifest, Fragment, Transaction) uses the same Proto as Lance;
//     path convention matches: _versions/{v}.manifest, _transactions/{rv}-{uuid}.txn.
//   - Not all Lance operations are implemented (e.g. CreateIndex, Rewrite, Merge,
//     UpdateConfig); Append, Delete, Overwrite and conflict/rebase semantics are aligned.
package storage2
