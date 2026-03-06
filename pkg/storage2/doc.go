// Package storage2 implements a versioned table storage engine aligned with
// Lance's metadata model (Manifest, Fragment, DataFile, Transaction) for
// comparison testing and future integration.
//
// It is independent of pkg/storage and other plan packages. Serialization
// uses the same Proto definitions as Lance (table.proto, transaction.proto);
// data files use a columnar format compatible with pkg/chunk, not Arrow.
//
// See STORAGE2_DEVELOPMENT_PLAN.md for the full design and task breakdown.
package storage2
