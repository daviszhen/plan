package storage2

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestCommitTransactionFirstCommit(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	handler := NewLocalRenameCommitHandler()

	m := NewManifest(0)
	m.Version = 0
	m.Fragments = []*DataFragment{}
	if err := handler.Commit(ctx, dir, 0, m); err != nil {
		t.Fatal(err)
	}
	// Now we have version 0. Commit a transaction that reads version 0 and appends.
	txn := NewTransactionAppend(0, "first", []*DataFragment{
		NewDataFragment(0, []*DataFile{NewDataFile("d.parquet", []int32{0}, 1, 0)}),
	})
	err := CommitTransaction(ctx, dir, handler, txn)
	if err != nil {
		t.Fatal(err)
	}
	latest, _ := handler.ResolveLatestVersion(ctx, dir)
	if latest != 1 {
		t.Errorf("latest version: got %d want 1", latest)
	}
	m1, _ := LoadManifest(ctx, dir, handler, 1)
	if len(m1.Fragments) != 1 || m1.Fragments[0].Id != 0 {
		t.Errorf("manifest 1 fragments: %v", m1.Fragments)
	}
	txnPath := filepath.Join(dir, TransactionsDir, "0-first.txn")
	if _, err := os.Stat(txnPath); err != nil {
		t.Errorf("txn file not written: %v", err)
	}
}

func TestCommitTransactionRebase(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	handler := NewLocalRenameCommitHandler()
	m0 := NewManifest(0)
	m0.Version = 0
	m0.Fragments = []*DataFragment{}
	_ = handler.Commit(ctx, dir, 0, m0)

	// First commit: append (produces v1).
	_ = CommitTransaction(ctx, dir, handler, NewTransactionAppend(0, "first", []*DataFragment{
		NewDataFragment(0, []*DataFile{NewDataFile("a.parquet", []int32{0}, 1, 0)}),
	}))
	// Second commit: append based on v1 (rebase path: no conflict with first).
	err := CommitTransaction(ctx, dir, handler, NewTransactionAppend(1, "second", []*DataFragment{
		NewDataFragment(0, []*DataFile{NewDataFile("b.parquet", []int32{0}, 1, 0)}),
	}))
	if err != nil {
		t.Fatal(err)
	}
	latest, _ := handler.ResolveLatestVersion(ctx, dir)
	if latest != 2 {
		t.Errorf("latest: got %d want 2", latest)
	}
	m2, _ := LoadManifest(ctx, dir, handler, 2)
	if len(m2.Fragments) != 2 {
		t.Errorf("manifest 2 fragments: got %d", len(m2.Fragments))
	}
}

func TestCommitTransactionConflict(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	handler := NewLocalRenameCommitHandler()

	m0 := NewManifest(0)
	m0.Version = 0
	m0.Fragments = []*DataFragment{}
	_ = handler.Commit(ctx, dir, 0, m0)

	txnAppend := NewTransactionAppend(0, "append", nil)
	err := CommitTransaction(ctx, dir, handler, txnAppend)
	if err != nil {
		t.Fatal(err)
	}
	// Now version 1 exists. Try to commit an Overwrite based on version 0 (conflicts with append that produced v1).
	txnOverwrite := NewTransactionOverwrite(0, "overwrite", nil, nil, nil)
	err = CommitTransaction(ctx, dir, handler, txnOverwrite)
	if err != ErrConflict {
		t.Errorf("expected ErrConflict, got %v", err)
	}
}
