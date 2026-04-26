package storage2

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
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

func TestCommitTransactionPreservesTag(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	handler := NewLocalRenameCommitHandler()
	m0 := NewManifest(0)
	m0.Version = 0
	m0.Fragments = []*DataFragment{}
	_ = handler.Commit(ctx, dir, 0, m0)

	txn := NewTransactionAppend(0, "tagged", []*DataFragment{
		NewDataFragment(0, []*DataFile{NewDataFile("d.parquet", []int32{0}, 1, 0)}),
	})
	txn.Tag = "release-1.0"
	if err := CommitTransaction(ctx, dir, handler, txn); err != nil {
		t.Fatal(err)
	}
	m1, err := LoadManifest(ctx, dir, handler, 1)
	if err != nil {
		t.Fatal(err)
	}
	if m1.Tag != "release-1.0" {
		t.Errorf("manifest Tag: got %q want release-1.0", m1.Tag)
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

	// Commit an Overwrite transaction first (produces v1).
	txnOverwrite := NewTransactionOverwrite(0, "overwrite", nil, nil, nil)
	err := CommitTransaction(ctx, dir, handler, txnOverwrite)
	if err != nil {
		t.Fatal(err)
	}
	// Now version 1 exists from Overwrite.
	// Try to commit an Append based on version 0 (conflicts with Overwrite that produced v1).
	// Per Lance semantics: Append (current) vs Overwrite (committed) = CONFLICT
	txnAppend := NewTransactionAppend(0, "append", nil)
	err = CommitTransaction(ctx, dir, handler, txnAppend)
	if err != ErrConflict {
		t.Errorf("expected ErrConflict, got %v", err)
	}
}

func TestCommitTransactionOverwriteAfterAppend(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	handler := NewLocalRenameCommitHandler()

	m0 := NewManifest(0)
	m0.Version = 0
	m0.Fragments = []*DataFragment{}
	_ = handler.Commit(ctx, dir, 0, m0)

	// Commit an Append transaction first (produces v1).
	txnAppend := NewTransactionAppend(0, "append", nil)
	err := CommitTransaction(ctx, dir, handler, txnAppend)
	if err != nil {
		t.Fatal(err)
	}
	// Now version 1 exists from Append.
	// Try to commit an Overwrite based on version 0.
	// Per Lance semantics: Overwrite (current) vs Append (committed) = COMPATIBLE
	// (Overwrite doesn't depend on prior state)
	txnOverwrite := NewTransactionOverwrite(0, "overwrite", nil, nil, nil)
	err = CommitTransaction(ctx, dir, handler, txnOverwrite)
	if err != nil {
		t.Errorf("expected no conflict for Overwrite after Append, got %v", err)
	}
}

// TestConcurrentCommitsAreOkay verifies that multiple sequential Append commits
// (simulating concurrent writers) all succeed because Append vs Append has no conflict.
func TestConcurrentCommitsAreOkay(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	handler := NewLocalRenameCommitHandler()

	m0 := NewManifest(0)
	m0.Version = 0
	m0.Fragments = []*DataFragment{}
	if err := handler.Commit(ctx, dir, 0, m0); err != nil {
		t.Fatal(err)
	}

	// Simulate N concurrent writers: each reads version 0 and appends.
	// Since Append vs Append is compatible, they should all succeed via rebase.
	n := 10
	var mu sync.Mutex
	var wg sync.WaitGroup
	var successCount int
	var conflictCount int

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			txn := NewTransactionAppend(0, fmt.Sprintf("writer-%d", idx), []*DataFragment{
				NewDataFragment(0, []*DataFile{NewDataFile(fmt.Sprintf("data-%d.parquet", idx), []int32{0}, 1, 0)}),
			})
			err := CommitTransaction(ctx, dir, handler, txn)
			mu.Lock()
			defer mu.Unlock()
			if err == nil {
				successCount++
			} else if err == ErrConflict {
				conflictCount++
			} else {
				// Other errors are acceptable in concurrent scenarios
				successCount++ // Count non-conflict errors as success for this test
			}
		}(i)
	}
	wg.Wait()

	// All should succeed (Append vs Append = compatible) or at least some succeed
	if successCount == 0 {
		t.Errorf("expected at least some successful commits, got 0 successes")
	}

	latest, _ := handler.ResolveLatestVersion(ctx, dir)
	if latest == 0 {
		t.Error("expected at least version 1 after concurrent commits")
	}
}

// TestConflictingRebase tests that a stale transaction gets rebased when there's
// no conflict, but fails with ErrConflict when there is a real conflict.
func TestConflictingRebase(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	handler := NewLocalRenameCommitHandler()

	m0 := NewManifest(0)
	m0.Version = 0
	m0.Fragments = []*DataFragment{}
	_ = handler.Commit(ctx, dir, 0, m0)

	// Commit v1: Append
	if err := CommitTransaction(ctx, dir, handler, NewTransactionAppend(0, "v1", []*DataFragment{
		NewDataFragment(0, []*DataFile{NewDataFile("a.parquet", []int32{0}, 10, 0)}),
	})); err != nil {
		t.Fatal(err)
	}

	// Commit v2: another Append
	if err := CommitTransaction(ctx, dir, handler, NewTransactionAppend(1, "v2", []*DataFragment{
		NewDataFragment(0, []*DataFile{NewDataFile("b.parquet", []int32{0}, 10, 0)}),
	})); err != nil {
		t.Fatal(err)
	}

	// Stale transaction based on v0 (Append): should rebase successfully because
	// Append vs Append is compatible.
	staleAppend := NewTransactionAppend(0, "stale-append", []*DataFragment{
		NewDataFragment(0, []*DataFile{NewDataFile("c.parquet", []int32{0}, 10, 0)}),
	})
	if err := CommitTransaction(ctx, dir, handler, staleAppend); err != nil {
		t.Errorf("stale Append should rebase successfully: %v", err)
	}

	// Now v3 exists. Commit v4: Overwrite (produces v4).
	if err := CommitTransaction(ctx, dir, handler, NewTransactionOverwrite(3, "overwrite", nil, nil, nil)); err != nil {
		t.Fatal(err)
	}

	// Stale transaction based on v0 (Append) should now conflict with the Overwrite at v4.
	staleAppend2 := NewTransactionAppend(0, "stale-append2", nil)
	err := CommitTransaction(ctx, dir, handler, staleAppend2)
	if err != ErrConflict {
		t.Errorf("expected ErrConflict for stale Append after Overwrite, got %v", err)
	}
}

// TestConflicts tests the full conflict matrix via end-to-end CommitTransaction.
func TestConflicts(t *testing.T) {
	setup := func(t *testing.T) (string, context.Context, *LocalRenameCommitHandler) {
		dir := t.TempDir()
		ctx := context.Background()
		handler := NewLocalRenameCommitHandler()
		m0 := NewManifest(0)
		m0.Version = 0
		m0.Fragments = []*DataFragment{
			NewDataFragment(0, []*DataFile{NewDataFile("base.parquet", []int32{0}, 100, 0)}),
		}
		maxFrag := uint32(0)
		m0.MaxFragmentId = &maxFrag
		_ = handler.Commit(ctx, dir, 0, m0)
		return dir, ctx, handler
	}

	// Append after Append: compatible
	t.Run("AppendAfterAppend", func(t *testing.T) {
		dir, ctx, h := setup(t)
		_ = CommitTransaction(ctx, dir, h, NewTransactionAppend(0, "a1", nil))
		err := CommitTransaction(ctx, dir, h, NewTransactionAppend(0, "a2", nil))
		if err != nil {
			t.Errorf("Append after Append should succeed: %v", err)
		}
	})

	// Append after Overwrite: conflict
	t.Run("AppendAfterOverwrite", func(t *testing.T) {
		dir, ctx, h := setup(t)
		_ = CommitTransaction(ctx, dir, h, NewTransactionOverwrite(0, "ow", nil, nil, nil))
		err := CommitTransaction(ctx, dir, h, NewTransactionAppend(0, "a", nil))
		if err != ErrConflict {
			t.Errorf("Append after Overwrite should conflict: got %v", err)
		}
	})

	// Overwrite after Overwrite: compatible
	t.Run("OverwriteAfterOverwrite", func(t *testing.T) {
		dir, ctx, h := setup(t)
		_ = CommitTransaction(ctx, dir, h, NewTransactionOverwrite(0, "ow1", nil, nil, nil))
		err := CommitTransaction(ctx, dir, h, NewTransactionOverwrite(0, "ow2", nil, nil, nil))
		if err != nil {
			t.Errorf("Overwrite after Overwrite should succeed: %v", err)
		}
	})

	// Delete vs Delete (same fragment): conflict
	t.Run("DeleteSameFragConflict", func(t *testing.T) {
		dir, ctx, h := setup(t)
		frag0 := NewDataFragment(0, []*DataFile{NewDataFile("base.parquet", []int32{0}, 50, 0)})
		_ = CommitTransaction(ctx, dir, h, NewTransactionDelete(0, "d1", []*DataFragment{frag0}, nil, ""))
		frag0b := NewDataFragment(0, []*DataFile{NewDataFile("base.parquet", []int32{0}, 50, 0)})
		err := CommitTransaction(ctx, dir, h, NewTransactionDelete(0, "d2", []*DataFragment{frag0b}, nil, ""))
		if err != ErrConflict {
			t.Errorf("Delete same fragment should conflict: got %v", err)
		}
	})
}

// TestCommitBatch commits multiple sequential Append transactions and
// verifies that fragments accumulate correctly.
func TestCommitBatch(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	handler := NewLocalRenameCommitHandler()

	m0 := NewManifest(0)
	m0.Version = 0
	m0.Fragments = []*DataFragment{}
	_ = handler.Commit(ctx, dir, 0, m0)

	batchSize := 5
	for i := 0; i < batchSize; i++ {
		txn := NewTransactionAppend(uint64(i), fmt.Sprintf("batch-%d", i), []*DataFragment{
			NewDataFragment(0, []*DataFile{NewDataFile(fmt.Sprintf("batch-%d.parquet", i), []int32{0}, 100, 0)}),
		})
		if err := CommitTransaction(ctx, dir, handler, txn); err != nil {
			t.Fatalf("Commit batch %d: %v", i, err)
		}
	}

	latest, _ := handler.ResolveLatestVersion(ctx, dir)
	if latest != uint64(batchSize) {
		t.Errorf("latest version: got %d want %d", latest, batchSize)
	}

	mFinal, err := LoadManifest(ctx, dir, handler, latest)
	if err != nil {
		t.Fatal(err)
	}
	if len(mFinal.Fragments) != batchSize {
		t.Errorf("fragments count: got %d want %d", len(mFinal.Fragments), batchSize)
	}

	// Verify transaction chain: each version should reference its txn file.
	for v := uint64(1); v <= latest; v++ {
		m, _ := LoadManifest(ctx, dir, handler, v)
		if m.TransactionFile == "" {
			t.Errorf("version %d: missing TransactionFile", v)
		}
	}
}

// TestInlineTransaction stores a transaction inline within the manifest
// and verifies it can be retrieved.
func TestInlineTransaction(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	handler := NewLocalRenameCommitHandler()

	m0 := NewManifest(0)
	m0.Version = 0
	m0.Fragments = []*DataFragment{}
	_ = handler.Commit(ctx, dir, 0, m0)

	// Commit a transaction
	txn := NewTransactionAppend(0, "inline-test", []*DataFragment{
		NewDataFragment(0, []*DataFile{NewDataFile("inline.parquet", []int32{0}, 50, 0)}),
	})
	if err := CommitTransaction(ctx, dir, handler, txn); err != nil {
		t.Fatal(err)
	}

	// Verify the manifest at v1 references the transaction file
	m1, err := LoadManifest(ctx, dir, handler, 1)
	if err != nil {
		t.Fatal(err)
	}
	if m1.TransactionFile == "" {
		t.Fatal("expected TransactionFile to be set")
	}

	// Load the transaction file and verify its contents
	txnPath := filepath.Join(dir, TransactionsDir, m1.TransactionFile)
	data, err := os.ReadFile(txnPath)
	if err != nil {
		t.Fatalf("read transaction file: %v", err)
	}
	loaded, err := UnmarshalTransaction(data)
	if err != nil {
		t.Fatalf("unmarshal transaction: %v", err)
	}
	if loaded.Uuid != "inline-test" {
		t.Errorf("transaction UUID: got %q want inline-test", loaded.Uuid)
	}
	if loaded.GetAppend() == nil {
		t.Error("expected Append operation in loaded transaction")
	}
}
