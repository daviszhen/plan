package sdk

import (
	"context"
	"testing"
	"time"

	"github.com/daviszhen/plan/pkg/storage2"
)

func TestDetachedAppend(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	// Create dataset
	ds, err := CreateDataset(ctx, dir).Build()
	if err != nil {
		t.Fatalf("CreateDataset: %v", err)
	}
	defer ds.Close()

	// Create detached append transaction
	txnID, err := ds.CreateDetachedAppend(ctx, nil, nil)
	if err != nil {
		t.Fatalf("CreateDetachedAppend: %v", err)
	}

	if txnID == "" {
		t.Error("expected non-empty transaction ID")
	}

	// Check status
	state, err := ds.GetDetachedStatus(ctx, txnID)
	if err != nil {
		t.Fatalf("GetDetachedStatus: %v", err)
	}

	if state.Status != storage2.DetachedStatusPending {
		t.Errorf("expected status pending, got %q", state.Status)
	}

	// Commit the detached transaction
	version, err := ds.CommitDetached(ctx, txnID)
	if err != nil {
		t.Fatalf("CommitDetached: %v", err)
	}

	if version != 1 {
		t.Errorf("expected version 1, got %d", version)
	}

	// Check status after commit
	state, err = ds.GetDetachedStatus(ctx, txnID)
	if err != nil {
		t.Fatalf("GetDetachedStatus: %v", err)
	}

	if state.Status != storage2.DetachedStatusCommitted {
		t.Errorf("expected status committed, got %q", state.Status)
	}
	if state.ResultVersion == nil || *state.ResultVersion != 1 {
		t.Errorf("expected result version 1, got %v", state.ResultVersion)
	}

	// Verify dataset version updated
	if ds.Version() != 1 {
		t.Errorf("expected dataset version 1, got %d", ds.Version())
	}
}

func TestDetachedDelete(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	// Create dataset
	ds, err := CreateDataset(ctx, dir).Build()
	if err != nil {
		t.Fatalf("CreateDataset: %v", err)
	}
	defer ds.Close()

	// First append some data
	err = ds.Append(ctx, nil)
	if err != nil {
		t.Fatalf("Append: %v", err)
	}

	// Create detached delete transaction
	txnID, err := ds.CreateDetachedDelete(ctx, "id > 10", nil)
	if err != nil {
		t.Fatalf("CreateDetachedDelete: %v", err)
	}

	// Check status
	state, err := ds.GetDetachedStatus(ctx, txnID)
	if err != nil {
		t.Fatalf("GetDetachedStatus: %v", err)
	}

	if state.Status != storage2.DetachedStatusPending {
		t.Errorf("expected status pending, got %q", state.Status)
	}

	// Commit
	version, err := ds.CommitDetached(ctx, txnID)
	if err != nil {
		t.Fatalf("CommitDetached: %v", err)
	}

	if version != 2 {
		t.Errorf("expected version 2, got %d", version)
	}
}

func TestDetachedOverwrite(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	// Create dataset
	ds, err := CreateDataset(ctx, dir).Build()
	if err != nil {
		t.Fatalf("CreateDataset: %v", err)
	}
	defer ds.Close()

	// Create detached overwrite transaction
	txnID, err := ds.CreateDetachedOverwrite(ctx, nil, nil)
	if err != nil {
		t.Fatalf("CreateDetachedOverwrite: %v", err)
	}

	// Commit
	version, err := ds.CommitDetached(ctx, txnID)
	if err != nil {
		t.Fatalf("CommitDetached: %v", err)
	}

	if version != 1 {
		t.Errorf("expected version 1, got %d", version)
	}
}

func TestDetachedWithTimeout(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	// Create dataset
	ds, err := CreateDataset(ctx, dir).Build()
	if err != nil {
		t.Fatalf("CreateDataset: %v", err)
	}
	defer ds.Close()

	// Create detached transaction with short timeout
	timeout := 1 * time.Hour
	txnID, err := ds.CreateDetachedAppend(ctx, nil, &timeout)
	if err != nil {
		t.Fatalf("CreateDetachedAppend: %v", err)
	}

	// Check expiration is set
	state, err := ds.GetDetachedStatus(ctx, txnID)
	if err != nil {
		t.Fatalf("GetDetachedStatus: %v", err)
	}

	if state.ExpiresAt == nil {
		t.Error("expected expires_at to be set")
	}
}

func TestListDetached(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	// Create dataset
	ds, err := CreateDataset(ctx, dir).Build()
	if err != nil {
		t.Fatalf("CreateDataset: %v", err)
	}
	defer ds.Close()

	// Create multiple detached transactions
	id1, _ := ds.CreateDetachedAppend(ctx, nil, nil)
	id2, _ := ds.CreateDetachedAppend(ctx, nil, nil)
	id3, _ := ds.CreateDetachedAppend(ctx, nil, nil)

	// List pending
	pending, err := ds.ListDetached(ctx, storage2.DetachedStatusPending)
	if err != nil {
		t.Fatalf("ListDetached: %v", err)
	}

	if len(pending) != 3 {
		t.Errorf("expected 3 pending transactions, got %d", len(pending))
	}

	// Commit one
	_, err = ds.CommitDetached(ctx, id1)
	if err != nil {
		t.Fatalf("CommitDetached: %v", err)
	}

	// List committed
	committed, err := ds.ListDetached(ctx, storage2.DetachedStatusCommitted)
	if err != nil {
		t.Fatalf("ListDetached: %v", err)
	}

	if len(committed) != 1 {
		t.Errorf("expected 1 committed transaction, got %d", len(committed))
	}

	// List pending again
	pending, err = ds.ListDetached(ctx, storage2.DetachedStatusPending)
	if err != nil {
		t.Fatalf("ListDetached: %v", err)
	}

	if len(pending) != 2 {
		t.Errorf("expected 2 pending transactions, got %d", len(pending))
	}

	// Verify IDs
	pendingIDs := make(map[string]bool)
	for _, s := range pending {
		pendingIDs[s.ID] = true
	}
	if !pendingIDs[id2] || !pendingIDs[id3] {
		t.Error("expected id2 and id3 to be in pending list")
	}
}

func TestCleanupExpiredDetached(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	// Create dataset
	ds, err := CreateDataset(ctx, dir).Build()
	if err != nil {
		t.Fatalf("CreateDataset: %v", err)
	}
	defer ds.Close()

	// Create a transaction with very short timeout
	timeout := 1 * time.Millisecond
	txnID, _ := ds.CreateDetachedAppend(ctx, nil, &timeout)

	// Wait for expiration
	time.Sleep(10 * time.Millisecond)

	// Cleanup
	count, err := ds.CleanupExpiredDetached(ctx)
	if err != nil {
		t.Fatalf("CleanupExpiredDetached: %v", err)
	}

	if count != 1 {
		t.Errorf("expected 1 expired transaction, got %d", count)
	}

	// Check status
	state, err := ds.GetDetachedStatus(ctx, txnID)
	if err != nil {
		t.Fatalf("GetDetachedStatus: %v", err)
	}

	if state.Status != storage2.DetachedStatusExpired {
		t.Errorf("expected status expired, got %q", state.Status)
	}

	// Try to commit expired transaction
	_, err = ds.CommitDetached(ctx, txnID)
	if err == nil {
		t.Error("expected error for expired transaction")
	}
}

func TestDeleteDetached(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	// Create dataset
	ds, err := CreateDataset(ctx, dir).Build()
	if err != nil {
		t.Fatalf("CreateDataset: %v", err)
	}
	defer ds.Close()

	// Create detached transaction
	txnID, _ := ds.CreateDetachedAppend(ctx, nil, nil)

	// Verify it exists
	state, _ := ds.GetDetachedStatus(ctx, txnID)
	if state == nil {
		t.Fatal("transaction should exist")
	}

	// Delete it
	err = ds.DeleteDetached(txnID)
	if err != nil {
		t.Fatalf("DeleteDetached: %v", err)
	}

	// Verify it's gone
	state, _ = ds.GetDetachedStatus(ctx, txnID)
	if state != nil {
		t.Error("transaction should be deleted")
	}
}

func TestDetachedTransactionWorkflow(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	// Create dataset
	ds, err := CreateDataset(ctx, dir).Build()
	if err != nil {
		t.Fatalf("CreateDataset: %v", err)
	}
	defer ds.Close()

	// Simulate a long-running workflow:
	// 1. Create detached transaction
	// 2. Check status (pending)
	// 3. Commit later (possibly from another process)
	// 4. Check status (committed)

	// Step 1: Create
	txnID, err := ds.CreateDetachedAppend(ctx, nil, nil)
	if err != nil {
		t.Fatalf("CreateDetachedAppend: %v", err)
	}

	// Step 2: Check status
	state, _ := ds.GetDetachedStatus(ctx, txnID)
	if state.Status != storage2.DetachedStatusPending {
		t.Errorf("expected pending, got %q", state.Status)
	}

	// Step 3: Commit
	version, err := ds.CommitDetached(ctx, txnID)
	if err != nil {
		t.Fatalf("CommitDetached: %v", err)
	}

	// Step 4: Check status
	state, _ = ds.GetDetachedStatus(ctx, txnID)
	if state.Status != storage2.DetachedStatusCommitted {
		t.Errorf("expected committed, got %q", state.Status)
	}
	if state.ResultVersion == nil || *state.ResultVersion != version {
		t.Errorf("result version mismatch")
	}

	// Step 5: Verify dataset state
	if ds.Version() != version {
		t.Errorf("dataset version should be %d, got %d", version, ds.Version())
	}
}
