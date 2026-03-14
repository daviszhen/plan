package storage2

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestCreateDetachedTransaction(t *testing.T) {
	dir := t.TempDir()
	handler := NewLocalRenameCommitHandler()

	// Create initial dataset
	m0 := NewManifest(0)
	if err := handler.Commit(context.Background(), dir, 0, m0); err != nil {
		t.Fatalf("create dataset: %v", err)
	}

	// Create a transaction
	txn := NewTransactionAppend(0, "test-uuid-1", nil)

	// Create detached transaction
	id, err := CreateDetachedTransaction(context.Background(), dir, handler, txn, nil)
	if err != nil {
		t.Fatalf("CreateDetachedTransaction: %v", err)
	}

	if id != "test-uuid-1" {
		t.Errorf("expected id test-uuid-1, got %q", id)
	}

	// Verify state file exists
	state, err := loadDetachedState(dir, id)
	if err != nil {
		t.Fatalf("loadDetachedState: %v", err)
	}
	if state == nil {
		t.Fatal("state should not be nil")
	}

	if state.Status != DetachedStatusPending {
		t.Errorf("expected status pending, got %q", state.Status)
	}
	if state.ID != id {
		t.Errorf("expected id %q, got %q", id, state.ID)
	}
	if state.Transaction == nil {
		t.Error("transaction should not be nil")
	}
	if state.ExpiresAt == nil {
		t.Error("expires_at should be set")
	}
}

func TestCreateDetachedTransactionWithTimeout(t *testing.T) {
	dir := t.TempDir()
	handler := NewLocalRenameCommitHandler()

	// Create initial dataset
	m0 := NewManifest(0)
	if err := handler.Commit(context.Background(), dir, 0, m0); err != nil {
		t.Fatalf("create dataset: %v", err)
	}

	// Create a transaction
	txn := NewTransactionAppend(0, "test-uuid-2", nil)

	// Create detached transaction with custom timeout
	opts := &DetachedTransactionOptions{
		Timeout: 1 * time.Hour,
	}
	id, err := CreateDetachedTransaction(context.Background(), dir, handler, txn, opts)
	if err != nil {
		t.Fatalf("CreateDetachedTransaction: %v", err)
	}

	state, err := loadDetachedState(dir, id)
	if err != nil {
		t.Fatalf("loadDetachedState: %v", err)
	}

	// Check that expiration is approximately 1 hour from now
	expectedExpiry := time.Now().Add(1 * time.Hour)
	diff := state.ExpiresAt.Sub(expectedExpiry)
	if diff < -time.Minute || diff > time.Minute {
		t.Errorf("expiration time should be ~1 hour from now, got %v", state.ExpiresAt)
	}
}

func TestCommitDetachedTransaction(t *testing.T) {
	dir := t.TempDir()
	handler := NewLocalRenameCommitHandler()

	// Create initial dataset
	m0 := NewManifest(0)
	if err := handler.Commit(context.Background(), dir, 0, m0); err != nil {
		t.Fatalf("create dataset: %v", err)
	}

	// Create a transaction
	txn := NewTransactionAppend(0, "test-uuid-3", nil)

	// Create detached transaction
	id, err := CreateDetachedTransaction(context.Background(), dir, handler, txn, nil)
	if err != nil {
		t.Fatalf("CreateDetachedTransaction: %v", err)
	}

	// Commit the detached transaction
	version, err := CommitDetachedTransaction(context.Background(), dir, handler, id)
	if err != nil {
		t.Fatalf("CommitDetachedTransaction: %v", err)
	}

	if version != 1 {
		t.Errorf("expected version 1, got %d", version)
	}

	// Check status
	state, err := loadDetachedState(dir, id)
	if err != nil {
		t.Fatalf("loadDetachedState: %v", err)
	}

	if state.Status != DetachedStatusCommitted {
		t.Errorf("expected status committed, got %q", state.Status)
	}
	if state.ResultVersion == nil || *state.ResultVersion != 1 {
		t.Errorf("expected result version 1, got %v", state.ResultVersion)
	}
}

func TestCommitDetachedTransactionNotFound(t *testing.T) {
	dir := t.TempDir()
	handler := NewLocalRenameCommitHandler()

	// Try to commit a non-existent transaction
	_, err := CommitDetachedTransaction(context.Background(), dir, handler, "non-existent")
	if err == nil {
		t.Error("expected error for non-existent transaction")
	}
}

func TestCommitDetachedTransactionAlreadyCommitted(t *testing.T) {
	dir := t.TempDir()
	handler := NewLocalRenameCommitHandler()

	// Create initial dataset
	m0 := NewManifest(0)
	if err := handler.Commit(context.Background(), dir, 0, m0); err != nil {
		t.Fatalf("create dataset: %v", err)
	}

	// Create and commit a transaction
	txn := NewTransactionAppend(0, "test-uuid-4", nil)
	id, _ := CreateDetachedTransaction(context.Background(), dir, handler, txn, nil)
	_, err := CommitDetachedTransaction(context.Background(), dir, handler, id)
	if err != nil {
		t.Fatalf("first commit: %v", err)
	}

	// Try to commit again
	version, err := CommitDetachedTransaction(context.Background(), dir, handler, id)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if version != 1 {
		t.Errorf("expected version 1, got %d", version)
	}
}

func TestGetDetachedTransactionStatus(t *testing.T) {
	dir := t.TempDir()
	handler := NewLocalRenameCommitHandler()

	// Create initial dataset
	m0 := NewManifest(0)
	if err := handler.Commit(context.Background(), dir, 0, m0); err != nil {
		t.Fatalf("create dataset: %v", err)
	}

	// Create a transaction
	txn := NewTransactionAppend(0, "test-uuid-5", nil)
	id, _ := CreateDetachedTransaction(context.Background(), dir, handler, txn, nil)

	// Get status
	state, err := GetDetachedTransactionStatus(context.Background(), dir, id)
	if err != nil {
		t.Fatalf("GetDetachedTransactionStatus: %v", err)
	}

	if state.Status != DetachedStatusPending {
		t.Errorf("expected status pending, got %q", state.Status)
	}
}

func TestListDetachedTransactions(t *testing.T) {
	dir := t.TempDir()
	handler := NewLocalRenameCommitHandler()

	// Create initial dataset
	m0 := NewManifest(0)
	if err := handler.Commit(context.Background(), dir, 0, m0); err != nil {
		t.Fatalf("create dataset: %v", err)
	}

	// Create multiple transactions
	for i := 0; i < 3; i++ {
		txn := NewTransactionAppend(0, "test-uuid-"+string(rune('a'+i)), nil)
		_, err := CreateDetachedTransaction(context.Background(), dir, handler, txn, nil)
		if err != nil {
			t.Fatalf("CreateDetachedTransaction %d: %v", i, err)
		}
	}

	// List all pending transactions
	pending, err := ListDetachedTransactions(context.Background(), dir, DetachedStatusPending)
	if err != nil {
		t.Fatalf("ListDetachedTransactions: %v", err)
	}

	if len(pending) != 3 {
		t.Errorf("expected 3 pending transactions, got %d", len(pending))
	}

	// Commit one
	_, err = CommitDetachedTransaction(context.Background(), dir, handler, "test-uuid-a")
	if err != nil {
		t.Fatalf("CommitDetachedTransaction: %v", err)
	}

	// List committed
	committed, err := ListDetachedTransactions(context.Background(), dir, DetachedStatusCommitted)
	if err != nil {
		t.Fatalf("ListDetachedTransactions: %v", err)
	}

	if len(committed) != 1 {
		t.Errorf("expected 1 committed transaction, got %d", len(committed))
	}

	// List pending again
	pending, err = ListDetachedTransactions(context.Background(), dir, DetachedStatusPending)
	if err != nil {
		t.Fatalf("ListDetachedTransactions: %v", err)
	}

	if len(pending) != 2 {
		t.Errorf("expected 2 pending transactions, got %d", len(pending))
	}
}

func TestCleanupExpiredDetachedTransactions(t *testing.T) {
	dir := t.TempDir()
	handler := NewLocalRenameCommitHandler()

	// Create initial dataset
	m0 := NewManifest(0)
	if err := handler.Commit(context.Background(), dir, 0, m0); err != nil {
		t.Fatalf("create dataset: %v", err)
	}

	// Create a transaction with very short timeout
	txn := NewTransactionAppend(0, "test-uuid-expired", nil)
	opts := &DetachedTransactionOptions{
		Timeout: 1 * time.Millisecond,
	}
	id, err := CreateDetachedTransaction(context.Background(), dir, handler, txn, opts)
	if err != nil {
		t.Fatalf("CreateDetachedTransaction: %v", err)
	}

	// Wait for expiration
	time.Sleep(10 * time.Millisecond)

	// Cleanup expired transactions
	count, err := CleanupExpiredDetachedTransactions(context.Background(), dir)
	if err != nil {
		t.Fatalf("CleanupExpiredDetachedTransactions: %v", err)
	}

	if count != 1 {
		t.Errorf("expected 1 expired transaction, got %d", count)
	}

	// Check status
	state, err := loadDetachedState(dir, id)
	if err != nil {
		t.Fatalf("loadDetachedState: %v", err)
	}

	if state.Status != DetachedStatusExpired {
		t.Errorf("expected status expired, got %q", state.Status)
	}

	// Try to commit expired transaction
	_, err = CommitDetachedTransaction(context.Background(), dir, handler, id)
	if err == nil {
		t.Error("expected error for expired transaction")
	}
}

func TestDeleteDetachedTransaction(t *testing.T) {
	dir := t.TempDir()
	handler := NewLocalRenameCommitHandler()

	// Create initial dataset
	m0 := NewManifest(0)
	if err := handler.Commit(context.Background(), dir, 0, m0); err != nil {
		t.Fatalf("create dataset: %v", err)
	}

	// Create a transaction
	txn := NewTransactionAppend(0, "test-uuid-delete", nil)
	id, _ := CreateDetachedTransaction(context.Background(), dir, handler, txn, nil)

	// Verify it exists
	state, _ := loadDetachedState(dir, id)
	if state == nil {
		t.Fatal("transaction should exist")
	}

	// Delete it
	err := DeleteDetachedTransaction(dir, id)
	if err != nil {
		t.Fatalf("DeleteDetachedTransaction: %v", err)
	}

	// Verify it's gone
	state, _ = loadDetachedState(dir, id)
	if state != nil {
		t.Error("transaction should be deleted")
	}
}

func TestDetachedTransactionFiles(t *testing.T) {
	dir := t.TempDir()
	handler := NewLocalRenameCommitHandler()

	// Create initial dataset
	m0 := NewManifest(0)
	if err := handler.Commit(context.Background(), dir, 0, m0); err != nil {
		t.Fatalf("create dataset: %v", err)
	}

	// Create a transaction
	txn := NewTransactionAppend(0, "test-uuid-files", nil)

	// Create detached transaction
	id, err := CreateDetachedTransaction(context.Background(), dir, handler, txn, nil)
	if err != nil {
		t.Fatalf("CreateDetachedTransaction: %v", err)
	}

	// Verify detached state file exists
	detachedPath := filepath.Join(dir, DetachedDir, id+"."+DetachedExtension)
	if _, err := os.Stat(detachedPath); os.IsNotExist(err) {
		t.Error("detached state file should exist")
	}

	// Verify transaction file exists (for compatibility)
	txnPath := filepath.Join(dir, TransactionsDir, "0-"+id+".txn")
	if _, err := os.Stat(txnPath); os.IsNotExist(err) {
		t.Error("transaction file should exist")
	}
}
