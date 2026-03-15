package storage2

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// Detached transaction state constants
const (
	// DetachedDir is the directory for detached transaction files
	DetachedDir = "_detached"
	// DetachedExtension is the file extension for detached transaction state
	DetachedExtension = "dtxn"
	// DefaultDetachedTimeout is the default timeout for detached transactions (24 hours)
	DefaultDetachedTimeout = 24 * time.Hour
)

// DetachedTransactionState represents the state of a detached transaction
type DetachedTransactionState struct {
	// ID is the unique identifier for the detached transaction
	ID string `json:"id"`
	// Transaction is the underlying transaction
	Transaction *Transaction `json:"transaction"`
	// Status is the current state of the transaction
	Status DetachedTransactionStatus `json:"status"`
	// CreatedAt is when the transaction was created
	CreatedAt time.Time `json:"created_at"`
	// UpdatedAt is when the transaction was last updated
	UpdatedAt time.Time `json:"updated_at"`
	// ExpiresAt is when the transaction will expire (optional)
	ExpiresAt *time.Time `json:"expires_at,omitempty"`
	// ResultVersion is the version created by this transaction (after commit)
	ResultVersion *uint64 `json:"result_version,omitempty"`
	// Error is the error message if the transaction failed
	Error string `json:"error,omitempty"`
}

// DetachedTransactionStatus represents the status of a detached transaction
type DetachedTransactionStatus string

const (
	// DetachedStatusPending means the transaction is created but not yet committed
	DetachedStatusPending DetachedTransactionStatus = "pending"
	// DetachedStatusCommitting means the transaction is being committed
	DetachedStatusCommitting DetachedTransactionStatus = "committing"
	// DetachedStatusCommitted means the transaction was successfully committed
	DetachedStatusCommitted DetachedTransactionStatus = "committed"
	// DetachedStatusFailed means the transaction failed to commit
	DetachedStatusFailed DetachedTransactionStatus = "failed"
	// DetachedStatusExpired means the transaction expired without being committed
	DetachedStatusExpired DetachedTransactionStatus = "expired"
)

// DetachedTransactionOptions contains options for creating a detached transaction
type DetachedTransactionOptions struct {
	// Timeout is the duration after which the transaction will expire
	Timeout time.Duration
}

// CreateDetachedTransaction creates a detached transaction that can be committed later.
// The transaction is stored persistently and can be committed by ID.
// Returns the transaction ID that can be used to commit or check status.
func CreateDetachedTransaction(ctx context.Context, basePath string, handler CommitHandler, txn *Transaction, opts *DetachedTransactionOptions) (string, error) {
	if txn == nil {
		return "", fmt.Errorf("transaction is nil")
	}
	if txn.Uuid == "" {
		return "", fmt.Errorf("transaction UUID is required")
	}

	// Use transaction UUID as the detached transaction ID
	id := txn.Uuid

	// Set default timeout
	timeout := DefaultDetachedTimeout
	if opts != nil && opts.Timeout > 0 {
		timeout = opts.Timeout
	}

	now := time.Now()
	expiresAt := now.Add(timeout)

	state := &DetachedTransactionState{
		ID:          id,
		Transaction: txn,
		Status:      DetachedStatusPending,
		CreatedAt:   now,
		UpdatedAt:   now,
		ExpiresAt:   &expiresAt,
	}

	// Write the detached transaction state
	if err := writeDetachedState(basePath, state); err != nil {
		return "", fmt.Errorf("write detached state: %w", err)
	}

	// Also write the transaction file (for compatibility with existing commit logic)
	if err := WriteTransactionFile(basePath, txn); err != nil {
		// Clean up the detached state file
		_ = deleteDetachedState(basePath, id)
		return "", fmt.Errorf("write transaction file: %w", err)
	}

	return id, nil
}

// CommitDetachedTransaction commits a detached transaction by ID.
// Returns the resulting version number on success.
func CommitDetachedTransaction(ctx context.Context, basePath string, handler CommitHandler, id string) (uint64, error) {
	// Load the detached transaction state
	state, err := loadDetachedState(basePath, id)
	if err != nil {
		return 0, fmt.Errorf("load detached state: %w", err)
	}

	// Check if transaction exists
	if state == nil {
		return 0, fmt.Errorf("detached transaction %q not found", id)
	}

	// Check current status
	switch state.Status {
	case DetachedStatusCommitted:
		if state.ResultVersion != nil {
			return *state.ResultVersion, nil
		}
		return 0, fmt.Errorf("transaction already committed but version is unknown")
	case DetachedStatusCommitting:
		return 0, fmt.Errorf("transaction is already being committed")
	case DetachedStatusFailed:
		return 0, fmt.Errorf("transaction failed: %s", state.Error)
	case DetachedStatusExpired:
		return 0, fmt.Errorf("transaction has expired")
	}

	// Check expiration
	if state.ExpiresAt != nil && time.Now().After(*state.ExpiresAt) {
		// Mark as expired
		state.Status = DetachedStatusExpired
		state.UpdatedAt = time.Now()
		_ = writeDetachedState(basePath, state)
		return 0, fmt.Errorf("transaction has expired")
	}

	// Update status to committing
	state.Status = DetachedStatusCommitting
	state.UpdatedAt = time.Now()
	if err := writeDetachedState(basePath, state); err != nil {
		return 0, fmt.Errorf("update state to committing: %w", err)
	}

	// Commit the transaction
	err = CommitTransaction(ctx, basePath, handler, state.Transaction)
	if err != nil {
		// Update status to failed
		state.Status = DetachedStatusFailed
		state.Error = err.Error()
		state.UpdatedAt = time.Now()
		_ = writeDetachedState(basePath, state)
		return 0, fmt.Errorf("commit transaction: %w", err)
	}

	// Get the resulting version
	version, err := handler.ResolveLatestVersion(ctx, basePath)
	if err != nil {
		// Transaction committed but we couldn't get the version
		// Still mark as committed but without version
		state.Status = DetachedStatusCommitted
		state.UpdatedAt = time.Now()
		_ = writeDetachedState(basePath, state)
		return 0, fmt.Errorf("get latest version after commit: %w", err)
	}

	// Update status to committed
	state.Status = DetachedStatusCommitted
	state.ResultVersion = &version
	state.UpdatedAt = time.Now()
	if err := writeDetachedState(basePath, state); err != nil {
		// Transaction is committed, just log the error
		// The state file might be inconsistent but the transaction is done
	}

	return version, nil
}

// GetDetachedTransactionStatus returns the status of a detached transaction.
// Returns nil if the transaction is not found.
func GetDetachedTransactionStatus(ctx context.Context, basePath string, id string) (*DetachedTransactionState, error) {
	state, err := loadDetachedState(basePath, id)
	if err != nil {
		return nil, fmt.Errorf("load detached state: %w", err)
	}
	return state, nil
}

// ListDetachedTransactions lists all detached transactions with the given status.
// If status is empty, all transactions are listed.
func ListDetachedTransactions(ctx context.Context, basePath string, status DetachedTransactionStatus) ([]*DetachedTransactionState, error) {
	dir := filepath.Join(basePath, DetachedDir)
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	var results []*DetachedTransactionState
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		ext := "." + DetachedExtension
		if len(entry.Name()) <= len(ext) || entry.Name()[len(entry.Name())-len(ext):] != ext {
			continue
		}

		state, err := loadDetachedState(basePath, entry.Name()[:len(entry.Name())-len(ext)])
		if err != nil {
			continue
		}
		if state == nil {
			continue
		}

		if status == "" || state.Status == status {
			results = append(results, state)
		}
	}

	return results, nil
}

// CleanupExpiredDetachedTransactions removes expired detached transactions.
// Returns the number of transactions cleaned up.
func CleanupExpiredDetachedTransactions(ctx context.Context, basePath string) (int, error) {
	// List all pending transactions
	pending, err := ListDetachedTransactions(ctx, basePath, DetachedStatusPending)
	if err != nil {
		return 0, fmt.Errorf("list pending transactions: %w", err)
	}

	count := 0
	now := time.Now()
	for _, state := range pending {
		if state.ExpiresAt != nil && now.After(*state.ExpiresAt) {
			// Mark as expired
			state.Status = DetachedStatusExpired
			state.UpdatedAt = now
			if err := writeDetachedState(basePath, state); err != nil {
				continue
			}
			count++
		}
	}

	return count, nil
}

// DeleteDetachedTransaction deletes a detached transaction state file.
// This should only be called for transactions that are no longer needed.
func DeleteDetachedTransaction(basePath string, id string) error {
	return deleteDetachedState(basePath, id)
}

// Helper functions

func detachedStatePath(basePath, id string) string {
	return filepath.Join(basePath, DetachedDir, id+"."+DetachedExtension)
}

func writeDetachedState(basePath string, state *DetachedTransactionState) error {
	if state == nil {
		return fmt.Errorf("state is nil")
	}

	// Marshal the transaction separately
	txnData, err := MarshalTransaction(state.Transaction)
	if err != nil {
		return fmt.Errorf("marshal transaction: %w", err)
	}

	// Create a JSON structure that includes the transaction as base64
	jsonState := struct {
		ID            string                    `json:"id"`
		Transaction   []byte                    `json:"transaction"`
		Status        DetachedTransactionStatus `json:"status"`
		CreatedAt     time.Time                 `json:"created_at"`
		UpdatedAt     time.Time                 `json:"updated_at"`
		ExpiresAt     *time.Time                `json:"expires_at,omitempty"`
		ResultVersion *uint64                   `json:"result_version,omitempty"`
		Error         string                    `json:"error,omitempty"`
	}{
		ID:            state.ID,
		Transaction:   txnData,
		Status:        state.Status,
		CreatedAt:     state.CreatedAt,
		UpdatedAt:     state.UpdatedAt,
		ExpiresAt:     state.ExpiresAt,
		ResultVersion: state.ResultVersion,
		Error:         state.Error,
	}

	data, err := json.MarshalIndent(jsonState, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal state: %w", err)
	}

	dir := filepath.Join(basePath, DetachedDir)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	path := detachedStatePath(basePath, state.ID)
	return os.WriteFile(path, data, 0644)
}

func loadDetachedState(basePath, id string) (*DetachedTransactionState, error) {
	path := detachedStatePath(basePath, id)
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	var jsonState struct {
		ID            string                    `json:"id"`
		Transaction   []byte                    `json:"transaction"`
		Status        DetachedTransactionStatus `json:"status"`
		CreatedAt     time.Time                 `json:"created_at"`
		UpdatedAt     time.Time                 `json:"updated_at"`
		ExpiresAt     *time.Time                `json:"expires_at,omitempty"`
		ResultVersion *uint64                   `json:"result_version,omitempty"`
		Error         string                    `json:"error,omitempty"`
	}

	if err := json.Unmarshal(data, &jsonState); err != nil {
		return nil, fmt.Errorf("unmarshal state: %w", err)
	}

	txn, err := UnmarshalTransaction(jsonState.Transaction)
	if err != nil {
		return nil, fmt.Errorf("unmarshal transaction: %w", err)
	}

	return &DetachedTransactionState{
		ID:            jsonState.ID,
		Transaction:   txn,
		Status:        jsonState.Status,
		CreatedAt:     jsonState.CreatedAt,
		UpdatedAt:     jsonState.UpdatedAt,
		ExpiresAt:     jsonState.ExpiresAt,
		ResultVersion: jsonState.ResultVersion,
		Error:         jsonState.Error,
	}, nil
}

func deleteDetachedState(basePath, id string) error {
	path := detachedStatePath(basePath, id)
	err := os.Remove(path)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}
