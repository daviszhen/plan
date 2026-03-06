package storage2

import (
	"context"
	"fmt"
)

// CommitTransaction writes the transaction file, checks for conflicts with already-committed
// transactions, rebases if compatible, builds the next manifest, and commits it.
// Returns ErrConflict if the transaction conflicts with a committed one.
func CommitTransaction(ctx context.Context, basePath string, handler CommitHandler, txn *Transaction) error {
	if txn == nil {
		return fmt.Errorf("transaction is nil")
	}
	if err := WriteTransactionFile(basePath, txn); err != nil {
		return fmt.Errorf("write transaction file: %w", err)
	}

	latest, err := handler.ResolveLatestVersion(ctx, basePath)
	if err != nil {
		return err
	}

	// Load transactions committed after txn.ReadVersion.
	list, err := LoadTransactionsAfter(ctx, basePath, handler, txn.ReadVersion)
	if err != nil {
		return err
	}

	for _, ct := range list {
		if CheckConflict(txn, ct.Transaction, ct.Version) {
			return ErrConflict
		}
	}

	// Rebase to latest: we will build manifest on top of the latest version.
	var latestVersion uint64
	if len(list) > 0 {
		latestVersion = list[len(list)-1].Version
	} else {
		latestVersion = latest
	}

	current, err := LoadManifest(ctx, basePath, handler, latestVersion)
	if err != nil {
		return fmt.Errorf("load manifest v%d: %w", latestVersion, err)
	}

	rebased := Rebase(txn, latestVersion)
	next, err := BuildManifest(current, rebased)
	if err != nil {
		return fmt.Errorf("build manifest: %w", err)
	}
	// Record which transaction produced this version (for LoadTransactionsAfter).
	next.TransactionFile = fmt.Sprintf("%d-%s.%s", rebased.ReadVersion, rebased.Uuid, TransactionExtension)

	return handler.Commit(ctx, basePath, next.Version, next)
}
