package storage2

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// WriteTransactionFile writes the transaction to _transactions/{read_version}-{uuid}.txn.
func WriteTransactionFile(basePath string, txn *Transaction) error {
	if txn == nil {
		return fmt.Errorf("transaction is nil")
	}
	data, err := MarshalTransaction(txn)
	if err != nil {
		return err
	}
	dir := filepath.Join(basePath, TransactionsDir)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	name := fmt.Sprintf("%d-%s.%s", txn.ReadVersion, txn.Uuid, TransactionExtension)
	fullPath := filepath.Join(dir, name)
	return os.WriteFile(fullPath, data, 0644)
}

// CommittedTransaction pairs a committed version with its transaction.
type CommittedTransaction struct {
	Version     uint64
	Transaction *Transaction
}

// LoadTransactionsAfter returns transactions committed after readVersion (i.e. that produced versions readVersion+1, readVersion+2, ...).
// It loads each manifest in that range and reads the transaction file referenced by manifest.TransactionFile.
func LoadTransactionsAfter(ctx context.Context, basePath string, handler CommitHandler, readVersion uint64) ([]CommittedTransaction, error) {
	latest, err := handler.ResolveLatestVersion(ctx, basePath)
	if err != nil {
		return nil, err
	}
	if latest <= readVersion {
		return nil, nil
	}
	var out []CommittedTransaction
	for v := readVersion + 1; v <= latest; v++ {
		m, err := LoadManifest(ctx, basePath, handler, v)
		if err != nil {
			return nil, fmt.Errorf("load manifest v%d: %w", v, err)
		}
		txnFile := m.TransactionFile
		if txnFile == "" {
			continue
		}
		txnPath := filepath.Join(basePath, TransactionsDir, txnFile)
		data, err := os.ReadFile(txnPath)
		if err != nil {
			return nil, fmt.Errorf("read txn file %s: %w", txnPath, err)
		}
		txn, err := UnmarshalTransaction(data)
		if err != nil {
			return nil, fmt.Errorf("unmarshal txn v%d: %w", v, err)
		}
		out = append(out, CommittedTransaction{Version: v, Transaction: txn})
	}
	return out, nil
}

// LoadManifest reads and unmarshals the manifest for the given version.
func LoadManifest(ctx context.Context, basePath string, handler CommitHandler, version uint64) (*Manifest, error) {
	relPath, err := handler.ResolveVersion(ctx, basePath, version)
	if err != nil {
		return nil, err
	}
	fullPath := filepath.Join(basePath, relPath)
	data, err := os.ReadFile(fullPath)
	if err != nil {
		return nil, err
	}
	return UnmarshalManifest(data)
}

// ParseTransactionFilename parses "read_version-uuid.txn" into (readVersion, uuid).
// Returns error if the filename does not match.
func ParseTransactionFilename(filename string) (readVersion uint64, uuid string, err error) {
	ext := "." + TransactionExtension
	if !strings.HasSuffix(filename, ext) {
		return 0, "", fmt.Errorf("not a txn file: %s", filename)
	}
	base := filename[:len(filename)-len(ext)]
	idx := strings.Index(base, "-")
	if idx <= 0 {
		return 0, "", fmt.Errorf("invalid txn filename: %s", filename)
	}
	readVersion, err = strconv.ParseUint(base[:idx], 10, 64)
	if err != nil {
		return 0, "", err
	}
	uuid = base[idx+1:]
	return readVersion, uuid, nil
}
