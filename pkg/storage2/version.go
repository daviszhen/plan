package storage2

import (
	"fmt"
	"path/filepath"
	"strconv"
)

const (
	// VersionsDir is the directory under dataset root for manifest files.
	VersionsDir = "_versions"
	// TransactionsDir is the directory under dataset root for transaction files.
	TransactionsDir      = "_transactions"
	ManifestExtension    = "manifest"
	TransactionExtension = "txn"
)

// ManifestPath returns the relative path for a version's manifest (e.g. _versions/1.manifest).
func ManifestPath(version uint64) string {
	return filepath.Join(VersionsDir, fmt.Sprintf("%d.%s", version, ManifestExtension))
}

// TransactionPath returns the relative path for a transaction file.
func TransactionPath(readVersion uint64, uuid string) string {
	return filepath.Join(TransactionsDir, fmt.Sprintf("%d-%s.%s", readVersion, uuid, TransactionExtension))
}

// ParseVersion parses the version from a manifest filename (e.g. 1.manifest -> 1).
func ParseVersion(filename string) (uint64, error) {
	ext := "." + ManifestExtension
	if len(filename) <= len(ext) || filename[len(filename)-len(ext):] != ext {
		return 0, fmt.Errorf("invalid manifest filename: %s", filename)
	}
	n := filename[:len(filename)-len(ext)]
	v, err := strconv.ParseUint(n, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse manifest version: %w", err)
	}
	return v, nil
}
