package storage2

import (
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
)

const (
	// VersionsDir is the directory under dataset root for manifest files.
	VersionsDir = "_versions"
	// TransactionsDir is the directory under dataset root for transaction files.
	TransactionsDir      = "_transactions"
	ManifestExtension    = "manifest"
	TransactionExtension = "txn"
)

// ManifestPath returns the relative path for a version's manifest using V1 naming (e.g. _versions/1.manifest).
func ManifestPath(version uint64) string {
	return ManifestPathV1(version)
}

// ManifestPathV1 returns the V1 manifest path (e.g. _versions/42.manifest).
func ManifestPathV1(version uint64) string {
	return filepath.Join(VersionsDir, fmt.Sprintf("%d.%s", version, ManifestExtension))
}

// TransactionPath returns the relative path for a transaction file.
func TransactionPath(readVersion uint64, uuid string) string {
	return filepath.Join(TransactionsDir, fmt.Sprintf("%d-%s.%s", readVersion, uuid, TransactionExtension))
}

// ParseVersion parses the version from a manifest filename (e.g. 1.manifest -> 1).
func ParseVersion(filename string) (uint64, error) {
	v, _, err := ParseVersionEx(filename)
	return v, err
}

// ParseVersionEx parses the version and naming scheme from a manifest filename.
// It auto-detects V1 ("{version}.manifest") and V2 ("%020d.manifest" inverted) formats.
func ParseVersionEx(filename string) (uint64, ManifestNamingScheme, error) {
	ext := "." + ManifestExtension
	if len(filename) <= len(ext) || !strings.HasSuffix(filename, ext) {
		return 0, ManifestNamingV1, fmt.Errorf("invalid manifest filename: %s", filename)
	}
	n := filename[:len(filename)-len(ext)]

	// V2 uses exactly 20-digit zero-padded format with inverted version numbers
	if len(n) == ManifestV2Width {
		inverted, err := strconv.ParseUint(n, 10, 64)
		if err != nil {
			return 0, ManifestNamingV2, fmt.Errorf("parse V2 manifest version: %w", err)
		}
		return ManifestNamingV2Max - inverted, ManifestNamingV2, nil
	}

	// V1 uses simple version numbers
	v, err := strconv.ParseUint(n, 10, 64)
	if err != nil {
		return 0, ManifestNamingV1, fmt.Errorf("parse manifest version: %w", err)
	}
	return v, ManifestNamingV1, nil
}
