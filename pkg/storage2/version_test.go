package storage2

import (
	"path/filepath"
	"testing"
)

func TestManifestPath(t *testing.T) {
	p := ManifestPath(1)
	if p != filepath.Join(VersionsDir, "1.manifest") {
		t.Errorf("ManifestPath(1) = %s", p)
	}
}

func TestTransactionPath(t *testing.T) {
	p := TransactionPath(1, "abc-def")
	if p != filepath.Join(TransactionsDir, "1-abc-def.txn") {
		t.Errorf("TransactionPath = %s", p)
	}
}

func TestParseVersion(t *testing.T) {
	v, err := ParseVersion("3.manifest")
	if err != nil || v != 3 {
		t.Errorf("ParseVersion(3.manifest) = %d, %v", v, err)
	}
	_, err = ParseVersion("x.manifest")
	if err == nil {
		t.Fatal("expected error for invalid version")
	}
	_, err = ParseVersion("1.txt")
	if err == nil {
		t.Fatal("expected error for wrong extension")
	}
}
