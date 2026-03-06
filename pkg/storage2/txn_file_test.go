package storage2

import (
	"os"
	"path/filepath"
	"testing"
)

func TestWriteTransactionFile(t *testing.T) {
	dir := t.TempDir()
	txn := NewTransactionAppend(1, "test-uuid", nil)
	if err := WriteTransactionFile(dir, txn); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(dir, TransactionsDir, "1-test-uuid.txn"))
	if err != nil {
		t.Fatal(err)
	}
	txn2, err := UnmarshalTransaction(data)
	if err != nil || txn2.ReadVersion != 1 || txn2.Uuid != "test-uuid" {
		t.Errorf("round trip: %v", err)
	}
}

func TestParseTransactionFilename(t *testing.T) {
	r, u, err := ParseTransactionFilename("5-abc-def.txn")
	if err != nil || r != 5 || u != "abc-def" {
		t.Errorf("ParseTransactionFilename: r=%d u=%s err=%v", r, u, err)
	}
	_, _, err = ParseTransactionFilename("x.txn")
	if err == nil {
		t.Error("expected error for invalid version")
	}
}
