package storage2

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

// TestLoadTransactionsAfter lists committed transactions after a version (Lance testReadTransaction).
func TestLoadTransactionsAfter(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	handler := NewLocalRenameCommitHandler()

	m0 := NewManifest(0)
	m0.Fragments = []*DataFragment{}
	if err := handler.Commit(ctx, dir, 0, m0); err != nil {
		t.Fatal(err)
	}
	if err := CommitTransaction(ctx, dir, handler, NewTransactionAppend(0, "tx1", []*DataFragment{
		NewDataFragment(0, []*DataFile{NewDataFile("a.dat", []int32{0}, 1, 0)}),
	})); err != nil {
		t.Fatal(err)
	}
	if err := CommitTransaction(ctx, dir, handler, NewTransactionAppend(1, "tx2", []*DataFragment{
		NewDataFragment(1, []*DataFile{NewDataFile("b.dat", []int32{0}, 1, 0)}),
	})); err != nil {
		t.Fatal(err)
	}

	list, err := LoadTransactionsAfter(ctx, dir, handler, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 2 {
		t.Fatalf("LoadTransactionsAfter(0): got %d commits want 2", len(list))
	}
	if list[0].Version != 1 || list[1].Version != 2 {
		t.Errorf("versions: got %d %d want 1 2", list[0].Version, list[1].Version)
	}
	// After version 1 there should be one transaction (v2).
	list1, _ := LoadTransactionsAfter(ctx, dir, handler, 1)
	if len(list1) != 1 || list1[0].Version != 2 {
		t.Errorf("LoadTransactionsAfter(1): got %v", list1)
	}
}

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
