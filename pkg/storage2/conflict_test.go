package storage2

import "testing"

func TestCheckConflictAppendAppend(t *testing.T) {
	a := NewTransactionAppend(1, "a", nil)
	b := NewTransactionAppend(1, "b", nil)
	if CheckConflict(a, b, 2) {
		t.Error("Append vs Append should be compatible")
	}
}

func TestCheckConflictAppendOverwrite(t *testing.T) {
	a := NewTransactionAppend(1, "a", nil)
	b := NewTransactionOverwrite(1, "b", nil, nil, nil)
	if !CheckConflict(a, b, 2) {
		t.Error("Append vs Overwrite should conflict")
	}
}

func TestRebase(t *testing.T) {
	txn := NewTransactionAppend(1, "u", nil)
	rebased := Rebase(txn, 3)
	if rebased.ReadVersion != 3 {
		t.Errorf("read_version: got %d want 3", rebased.ReadVersion)
	}
}
