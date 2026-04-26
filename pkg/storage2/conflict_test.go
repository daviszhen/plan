package storage2

import "testing"

// Helper constructors for concise test code.
func txnAppend() *Transaction    { return NewTransactionAppend(1, "a", nil) }
func txnDelete() *Transaction    { return NewTransactionDelete(1, "d", nil, nil, "") }
func txnOverwrite() *Transaction { return NewTransactionOverwrite(1, "o", nil, nil, nil) }
func txnProject() *Transaction   { return NewTransactionProject(1, "p", nil) }
func txnMerge() *Transaction     { return NewTransactionMerge(1, "m", nil, nil, nil) }
func txnRewrite() *Transaction   { return NewTransactionRewrite(1, "r", nil, nil) }

// assertConflict is a helper for conflict matrix tests.
// conflict=true means we expect CheckConflict to return true.
func assertConflict(t *testing.T, current, committed *Transaction, conflict bool, desc string) {
	t.Helper()
	got := CheckConflict(current, committed, 2)
	if got != conflict {
		t.Errorf("%s: got conflict=%v, want %v", desc, got, conflict)
	}
}

// ---- Append row ----
func TestConflictMatrix_Append(t *testing.T) {
	assertConflict(t, txnAppend(), txnAppend(), false, "Append vs Append")
	assertConflict(t, txnAppend(), txnDelete(), false, "Append vs Delete")
	assertConflict(t, txnAppend(), txnOverwrite(), true, "Append vs Overwrite")
	assertConflict(t, txnAppend(), txnRewrite(), false, "Append vs Rewrite")
	assertConflict(t, txnAppend(), txnMerge(), true, "Append vs Merge")
	assertConflict(t, txnAppend(), txnProject(), true, "Append vs Project")
}

// ---- Delete row ----
func TestConflictMatrix_Delete(t *testing.T) {
	assertConflict(t, txnDelete(), txnAppend(), false, "Delete vs Append")
	assertConflict(t, txnDelete(), txnOverwrite(), true, "Delete vs Overwrite")
	assertConflict(t, txnDelete(), txnRewrite(), false, "Delete(no frag) vs Rewrite(no frag)")
	assertConflict(t, txnDelete(), txnMerge(), true, "Delete vs Merge")
	assertConflict(t, txnDelete(), txnProject(), true, "Delete vs Project")
}

// ---- Overwrite row ----
func TestConflictMatrix_Overwrite(t *testing.T) {
	assertConflict(t, txnOverwrite(), txnAppend(), false, "Overwrite vs Append")
	assertConflict(t, txnOverwrite(), txnDelete(), false, "Overwrite vs Delete")
	assertConflict(t, txnOverwrite(), txnOverwrite(), false, "Overwrite vs Overwrite")
	assertConflict(t, txnOverwrite(), txnRewrite(), false, "Overwrite vs Rewrite")
	assertConflict(t, txnOverwrite(), txnMerge(), false, "Overwrite vs Merge")
	assertConflict(t, txnOverwrite(), txnProject(), false, "Overwrite vs Project")
}

// ---- CreateIndex row ----
// CreateIndex is compatible with everything except committed Overwrite/Restore.
func TestConflictMatrix_CreateIndex(t *testing.T) {
	ci := NewTransactionAppend(1, "ci", nil) // placeholder — we override OpKind below
	// We can't easily create a CreateIndex txn without proto, so test via CheckConflict internals.
	// Instead, test the helper directly.
	assertConflict(t, txnAppend(), txnAppend(), false, "sanity") // just to prove helper works
	_ = ci
	// Test via checkCreateIndexConflict2 directly.
	if checkCreateIndexConflict2(OpAppend) {
		t.Error("CreateIndex vs Append should be compatible")
	}
	if checkCreateIndexConflict2(OpDelete) {
		t.Error("CreateIndex vs Delete should be compatible")
	}
	if checkCreateIndexConflict2(OpCreateIndex) {
		t.Error("CreateIndex vs CreateIndex should be compatible")
	}
	if checkCreateIndexConflict2(OpRewrite) {
		t.Error("CreateIndex vs Rewrite should be compatible")
	}
	if checkCreateIndexConflict2(OpMerge) {
		t.Error("CreateIndex vs Merge should be compatible")
	}
	if checkCreateIndexConflict2(OpProject) {
		t.Error("CreateIndex vs Project should be compatible")
	}
}

// ---- Rewrite row ----
func TestConflictMatrix_Rewrite(t *testing.T) {
	assertConflict(t, txnRewrite(), txnAppend(), false, "Rewrite vs Append")
	assertConflict(t, txnRewrite(), txnOverwrite(), true, "Rewrite vs Overwrite")
	assertConflict(t, txnRewrite(), txnRewrite(), false, "Rewrite(no frag) vs Rewrite(no frag)")
	assertConflict(t, txnRewrite(), txnMerge(), true, "Rewrite vs Merge")
	assertConflict(t, txnRewrite(), txnProject(), true, "Rewrite vs Project")
}

// ---- Rewrite vs CreateIndex (asymmetric!) ----
func TestConflictMatrix_RewriteVsCreateIndex(t *testing.T) {
	// checkRewriteConflict with otherOp=OpCreateIndex should return true (conflict).
	if !checkRewriteConflict(txnRewrite(), txnRewrite(), OpCreateIndex) {
		t.Error("Rewrite vs CreateIndex(committed) should conflict")
	}
}

// ---- Merge row ----
func TestConflictMatrix_Merge(t *testing.T) {
	assertConflict(t, txnMerge(), txnAppend(), true, "Merge vs Append")
	assertConflict(t, txnMerge(), txnDelete(), true, "Merge vs Delete")
	assertConflict(t, txnMerge(), txnOverwrite(), true, "Merge vs Overwrite")
	assertConflict(t, txnMerge(), txnRewrite(), false, "Merge vs Rewrite")
	assertConflict(t, txnMerge(), txnMerge(), true, "Merge vs Merge")
	assertConflict(t, txnMerge(), txnProject(), true, "Merge vs Project")
}

// ---- Project row ----
func TestConflictMatrix_Project(t *testing.T) {
	assertConflict(t, txnProject(), txnAppend(), false, "Project vs Append")
	assertConflict(t, txnProject(), txnDelete(), false, "Project vs Delete")
	assertConflict(t, txnProject(), txnOverwrite(), true, "Project vs Overwrite")
	assertConflict(t, txnProject(), txnRewrite(), false, "Project vs Rewrite")
	assertConflict(t, txnProject(), txnMerge(), true, "Project vs Merge")
	assertConflict(t, txnProject(), txnProject(), false, "Project vs Project")
}

// ---- Key asymmetric pairs (the bugs that were fixed) ----
func TestConflictMatrix_AsymmetricPairs(t *testing.T) {
	// Project(current) vs Append(committed) = compatible
	assertConflict(t, txnProject(), txnAppend(), false, "Project vs Append(committed)")
	// Append(current) vs Project(committed) = conflict
	assertConflict(t, txnAppend(), txnProject(), true, "Append vs Project(committed)")

	// Project(current) vs Delete(committed) = compatible
	assertConflict(t, txnProject(), txnDelete(), false, "Project vs Delete(committed)")
	// Delete(current) vs Project(committed) = conflict
	assertConflict(t, txnDelete(), txnProject(), true, "Delete vs Project(committed)")

	// Merge(current) vs Rewrite(committed) = compatible
	assertConflict(t, txnMerge(), txnRewrite(), false, "Merge vs Rewrite(committed)")
	// Rewrite(current) vs Merge(committed) = conflict
	assertConflict(t, txnRewrite(), txnMerge(), true, "Rewrite vs Merge(committed)")

	// Project(current) vs Rewrite(committed) = compatible
	assertConflict(t, txnProject(), txnRewrite(), false, "Project vs Rewrite(committed)")
	// Rewrite(current) vs Project(committed) = conflict
	assertConflict(t, txnRewrite(), txnProject(), true, "Rewrite vs Project(committed)")
}

// ---- Fragment overlap tests ----
func TestConflictDeleteDeleteOverlap(t *testing.T) {
	frag := NewDataFragment(1, []*DataFile{NewDataFile("f.parquet", []int32{0}, 10, 0)})
	a := NewTransactionDelete(1, "a", []*DataFragment{frag}, nil, "")
	b := NewTransactionDelete(1, "b", []*DataFragment{frag}, nil, "")
	if !CheckConflict(a, b, 2) {
		t.Error("Delete vs Delete on same fragment should conflict")
	}
}

func TestConflictDeleteDeleteNoOverlap(t *testing.T) {
	fragA := NewDataFragment(1, []*DataFile{NewDataFile("a.parquet", []int32{0}, 10, 0)})
	fragB := NewDataFragment(2, []*DataFile{NewDataFile("b.parquet", []int32{0}, 10, 0)})
	a := NewTransactionDelete(1, "a", []*DataFragment{fragA}, nil, "")
	b := NewTransactionDelete(1, "b", []*DataFragment{fragB}, nil, "")
	if CheckConflict(a, b, 2) {
		t.Error("Delete vs Delete on different fragments should be compatible")
	}
}

func TestConflictDeleteRewriteOverlap(t *testing.T) {
	frag := NewDataFragment(1, []*DataFile{NewDataFile("f.parquet", []int32{0}, 10, 0)})
	del := NewTransactionDelete(1, "d", []*DataFragment{frag}, nil, "")
	rw := NewTransactionRewrite(1, "r", []*DataFragment{frag}, nil)
	if !CheckConflict(del, rw, 2) {
		t.Error("Delete vs Rewrite on same fragment should conflict")
	}
}

func TestConflictRewriteRewriteOverlap(t *testing.T) {
	frag := NewDataFragment(1, []*DataFile{NewDataFile("f.parquet", []int32{0}, 10, 0)})
	a := NewTransactionRewrite(1, "a", []*DataFragment{frag}, nil)
	b := NewTransactionRewrite(1, "b", []*DataFragment{frag}, nil)
	if !CheckConflict(a, b, 2) {
		t.Error("Rewrite vs Rewrite on same fragment should conflict")
	}
}

func TestRebase(t *testing.T) {
	txn := NewTransactionAppend(1, "u", nil)
	rebased := Rebase(txn, 3)
	if rebased.ReadVersion != 3 {
		t.Errorf("read_version: got %d want 3", rebased.ReadVersion)
	}
}
