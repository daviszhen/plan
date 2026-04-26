package storage2

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"

	storage2pb "github.com/daviszhen/plan/pkg/storage2/proto"
)

// testdataDir returns the path to pkg/storage2/testdata (same dir as this _test.go).
func testdataDir(t *testing.T) string {
	t.Helper()
	_, path, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(path), "testdata")
}

// TestGenerateFixtures writes minimal.manifest and sample_append.txn to testdata/.
// Run from any dir: go test -run TestGenerateFixtures ./pkg/storage2
func TestGenerateFixtures(t *testing.T) {
	dir := testdataDir(t)
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatal(err)
	}
	m := NewManifest(0)
	m.Fragments = []*DataFragment{
		NewDataFragment(0, []*DataFile{NewDataFile("data/0.parquet", []int32{0, 1}, 1, 0)}),
	}
	m.NextRowId = 1
	mBuf, err := MarshalManifest(m)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "minimal.manifest"), mBuf, 0644); err != nil {
		t.Fatal(err)
	}
	txn := NewTransactionAppend(0, "fixture-append", []*DataFragment{
		NewDataFragment(1, []*DataFile{NewDataFile("data/1.parquet", []int32{0}, 1, 0)}),
	})
	txnBuf, err := MarshalTransaction(txn)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "sample_append.txn"), txnBuf, 0644); err != nil {
		t.Fatal(err)
	}
	t.Logf("wrote fixtures to %s", dir)
}

// T.2 format comparison: load fixture and assert key fields; round-trip.
func TestLoadManifestFixture(t *testing.T) {
	dir := testdataDir(t)
	data, err := os.ReadFile(filepath.Join(dir, "minimal.manifest"))
	if err != nil {
		t.Skipf("fixture not found (run TestGenerateFixtures first): %v", err)
	}
	m, err := UnmarshalManifest(data)
	if err != nil {
		t.Fatal(err)
	}
	if m.Version != 0 {
		t.Errorf("manifest version: got %d want 0", m.Version)
	}
	if len(m.Fragments) != 1 {
		t.Fatalf("fragments: got %d want 1", len(m.Fragments))
	}
	if m.Fragments[0].Id != 0 || len(m.Fragments[0].Files) != 1 {
		t.Errorf("fragment: id=%d files=%d", m.Fragments[0].Id, len(m.Fragments[0].Files))
	}
	if m.Fragments[0].Files[0].Path != "data/0.parquet" {
		t.Errorf("data file path: got %q", m.Fragments[0].Files[0].Path)
	}
}

func TestLoadTransactionFixture(t *testing.T) {
	dir := testdataDir(t)
	data, err := os.ReadFile(filepath.Join(dir, "sample_append.txn"))
	if err != nil {
		t.Skipf("fixture not found (run TestGenerateFixtures first): %v", err)
	}
	txn, err := UnmarshalTransaction(data)
	if err != nil {
		t.Fatal(err)
	}
	if txn.ReadVersion != 0 || txn.Uuid != "fixture-append" {
		t.Errorf("txn: read_version=%d uuid=%q", txn.ReadVersion, txn.Uuid)
	}
	if _, ok := txn.Operation.(*storage2pb.Transaction_Append_); !ok {
		t.Error("expected Append operation")
	}
}

func TestManifestRoundTripViaFixture(t *testing.T) {
	m := NewManifest(0)
	m.Fragments = []*DataFragment{NewDataFragment(0, []*DataFile{NewDataFile("p.parquet", []int32{0}, 1, 0)})}
	b, err := MarshalManifest(m)
	if err != nil {
		t.Fatal(err)
	}
	m2, err := UnmarshalManifest(b)
	if err != nil {
		t.Fatal(err)
	}
	if m2.Version != m.Version || len(m2.Fragments) != len(m.Fragments) {
		t.Errorf("round-trip: version %d->%d fragments %d->%d", m.Version, m2.Version, len(m.Fragments), len(m2.Fragments))
	}
}

// T.3 version and path convention: _versions/{v}.manifest, _transactions/{rv}-{uuid}.txn
func TestPathConvention(t *testing.T) {
	if ManifestPath(0) != filepath.Join(VersionsDir, "0.manifest") {
		t.Errorf("ManifestPath(0) = %s", ManifestPath(0))
	}
	if ManifestPath(1) != filepath.Join(VersionsDir, "1.manifest") {
		t.Errorf("ManifestPath(1) = %s", ManifestPath(1))
	}
	if TransactionPath(0, "abc") != filepath.Join(TransactionsDir, "0-abc.txn") {
		t.Errorf("TransactionPath = %s", TransactionPath(0, "abc"))
	}
	if VersionsDir != "_versions" || TransactionsDir != "_transactions" {
		t.Errorf("dirs: versions=%q transactions=%q", VersionsDir, TransactionsDir)
	}
}

// T.4 operation behavior: same input -> result manifest fragments, version, next_row_id as expected
func TestOperationBehaviorAppend(t *testing.T) {
	current := NewManifest(1)
	current.Fragments = []*DataFragment{NewDataFragment(0, []*DataFile{NewDataFile("a.parquet", []int32{0}, 1, 0)})}
	maxZero := uint32(0)
	current.MaxFragmentId = &maxZero
	txn := NewTransactionAppend(1, "op", []*DataFragment{
		NewDataFragment(1, []*DataFile{NewDataFile("b.parquet", []int32{0}, 1, 0)}),
	})
	next, err := BuildManifest(current, txn)
	if err != nil {
		t.Fatal(err)
	}
	if next.Version != 2 {
		t.Errorf("Append: version got %d want 2", next.Version)
	}
	if len(next.Fragments) != 2 {
		t.Fatalf("Append: fragments got %d want 2", len(next.Fragments))
	}
	if next.Fragments[0].Id != 0 || next.Fragments[1].Id != 1 {
		t.Errorf("Append: fragment ids %d %d", next.Fragments[0].Id, next.Fragments[1].Id)
	}
}

func TestOperationBehaviorOverwrite(t *testing.T) {
	current := NewManifest(1)
	current.Fragments = []*DataFragment{NewDataFragment(0, nil)}
	txn := NewTransactionOverwrite(1, "ow", []*DataFragment{
		NewDataFragment(0, []*DataFile{NewDataFile("new.parquet", []int32{0}, 1, 0)}),
	}, nil, nil)
	next, err := BuildManifest(current, txn)
	if err != nil {
		t.Fatal(err)
	}
	if next.Version != 2 {
		t.Errorf("Overwrite: version got %d want 2", next.Version)
	}
	if len(next.Fragments) != 1 || next.Fragments[0].Id != 0 {
		t.Errorf("Overwrite: fragments %v", next.Fragments)
	}
}

func TestOperationBehaviorDelete(t *testing.T) {
	current := NewManifest(1)
	current.Fragments = []*DataFragment{
		NewDataFragment(0, nil),
		NewDataFragment(1, nil),
	}
	txn := NewTransactionDelete(1, "del", nil, []uint64{0}, "x=1")
	next, err := BuildManifest(current, txn)
	if err != nil {
		t.Fatal(err)
	}
	if next.Version != 2 {
		t.Errorf("Delete: version got %d want 2", next.Version)
	}
	if len(next.Fragments) != 1 || next.Fragments[0].Id != 1 {
		t.Errorf("Delete: fragments %v", next.Fragments)
	}
}

// T.5 conflict matrix: (A,B) conflict vs compatible; rebase result matches "A then B"
func TestConflictAppendAppendCompatible(t *testing.T) {
	a := NewTransactionAppend(1, "a", []*DataFragment{NewDataFragment(0, nil)})
	b := NewTransactionAppend(1, "b", []*DataFragment{NewDataFragment(0, nil)})
	if CheckConflict(a, b, 2) {
		t.Error("Append vs Append should be compatible")
	}
}

func TestConflictAppendOverwriteConflict(t *testing.T) {
	a := NewTransactionAppend(1, "a", nil)
	b := NewTransactionOverwrite(1, "b", nil, nil, nil)
	if !CheckConflict(a, b, 2) {
		t.Error("Append vs Overwrite should conflict")
	}
}

func TestRebaseResultMatchesOrder(t *testing.T) {
	// Rebase B onto "after A": result should match building manifest from current, then A, then rebased B
	current := NewManifest(1)
	current.Fragments = []*DataFragment{NewDataFragment(0, []*DataFile{NewDataFile("0.parquet", []int32{0}, 1, 0)})}
	maxZero := uint32(0)
	current.MaxFragmentId = &maxZero
	txnA := NewTransactionAppend(1, "a", []*DataFragment{NewDataFragment(1, []*DataFile{NewDataFile("1.parquet", []int32{0}, 1, 0)})})
	txnB := NewTransactionAppend(1, "b", []*DataFragment{NewDataFragment(2, []*DataFile{NewDataFile("2.parquet", []int32{0}, 1, 0)})})
	nextA, _ := BuildManifest(current, txnA)
	rebased := Rebase(txnB, nextA.Version)
	nextB, err := BuildManifest(nextA, rebased)
	if err != nil {
		t.Fatal(err)
	}
	if nextB.Version != 3 || len(nextB.Fragments) != 3 {
		t.Errorf("rebase then build: version=%d fragments=%d", nextB.Version, len(nextB.Fragments))
	}
}
