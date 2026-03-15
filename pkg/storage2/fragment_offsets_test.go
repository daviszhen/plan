package storage2

import (
	"context"
	"testing"

	storage2pb "github.com/daviszhen/plan/pkg/storage2/proto"
)

func TestComputeFragmentOffsets(t *testing.T) {
	m := NewManifest(1)
	m.Fragments = []*DataFragment{
		NewDataFragmentWithRows(0, 10, nil),
		NewDataFragmentWithRows(1, 20, nil),
		NewDataFragmentWithRows(2, 5, nil),
	}
	offsets := ComputeFragmentOffsets(m)
	if len(offsets) != 4 {
		t.Fatalf("len(offsets)=%d want 4", len(offsets))
	}
	if offsets[0] != 0 || offsets[1] != 10 || offsets[2] != 30 || offsets[3] != 35 {
		t.Errorf("offsets=%v", offsets)
	}
}

func TestComputeFragmentOffsetsEmpty(t *testing.T) {
	m := NewManifest(1)
	m.Fragments = nil
	if ComputeFragmentOffsets(m) != nil {
		t.Error("expected nil for empty fragments")
	}
}

func TestFragmentsByOffsetRange(t *testing.T) {
	m := NewManifest(1)
	m.Fragments = []*DataFragment{
		NewDataFragmentWithRows(0, 10, nil),
		NewDataFragmentWithRows(1, 20, nil),
		NewDataFragmentWithRows(2, 5, nil),
	}
	ranges := FragmentsByOffsetRange(m, 5, 25)
	if len(ranges) != 2 {
		t.Fatalf("len(ranges)=%d want 2", len(ranges))
	}
	if ranges[0].Start != 0 || ranges[0].End != 10 || ranges[1].Start != 10 || ranges[1].End != 30 {
		t.Errorf("ranges=%+v", ranges)
	}
}

// TestCreateFragment verifies fragment creation and Append commit flow.
func TestCreateFragment(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	handler := NewLocalRenameCommitHandler()

	m0 := NewManifest(0)
	m0.Version = 0
	m0.Fragments = []*DataFragment{}
	_ = handler.Commit(ctx, dir, 0, m0)

	// Create a fragment with multiple data files
	df1 := NewDataFile("data/file1.lance", []int32{0, 1, 2}, 2, 0)
	df2 := NewDataFile("data/file2.lance", []int32{3, 4}, 2, 0)
	frag := NewDataFragmentWithRows(0, 5000, []*DataFile{df1, df2})

	if frag.PhysicalRows != 5000 {
		t.Errorf("PhysicalRows: got %d want 5000", frag.PhysicalRows)
	}
	if len(frag.Files) != 2 {
		t.Fatalf("Files count: got %d want 2", len(frag.Files))
	}
	if frag.Files[0].Path != "data/file1.lance" {
		t.Errorf("File0 path: got %q", frag.Files[0].Path)
	}

	// Commit the fragment via Append
	txn := NewTransactionAppend(0, "create-frag", []*DataFragment{frag})
	if err := CommitTransaction(ctx, dir, handler, txn); err != nil {
		t.Fatal(err)
	}

	// Verify the manifest
	m1, _ := LoadManifest(ctx, dir, handler, 1)
	if len(m1.Fragments) != 1 {
		t.Fatalf("Fragments: got %d want 1", len(m1.Fragments))
	}
	if len(m1.Fragments[0].Files) != 2 {
		t.Errorf("Fragment files: got %d want 2", len(m1.Fragments[0].Files))
	}
	if m1.Fragments[0].PhysicalRows != 5000 {
		t.Errorf("Fragment PhysicalRows: got %d want 5000", m1.Fragments[0].PhysicalRows)
	}
}

// TestFragmentMetadata verifies deletion file, physical rows, and feature flags on fragments.
func TestFragmentMetadata(t *testing.T) {
	// Test DeletionFile creation
	delFile := NewDeletionFile(storage2pb.DeletionFile_BITMAP, 1, 0, 42)
	if delFile.FileType != storage2pb.DeletionFile_BITMAP {
		t.Errorf("DeletionFile type: got %v want BITMAP", delFile.FileType)
	}
	if delFile.ReadVersion != 1 || delFile.Id != 0 || delFile.NumDeletedRows != 42 {
		t.Errorf("DeletionFile fields: readVer=%d id=%d numDeleted=%d",
			delFile.ReadVersion, delFile.Id, delFile.NumDeletedRows)
	}

	// Test fragment with deletion file via Delete transaction
	dir := t.TempDir()
	ctx := context.Background()
	handler := NewLocalRenameCommitHandler()

	// Setup: create v0 with a fragment
	m0 := NewManifest(0)
	m0.Version = 0
	frag := NewDataFragmentWithRows(0, 1000, []*DataFile{
		NewDataFile("data.lance", []int32{0}, 2, 0),
	})
	m0.Fragments = []*DataFragment{frag}
	maxFrag := uint32(0)
	m0.MaxFragmentId = &maxFrag
	_ = handler.Commit(ctx, dir, 0, m0)

	// Simulate a Delete: update the fragment with a deletion file
	updatedFrag := NewDataFragmentWithRows(0, 1000, []*DataFile{
		NewDataFile("data.lance", []int32{0}, 2, 0),
	})
	updatedFrag.DeletionFile = NewDeletionFile(storage2pb.DeletionFile_BITMAP, 0, 0, 100)

	txn := NewTransactionDelete(0, "del-meta", []*DataFragment{updatedFrag}, nil, "x > 10")
	if err := CommitTransaction(ctx, dir, handler, txn); err != nil {
		t.Fatal(err)
	}

	m1, _ := LoadManifest(ctx, dir, handler, 1)
	if len(m1.Fragments) != 1 {
		t.Fatalf("Fragments: got %d", len(m1.Fragments))
	}
	if m1.Fragments[0].DeletionFile == nil {
		t.Fatal("expected DeletionFile to be set")
	}
	if m1.Fragments[0].DeletionFile.NumDeletedRows != 100 {
		t.Errorf("NumDeletedRows: got %d want 100", m1.Fragments[0].DeletionFile.NumDeletedRows)
	}
	if m1.Fragments[0].DeletionFile.FileType != storage2pb.DeletionFile_BITMAP {
		t.Errorf("DeletionFile type: got %v want BITMAP", m1.Fragments[0].DeletionFile.FileType)
	}
}
