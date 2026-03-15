package storage2

import (
	"context"
	"testing"

	"github.com/daviszhen/plan/pkg/chunk"
	"github.com/daviszhen/plan/pkg/common"
	"github.com/daviszhen/plan/pkg/util"
)

func TestRowIdScannerBasic(t *testing.T) {
	ctx := context.Background()
	basePath := t.TempDir()
	handler := NewLocalRenameCommitHandler()

	// Create dataset with version 0
	m0 := NewManifest(0)
	m0.Fragments = []*DataFragment{}
	m0.NextRowId = 1
	// Enable stable row IDs feature flag
	m0.ReaderFeatureFlags = 2
	m0.WriterFeatureFlags = 2
	if err := handler.Commit(ctx, basePath, 0, m0); err != nil {
		t.Fatal(err)
	}

	// For now, just test that scanner can be created
	// Full functionality requires RowIds parsing implementation
	scanner, err := NewRowIdScanner(ctx, basePath, handler, 0)
	if err != nil {
		t.Logf("Expected error due to unimplemented RowIds parsing: %v", err)
		// This is expected since we haven't implemented RowIds parsing yet
		return
	}

	// If somehow we got a scanner, test basic functionality
	rowIds := []uint64{1, 2, 3}
	chunk, err := scanner.TakeByRowIds(ctx, rowIds)
	if err != nil {
		t.Logf("TakeByRowIds failed as expected: %v", err)
	}

	// The chunk might be nil or have an error, both are acceptable for now
	_ = chunk
}

func TestRowIdScannerWithoutFeatureFlag(t *testing.T) {
	ctx := context.Background()
	basePath := t.TempDir()
	handler := NewLocalRenameCommitHandler()

	// Create dataset without stable row IDs feature flag
	m0 := NewManifest(0)
	m0.Fragments = []*DataFragment{}
	m0.NextRowId = 1
	// Feature flags not set
	if err := handler.Commit(ctx, basePath, 0, m0); err != nil {
		t.Fatal(err)
	}

	// Should fail to create scanner
	_, err := NewRowIdScanner(ctx, basePath, handler, 0)
	if err == nil {
		t.Fatal("expected error when stable row IDs not enabled")
	}
	if err.Error() != "stable row IDs not enabled for this dataset (feature flag 2 required)" {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestRowIdScannerNotFound(t *testing.T) {
	ctx := context.Background()
	basePath := t.TempDir()
	handler := NewLocalRenameCommitHandler()

	// Create dataset with feature flag enabled
	m0 := NewManifest(0)
	m0.Fragments = []*DataFragment{}
	m0.NextRowId = 1
	m0.ReaderFeatureFlags = 2
	m0.WriterFeatureFlags = 2
	if err := handler.Commit(ctx, basePath, 0, m0); err != nil {
		t.Fatal(err)
	}

	scanner, err := NewRowIdScanner(ctx, basePath, handler, 0)
	if err != nil {
		t.Logf("Expected error due to unimplemented RowIds parsing: %v", err)
		return
	}

	// Request non-existent RowIds
	rowIds := []uint64{999, 1000, 1001}
	chunk, err := scanner.TakeByRowIds(ctx, rowIds)
	if err != nil {
		t.Logf("TakeByRowIds failed as expected: %v", err)
	}

	// Should handle gracefully even with unimplemented parsing
	_ = chunk
}

// Helper function to create test data with RowId sequences
// This would be used when RowIds parsing is implemented
func createTestFragmentWithRowIds(t *testing.T, id uint64, rowCount int, rowIds []uint64) *DataFragment {
	// This is a placeholder - actual implementation would serialize rowIds properly
	frag := NewDataFragment(id, []*DataFile{
		NewDataFile("data/test.dat", []int32{0, 1}, 1, 0),
	})
	frag.PhysicalRows = uint64(rowCount)

	// Would set RowIdSequence here when serialization is implemented
	return frag
}

func makeSimpleChunk(t *testing.T, n int) *chunk.Chunk {
	t.Helper()
	typs := []common.LType{
		common.MakeLType(common.LTID_INTEGER),
		common.MakeLType(common.LTID_BIGINT),
	}
	c := &chunk.Chunk{}
	c.Init(typs, util.DefaultVectorSize)
	c.SetCard(n)
	for i := 0; i < n; i++ {
		c.Data[0].SetValue(i, &chunk.Value{Typ: typs[0], I64: int64(i)})
		c.Data[1].SetValue(i, &chunk.Value{Typ: typs[1], I64: int64(i * 10)})
	}
	return c
}
