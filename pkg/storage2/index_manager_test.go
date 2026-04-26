package storage2

import (
	"context"
	"testing"
)

// TestIndexManagerCreate tests creating scalar indexes.
func TestIndexManagerCreate(t *testing.T) {
	dir := t.TempDir()
	handler := NewLocalRenameCommitHandler()
	mgr := NewIndexManager(dir, handler)

	ctx := context.Background()

	// Create scalar index
	err := mgr.CreateScalarIndex(ctx, "idx_col0", 0)
	if err != nil {
		t.Fatalf("CreateScalarIndex failed: %v", err)
	}

	// Verify index exists
	idx, ok := mgr.GetIndex("idx_col0")
	if !ok {
		t.Error("GetIndex should find idx_col0")
	}
	if idx.Name() != "idx_col0" {
		t.Errorf("Name: got %q, want %q", idx.Name(), "idx_col0")
	}
	if idx.Type() != ScalarIndex {
		t.Errorf("Type: got %v, want ScalarIndex", idx.Type())
	}
	cols := idx.Columns()
	if len(cols) != 1 || cols[0] != 0 {
		t.Errorf("Columns: got %v, want [0]", cols)
	}
}

// TestIndexManagerCreateBitmap tests creating bitmap indexes.
func TestIndexManagerCreateBitmap(t *testing.T) {
	dir := t.TempDir()
	handler := NewLocalRenameCommitHandler()
	mgr := NewIndexManager(dir, handler)

	ctx := context.Background()

	err := mgr.CreateBitmapIndex(ctx, "idx_bitmap", 1)
	if err != nil {
		t.Fatalf("CreateBitmapIndex failed: %v", err)
	}

	idx, ok := mgr.GetIndex("idx_bitmap")
	if !ok {
		t.Error("GetIndex should find idx_bitmap")
	}
	if idx.Name() != "idx_bitmap" {
		t.Errorf("Name: got %q", idx.Name())
	}
}

// TestIndexManagerCreateZoneMap tests creating zonemap indexes.
func TestIndexManagerCreateZoneMap(t *testing.T) {
	dir := t.TempDir()
	handler := NewLocalRenameCommitHandler()
	mgr := NewIndexManager(dir, handler)

	ctx := context.Background()

	err := mgr.CreateZoneMapIndex(ctx, "idx_zonemap")
	if err != nil {
		t.Fatalf("CreateZoneMapIndex failed: %v", err)
	}

	idx, ok := mgr.GetIndex("idx_zonemap")
	if !ok {
		t.Error("GetIndex should find idx_zonemap")
	}
	if idx.Name() != "idx_zonemap" {
		t.Errorf("Name: got %q", idx.Name())
	}
}

// TestIndexManagerCreateBloom tests creating bloom filter indexes.
func TestIndexManagerCreateBloom(t *testing.T) {
	dir := t.TempDir()
	handler := NewLocalRenameCommitHandler()
	mgr := NewIndexManager(dir, handler)

	ctx := context.Background()

	err := mgr.CreateBloomFilterIndex(ctx, "idx_bloom")
	if err != nil {
		t.Fatalf("CreateBloomFilterIndex failed: %v", err)
	}

	idx, ok := mgr.GetIndex("idx_bloom")
	if !ok {
		t.Error("GetIndex should find idx_bloom")
	}
	if idx.Name() != "idx_bloom" {
		t.Errorf("Name: got %q", idx.Name())
	}
}

// TestIndexManagerDrop tests dropping indexes.
func TestIndexManagerDrop(t *testing.T) {
	dir := t.TempDir()
	handler := NewLocalRenameCommitHandler()
	mgr := NewIndexManager(dir, handler)

	ctx := context.Background()

	// Create and then drop
	mgr.CreateScalarIndex(ctx, "idx_to_drop", 0)

	if _, ok := mgr.GetIndex("idx_to_drop"); !ok {
		t.Error("index should exist before drop")
	}

	err := mgr.DropIndex(ctx, "idx_to_drop")
	if err != nil {
		t.Fatalf("DropIndex failed: %v", err)
	}

	if _, ok := mgr.GetIndex("idx_to_drop"); ok {
		t.Error("index should not exist after drop")
	}
}

// TestIndexManagerGetNotFound tests GetIndex for non-existent index.
func TestIndexManagerGetNotFound(t *testing.T) {
	dir := t.TempDir()
	handler := NewLocalRenameCommitHandler()
	mgr := NewIndexManager(dir, handler)

	_, ok := mgr.GetIndex("nonexistent")
	if ok {
		t.Error("GetIndex should return false for nonexistent index")
	}
}

// TestIndexManagerList tests listing all indexes.
func TestIndexManagerList(t *testing.T) {
	dir := t.TempDir()
	handler := NewLocalRenameCommitHandler()
	mgr := NewIndexManager(dir, handler)

	ctx := context.Background()

	// Initially empty
	list := mgr.ListIndexes()
	if len(list) != 0 {
		t.Errorf("ListIndexes should be empty initially, got %d", len(list))
	}

	// Add some indexes
	mgr.CreateScalarIndex(ctx, "idx1", 0)
	mgr.CreateBitmapIndex(ctx, "idx2", 1)
	mgr.CreateZoneMapIndex(ctx, "idx3")

	list = mgr.ListIndexes()
	if len(list) != 3 {
		t.Errorf("ListIndexes: got %d, want 3", len(list))
	}

	// Verify metadata
	names := make(map[string]bool)
	for _, meta := range list {
		names[meta.Name] = true
	}
	if !names["idx1"] || !names["idx2"] || !names["idx3"] {
		t.Errorf("ListIndexes missing expected indexes: %v", names)
	}
}

// TestIndexManagerOptimize tests optimizing indexes.
func TestIndexManagerOptimize(t *testing.T) {
	dir := t.TempDir()
	handler := NewLocalRenameCommitHandler()
	mgr := NewIndexManager(dir, handler)

	ctx := context.Background()

	// Create various index types and optimize them
	tests := []struct {
		name       string
		createFunc func() error
	}{
		{"scalar", func() error { return mgr.CreateScalarIndex(ctx, "idx_scalar", 0) }},
		{"bitmap", func() error { return mgr.CreateBitmapIndex(ctx, "idx_bitmap", 0) }},
		{"zonemap", func() error { return mgr.CreateZoneMapIndex(ctx, "idx_zonemap") }},
		{"bloom", func() error { return mgr.CreateBloomFilterIndex(ctx, "idx_bloom") }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.createFunc(); err != nil {
				t.Fatalf("create failed: %v", err)
			}
		})
	}

	// Optimize each index
	for _, name := range []string{"idx_scalar", "idx_bitmap", "idx_zonemap", "idx_bloom"} {
		if err := mgr.OptimizeIndex(ctx, name); err != nil {
			t.Errorf("OptimizeIndex(%s) failed: %v", name, err)
		}
	}

	// Optimize non-existent index should fail
	err := mgr.OptimizeIndex(ctx, "nonexistent")
	if err == nil {
		t.Error("OptimizeIndex should fail for nonexistent index")
	}
}

// TestIndexManagerCreateVector tests creating vector indexes.
func TestIndexManagerCreateVector(t *testing.T) {
	dir := t.TempDir()
	handler := NewLocalRenameCommitHandler()
	mgr := NewIndexManager(dir, handler)

	ctx := context.Background()

	// Should succeed now that vector index creation is implemented
	err := mgr.CreateVectorIndex(ctx, "idx_vector", 0, L2Metric)
	if err != nil {
		t.Errorf("CreateVectorIndex failed: %v", err)
	}

	idx, ok := mgr.GetIndex("idx_vector")
	if !ok {
		t.Error("expected to find idx_vector")
	}
	if idx.Type() != VectorIndex {
		t.Errorf("expected VectorIndex type, got %v", idx.Type())
	}
}

// TestIndexManagerCreateInverted tests creating inverted indexes.
func TestIndexManagerCreateInverted(t *testing.T) {
	dir := t.TempDir()
	handler := NewLocalRenameCommitHandler()
	mgr := NewIndexManager(dir, handler)

	ctx := context.Background()

	// Should succeed now that inverted index creation is implemented
	err := mgr.CreateInvertedIndex(ctx, "idx_inverted", 0)
	if err != nil {
		t.Errorf("CreateInvertedIndex failed: %v", err)
	}

	idx, ok := mgr.GetIndex("idx_inverted")
	if !ok {
		t.Error("expected to find idx_inverted")
	}
	if idx.Type() != InvertedIndex {
		t.Errorf("expected InvertedIndex type, got %v", idx.Type())
	}
}

// TestIndexTypeStringP1 tests IndexType.String method.
func TestIndexTypeStringP1(t *testing.T) {
	tests := []struct {
		typ  IndexType
		want string
	}{
		{ScalarIndex, "scalar"},
		{VectorIndex, "vector"},
		{InvertedIndex, "inverted"},
		{IndexType(99), "unknown"},
	}

	for _, tt := range tests {
		got := tt.typ.String()
		if got != tt.want {
			t.Errorf("IndexType(%d).String() = %q, want %q", tt.typ, got, tt.want)
		}
	}
}

// TestIndexStatsP1 tests getting index statistics.
func TestIndexStatsP1(t *testing.T) {
	dir := t.TempDir()
	handler := NewLocalRenameCommitHandler()
	mgr := NewIndexManager(dir, handler)

	ctx := context.Background()

	mgr.CreateScalarIndex(ctx, "idx_stats", 0)
	idx, _ := mgr.GetIndex("idx_stats")

	stats := idx.Statistics()
	// Just verify we can get stats without panic
	_ = stats.NumEntries
	_ = stats.SizeBytes
}
