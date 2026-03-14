package sdk

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/daviszhen/plan/pkg/chunk"
	"github.com/daviszhen/plan/pkg/common"
	"github.com/daviszhen/plan/pkg/storage2"
	"github.com/daviszhen/plan/pkg/storage2/proto"
	"github.com/daviszhen/plan/pkg/util"
)

// Helper functions for creating test data
func createChunkWithRows(rows int, t *testing.T) *chunk.Chunk {
	t.Helper()
	typs := []common.LType{
		common.MakeLType(common.LTID_INTEGER),
		common.MakeLType(common.LTID_BIGINT),
	}
	c := &chunk.Chunk{}
	c.Init(typs, util.DefaultVectorSize)
	c.SetCard(rows)
	for i := 0; i < rows; i++ {
		c.Data[0].SetValue(i, &chunk.Value{
			Typ: typs[0],
			I64: int64(i),
		})
		c.Data[1].SetValue(i, &chunk.Value{
			Typ: typs[1],
			I64: int64(i * 100),
		})
	}
	return c
}

func intPtr(i int) *int {
	return &i
}

func uint64Ptr(u uint64) *uint64 {
	return &u
}

func stringPtr(s string) *string {
	return &s
}

func boolPtr(b bool) *bool {
	return &b
}

func emptyChunk(t *testing.T) *chunk.Chunk {
	t.Helper()
	typs := []common.LType{
		common.MakeLType(common.LTID_INTEGER),
		common.MakeLType(common.LTID_BIGINT),
	}
	c := &chunk.Chunk{}
	c.Init(typs, util.DefaultVectorSize)
	c.SetCard(10)
	for i := 0; i < 10; i++ {
		c.Data[0].SetValue(i, &chunk.Value{
			Typ: typs[0],
			I64: int64(i),
		})
		c.Data[1].SetValue(i, &chunk.Value{
			Typ: typs[1],
			I64: int64(i * 100),
		})
	}
	return c
}

func TestCreateAndOpenDataset(t *testing.T) {
	ctx := context.Background()
	basePath := t.TempDir()

	// Create empty dataset
	ds, err := CreateDataset(ctx, basePath).WithCommitHandler(NewLocalRenameCommitHandler()).Build()
	if err != nil {
		t.Fatal(err)
	}
	defer ds.Close()

	if v := ds.Version(); v != 0 {
		t.Errorf("version want 0 got %d", v)
	}
	count, _ := ds.CountRows()
	if count != 0 {
		t.Errorf("count want 0 got %d", count)
	}

	// Write chunk to file and append fragment
	dataPath := filepath.Join(basePath, "data", "0.dat")
	if err := storage2.WriteChunkToFile(dataPath, emptyChunk(t)); err != nil {
		t.Fatal(err)
	}
	df := NewDataFile("data/0.dat", []int32{0, 1}, 1, 0)
	frag := NewDataFragmentWithRows(0, 10, []*DataFile{df})
	if err := ds.Append(ctx, []*DataFragment{frag}); err != nil {
		t.Fatal(err)
	}

	if v := ds.Version(); v != 1 {
		t.Errorf("version want 1 got %d", v)
	}
	count, _ = ds.CountRows()
	if count != 10 {
		t.Errorf("count want 10 got %d", count)
	}

	// Open at version 1
	ds2, err := OpenDataset(ctx, basePath).WithVersion(1).Build()
	if err != nil {
		t.Fatal(err)
	}
	defer ds2.Close()
	if ds2.Version() != 1 {
		t.Errorf("open version want 1 got %d", ds2.Version())
	}
}

func TestOpenDatasetLatest(t *testing.T) {
	ctx := context.Background()
	basePath := t.TempDir()

	// Create and append so we have version 1
	ds, err := CreateDataset(ctx, basePath).Build()
	if err != nil {
		t.Fatal(err)
	}
	dataPath := filepath.Join(basePath, "data", "0.dat")
	_ = storage2.WriteChunkToFile(dataPath, emptyChunk(t))
	df := NewDataFile("data/0.dat", []int32{0, 1}, 1, 0)
	frag := NewDataFragmentWithRows(0, 5, []*DataFile{df})
	_ = ds.Append(ctx, []*DataFragment{frag})
	ds.Close()

	// Open without WithVersion -> latest
	ds2, err := OpenDataset(ctx, basePath).Build()
	if err != nil {
		t.Fatal(err)
	}
	defer ds2.Close()
	if ds2.Version() != 1 {
		t.Errorf("latest version want 1 got %d", ds2.Version())
	}
}

func TestDeleteAndOverwrite(t *testing.T) {
	ctx := context.Background()
	basePath := t.TempDir()

	ds, err := CreateDataset(ctx, basePath).Build()
	if err != nil {
		t.Fatal(err)
	}
	dataPath := filepath.Join(basePath, "data", "0.dat")
	_ = storage2.WriteChunkToFile(dataPath, emptyChunk(t))
	df := NewDataFile("data/0.dat", []int32{0, 1}, 1, 0)
	frag := NewDataFragmentWithRows(0, 3, []*DataFile{df})
	_ = ds.Append(ctx, []*DataFragment{frag})
	ds.Close()

	ds2, _ := OpenDataset(ctx, basePath).Build()
	defer ds2.Close()
	if err := ds2.Delete(ctx, "id > 0"); err != nil {
		t.Fatal(err)
	}
	if ds2.Version() != 2 {
		t.Errorf("after delete version want 2 got %d", ds2.Version())
	}

	// Overwrite with new fragment
	dataPath2 := filepath.Join(basePath, "data", "1.dat")
	_ = storage2.WriteChunkToFile(dataPath2, emptyChunk(t))
	df2 := NewDataFile("data/1.dat", []int32{0, 1}, 1, 0)
	frag2 := NewDataFragmentWithRows(1, 2, []*DataFile{df2})
	if err := ds2.Overwrite(ctx, []*DataFragment{frag2}); err != nil {
		t.Fatal(err)
	}
	if ds2.Version() != 3 {
		t.Errorf("after overwrite version want 3 got %d", ds2.Version())
	}
	count, _ := ds2.CountRows()
	if count != 2 {
		t.Errorf("after overwrite count want 2 got %d", count)
	}
}

func TestDatasetSchema(t *testing.T) {
	ctx := context.Background()
	basePath := t.TempDir()

	// Create empty dataset - should have empty schema
	ds, err := CreateDataset(ctx, basePath).Build()
	if err != nil {
		t.Fatal(err)
	}
	defer ds.Close()

	schema := ds.Schema()
	if len(schema) != 0 {
		t.Errorf("empty dataset schema should be empty, got %d fields", len(schema))
	}

	// Create dataset with schema by manually creating manifest with fields
	handler := NewLocalRenameCommitHandler()

	// Create a manifest with some fields
	fields := []*storage2pb.Field{
		{
			Name:        "id",
			Type:        storage2pb.Field_LEAF,
			Id:          0,
			LogicalType: "int64",
			Nullable:    false,
		},
		{
			Name:        "name",
			Type:        storage2pb.Field_LEAF,
			Id:          1,
			LogicalType: "string",
			Nullable:    true,
		},
	}

	manifest := storage2.NewManifest(0)
	manifest.Fields = fields
	manifest.Fragments = []*storage2pb.DataFragment{}
	manifest.NextRowId = 1

	if err := handler.Commit(ctx, basePath, 0, manifest); err != nil {
		t.Fatal(err)
	}

	// Open the dataset and check schema
	ds2, err := OpenDataset(ctx, basePath).Build()
	if err != nil {
		t.Fatal(err)
	}
	defer ds2.Close()

	schema2 := ds2.Schema()
	if len(schema2) != 2 {
		t.Errorf("schema should have 2 fields, got %d", len(schema2))
	}

	if schema2[0].Name != "id" {
		t.Errorf("first field name want 'id' got '%s'", schema2[0].Name)
	}
	if schema2[0].LogicalType != "int64" {
		t.Errorf("first field logical type want 'int64' got '%s'", schema2[0].LogicalType)
	}

	if schema2[1].Name != "name" {
		t.Errorf("second field name want 'name' got '%s'", schema2[1].Name)
	}
	if schema2[1].LogicalType != "string" {
		t.Errorf("second field logical type want 'string' got '%s'", schema2[1].LogicalType)
	}
}

func TestDatasetSchemaMetadata(t *testing.T) {
	ctx := context.Background()
	basePath := t.TempDir()

	// Create empty dataset - should have empty schema metadata
	ds, err := CreateDataset(ctx, basePath).Build()
	if err != nil {
		t.Fatal(err)
	}
	defer ds.Close()

	metadata := ds.SchemaMetadata()
	if len(metadata) != 0 {
		t.Errorf("empty dataset schema metadata should be empty, got %d entries", len(metadata))
	}

	// Create dataset with schema metadata
	handler := NewLocalRenameCommitHandler()

	// Create a manifest with schema metadata
	schemaMetadata := map[string][]byte{
		"pandas_version": []byte("1.5.0"),
		"creator":        []byte("test"),
		"encoding":       []byte("utf-8"),
	}

	manifest := storage2.NewManifest(0)
	manifest.Fields = []*storage2pb.Field{}
	manifest.SchemaMetadata = schemaMetadata
	manifest.Fragments = []*storage2pb.DataFragment{}
	manifest.NextRowId = 1

	if err := handler.Commit(ctx, basePath, 0, manifest); err != nil {
		t.Fatal(err)
	}

	// Open the dataset and check schema metadata
	ds2, err := OpenDataset(ctx, basePath).Build()
	if err != nil {
		t.Fatal(err)
	}
	defer ds2.Close()

	metadata2 := ds2.SchemaMetadata()
	if len(metadata2) != 3 {
		t.Errorf("schema metadata should have 3 entries, got %d", len(metadata2))
	}

	if string(metadata2["pandas_version"]) != "1.5.0" {
		t.Errorf("pandas_version want '1.5.0' got '%s'", string(metadata2["pandas_version"]))
	}
	if string(metadata2["creator"]) != "test" {
		t.Errorf("creator want 'test' got '%s'", string(metadata2["creator"]))
	}
	if string(metadata2["encoding"]) != "utf-8" {
		t.Errorf("encoding want 'utf-8' got '%s'", string(metadata2["encoding"]))
	}
}

func TestDatasetFieldConfiguration(t *testing.T) {
	ctx := context.Background()
	basePath := t.TempDir()

	// Create dataset with complex schema including nested fields
	handler := NewLocalRenameCommitHandler()

	// Create fields with different configurations
	fields := []*storage2pb.Field{
		{
			Name:        "id",
			Type:        storage2pb.Field_LEAF,
			Id:          0,
			ParentId:    -1, // Top-level field
			LogicalType: "int64",
			Nullable:    false,
			Metadata: map[string][]byte{
				"description": []byte("Primary key"),
				"source":      []byte("database"),
			},
		},
		{
			Name:        "name",
			Type:        storage2pb.Field_LEAF,
			Id:          1,
			ParentId:    -1, // Top-level field
			LogicalType: "string",
			Nullable:    true,
			Metadata: map[string][]byte{
				"description": []byte("User name"),
				"max_length":  []byte("100"),
			},
		},
		{
			Name:        "address",
			Type:        storage2pb.Field_PARENT,
			Id:          2,
			ParentId:    -1, // Top-level field
			LogicalType: "struct",
			Nullable:    true,
		},
		{
			Name:        "street",
			Type:        storage2pb.Field_LEAF,
			Id:          3,
			ParentId:    2, // Child of address
			LogicalType: "string",
			Nullable:    true,
		},
		{
			Name:        "city",
			Type:        storage2pb.Field_LEAF,
			Id:          4,
			ParentId:    2, // Child of address
			LogicalType: "string",
			Nullable:    true,
		},
	}

	manifest := storage2.NewManifest(0)
	manifest.Fields = fields
	manifest.Fragments = []*storage2pb.DataFragment{}
	manifest.NextRowId = 1

	if err := handler.Commit(ctx, basePath, 0, manifest); err != nil {
		t.Fatal(err)
	}

	// Open the dataset and test field configuration APIs
	ds, err := OpenDataset(ctx, basePath).Build()
	if err != nil {
		t.Fatal(err)
	}
	defer ds.Close()

	// Test FieldByName
	idField := ds.FieldByName("id")
	if idField == nil {
		t.Fatal("FieldByName('id') should not be nil")
	}
	if idField.Name != "id" || idField.LogicalType != "int64" || idField.Nullable != false {
		t.Errorf("id field config incorrect: %+v", idField)
	}

	nameField := ds.FieldByName("name")
	if nameField == nil {
		t.Fatal("FieldByName('name') should not be nil")
	}
	if nameField.Name != "name" || nameField.LogicalType != "string" || nameField.Nullable != true {
		t.Errorf("name field config incorrect: %+v", nameField)
	}

	// Test non-existent field
	nonExistent := ds.FieldByName("nonexistent")
	if nonExistent != nil {
		t.Errorf("FieldByName('nonexistent') should be nil, got %+v", nonExistent)
	}

	// Test FieldByID
	fieldByID := ds.FieldByID(0)
	if fieldByID == nil || fieldByID.Name != "id" {
		t.Errorf("FieldByID(0) should return id field, got %+v", fieldByID)
	}

	fieldByID2 := ds.FieldByID(3)
	if fieldByID2 == nil || fieldByID2.Name != "street" {
		t.Errorf("FieldByID(3) should return street field, got %+v", fieldByID2)
	}

	// Test FieldsByParentID
	addressChildren := ds.FieldsByParentID(2)
	if len(addressChildren) != 2 {
		t.Errorf("FieldsByParentID(2) should return 2 children, got %d", len(addressChildren))
	}

	// Check that we got street and city
	foundStreet := false
	foundCity := false
	for _, child := range addressChildren {
		if child.Name == "street" {
			foundStreet = true
		}
		if child.Name == "city" {
			foundCity = true
		}
	}
	if !foundStreet || !foundCity {
		t.Errorf("Missing expected child fields: street=%v, city=%v", foundStreet, foundCity)
	}

	// Test root fields (ParentId = -1)
	rootFields := ds.FieldsByParentID(-1)
	if len(rootFields) != 3 {
		t.Errorf("Root fields should have 3 entries, got %d", len(rootFields))
	}
}

func TestDatasetCompact(t *testing.T) {
	ctx := context.Background()
	basePath := t.TempDir()

	// Create dataset with multiple small fragments
	ds, err := CreateDataset(ctx, basePath).Build()
	if err != nil {
		t.Fatal(err)
	}
	defer ds.Close()

	// Create 3 small fragments
	for i := 0; i < 3; i++ {
		dataPath := filepath.Join(basePath, "data", fmt.Sprintf("%d.dat", i))
		if err := storage2.WriteChunkToFile(dataPath, emptyChunk(t)); err != nil {
			t.Fatal(err)
		}
		df := NewDataFile(fmt.Sprintf("data/%d.dat", i), []int32{0, 1}, 1, 0)
		frag := NewDataFragmentWithRows(uint64(i), 10, []*DataFile{df})
		if err := ds.Append(ctx, []*DataFragment{frag}); err != nil {
			t.Fatal(err)
		}
	}

	// Verify we have 3 fragments and 30 rows
	initialCount, _ := ds.CountRows()
	if initialCount != 30 {
		t.Errorf("Initial count want 30 got %d", initialCount)
	}

	// Perform compaction
	if err := ds.Compact(ctx); err != nil {
		t.Fatal(err)
	}

	// Verify after compaction we still have 30 rows but fewer fragments
	finalCount, _ := ds.CountRows()
	if finalCount != 30 {
		t.Errorf("After compaction count want 30 got %d", finalCount)
	}
}

func TestDatasetCompactWithDeletions(t *testing.T) {
	ctx := context.Background()
	basePath := t.TempDir()

	// Create dataset with fragments that have deletions
	ds, err := CreateDataset(ctx, basePath).Build()
	if err != nil {
		t.Fatal(err)
	}
	defer ds.Close()

	// Create 2 fragments
	for i := 0; i < 2; i++ {
		dataPath := filepath.Join(basePath, "data", fmt.Sprintf("%d.dat", i))
		if err := storage2.WriteChunkToFile(dataPath, emptyChunk(t)); err != nil {
			t.Fatal(err)
		}
		df := NewDataFile(fmt.Sprintf("data/%d.dat", i), []int32{0, 1}, 1, 0)
		frag := NewDataFragmentWithRows(uint64(i), 10, []*DataFile{df})

		// Add deletion file to first fragment
		if i == 0 {
			deletionFile := storage2.NewDeletionFile(storage2pb.DeletionFile_ARROW_ARRAY, 0, 0, 3)
			frag.DeletionFile = deletionFile
		}

		if err := ds.Append(ctx, []*DataFragment{frag}); err != nil {
			t.Fatal(err)
		}
	}

	// Verify initial state: 20 physical rows, but 17 logical rows due to deletions
	initialPhysicalRows, _ := ds.CountRows()
	if initialPhysicalRows != 20 {
		t.Errorf("Initial physical rows want 20 got %d", initialPhysicalRows)
	}

	// Perform compaction with deletions
	if err := ds.Compact(ctx); err != nil {
		t.Fatal(err)
	}

	// Verify after compaction
	finalPhysicalRows, _ := ds.CountRows()
	if finalPhysicalRows != 20 {
		t.Errorf("After compaction physical rows want 20 got %d", finalPhysicalRows)
	}
}

func TestDatasetCompactWithOptions(t *testing.T) {
	ctx := context.Background()
	basePath := t.TempDir()

	// Create dataset with multiple fragments of different sizes
	ds, err := CreateDataset(ctx, basePath).Build()
	if err != nil {
		t.Fatal(err)
	}
	defer ds.Close()

	// Create 4 fragments with different sizes
	fragmentSizes := []uint64{5, 10, 15, 20} // rows per fragment
	for i, size := range fragmentSizes {
		dataPath := filepath.Join(basePath, "data", fmt.Sprintf("%d.dat", i))
		chunk := createChunkWithRows(int(size), t)
		if err := storage2.WriteChunkToFile(dataPath, chunk); err != nil {
			t.Fatal(err)
		}
		df := NewDataFile(fmt.Sprintf("data/%d.dat", i), []int32{0, 1}, 1, 0)
		df.FileSizeBytes = size * 100 // approximate size
		frag := NewDataFragmentWithRows(uint64(i), size, []*DataFile{df})
		if err := ds.Append(ctx, []*DataFragment{frag}); err != nil {
			t.Fatal(err)
		}
	}

	// Test 1: Compact with batch size = 2
	opts1 := CompactionOptions{
		BatchSize: intPtr(2),
	}

	if err := ds.CompactWithOptions(ctx, opts1); err != nil {
		t.Fatal(err)
	}

	// Test 2: Compact with max bytes constraint
	opts2 := CompactionOptions{
		MaxBytes:  uint64Ptr(2000), // Only compact fragments under 2000 bytes
		BatchSize: intPtr(3),
	}

	if err := ds.CompactWithOptions(ctx, opts2); err != nil {
		t.Fatal(err)
	}

	// Test 3: Compact with target fragment size
	opts3 := CompactionOptions{
		TargetFragmentSize: uint64Ptr(1000),
		BatchSize:          intPtr(2),
	}

	if err := ds.CompactWithOptions(ctx, opts3); err != nil {
		t.Fatal(err)
	}
}

func TestDatasetMultipleCompactions(t *testing.T) {
	ctx := context.Background()
	basePath := t.TempDir()

	// Create dataset with many small fragments
	ds, err := CreateDataset(ctx, basePath).Build()
	if err != nil {
		t.Fatal(err)
	}
	defer ds.Close()

	// Create 6 small fragments
	for i := 0; i < 6; i++ {
		dataPath := filepath.Join(basePath, "data", fmt.Sprintf("%d.dat", i))
		chunk := createChunkWithRows(5, t) // 5 rows each
		if err := storage2.WriteChunkToFile(dataPath, chunk); err != nil {
			t.Fatal(err)
		}
		df := NewDataFile(fmt.Sprintf("data/%d.dat", i), []int32{0, 1}, 1, 0)
		frag := NewDataFragmentWithRows(uint64(i), 5, []*DataFile{df})
		if err := ds.Append(ctx, []*DataFragment{frag}); err != nil {
			t.Fatal(err)
		}
	}

	initialFragments := len(ds.(*datasetImpl).currentManifest.Fragments)
	if initialFragments != 6 {
		t.Errorf("Expected 6 initial fragments, got %d", initialFragments)
	}

	// First compaction: batch size 3
	opts1 := CompactionOptions{
		BatchSize: intPtr(3),
	}

	if err := ds.CompactWithOptions(ctx, opts1); err != nil {
		t.Fatal(err)
	}

	// Check intermediate state
	intermediateFragments := len(ds.(*datasetImpl).currentManifest.Fragments)
	if intermediateFragments >= 6 {
		t.Errorf("First compaction should reduce fragments from 6, got %d", intermediateFragments)
	}

	// Second compaction: batch size 2
	opts2 := CompactionOptions{
		BatchSize: intPtr(2),
	}

	if err := ds.CompactWithOptions(ctx, opts2); err != nil {
		t.Fatal(err)
	}

	// Final check
	finalFragments := len(ds.(*datasetImpl).currentManifest.Fragments)
	if finalFragments >= intermediateFragments {
		t.Errorf("Second compaction should further reduce fragments, got %d", finalFragments)
	}

	// Verify data integrity: should still have 30 rows total
	finalCount, _ := ds.CountRows()
	if finalCount != 30 {
		t.Errorf("After multiple compactions, row count should be 30, got %d", finalCount)
	}
}

func TestDatasetCompactWithOptionsEdgeCases(t *testing.T) {
	ctx := context.Background()
	basePath := t.TempDir()

	// Test with closed dataset
	ds, err := CreateDataset(ctx, basePath).Build()
	if err != nil {
		t.Fatal(err)
	}
	ds.Close()

	opts := CompactionOptions{}
	err = ds.CompactWithOptions(ctx, opts)
	if err == nil {
		t.Fatal("expected error when compacting closed dataset")
	}

	// Test with single fragment (nothing to compact)
	ds2, err := CreateDataset(ctx, basePath+"_2").Build()
	if err != nil {
		t.Fatal(err)
	}
	defer ds2.Close()

	dataPath := filepath.Join(basePath+"_2", "data", "0.dat")
	if err := storage2.WriteChunkToFile(dataPath, emptyChunk(t)); err != nil {
		t.Fatal(err)
	}
	df := NewDataFile("data/0.dat", []int32{0, 1}, 1, 0)
	frag := NewDataFragmentWithRows(0, 10, []*DataFile{df})
	if err := ds2.Append(ctx, []*DataFragment{frag}); err != nil {
		t.Fatal(err)
	}

	// Should complete successfully but do nothing
	if err := ds2.CompactWithOptions(ctx, opts); err != nil {
		t.Errorf("expected no error for single fragment, got %v", err)
	}
}

func TestDatasetCompactWithAllOptions(t *testing.T) {
	ctx := context.Background()
	basePath := t.TempDir()

	// Create dataset with various fragment configurations
	ds, err := CreateDataset(ctx, basePath).Build()
	if err != nil {
		t.Fatal(err)
	}
	defer ds.Close()

	// Create fragments with different characteristics
	fragmentConfigs := []struct {
		rows       uint64
		hasDeletes bool
		fileSize   uint64
	}{
		{rows: 5, hasDeletes: true, fileSize: 500},    // Small with deletions
		{rows: 10, hasDeletes: false, fileSize: 1000}, // Medium without deletions
		{rows: 20, hasDeletes: false, fileSize: 2000}, // Large without deletions
		{rows: 15, hasDeletes: true, fileSize: 1500},  // Medium with deletions
	}

	for i, config := range fragmentConfigs {
		dataPath := filepath.Join(basePath, "data", fmt.Sprintf("%d.dat", i))
		chunk := createChunkWithRows(int(config.rows), t)
		if err := storage2.WriteChunkToFile(dataPath, chunk); err != nil {
			t.Fatal(err)
		}
		df := NewDataFile(fmt.Sprintf("data/%d.dat", i), []int32{0, 1}, 1, 0)
		df.FileSizeBytes = config.fileSize
		frag := NewDataFragmentWithRows(uint64(i), config.rows, []*DataFile{df})

		// Add deletion file if needed
		if config.hasDeletes {
			deletionFile := storage2.NewDeletionFile(storage2pb.DeletionFile_ARROW_ARRAY, 0, uint64(i), 2)
			frag.DeletionFile = deletionFile
		}

		if err := ds.Append(ctx, []*DataFragment{frag}); err != nil {
			t.Fatal(err)
		}
	}

	// Test 1: Size-based compaction with min/max constraints
	opts1 := CompactionOptions{
		MinFragmentSize:  uint64Ptr(800),  // Compact fragments smaller than 800 bytes
		MaxFragmentSize:  uint64Ptr(1800), // Don't compact fragments larger than 1800 bytes
		CompactionMethod: stringPtr("size_based"),
		BatchSize:        intPtr(2),
	}

	if err := ds.CompactWithOptions(ctx, opts1); err != nil {
		t.Fatal(err)
	}

	// Test 2: Count-based compaction
	opts2 := CompactionOptions{
		CompactionMethod: stringPtr("count_based"),
		BatchSize:        intPtr(3), // Compact in groups of 3
	}

	if err := ds.CompactWithOptions(ctx, opts2); err != nil {
		t.Fatal(err)
	}

	// Test 3: Hybrid compaction
	opts3 := CompactionOptions{
		CompactionMethod:   stringPtr("hybrid"),
		MaxBytes:           uint64Ptr(3000),
		BatchSize:          intPtr(2),
		IncludeDeletedRows: boolPtr(false), // Exclude fragments with deletions
	}

	if err := ds.CompactWithOptions(ctx, opts3); err != nil {
		t.Fatal(err)
	}

	// Test 4: Preserve row IDs
	opts4 := CompactionOptions{
		CompactionMethod: stringPtr("size_based"),
		BatchSize:        intPtr(2),
		PreserveRowIds:   boolPtr(true),
	}

	if err := ds.CompactWithOptions(ctx, opts4); err != nil {
		t.Fatal(err)
	}

	// Verify final state
	finalCount, _ := ds.CountRows()
	expectedRows := uint64(50) // 5 + 10 + 20 + 15 = 50
	if finalCount != expectedRows {
		t.Errorf("Final row count want %d got %d", expectedRows, finalCount)
	}
}

// ============================================================
// Phase 4: Schema Evolution Tests
// ============================================================

func createDatasetWithSchema(t *testing.T, basePath string, fields []*storage2pb.Field) Dataset {
	t.Helper()
	ctx := context.Background()
	handler := NewLocalRenameCommitHandler()

	manifest := storage2.NewManifest(0)
	manifest.Fields = fields
	manifest.Fragments = []*storage2pb.DataFragment{}
	manifest.NextRowId = 1

	if err := handler.Commit(ctx, basePath, 0, manifest); err != nil {
		t.Fatal(err)
	}

	ds, err := OpenDataset(ctx, basePath).Build()
	if err != nil {
		t.Fatal(err)
	}
	return ds
}

func TestDropColumns(t *testing.T) {
	basePath := t.TempDir()
	fields := []*storage2pb.Field{
		{Name: "id", Type: storage2pb.Field_LEAF, Id: 0, ParentId: -1, LogicalType: "int64", Nullable: false},
		{Name: "name", Type: storage2pb.Field_LEAF, Id: 1, ParentId: -1, LogicalType: "string", Nullable: true},
		{Name: "age", Type: storage2pb.Field_LEAF, Id: 2, ParentId: -1, LogicalType: "int32", Nullable: true},
		{Name: "email", Type: storage2pb.Field_LEAF, Id: 3, ParentId: -1, LogicalType: "string", Nullable: true},
	}

	ds := createDatasetWithSchema(t, basePath, fields)
	defer ds.Close()
	ctx := context.Background()

	// Verify initial schema
	if len(ds.Schema()) != 4 {
		t.Fatalf("initial schema should have 4 fields, got %d", len(ds.Schema()))
	}

	// Drop a single column
	if err := ds.DropColumns(ctx, []string{"age"}); err != nil {
		t.Fatal(err)
	}

	schema := ds.Schema()
	if len(schema) != 3 {
		t.Fatalf("after drop 'age': want 3 fields, got %d", len(schema))
	}
	for _, f := range schema {
		if f.Name == "age" {
			t.Error("'age' field should have been dropped")
		}
	}

	// Drop multiple columns
	if err := ds.DropColumns(ctx, []string{"name", "email"}); err != nil {
		t.Fatal(err)
	}

	schema = ds.Schema()
	if len(schema) != 1 {
		t.Fatalf("after drop 'name','email': want 1 field, got %d", len(schema))
	}
	if schema[0].Name != "id" {
		t.Errorf("remaining field should be 'id', got '%s'", schema[0].Name)
	}
}

func TestDropColumnsErrors(t *testing.T) {
	basePath := t.TempDir()
	fields := []*storage2pb.Field{
		{Name: "id", Type: storage2pb.Field_LEAF, Id: 0, ParentId: -1, LogicalType: "int64"},
		{Name: "name", Type: storage2pb.Field_LEAF, Id: 1, ParentId: -1, LogicalType: "string"},
	}

	ds := createDatasetWithSchema(t, basePath, fields)
	defer ds.Close()
	ctx := context.Background()

	// Drop non-existent column
	err := ds.DropColumns(ctx, []string{"nonexistent"})
	if err == nil {
		t.Fatal("expected error dropping non-existent column")
	}

	// Drop all columns
	err = ds.DropColumns(ctx, []string{"id", "name"})
	if err == nil {
		t.Fatal("expected error dropping all columns")
	}

	// Empty column list
	err = ds.DropColumns(ctx, []string{})
	if err == nil {
		t.Fatal("expected error with empty column list")
	}

	// Drop on closed dataset
	ds.Close()
	err = ds.DropColumns(ctx, []string{"id"})
	if err == nil {
		t.Fatal("expected error on closed dataset")
	}
}

func TestDropColumnsWithNestedFields(t *testing.T) {
	basePath := t.TempDir()
	fields := []*storage2pb.Field{
		{Name: "id", Type: storage2pb.Field_LEAF, Id: 0, ParentId: -1, LogicalType: "int64"},
		{Name: "address", Type: storage2pb.Field_PARENT, Id: 1, ParentId: -1, LogicalType: "struct"},
		{Name: "street", Type: storage2pb.Field_LEAF, Id: 2, ParentId: 1, LogicalType: "string"},
		{Name: "city", Type: storage2pb.Field_LEAF, Id: 3, ParentId: 1, LogicalType: "string"},
		{Name: "zip", Type: storage2pb.Field_LEAF, Id: 4, ParentId: 1, LogicalType: "string"},
	}

	ds := createDatasetWithSchema(t, basePath, fields)
	defer ds.Close()
	ctx := context.Background()

	// Drop parent field should also drop all children
	if err := ds.DropColumns(ctx, []string{"address"}); err != nil {
		t.Fatal(err)
	}

	schema := ds.Schema()
	if len(schema) != 1 {
		t.Fatalf("after drop 'address': want 1 field, got %d", len(schema))
	}
	if schema[0].Name != "id" {
		t.Errorf("remaining field should be 'id', got '%s'", schema[0].Name)
	}
}

func TestAlterColumns(t *testing.T) {
	basePath := t.TempDir()
	fields := []*storage2pb.Field{
		{Name: "id", Type: storage2pb.Field_LEAF, Id: 0, ParentId: -1, LogicalType: "int64", Nullable: false},
		{Name: "name", Type: storage2pb.Field_LEAF, Id: 1, ParentId: -1, LogicalType: "string", Nullable: true},
		{Name: "age", Type: storage2pb.Field_LEAF, Id: 2, ParentId: -1, LogicalType: "int32", Nullable: false},
	}

	ds := createDatasetWithSchema(t, basePath, fields)
	defer ds.Close()
	ctx := context.Background()

	// Rename a column
	if err := ds.AlterColumns(ctx, []ColumnAlteration{
		{Path: "name", NewName: "full_name"},
	}); err != nil {
		t.Fatal(err)
	}

	f := ds.FieldByName("full_name")
	if f == nil {
		t.Fatal("renamed field 'full_name' not found")
	}
	if f.LogicalType != "string" {
		t.Errorf("renamed field type want 'string' got '%s'", f.LogicalType)
	}

	// Verify old name is gone
	if ds.FieldByName("name") != nil {
		t.Error("old field name 'name' should not exist after rename")
	}

	// Change nullable
	if err := ds.AlterColumns(ctx, []ColumnAlteration{
		{Path: "age", NewNullable: boolPtr(true)},
	}); err != nil {
		t.Fatal(err)
	}

	ageField := ds.FieldByName("age")
	if ageField == nil {
		t.Fatal("field 'age' not found")
	}
	if !ageField.Nullable {
		t.Error("age field should be nullable after alter")
	}

	// Change data type
	if err := ds.AlterColumns(ctx, []ColumnAlteration{
		{Path: "age", NewDataType: "int64"},
	}); err != nil {
		t.Fatal(err)
	}

	ageField = ds.FieldByName("age")
	if ageField.LogicalType != "int64" {
		t.Errorf("age field type want 'int64' got '%s'", ageField.LogicalType)
	}

	// Multiple alterations at once
	if err := ds.AlterColumns(ctx, []ColumnAlteration{
		{Path: "id", NewNullable: boolPtr(true)},
		{Path: "full_name", NewName: "user_name", NewNullable: boolPtr(false)},
	}); err != nil {
		t.Fatal(err)
	}

	idField := ds.FieldByName("id")
	if !idField.Nullable {
		t.Error("id should be nullable after alter")
	}
	userField := ds.FieldByName("user_name")
	if userField == nil {
		t.Fatal("'user_name' not found")
	}
	if userField.Nullable {
		t.Error("user_name should not be nullable after alter")
	}
}

func TestAlterColumnsErrors(t *testing.T) {
	basePath := t.TempDir()
	fields := []*storage2pb.Field{
		{Name: "id", Type: storage2pb.Field_LEAF, Id: 0, ParentId: -1, LogicalType: "int64"},
	}

	ds := createDatasetWithSchema(t, basePath, fields)
	defer ds.Close()
	ctx := context.Background()

	// Alter non-existent column
	err := ds.AlterColumns(ctx, []ColumnAlteration{
		{Path: "nonexistent", NewName: "x"},
	})
	if err == nil {
		t.Fatal("expected error altering non-existent column")
	}

	// Empty alteration list
	err = ds.AlterColumns(ctx, []ColumnAlteration{})
	if err == nil {
		t.Fatal("expected error with empty alteration list")
	}

	// Alter on closed dataset
	ds.Close()
	err = ds.AlterColumns(ctx, []ColumnAlteration{
		{Path: "id", NewName: "pk"},
	})
	if err == nil {
		t.Fatal("expected error on closed dataset")
	}
}

func TestAddColumns(t *testing.T) {
	basePath := t.TempDir()
	fields := []*storage2pb.Field{
		{Name: "id", Type: storage2pb.Field_LEAF, Id: 0, ParentId: -1, LogicalType: "int64", Nullable: false},
		{Name: "name", Type: storage2pb.Field_LEAF, Id: 1, ParentId: -1, LogicalType: "string", Nullable: true},
	}

	ds := createDatasetWithSchema(t, basePath, fields)
	defer ds.Close()
	ctx := context.Background()

	// Add a single column
	if err := ds.AddColumns(ctx, []ColumnAddition{
		{
			Field: &storage2pb.Field{
				Name:        "age",
				Type:        storage2pb.Field_LEAF,
				ParentId:    -1,
				LogicalType: "int32",
				Nullable:    true,
			},
			DefaultValue: "0",
		},
	}); err != nil {
		t.Fatal(err)
	}

	schema := ds.Schema()
	if len(schema) != 3 {
		t.Fatalf("after add: want 3 fields, got %d", len(schema))
	}

	ageField := ds.FieldByName("age")
	if ageField == nil {
		t.Fatal("added field 'age' not found")
	}
	if ageField.LogicalType != "int32" {
		t.Errorf("age type want 'int32' got '%s'", ageField.LogicalType)
	}
	if !ageField.Nullable {
		t.Error("age should be nullable")
	}

	// Add multiple columns at once
	if err := ds.AddColumns(ctx, []ColumnAddition{
		{
			Field: &storage2pb.Field{
				Name:        "email",
				Type:        storage2pb.Field_LEAF,
				ParentId:    -1,
				LogicalType: "string",
				Nullable:    true,
			},
			DefaultValue: "NULL",
		},
		{
			Field: &storage2pb.Field{
				Name:        "score",
				Type:        storage2pb.Field_LEAF,
				ParentId:    -1,
				LogicalType: "double",
				Nullable:    true,
			},
			DefaultValue: "0.0",
		},
	}); err != nil {
		t.Fatal(err)
	}

	schema = ds.Schema()
	if len(schema) != 5 {
		t.Fatalf("after adding 2 more: want 5 fields, got %d", len(schema))
	}

	if ds.FieldByName("email") == nil {
		t.Error("added field 'email' not found")
	}
	if ds.FieldByName("score") == nil {
		t.Error("added field 'score' not found")
	}
}

func TestAddColumnsErrors(t *testing.T) {
	basePath := t.TempDir()
	fields := []*storage2pb.Field{
		{Name: "id", Type: storage2pb.Field_LEAF, Id: 0, ParentId: -1, LogicalType: "int64"},
	}

	ds := createDatasetWithSchema(t, basePath, fields)
	defer ds.Close()
	ctx := context.Background()

	// Add duplicate column
	err := ds.AddColumns(ctx, []ColumnAddition{
		{
			Field: &storage2pb.Field{
				Name:        "id",
				Type:        storage2pb.Field_LEAF,
				LogicalType: "int64",
			},
		},
	})
	if err == nil {
		t.Fatal("expected error adding duplicate column")
	}

	// Empty addition list
	err = ds.AddColumns(ctx, []ColumnAddition{})
	if err == nil {
		t.Fatal("expected error with empty addition list")
	}

	// Add on closed dataset
	ds.Close()
	err = ds.AddColumns(ctx, []ColumnAddition{
		{
			Field: &storage2pb.Field{
				Name:        "new_col",
				Type:        storage2pb.Field_LEAF,
				LogicalType: "string",
			},
		},
	})
	if err == nil {
		t.Fatal("expected error on closed dataset")
	}
}

func TestAddColumnsWithSQLExpressions(t *testing.T) {
	basePath := t.TempDir()
	fields := []*storage2pb.Field{
		{Name: "id", Type: storage2pb.Field_LEAF, Id: 0, ParentId: -1, LogicalType: "int64"},
		{Name: "price", Type: storage2pb.Field_LEAF, Id: 1, ParentId: -1, LogicalType: "double"},
	}

	ds := createDatasetWithSchema(t, basePath, fields)
	defer ds.Close()
	ctx := context.Background()

	// Add columns with SQL expression defaults
	if err := ds.AddColumns(ctx, []ColumnAddition{
		{
			Field: &storage2pb.Field{
				Name:        "tax",
				Type:        storage2pb.Field_LEAF,
				ParentId:    -1,
				LogicalType: "double",
				Nullable:    true,
			},
			DefaultValue: "price * 0.1",
		},
		{
			Field: &storage2pb.Field{
				Name:        "total",
				Type:        storage2pb.Field_LEAF,
				ParentId:    -1,
				LogicalType: "double",
				Nullable:    true,
			},
			DefaultValue: "price * 1.1",
		},
	}); err != nil {
		t.Fatal(err)
	}

	schema := ds.Schema()
	if len(schema) != 4 {
		t.Fatalf("want 4 fields, got %d", len(schema))
	}

	taxField := ds.FieldByName("tax")
	if taxField == nil {
		t.Fatal("'tax' field not found")
	}
	totalField := ds.FieldByName("total")
	if totalField == nil {
		t.Fatal("'total' field not found")
	}
}

func TestDropPath(t *testing.T) {
	basePath := t.TempDir()
	fields := []*storage2pb.Field{
		{Name: "id", Type: storage2pb.Field_LEAF, Id: 0, ParentId: -1, LogicalType: "int64"},
		{Name: "address", Type: storage2pb.Field_PARENT, Id: 1, ParentId: -1, LogicalType: "struct"},
		{Name: "street", Type: storage2pb.Field_LEAF, Id: 2, ParentId: 1, LogicalType: "string"},
		{Name: "city", Type: storage2pb.Field_LEAF, Id: 3, ParentId: 1, LogicalType: "string"},
		{Name: "zip", Type: storage2pb.Field_LEAF, Id: 4, ParentId: 1, LogicalType: "string"},
		{Name: "name", Type: storage2pb.Field_LEAF, Id: 5, ParentId: -1, LogicalType: "string"},
	}

	ds := createDatasetWithSchema(t, basePath, fields)
	defer ds.Close()
	ctx := context.Background()

	// Drop a nested path: "address.city"
	if err := ds.DropPath(ctx, "address.city"); err != nil {
		t.Fatal(err)
	}

	// "city" should be gone, but "street" and "zip" remain
	schema := ds.Schema()
	if len(schema) != 5 { // id, address, street, zip, name
		t.Fatalf("after drop 'address.city': want 5 fields, got %d", len(schema))
	}

	for _, f := range schema {
		if f.Name == "city" {
			t.Error("'city' field should have been dropped")
		}
	}

	// Drop entire nested path: "address"
	if err := ds.DropPath(ctx, "address"); err != nil {
		t.Fatal(err)
	}

	schema = ds.Schema()
	if len(schema) != 2 { // id, name
		t.Fatalf("after drop 'address': want 2 fields, got %d", len(schema))
	}

	for _, f := range schema {
		if f.Name == "address" || f.Name == "street" || f.Name == "zip" {
			t.Errorf("field '%s' should have been dropped", f.Name)
		}
	}
}

func TestDropPathErrors(t *testing.T) {
	basePath := t.TempDir()
	fields := []*storage2pb.Field{
		{Name: "id", Type: storage2pb.Field_LEAF, Id: 0, ParentId: -1, LogicalType: "int64"},
	}

	ds := createDatasetWithSchema(t, basePath, fields)
	defer ds.Close()
	ctx := context.Background()

	// Non-existent path
	err := ds.DropPath(ctx, "nonexistent.path")
	if err == nil {
		t.Fatal("expected error for non-existent path")
	}

	// Empty path
	err = ds.DropPath(ctx, "")
	if err == nil {
		t.Fatal("expected error for empty path")
	}

	// Drop only field
	err = ds.DropPath(ctx, "id")
	if err == nil {
		t.Fatal("expected error dropping all fields")
	}

	// Drop on closed dataset
	ds.Close()
	err = ds.DropPath(ctx, "id")
	if err == nil {
		t.Fatal("expected error on closed dataset")
	}
}

func TestShallowClone(t *testing.T) {
	ctx := context.Background()
	srcPath := t.TempDir()

	// Create source dataset with schema and data
	handler := NewLocalRenameCommitHandler()
	fields := []*storage2pb.Field{
		{Name: "id", Type: storage2pb.Field_LEAF, Id: 0, ParentId: -1, LogicalType: "int64"},
		{Name: "value", Type: storage2pb.Field_LEAF, Id: 1, ParentId: -1, LogicalType: "string"},
	}
	manifest := storage2.NewManifest(0)
	manifest.Fields = fields
	manifest.Fragments = []*storage2pb.DataFragment{}
	manifest.NextRowId = 1

	if err := handler.Commit(ctx, srcPath, 0, manifest); err != nil {
		t.Fatal(err)
	}

	srcDS, err := OpenDataset(ctx, srcPath).Build()
	if err != nil {
		t.Fatal(err)
	}
	defer srcDS.Close()

	// Add data to source
	dataPath := filepath.Join(srcPath, "data", "0.dat")
	if err := storage2.WriteChunkToFile(dataPath, emptyChunk(t)); err != nil {
		t.Fatal(err)
	}
	df := NewDataFile("data/0.dat", []int32{0, 1}, 1, 0)
	frag := NewDataFragmentWithRows(0, 10, []*DataFile{df})
	if err := srcDS.Append(ctx, []*DataFragment{frag}); err != nil {
		t.Fatal(err)
	}

	// Shallow clone to target
	targetPath := t.TempDir()
	clonedDS, err := srcDS.ShallowClone(ctx, targetPath)
	if err != nil {
		t.Fatal(err)
	}
	defer clonedDS.Close()

	// Verify clone version
	if clonedDS.Version() < 1 {
		t.Errorf("cloned dataset should have version >= 1, got %d", clonedDS.Version())
	}

	// Verify schema is preserved
	clonedSchema := clonedDS.Schema()
	if len(clonedSchema) != 2 {
		t.Fatalf("cloned schema should have 2 fields, got %d", len(clonedSchema))
	}
}

func TestShallowCloneErrors(t *testing.T) {
	ctx := context.Background()
	srcPath := t.TempDir()

	ds := createDatasetWithSchema(t, srcPath, []*storage2pb.Field{
		{Name: "id", Type: storage2pb.Field_LEAF, Id: 0, ParentId: -1, LogicalType: "int64"},
	})

	// Clone on closed dataset
	ds.Close()
	_, err := ds.ShallowClone(ctx, t.TempDir())
	if err == nil {
		t.Fatal("expected error on closed dataset")
	}
}

func TestSchemaEvolutionCombined(t *testing.T) {
	// Test combining add, alter, and drop columns
	basePath := t.TempDir()
	fields := []*storage2pb.Field{
		{Name: "id", Type: storage2pb.Field_LEAF, Id: 0, ParentId: -1, LogicalType: "int64", Nullable: false},
		{Name: "name", Type: storage2pb.Field_LEAF, Id: 1, ParentId: -1, LogicalType: "string", Nullable: true},
	}

	ds := createDatasetWithSchema(t, basePath, fields)
	defer ds.Close()
	ctx := context.Background()

	// Step 1: Add a new column
	if err := ds.AddColumns(ctx, []ColumnAddition{
		{
			Field: &storage2pb.Field{
				Name: "age", Type: storage2pb.Field_LEAF,
				ParentId: -1, LogicalType: "int32", Nullable: true,
			},
		},
	}); err != nil {
		t.Fatal(err)
	}
	if len(ds.Schema()) != 3 {
		t.Fatalf("after add: want 3 fields, got %d", len(ds.Schema()))
	}

	// Step 2: Rename 'name' -> 'full_name'
	if err := ds.AlterColumns(ctx, []ColumnAlteration{
		{Path: "name", NewName: "full_name"},
	}); err != nil {
		t.Fatal(err)
	}
	if ds.FieldByName("full_name") == nil {
		t.Fatal("'full_name' not found after rename")
	}

	// Step 3: Drop 'age'
	if err := ds.DropColumns(ctx, []string{"age"}); err != nil {
		t.Fatal(err)
	}
	if len(ds.Schema()) != 2 {
		t.Fatalf("after drop: want 2 fields, got %d", len(ds.Schema()))
	}
	if ds.FieldByName("age") != nil {
		t.Error("'age' should be gone after drop")
	}

	// Final schema: id, full_name
	schema := ds.Schema()
	names := map[string]bool{}
	for _, f := range schema {
		names[f.Name] = true
	}
	if !names["id"] || !names["full_name"] {
		t.Errorf("final schema should have 'id' and 'full_name', got %v", names)
	}
}
