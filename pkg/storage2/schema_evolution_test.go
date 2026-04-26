package storage2

import (
	"testing"

	storage2pb "github.com/daviszhen/plan/pkg/storage2/proto"
)

func TestBuildManifestProject(t *testing.T) {
	// Setup: manifest with 4 fields
	current := NewManifest(1)
	current.Fields = []*storage2pb.Field{
		{Name: "id", Type: storage2pb.Field_LEAF, Id: 0, ParentId: -1, LogicalType: "int64"},
		{Name: "name", Type: storage2pb.Field_LEAF, Id: 1, ParentId: -1, LogicalType: "string"},
		{Name: "age", Type: storage2pb.Field_LEAF, Id: 2, ParentId: -1, LogicalType: "int32"},
		{Name: "email", Type: storage2pb.Field_LEAF, Id: 3, ParentId: -1, LogicalType: "string"},
	}
	current.Fragments = []*DataFragment{
		NewDataFragmentWithRows(0, 10, []*DataFile{
			NewDataFile("data/0.dat", []int32{0, 1, 2, 3}, 1, 0),
		}),
	}

	// Project: keep only id and name (drop age and email)
	projectedSchema := []*storage2pb.Field{
		{Name: "id", Type: storage2pb.Field_LEAF, Id: 0, ParentId: -1, LogicalType: "int64"},
		{Name: "name", Type: storage2pb.Field_LEAF, Id: 1, ParentId: -1, LogicalType: "string"},
	}

	txn := NewTransactionProject(1, "test-uuid-1", projectedSchema)
	next, err := BuildManifest(current, txn)
	if err != nil {
		t.Fatal(err)
	}

	if next.Version != 2 {
		t.Errorf("version want 2 got %d", next.Version)
	}
	if len(next.Fields) != 2 {
		t.Fatalf("fields want 2 got %d", len(next.Fields))
	}
	if next.Fields[0].Name != "id" || next.Fields[1].Name != "name" {
		t.Errorf("fields want [id, name] got [%s, %s]", next.Fields[0].Name, next.Fields[1].Name)
	}

	// Check that fragment data files are updated
	if len(next.Fragments) != 1 {
		t.Fatalf("fragments want 1 got %d", len(next.Fragments))
	}
	if len(next.Fragments[0].Files) != 1 {
		t.Fatalf("files want 1 got %d", len(next.Fragments[0].Files))
	}
	fields := next.Fragments[0].Files[0].Fields
	if len(fields) != 2 {
		t.Fatalf("file fields want 2 got %d", len(fields))
	}
	if fields[0] != 0 || fields[1] != 1 {
		t.Errorf("file fields want [0, 1] got %v", fields)
	}
}

func TestBuildManifestProjectVersionMismatch(t *testing.T) {
	current := NewManifest(5)
	current.Fields = []*storage2pb.Field{
		{Name: "id", Type: storage2pb.Field_LEAF, Id: 0, LogicalType: "int64"},
	}

	txn := NewTransactionProject(3, "uuid", current.Fields) // read_version mismatch
	_, err := BuildManifest(current, txn)
	if err == nil {
		t.Fatal("expected error for version mismatch")
	}
}

func TestBuildManifestMerge(t *testing.T) {
	current := NewManifest(1)
	current.Fields = []*storage2pb.Field{
		{Name: "id", Type: storage2pb.Field_LEAF, Id: 0, ParentId: -1, LogicalType: "int64"},
		{Name: "name", Type: storage2pb.Field_LEAF, Id: 1, ParentId: -1, LogicalType: "string"},
	}
	current.Fragments = []*DataFragment{
		NewDataFragmentWithRows(0, 10, []*DataFile{
			NewDataFile("data/0.dat", []int32{0, 1}, 1, 0),
		}),
	}

	// Merge: add a new column "age"
	newSchema := []*storage2pb.Field{
		{Name: "id", Type: storage2pb.Field_LEAF, Id: 0, ParentId: -1, LogicalType: "int64"},
		{Name: "name", Type: storage2pb.Field_LEAF, Id: 1, ParentId: -1, LogicalType: "string"},
		{Name: "age", Type: storage2pb.Field_LEAF, Id: 2, ParentId: -1, LogicalType: "int32"},
	}

	// The merge includes updated fragments with new data files for the new column
	mergedFragments := []*DataFragment{
		{
			Id: 0,
			Files: []*DataFile{
				NewDataFile("data/0_age.dat", []int32{2}, 1, 0),
			},
		},
	}

	txn := NewTransactionMerge(1, "test-uuid-2", mergedFragments, newSchema, nil)
	next, err := BuildManifest(current, txn)
	if err != nil {
		t.Fatal(err)
	}

	if next.Version != 2 {
		t.Errorf("version want 2 got %d", next.Version)
	}
	if len(next.Fields) != 3 {
		t.Fatalf("fields want 3 got %d", len(next.Fields))
	}
	if next.Fields[2].Name != "age" {
		t.Errorf("new field want 'age' got '%s'", next.Fields[2].Name)
	}

	// Fragment should now have 2 data files
	if len(next.Fragments) != 1 {
		t.Fatalf("fragments want 1 got %d", len(next.Fragments))
	}
	if len(next.Fragments[0].Files) != 2 {
		t.Fatalf("fragment files want 2 got %d", len(next.Fragments[0].Files))
	}
}

func TestBuildManifestMergeWithSchemaMetadata(t *testing.T) {
	current := NewManifest(1)
	current.Fields = []*storage2pb.Field{
		{Name: "id", Type: storage2pb.Field_LEAF, Id: 0, LogicalType: "int64"},
	}

	newSchema := []*storage2pb.Field{
		{Name: "id", Type: storage2pb.Field_LEAF, Id: 0, LogicalType: "int64"},
		{Name: "val", Type: storage2pb.Field_LEAF, Id: 1, LogicalType: "string"},
	}
	schemaMeta := map[string][]byte{
		"version": []byte("2.0"),
	}

	txn := NewTransactionMerge(1, "uuid", nil, newSchema, schemaMeta)
	next, err := BuildManifest(current, txn)
	if err != nil {
		t.Fatal(err)
	}

	if len(next.Fields) != 2 {
		t.Fatalf("fields want 2 got %d", len(next.Fields))
	}
	if string(next.SchemaMetadata["version"]) != "2.0" {
		t.Errorf("schema metadata 'version' want '2.0' got '%s'", string(next.SchemaMetadata["version"]))
	}
}

func TestBuildManifestCloneShallow(t *testing.T) {
	current := NewManifest(1)
	current.Fields = []*storage2pb.Field{
		{Name: "id", Type: storage2pb.Field_LEAF, Id: 0, LogicalType: "int64"},
	}
	current.Fragments = []*DataFragment{
		NewDataFragmentWithRows(0, 10, []*DataFile{
			NewDataFile("data/0.dat", []int32{0}, 1, 0),
		}),
	}

	refName := "source-v1"
	txn := NewTransactionClone(1, "uuid-clone", true, &refName, 1, "/path/to/source")
	next, err := BuildManifest(current, txn)
	if err != nil {
		t.Fatal(err)
	}

	if next.Version != 2 {
		t.Errorf("version want 2 got %d", next.Version)
	}

	// Verify base path was added
	if len(next.BasePaths) != 1 {
		t.Fatalf("base paths want 1 got %d", len(next.BasePaths))
	}
	if next.BasePaths[0].Path != "/path/to/source" {
		t.Errorf("base path want '/path/to/source' got '%s'", next.BasePaths[0].Path)
	}
	if !next.BasePaths[0].IsDatasetRoot {
		t.Error("base path should be dataset root")
	}
	if next.BasePaths[0].GetName() != "source-v1" {
		t.Errorf("base path name want 'source-v1' got '%s'", next.BasePaths[0].GetName())
	}

	// Verify fragments reference the base path
	for _, frag := range next.Fragments {
		for _, file := range frag.Files {
			if file.BaseId == nil || *file.BaseId != 0 {
				t.Errorf("file should reference base_id 0, got %v", file.BaseId)
			}
		}
	}
}

func TestNewTransactionProject(t *testing.T) {
	schema := []*storage2pb.Field{
		{Name: "id", Type: storage2pb.Field_LEAF, Id: 0, LogicalType: "int64"},
	}

	txn := NewTransactionProject(5, "uuid-proj", schema)
	if txn.ReadVersion != 5 {
		t.Errorf("read version want 5 got %d", txn.ReadVersion)
	}
	if txn.Uuid != "uuid-proj" {
		t.Errorf("uuid want 'uuid-proj' got '%s'", txn.Uuid)
	}

	projectOp, ok := txn.Operation.(*storage2pb.Transaction_Project_)
	if !ok {
		t.Fatal("operation should be Project")
	}
	if len(projectOp.Project.Schema) != 1 {
		t.Fatalf("schema want 1 field got %d", len(projectOp.Project.Schema))
	}
}

func TestNewTransactionMerge(t *testing.T) {
	schema := []*storage2pb.Field{
		{Name: "id", Type: storage2pb.Field_LEAF, Id: 0, LogicalType: "int64"},
		{Name: "val", Type: storage2pb.Field_LEAF, Id: 1, LogicalType: "string"},
	}
	meta := map[string][]byte{"key": []byte("val")}

	txn := NewTransactionMerge(3, "uuid-merge", nil, schema, meta)
	if txn.ReadVersion != 3 {
		t.Errorf("read version want 3 got %d", txn.ReadVersion)
	}

	mergeOp, ok := txn.Operation.(*storage2pb.Transaction_Merge_)
	if !ok {
		t.Fatal("operation should be Merge")
	}
	if len(mergeOp.Merge.Schema) != 2 {
		t.Fatalf("schema want 2 fields got %d", len(mergeOp.Merge.Schema))
	}
	if string(mergeOp.Merge.SchemaMetadata["key"]) != "val" {
		t.Errorf("schema metadata 'key' want 'val' got '%s'", string(mergeOp.Merge.SchemaMetadata["key"]))
	}
}

func TestNewTransactionClone(t *testing.T) {
	refName := "v1-tag"
	txn := NewTransactionClone(10, "uuid-clone", true, &refName, 5, "/src/path")
	if txn.ReadVersion != 10 {
		t.Errorf("read version want 10 got %d", txn.ReadVersion)
	}

	cloneOp, ok := txn.Operation.(*storage2pb.Transaction_Clone_)
	if !ok {
		t.Fatal("operation should be Clone")
	}
	if !cloneOp.Clone.IsShallow {
		t.Error("should be shallow clone")
	}
	if cloneOp.Clone.GetRefName() != "v1-tag" {
		t.Errorf("ref name want 'v1-tag' got '%s'", cloneOp.Clone.GetRefName())
	}
	if cloneOp.Clone.RefVersion != 5 {
		t.Errorf("ref version want 5 got %d", cloneOp.Clone.RefVersion)
	}
	if cloneOp.Clone.RefPath != "/src/path" {
		t.Errorf("ref path want '/src/path' got '%s'", cloneOp.Clone.RefPath)
	}
}
