package storage2

import (
	"context"
	"testing"

	"github.com/daviszhen/plan/pkg/storage2/proto"
)

func TestManifestRoundTrip(t *testing.T) {
	m := NewManifest(1)
	m.NextRowId = 100
	df := NewDataFile("data/0.parquet", []int32{0, 1}, 1, 0)
	frag := NewDataFragment(0, []*DataFile{df})
	m.Fragments = []*DataFragment{frag}

	data, err := MarshalManifest(m)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) == 0 {
		t.Fatal("MarshalManifest returned empty bytes")
	}

	m2, err := UnmarshalManifest(data)
	if err != nil {
		t.Fatal(err)
	}
	if m2.Version != m.Version {
		t.Errorf("version: got %d want %d", m2.Version, m.Version)
	}
	if m2.NextRowId != m.NextRowId {
		t.Errorf("next_row_id: got %d want %d", m2.NextRowId, m.NextRowId)
	}
	if len(m2.Fragments) != 1 {
		t.Fatalf("fragments: got %d want 1", len(m2.Fragments))
	}
	if m2.Fragments[0].Id != frag.Id {
		t.Errorf("fragment id: got %d want %d", m2.Fragments[0].Id, frag.Id)
	}
	if len(m2.Fragments[0].Files) != 1 {
		t.Fatalf("fragment files: got %d want 1", len(m2.Fragments[0].Files))
	}
	if m2.Fragments[0].Files[0].Path != df.Path {
		t.Errorf("data file path: got %q want %q", m2.Fragments[0].Files[0].Path, df.Path)
	}
}

func TestBuildManifestAppendFromManifestTest(t *testing.T) {
	m := NewManifest(1)
	m.Fragments = []*DataFragment{NewDataFragment(0, []*DataFile{NewDataFile("a.parquet", []int32{0}, 1, 0)})}
	maxZero := uint32(0)
	m.MaxFragmentId = &maxZero
	txn := NewTransactionAppend(1, "u", []*DataFragment{NewDataFragment(0, []*DataFile{NewDataFile("b.parquet", []int32{0}, 1, 0)})})
	next, err := BuildManifest(m, txn)
	if err != nil {
		t.Fatal(err)
	}
	if next.Version != 2 || len(next.Fragments) != 2 || next.Fragments[1].Id != 1 {
		t.Errorf("BuildManifest Append: version=%d nfrag=%d", next.Version, len(next.Fragments))
	}
}

func TestUnmarshalManifestNil(t *testing.T) {
	_, err := MarshalManifest(nil)
	if err == nil {
		t.Fatal("expected error when marshaling nil manifest")
	}
}

func TestBuildManifestOverwriteWithInitialBases(t *testing.T) {
	current := NewManifest(1)
	current.Fragments = []*DataFragment{NewDataFragment(0, nil)}
	base := NewBasePath(1, "base/path", nil, true)
	txn := NewTransactionOverwrite(1, "ow", nil, nil, nil)
	if ow, ok := txn.Operation.(*storage2pb.Transaction_Overwrite_); ok && ow.Overwrite != nil {
		ow.Overwrite.InitialBases = []*storage2pb.BasePath{base}
	}
	next, err := BuildManifest(current, txn)
	if err != nil {
		t.Fatal(err)
	}
	if len(next.BasePaths) != 1 || next.BasePaths[0].Id != 1 || next.BasePaths[0].Path != "base/path" {
		t.Errorf("BasePaths: got %v", next.BasePaths)
	}
}

func TestManifestRoundtripFull(t *testing.T) {
	original := &Manifest{
		Version:            5,
		Fields:             []*storage2pb.Field{{Id: 1, Name: "col1"}, {Id: 2, Name: "col2"}},
		Fragments:          []*DataFragment{NewDataFragmentWithRows(0, 1000, []*DataFile{NewDataFile("f0.lance", []int32{1, 2}, 1, 0)})},
		MaxFragmentId:      ptrUint32(0),
		ReaderFeatureFlags: 1,
		WriterFeatureFlags: 3,
		Tag:                "v5-release",
		Config:             map[string]string{"key": "val"},
		TransactionFile:    "4-uuid1.txn",
	}

	data, err := MarshalManifest(original)
	if err != nil {
		t.Fatalf("MarshalManifest: %v", err)
	}
	if len(data) == 0 {
		t.Fatal("MarshalManifest returned empty data")
	}

	restored, err := UnmarshalManifest(data)
	if err != nil {
		t.Fatalf("UnmarshalManifest: %v", err)
	}

	// Verify all fields
	if restored.Version != original.Version {
		t.Errorf("Version: got %d want %d", restored.Version, original.Version)
	}
	if len(restored.Fields) != 2 {
		t.Errorf("Fields count: got %d want 2", len(restored.Fields))
	}
	if restored.Fields[0].Name != "col1" || restored.Fields[1].Name != "col2" {
		t.Error("Fields name mismatch")
	}
	if len(restored.Fragments) != 1 {
		t.Fatalf("Fragments count: got %d want 1", len(restored.Fragments))
	}
	if restored.Fragments[0].PhysicalRows != 1000 {
		t.Errorf("Fragment PhysicalRows: got %d want 1000", restored.Fragments[0].PhysicalRows)
	}
	if restored.Tag != "v5-release" {
		t.Errorf("Tag: got %q want %q", restored.Tag, "v5-release")
	}
	if restored.Config["key"] != "val" {
		t.Errorf("Config: got %v", restored.Config)
	}
	if restored.ReaderFeatureFlags != 1 || restored.WriterFeatureFlags != 3 {
		t.Errorf("FeatureFlags: reader=%d writer=%d", restored.ReaderFeatureFlags, restored.WriterFeatureFlags)
	}
	if restored.TransactionFile != "4-uuid1.txn" {
		t.Errorf("TransactionFile: got %q", restored.TransactionFile)
	}
}

func TestCurrentManifestPath(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	handler := NewLocalRenameCommitHandler()

	// Empty dataset - no versions
	latest, err := handler.ResolveLatestVersion(ctx, dir)
	if err != nil {
		t.Fatalf("ResolveLatestVersion empty: %v", err)
	}
	if latest != 0 {
		t.Errorf("Empty dataset latest: got %d want 0", latest)
	}

	// Create versions 1, 2, 3
	for v := uint64(1); v <= 3; v++ {
		m := NewManifest(v)
		if err := handler.Commit(ctx, dir, v, m); err != nil {
			t.Fatalf("Commit v%d: %v", v, err)
		}
	}

	// Latest should be 3
	latest, err = handler.ResolveLatestVersion(ctx, dir)
	if err != nil {
		t.Fatalf("ResolveLatestVersion: %v", err)
	}
	if latest != 3 {
		t.Errorf("Latest version: got %d want 3", latest)
	}

	// Can load specific version
	m2, err := LoadManifest(ctx, dir, handler, 2)
	if err != nil {
		t.Fatalf("LoadManifest v2: %v", err)
	}
	if m2.Version != 2 {
		t.Errorf("Loaded manifest version: got %d want 2", m2.Version)
	}

	// Non-existent version should fail
	_, err = LoadManifest(ctx, dir, handler, 99)
	if err == nil {
		t.Error("LoadManifest non-existent should fail")
	}
}
