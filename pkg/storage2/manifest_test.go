package storage2

import (
	"testing"
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
