package storage2

import (
	"testing"

	storage2pb "github.com/daviszhen/plan/pkg/storage2/proto"
)

func TestBuildManifestUpdateConfigFieldMetadata(t *testing.T) {
	current := NewManifest(1)
	current.Config = map[string]string{
		"a": "1",
	}

	// Create an UpdateConfig with field_metadata_updates
	update := &storage2pb.Transaction_UpdateConfig{}

	// Add field metadata update for field ID 1
	fieldUpdates := make(map[int32]*storage2pb.Transaction_UpdateMap)
	fieldUpdates[1] = &storage2pb.Transaction_UpdateMap{
		UpdateEntries: []*storage2pb.Transaction_UpdateMapEntry{
			{
				Key:   "description",
				Value: ptrString("test field"),
			},
			{
				Key:   "unit",
				Value: ptrString("count"),
			},
		},
	}
	update.FieldMetadataUpdates = fieldUpdates

	txn := &Transaction{
		ReadVersion: 1,
		Operation: &storage2pb.Transaction_UpdateConfig_{
			UpdateConfig: update,
		},
	}

	next, err := BuildManifest(current, txn)
	if err != nil {
		t.Fatal(err)
	}

	// Verify version incremented
	if next.Version != 2 {
		t.Errorf("next.Version = %d, want 2", next.Version)
	}

	// Verify existing config preserved
	if got := next.Config["a"]; got != "1" {
		t.Errorf("config[a] = %q, want %q", got, "1")
	}

	// For now, field metadata updates should be ignored (not cause errors)
	// TODO: When field metadata support is implemented, add assertions here

	t.Log("Field metadata updates processed without error (currently ignored)")
}

func ptrString(s string) *string {
	return &s
}
