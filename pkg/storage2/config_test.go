package storage2

import (
	"testing"

	storage2pb "github.com/daviszhen/plan/pkg/storage2/proto"
)

func TestBuildManifestUpdateConfigUpsertAndDelete(t *testing.T) {
	current := NewManifest(1)
	current.Config = map[string]string{
		"a": "1",
		"b": "2",
	}

	update := &storage2pb.Transaction_UpdateConfig{}
	update.UpsertValues = map[string]string{
		"a": "10",  // overwrite existing
		"c": "3",   // new key
	}
	update.DeleteKeys = []string{"b"} // remove existing

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
	if next.Version != 2 {
		t.Fatalf("next.Version = %d, want 2", next.Version)
	}
	if got := next.Config["a"]; got != "10" {
		t.Errorf("config[a] = %q, want %q", got, "10")
	}
	if _, ok := next.Config["b"]; ok {
		t.Errorf("config[b] should be deleted")
	}
	if got := next.Config["c"]; got != "3" {
		t.Errorf("config[c] = %q, want %q", got, "3")
	}
}

