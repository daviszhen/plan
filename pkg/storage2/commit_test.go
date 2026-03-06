package storage2

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestLocalRenameCommit(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	h := NewLocalRenameCommitHandler()

	v, err := h.ResolveLatestVersion(ctx, dir)
	if err != nil || v != 0 {
		t.Fatalf("ResolveLatestVersion empty: got %d %v", v, err)
	}

	m := NewManifest(1)
	m.NextRowId = 10
	if err := h.Commit(ctx, dir, 1, m); err != nil {
		t.Fatal(err)
	}
	path, err := h.ResolveVersion(ctx, dir, 1)
	if err != nil {
		t.Fatal(err)
	}
	fullPath := filepath.Join(dir, path)
	data, err := os.ReadFile(fullPath)
	if err != nil {
		t.Fatal(err)
	}
	m2, err := UnmarshalManifest(data)
	if err != nil {
		t.Fatal(err)
	}
	if m2.Version != 1 || m2.NextRowId != 10 {
		t.Errorf("committed manifest: version=%d next_row_id=%d", m2.Version, m2.NextRowId)
	}

	v, err = h.ResolveLatestVersion(ctx, dir)
	if err != nil || v != 1 {
		t.Errorf("ResolveLatestVersion after commit: got %d %v", v, err)
	}
}
