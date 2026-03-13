package storage2

import (
	"context"
	"testing"
)

func TestCalculateManifestDataSize(t *testing.T) {
	m := NewManifest(0)

	f0 := NewDataFragment(0, []*DataFile{
		{
			Path:          "data/0.dat",
			FileSizeBytes: 100,
		},
		{
			Path:          "data/1.dat",
			FileSizeBytes: 200,
		},
	})
	f1 := NewDataFragment(1, []*DataFile{
		{
			Path:          "data/2.dat",
			FileSizeBytes: 300,
		},
	})
	m.Fragments = []*DataFragment{f0, f1}

	if got := CalculateManifestDataSize(m); got != 600 {
		t.Fatalf("CalculateManifestDataSize = %d, want 600", got)
	}
}

func TestCalculateDatasetDataSize(t *testing.T) {
	ctx := context.Background()
	basePath := t.TempDir()
	handler := NewLocalRenameCommitHandler()

	// version 0 manifest with explicit FileSizeBytes values
	m0 := NewManifest(0)
	m0.Fragments = []*DataFragment{
		NewDataFragment(0, []*DataFile{
			{
				Path:          "data/0.dat",
				FileSizeBytes: 128,
			},
		}),
		NewDataFragment(1, []*DataFile{
			{
				Path:          "data/1.dat",
				FileSizeBytes: 256,
			},
		}),
	}
	if err := handler.Commit(ctx, basePath, 0, m0); err != nil {
		t.Fatal(err)
	}

	size, err := CalculateDatasetDataSize(ctx, basePath, handler, 0)
	if err != nil {
		t.Fatal(err)
	}
	if size != 384 {
		t.Fatalf("CalculateDatasetDataSize = %d, want 384", size)
	}
}

