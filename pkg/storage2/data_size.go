package storage2

import "context"

// CalculateManifestDataSize returns the sum of FileSizeBytes over all data files
// referenced by the given manifest's fragments.
//
// This is a simple metadata-based computation and does not touch the filesystem.
func CalculateManifestDataSize(m *Manifest) uint64 {
	if m == nil {
		return 0
	}
	var total uint64
	for _, frag := range m.Fragments {
		if frag == nil {
			continue
		}
		for _, df := range frag.Files {
			if df == nil {
				continue
			}
			total += df.GetFileSizeBytes()
		}
	}
	return total
}

// CalculateDatasetDataSize loads the manifest for the given version and
// returns the sum of FileSizeBytes over all referenced data files.
func CalculateDatasetDataSize(ctx context.Context, basePath string, handler CommitHandler, version uint64) (uint64, error) {
	m, err := LoadManifest(ctx, basePath, handler, version)
	if err != nil {
		return 0, err
	}
	return CalculateManifestDataSize(m), nil
}

