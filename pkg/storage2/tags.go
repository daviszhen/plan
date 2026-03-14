package storage2

import (
	"context"
)

// ListTags returns a map from tag name to the versions that carry this tag.
// It scans manifests from version 0..latest using the provided CommitHandler.
func ListTags(ctx context.Context, basePath string, handler CommitHandler) (map[string][]uint64, error) {
	result := make(map[string][]uint64)

	latest, err := handler.ResolveLatestVersion(ctx, basePath)
	if err != nil {
		return nil, err
	}
	for v := uint64(0); v <= latest; v++ {
		m, err := LoadManifest(ctx, basePath, handler, v)
		if err != nil {
			// If a manifest is missing or unreadable we stop and return error,
			// since this indicates dataset corruption.
			return nil, err
		}
		if m.Tag != "" {
			result[m.Tag] = append(result[m.Tag], v)
		}
	}
	return result, nil
}

// ResolveTagVersion returns the latest version that has the given tag.
// If the tag does not exist, (0, false, nil) is returned.
func ResolveTagVersion(ctx context.Context, basePath string, handler CommitHandler, tag string) (version uint64, ok bool, err error) {
	tags, err := ListTags(ctx, basePath, handler)
	if err != nil {
		return 0, false, err
	}
	versions, exists := tags[tag]
	if !exists || len(versions) == 0 {
		return 0, false, nil
	}
	// Return the latest version carrying this tag.
	return versions[len(versions)-1], true, nil
}
