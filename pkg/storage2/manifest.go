package storage2

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	protobuf "google.golang.org/protobuf/proto"

	"github.com/daviszhen/plan/pkg/storage2/proto"
)

// Manifest is the version snapshot (schema, version, fragments, config, etc.).
// It is the same as the proto Manifest; use storage2pb.Manifest or via helpers here.
type Manifest = storage2pb.Manifest

// IndexSection is the proto type for index metadata section
type IndexSection = storage2pb.IndexSection

// ProtoIndexMetadata is the proto type for index metadata
type ProtoIndexMetadata = storage2pb.IndexMetadata

// MarshalManifest serializes the manifest to Proto binary format.
func MarshalManifest(m *Manifest) ([]byte, error) {
	if m == nil {
		return nil, fmt.Errorf("manifest is nil")
	}
	return protobuf.Marshal(m)
}

// UnmarshalManifest deserializes the manifest from Proto binary format.
func UnmarshalManifest(data []byte) (*Manifest, error) {
	m := new(Manifest)
	if err := protobuf.Unmarshal(data, m); err != nil {
		return nil, err
	}
	return m, nil
}

// NewManifest creates a minimal manifest with the given version and empty fragments.
func NewManifest(version uint64) *Manifest {
	return &Manifest{
		Version:   version,
		Fragments: []*storage2pb.DataFragment{},
	}
}

// ManifestIndexManager provides helper methods to manage index metadata in manifests
type ManifestIndexManager struct {
	basePath string
	handler  CommitHandler
}

// NewManifestIndexManager creates a new ManifestIndexManager
func NewManifestIndexManager(basePath string, handler CommitHandler) *ManifestIndexManager {
	return &ManifestIndexManager{
		basePath: basePath,
		handler:  handler,
	}
}

// indexSectionPath returns the path to the index section file
func (m *ManifestIndexManager) indexSectionPath() string {
	return filepath.Join(m.basePath, "_indices", "index_section.json")
}

// LoadIndexSection loads the index section from disk
func (m *ManifestIndexManager) LoadIndexSection(ctx context.Context) (*IndexSection, error) {
	data, err := os.ReadFile(m.indexSectionPath())
	if err != nil {
		if os.IsNotExist(err) {
			return &IndexSection{Indices: []*ProtoIndexMetadata{}}, nil
		}
		return nil, fmt.Errorf("failed to read index section: %w", err)
	}

	// Use JSON for easier debugging; could use protobuf for production
	var indices []*IndexMetadata
	if err := json.Unmarshal(data, &indices); err != nil {
		return nil, fmt.Errorf("failed to unmarshal index section: %w", err)
	}

	// Convert to proto format
	section := &IndexSection{
		Indices: make([]*ProtoIndexMetadata, len(indices)),
	}
	for i, idx := range indices {
		section.Indices[i] = &ProtoIndexMetadata{
			Name:           idx.Name,
			DatasetVersion: idx.Version,
		}
		for _, col := range idx.ColumnIndices {
			section.Indices[i].Fields = append(section.Indices[i].Fields, int32(col))
		}
	}

	return section, nil
}

// SaveIndexSection saves the index section to disk
func (m *ManifestIndexManager) SaveIndexSection(ctx context.Context, section *IndexSection) error {
	// Convert to JSON-friendly format
	indices := make([]*IndexMetadata, len(section.Indices))
	for i, idx := range section.Indices {
		cols := make([]int, len(idx.Fields))
		for j, f := range idx.Fields {
			cols[j] = int(f)
		}
		indices[i] = &IndexMetadata{
			Name:          idx.Name,
			Version:       idx.DatasetVersion,
			ColumnIndices: cols,
			Type:          ScalarIndex, // Default type
		}
	}

	data, err := json.MarshalIndent(indices, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal index section: %w", err)
	}

	dir := filepath.Dir(m.indexSectionPath())
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create index directory: %w", err)
	}

	if err := os.WriteFile(m.indexSectionPath(), data, 0644); err != nil {
		return fmt.Errorf("failed to write index section: %w", err)
	}

	return nil
}

// AddIndex adds an index to the index section
func (m *ManifestIndexManager) AddIndex(ctx context.Context, metadata *IndexMetadata) error {
	section, err := m.LoadIndexSection(ctx)
	if err != nil {
		return err
	}

	// Check for duplicate
	for _, idx := range section.Indices {
		if idx.Name == metadata.Name {
			return fmt.Errorf("index %q already exists", metadata.Name)
		}
	}

	// Add new index
	protoMeta := &ProtoIndexMetadata{
		Name:           metadata.Name,
		DatasetVersion: metadata.Version,
	}
	for _, col := range metadata.ColumnIndices {
		protoMeta.Fields = append(protoMeta.Fields, int32(col))
	}
	section.Indices = append(section.Indices, protoMeta)

	return m.SaveIndexSection(ctx, section)
}

// RemoveIndex removes an index from the index section
func (m *ManifestIndexManager) RemoveIndex(ctx context.Context, name string) error {
	section, err := m.LoadIndexSection(ctx)
	if err != nil {
		return err
	}

	found := false
	var newIndices []*ProtoIndexMetadata
	for _, idx := range section.Indices {
		if idx.Name == name {
			found = true
			continue
		}
		newIndices = append(newIndices, idx)
	}

	if !found {
		return fmt.Errorf("index %q not found", name)
	}

	section.Indices = newIndices
	return m.SaveIndexSection(ctx, section)
}

// ListIndexes lists all indexes in the index section
func (m *ManifestIndexManager) ListIndexes(ctx context.Context) ([]*IndexMetadata, error) {
	section, err := m.LoadIndexSection(ctx)
	if err != nil {
		return nil, err
	}

	result := make([]*IndexMetadata, len(section.Indices))
	for i, idx := range section.Indices {
		cols := make([]int, len(idx.Fields))
		for j, f := range idx.Fields {
			cols[j] = int(f)
		}
		result[i] = &IndexMetadata{
			Name:          idx.Name,
			Version:       idx.DatasetVersion,
			ColumnIndices: cols,
			Type:          ScalarIndex,
		}
	}

	return result, nil
}

// GetIndex retrieves a specific index by name
func (m *ManifestIndexManager) GetIndex(ctx context.Context, name string) (*IndexMetadata, error) {
	section, err := m.LoadIndexSection(ctx)
	if err != nil {
		return nil, err
	}

	for _, idx := range section.Indices {
		if idx.Name == name {
			cols := make([]int, len(idx.Fields))
			for j, f := range idx.Fields {
				cols[j] = int(f)
			}
			return &IndexMetadata{
				Name:          idx.Name,
				Version:       idx.DatasetVersion,
				ColumnIndices: cols,
				Type:          ScalarIndex,
			}, nil
		}
	}

	return nil, fmt.Errorf("index %q not found", name)
}
