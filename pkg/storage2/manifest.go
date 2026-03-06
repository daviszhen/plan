package storage2

import (
	"fmt"

	protobuf "google.golang.org/protobuf/proto"

	"github.com/daviszhen/plan/pkg/storage2/proto"
)

// Manifest is the version snapshot (schema, version, fragments, config, etc.).
// It is the same as the proto Manifest; use storage2pb.Manifest or via helpers here.
type Manifest = storage2pb.Manifest

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
