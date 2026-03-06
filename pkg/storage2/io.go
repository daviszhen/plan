package storage2

import (
	"os"
	"path/filepath"
)

// ObjectStore abstracts storage for manifest and transaction files.
// Implementations can be local FS, S3, GCS, etc.
type ObjectStore interface {
	Read(path string) ([]byte, error)
	Write(path string, data []byte) error
	List(dir string) ([]string, error)
	MkdirAll(dir string) error
}

// LocalObjectStore uses the OS filesystem with a root base path.
type LocalObjectStore struct {
	Root string
}

// NewLocalObjectStore returns an ObjectStore under root.
func NewLocalObjectStore(root string) *LocalObjectStore {
	return &LocalObjectStore{Root: root}
}

func (s *LocalObjectStore) Read(path string) ([]byte, error) {
	return os.ReadFile(filepath.Join(s.Root, path))
}

func (s *LocalObjectStore) Write(path string, data []byte) error {
	full := filepath.Join(s.Root, path)
	if err := os.MkdirAll(filepath.Dir(full), 0755); err != nil {
		return err
	}
	return os.WriteFile(full, data, 0644)
}

func (s *LocalObjectStore) List(dir string) ([]string, error) {
	entries, err := os.ReadDir(filepath.Join(s.Root, dir))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	return names, nil
}

func (s *LocalObjectStore) MkdirAll(dir string) error {
	return os.MkdirAll(filepath.Join(s.Root, dir), 0755)
}
