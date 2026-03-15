package storage2

import (
	"io"
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

// OpenWriter returns a writer for the given path, creating parent directories as needed.
func (s *LocalObjectStore) OpenWriter(path string) (io.WriteCloser, error) {
	full := filepath.Join(s.Root, path)
	if err := os.MkdirAll(filepath.Dir(full), 0755); err != nil {
		return nil, err
	}
	return os.Create(full)
}

// OpenReader returns a reader for the given path.
func (s *LocalObjectStore) OpenReader(path string) (io.ReadCloser, error) {
	return os.Open(filepath.Join(s.Root, path))
}

// GetSize returns the size of the file at the given path.
func (s *LocalObjectStore) GetSize(path string) (int64, error) {
	info, err := os.Stat(filepath.Join(s.Root, path))
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

// Exists returns true if the file at the given path exists.
func (s *LocalObjectStore) Exists(path string) (bool, error) {
	_, err := os.Stat(filepath.Join(s.Root, path))
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

// Delete removes the file at the given path.
func (s *LocalObjectStore) Delete(path string) error {
	return os.Remove(filepath.Join(s.Root, path))
}

// Copy copies a file from src to dst.
func (s *LocalObjectStore) Copy(src, dst string) error {
	srcFull := filepath.Join(s.Root, src)
	dstFull := filepath.Join(s.Root, dst)
	if err := os.MkdirAll(filepath.Dir(dstFull), 0755); err != nil {
		return err
	}
	in, err := os.Open(srcFull)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.Create(dstFull)
	if err != nil {
		return err
	}
	defer out.Close()
	_, err = io.Copy(out, in)
	return err
}

// Rename renames (moves) a file from src to dst.
func (s *LocalObjectStore) Rename(src, dst string) error {
	srcFull := filepath.Join(s.Root, src)
	dstFull := filepath.Join(s.Root, dst)
	if err := os.MkdirAll(filepath.Dir(dstFull), 0755); err != nil {
		return err
	}
	return os.Rename(srcFull, dstFull)
}
