package storage2

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"
)

// StoreFactory creates ObjectStore instances based on URI
type StoreFactory struct {
	mu      sync.RWMutex
	stores  map[string]ObjectStoreExt // cache by URI string
	options StoreFactoryOptions
}

// StoreFactoryOptions contains options for StoreFactory
type StoreFactoryOptions struct {
	// DefaultRegion is the default cloud region
	DefaultRegion string
	// S3CredentialsProvider provides S3 credentials
	S3CredentialsProvider func() *S3Credentials
	// GSCredentialsProvider provides GCS credentials
	GSCredentialsProvider func() *GSCredentials
	// AZCredentialsProvider provides Azure credentials
	AZCredentialsProvider func() *AZCredentials
	// EnableCache enables caching of store instances
	EnableCache bool
}

// NewStoreFactory creates a new StoreFactory
func NewStoreFactory(opts StoreFactoryOptions) *StoreFactory {
	return &StoreFactory{
		stores:  make(map[string]ObjectStoreExt),
		options: opts,
	}
}

// GetStore returns an ObjectStore for the given URI
// It creates a new store or returns a cached one
func (f *StoreFactory) GetStore(ctx context.Context, uri string) (ObjectStoreExt, error) {
	parsed, err := ParseURI(uri)
	if err != nil {
		return nil, err
	}

	// Check cache
	if f.options.EnableCache {
		f.mu.RLock()
		if store, ok := f.stores[uri]; ok {
			f.mu.RUnlock()
			return store, nil
		}
		f.mu.RUnlock()
	}

	// Create new store
	var store ObjectStoreExt
	switch parsed.Scheme {
	case "file", "":
		store = NewLocalObjectStoreExt(parsed.Path, nil)

	case "s3":
		region := parsed.Region
		if region == "" {
			region = f.options.DefaultRegion
			if region == "" {
				region = "us-east-1"
			}
		}

		var creds *S3Credentials
		if f.options.S3CredentialsProvider != nil {
			creds = f.options.S3CredentialsProvider()
		}

		s3Store, err := NewS3ObjectStore(ctx, S3ObjectStoreOptions{
			Bucket:         parsed.Bucket,
			Prefix:         parsed.Path,
			Region:         region,
			Endpoint:       parsed.Endpoint,
			ForcePathStyle: parsed.Endpoint != "", // Force path style for custom endpoints
			Credentials:    creds,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create S3 store: %w", err)
		}
		store = &s3ObjectStoreExt{s3Store}

	case "gs":
		// TODO: Implement GCS store
		return nil, fmt.Errorf("GCS storage not yet implemented")

	case "az":
		// TODO: Implement Azure store
		return nil, fmt.Errorf("Azure storage not yet implemented")

	case "mem":
		// In-memory store for testing
		store = NewMemoryObjectStore(parsed.Path)

	default:
		return nil, fmt.Errorf("unsupported storage scheme: %s", parsed.Scheme)
	}

	// Cache the store
	if f.options.EnableCache {
		f.mu.Lock()
		f.stores[uri] = store
		f.mu.Unlock()
	}

	return store, nil
}

// GetCommitHandler returns a CommitHandler for the given URI
func (f *StoreFactory) GetCommitHandler(ctx context.Context, uri string) (CommitHandler, error) {
	parsed, err := ParseURI(uri)
	if err != nil {
		return nil, err
	}

	switch parsed.Scheme {
	case "file", "":
		return NewLocalRenameCommitHandler(), nil

	case "s3":
		store, err := f.GetStore(ctx, uri)
		if err != nil {
			return nil, err
		}
		s3Store := store.(*s3ObjectStoreExt).S3ObjectStore
		return NewS3CommitHandlerWithLock(s3Store), nil

	case "mem":
		return NewMemoryCommitHandler(), nil

	default:
		return nil, fmt.Errorf("unsupported storage scheme for commit: %s", parsed.Scheme)
	}
}

// s3ObjectStoreExt wraps S3ObjectStore to implement ObjectStoreExt
type s3ObjectStoreExt struct {
	*S3ObjectStore
}

// MemoryObjectStore is an in-memory implementation for testing
type MemoryObjectStore struct {
	mu     sync.RWMutex
	data   map[string][]byte
	prefix string
}

// NewMemoryObjectStore creates a new MemoryObjectStore
func NewMemoryObjectStore(prefix string) *MemoryObjectStore {
	return &MemoryObjectStore{
		data:   make(map[string][]byte),
		prefix: prefix,
	}
}

func (m *MemoryObjectStore) fullKey(path string) string {
	if m.prefix == "" {
		return path
	}
	return m.prefix + "/" + path
}

// Read implements ObjectStore
func (m *MemoryObjectStore) Read(path string) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	key := m.fullKey(path)
	data, ok := m.data[key]
	if !ok {
		return nil, fmt.Errorf("object not found: %s", key)
	}
	// Return a copy
	result := make([]byte, len(data))
	copy(result, data)
	return result, nil
}

// Write implements ObjectStore
func (m *MemoryObjectStore) Write(path string, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := m.fullKey(path)
	// Store a copy
	m.data[key] = make([]byte, len(data))
	copy(m.data[key], data)
	return nil
}

// List implements ObjectStore
func (m *MemoryObjectStore) List(dir string) ([]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	prefix := m.fullKey(dir)
	if prefix != "" && prefix[len(prefix)-1] != '/' {
		prefix += "/"
	}

	var names []string
	seen := make(map[string]bool)
	for key := range m.data {
		if len(key) >= len(prefix) && key[:len(prefix)] == prefix {
			rest := key[len(prefix):]
			// Get the first component
			if idx := findFirstSlash(rest); idx >= 0 {
				rest = rest[:idx]
			}
			if rest != "" && !seen[rest] {
				names = append(names, rest)
				seen[rest] = true
			}
		}
	}
	return names, nil
}

// MkdirAll implements ObjectStore
func (m *MemoryObjectStore) MkdirAll(dir string) error {
	return nil
}

// ReadRange implements ObjectStoreExt
func (m *MemoryObjectStore) ReadRange(ctx context.Context, path string, opts ReadOptions) ([]byte, error) {
	data, err := m.Read(path)
	if err != nil {
		return nil, err
	}

	if opts.Offset < 0 {
		opts.Offset = 0
	}
	if opts.Offset > int64(len(data)) {
		return nil, nil
	}

	end := int64(len(data))
	if opts.Length > 0 && opts.Offset+opts.Length < end {
		end = opts.Offset + opts.Length
	}

	return data[opts.Offset:end], nil
}

// ReadStream implements ObjectStoreExt
func (m *MemoryObjectStore) ReadStream(ctx context.Context, path string, opts ReadOptions) (io.ReadCloser, error) {
	data, err := m.Read(path)
	if err != nil {
		return nil, err
	}

	if opts.Offset > 0 {
		if opts.Offset > int64(len(data)) {
			return io.NopCloser(bytes.NewReader(nil)), nil
		}
		data = data[opts.Offset:]
	}

	if opts.Length > 0 && opts.Length < int64(len(data)) {
		data = data[:opts.Length]
	}

	return io.NopCloser(bytes.NewReader(data)), nil
}

// WriteStream implements ObjectStoreExt
func (m *MemoryObjectStore) WriteStream(ctx context.Context, path string, opts WriteOptions) (io.WriteCloser, error) {
	return &memoryWriter{store: m, path: path}, nil
}

// Copy implements ObjectStoreExt
func (m *MemoryObjectStore) Copy(ctx context.Context, src, dst string) error {
	data, err := m.Read(src)
	if err != nil {
		return err
	}
	return m.Write(dst, data)
}

// Rename implements ObjectStoreExt
func (m *MemoryObjectStore) Rename(ctx context.Context, src, dst string) error {
	if err := m.Copy(ctx, src, dst); err != nil {
		return err
	}
	return m.Delete(ctx, src)
}

// Delete removes an object
func (m *MemoryObjectStore) Delete(ctx context.Context, path string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.data, m.fullKey(path))
	return nil
}

// Exists checks if an object exists
func (m *MemoryObjectStore) Exists(ctx context.Context, path string) (bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, ok := m.data[m.fullKey(path)]
	return ok, nil
}

// GetSize implements ObjectStoreExt
func (m *MemoryObjectStore) GetSize(ctx context.Context, path string) (int64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	data, ok := m.data[m.fullKey(path)]
	if !ok {
		return 0, fmt.Errorf("object not found")
	}
	return int64(len(data)), nil
}

// GetETag implements ObjectStoreExt
func (m *MemoryObjectStore) GetETag(ctx context.Context, path string) (string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	data, ok := m.data[m.fullKey(path)]
	if !ok {
		return "", fmt.Errorf("object not found")
	}
	return fmt.Sprintf("%x", len(data)), nil
}

// memoryWriter implements io.WriteCloser for MemoryObjectStore
type memoryWriter struct {
	store *MemoryObjectStore
	path  string
	buf   bytes.Buffer
}

func (w *memoryWriter) Write(p []byte) (int, error) {
	return w.buf.Write(p)
}

func (w *memoryWriter) Close() error {
	return w.store.Write(w.path, w.buf.Bytes())
}

// MemoryCommitHandler implements CommitHandler for MemoryObjectStore
type MemoryCommitHandler struct {
	store *MemoryObjectStore
}

// NewMemoryCommitHandler creates a new MemoryCommitHandler
func NewMemoryCommitHandler() *MemoryCommitHandler {
	return &MemoryCommitHandler{}
}

// ResolveLatestVersion finds the latest manifest version
func (h *MemoryCommitHandler) ResolveLatestVersion(ctx context.Context, basePath string) (uint64, error) {
	// For memory store, we need to list files and find max version
	// This is a simplified implementation
	return 0, nil
}

// ResolveVersion returns the path to the manifest file
func (h *MemoryCommitHandler) ResolveVersion(ctx context.Context, basePath string, version uint64) (string, error) {
	return ManifestPath(version), nil
}

// Commit writes a new manifest
func (h *MemoryCommitHandler) Commit(ctx context.Context, basePath string, version uint64, manifest *Manifest) error {
	data, err := MarshalManifest(manifest)
	if err != nil {
		return err
	}
	return h.store.Write(ManifestPath(version), data)
}

// Helper functions

func findFirstSlash(s string) int {
	for i, c := range s {
		if c == '/' {
			return i
		}
	}
	return -1
}
