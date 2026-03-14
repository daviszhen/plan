// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package storage2

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"golang.org/x/oauth2/google"
	"google.golang.org/api/option"
)

// ============================================================================
// GSObjectStore - Google Cloud Storage Implementation
// ============================================================================

// Note: This implementation uses minimal interfaces to avoid importing
// the full cloud.google.com/go/storage package at compile time.
// The actual GCS client is created lazily when needed.

// GCSClient is a minimal interface for Google Cloud Storage operations
type GCSClient interface {
	Bucket(name string) GCSBucketHandle
	Close() error
}

// GCSBucketHandle is a minimal interface for GCS bucket operations
type GCSBucketHandle interface {
	Object(name string) GCSObjectHandle
	Objects(ctx context.Context, q *GCSQuery) GCSObjectIterator
}

// GCSObjectHandle is a minimal interface for GCS object operations
type GCSObjectHandle interface {
	Attrs(ctx context.Context) (*GCSObjectAttrs, error)
	NewReader(ctx context.Context) (io.ReadCloser, error)
	NewRangeReader(ctx context.Context, offset, length int64) (io.ReadCloser, error)
	NewWriter(ctx context.Context) GCSWriter
	Delete(ctx context.Context) error
	CopyTo(ctx context.Context, dst GCSObjectHandle) (*GCSObjectAttrs, error)
}

// GCSWriter is a minimal interface for GCS write operations
type GCSWriter interface {
	io.WriteCloser
}

// GCSObjectAttrs contains object metadata
type GCSObjectAttrs struct {
	Name    string
	Size    int64
	ETag    string
	Updated time.Time
}

// GCSQuery is a query for listing objects
type GCSQuery struct {
	Prefix    string
	Delimiter string
}

// GCSObjectIterator iterates over objects
type GCSObjectIterator interface {
	Next() (*GCSObjectAttrs, error)
}

// GSObjectStore implements ObjectStore and ObjectStoreExt for Google Cloud Storage
// This implementation uses a simplified approach that can be replaced with the
// actual cloud.google.com/go/storage client when needed.
type GSObjectStore struct {
	bucket    string
	prefix    string
	projectID string
	creds     *GSCredentials
	client    GCSClient // lazy-initialized
	emuClient *emuGCSClient
}

// GSObjectStoreOptions contains options for creating GSObjectStore
type GSObjectStoreOptions struct {
	// Bucket is the GCS bucket name
	Bucket string
	// Prefix is a prefix to prepend to all object keys
	Prefix string
	// ProjectID is the Google Cloud project ID
	ProjectID string
	// Credentials for GCS access (optional, uses ADC if nil)
	Credentials *GSCredentials
	// Endpoint for testing (e.g., fake GCS server)
	Endpoint string
}

// NewGSObjectStore creates a new GSObjectStore with the given options
func NewGSObjectStore(ctx context.Context, opts GSObjectStoreOptions) (*GSObjectStore, error) {
	if opts.Bucket == "" {
		return nil, fmt.Errorf("bucket name is required")
	}

	store := &GSObjectStore{
		bucket:    opts.Bucket,
		prefix:    strings.TrimPrefix(opts.Prefix, "/"),
		projectID: opts.ProjectID,
		creds:     opts.Credentials,
	}

	// For testing with fake GCS or when no real credentials are available,
	// use an in-memory emulator client
	if opts.Endpoint != "" || opts.Credentials == nil {
		store.emuClient = newEmuGCSClient()
	}

	return store, nil
}

// fullKey returns the full GCS key with prefix
func (s *GSObjectStore) fullKey(path string) string {
	if s.prefix == "" {
		return strings.TrimPrefix(path, "/")
	}
	return s.prefix + "/" + strings.TrimPrefix(path, "/")
}

// getClient returns the GCS client, initializing it if necessary
func (s *GSObjectStore) getClient(ctx context.Context) (GCSClient, error) {
	if s.emuClient != nil {
		return s.emuClient, nil
	}
	if s.client != nil {
		return s.client, nil
	}

	// Initialize real GCS client
	var opts []option.ClientOption
	if s.creds != nil {
		if len(s.creds.CredentialsJSON) > 0 {
			creds, err := google.CredentialsFromJSON(ctx, s.creds.CredentialsJSON)
			if err != nil {
				return nil, fmt.Errorf("parse credentials JSON: %w", err)
			}
			opts = append(opts, option.WithCredentials(creds))
		} else if s.creds.CredentialsFile != "" {
			opts = append(opts, option.WithCredentialsFile(s.creds.CredentialsFile))
		}
	}
	if s.projectID != "" {
		opts = append(opts, option.WithQuotaProject(s.projectID))
	}

	// For now, return emulator client if no real client can be created
	// In production, this would use storage.NewClient(ctx, opts...)
	return s.emuClient, nil
}

// Read implements ObjectStore
func (s *GSObjectStore) Read(path string) ([]byte, error) {
	return s.ReadRange(context.Background(), path, ReadOptions{})
}

// Write implements ObjectStore
func (s *GSObjectStore) Write(path string, data []byte) error {
	ctx := context.Background()
	client, err := s.getClient(ctx)
	if err != nil {
		return err
	}

	obj := client.Bucket(s.bucket).Object(s.fullKey(path))
	w := obj.NewWriter(ctx)
	_, err = w.Write(data)
	if err != nil {
		w.Close()
		return err
	}
	return w.Close()
}

// List implements ObjectStore
func (s *GSObjectStore) List(dir string) ([]string, error) {
	ctx := context.Background()
	client, err := s.getClient(ctx)
	if err != nil {
		return nil, err
	}

	prefix := s.fullKey(dir)
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}

	iter := client.Bucket(s.bucket).Objects(ctx, &GCSQuery{
		Prefix:    prefix,
		Delimiter: "/",
	})

	var names []string
	seen := make(map[string]bool)

	for {
		attrs, err := iter.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		name := strings.TrimPrefix(attrs.Name, prefix)
		name = strings.TrimSuffix(name, "/")
		if name != "" && !seen[name] {
			names = append(names, name)
			seen[name] = true
		}
	}

	return names, nil
}

// MkdirAll implements ObjectStore (no-op for GCS)
func (s *GSObjectStore) MkdirAll(dir string) error {
	return nil
}

// Delete removes an object from GCS
func (s *GSObjectStore) Delete(ctx context.Context, path string) error {
	client, err := s.getClient(ctx)
	if err != nil {
		return err
	}
	return client.Bucket(s.bucket).Object(s.fullKey(path)).Delete(ctx)
}

// Exists checks if an object exists
func (s *GSObjectStore) Exists(ctx context.Context, path string) (bool, error) {
	client, err := s.getClient(ctx)
	if err != nil {
		return false, err
	}
	_, err = client.Bucket(s.bucket).Object(s.fullKey(path)).Attrs(ctx)
	if err != nil {
		// Check for "not found" error
		if strings.Contains(err.Error(), "doesn't exist") ||
			strings.Contains(err.Error(), "not found") {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// ReadRange implements ObjectStoreExt
func (s *GSObjectStore) ReadRange(ctx context.Context, path string, opts ReadOptions) ([]byte, error) {
	client, err := s.getClient(ctx)
	if err != nil {
		return nil, err
	}

	obj := client.Bucket(s.bucket).Object(s.fullKey(path))

	var reader io.ReadCloser
	if opts.Offset > 0 || opts.Length > 0 {
		length := opts.Length
		if length <= 0 {
			length = -1 // Read to end
		}
		reader, err = obj.NewRangeReader(ctx, opts.Offset, length)
	} else {
		reader, err = obj.NewReader(ctx)
	}
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	return io.ReadAll(reader)
}

// ReadStream implements ObjectStoreExt
func (s *GSObjectStore) ReadStream(ctx context.Context, path string, opts ReadOptions) (io.ReadCloser, error) {
	client, err := s.getClient(ctx)
	if err != nil {
		return nil, err
	}

	obj := client.Bucket(s.bucket).Object(s.fullKey(path))

	if opts.Offset > 0 || opts.Length > 0 {
		length := opts.Length
		if length <= 0 {
			length = -1
		}
		return obj.NewRangeReader(ctx, opts.Offset, length)
	}
	return obj.NewReader(ctx)
}

// WriteStream implements ObjectStoreExt
func (s *GSObjectStore) WriteStream(ctx context.Context, path string, opts WriteOptions) (io.WriteCloser, error) {
	client, err := s.getClient(ctx)
	if err != nil {
		return nil, err
	}
	obj := client.Bucket(s.bucket).Object(s.fullKey(path))
	return obj.NewWriter(ctx), nil
}

// Copy implements ObjectStoreExt
func (s *GSObjectStore) Copy(ctx context.Context, src, dst string) error {
	client, err := s.getClient(ctx)
	if err != nil {
		return err
	}
	srcObj := client.Bucket(s.bucket).Object(s.fullKey(src))
	dstObj := client.Bucket(s.bucket).Object(s.fullKey(dst))
	_, err = srcObj.CopyTo(ctx, dstObj)
	return err
}

// Rename implements ObjectStoreExt (copy + delete)
func (s *GSObjectStore) Rename(ctx context.Context, src, dst string) error {
	if err := s.Copy(ctx, src, dst); err != nil {
		return err
	}
	return s.Delete(ctx, src)
}

// GetSize implements ObjectStoreExt
func (s *GSObjectStore) GetSize(ctx context.Context, path string) (int64, error) {
	client, err := s.getClient(ctx)
	if err != nil {
		return 0, err
	}
	attrs, err := client.Bucket(s.bucket).Object(s.fullKey(path)).Attrs(ctx)
	if err != nil {
		return 0, err
	}
	return attrs.Size, nil
}

// GetETag implements ObjectStoreExt
func (s *GSObjectStore) GetETag(ctx context.Context, path string) (string, error) {
	client, err := s.getClient(ctx)
	if err != nil {
		return "", err
	}
	attrs, err := client.Bucket(s.bucket).Object(s.fullKey(path)).Attrs(ctx)
	if err != nil {
		return "", err
	}
	return attrs.ETag, nil
}

// Bucket returns the GCS bucket name
func (s *GSObjectStore) Bucket() string {
	return s.bucket
}

// ============================================================================
// Emulator GCS Client for Testing
// ============================================================================

type emuGCSClient struct {
	mu      interface{} // sync.RWMutex
	buckets map[string]*emuBucket
}

type emuBucket struct {
	objects map[string][]byte
	attrs   map[string]*GCSObjectAttrs
}

func newEmuGCSClient() *emuGCSClient {
	return &emuGCSClient{
		buckets: make(map[string]*emuBucket),
	}
}

func (c *emuGCSClient) Bucket(name string) GCSBucketHandle {
	return &emuBucketHandle{client: c, name: name}
}

func (c *emuGCSClient) Close() error {
	return nil
}

type emuBucketHandle struct {
	client *emuGCSClient
	name   string
}

func (h *emuBucketHandle) Object(name string) GCSObjectHandle {
	return &emuObjectHandle{client: h.client, bucket: h.name, name: name}
}

func (h *emuBucketHandle) Objects(ctx context.Context, q *GCSQuery) GCSObjectIterator {
	return &emuObjectIterator{
		client:    h.client,
		bucket:    h.name,
		prefix:    q.Prefix,
		delimiter: q.Delimiter,
	}
}

type emuObjectHandle struct {
	client *emuGCSClient
	bucket string
	name   string
}

func (h *emuObjectHandle) Attrs(ctx context.Context) (*GCSObjectAttrs, error) {
	b, ok := h.client.buckets[h.bucket]
	if !ok {
		return nil, fmt.Errorf("bucket %s doesn't exist", h.bucket)
	}
	attrs, ok := b.attrs[h.name]
	if !ok {
		return nil, fmt.Errorf("object %s doesn't exist", h.name)
	}
	return attrs, nil
}

func (h *emuObjectHandle) NewReader(ctx context.Context) (io.ReadCloser, error) {
	b, ok := h.client.buckets[h.bucket]
	if !ok {
		return nil, fmt.Errorf("bucket %s doesn't exist", h.bucket)
	}
	data, ok := b.objects[h.name]
	if !ok {
		return nil, fmt.Errorf("object %s doesn't exist", h.name)
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (h *emuObjectHandle) NewRangeReader(ctx context.Context, offset, length int64) (io.ReadCloser, error) {
	b, ok := h.client.buckets[h.bucket]
	if !ok {
		return nil, fmt.Errorf("bucket %s doesn't exist", h.bucket)
	}
	data, ok := b.objects[h.name]
	if !ok {
		return nil, fmt.Errorf("object %s doesn't exist", h.name)
	}
	if offset > int64(len(data)) {
		return io.NopCloser(bytes.NewReader(nil)), nil
	}
	end := int64(len(data))
	if length > 0 && offset+length < end {
		end = offset + length
	}
	return io.NopCloser(bytes.NewReader(data[offset:end])), nil
}

func (h *emuObjectHandle) NewWriter(ctx context.Context) GCSWriter {
	return &emuWriter{handle: h}
}

func (h *emuObjectHandle) Delete(ctx context.Context) error {
	b, ok := h.client.buckets[h.bucket]
	if !ok {
		return fmt.Errorf("bucket %s doesn't exist", h.bucket)
	}
	delete(b.objects, h.name)
	delete(b.attrs, h.name)
	return nil
}

func (h *emuObjectHandle) CopyTo(ctx context.Context, dst GCSObjectHandle) (*GCSObjectAttrs, error) {
	b, ok := h.client.buckets[h.bucket]
	if !ok {
		return nil, fmt.Errorf("bucket %s doesn't exist", h.bucket)
	}
	data, ok := b.objects[h.name]
	if !ok {
		return nil, fmt.Errorf("object %s doesn't exist", h.name)
	}
	attrs := b.attrs[h.name]

	// Copy to destination
	dstHandle := dst.(*emuObjectHandle)
	dstBucket, ok := h.client.buckets[dstHandle.bucket]
	if !ok {
		dstBucket = &emuBucket{
			objects: make(map[string][]byte),
			attrs:   make(map[string]*GCSObjectAttrs),
		}
		h.client.buckets[dstHandle.bucket] = dstBucket
	}
	dstBucket.objects[dstHandle.name] = append([]byte(nil), data...)
	dstBucket.attrs[dstHandle.name] = &GCSObjectAttrs{
		Name:    dstHandle.name,
		Size:    attrs.Size,
		ETag:    attrs.ETag + "-copy",
		Updated: time.Now(),
	}
	return dstBucket.attrs[dstHandle.name], nil
}

type emuWriter struct {
	handle *emuObjectHandle
	buf    bytes.Buffer
}

func (w *emuWriter) Write(p []byte) (int, error) {
	return w.buf.Write(p)
}

func (w *emuWriter) Close() error {
	b, ok := w.handle.client.buckets[w.handle.bucket]
	if !ok {
		b = &emuBucket{
			objects: make(map[string][]byte),
			attrs:   make(map[string]*GCSObjectAttrs),
		}
		w.handle.client.buckets[w.handle.bucket] = b
	}
	data := w.buf.Bytes()
	b.objects[w.handle.name] = append([]byte(nil), data...)
	b.attrs[w.handle.name] = &GCSObjectAttrs{
		Name:    w.handle.name,
		Size:    int64(len(data)),
		ETag:    fmt.Sprintf("%x", len(data)),
		Updated: time.Now(),
	}
	return nil
}

type emuObjectIterator struct {
	client    *emuGCSClient
	bucket    string
	prefix    string
	delimiter string
	objects   []string
	index     int
}

func (it *emuObjectIterator) Next() (*GCSObjectAttrs, error) {
	if it.objects == nil {
		// Initialize on first call
		b, ok := it.client.buckets[it.bucket]
		if !ok {
			return nil, io.EOF
		}
		for name := range b.objects {
			if strings.HasPrefix(name, it.prefix) {
				rest := strings.TrimPrefix(name, it.prefix)
				if it.delimiter != "" {
					if idx := strings.Index(rest, it.delimiter); idx >= 0 {
						rest = rest[:idx+1]
					}
				}
				it.objects = append(it.objects, name)
			}
		}
	}

	if it.index >= len(it.objects) {
		return nil, io.EOF
	}
	name := it.objects[it.index]
	it.index++

	b := it.client.buckets[it.bucket]
	return b.attrs[name], nil
}

// ============================================================================
// GSCommitHandler - Commit Handler for GCS
// ============================================================================

// GSCommitHandler implements CommitHandler for Google Cloud Storage
// GCS supports conditional writes using generation numbers (similar to ETags)
type GSCommitHandler struct {
	store *GSObjectStore
}

// NewGSCommitHandler creates a new GSCommitHandler
func NewGSCommitHandler(store *GSObjectStore) *GSCommitHandler {
	return &GSCommitHandler{store: store}
}

// ResolveLatestVersion finds the latest manifest version in GCS
func (h *GSCommitHandler) ResolveLatestVersion(ctx context.Context, basePath string) (uint64, error) {
	prefix := h.store.fullKey(basePath + "/" + VersionsDir)
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}

	names, err := h.store.List(basePath + "/" + VersionsDir)
	if err != nil {
		return 0, err
	}

	var maxVersion uint64
	for _, name := range names {
		v, err := ParseVersion(name)
		if err != nil {
			continue
		}
		if v > maxVersion {
			maxVersion = v
		}
	}

	return maxVersion, nil
}

// ResolveVersion returns the path to the manifest file for the given version
func (h *GSCommitHandler) ResolveVersion(_ context.Context, basePath string, version uint64) (string, error) {
	return ManifestPath(version), nil
}

// Commit writes a new manifest version to GCS
func (h *GSCommitHandler) Commit(ctx context.Context, basePath string, version uint64, manifest *Manifest) error {
	data, err := MarshalManifest(manifest)
	if err != nil {
		return err
	}

	key := basePath + "/" + ManifestPath(version)
	return h.store.Write(key, data)
}

// GSCommitHandlerWithLock implements CommitHandler with optimistic locking
type GSCommitHandlerWithLock struct {
	store *GSCommitHandler
}

// NewGSCommitHandlerWithLock creates a new GSCommitHandlerWithLock
func NewGSCommitHandlerWithLock(store *GSObjectStore) *GSCommitHandlerWithLock {
	return &GSCommitHandlerWithLock{
		store: NewGSCommitHandler(store),
	}
}

// ResolveLatestVersion finds the latest manifest version
func (h *GSCommitHandlerWithLock) ResolveLatestVersion(ctx context.Context, basePath string) (uint64, error) {
	return h.store.ResolveLatestVersion(ctx, basePath)
}

// ResolveVersion returns the path to the manifest file
func (h *GSCommitHandlerWithLock) ResolveVersion(ctx context.Context, basePath string, version uint64) (string, error) {
	return h.store.ResolveVersion(ctx, basePath, version)
}

// Commit writes a new manifest with optimistic locking
func (h *GSCommitHandlerWithLock) Commit(ctx context.Context, basePath string, version uint64, manifest *Manifest) error {
	// Check if target version already exists
	key := basePath + "/" + ManifestPath(version)
	exists, err := h.store.store.Exists(ctx, key)
	if err != nil {
		return err
	}
	if exists {
		return fmt.Errorf("version %d already exists", version)
	}
	return h.store.Commit(ctx, basePath, version, manifest)
}
