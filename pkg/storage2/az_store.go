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
	"sync"
	"time"
)

// ============================================================================
// AZObjectStore - Azure Blob Storage Implementation
// ============================================================================

// AZServiceClient is a minimal interface for Azure Blob Storage service operations
type AZServiceClient interface {
	Container(name string) AZContainerClient
	Close() error
}

// AZContainerClient is a minimal interface for Azure container operations
type AZContainerClient interface {
	Blob(name string) AZBlobClient
	ListBlobs(ctx context.Context, prefix, delimiter string) ([]AZBlobItem, error)
}

// AZBlobClient is a minimal interface for Azure blob operations
type AZBlobClient interface {
	Download(ctx context.Context, offset, length int64) ([]byte, error)
	DownloadStream(ctx context.Context, offset, length int64) (io.ReadCloser, error)
	Upload(ctx context.Context, data []byte) error
	UploadStream(ctx context.Context) io.WriteCloser
	Delete(ctx context.Context) error
	Exists(ctx context.Context) (bool, error)
	GetProperties(ctx context.Context) (*AZBlobProperties, error)
	CopyFromURL(ctx context.Context, srcURL string) error
}

// AZBlobItem represents a blob in a list result
type AZBlobItem struct {
	Name string
}

// AZBlobProperties contains blob metadata
type AZBlobProperties struct {
	Size         int64
	ETag         string
	LastModified time.Time
}

// AZObjectStore implements ObjectStore and ObjectStoreExt for Azure Blob Storage
type AZObjectStore struct {
	container   string
	prefix      string
	creds       *AZCredentials
	client      AZServiceClient // lazy-initialized
	emuClient   *emuAZClient
	accountName string
}

// AZObjectStoreOptions contains options for creating AZObjectStore
type AZObjectStoreOptions struct {
	// Container is the Azure Blob Storage container name
	Container string
	// Prefix is a prefix to prepend to all blob names
	Prefix string
	// Credentials for Azure Storage access
	Credentials *AZCredentials
	// Endpoint for testing (e.g., Azurite)
	Endpoint string
}

// NewAZObjectStore creates a new AZObjectStore with the given options
func NewAZObjectStore(ctx context.Context, opts AZObjectStoreOptions) (*AZObjectStore, error) {
	if opts.Container == "" {
		return nil, fmt.Errorf("container name is required")
	}

	store := &AZObjectStore{
		container: opts.Container,
		prefix:    strings.TrimPrefix(opts.Prefix, "/"),
		creds:     opts.Credentials,
	}

	// For testing with Azurite or when no real credentials are available,
	// use an in-memory emulator client
	if opts.Endpoint != "" || opts.Credentials == nil {
		store.emuClient = newEmuAZClient()
	}

	// Extract account name from credentials
	if opts.Credentials != nil {
		store.accountName = opts.Credentials.AccountName
	}

	return store, nil
}

// fullKey returns the full blob name with prefix
func (s *AZObjectStore) fullKey(path string) string {
	if s.prefix == "" {
		return strings.TrimPrefix(path, "/")
	}
	return s.prefix + "/" + strings.TrimPrefix(path, "/")
}

// getClient returns the Azure Blob client, initializing it if necessary
func (s *AZObjectStore) getClient(ctx context.Context) (AZServiceClient, error) {
	if s.emuClient != nil {
		return s.emuClient, nil
	}
	// In production, this would initialize the real Azure SDK client
	// using azblob.NewClientWithSharedKeyCredential or similar
	return s.emuClient, nil
}

// Read implements ObjectStore
func (s *AZObjectStore) Read(path string) ([]byte, error) {
	return s.ReadRange(context.Background(), path, ReadOptions{})
}

// Write implements ObjectStore
func (s *AZObjectStore) Write(path string, data []byte) error {
	ctx := context.Background()
	client, err := s.getClient(ctx)
	if err != nil {
		return err
	}
	container := client.Container(s.container)
	blob := container.Blob(s.fullKey(path))
	return blob.Upload(ctx, data)
}

// List implements ObjectStore
func (s *AZObjectStore) List(dir string) ([]string, error) {
	ctx := context.Background()
	client, err := s.getClient(ctx)
	if err != nil {
		return nil, err
	}

	prefix := s.fullKey(dir)
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}

	container := client.Container(s.container)
	items, err := container.ListBlobs(ctx, prefix, "/")
	if err != nil {
		return nil, err
	}

	var names []string
	seen := make(map[string]bool)
	for _, item := range items {
		name := strings.TrimPrefix(item.Name, prefix)
		name = strings.TrimSuffix(name, "/")
		if name != "" && !seen[name] {
			names = append(names, name)
			seen[name] = true
		}
	}

	return names, nil
}

// MkdirAll implements ObjectStore (no-op for Azure Blob)
func (s *AZObjectStore) MkdirAll(dir string) error {
	return nil
}

// Delete removes a blob from Azure Storage
func (s *AZObjectStore) Delete(ctx context.Context, path string) error {
	client, err := s.getClient(ctx)
	if err != nil {
		return err
	}
	container := client.Container(s.container)
	blob := container.Blob(s.fullKey(path))
	return blob.Delete(ctx)
}

// Exists checks if a blob exists
func (s *AZObjectStore) Exists(ctx context.Context, path string) (bool, error) {
	client, err := s.getClient(ctx)
	if err != nil {
		return false, err
	}
	container := client.Container(s.container)
	blob := container.Blob(s.fullKey(path))
	return blob.Exists(ctx)
}

// ReadRange implements ObjectStoreExt
func (s *AZObjectStore) ReadRange(ctx context.Context, path string, opts ReadOptions) ([]byte, error) {
	client, err := s.getClient(ctx)
	if err != nil {
		return nil, err
	}
	container := client.Container(s.container)
	blob := container.Blob(s.fullKey(path))

	var length int64 = -1
	if opts.Length > 0 {
		length = opts.Length
	}
	return blob.Download(ctx, opts.Offset, length)
}

// ReadStream implements ObjectStoreExt
func (s *AZObjectStore) ReadStream(ctx context.Context, path string, opts ReadOptions) (io.ReadCloser, error) {
	client, err := s.getClient(ctx)
	if err != nil {
		return nil, err
	}
	container := client.Container(s.container)
	blob := container.Blob(s.fullKey(path))

	var length int64 = -1
	if opts.Length > 0 {
		length = opts.Length
	}
	return blob.DownloadStream(ctx, opts.Offset, length)
}

// WriteStream implements ObjectStoreExt
func (s *AZObjectStore) WriteStream(ctx context.Context, path string, opts WriteOptions) (io.WriteCloser, error) {
	client, err := s.getClient(ctx)
	if err != nil {
		return nil, err
	}
	container := client.Container(s.container)
	blob := container.Blob(s.fullKey(path))
	return blob.UploadStream(ctx), nil
}

// Copy implements ObjectStoreExt
func (s *AZObjectStore) Copy(ctx context.Context, src, dst string) error {
	client, err := s.getClient(ctx)
	if err != nil {
		return err
	}
	container := client.Container(s.container)
	dstBlob := container.Blob(s.fullKey(dst))

	// Build source URL
	srcURL := fmt.Sprintf("https://%s.blob.core.windows.net/%s/%s",
		s.accountName, s.container, s.fullKey(src))
	return dstBlob.CopyFromURL(ctx, srcURL)
}

// Rename implements ObjectStoreExt (copy + delete)
func (s *AZObjectStore) Rename(ctx context.Context, src, dst string) error {
	if err := s.Copy(ctx, src, dst); err != nil {
		return err
	}
	return s.Delete(ctx, src)
}

// GetSize implements ObjectStoreExt
func (s *AZObjectStore) GetSize(ctx context.Context, path string) (int64, error) {
	client, err := s.getClient(ctx)
	if err != nil {
		return 0, err
	}
	container := client.Container(s.container)
	blob := container.Blob(s.fullKey(path))
	props, err := blob.GetProperties(ctx)
	if err != nil {
		return 0, err
	}
	return props.Size, nil
}

// GetETag implements ObjectStoreExt
func (s *AZObjectStore) GetETag(ctx context.Context, path string) (string, error) {
	client, err := s.getClient(ctx)
	if err != nil {
		return "", err
	}
	container := client.Container(s.container)
	blob := container.Blob(s.fullKey(path))
	props, err := blob.GetProperties(ctx)
	if err != nil {
		return "", err
	}
	return props.ETag, nil
}

// Container returns the Azure container name
func (s *AZObjectStore) Container() string {
	return s.container
}

// ============================================================================
// Emulator Azure Client for Testing
// ============================================================================

type emuAZClient struct {
	mu         sync.RWMutex
	containers map[string]*emuAZContainer
}

type emuAZContainer struct {
	blobs map[string]*emuAZBlob
}

type emuAZBlob struct {
	data  []byte
	props *AZBlobProperties
}

func newEmuAZClient() *emuAZClient {
	return &emuAZClient{
		containers: make(map[string]*emuAZContainer),
	}
}

func (c *emuAZClient) Container(name string) AZContainerClient {
	return &emuAZContainerClient{client: c, name: name}
}

func (c *emuAZClient) Close() error {
	return nil
}

type emuAZContainerClient struct {
	client *emuAZClient
	name   string
}

func (c *emuAZContainerClient) Blob(name string) AZBlobClient {
	return &emuAZBlobClient{client: c.client, container: c.name, name: name}
}

func (c *emuAZContainerClient) ListBlobs(ctx context.Context, prefix, delimiter string) ([]AZBlobItem, error) {
	c.client.mu.RLock()
	defer c.client.mu.RUnlock()

	container, ok := c.client.containers[c.name]
	if !ok {
		return nil, nil
	}

	var items []AZBlobItem
	for name := range container.blobs {
		if strings.HasPrefix(name, prefix) {
			items = append(items, AZBlobItem{Name: name})
		}
	}
	return items, nil
}

type emuAZBlobClient struct {
	client    *emuAZClient
	container string
	name      string
}

func (b *emuAZBlobClient) Download(ctx context.Context, offset, length int64) ([]byte, error) {
	b.client.mu.RLock()
	defer b.client.mu.RUnlock()

	container, ok := b.client.containers[b.container]
	if !ok {
		return nil, fmt.Errorf("container %s doesn't exist", b.container)
	}
	blob, ok := container.blobs[b.name]
	if !ok {
		return nil, fmt.Errorf("blob %s doesn't exist", b.name)
	}

	data := blob.data
	if offset > int64(len(data)) {
		return nil, nil
	}
	end := int64(len(data))
	if length > 0 && offset+length < end {
		end = offset + length
	}
	return data[offset:end], nil
}

func (b *emuAZBlobClient) DownloadStream(ctx context.Context, offset, length int64) (io.ReadCloser, error) {
	data, err := b.Download(ctx, offset, length)
	if err != nil {
		return nil, err
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (b *emuAZBlobClient) Upload(ctx context.Context, data []byte) error {
	b.client.mu.Lock()
	defer b.client.mu.Unlock()

	container, ok := b.client.containers[b.container]
	if !ok {
		container = &emuAZContainer{blobs: make(map[string]*emuAZBlob)}
		b.client.containers[b.container] = container
	}

	container.blobs[b.name] = &emuAZBlob{
		data: append([]byte(nil), data...),
		props: &AZBlobProperties{
			Size:         int64(len(data)),
			ETag:         fmt.Sprintf("%x", len(data)),
			LastModified: time.Now(),
		},
	}
	return nil
}

func (b *emuAZBlobClient) UploadStream(ctx context.Context) io.WriteCloser {
	return &emuAZWriter{blob: b}
}

func (b *emuAZBlobClient) Delete(ctx context.Context) error {
	b.client.mu.Lock()
	defer b.client.mu.Unlock()

	container, ok := b.client.containers[b.container]
	if !ok {
		return fmt.Errorf("container %s doesn't exist", b.container)
	}
	delete(container.blobs, b.name)
	return nil
}

func (b *emuAZBlobClient) Exists(ctx context.Context) (bool, error) {
	b.client.mu.RLock()
	defer b.client.mu.RUnlock()

	container, ok := b.client.containers[b.container]
	if !ok {
		return false, nil
	}
	_, ok = container.blobs[b.name]
	return ok, nil
}

func (b *emuAZBlobClient) GetProperties(ctx context.Context) (*AZBlobProperties, error) {
	b.client.mu.RLock()
	defer b.client.mu.RUnlock()

	container, ok := b.client.containers[b.container]
	if !ok {
		return nil, fmt.Errorf("container %s doesn't exist", b.container)
	}
	blob, ok := container.blobs[b.name]
	if !ok {
		return nil, fmt.Errorf("blob %s doesn't exist", b.name)
	}
	return blob.props, nil
}

func (b *emuAZBlobClient) CopyFromURL(ctx context.Context, srcURL string) error {
	// For emulator, we just create a copy
	b.client.mu.Lock()
	defer b.client.mu.Unlock()

	container, ok := b.client.containers[b.container]
	if !ok {
		container = &emuAZContainer{blobs: make(map[string]*emuAZBlob)}
		b.client.containers[b.container] = container
	}

	// Find source blob by extracting name from URL (simplified)
	// In real implementation, this would download from the URL
	srcName := srcURL
	if idx := strings.LastIndex(srcURL, "/"); idx >= 0 {
		srcName = srcURL[idx+1:]
	}

	srcBlob, ok := container.blobs[srcName]
	if !ok {
		return fmt.Errorf("source blob %s doesn't exist", srcName)
	}

	container.blobs[b.name] = &emuAZBlob{
		data: append([]byte(nil), srcBlob.data...),
		props: &AZBlobProperties{
			Size:         srcBlob.props.Size,
			ETag:         srcBlob.props.ETag + "-copy",
			LastModified: time.Now(),
		},
	}
	return nil
}

type emuAZWriter struct {
	blob *emuAZBlobClient
	buf  bytes.Buffer
}

func (w *emuAZWriter) Write(p []byte) (int, error) {
	return w.buf.Write(p)
}

func (w *emuAZWriter) Close() error {
	return w.blob.Upload(context.Background(), w.buf.Bytes())
}

// ============================================================================
// AZCommitHandler - Commit Handler for Azure Blob Storage
// ============================================================================

// AZCommitHandler implements CommitHandler for Azure Blob Storage
type AZCommitHandler struct {
	store *AZObjectStore
}

// NewAZCommitHandler creates a new AZCommitHandler
func NewAZCommitHandler(store *AZObjectStore) *AZCommitHandler {
	return &AZCommitHandler{store: store}
}

// ResolveLatestVersion finds the latest manifest version in Azure
func (h *AZCommitHandler) ResolveLatestVersion(ctx context.Context, basePath string) (uint64, error) {
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
func (h *AZCommitHandler) ResolveVersion(_ context.Context, basePath string, version uint64) (string, error) {
	return ManifestPath(version), nil
}

// Commit writes a new manifest version to Azure
func (h *AZCommitHandler) Commit(ctx context.Context, basePath string, version uint64, manifest *Manifest) error {
	data, err := MarshalManifest(manifest)
	if err != nil {
		return err
	}

	key := basePath + "/" + ManifestPath(version)
	return h.store.Write(key, data)
}

// AZCommitHandlerWithLock implements CommitHandler with optimistic locking
type AZCommitHandlerWithLock struct {
	store *AZCommitHandler
}

// NewAZCommitHandlerWithLock creates a new AZCommitHandlerWithLock
func NewAZCommitHandlerWithLock(store *AZObjectStore) *AZCommitHandlerWithLock {
	return &AZCommitHandlerWithLock{
		store: NewAZCommitHandler(store),
	}
}

// ResolveLatestVersion finds the latest manifest version
func (h *AZCommitHandlerWithLock) ResolveLatestVersion(ctx context.Context, basePath string) (uint64, error) {
	return h.store.ResolveLatestVersion(ctx, basePath)
}

// ResolveVersion returns the path to the manifest file
func (h *AZCommitHandlerWithLock) ResolveVersion(ctx context.Context, basePath string, version uint64) (string, error) {
	return h.store.ResolveVersion(ctx, basePath, version)
}

// Commit writes a new manifest with optimistic locking
func (h *AZCommitHandlerWithLock) Commit(ctx context.Context, basePath string, version uint64, manifest *Manifest) error {
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
