package storage2

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// MockS3Client is a mock S3 client for testing
type MockS3Client struct {
	data   map[string][]byte
	etags  map[string]string
	bucket string
}

func NewMockS3Client(bucket string) *MockS3Client {
	return &MockS3Client{
		data:   make(map[string][]byte),
		etags:  make(map[string]string),
		bucket: bucket,
	}
}

func (m *MockS3Client) fullKey(path string) string {
	return m.bucket + "/" + path
}

func (m *MockS3Client) Read(path string) ([]byte, error) {
	data, ok := m.data[m.fullKey(path)]
	if !ok {
		return nil, fmt.Errorf("object not found: %s", path)
	}
	return data, nil
}

func (m *MockS3Client) Write(path string, data []byte) error {
	m.data[m.fullKey(path)] = data
	m.etags[m.fullKey(path)] = fmt.Sprintf("%d", time.Now().UnixNano())
	return nil
}

func (m *MockS3Client) List(prefix string) []string {
	var names []string
	for key := range m.data {
		if len(key) > len(prefix) && key[:len(prefix)] == prefix {
			rest := key[len(prefix):]
			if rest[0] == '/' {
				rest = rest[1:]
			}
			if !bytes.Contains([]byte(rest), []byte("/")) {
				names = append(names, rest)
			}
		}
	}
	return names
}

func (m *MockS3Client) Delete(path string) error {
	delete(m.data, m.fullKey(path))
	return nil
}

func (m *MockS3Client) Exists(path string) bool {
	_, ok := m.data[m.fullKey(path)]
	return ok
}

func (m *MockS3Client) Size(path string) int64 {
	data, ok := m.data[m.fullKey(path)]
	if !ok {
		return 0
	}
	return int64(len(data))
}

// TestMemoryObjectStore tests the in-memory object store
func TestMemoryObjectStore_Basic(t *testing.T) {
	store := NewMemoryObjectStore("test-prefix")

	ctx := context.Background()

	// Test Write
	err := store.Write("file1.txt", []byte("hello world"))
	require.NoError(t, err)

	// Test Read
	data, err := store.Read("file1.txt")
	require.NoError(t, err)
	require.Equal(t, "hello world", string(data))

	// Test Exists
	exists, err := store.Exists(ctx, "file1.txt")
	require.NoError(t, err)
	require.True(t, exists)

	exists, err = store.Exists(ctx, "nonexistent.txt")
	require.NoError(t, err)
	require.False(t, exists)

	// Test GetSize
	size, err := store.GetSize(ctx, "file1.txt")
	require.NoError(t, err)
	require.Equal(t, int64(11), size)

	// Test List
	err = store.Write("file2.txt", []byte("another file"))
	require.NoError(t, err)

	names, err := store.List("")
	require.NoError(t, err)
	require.Len(t, names, 2)
	require.Contains(t, names, "file1.txt")
	require.Contains(t, names, "file2.txt")

	// Test Delete
	err = store.Delete(ctx, "file1.txt")
	require.NoError(t, err)

	exists, err = store.Exists(ctx, "file1.txt")
	require.NoError(t, err)
	require.False(t, exists)
}

func TestMemoryObjectStore_ReadRange(t *testing.T) {
	store := NewMemoryObjectStore("")

	ctx := context.Background()
	testData := []byte("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ")

	err := store.Write("test/range.txt", testData)
	require.NoError(t, err)

	// Test full read
	data, err := store.ReadRange(ctx, "test/range.txt", ReadOptions{})
	require.NoError(t, err)
	require.Equal(t, testData, data)

	// Test range read with offset
	data, err = store.ReadRange(ctx, "test/range.txt", ReadOptions{Offset: 10})
	require.NoError(t, err)
	require.Equal(t, testData[10:], data)

	// Test range read with offset and length
	data, err = store.ReadRange(ctx, "test/range.txt", ReadOptions{Offset: 10, Length: 5})
	require.NoError(t, err)
	require.Equal(t, testData[10:15], data)
}

func TestMemoryObjectStore_Stream(t *testing.T) {
	store := NewMemoryObjectStore("")

	ctx := context.Background()

	// Test WriteStream
	writer, err := store.WriteStream(ctx, "test/stream.txt", WriteOptions{Create: true})
	require.NoError(t, err)

	_, err = writer.Write([]byte("line1\n"))
	require.NoError(t, err)
	_, err = writer.Write([]byte("line2\n"))
	require.NoError(t, err)
	err = writer.Close()
	require.NoError(t, err)

	// Test ReadStream
	reader, err := store.ReadStream(ctx, "test/stream.txt", ReadOptions{})
	require.NoError(t, err)
	defer reader.Close()

	data, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.Equal(t, "line1\nline2\n", string(data))
}

func TestMemoryObjectStore_CopyRename(t *testing.T) {
	store := NewMemoryObjectStore("")

	ctx := context.Background()

	// Write source file
	err := store.Write("test/src.txt", []byte("source content"))
	require.NoError(t, err)

	// Test Copy
	err = store.Copy(ctx, "test/src.txt", "test/dst.txt")
	require.NoError(t, err)

	data, err := store.Read("test/dst.txt")
	require.NoError(t, err)
	require.Equal(t, "source content", string(data))

	// Test Rename
	err = store.Rename(ctx, "test/src.txt", "test/renamed.txt")
	require.NoError(t, err)

	data, err = store.Read("test/renamed.txt")
	require.NoError(t, err)
	require.Equal(t, "source content", string(data))

	// Original should not exist
	exists, err := store.Exists(ctx, "test/src.txt")
	require.NoError(t, err)
	require.False(t, exists)
}

func TestStoreFactory_Local(t *testing.T) {
	ctx := context.Background()
	factory := NewStoreFactory(StoreFactoryOptions{
		EnableCache: true,
	})

	// Test local file store
	store, err := factory.GetStore(ctx, "/tmp/test-data")
	require.NoError(t, err)
	require.NotNil(t, store)

	// Test that cache works
	store2, err := factory.GetStore(ctx, "/tmp/test-data")
	require.NoError(t, err)
	require.Equal(t, store, store2)

	// Test commit handler for local
	handler, err := factory.GetCommitHandler(ctx, "/tmp/test-data")
	require.NoError(t, err)
	require.NotNil(t, handler)
}

func TestStoreFactory_Memory(t *testing.T) {
	ctx := context.Background()
	factory := NewStoreFactory(StoreFactoryOptions{})

	// Test memory store
	store, err := factory.GetStore(ctx, "mem://test-dataset")
	require.NoError(t, err)
	require.NotNil(t, store)

	// Test basic operations
	err = store.Write("test.txt", []byte("hello"))
	require.NoError(t, err)

	data, err := store.Read("test.txt")
	require.NoError(t, err)
	require.Equal(t, "hello", string(data))

	// Test commit handler for memory
	handler, err := factory.GetCommitHandler(ctx, "mem://test-dataset")
	require.NoError(t, err)
	require.NotNil(t, handler)
}

func TestStoreFactory_Unsupported(t *testing.T) {
	ctx := context.Background()
	factory := NewStoreFactory(StoreFactoryOptions{})

	// Test unsupported scheme
	_, err := factory.GetStore(ctx, "ftp://server/path")
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported")

	// Test GCS (now implemented)
	_, err = factory.GetStore(ctx, "gs://bucket/path")
	require.NoError(t, err)

	// Test Azure (now implemented)
	_, err = factory.GetStore(ctx, "az://container/path")
	require.NoError(t, err)
}

func TestMemoryCommitHandler(t *testing.T) {
	handler := NewMemoryCommitHandler()
	ctx := context.Background()

	// Test ResolveLatestVersion on empty dataset
	version, err := handler.ResolveLatestVersion(ctx, "")
	require.NoError(t, err)
	require.Equal(t, uint64(0), version)

	// Test ResolveVersion
	path, err := handler.ResolveVersion(ctx, "", 1)
	require.NoError(t, err)
	require.Contains(t, path, "1.manifest")
}

func TestS3Credentials(t *testing.T) {
	// Test S3Credentials struct
	creds := &S3Credentials{
		AccessKeyID:     "test-key",
		SecretAccessKey: "test-secret",
		SessionToken:    "test-token",
		Region:          "us-west-2",
	}
	require.Equal(t, "test-key", creds.AccessKeyID)
	require.Equal(t, "test-secret", creds.SecretAccessKey)
	require.Equal(t, "test-token", creds.SessionToken)
	require.Equal(t, "us-west-2", creds.Region)
}

func TestGSCredentials(t *testing.T) {
	creds := &GSCredentials{
		ProjectID:       "test-project",
		CredentialsFile: "/path/to/creds.json",
	}
	require.Equal(t, "test-project", creds.ProjectID)
	require.Equal(t, "/path/to/creds.json", creds.CredentialsFile)
}

func TestAZCredentials(t *testing.T) {
	creds := &AZCredentials{
		AccountName:      "testaccount",
		AccountKey:       "test-key",
		ConnectionString: "DefaultEndpointsProtocol=https;AccountName=test",
	}
	require.Equal(t, "testaccount", creds.AccountName)
	require.Equal(t, "test-key", creds.AccountKey)
	require.Contains(t, creds.ConnectionString, "test")
}

func TestStorageConfig(t *testing.T) {
	cfg, err := NewStorageConfig("s3://my-bucket/path/to/dataset?region=us-west-2")
	require.NoError(t, err)
	require.Equal(t, "s3", cfg.URI.Scheme)
	require.Equal(t, "my-bucket", cfg.URI.Bucket)
	require.Equal(t, "path/to/dataset", cfg.URI.Path)
	require.Equal(t, "us-west-2", cfg.URI.Query.Get("region"))
}

func TestMemoryObjectStore_ETag(t *testing.T) {
	store := NewMemoryObjectStore("")
	ctx := context.Background()

	err := store.Write("test.txt", []byte("content"))
	require.NoError(t, err)

	etag, err := store.GetETag(ctx, "test.txt")
	require.NoError(t, err)
	require.NotEmpty(t, etag)
}

func TestMemoryObjectStore_ListWithPrefix(t *testing.T) {
	store := NewMemoryObjectStore("")

	// Create nested structure
	_ = store.Write("dir1/file1.txt", []byte("1"))
	_ = store.Write("dir1/file2.txt", []byte("2"))
	_ = store.Write("dir2/file3.txt", []byte("3"))
	_ = store.Write("file4.txt", []byte("4"))

	// List root
	names, err := store.List("")
	require.NoError(t, err)
	require.Contains(t, names, "dir1")
	require.Contains(t, names, "dir2")
	require.Contains(t, names, "file4.txt")

	// List subdirectory
	names, err = store.List("dir1")
	require.NoError(t, err)
	require.Contains(t, names, "file1.txt")
	require.Contains(t, names, "file2.txt")
	require.Len(t, names, 2)
}

func TestMemoryObjectStore_ReadStreamWithRange(t *testing.T) {
	store := NewMemoryObjectStore("")
	ctx := context.Background()

	testData := []byte("0123456789ABCDEFGHIJ")
	_ = store.Write("test.txt", testData)

	// Read with offset
	reader, err := store.ReadStream(ctx, "test.txt", ReadOptions{Offset: 5})
	require.NoError(t, err)
	data, err := io.ReadAll(reader)
	reader.Close()
	require.NoError(t, err)
	require.Equal(t, "56789ABCDEFGHIJ", string(data))

	// Read with offset and length
	reader, err = store.ReadStream(ctx, "test.txt", ReadOptions{Offset: 5, Length: 5})
	require.NoError(t, err)
	data, err = io.ReadAll(reader)
	reader.Close()
	require.NoError(t, err)
	require.Equal(t, "56789", string(data))
}
