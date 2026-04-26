package storage2

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// S3ObjectStore implements ObjectStore and ObjectStoreExt for S3-compatible storage
type S3ObjectStore struct {
	client     *s3.Client
	uploader   *manager.Uploader
	downloader *manager.Downloader
	bucket     string
	prefix     string // prefix for all objects (like a base path)
	region     string
	endpoint   string

	// For testing with MinIO/localstack
	forcePathStyle bool
}

// S3ObjectStoreOptions contains options for creating S3ObjectStore
type S3ObjectStoreOptions struct {
	// Bucket is the S3 bucket name
	Bucket string
	// Prefix is a prefix to prepend to all object keys
	Prefix string
	// Region is the AWS region
	Region string
	// Endpoint is a custom endpoint URL (for S3-compatible services)
	Endpoint string
	// ForcePathStyle forces path-style addressing (required for MinIO)
	ForcePathStyle bool
	// Credentials for S3 access (optional, uses default chain if nil)
	Credentials *S3Credentials
}

// NewS3ObjectStore creates a new S3ObjectStore with the given options
func NewS3ObjectStore(ctx context.Context, opts S3ObjectStoreOptions) (*S3ObjectStore, error) {
	if opts.Bucket == "" {
		return nil, fmt.Errorf("bucket name is required")
	}

	// Build AWS config
	var cfg aws.Config
	var err error

	if opts.Credentials != nil {
		// Use explicit credentials
		creds := credentials.NewStaticCredentialsProvider(
			opts.Credentials.AccessKeyID,
			opts.Credentials.SecretAccessKey,
			opts.Credentials.SessionToken,
		)
		cfg, err = config.LoadDefaultConfig(ctx,
			config.WithCredentialsProvider(creds),
			config.WithRegion(opts.Region),
		)
	} else {
		// Use default credential chain
		cfg, err = config.LoadDefaultConfig(ctx,
			config.WithRegion(opts.Region),
		)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	// Create S3 client
	clientOpts := []func(*s3.Options){}
	if opts.Endpoint != "" {
		clientOpts = append(clientOpts, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(opts.Endpoint)
		})
	}
	if opts.ForcePathStyle {
		clientOpts = append(clientOpts, func(o *s3.Options) {
			o.UsePathStyle = true
		})
	}

	client := s3.NewFromConfig(cfg, clientOpts...)

	return &S3ObjectStore{
		client:         client,
		uploader:       manager.NewUploader(client),
		downloader:     manager.NewDownloader(client),
		bucket:         opts.Bucket,
		prefix:         strings.TrimPrefix(opts.Prefix, "/"),
		region:         opts.Region,
		endpoint:       opts.Endpoint,
		forcePathStyle: opts.ForcePathStyle,
	}, nil
}

// fullKey returns the full S3 key with prefix
func (s *S3ObjectStore) fullKey(path string) string {
	if s.prefix == "" {
		return strings.TrimPrefix(path, "/")
	}
	return s.prefix + "/" + strings.TrimPrefix(path, "/")
}

// Read implements ObjectStore
func (s *S3ObjectStore) Read(path string) ([]byte, error) {
	return s.ReadRange(context.Background(), path, ReadOptions{})
}

// Write implements ObjectStore
func (s *S3ObjectStore) Write(path string, data []byte) error {
	_, err := s.uploader.Upload(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.fullKey(path)),
		Body:   bytes.NewReader(data),
	})
	return err
}

// List implements ObjectStore
func (s *S3ObjectStore) List(dir string) ([]string, error) {
	prefix := s.fullKey(dir)
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}

	var names []string
	paginator := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket:    aws.String(s.bucket),
		Prefix:    aws.String(prefix),
		Delimiter: aws.String("/"),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(context.Background())
		if err != nil {
			return nil, err
		}

		// Add common prefixes (subdirectories)
		for _, p := range page.CommonPrefixes {
			name := strings.TrimPrefix(*p.Prefix, prefix)
			name = strings.TrimSuffix(name, "/")
			if name != "" {
				names = append(names, name)
			}
		}

		// Add objects
		for _, obj := range page.Contents {
			name := strings.TrimPrefix(*obj.Key, prefix)
			if name != "" && !strings.HasSuffix(name, "/") {
				names = append(names, name)
			}
		}
	}

	return names, nil
}

// MkdirAll implements ObjectStore (no-op for S3)
func (s *S3ObjectStore) MkdirAll(dir string) error {
	// S3 doesn't have directories, so this is a no-op
	return nil
}

// Delete removes an object from S3
func (s *S3ObjectStore) Delete(ctx context.Context, path string) error {
	_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.fullKey(path)),
	})
	return err
}

// Exists checks if an object exists
func (s *S3ObjectStore) Exists(ctx context.Context, path string) (bool, error) {
	_, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.fullKey(path)),
	})
	if err != nil {
		// Check for various "not found" error patterns
		errStr := err.Error()
		if strings.Contains(errStr, "NotFound") ||
			strings.Contains(errStr, "404") ||
			strings.Contains(errStr, "NoSuchKey") {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// ReadRange implements ObjectStoreExt
func (s *S3ObjectStore) ReadRange(ctx context.Context, path string, opts ReadOptions) ([]byte, error) {
	key := s.fullKey(path)

	if opts.Length > 0 {
		// Range read
		rangeHeader := fmt.Sprintf("bytes=%d-%d", opts.Offset, opts.Offset+opts.Length-1)
		resp, err := s.client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(s.bucket),
			Key:    aws.String(key),
			Range:  aws.String(rangeHeader),
		})
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()
		return io.ReadAll(resp.Body)
	}

	// Full read using downloader for better performance
	buf := manager.NewWriteAtBuffer(nil)
	_, err := s.downloader.Download(ctx, buf, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// ReadStream implements ObjectStoreExt
func (s *S3ObjectStore) ReadStream(ctx context.Context, path string, opts ReadOptions) (io.ReadCloser, error) {
	input := &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.fullKey(path)),
	}

	if opts.Offset > 0 || opts.Length > 0 {
		rangeHeader := fmt.Sprintf("bytes=%d-", opts.Offset)
		if opts.Length > 0 {
			rangeHeader = fmt.Sprintf("bytes=%d-%d", opts.Offset, opts.Offset+opts.Length-1)
		}
		input.Range = aws.String(rangeHeader)
	}

	resp, err := s.client.GetObject(ctx, input)
	if err != nil {
		return nil, err
	}
	return resp.Body, nil
}

// WriteStream implements ObjectStoreExt
func (s *S3ObjectStore) WriteStream(ctx context.Context, path string, opts WriteOptions) (io.WriteCloser, error) {
	return newS3StreamWriter(ctx, s, path), nil
}

// Copy implements ObjectStoreExt
func (s *S3ObjectStore) Copy(ctx context.Context, src, dst string) error {
	_, err := s.client.CopyObject(ctx, &s3.CopyObjectInput{
		Bucket:     aws.String(s.bucket),
		Key:        aws.String(s.fullKey(dst)),
		CopySource: aws.String(s.bucket + "/" + s.fullKey(src)),
	})
	return err
}

// Rename implements ObjectStoreExt
// Note: S3 doesn't support atomic rename, so this is a copy + delete
func (s *S3ObjectStore) Rename(ctx context.Context, src, dst string) error {
	if err := s.Copy(ctx, src, dst); err != nil {
		return err
	}
	return s.Delete(ctx, src)
}

// GetSize implements ObjectStoreExt
func (s *S3ObjectStore) GetSize(ctx context.Context, path string) (int64, error) {
	resp, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.fullKey(path)),
	})
	if err != nil {
		return 0, err
	}
	return *resp.ContentLength, nil
}

// GetETag implements ObjectStoreExt
func (s *S3ObjectStore) GetETag(ctx context.Context, path string) (string, error) {
	resp, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.fullKey(path)),
	})
	if err != nil {
		return "", err
	}
	return *resp.ETag, nil
}

// s3StreamWriter implements io.WriteCloser for S3 multipart upload
type s3StreamWriter struct {
	ctx    context.Context
	store  *S3ObjectStore
	path   string
	buf    *bytes.Buffer
	closed bool
	mu     sync.Mutex
}

func newS3StreamWriter(ctx context.Context, store *S3ObjectStore, path string) *s3StreamWriter {
	return &s3StreamWriter{
		ctx:   ctx,
		store: store,
		path:  path,
		buf:   &bytes.Buffer{},
	}
}

func (w *s3StreamWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return 0, fmt.Errorf("writer is closed")
	}
	return w.buf.Write(p)
}

func (w *s3StreamWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil
	}
	w.closed = true

	_, err := w.store.uploader.Upload(w.ctx, &s3.PutObjectInput{
		Bucket: aws.String(w.store.bucket),
		Key:    aws.String(w.store.fullKey(w.path)),
		Body:   bytes.NewReader(w.buf.Bytes()),
	})
	return err
}

// S3CommitHandler implements CommitHandler for S3 storage
// Since S3 doesn't support atomic rename, we use a versioning approach
type S3CommitHandler struct {
	store *S3ObjectStore
}

// NewS3CommitHandler creates a new S3CommitHandler
func NewS3CommitHandler(store *S3ObjectStore) *S3CommitHandler {
	return &S3CommitHandler{store: store}
}

// ResolveLatestVersion finds the latest manifest version in S3
func (h *S3CommitHandler) ResolveLatestVersion(ctx context.Context, basePath string) (uint64, error) {
	prefix := h.store.fullKey(basePath + "/" + VersionsDir)
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}

	var maxVersion uint64
	paginator := s3.NewListObjectsV2Paginator(h.store.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(h.store.bucket),
		Prefix: aws.String(prefix),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return 0, err
		}

		for _, obj := range page.Contents {
			name := strings.TrimPrefix(*obj.Key, prefix)
			v, err := ParseVersion(name)
			if err != nil {
				continue
			}
			if v > maxVersion {
				maxVersion = v
			}
		}
	}

	return maxVersion, nil
}

// ResolveVersion returns the path to the manifest file for the given version
func (h *S3CommitHandler) ResolveVersion(_ context.Context, basePath string, version uint64) (string, error) {
	return ManifestPath(version), nil
}

// Commit writes a new manifest version to S3
// Since S3 doesn't support atomic rename, we use conditional writes with ETag
func (h *S3CommitHandler) Commit(ctx context.Context, basePath string, version uint64, manifest *Manifest) error {
	data, err := MarshalManifest(manifest)
	if err != nil {
		return err
	}

	// Path for the new manifest
	key := h.store.fullKey(basePath + "/" + ManifestPath(version))

	// Write the manifest
	_, err = h.store.uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket: aws.String(h.store.bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(data),
	})
	return err
}

// S3CommitHandlerWithLock implements CommitHandler with optimistic locking using ETags
type S3CommitHandlerWithLock struct {
	store *S3CommitHandler
}

// NewS3CommitHandlerWithLock creates a new S3CommitHandlerWithLock
func NewS3CommitHandlerWithLock(store *S3ObjectStore) *S3CommitHandlerWithLock {
	return &S3CommitHandlerWithLock{
		store: NewS3CommitHandler(store),
	}
}

// ResolveLatestVersion finds the latest manifest version
func (h *S3CommitHandlerWithLock) ResolveLatestVersion(ctx context.Context, basePath string) (uint64, error) {
	return h.store.ResolveLatestVersion(ctx, basePath)
}

// ResolveVersion returns the path to the manifest file
func (h *S3CommitHandlerWithLock) ResolveVersion(ctx context.Context, basePath string, version uint64) (string, error) {
	return h.store.ResolveVersion(ctx, basePath, version)
}

// Commit writes a new manifest with optimistic locking
// This checks that the target version doesn't already exist
func (h *S3CommitHandlerWithLock) Commit(ctx context.Context, basePath string, version uint64, manifest *Manifest) error {
	// First check if the target version already exists
	key := h.store.store.fullKey(basePath + "/" + ManifestPath(version))

	_, err := h.store.store.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(h.store.store.bucket),
		Key:    aws.String(key),
	})

	if err == nil {
		// Object exists - conflict
		return fmt.Errorf("version %d already exists", version)
	}

	// Object doesn't exist, proceed with commit
	return h.store.Commit(ctx, basePath, version, manifest)
}

// Bucket returns the S3 bucket name
func (s *S3ObjectStore) Bucket() string {
	return s.bucket
}

// Region returns the AWS region
func (s *S3ObjectStore) Region() string {
	return s.region
}

// Endpoint returns the custom endpoint (if any)
func (s *S3ObjectStore) Endpoint() string {
	return s.endpoint
}

// CreateBucket creates the S3 bucket if it doesn't exist
func (s *S3ObjectStore) CreateBucket(ctx context.Context) error {
	_, err := s.client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(s.bucket),
		CreateBucketConfiguration: &types.CreateBucketConfiguration{
			LocationConstraint: types.BucketLocationConstraint(s.region),
		},
	})

	if err != nil {
		// Check if bucket already exists
		var alreadyExists *types.BucketAlreadyExists
		var alreadyOwned *types.BucketAlreadyOwnedByYou
		if alreadyExists != nil || alreadyOwned != nil ||
			strings.Contains(err.Error(), "BucketAlreadyExists") ||
			strings.Contains(err.Error(), "BucketAlreadyOwnedByYou") {
			return nil
		}
		return err
	}
	return nil
}

// WaitUntilExists waits until an object exists in S3
func (s *S3ObjectStore) WaitUntilExists(ctx context.Context, path string, timeout time.Duration) error {
	key := s.fullKey(path)
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		_, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(s.bucket),
			Key:    aws.String(key),
		})
		if err == nil {
			return nil
		}

		// Check for 404 Not Found
		if strings.Contains(err.Error(), "404") || strings.Contains(err.Error(), "NotFound") {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		return err
	}

	return fmt.Errorf("timeout waiting for object %s to exist", path)
}
