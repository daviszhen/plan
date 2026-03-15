// Copyright 2024 daviszhen <daviszhen@163.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package storage2

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// S3CommitOptions contains options for S3 commit behavior
type S3CommitOptions struct {
	// MaxRetries is the maximum number of retry attempts on conflict
	MaxRetries int
	// RetryDelay is the initial delay between retries (exponential backoff)
	RetryDelay time.Duration
	// MaxRetryDelay is the maximum delay between retries
	MaxRetryDelay time.Duration
	// UseExternalLock enables external coordination service (DynamoDB, etc.)
	UseExternalLock bool
}

// DefaultS3CommitOptions returns default S3 commit options
func DefaultS3CommitOptions() S3CommitOptions {
	return S3CommitOptions{
		MaxRetries:      3,
		RetryDelay:      100 * time.Millisecond,
		MaxRetryDelay:   1 * time.Second,
		UseExternalLock: false,
	}
}

// ExternalLocker defines the interface for external coordination services
// Implementations can use DynamoDB, etcd, ZooKeeper, etc.
type ExternalLocker interface {
	// Acquire attempts to acquire a lock for the given key
	// Returns true if lock was acquired, false if already locked
	Acquire(ctx context.Context, key string, ttl time.Duration) (bool, error)
	// Release releases the lock for the given key
	Release(ctx context.Context, key string) error
	// IsLocked checks if the key is currently locked
	IsLocked(ctx context.Context, key string) (bool, error)
}

// S3CommitHandlerV2 implements CommitHandler with enhanced optimistic locking and retry
type S3CommitHandlerV2 struct {
	store   *S3ObjectStore
	options S3CommitOptions
	locker  ExternalLocker // optional external lock service
}

// NewS3CommitHandlerV2 creates a new S3CommitHandlerV2 with options
func NewS3CommitHandlerV2(store *S3ObjectStore, options S3CommitOptions) *S3CommitHandlerV2 {
	return &S3CommitHandlerV2{
		store:   store,
		options: options,
	}
}

// SetLocker sets an external lock service for coordination
func (h *S3CommitHandlerV2) SetLocker(locker ExternalLocker) {
	h.locker = locker
}

// ResolveLatestVersion finds the latest manifest version in S3
func (h *S3CommitHandlerV2) ResolveLatestVersion(ctx context.Context, basePath string) (uint64, error) {
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
func (h *S3CommitHandlerV2) ResolveVersion(_ context.Context, basePath string, version uint64) (string, error) {
	return ManifestPath(version), nil
}

// Commit writes a new manifest with optimistic locking and retry
func (h *S3CommitHandlerV2) Commit(ctx context.Context, basePath string, version uint64, manifest *Manifest) error {
	return h.commitWithRetry(ctx, basePath, version, manifest, 0)
}

// commitWithRetry implements exponential backoff retry logic
func (h *S3CommitHandlerV2) commitWithRetry(ctx context.Context, basePath string, version uint64, manifest *Manifest, attempt int) error {
	err := h.commitOnce(ctx, basePath, version, manifest)
	if err == nil {
		return nil
	}

	// Check if it's a conflict error
	if isConflictError(err) {
		if attempt >= h.options.MaxRetries {
			return fmt.Errorf("commit failed after %d retries: %w", attempt, err)
		}

		// Calculate backoff delay with exponential increase
		delay := h.options.RetryDelay * time.Duration(1<<uint(attempt))
		if delay > h.options.MaxRetryDelay {
			delay = h.options.MaxRetryDelay
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}

		// Re-resolve latest version and check if we need to abort
		latest, err := h.ResolveLatestVersion(ctx, basePath)
		if err != nil {
			return err
		}
		if latest >= version {
			// Someone else committed our version, this is a real conflict
			return ErrConflict
		}

		// Retry
		return h.commitWithRetry(ctx, basePath, version, manifest, attempt+1)
	}

	return err
}

// commitOnce performs a single commit attempt with conflict detection
func (h *S3CommitHandlerV2) commitOnce(ctx context.Context, basePath string, version uint64, manifest *Manifest) error {
	data, err := MarshalManifest(manifest)
	if err != nil {
		return err
	}

	key := h.store.fullKey(basePath + "/" + ManifestPath(version))

	// Use external lock if available
	if h.locker != nil && h.options.UseExternalLock {
		lockKey := fmt.Sprintf("%s/%s", basePath, ManifestPath(version))
		acquired, err := h.locker.Acquire(ctx, lockKey, 30*time.Second)
		if err != nil {
			return fmt.Errorf("failed to acquire lock: %w", err)
		}
		if !acquired {
			return ErrConflict
		}
		defer h.locker.Release(ctx, lockKey)

		// When using external lock, we need to explicitly check if the file exists
		// because MinIO and some S3-compatible stores don't support IfNoneMatch.
		// The lock ensures no concurrent writes, but we still need to check for
		// previous sequential writes.
		exists, err := h.store.Exists(ctx, basePath+"/"+ManifestPath(version))
		if err != nil {
			return fmt.Errorf("failed to check existence: %w", err)
		}
		if exists {
			return ErrConflict
		}

		// Write without conditional header since we've checked existence under lock
		_, err = h.store.client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(h.store.bucket),
			Key:    aws.String(key),
			Body:   bytes.NewReader(data),
		})
		return err
	}

	// Without external lock, use conditional write with IfNoneMatch: "*" to ensure
	// atomic check-and-put. This tells S3 to fail if the object already exists,
	// preventing two concurrent writes from both succeeding.
	// Note: Some S3-compatible stores (like MinIO) don't support IfNoneMatch,
	// so external locking is recommended for production use with such stores.
	_, err = h.store.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(h.store.bucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader(data),
		IfNoneMatch: aws.String("*"),
	})
	if err != nil {
		// Check if write failed due to concurrent write (precondition failed)
		if isConditionalWriteError(err) || isPreconditionFailed(err) {
			return ErrConflict
		}
		return err
	}

	return nil
}

// isConflictError checks if the error indicates a conflict
func isConflictError(err error) bool {
	if err == nil {
		return false
	}
	if err == ErrConflict {
		return true
	}
	errStr := err.Error()
	return strings.Contains(errStr, "transaction conflict") ||
		strings.Contains(errStr, "already exists") ||
		strings.Contains(errStr, "ConditionCheckFailed") ||
		strings.Contains(errStr, "PreconditionFailed") ||
		strings.Contains(errStr, "409")
}

// isConditionalWriteError checks if the error is from a failed conditional write
func isConditionalWriteError(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	return strings.Contains(errStr, "ConditionCheckFailed") ||
		strings.Contains(errStr, "PreconditionFailed")
}

// isPreconditionFailed checks if the error is an HTTP 412 Precondition Failed
// This is returned by S3 when IfNoneMatch: "*" is used and the object exists
func isPreconditionFailed(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	return strings.Contains(errStr, "412") ||
		strings.Contains(errStr, "PreconditionFailed") ||
		strings.Contains(errStr, "Precondition Failed") ||
		strings.Contains(errStr, "At least one of the pre-conditions")
}

// CommitTransactionWithRetry wraps CommitTransaction with S3-specific retry logic
// This is useful for handling transient conflicts in low-concurrency scenarios
func CommitTransactionWithRetry(ctx context.Context, basePath string, handler *S3CommitHandlerV2, txn *Transaction, maxAttempts int) error {
	var lastErr error
	delay := handler.options.RetryDelay

	for attempt := 0; attempt < maxAttempts; attempt++ {
		err := CommitTransaction(ctx, basePath, handler, txn)
		if err == nil {
			return nil
		}
		if err != ErrConflict {
			return err
		}

		lastErr = err

		// Wait before retry
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}

		// Exponential backoff
		delay *= 2
		if delay > handler.options.MaxRetryDelay {
			delay = handler.options.MaxRetryDelay
		}
	}

	return fmt.Errorf("transaction commit failed after %d attempts: %w", maxAttempts, lastErr)
}
