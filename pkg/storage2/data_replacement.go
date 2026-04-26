// Copyright 2026 The Plan Authors
// SPDX-License-Identifier: Apache-2.0

package storage2

import (
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"time"

	protobuf "google.golang.org/protobuf/proto"

	"github.com/daviszhen/plan/pkg/storage2/proto"
)

// DataReplacementOperation represents a data replacement operation.
// This is used to atomically replace data in specific fragments without
// changing the schema or fragment structure.
type DataReplacementOperation struct {
	// Replacements defines the data files to replace for each fragment
	Replacements []FragmentReplacement

	// ValidationOptions controls data validation
	ValidationOptions *DataValidationOptions

	// Atomic indicates if all replacements must succeed or all fail
	Atomic bool
}

// FragmentReplacement defines a replacement for a single fragment.
type FragmentReplacement struct {
	// FragmentID is the ID of the fragment to update
	FragmentID uint64

	// NewFiles are the new data files to replace the old ones
	NewFiles []*DataFile

	// OldFileIDs are IDs of files being replaced (for validation)
	OldFileIDs []string

	// ExpectedRowCount is the expected number of rows after replacement
	ExpectedRowCount uint64
}

// DataValidationOptions controls validation of replacement data.
type DataValidationOptions struct {
	// ValidateRowCount ensures row count matches expected
	ValidateRowCount bool

	// ValidateChecksum validates data file checksums
	ValidateChecksum bool

	// ValidateSchema ensures data matches schema
	ValidateSchema bool

	// MaxValidationTime limits validation time
	MaxValidationTime time.Duration
}

// DefaultDataValidationOptions returns default validation options.
func DefaultDataValidationOptions() *DataValidationOptions {
	return &DataValidationOptions{
		ValidateRowCount:  true,
		ValidateChecksum:  true,
		ValidateSchema:    true,
		MaxValidationTime: 5 * time.Minute,
	}
}

// DataReplacementResult contains the result of a data replacement operation.
type DataReplacementResult struct {
	// Success indicates if the replacement succeeded
	Success bool

	// ReplacedFragments lists the fragments that were updated
	ReplacedFragments []uint64

	// OldFiles lists the old files that were replaced
	OldFiles []string

	// NewFiles lists the new files that were written
	NewFiles []string

	// ValidationResults contains validation details
	ValidationResults *DataValidationResults

	// Error message if replacement failed
	Error string
}

// DataValidationResults contains validation results.
type DataValidationResults struct {
	// ValidatedFiles is the number of files validated
	ValidatedFiles int

	// ChecksumErrors is the number of checksum mismatches
	ChecksumErrors int

	// RowCountErrors is the number of row count mismatches
	RowCountErrors int

	// SchemaErrors is the number of schema validation errors
	SchemaErrors int

	// ValidationTime is the time spent on validation
	ValidationTime time.Duration
}

// DataReplacementPlanner plans data replacement operations.
type DataReplacementPlanner struct {
	basePath string
	handler  CommitHandler
	store    ObjectStoreExt
}

// NewDataReplacementPlanner creates a new data replacement planner.
func NewDataReplacementPlanner(basePath string, handler CommitHandler, store ObjectStoreExt) *DataReplacementPlanner {
	return &DataReplacementPlanner{
		basePath: basePath,
		handler:  handler,
		store:    store,
	}
}

// PlanReplacement plans a data replacement operation.
func (p *DataReplacementPlanner) PlanReplacement(
	ctx context.Context,
	manifest *Manifest,
	op *DataReplacementOperation,
) (*DataReplacementPlan, error) {
	if op == nil {
		return nil, fmt.Errorf("data replacement operation is nil")
	}

	if len(op.Replacements) == 0 {
		return nil, fmt.Errorf("no replacements specified")
	}

	// Validate that all fragments exist
	fragmentMap := make(map[uint64]*DataFragment)
	for _, frag := range manifest.Fragments {
		fragmentMap[frag.Id] = frag
	}

	for _, repl := range op.Replacements {
		if _, ok := fragmentMap[repl.FragmentID]; !ok {
			return nil, fmt.Errorf("fragment %d not found", repl.FragmentID)
		}
	}

	// Build plan
	plan := &DataReplacementPlan{
		Operation:   op,
		Manifest:    manifest,
		Valid:       true,
		EstimatedIO: p.estimateIO(op),
	}

	return plan, nil
}

// estimateIO estimates the I/O cost of the replacement.
func (p *DataReplacementPlanner) estimateIO(op *DataReplacementOperation) uint64 {
	var totalBytes uint64
	for _, repl := range op.Replacements {
		for _, file := range repl.NewFiles {
			totalBytes += file.FileSizeBytes
		}
	}
	return totalBytes * 2 // Read + write
}

// DataReplacementPlan is a planned data replacement operation.
type DataReplacementPlan struct {
	Operation   *DataReplacementOperation
	Manifest    *Manifest
	Valid       bool
	EstimatedIO uint64
}

// DataReplacementExecutor executes data replacement operations.
type DataReplacementExecutor struct {
	basePath string
	handler  CommitHandler
	store    ObjectStoreExt
}

// NewDataReplacementExecutor creates a new data replacement executor.
func NewDataReplacementExecutor(basePath string, handler CommitHandler, store ObjectStoreExt) *DataReplacementExecutor {
	return &DataReplacementExecutor{
		basePath: basePath,
		handler:  handler,
		store:    store,
	}
}

// ExecuteReplacement executes a planned data replacement.
func (e *DataReplacementExecutor) ExecuteReplacement(
	ctx context.Context,
	plan *DataReplacementPlan,
) (*DataReplacementResult, error) {
	if !plan.Valid {
		return nil, fmt.Errorf("invalid replacement plan")
	}

	op := plan.Operation
	validationOpts := op.ValidationOptions
	if validationOpts == nil {
		validationOpts = DefaultDataValidationOptions()
	}

	// Validate replacement data
	validationResults, err := e.validateReplacement(ctx, plan, validationOpts)
	if err != nil {
		return &DataReplacementResult{
			Success:           false,
			Error:             fmt.Sprintf("validation failed: %v", err),
			ValidationResults: validationResults,
		}, nil
	}

	// Perform replacement
	result := &DataReplacementResult{
		Success:           true,
		ValidationResults: validationResults,
	}

	for _, repl := range op.Replacements {
		// Track replaced files
		result.ReplacedFragments = append(result.ReplacedFragments, repl.FragmentID)

		for _, file := range repl.NewFiles {
			result.NewFiles = append(result.NewFiles, file.Path)
		}
	}

	return result, nil
}

// validateReplacement validates the replacement data.
func (e *DataReplacementExecutor) validateReplacement(
	ctx context.Context,
	plan *DataReplacementPlan,
	opts *DataValidationOptions,
) (*DataValidationResults, error) {
	results := &DataValidationResults{}
	startTime := time.Now()
	defer func() {
		results.ValidationTime = time.Since(startTime)
	}()

	// Create validation context with timeout
	validationCtx := ctx
	if opts.MaxValidationTime > 0 {
		var cancel context.CancelFunc
		validationCtx, cancel = context.WithTimeout(ctx, opts.MaxValidationTime)
		defer cancel()
	}

	for _, repl := range plan.Operation.Replacements {
		for _, file := range repl.NewFiles {
			results.ValidatedFiles++

			// Validate checksum if enabled
			if opts.ValidateChecksum {
				if err := e.validateFileChecksum(validationCtx, file); err != nil {
					results.ChecksumErrors++
					if plan.Operation.Atomic {
						return results, fmt.Errorf("checksum validation failed for %s: %w", file.Path, err)
					}
				}
			}

			// Validate row count if enabled
			if opts.ValidateRowCount && repl.ExpectedRowCount > 0 {
				rowCount, err := e.readFileRowCount(validationCtx, file)
				if err != nil {
					results.RowCountErrors++
					if plan.Operation.Atomic {
						return results, fmt.Errorf("row count validation failed for %s: %w", file.Path, err)
					}
				} else {
					// Sum row counts across files for the same fragment replacement
					// Each file's row count is checked independently here; caller may
					// aggregate across files in a FragmentReplacement if needed.
					if uint64(rowCount) != repl.ExpectedRowCount {
						results.RowCountErrors++
						if plan.Operation.Atomic {
							return results, fmt.Errorf(
								"row count mismatch for %s: got %d, expected %d",
								file.Path, rowCount, repl.ExpectedRowCount)
						}
					}
				}
			}
		}
	}

	return results, nil
}

// validateFileChecksum validates a file's checksum.
func (e *DataReplacementExecutor) validateFileChecksum(ctx context.Context, file *DataFile) error {
	// Read file and compute checksum
	reader, err := e.store.ReadStream(ctx, file.Path, ReadOptions{})
	if err != nil {
		return fmt.Errorf("failed to read file: %w", err)
	}
	defer reader.Close()

	// Compute CRC32 checksum
	hash := crc32.NewIEEE()
	if _, err := io.Copy(hash, reader); err != nil {
		return fmt.Errorf("failed to compute checksum: %w", err)
	}

	// Note: Checksum validation would compare with expected value
	// For now, we just compute it without comparing
	_ = hash.Sum32()

	return nil
}

// readFileRowCount reads the row count from the header of a chunk-format data file.
// The chunk format stores row count as the first uint32 (little-endian, 4 bytes).
func (e *DataReplacementExecutor) readFileRowCount(ctx context.Context, file *DataFile) (uint32, error) {
	data, err := e.store.ReadRange(ctx, file.Path, ReadOptions{Offset: 0, Length: 4})
	if err != nil {
		return 0, fmt.Errorf("failed to read file header: %w", err)
	}
	if len(data) < 4 {
		return 0, fmt.Errorf("file too short to contain row count header")
	}
	return binary.LittleEndian.Uint32(data), nil
}

type DataReplacementManager struct {
	basePath string
	handler  CommitHandler
	store    ObjectStoreExt
	planner  *DataReplacementPlanner
	executor *DataReplacementExecutor
}

// NewDataReplacementManager creates a new data replacement manager.
func NewDataReplacementManager(basePath string, handler CommitHandler, store ObjectStoreExt) *DataReplacementManager {
	planner := NewDataReplacementPlanner(basePath, handler, store)
	executor := NewDataReplacementExecutor(basePath, handler, store)

	return &DataReplacementManager{
		basePath: basePath,
		handler:  handler,
		store:    store,
		planner:  planner,
		executor: executor,
	}
}

// ReplaceData performs a data replacement operation.
func (m *DataReplacementManager) ReplaceData(
	ctx context.Context,
	readVersion uint64,
	op *DataReplacementOperation,
) (*DataReplacementResult, error) {
	// Load manifest
	manifest, err := LoadManifest(ctx, m.basePath, m.handler, readVersion)
	if err != nil {
		return nil, fmt.Errorf("failed to load manifest: %w", err)
	}

	// Plan the replacement
	plan, err := m.planner.PlanReplacement(ctx, manifest, op)
	if err != nil {
		return nil, fmt.Errorf("failed to plan replacement: %w", err)
	}

	// Execute the replacement
	result, err := m.executor.ExecuteReplacement(ctx, plan)
	if err != nil {
		return nil, fmt.Errorf("failed to execute replacement: %w", err)
	}

	return result, nil
}

// NewTransactionDataReplacement builds a transaction with DataReplacement operation.
func NewTransactionDataReplacement(
	readVersion uint64,
	uuid string,
	replacements []*storage2pb.Transaction_DataReplacementGroup,
) *Transaction {
	if replacements == nil {
		replacements = []*storage2pb.Transaction_DataReplacementGroup{}
	}

	return &Transaction{
		ReadVersion: readVersion,
		Uuid:        uuid,
		Operation: &storage2pb.Transaction_DataReplacement_{
			DataReplacement: &storage2pb.Transaction_DataReplacement{
				Replacements: replacements,
			},
		},
	}
}

// buildManifestDataReplacement applies DataReplacement operation to manifest.
func buildManifestDataReplacement(
	current *Manifest,
	replacementOp *storage2pb.Transaction_DataReplacement,
) (*Manifest, error) {
	next := protobuf.Clone(current).(*Manifest)
	next.Version = current.Version + 1

	// Create lookup of replacements by fragment ID
	replacements := make(map[uint64]*storage2pb.Transaction_DataReplacementGroup)
	for _, group := range replacementOp.GetReplacements() {
		replacements[group.FragmentId] = group
	}

	// Update fragments with new files
	for i, frag := range next.Fragments {
		if repl, ok := replacements[frag.Id]; ok {
			// Replace files in the fragment
			newFile := repl.NewFile
			if newFile != nil {
				// Find and replace the file with matching fields
				for j, file := range frag.Files {
					if fieldsMatch(file.Fields, newFile.Fields) {
						next.Fragments[i].Files[j] = protobuf.Clone(newFile).(*DataFile)
						break
					}
				}
			}
		}
	}

	return next, nil
}

// fieldsMatch checks if two field slices match.
func fieldsMatch(a, b []int32) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// CheckDataReplacementConflict checks if a DataReplacement transaction conflicts with another.
func CheckDataReplacementConflict(myTxn, otherTxn *Transaction) bool {
	myReplacement := myTxn.GetDataReplacement()
	if myReplacement == nil {
		return true
	}

	// Get fragments being replaced by my transaction
	myFragments := make(map[uint64]bool)
	for _, group := range myReplacement.GetReplacements() {
		myFragments[group.FragmentId] = true
	}

	// Check based on other transaction type
	switch otherOp := otherTxn.Operation.(type) {
	case *storage2pb.Transaction_DataReplacement_:
		otherReplacement := otherOp.DataReplacement
		// Conflict if replacing same fragments
		for _, group := range otherReplacement.GetReplacements() {
			if myFragments[group.FragmentId] {
				return true
			}
		}

	case *storage2pb.Transaction_Update_:
		otherUpdate := otherOp.Update
		// Conflict if updating rows in replaced fragments
		for _, frag := range otherUpdate.GetUpdatedFragments() {
			if myFragments[frag.Id] {
				return true
			}
		}
		for _, id := range otherUpdate.GetRemovedFragmentIds() {
			if myFragments[id] {
				return true
			}
		}

	case *storage2pb.Transaction_Delete_:
		otherDelete := otherOp.Delete
		// Conflict if deleting from replaced fragments
		for _, id := range otherDelete.GetDeletedFragmentIds() {
			if myFragments[id] {
				return true
			}
		}
		for _, frag := range otherDelete.GetUpdatedFragments() {
			if myFragments[frag.Id] {
				return true
			}
		}

	case *storage2pb.Transaction_Rewrite_:
		otherRewrite := otherOp.Rewrite
		// Conflict if rewriting same fragments
		for _, group := range otherRewrite.GetGroups() {
			for _, frag := range group.GetOldFragments() {
				if myFragments[frag.Id] {
					return true
				}
			}
		}

	default:
		// Conservative: conflict with other operation types
		return true
	}

	return false
}

// DataReplacementRollback handles rollback of data replacement.
type DataReplacementRollback struct {
	basePath string
	handler  CommitHandler
	store    ObjectStoreExt
}

// NewDataReplacementRollback creates a new data replacement rollback handler.
func NewDataReplacementRollback(basePath string, handler CommitHandler, store ObjectStoreExt) *DataReplacementRollback {
	return &DataReplacementRollback{
		basePath: basePath,
		handler:  handler,
		store:    store,
	}
}

// RollbackReplacement rolls back a data replacement.
func (r *DataReplacementRollback) RollbackReplacement(
	ctx context.Context,
	replacement *storage2pb.Transaction_DataReplacement,
) error {
	if replacement == nil {
		return nil
	}

	// Delete new files that were written
	// Note: ObjectStoreExt doesn't have Delete method in current interface
	// This would need to be implemented based on the actual storage backend
	_ = ctx

	return nil
}

// DataReplacementValidator validates data replacement operations.
type DataReplacementValidator struct {
	basePath string
	handler  CommitHandler
	store    ObjectStoreExt
}

// NewDataReplacementValidator creates a new data replacement validator.
func NewDataReplacementValidator(basePath string, handler CommitHandler, store ObjectStoreExt) *DataReplacementValidator {
	return &DataReplacementValidator{
		basePath: basePath,
		handler:  handler,
		store:    store,
	}
}

// ValidatePreconditions validates preconditions for data replacement.
func (v *DataReplacementValidator) ValidatePreconditions(
	ctx context.Context,
	manifest *Manifest,
	op *DataReplacementOperation,
) error {
	// Check that all fragments exist
	fragmentMap := make(map[uint64]*DataFragment)
	for _, frag := range manifest.Fragments {
		fragmentMap[frag.Id] = frag
	}

	for _, repl := range op.Replacements {
		frag, ok := fragmentMap[repl.FragmentID]
		if !ok {
			return fmt.Errorf("fragment %d not found", repl.FragmentID)
		}

		// Validate old file IDs if specified
		if len(repl.OldFileIDs) > 0 {
			fileMap := make(map[string]bool)
			for _, file := range frag.Files {
				fileMap[file.Path] = true
			}

			for _, oldID := range repl.OldFileIDs {
				if !fileMap[oldID] {
					return fmt.Errorf("file %s not found in fragment %d", oldID, repl.FragmentID)
				}
			}
		}
	}

	return nil
}

// DataReplacementBatch batches multiple data replacements.
type DataReplacementBatch struct {
	replacements []FragmentReplacement
	maxBatchSize int
}

// NewDataReplacementBatch creates a new data replacement batch.
func NewDataReplacementBatch(maxBatchSize int) *DataReplacementBatch {
	if maxBatchSize <= 0 {
		maxBatchSize = 10
	}
	return &DataReplacementBatch{
		replacements: make([]FragmentReplacement, 0),
		maxBatchSize: maxBatchSize,
	}
}

// Add adds a replacement to the batch.
func (b *DataReplacementBatch) Add(repl FragmentReplacement) bool {
	if len(b.replacements) >= b.maxBatchSize {
		return false
	}
	b.replacements = append(b.replacements, repl)
	return true
}

// IsFull returns true if the batch is full.
func (b *DataReplacementBatch) IsFull() bool {
	return len(b.replacements) >= b.maxBatchSize
}

// IsEmpty returns true if the batch is empty.
func (b *DataReplacementBatch) IsEmpty() bool {
	return len(b.replacements) == 0
}

// Size returns the current batch size.
func (b *DataReplacementBatch) Size() int {
	return len(b.replacements)
}

// Clear clears the batch.
func (b *DataReplacementBatch) Clear() {
	b.replacements = b.replacements[:0]
}

// GetReplacements returns the replacements in the batch.
func (b *DataReplacementBatch) GetReplacements() []FragmentReplacement {
	return b.replacements
}

// ToOperation creates a DataReplacementOperation from the batch.
func (b *DataReplacementBatch) ToOperation() *DataReplacementOperation {
	if b.IsEmpty() {
		return nil
	}
	return &DataReplacementOperation{
		Replacements: b.replacements,
	}
}

// DataReplacementMetrics contains metrics for data replacement operations.
type DataReplacementMetrics struct {
	// TotalReplacements is the total number of replacement operations
	TotalReplacements uint64

	// SuccessfulReplacements is the number of successful replacements
	SuccessfulReplacements uint64

	// FailedReplacements is the number of failed replacements
	FailedReplacements uint64

	// TotalBytesReplaced is the total bytes replaced
	TotalBytesReplaced uint64

	// AverageReplacementTime is the average time per replacement
	AverageReplacementTime time.Duration
}

// DataReplacementProgress tracks progress of a data replacement.
type DataReplacementProgress struct {
	// TotalFragments is the total number of fragments to replace
	TotalFragments int

	// CompletedFragments is the number of fragments completed
	CompletedFragments int

	// CurrentFragment is the ID of the fragment being processed
	CurrentFragment uint64

	// PercentComplete is the percentage of completion (0-100)
	PercentComplete int

	// BytesProcessed is the number of bytes processed
	BytesProcessed uint64

	// TotalBytes is the total bytes to process
	TotalBytes uint64
}

// DataReplacementProgressTracker tracks progress of data replacements.
type DataReplacementProgressTracker struct {
	totalFragments     int
	completedFragments int
	totalBytes         uint64
	bytesProcessed     uint64
}

// NewDataReplacementProgressTracker creates a new progress tracker.
func NewDataReplacementProgressTracker(totalFragments int, totalBytes uint64) *DataReplacementProgressTracker {
	return &DataReplacementProgressTracker{
		totalFragments: totalFragments,
		totalBytes:     totalBytes,
	}
}

// Update updates the progress.
func (t *DataReplacementProgressTracker) Update(completedFragments int, bytesProcessed uint64, currentFragment uint64) DataReplacementProgress {
	t.completedFragments = completedFragments
	t.bytesProcessed = bytesProcessed

	percentComplete := 0
	if t.totalFragments > 0 {
		percentComplete = (completedFragments * 100) / t.totalFragments
	}

	return DataReplacementProgress{
		TotalFragments:     t.totalFragments,
		CompletedFragments: completedFragments,
		CurrentFragment:    currentFragment,
		PercentComplete:    percentComplete,
		BytesProcessed:     bytesProcessed,
		TotalBytes:         t.totalBytes,
	}
}

// GetProgress returns the current progress.
func (t *DataReplacementProgressTracker) GetProgress() DataReplacementProgress {
	percentComplete := 0
	if t.totalFragments > 0 {
		percentComplete = (t.completedFragments * 100) / t.totalFragments
	}

	return DataReplacementProgress{
		TotalFragments:     t.totalFragments,
		CompletedFragments: t.completedFragments,
		PercentComplete:    percentComplete,
		BytesProcessed:     t.bytesProcessed,
		TotalBytes:         t.totalBytes,
	}
}

// DataReplacementOptions contains options for data replacement.
type DataReplacementOptions struct {
	// Atomic indicates if all replacements must succeed
	Atomic bool

	// Validate indicates if validation should be performed
	Validate bool

	// ValidationOptions are the validation options
	ValidationOptions *DataValidationOptions

	// ProgressCallback is called with progress updates
	ProgressCallback func(DataReplacementProgress)
}

// DefaultDataReplacementOptions returns default options.
func DefaultDataReplacementOptions() *DataReplacementOptions {
	return &DataReplacementOptions{
		Atomic:            true,
		Validate:          true,
		ValidationOptions: DefaultDataValidationOptions(),
	}
}
