// Package sdk provides a high-level API for the storage2 engine (Dataset, OpenDataset, CreateDataset).
package sdk

import (
	"context"
	"crypto/rand"
	"fmt"
	"time"

	"github.com/daviszhen/plan/pkg/chunk"
	"github.com/daviszhen/plan/pkg/storage2"
	"github.com/daviszhen/plan/pkg/storage2/proto"
)

// DefaultStoreFactory is the global default store factory
var defaultStoreFactory = storage2.NewStoreFactory(storage2.StoreFactoryOptions{
	EnableCache: true,
})

// SetDefaultStoreFactory sets the global default store factory
func SetDefaultStoreFactory(factory *storage2.StoreFactory) {
	defaultStoreFactory = factory
}

// GetDefaultStoreFactory returns the global default store factory
func GetDefaultStoreFactory() *storage2.StoreFactory {
	return defaultStoreFactory
}

// ColumnAlteration describes how to alter an existing column.
type ColumnAlteration struct {
	// Path is the field path to alter (e.g., "name" or "address.city").
	Path string
	// NewName is the new name for the column (empty means no rename).
	NewName string
	// NewNullable changes the nullable property (nil means no change).
	NewNullable *bool
	// NewDataType changes the logical data type (empty means no change).
	NewDataType string
}

// ColumnAddition describes a new column to add to the dataset.
type ColumnAddition struct {
	// Field is the field definition for the new column.
	Field *storage2pb.Field
	// DefaultValue is a SQL expression for the default value (e.g., "0", "'unknown'", "NULL").
	DefaultValue string
}

// CompactionOptions controls the behavior of compaction operations.
type CompactionOptions struct {
	// MaxBytes is the maximum size in bytes for a compacted fragment.
	// Fragments larger than this will not be further compacted.
	MaxBytes *uint64
	// BatchSize is the maximum number of fragments to compact in a single operation.
	BatchSize *int
	// TargetFragmentSize is the desired size of compacted fragments.
	TargetFragmentSize *uint64
	// MinFragmentSize is the minimum size in bytes below which fragments will be compacted.
	MinFragmentSize *uint64
	// MaxFragmentSize is the maximum size in bytes above which fragments will be split.
	MaxFragmentSize *uint64
	// CompactionMethod specifies the compaction strategy to use.
	CompactionMethod *string // "size_based", "count_based", "hybrid"
	// IncludeDeletedRows controls whether deleted rows should be included in compaction.
	IncludeDeletedRows *bool
	// PreserveRowIds controls whether row IDs should be preserved during compaction.
	PreserveRowIds *bool
}

// DistributedCompactionOptions controls distributed compaction behavior.
type DistributedCompactionOptions struct {
	// MaxConcurrency is the maximum number of parallel compaction workers.
	MaxConcurrency int
	// TargetTaskSize is the target size in bytes for each compaction task.
	TargetTaskSize uint64
	// MaxTaskSize is the maximum size in bytes for each compaction task.
	MaxTaskSize uint64
	// MaxFragmentsPerTask limits the number of fragments per task.
	MaxFragmentsPerTask int
	// MinFragmentsPerTask is the minimum number of fragments per task.
	MinFragmentsPerTask int
	// Strategy determines how fragments are grouped ("size", "count", "hybrid", "bin_packing").
	Strategy string
	// RetryOnConflict whether to retry on commit conflict.
	RetryOnConflict bool
	// MaxRetries is the maximum number of retries on conflict.
	MaxRetries int
	// PreserveRowIds whether to preserve row IDs during compaction.
	PreserveRowIds bool
	// IncludeDeletedRows whether to include deleted rows.
	IncludeDeletedRows bool
}

// Dataset is the main interface for a versioned table (open or create).
type Dataset interface {
	Close() error
	Version() uint64
	LatestVersion() (uint64, error)
	CountRows() (uint64, error)
	// CountRowsWithFilter counts rows matching the given filter predicate.
	// See CountRowsWithFilter method documentation for filter syntax.
	CountRowsWithFilter(ctx context.Context, filter string) (uint64, error)
	// DataSize returns the total data size in bytes based on manifest metadata.
	DataSize() (uint64, error)
	// Schema returns the schema of the current version.
	Schema() []*storage2pb.Field
	// SchemaMetadata returns the schema metadata of the current version.
	SchemaMetadata() map[string][]byte
	// FieldByName returns the field configuration for a field with the given name.
	FieldByName(name string) *storage2pb.Field
	// FieldByID returns the field configuration for a field with the given ID.
	FieldByID(id int32) *storage2pb.Field
	// FieldsByParentID returns all fields that have the given parent ID.
	FieldsByParentID(parentID int32) []*storage2pb.Field
	Append(ctx context.Context, fragments []*DataFragment) error
	Delete(ctx context.Context, predicate string) error
	Overwrite(ctx context.Context, fragments []*DataFragment) error
	// DropColumns removes the specified columns from the dataset schema.
	DropColumns(ctx context.Context, columnNames []string) error
	// AlterColumns modifies column properties (name, nullable, data type).
	AlterColumns(ctx context.Context, alterations []ColumnAlteration) error
	// AddColumns adds new columns to the dataset using SQL expressions or default values.
	AddColumns(ctx context.Context, additions []ColumnAddition) error
	// DropPath removes a nested field path from the dataset schema.
	DropPath(ctx context.Context, path string) error
	// ShallowClone creates a shallow clone of the dataset at the target path.
	// The clone references the original data files without copying them.
	ShallowClone(ctx context.Context, targetPath string) (Dataset, error)
	// Compact performs data compaction to merge small fragments and clean up deleted data.
	Compact(ctx context.Context) error
	// CompactWithOptions performs data compaction with specific options.
	CompactWithOptions(ctx context.Context, opts CompactionOptions) error
	// DistributedCompact performs distributed compaction with parallel workers.
	DistributedCompact(ctx context.Context, opts DistributedCompactionOptions) (*storage2.CompactionStats, error)
	// Take returns rows at the given logical indices for the current version.
	// This is a minimal random-access API built on top of storage2.TakeRows.
	Take(ctx context.Context, indices []uint64) (*chunk.Chunk, error)
	// TakeProjected returns rows at the given logical indices and only the specified
	// zero-based column indices. If columns is empty, it behaves like Take.
	TakeProjected(ctx context.Context, indices []uint64, columns []int) (*chunk.Chunk, error)
	// Update performs row-level updates on the dataset.
	// predicate filters which rows to update (empty string means all rows).
	// updates maps column names to their new values.
	Update(ctx context.Context, predicate string, updates map[string]interface{}) (*storage2.UpdateResult, error)

	// Merge applies a merge operation that adds new fragments and/or updates the schema.
	Merge(ctx context.Context, fragments []*DataFragment, newSchema []*storage2pb.Field) error

	// Restore restores the dataset to a previous version by creating a new version
	// with the content of the specified version.
	Restore(ctx context.Context, version uint64) error

	// CheckoutVersion switches the dataset to a read-only view of the specified version.
	CheckoutVersion(ctx context.Context, version uint64) error

	// CreateTag creates a tag pointing to the specified version.
	CreateTag(ctx context.Context, tag string, version uint64) error
	// DeleteTag deletes a tag by name.
	DeleteTag(ctx context.Context, tag string) error
	// ListTags returns all tags as a map of tag name to version number.
	ListTags(ctx context.Context) (map[string]uint64, error)

	// Scanner creates a ScannerBuilder for streaming reads.
	Scanner() *ScannerBuilder

	// Detached Transaction methods

	// CreateDetachedAppend creates a detached append transaction.
	// Returns a transaction ID that can be used to commit later.
	CreateDetachedAppend(ctx context.Context, fragments []*DataFragment, timeout *time.Duration) (string, error)
	// CreateDetachedDelete creates a detached delete transaction.
	CreateDetachedDelete(ctx context.Context, predicate string, timeout *time.Duration) (string, error)
	// CreateDetachedOverwrite creates a detached overwrite transaction.
	CreateDetachedOverwrite(ctx context.Context, fragments []*DataFragment, timeout *time.Duration) (string, error)
	// CommitDetached commits a detached transaction by ID.
	// Returns the resulting version number.
	CommitDetached(ctx context.Context, txnID string) (uint64, error)
	// GetDetachedStatus returns the status of a detached transaction.
	GetDetachedStatus(ctx context.Context, txnID string) (*storage2.DetachedTransactionState, error)
	// ListDetached lists detached transactions with the given status.
	ListDetached(ctx context.Context, status storage2.DetachedTransactionStatus) ([]*storage2.DetachedTransactionState, error)
	// CleanupExpiredDetached removes expired detached transactions.
	CleanupExpiredDetached(ctx context.Context) (int, error)
	// DeleteDetached deletes a detached transaction state file.
	DeleteDetached(txnID string) error

	// KNN Vector Search methods

	// CreateVectorIndex creates a vector index for KNN search.
	// config specifies the index type (IVF or HNSW), dimension, metric, etc.
	CreateVectorIndex(ctx context.Context, config storage2.VectorSearchIndexConfig) error
	// CreateVectorIndexSimple creates a vector index with default parameters.
	// indexType should be "ivf" or "hnsw".
	CreateVectorIndexSimple(ctx context.Context, name string, columnIdx int, dimension int, metric storage2.MetricType, indexType string) error
	// DropVectorIndex drops a vector index.
	DropVectorIndex(ctx context.Context, name string) error
	// ListVectorIndexes lists all vector index names.
	ListVectorIndexes() []string
	// SearchNearest performs KNN search using the specified index.
	// Returns up to k nearest neighbors with their distances.
	SearchNearest(ctx context.Context, indexName string, queryVector []float32, k int) (*storage2.SearchResults, error)
	// BuildVectorIndex builds a vector index from the given vectors.
	BuildVectorIndex(ctx context.Context, indexName string, vectors map[uint64][]float32) error
	// SaveVectorIndex persists a vector index to storage.
	SaveVectorIndex(ctx context.Context, name string) error
	// LoadVectorIndex loads a vector index from storage.
	LoadVectorIndex(ctx context.Context, name string, config storage2.VectorSearchIndexConfig) error
	// GetVectorIndex returns the vector index by name.
	GetVectorIndex(name string) (storage2.VectorSearchIndex, bool)
}

type datasetImpl struct {
	basePath        string
	handler         storage2.CommitHandler
	currentManifest *storage2.Manifest
	version         uint64
	closed          bool
	knnManager      *storage2.KNNIndexManager
	indexPersist    *storage2.IndexPersistence
}

func (d *datasetImpl) Close() error {
	if d.closed {
		return nil
	}
	d.closed = true
	return nil
}

func (d *datasetImpl) Version() uint64 {
	return d.version
}

func (d *datasetImpl) LatestVersion() (uint64, error) {
	return d.handler.ResolveLatestVersion(context.Background(), d.basePath)
}

func (d *datasetImpl) CountRows() (uint64, error) {
	if d.currentManifest == nil {
		return 0, nil
	}
	var total uint64
	for _, frag := range d.currentManifest.Fragments {
		total += frag.PhysicalRows
	}
	return total, nil
}

// CountRowsWithFilter counts rows matching the given filter predicate.
// The filter string supports:
//   - Comparison: "c0 = 1", "c1 > 10", "c2 <= 100"
//   - String comparison: "name = 'foo'", "name LIKE 'bar%'"
//   - NULL checks: "c0 IS NULL", "c1 IS NOT NULL"
//   - IN expressions: "id IN (1, 2, 3)"
//   - Boolean operators: "c0 > 10 AND c1 < 100", "c0 = 1 OR c0 = 2", "NOT c0 = 1"
//   - Parentheses: "(c0 > 10 OR c1 > 10) AND c2 < 100"
func (d *datasetImpl) CountRowsWithFilter(ctx context.Context, filter string) (uint64, error) {
	if d.currentManifest == nil {
		return 0, nil
	}

	// Parse filter expression
	predicate, err := storage2.ParseFilter(filter, nil)
	if err != nil {
		return 0, fmt.Errorf("failed to parse filter: %w", err)
	}

	if predicate == nil {
		return d.CountRows()
	}

	return storage2.CountRowsWithFilter(ctx, d.basePath, d.handler, d.version, predicate)
}

func (d *datasetImpl) DataSize() (uint64, error) {
	return storage2.CalculateDatasetDataSize(context.Background(), d.basePath, d.handler, d.version)
}

func (d *datasetImpl) Schema() []*storage2pb.Field {
	if d.currentManifest == nil {
		return nil
	}
	return d.currentManifest.Fields
}

func (d *datasetImpl) SchemaMetadata() map[string][]byte {
	if d.currentManifest == nil {
		return nil
	}
	return d.currentManifest.SchemaMetadata
}

func (d *datasetImpl) FieldByName(name string) *storage2pb.Field {
	if d.currentManifest == nil {
		return nil
	}

	for _, field := range d.currentManifest.Fields {
		if field.Name == name {
			return field
		}
	}
	return nil
}

func (d *datasetImpl) FieldByID(id int32) *storage2pb.Field {
	if d.currentManifest == nil {
		return nil
	}

	for _, field := range d.currentManifest.Fields {
		if field.Id == id {
			return field
		}
	}
	return nil
}

func (d *datasetImpl) FieldsByParentID(parentID int32) []*storage2pb.Field {
	if d.currentManifest == nil {
		return nil
	}

	var result []*storage2pb.Field
	for _, field := range d.currentManifest.Fields {
		if field.ParentId == parentID {
			result = append(result, field)
		}
	}
	return result
}

func (d *datasetImpl) Compact(ctx context.Context) error {
	return d.CompactWithOptions(ctx, CompactionOptions{})
}

func (d *datasetImpl) CompactWithOptions(ctx context.Context, opts CompactionOptions) error {
	if d.closed {
		return fmt.Errorf("dataset is closed")
	}

	// Simple compaction strategy: merge fragments based on options
	if len(d.currentManifest.Fragments) <= 1 {
		// Nothing to compact
		return nil
	}

	readVersion := d.version
	uuid := generateUUID()

	// Apply compaction options
	fragments := d.currentManifest.Fragments
	batchSize := 2 // default batch size
	if opts.BatchSize != nil {
		batchSize = *opts.BatchSize
		if batchSize < 2 {
			batchSize = 2
		}
	}

	var groups []*storage2pb.Transaction_Rewrite_RewriteGroup

	// Group fragments for compaction
	for i := 0; i < len(fragments); i += batchSize {
		end := i + batchSize
		if end > len(fragments) {
			end = len(fragments)
		}

		batch := fragments[i:end]

		// Check if this batch should be compacted based on options
		shouldCompact := true

		// Check max bytes constraint
		if opts.MaxBytes != nil {
			var totalSize uint64
			for _, frag := range batch {
				for _, file := range frag.Files {
					totalSize += file.FileSizeBytes
				}
			}
			if totalSize > *opts.MaxBytes {
				shouldCompact = false
			}
		}

		// Check min fragment size constraint
		if shouldCompact && opts.MinFragmentSize != nil {
			var totalSize uint64
			for _, frag := range batch {
				for _, file := range frag.Files {
					totalSize += file.FileSizeBytes
				}
			}
			if totalSize < *opts.MinFragmentSize {
				shouldCompact = true // Force compaction for small fragments
			}
		}

		// Check compaction method
		if shouldCompact && opts.CompactionMethod != nil {
			method := *opts.CompactionMethod
			switch method {
			case "size_based":
				// Already handled by MaxBytes/MinFragmentSize
			case "count_based":
				// Compact based on fragment count
				if len(batch) < 2 {
					shouldCompact = false
				}
			case "hybrid":
				// Combine size and count constraints
				var totalSize uint64
				for _, frag := range batch {
					for _, file := range frag.Files {
						totalSize += file.FileSizeBytes
					}
				}
				if len(batch) < 2 || totalSize > getDefaultMaxBytes() {
					shouldCompact = false
				}
			}
		}

		// Check deleted rows inclusion
		hasDeletions := false
		for _, frag := range batch {
			if frag.DeletionFile != nil {
				hasDeletions = true
				break
			}
		}

		if shouldCompact && hasDeletions && opts.IncludeDeletedRows != nil && !*opts.IncludeDeletedRows {
			// Skip batches with deletions if not explicitly included
			shouldCompact = false
		}

		if shouldCompact && len(batch) > 1 {
			// Calculate total rows for new fragment
			var totalRows uint64
			for _, frag := range batch {
				totalRows += frag.PhysicalRows
			}

			newFragment := storage2.NewDataFragmentWithRows(
				uint64(len(groups)), // new fragment ID
				totalRows,
				nil, // files will be populated during actual data processing
			)

			// Handle row ID preservation
			if opts.PreserveRowIds != nil && *opts.PreserveRowIds {
				// Preserve row IDs by copying sequence (simplified implementation)
				// In a real implementation, this would need to merge row ID sequences properly
			}

			group := &storage2pb.Transaction_Rewrite_RewriteGroup{
				OldFragments: batch,
				NewFragments: []*storage2.DataFragment{newFragment},
			}
			groups = append(groups, group)
		}
	}

	if len(groups) == 0 {
		// No compaction needed based on options
		return nil
	}

	txn := &storage2.Transaction{
		ReadVersion: readVersion,
		Uuid:        uuid,
		Operation: &storage2pb.Transaction_Rewrite_{
			Rewrite: &storage2pb.Transaction_Rewrite{
				Groups: groups,
			},
		},
	}

	if err := storage2.CommitTransaction(ctx, d.basePath, d.handler, txn); err != nil {
		return err
	}

	// Update dataset state
	latest, err := d.handler.ResolveLatestVersion(ctx, d.basePath)
	if err != nil {
		return err
	}
	manifest, err := storage2.LoadManifest(ctx, d.basePath, d.handler, latest)
	if err != nil {
		return err
	}
	d.currentManifest = manifest
	d.version = latest

	return nil
}

// Helper function to get default max bytes
func getDefaultMaxBytes() uint64 {
	return 2 * 1024 * 1024 * 1024 // 2GB default
}

// DistributedCompact performs distributed compaction with parallel workers.
func (d *datasetImpl) DistributedCompact(ctx context.Context, opts DistributedCompactionOptions) (*storage2.CompactionStats, error) {
	if d.closed {
		return nil, fmt.Errorf("dataset is closed")
	}

	if len(d.currentManifest.Fragments) <= 1 {
		// Nothing to compact
		return &storage2.CompactionStats{}, nil
	}

	// Convert SDK options to storage2 options
	plannerCfg := storage2.DefaultCompactionPlannerConfig()
	if opts.TargetTaskSize > 0 {
		plannerCfg.TargetTaskSize = opts.TargetTaskSize
	}
	if opts.MaxTaskSize > 0 {
		plannerCfg.MaxTaskSize = opts.MaxTaskSize
	}
	if opts.MaxFragmentsPerTask > 0 {
		plannerCfg.MaxFragmentsPerTask = opts.MaxFragmentsPerTask
	}
	if opts.MinFragmentsPerTask > 0 {
		plannerCfg.MinFragmentsPerTask = opts.MinFragmentsPerTask
	}
	if opts.Strategy != "" {
		plannerCfg.Strategy = storage2.CompactionStrategy(opts.Strategy)
	}
	plannerCfg.IncludeDeletions = opts.IncludeDeletedRows

	coordCfg := storage2.DefaultCompactionCoordinatorConfig()
	if opts.MaxConcurrency > 0 {
		coordCfg.MaxConcurrency = opts.MaxConcurrency
	}
	coordCfg.RetryOnConflict = opts.RetryOnConflict
	if opts.MaxRetries > 0 {
		coordCfg.MaxRetries = opts.MaxRetries
	}
	coordCfg.PreserveRowIds = opts.PreserveRowIds
	coordCfg.IncludeDeletedRows = opts.IncludeDeletedRows

	store := storage2.NewLocalObjectStoreExt(d.basePath, nil)
	stats, err := storage2.DistributedCompaction(
		ctx,
		d.basePath,
		d.handler,
		store,
		d.currentManifest,
		storage2.DistributedCompactionOptions{
			PlannerConfig:     plannerCfg,
			CoordinatorConfig: coordCfg,
		},
	)
	if err != nil {
		return stats, err
	}

	// Refresh state after compaction
	latest, err := d.handler.ResolveLatestVersion(ctx, d.basePath)
	if err != nil {
		return stats, err
	}
	manifest, err := storage2.LoadManifest(ctx, d.basePath, d.handler, latest)
	if err != nil {
		return stats, err
	}
	d.currentManifest = manifest
	d.version = latest

	return stats, nil
}

func (d *datasetImpl) Append(ctx context.Context, fragments []*DataFragment) error {
	readVersion := d.version
	uuid := generateUUID()
	txn := storage2.NewTransactionAppend(readVersion, uuid, fragments)
	if err := storage2.CommitTransaction(ctx, d.basePath, d.handler, txn); err != nil {
		return err
	}
	latest, err := d.handler.ResolveLatestVersion(ctx, d.basePath)
	if err != nil {
		return err
	}
	manifest, err := storage2.LoadManifest(ctx, d.basePath, d.handler, latest)
	if err != nil {
		return err
	}
	d.currentManifest = manifest
	d.version = latest
	return nil
}

func (d *datasetImpl) Delete(ctx context.Context, predicate string) error {
	readVersion := d.version
	uuid := generateUUID()
	var deletedIds []uint64
	for _, frag := range d.currentManifest.Fragments {
		deletedIds = append(deletedIds, frag.Id)
	}
	txn := storage2.NewTransactionDelete(readVersion, uuid, nil, deletedIds, predicate)
	if err := storage2.CommitTransaction(ctx, d.basePath, d.handler, txn); err != nil {
		return err
	}
	latest, err := d.handler.ResolveLatestVersion(ctx, d.basePath)
	if err != nil {
		return err
	}
	manifest, err := storage2.LoadManifest(ctx, d.basePath, d.handler, latest)
	if err != nil {
		return err
	}
	d.currentManifest = manifest
	d.version = latest
	return nil
}

func (d *datasetImpl) DropColumns(ctx context.Context, columnNames []string) error {
	if d.closed {
		return fmt.Errorf("dataset is closed")
	}

	if len(columnNames) == 0 {
		return fmt.Errorf("no columns specified to drop")
	}

	// Build a set of column names to drop
	dropSet := make(map[string]bool)
	for _, name := range columnNames {
		dropSet[name] = true
	}

	// Validate that all columns exist
	for name := range dropSet {
		found := false
		for _, field := range d.currentManifest.Fields {
			if field.Name == name {
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("column %q not found in schema", name)
		}
	}

	// Build the projected schema (excluding dropped columns and their children)
	droppedIDs := make(map[int32]bool)
	for _, field := range d.currentManifest.Fields {
		if dropSet[field.Name] {
			droppedIDs[field.Id] = true
		}
	}

	// Recursively find all children of dropped fields
	changed := true
	for changed {
		changed = false
		for _, field := range d.currentManifest.Fields {
			if !droppedIDs[field.Id] && droppedIDs[field.ParentId] {
				droppedIDs[field.Id] = true
				changed = true
			}
		}
	}

	var projectedSchema []*storage2pb.Field
	for _, field := range d.currentManifest.Fields {
		if !droppedIDs[field.Id] {
			projectedSchema = append(projectedSchema, field)
		}
	}

	if len(projectedSchema) == 0 {
		return fmt.Errorf("cannot drop all columns")
	}

	readVersion := d.version
	uuid := generateUUID()
	txn := storage2.NewTransactionProject(readVersion, uuid, projectedSchema)
	if err := storage2.CommitTransaction(ctx, d.basePath, d.handler, txn); err != nil {
		return err
	}

	return d.refreshState(ctx)
}

func (d *datasetImpl) AlterColumns(ctx context.Context, alterations []ColumnAlteration) error {
	if d.closed {
		return fmt.Errorf("dataset is closed")
	}

	if len(alterations) == 0 {
		return fmt.Errorf("no alterations specified")
	}

	// Build alteration map by path
	alterMap := make(map[string]ColumnAlteration)
	for _, alt := range alterations {
		alterMap[alt.Path] = alt
	}

	// Validate that all target columns exist
	for path := range alterMap {
		found := false
		for _, field := range d.currentManifest.Fields {
			if field.Name == path {
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("column %q not found in schema", path)
		}
	}

	// Build the new schema with alterations applied
	var newSchema []*storage2pb.Field
	for _, field := range d.currentManifest.Fields {
		newField := &storage2pb.Field{
			Name:        field.Name,
			Type:        field.Type,
			Id:          field.Id,
			ParentId:    field.ParentId,
			LogicalType: field.LogicalType,
			Nullable:    field.Nullable,
			Metadata:    field.Metadata,
		}

		if alt, ok := alterMap[field.Name]; ok {
			if alt.NewName != "" {
				newField.Name = alt.NewName
			}
			if alt.NewNullable != nil {
				newField.Nullable = *alt.NewNullable
			}
			if alt.NewDataType != "" {
				newField.LogicalType = alt.NewDataType
			}
		}

		newSchema = append(newSchema, newField)
	}

	// Use a Merge transaction to update the schema
	readVersion := d.version
	uuid := generateUUID()
	txn := storage2.NewTransactionMerge(readVersion, uuid, nil, newSchema, nil)
	if err := storage2.CommitTransaction(ctx, d.basePath, d.handler, txn); err != nil {
		return err
	}

	return d.refreshState(ctx)
}

func (d *datasetImpl) AddColumns(ctx context.Context, additions []ColumnAddition) error {
	if d.closed {
		return fmt.Errorf("dataset is closed")
	}

	if len(additions) == 0 {
		return fmt.Errorf("no columns specified to add")
	}

	// Validate that no duplicate names exist
	existingNames := make(map[string]bool)
	for _, field := range d.currentManifest.Fields {
		existingNames[field.Name] = true
	}
	for _, addition := range additions {
		if existingNames[addition.Field.Name] {
			return fmt.Errorf("column %q already exists in schema", addition.Field.Name)
		}
	}

	// Build the new schema by combining existing and new fields
	var maxID int32
	for _, field := range d.currentManifest.Fields {
		if field.Id > maxID {
			maxID = field.Id
		}
	}

	newSchema := make([]*storage2pb.Field, len(d.currentManifest.Fields))
	copy(newSchema, d.currentManifest.Fields)

	for _, addition := range additions {
		newField := &storage2pb.Field{
			Name:        addition.Field.Name,
			Type:        addition.Field.Type,
			Id:          maxID + 1,
			ParentId:    addition.Field.ParentId,
			LogicalType: addition.Field.LogicalType,
			Nullable:    addition.Field.Nullable,
			Metadata:    addition.Field.Metadata,
		}
		maxID++
		newSchema = append(newSchema, newField)
	}

	readVersion := d.version
	uuid := generateUUID()
	txn := storage2.NewTransactionMerge(readVersion, uuid, nil, newSchema, nil)
	if err := storage2.CommitTransaction(ctx, d.basePath, d.handler, txn); err != nil {
		return err
	}

	return d.refreshState(ctx)
}

func (d *datasetImpl) DropPath(ctx context.Context, path string) error {
	if d.closed {
		return fmt.Errorf("dataset is closed")
	}

	if path == "" {
		return fmt.Errorf("path cannot be empty")
	}

	// Parse the path and find the target field
	// Path format: "parent.child.grandchild"
	parts := splitPath(path)
	if len(parts) == 0 {
		return fmt.Errorf("invalid path: %q", path)
	}

	// Find the target field by walking the path
	targetFieldID := int32(-1)
	currentParentID := int32(-1) // top-level

	for _, part := range parts {
		found := false
		for _, field := range d.currentManifest.Fields {
			if field.Name == part && field.ParentId == currentParentID {
				targetFieldID = field.Id
				currentParentID = field.Id
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("path %q not found: field %q not found under parent %d", path, part, currentParentID)
		}
	}

	// Collect the target field and all its descendants
	droppedIDs := map[int32]bool{targetFieldID: true}
	changed := true
	for changed {
		changed = false
		for _, field := range d.currentManifest.Fields {
			if !droppedIDs[field.Id] && droppedIDs[field.ParentId] {
				droppedIDs[field.Id] = true
				changed = true
			}
		}
	}

	// Build projected schema excluding the dropped path
	var projectedSchema []*storage2pb.Field
	for _, field := range d.currentManifest.Fields {
		if !droppedIDs[field.Id] {
			projectedSchema = append(projectedSchema, field)
		}
	}

	if len(projectedSchema) == 0 {
		return fmt.Errorf("cannot drop all fields")
	}

	readVersion := d.version
	uuid := generateUUID()
	txn := storage2.NewTransactionProject(readVersion, uuid, projectedSchema)
	if err := storage2.CommitTransaction(ctx, d.basePath, d.handler, txn); err != nil {
		return err
	}

	return d.refreshState(ctx)
}

func (d *datasetImpl) ShallowClone(ctx context.Context, targetPath string) (Dataset, error) {
	if d.closed {
		return nil, fmt.Errorf("dataset is closed")
	}

	// Create the target dataset first
	targetDS, err := CreateDataset(ctx, targetPath).WithCommitHandler(d.handler).Build()
	if err != nil {
		return nil, fmt.Errorf("failed to create target dataset: %w", err)
	}

	targetImpl := targetDS.(*datasetImpl)

	// Load source manifest to copy schema, fragments, and metadata
	srcManifest := d.currentManifest

	// Build the cloned manifest by copying the source manifest's structure
	// and adding a base path that references the source dataset for data files
	var nextBaseID uint32
	for _, bp := range srcManifest.BasePaths {
		if bp.Id >= nextBaseID {
			nextBaseID = bp.Id + 1
		}
	}

	newBasePath := &storage2pb.BasePath{
		Id:            nextBaseID,
		Path:          d.basePath,
		IsDatasetRoot: true,
	}

	// Clone fragments and update their file references to point to the source base path
	clonedFragments := make([]*DataFragment, len(srcManifest.Fragments))
	for i, frag := range srcManifest.Fragments {
		clonedFrag := NewDataFragmentWithRows(frag.Id, frag.PhysicalRows, nil)
		clonedFrag.DeletionFile = frag.DeletionFile
		var clonedFiles []*DataFile
		for _, file := range frag.Files {
			cf := NewDataFile(file.Path, file.Fields, file.FileMajorVersion, file.FileMinorVersion)
			cf.FileSizeBytes = file.FileSizeBytes
			baseID := nextBaseID
			cf.BaseId = &baseID
			clonedFiles = append(clonedFiles, cf)
		}
		clonedFrag.Files = clonedFiles
		clonedFragments[i] = clonedFrag
	}

	// Build target manifest with source schema and cloned fragments
	targetManifest := storage2.NewManifest(1)
	targetManifest.Fields = srcManifest.Fields
	if srcManifest.SchemaMetadata != nil {
		targetManifest.SchemaMetadata = make(map[string][]byte)
		for k, v := range srcManifest.SchemaMetadata {
			targetManifest.SchemaMetadata[k] = append([]byte(nil), v...)
		}
	}
	targetManifest.Fragments = clonedFragments
	targetManifest.BasePaths = append(srcManifest.BasePaths, newBasePath)
	if len(clonedFragments) > 0 {
		var maxID uint32
		for _, f := range clonedFragments {
			if uint32(f.Id) > maxID {
				maxID = uint32(f.Id)
			}
		}
		maxIDPtr := maxID
		targetManifest.MaxFragmentId = &maxIDPtr
	}

	// Commit the cloned manifest
	if err := d.handler.Commit(ctx, targetPath, 1, targetManifest); err != nil {
		targetDS.Close()
		return nil, fmt.Errorf("failed to commit cloned manifest: %w", err)
	}

	// Refresh target dataset state
	targetImpl.currentManifest = targetManifest
	targetImpl.version = 1

	return targetDS, nil
}

// refreshState reloads the manifest from storage after a transaction.
func (d *datasetImpl) refreshState(ctx context.Context) error {
	latest, err := d.handler.ResolveLatestVersion(ctx, d.basePath)
	if err != nil {
		return err
	}
	manifest, err := storage2.LoadManifest(ctx, d.basePath, d.handler, latest)
	if err != nil {
		return err
	}
	d.currentManifest = manifest
	d.version = latest
	return nil
}

func (d *datasetImpl) Update(ctx context.Context, predicate string, updates map[string]interface{}) (*storage2.UpdateResult, error) {
	if d.closed {
		return nil, fmt.Errorf("dataset is closed")
	}
	if len(updates) == 0 {
		return &storage2.UpdateResult{RowsUpdated: 0}, nil
	}

	result, err := storage2.Update(ctx, d.basePath, d.handler, d.version, predicate, updates, nil)
	if err != nil {
		return nil, err
	}

	if result.RowsUpdated == 0 {
		return result, nil
	}

	// Build and commit Update transaction
	readVersion := d.version
	uuid := generateUUID()

	var removedIDs []uint64
	for _, f := range result.OldFragments {
		removedIDs = append(removedIDs, f.Id)
	}

	var fieldsModified []uint32
	for colName := range updates {
		for i, field := range d.currentManifest.Fields {
			if field.Name == colName {
				fieldsModified = append(fieldsModified, uint32(i))
				break
			}
		}
	}

	txn := storage2.NewTransactionUpdate(
		readVersion, uuid,
		removedIDs,
		nil,
		result.NewFragments,
		fieldsModified,
		storage2.UpdateModeRewriteRows,
	)
	if err := storage2.CommitTransaction(ctx, d.basePath, d.handler, txn); err != nil {
		return nil, err
	}

	if err := d.refreshState(ctx); err != nil {
		return nil, err
	}
	return result, nil
}

func (d *datasetImpl) Merge(ctx context.Context, fragments []*DataFragment, newSchema []*storage2pb.Field) error {
	if d.closed {
		return fmt.Errorf("dataset is closed")
	}

	readVersion := d.version
	uuid := generateUUID()
	txn := storage2.NewTransactionMerge(readVersion, uuid, fragments, newSchema, nil)
	if err := storage2.CommitTransaction(ctx, d.basePath, d.handler, txn); err != nil {
		return err
	}
	return d.refreshState(ctx)
}

func (d *datasetImpl) Restore(ctx context.Context, version uint64) error {
	if d.closed {
		return fmt.Errorf("dataset is closed")
	}

	_, err := storage2.Restore(ctx, d.basePath, d.handler, version)
	if err != nil {
		return err
	}
	return d.refreshState(ctx)
}

func (d *datasetImpl) CheckoutVersion(ctx context.Context, version uint64) error {
	if d.closed {
		return fmt.Errorf("dataset is closed")
	}

	manifest, err := storage2.CheckoutVersion(ctx, d.basePath, d.handler, version)
	if err != nil {
		return err
	}
	d.currentManifest = manifest
	d.version = version
	return nil
}

func (d *datasetImpl) initRefs() *storage2.Refs {
	store := storage2.NewLocalObjectStore(d.basePath)
	return storage2.NewRefs(d.basePath, d.handler, store)
}

func (d *datasetImpl) CreateTag(ctx context.Context, tag string, version uint64) error {
	if d.closed {
		return fmt.Errorf("dataset is closed")
	}
	return d.initRefs().Tags().Create(ctx, tag, version)
}

func (d *datasetImpl) DeleteTag(ctx context.Context, tag string) error {
	if d.closed {
		return fmt.Errorf("dataset is closed")
	}
	return d.initRefs().Tags().Delete(ctx, tag)
}

func (d *datasetImpl) ListTags(ctx context.Context) (map[string]uint64, error) {
	if d.closed {
		return nil, fmt.Errorf("dataset is closed")
	}
	tags, err := d.initRefs().Tags().List(ctx)
	if err != nil {
		return nil, err
	}
	result := make(map[string]uint64, len(tags))
	for name, contents := range tags {
		result[name] = contents.Version
	}
	return result, nil
}

// splitPath splits a dot-separated field path into parts.
func splitPath(path string) []string {
	var parts []string
	current := ""
	for _, ch := range path {
		if ch == '.' {
			if current != "" {
				parts = append(parts, current)
				current = ""
			}
		} else {
			current += string(ch)
		}
	}
	if current != "" {
		parts = append(parts, current)
	}
	return parts
}

func (d *datasetImpl) Overwrite(ctx context.Context, fragments []*DataFragment) error {
	readVersion := d.version
	uuid := generateUUID()
	txn := storage2.NewTransactionOverwrite(readVersion, uuid, fragments, nil, nil)
	if err := storage2.CommitTransaction(ctx, d.basePath, d.handler, txn); err != nil {
		return err
	}
	latest, err := d.handler.ResolveLatestVersion(ctx, d.basePath)
	if err != nil {
		return err
	}
	manifest, err := storage2.LoadManifest(ctx, d.basePath, d.handler, latest)
	if err != nil {
		return err
	}
	d.currentManifest = manifest
	d.version = latest
	return nil
}

func (d *datasetImpl) Take(ctx context.Context, indices []uint64) (*chunk.Chunk, error) {
	return storage2.TakeRows(ctx, d.basePath, d.handler, d.version, indices)
}

func (d *datasetImpl) TakeProjected(ctx context.Context, indices []uint64, columns []int) (*chunk.Chunk, error) {
	return storage2.TakeRowsProjected(ctx, d.basePath, d.handler, d.version, indices, columns)
}

// Detached Transaction methods

func (d *datasetImpl) CreateDetachedAppend(ctx context.Context, fragments []*DataFragment, timeout *time.Duration) (string, error) {
	if d.closed {
		return "", fmt.Errorf("dataset is closed")
	}
	readVersion := d.version
	uuid := generateUUID()
	txn := storage2.NewTransactionAppend(readVersion, uuid, fragments)

	opts := &storage2.DetachedTransactionOptions{}
	if timeout != nil {
		opts.Timeout = *timeout
	}

	return storage2.CreateDetachedTransaction(ctx, d.basePath, d.handler, txn, opts)
}

func (d *datasetImpl) CreateDetachedDelete(ctx context.Context, predicate string, timeout *time.Duration) (string, error) {
	if d.closed {
		return "", fmt.Errorf("dataset is closed")
	}
	readVersion := d.version
	uuid := generateUUID()
	var deletedIds []uint64
	for _, frag := range d.currentManifest.Fragments {
		deletedIds = append(deletedIds, frag.Id)
	}
	txn := storage2.NewTransactionDelete(readVersion, uuid, nil, deletedIds, predicate)

	opts := &storage2.DetachedTransactionOptions{}
	if timeout != nil {
		opts.Timeout = *timeout
	}

	return storage2.CreateDetachedTransaction(ctx, d.basePath, d.handler, txn, opts)
}

func (d *datasetImpl) CreateDetachedOverwrite(ctx context.Context, fragments []*DataFragment, timeout *time.Duration) (string, error) {
	if d.closed {
		return "", fmt.Errorf("dataset is closed")
	}
	readVersion := d.version
	uuid := generateUUID()
	txn := storage2.NewTransactionOverwrite(readVersion, uuid, fragments, nil, nil)

	opts := &storage2.DetachedTransactionOptions{}
	if timeout != nil {
		opts.Timeout = *timeout
	}

	return storage2.CreateDetachedTransaction(ctx, d.basePath, d.handler, txn, opts)
}

func (d *datasetImpl) CommitDetached(ctx context.Context, txnID string) (uint64, error) {
	if d.closed {
		return 0, fmt.Errorf("dataset is closed")
	}
	version, err := storage2.CommitDetachedTransaction(ctx, d.basePath, d.handler, txnID)
	if err != nil {
		return version, err
	}

	// Refresh dataset state
	if err := d.refreshState(ctx); err != nil {
		return version, err
	}

	return version, nil
}

func (d *datasetImpl) GetDetachedStatus(ctx context.Context, txnID string) (*storage2.DetachedTransactionState, error) {
	return storage2.GetDetachedTransactionStatus(ctx, d.basePath, txnID)
}

func (d *datasetImpl) ListDetached(ctx context.Context, status storage2.DetachedTransactionStatus) ([]*storage2.DetachedTransactionState, error) {
	return storage2.ListDetachedTransactions(ctx, d.basePath, status)
}

func (d *datasetImpl) CleanupExpiredDetached(ctx context.Context) (int, error) {
	return storage2.CleanupExpiredDetachedTransactions(ctx, d.basePath)
}

func (d *datasetImpl) DeleteDetached(txnID string) error {
	return storage2.DeleteDetachedTransaction(d.basePath, txnID)
}

// KNN Vector Search methods

func (d *datasetImpl) initKNNManager() {
	if d.knnManager == nil {
		d.knnManager = storage2.NewKNNIndexManager(d.basePath, d.handler)
		d.indexPersist = storage2.NewIndexPersistence(d.basePath, d.handler)
	}
}

func (d *datasetImpl) CreateVectorIndex(ctx context.Context, config storage2.VectorSearchIndexConfig) error {
	if d.closed {
		return fmt.Errorf("dataset is closed")
	}
	d.initKNNManager()
	_, err := d.knnManager.CreateIndex(ctx, config)
	return err
}

func (d *datasetImpl) CreateVectorIndexSimple(ctx context.Context, name string, columnIdx int, dimension int, metric storage2.MetricType, indexType string) error {
	config := storage2.VectorSearchIndexConfig{
		Name:      name,
		ColumnIdx: columnIdx,
		Dimension: dimension,
		Metric:    metric,
		IndexType: indexType,
	}
	return d.CreateVectorIndex(ctx, config)
}

func (d *datasetImpl) DropVectorIndex(ctx context.Context, name string) error {
	if d.closed {
		return fmt.Errorf("dataset is closed")
	}
	d.initKNNManager()
	return d.knnManager.DropIndex(name)
}

func (d *datasetImpl) ListVectorIndexes() []string {
	if d.knnManager == nil {
		return nil
	}
	return d.knnManager.ListIndexes()
}

func (d *datasetImpl) SearchNearest(ctx context.Context, indexName string, queryVector []float32, k int) (*storage2.SearchResults, error) {
	if d.closed {
		return nil, fmt.Errorf("dataset is closed")
	}
	d.initKNNManager()
	return d.knnManager.Search(ctx, indexName, queryVector, k)
}

func (d *datasetImpl) BuildVectorIndex(ctx context.Context, indexName string, vectors map[uint64][]float32) error {
	if d.closed {
		return fmt.Errorf("dataset is closed")
	}
	d.initKNNManager()
	return d.knnManager.BuildIndex(ctx, indexName, vectors)
}

func (d *datasetImpl) SaveVectorIndex(ctx context.Context, name string) error {
	if d.closed {
		return fmt.Errorf("dataset is closed")
	}
	d.initKNNManager()
	idx, ok := d.knnManager.GetIndex(name)
	if !ok {
		return fmt.Errorf("index %q not found", name)
	}
	return d.indexPersist.SaveIndex(ctx, name, idx)
}

func (d *datasetImpl) LoadVectorIndex(ctx context.Context, name string, config storage2.VectorSearchIndexConfig) error {
	if d.closed {
		return fmt.Errorf("dataset is closed")
	}
	d.initKNNManager()
	idx, err := d.indexPersist.LoadIndex(ctx, name, config)
	if err != nil {
		return err
	}
	// Add loaded index to manager
	return d.knnManager.AddIndex(name, idx)
}

func (d *datasetImpl) GetVectorIndex(name string) (storage2.VectorSearchIndex, bool) {
	if d.knnManager == nil {
		return nil, false
	}
	return d.knnManager.GetIndex(name)
}

// datasetBuilder is used by OpenDataset and CreateDataset.
type datasetBuilder struct {
	uri      string
	basePath string
	version  *uint64
	handler  storage2.CommitHandler
	isCreate bool
	readOpts *ReadOptions
	store    storage2.ObjectStoreExt
}

// parseURIToPath parses a URI and returns the local path for backward compatibility
// or the full URI for cloud storage. It also returns a CommitHandler appropriate
// for the storage scheme.
func parseURIToPath(ctx context.Context, uri string) (string, storage2.CommitHandler, error) {
	parsed, err := storage2.ParseURI(uri)
	if err != nil {
		return "", nil, err
	}

	// For local filesystem, return the path directly
	if parsed.IsLocal() {
		return parsed.Path, storage2.NewLocalRenameCommitHandler(), nil
	}

	// For cloud storage, use the store factory
	handler, err := defaultStoreFactory.GetCommitHandler(ctx, uri)
	if err != nil {
		return "", nil, fmt.Errorf("failed to get commit handler for URI %q: %w", uri, err)
	}

	// For cloud storage, basePath is the URI path within the bucket
	return parsed.Path, handler, nil
}

// OpenDataset opens an existing dataset at basePath. Use WithVersion and WithCommitHandler, then Build().
// The basePath can be:
//   - A local path: "/path/to/dataset" or "file:///path/to/dataset"
//   - An S3 URI: "s3://bucket/path/to/dataset"
//   - A memory URI: "mem://dataset-name" (for testing)
func OpenDataset(ctx context.Context, basePath string) *datasetBuilder {
	// Try to parse as URI and get appropriate handler
	path, handler, err := parseURIToPath(ctx, basePath)
	if err != nil {
		// Fall back to local filesystem for backward compatibility
		return &datasetBuilder{
			uri:      basePath,
			basePath: basePath,
			handler:  storage2.NewLocalRenameCommitHandler(),
			isCreate: false,
		}
	}

	return &datasetBuilder{
		uri:      basePath,
		basePath: path,
		handler:  handler,
		isCreate: false,
	}
}

// CreateDataset creates a new empty dataset at basePath. Use WithCommitHandler, then Build().
// The basePath can be:
//   - A local path: "/path/to/dataset" or "file:///path/to/dataset"
//   - An S3 URI: "s3://bucket/path/to/dataset"
//   - A memory URI: "mem://dataset-name" (for testing)
func CreateDataset(ctx context.Context, basePath string) *datasetBuilder {
	// Try to parse as URI and get appropriate handler
	path, handler, err := parseURIToPath(ctx, basePath)
	if err != nil {
		// Fall back to local filesystem for backward compatibility
		return &datasetBuilder{
			uri:      basePath,
			basePath: basePath,
			handler:  storage2.NewLocalRenameCommitHandler(),
			isCreate: true,
		}
	}

	return &datasetBuilder{
		uri:      basePath,
		basePath: path,
		handler:  handler,
		isCreate: true,
	}
}

// WithVersion sets the manifest version to open (only for OpenDataset). Nil means latest.
func (b *datasetBuilder) WithVersion(v uint64) *datasetBuilder {
	b.version = &v
	return b
}

// WithCommitHandler sets the CommitHandler (e.g. for custom storage).
func (b *datasetBuilder) WithCommitHandler(handler storage2.CommitHandler) *datasetBuilder {
	b.handler = handler
	return b
}

// OpenDatasetWithTag opens a dataset at the version referenced by the given tag.
// It uses the default CommitHandler and ResolveTagVersion under the hood.
func OpenDatasetWithTag(ctx context.Context, basePath, tag string) (Dataset, error) {
	handler := storage2.NewLocalRenameCommitHandler()
	version, ok, err := storage2.ResolveTagVersion(ctx, basePath, handler, tag)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("tag %q not found", tag)
	}
	return OpenDataset(ctx, basePath).WithCommitHandler(handler).WithVersion(version).Build()
}

// Build opens or creates the dataset. For OpenDataset it loads the manifest; for CreateDataset it commits version 0.
func (b *datasetBuilder) Build() (Dataset, error) {
	if b.isCreate {
		return b.buildCreate()
	}
	return b.buildOpen()
}

func (b *datasetBuilder) buildOpen() (Dataset, error) {
	var version uint64
	if b.version != nil {
		version = *b.version
	} else {
		var err error
		version, err = b.handler.ResolveLatestVersion(context.Background(), b.basePath)
		if err != nil {
			return nil, err
		}
	}
	manifest, err := storage2.LoadManifest(context.Background(), b.basePath, b.handler, version)
	if err != nil {
		return nil, err
	}
	return &datasetImpl{
		basePath:        b.basePath,
		handler:         b.handler,
		currentManifest: manifest,
		version:         version,
	}, nil
}

func (b *datasetBuilder) buildCreate() (Dataset, error) {
	m0 := storage2.NewManifest(0)
	m0.Fragments = []*storage2.DataFragment{}
	m0.NextRowId = 1
	if err := b.handler.Commit(context.Background(), b.basePath, 0, m0); err != nil {
		return nil, err
	}
	return &datasetImpl{
		basePath:        b.basePath,
		handler:         b.handler,
		currentManifest: m0,
		version:         0,
	}, nil
}

func generateUUID() string {
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return fmt.Sprintf("fallback-%d", buf[0])
	}
	buf[6] = (buf[6] & 0x0f) | 0x40
	buf[8] = (buf[8] & 0x3f) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%12x", buf[0:4], buf[4:6], buf[6:8], buf[8:10], buf[10:16])
}
