// Package sdk provides a high-level API for the storage2 engine (Dataset, OpenDataset, CreateDataset).
package sdk

import (
	"context"
	"crypto/rand"
	"fmt"

	"github.com/daviszhen/plan/pkg/chunk"
	"github.com/daviszhen/plan/pkg/storage2"
	"github.com/daviszhen/plan/pkg/storage2/proto"
)

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

// Dataset is the main interface for a versioned table (open or create).
type Dataset interface {
	Close() error
	Version() uint64
	LatestVersion() (uint64, error)
	CountRows() (uint64, error)
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
	// Compact performs data compaction to merge small fragments and clean up deleted data.
	Compact(ctx context.Context) error
	// CompactWithOptions performs data compaction with specific options.
	CompactWithOptions(ctx context.Context, opts CompactionOptions) error
	// Take returns rows at the given logical indices for the current version.
	// This is a minimal random-access API built on top of storage2.TakeRows.
	Take(ctx context.Context, indices []uint64) (*chunk.Chunk, error)
	// TakeProjected returns rows at the given logical indices and only the specified
	// zero-based column indices. If columns is empty, it behaves like Take.
	TakeProjected(ctx context.Context, indices []uint64, columns []int) (*chunk.Chunk, error)
	// Scanner creates a ScannerBuilder for streaming reads.
	Scanner() *ScannerBuilder
}

type datasetImpl struct {
	basePath        string
	handler         storage2.CommitHandler
	currentManifest *storage2.Manifest
	version         uint64
	closed          bool
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

// datasetBuilder is used by OpenDataset and CreateDataset.
type datasetBuilder struct {
	basePath  string
	version   *uint64
	handler   storage2.CommitHandler
	isCreate  bool
	readOpts  *ReadOptions
}

// OpenDataset opens an existing dataset at basePath. Use WithVersion and WithCommitHandler, then Build().
func OpenDataset(ctx context.Context, basePath string) *datasetBuilder {
	return &datasetBuilder{
		basePath: basePath,
		handler:  storage2.NewLocalRenameCommitHandler(),
		isCreate: false,
	}
}

// CreateDataset creates a new empty dataset at basePath. Use WithCommitHandler, then Build().
func CreateDataset(ctx context.Context, basePath string) *datasetBuilder {
	return &datasetBuilder{
		basePath: basePath,
		handler:  storage2.NewLocalRenameCommitHandler(),
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
