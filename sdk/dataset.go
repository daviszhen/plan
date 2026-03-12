// Package sdk provides a high-level API for the storage2 engine (Dataset, OpenDataset, CreateDataset).
package sdk

import (
	"context"
	"crypto/rand"
	"fmt"

	"github.com/daviszhen/plan/pkg/chunk"
	"github.com/daviszhen/plan/pkg/storage2"
)

// Dataset is the main interface for a versioned table (open or create).
type Dataset interface {
	Close() error
	Version() uint64
	LatestVersion() (uint64, error)
	CountRows() (uint64, error)
	Append(ctx context.Context, fragments []*DataFragment) error
	Delete(ctx context.Context, predicate string) error
	Overwrite(ctx context.Context, fragments []*DataFragment) error
	// Take returns rows at the given logical indices for the current version.
	// This is a minimal random-access API built on top of storage2.TakeRows.
	Take(ctx context.Context, indices []uint64) (*chunk.Chunk, error)
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
