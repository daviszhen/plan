// Copyright 2024 PlanDB Authors
// Licensed under the Apache License, Version 2.0 (the "License");

package storage2

import (
	"context"
	"sync"
	"time"
)

// PreFilter is the interface for pre-filtering rows before vector search.
// It allows filtering out deleted rows and rows that don't match scalar predicates.
type PreFilter interface {
	// WaitForReady waits for the filter to be ready (for async loading scenarios)
	WaitForReady(ctx context.Context) error

	// IsEmpty returns true if the filter doesn't filter any rows
	IsEmpty() bool

	// ShouldInclude returns true if the row should be included in search
	ShouldInclude(rowID uint64) bool

	// FilterRowIDs filters a batch of row IDs and returns only those that should be included
	FilterRowIDs(rowIDs []uint64) []uint64

	// Cardinality returns the number of valid (non-filtered) rows
	Cardinality() uint64
}

// NoFilter is a filter that doesn't filter any rows
type NoFilter struct{}

var _ PreFilter = (*NoFilter)(nil)

// WaitForReady implements PreFilter
func (f *NoFilter) WaitForReady(ctx context.Context) error { return nil }

// IsEmpty implements PreFilter
func (f *NoFilter) IsEmpty() bool { return true }

// ShouldInclude implements PreFilter
func (f *NoFilter) ShouldInclude(rowID uint64) bool { return true }

// FilterRowIDs implements PreFilter
func (f *NoFilter) FilterRowIDs(rowIDs []uint64) []uint64 { return rowIDs }

// Cardinality implements PreFilter
func (f *NoFilter) Cardinality() uint64 { return 0 }

// BitmapPreFilter filters rows using a bitmap of valid row IDs
type BitmapPreFilter struct {
	// validRows contains all row IDs that should be included
	validRows map[uint64]struct{}
	// totalRows is the total number of rows before filtering
	totalRows uint64

	mu    sync.RWMutex
	ready bool
}

var _ PreFilter = (*BitmapPreFilter)(nil)

// NewBitmapPreFilter creates a new BitmapPreFilter
func NewBitmapPreFilter(totalRows uint64) *BitmapPreFilter {
	return &BitmapPreFilter{
		validRows: make(map[uint64]struct{}),
		totalRows: totalRows,
		ready:     true,
	}
}

// NewBitmapPreFilterFromValid creates a BitmapPreFilter from a set of valid row IDs
func NewBitmapPreFilterFromValid(validRowIDs []uint64, totalRows uint64) *BitmapPreFilter {
	f := &BitmapPreFilter{
		validRows: make(map[uint64]struct{}, len(validRowIDs)),
		totalRows: totalRows,
		ready:     true,
	}
	for _, id := range validRowIDs {
		f.validRows[id] = struct{}{}
	}
	return f
}

// AddValidRow marks a row as valid (should be included in search)
func (f *BitmapPreFilter) AddValidRow(rowID uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.validRows[rowID] = struct{}{}
}

// RemoveRow marks a row as invalid (should be excluded from search)
func (f *BitmapPreFilter) RemoveRow(rowID uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.validRows, rowID)
}

// SetReady marks the filter as ready
func (f *BitmapPreFilter) SetReady() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ready = true
}

// WaitForReady implements PreFilter
func (f *BitmapPreFilter) WaitForReady(ctx context.Context) error {
	// Poll until ready or context cancelled
	for {
		f.mu.RLock()
		ready := f.ready
		f.mu.RUnlock()
		if ready {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			// Continue polling
		}
	}
}

// IsEmpty implements PreFilter
func (f *BitmapPreFilter) IsEmpty() bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return len(f.validRows) == int(f.totalRows)
}

// ShouldInclude implements PreFilter
func (f *BitmapPreFilter) ShouldInclude(rowID uint64) bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	_, ok := f.validRows[rowID]
	return ok
}

// FilterRowIDs implements PreFilter
func (f *BitmapPreFilter) FilterRowIDs(rowIDs []uint64) []uint64 {
	f.mu.RLock()
	defer f.mu.RUnlock()

	result := make([]uint64, 0, len(rowIDs))
	for _, id := range rowIDs {
		if _, ok := f.validRows[id]; ok {
			result = append(result, id)
		}
	}
	return result
}

// Cardinality implements PreFilter
func (f *BitmapPreFilter) Cardinality() uint64 {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return uint64(len(f.validRows))
}

// AsyncPreFilter wraps another filter and loads it asynchronously
type AsyncPreFilter struct {
	inner   PreFilter
	loadErr error

	mu      sync.Mutex
	cond    *sync.Cond
	loading bool
	ready   bool
}

// NewAsyncPreFilter creates a new AsyncPreFilter that will load asynchronously
func NewAsyncPreFilter(loadFunc func() (PreFilter, error)) *AsyncPreFilter {
	f := &AsyncPreFilter{
		loading: true,
	}
	f.cond = sync.NewCond(&f.mu)

	// Start loading in background
	go func() {
		inner, err := loadFunc()

		f.mu.Lock()
		f.inner = inner
		f.loadErr = err
		f.loading = false
		f.ready = true
		f.cond.Broadcast()
		f.mu.Unlock()
	}()

	return f
}

// WaitForReady implements PreFilter
func (f *AsyncPreFilter) WaitForReady(ctx context.Context) error {
	// Poll with context support
	ticker := time.NewTicker(1 * time.Millisecond)
	defer ticker.Stop()

	for {
		f.mu.Lock()
		ready := f.ready
		err := f.loadErr
		f.mu.Unlock()

		if ready {
			return err
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			// Continue polling
		}
	}
}

// IsEmpty implements PreFilter
func (f *AsyncPreFilter) IsEmpty() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.inner == nil {
		return true
	}
	return f.inner.IsEmpty()
}

// ShouldInclude implements PreFilter
func (f *AsyncPreFilter) ShouldInclude(rowID uint64) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.inner == nil {
		return true // Default to include if not ready
	}
	return f.inner.ShouldInclude(rowID)
}

// FilterRowIDs implements PreFilter
func (f *AsyncPreFilter) FilterRowIDs(rowIDs []uint64) []uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.inner == nil {
		return rowIDs // Default to include all if not ready
	}
	return f.inner.FilterRowIDs(rowIDs)
}

// Cardinality implements PreFilter
func (f *AsyncPreFilter) Cardinality() uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.inner == nil {
		return 0
	}
	return f.inner.Cardinality()
}

// DeletionPreFilter filters out deleted rows based on deletion bitmap
type DeletionPreFilter struct {
	deletedRows map[uint64]struct{}
	totalRows   uint64
}

var _ PreFilter = (*DeletionPreFilter)(nil)

// NewDeletionPreFilter creates a filter from a deletion bitmap
func NewDeletionPreFilter(bitmap *DeletionBitmap, totalRows uint64) *DeletionPreFilter {
	f := &DeletionPreFilter{
		deletedRows: make(map[uint64]struct{}),
		totalRows:   totalRows,
	}
	if bitmap != nil {
		for rowID := range bitmap.deleted {
			f.deletedRows[rowID] = struct{}{}
		}
	}
	return f
}

// WaitForReady implements PreFilter
func (f *DeletionPreFilter) WaitForReady(ctx context.Context) error { return nil }

// IsEmpty implements PreFilter
func (f *DeletionPreFilter) IsEmpty() bool {
	return len(f.deletedRows) == 0
}

// ShouldInclude implements PreFilter
func (f *DeletionPreFilter) ShouldInclude(rowID uint64) bool {
	_, deleted := f.deletedRows[rowID]
	return !deleted
}

// FilterRowIDs implements PreFilter
func (f *DeletionPreFilter) FilterRowIDs(rowIDs []uint64) []uint64 {
	if len(f.deletedRows) == 0 {
		return rowIDs
	}

	result := make([]uint64, 0, len(rowIDs))
	for _, id := range rowIDs {
		if _, deleted := f.deletedRows[id]; !deleted {
			result = append(result, id)
		}
	}
	return result
}

// Cardinality implements PreFilter
func (f *DeletionPreFilter) Cardinality() uint64 {
	return f.totalRows - uint64(len(f.deletedRows))
}

// CombinedPreFilter combines multiple filters with AND logic
type CombinedPreFilter struct {
	filters []PreFilter
}

var _ PreFilter = (*CombinedPreFilter)(nil)

// NewCombinedPreFilter creates a filter that combines multiple filters
func NewCombinedPreFilter(filters ...PreFilter) *CombinedPreFilter {
	return &CombinedPreFilter{filters: filters}
}

// WaitForReady implements PreFilter
func (f *CombinedPreFilter) WaitForReady(ctx context.Context) error {
	for _, filter := range f.filters {
		if err := filter.WaitForReady(ctx); err != nil {
			return err
		}
	}
	return nil
}

// IsEmpty implements PreFilter
func (f *CombinedPreFilter) IsEmpty() bool {
	for _, filter := range f.filters {
		if !filter.IsEmpty() {
			return false
		}
	}
	return true
}

// ShouldInclude implements PreFilter
func (f *CombinedPreFilter) ShouldInclude(rowID uint64) bool {
	for _, filter := range f.filters {
		if !filter.ShouldInclude(rowID) {
			return false
		}
	}
	return true
}

// FilterRowIDs implements PreFilter
func (f *CombinedPreFilter) FilterRowIDs(rowIDs []uint64) []uint64 {
	result := rowIDs
	for _, filter := range f.filters {
		result = filter.FilterRowIDs(result)
		if len(result) == 0 {
			break
		}
	}
	return result
}

// Cardinality implements PreFilter - returns minimum cardinality
func (f *CombinedPreFilter) Cardinality() uint64 {
	if len(f.filters) == 0 {
		return 0
	}
	min := f.filters[0].Cardinality()
	for _, filter := range f.filters[1:] {
		if c := filter.Cardinality(); c < min {
			min = c
		}
	}
	return min
}

// PreFilterBuilder helps build pre-filters for vector search
type PreFilterBuilder struct {
	basePath string
	store    ObjectStoreExt
	handler  CommitHandler
	manifest *Manifest
}

// NewPreFilterBuilder creates a new PreFilterBuilder
func NewPreFilterBuilder(basePath string, store ObjectStoreExt, handler CommitHandler) *PreFilterBuilder {
	return &PreFilterBuilder{
		basePath: basePath,
		store:    store,
		handler:  handler,
	}
}

// WithManifest sets the manifest to use
func (b *PreFilterBuilder) WithManifest(m *Manifest) *PreFilterBuilder {
	b.manifest = m
	return b
}

// BuildDeletionFilter builds a filter that excludes deleted rows
func (b *PreFilterBuilder) BuildDeletionFilter(ctx context.Context) (PreFilter, error) {
	if b.manifest == nil {
		version, err := b.handler.ResolveLatestVersion(ctx, b.basePath)
		if err != nil {
			return nil, err
		}
		b.manifest, err = LoadManifest(ctx, b.basePath, b.handler, version)
		if err != nil {
			return nil, err
		}
	}

	// Calculate total rows
	var totalRows uint64
	for _, frag := range b.manifest.Fragments {
		if frag != nil {
			totalRows += frag.PhysicalRows
		}
	}

	// Collect all deleted rows
	combinedBitmap := NewDeletionBitmap()
	offsets := ComputeFragmentOffsets(b.manifest)

	for i, frag := range b.manifest.Fragments {
		if frag == nil || frag.DeletionFile == nil {
			continue
		}

		bitmap, err := LoadFragmentDeletionBitmap(ctx, b.store, b.basePath, frag)
		if err != nil {
			continue // Skip on error
		}
		if bitmap == nil {
			continue
		}

		// Convert local row IDs to global row IDs
		for localRowID := range bitmap.deleted {
			globalRowID := offsets[i] + localRowID
			combinedBitmap.MarkDeleted(globalRowID)
		}
	}

	return NewDeletionPreFilter(combinedBitmap, totalRows), nil
}

// BuildAsyncDeletionFilter builds a deletion filter that loads asynchronously
func (b *PreFilterBuilder) BuildAsyncDeletionFilter(ctx context.Context) PreFilter {
	return NewAsyncPreFilter(func() (PreFilter, error) {
		return b.BuildDeletionFilter(ctx)
	})
}
