// Copyright 2024 PlanDB Authors
// Licensed under the Apache License, Version 2.0 (the "License");

package storage2

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// DataStatistics holds dataset-level statistics
type DataStatistics struct {
	// TotalRows is the total number of rows in the dataset
	TotalRows uint64 `json:"total_rows"`
	// DeletedRows is the number of deleted rows
	DeletedRows uint64 `json:"deleted_rows"`
	// ActiveRows is TotalRows - DeletedRows
	ActiveRows uint64 `json:"active_rows"`
	// FieldStats contains per-field statistics
	FieldStats map[string]*FieldStatistics `json:"field_stats"`
	// ComputedAt is when these statistics were computed
	ComputedAt time.Time `json:"computed_at"`
	// Version is the manifest version these stats were computed from
	Version uint64 `json:"version"`
	// NumFragments is the number of fragments
	NumFragments int `json:"num_fragments"`
	// TotalBytesOnDisk is the total size of all data files
	TotalBytesOnDisk uint64 `json:"total_bytes_on_disk"`
}

// FieldStatistics holds statistics for a single field/column
type FieldStatistics struct {
	// Name is the field name
	Name string `json:"name"`
	// FieldIndex is the column index
	FieldIndex int `json:"field_index"`
	// BytesOnDisk is the total bytes used by this field
	BytesOnDisk uint64 `json:"bytes_on_disk"`
	// NullCount is the number of null values
	NullCount uint64 `json:"null_count"`
	// DistinctCount is the approximate number of distinct values (optional)
	DistinctCount uint64 `json:"distinct_count,omitempty"`
	// MinValue is the minimum value (for comparable types)
	MinValue interface{} `json:"min_value,omitempty"`
	// MaxValue is the maximum value (for comparable types)
	MaxValue interface{} `json:"max_value,omitempty"`
	// AvgSize is the average size per value (for variable-length types)
	AvgSize float64 `json:"avg_size,omitempty"`
}

// ComputeStatistics computes statistics for a dataset
func ComputeStatistics(ctx context.Context, basePath string, store ObjectStoreExt,
	handler CommitHandler) (*DataStatistics, error) {

	// Load current manifest
	version, err := handler.ResolveLatestVersion(ctx, basePath)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve latest version: %w", err)
	}

	manifest, err := LoadManifest(ctx, basePath, handler, version)
	if err != nil {
		return nil, fmt.Errorf("failed to load manifest: %w", err)
	}

	return ComputeStatisticsFromManifest(ctx, basePath, store, manifest)
}

// ComputeStatisticsFromManifest computes statistics from a loaded manifest
func ComputeStatisticsFromManifest(ctx context.Context, basePath string,
	store ObjectStoreExt, manifest *Manifest) (*DataStatistics, error) {

	stats := &DataStatistics{
		FieldStats:   make(map[string]*FieldStatistics),
		ComputedAt:   time.Now(),
		Version:      manifest.Version,
		NumFragments: len(manifest.Fragments),
	}

	// Initialize field statistics from schema (Fields)
	if manifest.Fields != nil {
		for i, field := range manifest.Fields {
			name := field.Name
			if name == "" {
				name = fmt.Sprintf("field_%d", i)
			}
			stats.FieldStats[name] = &FieldStatistics{
				Name:       name,
				FieldIndex: i,
			}
		}
	}

	// Accumulate statistics from fragments
	for _, frag := range manifest.Fragments {
		if frag == nil {
			continue
		}

		// Count rows (PhysicalRows is the total rows including deleted)
		stats.TotalRows += frag.PhysicalRows

		// Count deleted rows
		if frag.DeletionFile != nil {
			stats.DeletedRows += frag.DeletionFile.NumDeletedRows
		}

		// Accumulate per-field statistics from data files
		for _, df := range frag.Files {
			if df == nil {
				continue
			}

			// DataFile doesn't have FileSize, estimate from path or skip
			// For now, just count the number of fields per file
			numFields := len(df.Fields)
			if numFields > 0 {
				// Map field IDs to field names
				for _, fieldID := range df.Fields {
					if fieldID < 0 {
						continue // Tombstoned or invalid field
					}
					fieldName := fmt.Sprintf("field_%d", fieldID)
					if manifest.Fields != nil && int(fieldID) < len(manifest.Fields) {
						if manifest.Fields[fieldID].Name != "" {
							fieldName = manifest.Fields[fieldID].Name
						}
					}

					if fs, ok := stats.FieldStats[fieldName]; ok {
						// Just note that this field has data
						fs.BytesOnDisk += 1 // Placeholder
					}
				}
			}
		}
	}

	stats.ActiveRows = stats.TotalRows - stats.DeletedRows

	return stats, nil
}

// GetFieldBytesOnDisk returns the disk usage for a specific field
func GetFieldBytesOnDisk(ctx context.Context, basePath string, store ObjectStoreExt,
	handler CommitHandler, fieldName string) (uint64, error) {

	stats, err := ComputeStatistics(ctx, basePath, store, handler)
	if err != nil {
		return 0, err
	}

	if fs, ok := stats.FieldStats[fieldName]; ok {
		return fs.BytesOnDisk, nil
	}

	return 0, fmt.Errorf("field %q not found", fieldName)
}

// SaveStatistics saves statistics to a JSON file
func SaveStatistics(ctx context.Context, stats *DataStatistics, basePath string) error {
	statsPath := filepath.Join(basePath, "_statistics.json")

	data, err := json.MarshalIndent(stats, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal statistics: %w", err)
	}

	if err := os.WriteFile(statsPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write statistics: %w", err)
	}

	return nil
}

// LoadStatistics loads statistics from a JSON file
func LoadStatistics(ctx context.Context, basePath string) (*DataStatistics, error) {
	statsPath := filepath.Join(basePath, "_statistics.json")

	data, err := os.ReadFile(statsPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("statistics file not found")
		}
		return nil, fmt.Errorf("failed to read statistics: %w", err)
	}

	var stats DataStatistics
	if err := json.Unmarshal(data, &stats); err != nil {
		return nil, fmt.Errorf("failed to unmarshal statistics: %w", err)
	}

	return &stats, nil
}

// StatisticsNeedRefresh checks if statistics need to be recomputed
func StatisticsNeedRefresh(stats *DataStatistics, currentVersion uint64) bool {
	if stats == nil {
		return true
	}
	return stats.Version != currentVersion
}

// DataStatisticsBuilder provides a fluent interface for building statistics
type DataStatisticsBuilder struct {
	stats *DataStatistics
}

// NewDataStatisticsBuilder creates a new statistics builder
func NewDataStatisticsBuilder() *DataStatisticsBuilder {
	return &DataStatisticsBuilder{
		stats: &DataStatistics{
			FieldStats: make(map[string]*FieldStatistics),
			ComputedAt: time.Now(),
		},
	}
}

// WithTotalRows sets the total rows
func (b *DataStatisticsBuilder) WithTotalRows(rows uint64) *DataStatisticsBuilder {
	b.stats.TotalRows = rows
	b.stats.ActiveRows = rows - b.stats.DeletedRows
	return b
}

// WithDeletedRows sets the deleted rows count
func (b *DataStatisticsBuilder) WithDeletedRows(rows uint64) *DataStatisticsBuilder {
	b.stats.DeletedRows = rows
	b.stats.ActiveRows = b.stats.TotalRows - rows
	return b
}

// WithVersion sets the manifest version
func (b *DataStatisticsBuilder) WithVersion(version uint64) *DataStatisticsBuilder {
	b.stats.Version = version
	return b
}

// AddFieldStats adds statistics for a field
func (b *DataStatisticsBuilder) AddFieldStats(name string, fs *FieldStatistics) *DataStatisticsBuilder {
	fs.Name = name
	b.stats.FieldStats[name] = fs
	return b
}

// Build returns the constructed statistics
func (b *DataStatisticsBuilder) Build() *DataStatistics {
	return b.stats
}

// Summary returns a human-readable summary of the statistics
func (stats *DataStatistics) Summary() string {
	return fmt.Sprintf(
		"DataStatistics{Version: %d, TotalRows: %d, DeletedRows: %d, ActiveRows: %d, NumFragments: %d, TotalBytes: %d}",
		stats.Version, stats.TotalRows, stats.DeletedRows, stats.ActiveRows,
		stats.NumFragments, stats.TotalBytesOnDisk,
	)
}

// GetCompressionRatio estimates the compression ratio based on statistics
func (stats *DataStatistics) GetCompressionRatio(rawBytesPerRow uint64) float64 {
	if stats.ActiveRows == 0 || rawBytesPerRow == 0 {
		return 1.0
	}

	rawSize := float64(stats.ActiveRows * rawBytesPerRow)
	compressedSize := float64(stats.TotalBytesOnDisk)

	if compressedSize == 0 {
		return 1.0
	}

	return rawSize / compressedSize
}

// MergeStatistics merges multiple statistics (e.g., from different fragments)
func MergeStatistics(statsList []*DataStatistics) *DataStatistics {
	if len(statsList) == 0 {
		return nil
	}

	merged := &DataStatistics{
		FieldStats: make(map[string]*FieldStatistics),
		ComputedAt: time.Now(),
	}

	for _, stats := range statsList {
		if stats == nil {
			continue
		}

		merged.TotalRows += stats.TotalRows
		merged.DeletedRows += stats.DeletedRows
		merged.NumFragments += stats.NumFragments
		merged.TotalBytesOnDisk += stats.TotalBytesOnDisk

		// Use latest version
		if stats.Version > merged.Version {
			merged.Version = stats.Version
		}

		// Merge field statistics
		for name, fs := range stats.FieldStats {
			if existing, ok := merged.FieldStats[name]; ok {
				existing.BytesOnDisk += fs.BytesOnDisk
				existing.NullCount += fs.NullCount
			} else {
				merged.FieldStats[name] = &FieldStatistics{
					Name:        fs.Name,
					FieldIndex:  fs.FieldIndex,
					BytesOnDisk: fs.BytesOnDisk,
					NullCount:   fs.NullCount,
				}
			}
		}
	}

	merged.ActiveRows = merged.TotalRows - merged.DeletedRows

	return merged
}
