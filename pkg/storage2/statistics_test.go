// Copyright 2024 PlanDB Authors
// Licensed under the Apache License, Version 2.0 (the "License");

package storage2

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestDataStatisticsBuilder(t *testing.T) {
	stats := NewDataStatisticsBuilder().
		WithTotalRows(1000).
		WithDeletedRows(100).
		WithVersion(5).
		AddFieldStats("id", &FieldStatistics{
			FieldIndex:  0,
			BytesOnDisk: 4000,
			NullCount:   0,
		}).
		AddFieldStats("name", &FieldStatistics{
			FieldIndex:  1,
			BytesOnDisk: 8000,
			NullCount:   50,
		}).
		Build()

	if stats.TotalRows != 1000 {
		t.Errorf("expected TotalRows 1000, got %d", stats.TotalRows)
	}

	if stats.DeletedRows != 100 {
		t.Errorf("expected DeletedRows 100, got %d", stats.DeletedRows)
	}

	if stats.ActiveRows != 900 {
		t.Errorf("expected ActiveRows 900, got %d", stats.ActiveRows)
	}

	if stats.Version != 5 {
		t.Errorf("expected Version 5, got %d", stats.Version)
	}

	if len(stats.FieldStats) != 2 {
		t.Errorf("expected 2 field stats, got %d", len(stats.FieldStats))
	}

	if fs, ok := stats.FieldStats["name"]; !ok || fs.BytesOnDisk != 8000 {
		t.Error("field stats for 'name' not correct")
	}
}

func TestDataStatisticsSaveLoad(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	// Create statistics
	original := NewDataStatisticsBuilder().
		WithTotalRows(5000).
		WithDeletedRows(500).
		WithVersion(10).
		AddFieldStats("col1", &FieldStatistics{
			FieldIndex:  0,
			BytesOnDisk: 20000,
		}).
		Build()

	// Save
	if err := SaveStatistics(ctx, original, dir); err != nil {
		t.Fatalf("failed to save statistics: %v", err)
	}

	// Verify file exists
	statsPath := filepath.Join(dir, "_statistics.json")
	if _, err := os.Stat(statsPath); err != nil {
		t.Error("statistics file not created")
	}

	// Load
	loaded, err := LoadStatistics(ctx, dir)
	if err != nil {
		t.Fatalf("failed to load statistics: %v", err)
	}

	// Verify
	if loaded.TotalRows != original.TotalRows {
		t.Errorf("TotalRows mismatch: got %d, want %d", loaded.TotalRows, original.TotalRows)
	}

	if loaded.DeletedRows != original.DeletedRows {
		t.Errorf("DeletedRows mismatch: got %d, want %d", loaded.DeletedRows, original.DeletedRows)
	}

	if loaded.Version != original.Version {
		t.Errorf("Version mismatch: got %d, want %d", loaded.Version, original.Version)
	}

	if fs, ok := loaded.FieldStats["col1"]; !ok || fs.BytesOnDisk != 20000 {
		t.Error("field stats not loaded correctly")
	}
}

func TestStatisticsNeedRefresh(t *testing.T) {
	stats := &DataStatistics{Version: 5}

	// Same version - no refresh needed
	if StatisticsNeedRefresh(stats, 5) {
		t.Error("should not need refresh for same version")
	}

	// Different version - needs refresh
	if !StatisticsNeedRefresh(stats, 6) {
		t.Error("should need refresh for different version")
	}

	// Nil stats - needs refresh
	if !StatisticsNeedRefresh(nil, 5) {
		t.Error("should need refresh for nil stats")
	}
}

func TestMergeStatistics(t *testing.T) {
	stats1 := &DataStatistics{
		TotalRows:        1000,
		DeletedRows:      100,
		NumFragments:     2,
		TotalBytesOnDisk: 50000,
		Version:          5,
		FieldStats: map[string]*FieldStatistics{
			"id": {Name: "id", BytesOnDisk: 4000, NullCount: 0},
		},
	}

	stats2 := &DataStatistics{
		TotalRows:        2000,
		DeletedRows:      200,
		NumFragments:     3,
		TotalBytesOnDisk: 100000,
		Version:          7,
		FieldStats: map[string]*FieldStatistics{
			"id":   {Name: "id", BytesOnDisk: 8000, NullCount: 10},
			"name": {Name: "name", BytesOnDisk: 20000, NullCount: 50},
		},
	}

	merged := MergeStatistics([]*DataStatistics{stats1, stats2})

	if merged.TotalRows != 3000 {
		t.Errorf("expected TotalRows 3000, got %d", merged.TotalRows)
	}

	if merged.DeletedRows != 300 {
		t.Errorf("expected DeletedRows 300, got %d", merged.DeletedRows)
	}

	if merged.ActiveRows != 2700 {
		t.Errorf("expected ActiveRows 2700, got %d", merged.ActiveRows)
	}

	if merged.NumFragments != 5 {
		t.Errorf("expected NumFragments 5, got %d", merged.NumFragments)
	}

	if merged.TotalBytesOnDisk != 150000 {
		t.Errorf("expected TotalBytesOnDisk 150000, got %d", merged.TotalBytesOnDisk)
	}

	// Should use latest version
	if merged.Version != 7 {
		t.Errorf("expected Version 7, got %d", merged.Version)
	}

	// Check merged field stats
	if fs, ok := merged.FieldStats["id"]; !ok || fs.BytesOnDisk != 12000 {
		t.Errorf("expected 'id' BytesOnDisk 12000, got %d", fs.BytesOnDisk)
	}

	if fs, ok := merged.FieldStats["name"]; !ok || fs.BytesOnDisk != 20000 {
		t.Error("field 'name' stats not correct")
	}
}

func TestMergeStatisticsEmpty(t *testing.T) {
	merged := MergeStatistics(nil)
	if merged != nil {
		t.Error("expected nil for empty list")
	}

	merged = MergeStatistics([]*DataStatistics{})
	if merged != nil {
		t.Error("expected nil for empty list")
	}
}

func TestDataStatisticsSummary(t *testing.T) {
	stats := &DataStatistics{
		Version:          10,
		TotalRows:        1000,
		DeletedRows:      100,
		ActiveRows:       900,
		NumFragments:     5,
		TotalBytesOnDisk: 50000,
	}

	summary := stats.Summary()
	if summary == "" {
		t.Error("summary should not be empty")
	}

	// Check it contains key info
	expected := []string{"Version: 10", "TotalRows: 1000", "DeletedRows: 100", "ActiveRows: 900"}
	for _, s := range expected {
		found := false
		for i := 0; i+len(s) <= len(summary); i++ {
			if summary[i:i+len(s)] == s {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("summary missing %q", s)
		}
	}
}

func TestGetCompressionRatio(t *testing.T) {
	stats := &DataStatistics{
		ActiveRows:       1000,
		TotalBytesOnDisk: 4000,
	}

	// 1000 rows * 16 bytes raw = 16000 bytes
	// Compressed to 4000 bytes = 4:1 ratio
	ratio := stats.GetCompressionRatio(16)
	expected := 4.0

	if ratio != expected {
		t.Errorf("expected ratio %f, got %f", expected, ratio)
	}

	// Edge cases
	stats.ActiveRows = 0
	ratio = stats.GetCompressionRatio(16)
	if ratio != 1.0 {
		t.Errorf("expected ratio 1.0 for 0 active rows, got %f", ratio)
	}

	stats.ActiveRows = 1000
	stats.TotalBytesOnDisk = 0
	ratio = stats.GetCompressionRatio(16)
	if ratio != 1.0 {
		t.Errorf("expected ratio 1.0 for 0 bytes on disk, got %f", ratio)
	}
}

func TestFieldStatistics(t *testing.T) {
	fs := &FieldStatistics{
		Name:          "test_field",
		FieldIndex:    0,
		BytesOnDisk:   10000,
		NullCount:     50,
		DistinctCount: 500,
		MinValue:      1,
		MaxValue:      1000,
		AvgSize:       10.5,
	}

	if fs.Name != "test_field" {
		t.Errorf("expected name 'test_field', got %s", fs.Name)
	}

	if fs.BytesOnDisk != 10000 {
		t.Errorf("expected BytesOnDisk 10000, got %d", fs.BytesOnDisk)
	}

	if fs.NullCount != 50 {
		t.Errorf("expected NullCount 50, got %d", fs.NullCount)
	}
}

func TestLoadStatisticsNotFound(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	_, err := LoadStatistics(ctx, dir)
	if err == nil {
		t.Error("expected error for non-existent statistics file")
	}
}

func TestDataStatisticsComputedAt(t *testing.T) {
	before := time.Now()
	stats := NewDataStatisticsBuilder().Build()
	after := time.Now()

	if stats.ComputedAt.Before(before) || stats.ComputedAt.After(after) {
		t.Error("ComputedAt should be between before and after")
	}
}
