package storage2

import (
	"testing"
)

func TestIndexStatistics(t *testing.T) {
	// Create an index manager
	manager := NewIndexManager("/tmp/test", NewLocalRenameCommitHandler())

	// Create a B-tree index
	idx := NewBTreeIndex("test_idx", 0)
	manager.indexes["test_idx"] = idx

	// Insert some data
	for i := 0; i < 100; i++ {
		if err := idx.Insert(int64(i*10), uint64(i)); err != nil {
			t.Fatalf("Failed to insert: %v", err)
		}
	}

	// Get index statistics
	stats, err := manager.GetIndexStatistics("test_idx")
	if err != nil {
		t.Fatalf("GetIndexStatistics failed: %v", err)
	}

	// Verify basic stats
	if stats.NumEntries != 100 {
		t.Errorf("Expected 100 entries, got %d", stats.NumEntries)
	}

	if stats.IndexType != "btree" {
		t.Errorf("Expected index type 'btree', got '%s'", stats.IndexType)
	}

	if stats.DistinctValues != 100 {
		t.Errorf("Expected 100 distinct values, got %d", stats.DistinctValues)
	}

	// Verify min/max values
	if idx.minValue == nil {
		t.Error("MinValue should not be nil")
	} else if min, ok := idx.minValue.(int64); !ok || min != 0 {
		t.Errorf("Expected min value 0, got %v", idx.minValue)
	}

	if idx.maxValue == nil {
		t.Error("MaxValue should not be nil")
	} else if max, ok := idx.maxValue.(int64); !ok || max != 990 {
		t.Errorf("Expected max value 990, got %v", idx.maxValue)
	}

	// Verify index statistics fields
	if stats.IndexVersion != 1 {
		t.Errorf("Expected index version 1, got %d", stats.IndexVersion)
	}

	if stats.FragmentCoverage != 100.0 {
		t.Errorf("Expected fragment coverage 100.0, got %f", stats.FragmentCoverage)
	}
}

func TestIndexStatisticsWithDuplicates(t *testing.T) {
	manager := NewIndexManager("/tmp/test2", NewLocalRenameCommitHandler())
	idx := NewBTreeIndex("dup_idx", 0)
	manager.indexes["dup_idx"] = idx

	// Insert data with duplicates
	for i := 0; i < 50; i++ {
		key := int64(i % 10) // Only 10 distinct keys
		if err := idx.Insert(key, uint64(i)); err != nil {
			t.Fatalf("Failed to insert: %v", err)
		}
	}

	stats, err := manager.GetIndexStatistics("dup_idx")
	if err != nil {
		t.Fatalf("GetIndexStatistics failed: %v", err)
	}

	if stats.NumEntries != 50 {
		t.Errorf("Expected 50 entries, got %d", stats.NumEntries)
	}

	if stats.DistinctValues != 10 {
		t.Errorf("Expected 10 distinct values, got %d", stats.DistinctValues)
	}
}

func TestAllIndexStatistics(t *testing.T) {
	manager := NewIndexManager("/tmp/test3", NewLocalRenameCommitHandler())

	// Create multiple indexes
	idx1 := NewBTreeIndex("idx1", 0)
	idx2 := NewBTreeIndex("idx2", 1)
	manager.indexes["idx1"] = idx1
	manager.indexes["idx2"] = idx2

	// Insert data
	for i := 0; i < 10; i++ {
		idx1.Insert(int64(i), uint64(i))
		idx2.Insert(int64(i*2), uint64(i))
	}

	// Get all statistics
	allStats, err := manager.GetAllIndexStatistics()
	if err != nil {
		t.Fatalf("GetAllIndexStatistics failed: %v", err)
	}

	if len(allStats) != 2 {
		t.Errorf("Expected 2 index statistics, got %d", len(allStats))
	}
}

func TestIndexStatisticsNotFound(t *testing.T) {
	manager := NewIndexManager("/tmp/test4", NewLocalRenameCommitHandler())

	_, err := manager.GetIndexStatistics("nonexistent")
	if err == nil {
		t.Error("Expected error for non-existent index")
	}
}
