package sdk

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/daviszhen/plan/pkg/storage2"
	"github.com/stretchr/testify/require"
)

func TestDataset_CreateVectorIndex(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	// Create dataset
	dataset, err := CreateDataset(ctx, tmpDir).Build()
	require.NoError(t, err)
	defer dataset.Close()

	// Create IVF index
	err = dataset.CreateVectorIndexSimple(ctx, "test_ivf", 0, 64, storage2.L2Metric, "ivf")
	require.NoError(t, err)

	// Create HNSW index
	err = dataset.CreateVectorIndexSimple(ctx, "test_hnsw", 0, 64, storage2.CosineMetric, "hnsw")
	require.NoError(t, err)

	// List indexes
	names := dataset.ListVectorIndexes()
	require.Len(t, names, 2)
	require.Contains(t, names, "test_ivf")
	require.Contains(t, names, "test_hnsw")
}

func TestDataset_DropVectorIndex(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	dataset, err := CreateDataset(ctx, tmpDir).Build()
	require.NoError(t, err)
	defer dataset.Close()

	// Create index
	err = dataset.CreateVectorIndexSimple(ctx, "to_drop", 0, 32, storage2.L2Metric, "ivf")
	require.NoError(t, err)

	// Verify exists
	names := dataset.ListVectorIndexes()
	require.Contains(t, names, "to_drop")

	// Drop index
	err = dataset.DropVectorIndex(ctx, "to_drop")
	require.NoError(t, err)

	// Verify dropped
	names = dataset.ListVectorIndexes()
	require.NotContains(t, names, "to_drop")
}

func TestDataset_SearchNearestIVF(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	dataset, err := CreateDataset(ctx, tmpDir).Build()
	require.NoError(t, err)
	defer dataset.Close()

	// Create index
	err = dataset.CreateVectorIndex(ctx, storage2.VectorSearchIndexConfig{
		Name:      "search_ivf",
		ColumnIdx: 0,
		Dimension: 4,
		Metric:    storage2.L2Metric,
		IndexType: "ivf",
		NList:     2,
		NProbe:    2,
	})
	require.NoError(t, err)

	// Build index with vectors
	vectors := make(map[uint64][]float32)
	for i := 0; i < 20; i++ {
		vectors[uint64(i)] = []float32{float32(i), float32(i), float32(i), float32(i)}
	}
	err = dataset.BuildVectorIndex(ctx, "search_ivf", vectors)
	require.NoError(t, err)

	// Search
	query := []float32{5, 5, 5, 5}
	results, err := dataset.SearchNearest(ctx, "search_ivf", query, 5)
	require.NoError(t, err)
	require.LessOrEqual(t, len(results.Results), 5)
	require.Greater(t, len(results.Results), 0)

	// Results should be sorted by distance
	for i := 1; i < len(results.Results); i++ {
		require.GreaterOrEqual(t, results.Results[i].Distance, results.Results[i-1].Distance)
	}

	// First result should be close to row 5
	require.Equal(t, uint64(5), results.Results[0].RowID)
}

func TestDataset_SearchNearestHNSW(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	dataset, err := CreateDataset(ctx, tmpDir).Build()
	require.NoError(t, err)
	defer dataset.Close()

	// Create index
	err = dataset.CreateVectorIndex(ctx, storage2.VectorSearchIndexConfig{
		Name:           "search_hnsw",
		ColumnIdx:      0,
		Dimension:      4,
		Metric:         storage2.CosineMetric,
		IndexType:      "hnsw",
		M:              8,
		EfConstruction: 50,
		EfSearch:       20,
	})
	require.NoError(t, err)

	// Build index with vectors
	vectors := make(map[uint64][]float32)
	for i := 0; i < 30; i++ {
		v := float32(i) / 30.0
		vectors[uint64(i)] = []float32{v, v, v, v}
	}
	err = dataset.BuildVectorIndex(ctx, "search_hnsw", vectors)
	require.NoError(t, err)

	// Search
	query := []float32{0.5, 0.5, 0.5, 0.5}
	results, err := dataset.SearchNearest(ctx, "search_hnsw", query, 5)
	require.NoError(t, err)
	require.LessOrEqual(t, len(results.Results), 5)
	require.Greater(t, len(results.Results), 0)
	require.Equal(t, "hnsw", results.IndexType)
	require.Equal(t, storage2.CosineMetric, results.Metric)
}

func TestDataset_SaveLoadVectorIndex(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	// Create and populate index
	dataset, err := CreateDataset(ctx, tmpDir).Build()
	require.NoError(t, err)

	err = dataset.CreateVectorIndex(ctx, storage2.VectorSearchIndexConfig{
		Name:      "persist_test",
		ColumnIdx: 0,
		Dimension: 8,
		Metric:    storage2.L2Metric,
		IndexType: "ivf",
		NList:     4,
		NProbe:    2,
	})
	require.NoError(t, err)

	vectors := make(map[uint64][]float32)
	for i := 0; i < 20; i++ {
		vectors[uint64(i)] = []float32{
			float32(i), float32(i + 1), float32(i + 2), float32(i + 3),
			float32(i + 4), float32(i + 5), float32(i + 6), float32(i + 7),
		}
	}
	err = dataset.BuildVectorIndex(ctx, "persist_test", vectors)
	require.NoError(t, err)

	// Save index
	err = dataset.SaveVectorIndex(ctx, "persist_test")
	require.NoError(t, err)

	// Verify file exists
	indexPath := filepath.Join(tmpDir, storage2.IndexDir, "persist_test.idx")
	_, err = os.Stat(indexPath)
	require.NoError(t, err)

	dataset.Close()

	// Reopen dataset and load index
	dataset2, err := OpenDataset(ctx, tmpDir).Build()
	require.NoError(t, err)
	defer dataset2.Close()

	err = dataset2.LoadVectorIndex(ctx, "persist_test", storage2.VectorSearchIndexConfig{
		Name:      "persist_test",
		ColumnIdx: 0,
		Dimension: 8,
		Metric:    storage2.L2Metric,
		IndexType: "ivf",
	})
	require.NoError(t, err)

	// Verify loaded index works
	query := []float32{5, 6, 7, 8, 9, 10, 11, 12}
	results, err := dataset2.SearchNearest(ctx, "persist_test", query, 3)
	require.NoError(t, err)
	require.Greater(t, len(results.Results), 0)
}

func TestDataset_GetVectorIndex(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	dataset, err := CreateDataset(ctx, tmpDir).Build()
	require.NoError(t, err)
	defer dataset.Close()

	// Create index
	err = dataset.CreateVectorIndexSimple(ctx, "get_test", 0, 16, storage2.DotMetric, "hnsw")
	require.NoError(t, err)

	// Get index
	idx, ok := dataset.GetVectorIndex("get_test")
	require.True(t, ok)
	require.NotNil(t, idx)
	require.Equal(t, "get_test", idx.Name())
	require.Equal(t, storage2.DotMetric, idx.GetMetricType())

	// Non-existent index
	_, ok = dataset.GetVectorIndex("nonexistent")
	require.False(t, ok)
}

func TestDataset_KNNWithRealData(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	// Create dataset with schema
	dataset, err := CreateDataset(ctx, tmpDir).Build()
	require.NoError(t, err)
	defer dataset.Close()

	// Create vector index (the index doesn't need actual data to exist)
	err = dataset.CreateVectorIndex(ctx, storage2.VectorSearchIndexConfig{
		Name:      "embedding_idx",
		ColumnIdx: 1,
		Dimension: 128,
		Metric:    storage2.CosineMetric,
		IndexType: "ivf",
		NList:     10,
		NProbe:    3,
	})
	require.NoError(t, err)

	// Verify index exists
	names := dataset.ListVectorIndexes()
	require.Contains(t, names, "embedding_idx")

	// Build index with some vectors
	vectors := make(map[uint64][]float32)
	for i := 0; i < 50; i++ {
		vec := make([]float32, 128)
		for j := 0; j < 128; j++ {
			vec[j] = float32(i*128 + j)
		}
		vectors[uint64(i)] = vec
	}
	err = dataset.BuildVectorIndex(ctx, "embedding_idx", vectors)
	require.NoError(t, err)

	// Search
	query := make([]float32, 128)
	for i := 0; i < 128; i++ {
		query[i] = float32(i)
	}
	results, err := dataset.SearchNearest(ctx, "embedding_idx", query, 5)
	require.NoError(t, err)
	require.Greater(t, len(results.Results), 0)
}

func TestSearchResults_JSON(t *testing.T) {
	results := &storage2.SearchResults{
		Results: []storage2.SearchResult{
			{RowID: 1, Distance: 0.1},
			{RowID: 5, Distance: 0.3},
		},
		QueryTime: 1000000,
		IndexType: "hnsw",
		Metric:    storage2.L2Metric,
	}

	// Verify structure
	require.Len(t, results.Results, 2)
	require.Equal(t, uint64(1), results.Results[0].RowID)
	require.Equal(t, float32(0.1), results.Results[0].Distance)
}

func TestVectorSearchIndexConfig_AllOptions(t *testing.T) {
	tests := []struct {
		name   string
		config storage2.VectorSearchIndexConfig
	}{
		{
			name: "ivf_full",
			config: storage2.VectorSearchIndexConfig{
				Name:      "ivf_test",
				ColumnIdx: 0,
				Dimension: 256,
				Metric:    storage2.L2Metric,
				IndexType: "ivf",
				NList:     100,
				NProbe:    10,
			},
		},
		{
			name: "hnsw_full",
			config: storage2.VectorSearchIndexConfig{
				Name:           "hnsw_test",
				ColumnIdx:      2,
				Dimension:      512,
				Metric:         storage2.CosineMetric,
				IndexType:      "hnsw",
				M:              32,
				EfConstruction: 400,
				EfSearch:       100,
			},
		},
		{
			name: "dot_metric",
			config: storage2.VectorSearchIndexConfig{
				Name:      "dot_test",
				ColumnIdx: 0,
				Dimension: 64,
				Metric:    storage2.DotMetric,
				IndexType: "ivf",
			},
		},
	}

	ctx := context.Background()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpDir := t.TempDir()
			dataset, err := CreateDataset(ctx, tmpDir).Build()
			require.NoError(t, err)
			defer dataset.Close()

			err = dataset.CreateVectorIndex(ctx, tt.config)
			require.NoError(t, err)

			idx, ok := dataset.GetVectorIndex(tt.config.Name)
			require.True(t, ok)
			require.Equal(t, tt.config.Name, idx.Name())
			require.Equal(t, tt.config.Metric, idx.GetMetricType())
		})
	}
}
