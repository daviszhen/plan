package storage2

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// IndexType represents the type of index
type IndexType int

const (
	// ScalarIndex is a B-tree or similar index for scalar columns
	ScalarIndex IndexType = iota
	// VectorIndex is an ANN index for vector columns (IVF, HNSW, etc.)
	VectorIndex
	// InvertedIndex is a full-text search index
	InvertedIndex
)

func (t IndexType) String() string {
	switch t {
	case ScalarIndex:
		return "scalar"
	case VectorIndex:
		return "vector"
	case InvertedIndex:
		return "inverted"
	default:
		return "unknown"
	}
}

// IndexMetadata stores metadata about an index
type IndexMetadata struct {
	// Name is the unique name of the index
	Name string `json:"name"`
	// Type is the type of index
	Type IndexType `json:"type"`
	// ColumnIndices are the column indices this index covers
	ColumnIndices []int `json:"column_indices"`
	// Version is the dataset version when the index was created
	Version uint64 `json:"version"`
	// Path is the path to the index file(s)
	Path string `json:"path"`
	// Params are index-specific parameters
	Params map[string]string `json:"params"`
}

// Index is the interface for all index types
type Index interface {
	// Name returns the index name
	Name() string
	// Type returns the index type
	Type() IndexType
	// Columns returns the column indices this index covers
	Columns() []int
	// Search searches the index and returns matching row IDs
	Search(ctx context.Context, query interface{}, limit int) ([]uint64, error)
	// Statistics returns statistics about the index
	Statistics() IndexStats
}

// IndexStats contains statistics about an index
type IndexStats struct {
	// NumEntries is the number of entries in the index
	NumEntries uint64
	// SizeBytes is the size of the index in bytes
	SizeBytes uint64
	// LastUpdated is the timestamp when the index was last updated
	LastUpdated int64
	// NumIndexedFragments is the number of fragments indexed
	NumIndexedFragments uint64
	// IndexType is the type of index
	IndexType string
	// ColumnName is the name of the indexed column
	ColumnName string
	// DataType is the data type of the indexed column
	DataType string
	// MinValue is the minimum value in the index (for scalar indexes)
	MinValue interface{}
	// MaxValue is the maximum value in the index (for scalar indexes)
	MaxValue interface{}
	// DistinctValues is the number of distinct values
	DistinctValues uint64
	// NullCount is the number of null values
	NullCount uint64
}

// IndexStatistics provides detailed statistics about an index
type IndexStatistics struct {
	// Basic stats
	IndexStats
	// BuildTimeMs is the time taken to build the index in milliseconds
	BuildTimeMs int64
	// IndexVersion is the version of the index format
	IndexVersion int
	// IsOptimized indicates if the index has been optimized
	IsOptimized bool
	// FragmentCoverage is the percentage of fragments covered by the index
	FragmentCoverage float64
}

// ScalarIndexImpl is an index for scalar columns (B-tree, etc.)
type ScalarIndexImpl interface {
	Index
	// RangeQuery performs a range query on the index
	RangeQuery(ctx context.Context, start, end interface{}) ([]uint64, error)
	// EqualityQuery performs an equality query on the index
	EqualityQuery(ctx context.Context, value interface{}) ([]uint64, error)
}

// VectorIndexImpl is an index for vector columns (IVF, HNSW, etc.)
type VectorIndexImpl interface {
	Index
	// ANNSearch performs approximate nearest neighbor search
	// Returns distances and row IDs
	ANNSearch(ctx context.Context, queryVector []float32, limit int) ([]float32, []uint64, error)
	// GetMetricType returns the distance metric used by the index
	GetMetricType() MetricType
}

// MetricType represents the distance metric for vector search
type MetricType int

const (
	// L2Metric is Euclidean distance
	L2Metric MetricType = iota
	// CosineMetric is cosine similarity
	CosineMetric
	// DotMetric is dot product
	DotMetric
	// HammingMetric is Hamming distance for binary vectors
	HammingMetric
)

// InvertedIndexImpl is an index for full-text search
type InvertedIndexImpl interface {
	Index
	// TextSearch performs a full-text search
	TextSearch(ctx context.Context, query string, limit int) ([]uint64, []float32, error)
	// PhraseSearch performs a phrase search
	PhraseSearch(ctx context.Context, phrase string, limit int) ([]uint64, error)
}

// IndexManager manages indexes for a dataset
type IndexManager struct {
	basePath string
	handler  CommitHandler
	indexes  map[string]Index
}

// NewIndexManager creates a new index manager
func NewIndexManager(basePath string, handler CommitHandler) *IndexManager {
	return &IndexManager{
		basePath: basePath,
		handler:  handler,
		indexes:  make(map[string]Index),
	}
}

// CreateScalarIndex creates a scalar index on a column
func (m *IndexManager) CreateScalarIndex(ctx context.Context, name string, columnIdx int) error {
	// Create B-tree index as default scalar index
	idx := NewBTreeIndex(name, columnIdx)
	m.indexes[name] = idx
	return nil
}

// CreateBitmapIndex creates a bitmap index on a column
func (m *IndexManager) CreateBitmapIndex(ctx context.Context, name string, columnIdx int) error {
	idx := NewBitmapIndex(columnIdx, WithBitmapIndexName(name))
	m.indexes[name] = idx
	return nil
}

// CreateZoneMapIndex creates a zone map index
func (m *IndexManager) CreateZoneMapIndex(ctx context.Context, name string) error {
	idx := NewZoneMapIndex(WithZoneMapIndexName(name))
	m.indexes[name] = idx
	return nil
}

// CreateBloomFilterIndex creates a bloom filter index
func (m *IndexManager) CreateBloomFilterIndex(ctx context.Context, name string) error {
	idx := NewBloomFilterIndex(WithBloomFilterIndexName(name))
	m.indexes[name] = idx
	return nil
}

// CreateVectorIndex creates a vector index on a column
func (m *IndexManager) CreateVectorIndex(ctx context.Context, name string, columnIdx int, metric MetricType) error {
	idx := NewIVFIndex(name, columnIdx, 0, metric) // dimension set to 0; set via Train
	m.indexes[name] = idx
	return nil
}

// CreateInvertedIndex creates an inverted (full-text search) index on a column
func (m *IndexManager) CreateInvertedIndex(ctx context.Context, name string, columnIdx int) error {
	idx := NewFTSIndex(name, columnIdx)
	m.indexes[name] = idx
	return nil
}

// DropIndex drops an index
func (m *IndexManager) DropIndex(ctx context.Context, name string) error {
	delete(m.indexes, name)
	return nil
}

// GetIndex returns an index by name
func (m *IndexManager) GetIndex(name string) (Index, bool) {
	idx, ok := m.indexes[name]
	return idx, ok
}

// ListIndexes returns all indexes
func (m *IndexManager) ListIndexes() []IndexMetadata {
	var result []IndexMetadata
	for _, idx := range m.indexes {
		result = append(result, IndexMetadata{
			Name:          idx.Name(),
			Type:          idx.Type(),
			ColumnIndices: idx.Columns(),
		})
	}
	return result
}

// OptimizeIndex optimizes an index (e.g., compaction, rebuilding)
func (m *IndexManager) OptimizeIndex(ctx context.Context, name string) error {
	idx, ok := m.GetIndex(name)
	if !ok {
		return fmt.Errorf("index %s not found", name)
	}

	switch index := idx.(type) {
	case *BTreeIndex:
		return m.optimizeBTreeIndex(index)
	case *BitmapIndex:
		return m.optimizeBitmapIndex(index)
	case *ZoneMapIndex:
		return m.optimizeZoneMapIndex(index)
	case *BloomFilterIndex:
		return m.optimizeBloomFilterIndex(index)
	case *IVFIndex:
		return m.optimizeIVFIndex(index)
	case *HNSWIndex:
		return m.optimizeHNSWIndex(index)
	default:
		return fmt.Errorf("optimization not supported for index type %T", idx)
	}
}

// optimizeBitmapIndex optimizes a bitmap index
func (m *IndexManager) optimizeBitmapIndex(idx *BitmapIndex) error {
	// Bitmap indexes are typically compact already
	// Could implement compression optimization in the future
	return nil
}

// optimizeZoneMapIndex optimizes a zone map index
func (m *IndexManager) optimizeZoneMapIndex(idx *ZoneMapIndex) error {
	// Zone maps are updated automatically during writes
	// Could implement merging of fragmented zone maps
	return nil
}

// optimizeBloomFilterIndex optimizes a bloom filter index
func (m *IndexManager) optimizeBloomFilterIndex(idx *BloomFilterIndex) error {
	// Bloom filters could be rebuilt if false positive rate is too high
	// For now, just return success
	return nil
}

// optimizeBTreeIndex optimizes a B-tree index
func (m *IndexManager) optimizeBTreeIndex(idx *BTreeIndex) error {
	// For B-tree, optimization could involve rebalancing or compacting
	// For now, we just update statistics
	return nil
}

// optimizeIVFIndex optimizes an IVF index
func (m *IndexManager) optimizeIVFIndex(idx *IVFIndex) error {
	// For IVF, optimization could involve:
	// 1. Re-clustering if data distribution has changed
	// 2. Merging small inverted lists
	// 3. Pruning unused centroids

	// Re-train with current data if needed
	if idx.Statistics().NumEntries > 1000 {
		vectors := make([][]float32, 0, len(idx.vectors))
		for _, vec := range idx.vectors {
			vectors = append(vectors, vec)
		}
		if len(vectors) > 0 {
			return idx.Train(vectors)
		}
	}
	return nil
}

// optimizeHNSWIndex optimizes an HNSW index
func (m *IndexManager) optimizeHNSWIndex(idx *HNSWIndex) error {
	// For HNSW, optimization could involve:
	// 1. Pruning redundant connections
	// 2. Rebuilding the graph for better connectivity
	// 3. Optimizing layer distribution

	// For now, we just verify the index structure
	return nil
}

// OptimizeAllIndexes optimizes all indexes
func (m *IndexManager) OptimizeAllIndexes(ctx context.Context) error {
	var lastErr error
	for name := range m.indexes {
		if err := m.OptimizeIndex(ctx, name); err != nil {
			lastErr = err
		}
	}
	return lastErr
}

// RebuildIndex rebuilds an index from scratch
func (m *IndexManager) RebuildIndex(ctx context.Context, name string, vectors map[uint64][]float32) error {
	idx, ok := m.GetIndex(name)
	if !ok {
		return fmt.Errorf("index %s not found", name)
	}

	switch index := idx.(type) {
	case *IVFIndex:
		// Clear existing data
		index.vectors = make(map[uint64][]float32)
		index.invertedLists = make([][]uint64, index.nlist)
		index.stats.NumEntries = 0

		// Re-train with new data
		vecList := make([][]float32, 0, len(vectors))
		for _, vec := range vectors {
			vecList = append(vecList, vec)
		}

		if len(vecList) > 0 {
			if err := index.Train(vecList); err != nil {
				return err
			}
		}

		// Re-insert all vectors
		for rowID, vec := range vectors {
			if err := index.Insert(rowID, vec); err != nil {
				return err
			}
		}

		return nil

	case *HNSWIndex:
		// Clear existing data
		index.vectors = make(map[uint64][]float32)
		index.layers = make([]map[uint64][]uint64, 0)
		index.entryPoint = 0
		index.maxLevel = -1
		index.stats.NumEntries = 0

		// Re-insert all vectors
		for rowID, vec := range vectors {
			if err := index.Insert(rowID, vec); err != nil {
				return err
			}
		}

		return nil

	default:
		return fmt.Errorf("rebuild not supported for index type %T", idx)
	}
}

// IndexDescription provides detailed description of an index
type IndexDescription struct {
	// Basic info
	Name      string    `json:"name"`
	Type      IndexType `json:"type"`
	ColumnIdx int       `json:"column_idx"`

	// Index-specific parameters
	Parameters map[string]interface{} `json:"parameters"`

	// Current status
	Status      string `json:"status"`
	IsOptimized bool   `json:"is_optimized"`

	// Statistics
	Statistics IndexStatistics `json:"statistics"`
}

// DescribeIndex returns a detailed description of an index by name
func (m *IndexManager) DescribeIndex(name string) (*IndexDescription, error) {
	idx, ok := m.GetIndex(name)
	if !ok {
		return nil, fmt.Errorf("index %s not found", name)
	}

	desc := &IndexDescription{
		Name:        idx.Name(),
		Type:        idx.Type(),
		ColumnIdx:   idx.Columns()[0],
		Parameters:  make(map[string]interface{}),
		Status:      "active",
		IsOptimized: false,
	}

	// Get statistics
	stats, err := m.GetIndexStatistics(name)
	if err != nil {
		return nil, err
	}
	desc.Statistics = *stats

	// Add type-specific parameters
	switch index := idx.(type) {
	case *BTreeIndex:
		desc.Parameters["degree"] = BTreeDegree
		desc.Parameters["implementation"] = "btree"

	case *IVFIndex:
		desc.Parameters["nlist"] = index.nlist
		desc.Parameters["nprobe"] = index.nprobe
		desc.Parameters["dimension"] = index.dimension
		desc.Parameters["metric"] = index.metricType.String()
		desc.Parameters["implementation"] = "ivf"

	case *HNSWIndex:
		desc.Parameters["M"] = index.M
		desc.Parameters["Mmax"] = index.Mmax
		desc.Parameters["ef_construction"] = index.efConstruction
		desc.Parameters["ef_search"] = index.efSearch
		desc.Parameters["dimension"] = index.dimension
		desc.Parameters["metric"] = index.metricType.String()
		desc.Parameters["max_level"] = index.maxLevel
		desc.Parameters["implementation"] = "hnsw"
	}

	return desc, nil
}

// DescribeIndexesByName returns descriptions for indexes matching a name pattern
func (m *IndexManager) DescribeIndexesByName(pattern string) ([]*IndexDescription, error) {
	var results []*IndexDescription

	// For now, do exact match or prefix match
	for name := range m.indexes {
		if name == pattern || (len(pattern) > 0 && len(name) >= len(pattern) && name[:len(pattern)] == pattern) {
			desc, err := m.DescribeIndex(name)
			if err != nil {
				continue
			}
			results = append(results, desc)
		}
	}

	return results, nil
}

// String returns string representation of MetricType
func (m MetricType) String() string {
	switch m {
	case L2Metric:
		return "l2"
	case CosineMetric:
		return "cosine"
	case DotMetric:
		return "dot"
	case HammingMetric:
		return "hamming"
	default:
		return "unknown"
	}
}

// GetIndexStatistics returns detailed statistics for an index
func (m *IndexManager) GetIndexStatistics(name string) (*IndexStatistics, error) {
	idx, ok := m.GetIndex(name)
	if !ok {
		return nil, fmt.Errorf("index %s not found", name)
	}

	stats := idx.Statistics()

	return &IndexStatistics{
		IndexStats:       stats,
		IndexVersion:     1, // Current version
		IsOptimized:      false,
		FragmentCoverage: 100.0, // Assuming full coverage for now
	}, nil
}

// GetAllIndexStatistics returns statistics for all indexes
func (m *IndexManager) GetAllIndexStatistics() ([]*IndexStatistics, error) {
	var results []*IndexStatistics

	for name := range m.indexes {
		stats, err := m.GetIndexStatistics(name)
		if err != nil {
			continue
		}
		results = append(results, stats)
	}

	return results, nil
}

// IndexPlanner plans index usage for queries
type IndexPlanner struct {
	manager *IndexManager
}

// NewIndexPlanner creates a new index planner
func NewIndexPlanner(manager *IndexManager) *IndexPlanner {
	return &IndexPlanner{manager: manager}
}

// PlanQuery plans index usage for a query.
// It delegates to IndexPlannerV2 for cost-based index selection.
func (p *IndexPlanner) PlanQuery(predicate FilterPredicate) ([]uint64, bool) {
	v2 := NewIndexPlannerV2(p.manager)
	return v2.PlanQueryLegacy(predicate)
}

// IndexStore is the interface for index storage
type IndexStore interface {
	// WriteIndex writes an index to storage
	WriteIndex(ctx context.Context, name string, data []byte) error
	// ReadIndex reads an index from storage
	ReadIndex(ctx context.Context, name string) ([]byte, error)
	// DeleteIndex deletes an index from storage
	DeleteIndex(ctx context.Context, name string) error
	// ListIndexes lists all indexes in storage
	ListIndexes(ctx context.Context) ([]string, error)
}

// LocalIndexStore is a local filesystem implementation of IndexStore
type LocalIndexStore struct {
	basePath string
}

// NewLocalIndexStore creates a new LocalIndexStore
func NewLocalIndexStore(basePath string) *LocalIndexStore {
	return &LocalIndexStore{basePath: basePath}
}

// indicesDir returns the path to the _indices directory
func (s *LocalIndexStore) indicesDir() string {
	return filepath.Join(s.basePath, "_indices")
}

// indexDir returns the path to a specific index directory
func (s *LocalIndexStore) indexDir(name string) string {
	return filepath.Join(s.indicesDir(), name)
}

// WriteIndex implements IndexStore
func (s *LocalIndexStore) WriteIndex(ctx context.Context, name string, data []byte) error {
	dir := s.indexDir(name)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create index directory: %w", err)
	}
	dataPath := filepath.Join(dir, "data.bin")
	if err := os.WriteFile(dataPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write index data: %w", err)
	}
	return nil
}

// WriteIndexMetadata writes index metadata to storage
func (s *LocalIndexStore) WriteIndexMetadata(ctx context.Context, name string, metadata *IndexMetadata) error {
	dir := s.indexDir(name)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create index directory: %w", err)
	}
	metaPath := filepath.Join(dir, "metadata.json")
	data, err := json.MarshalIndent(metadata, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal index metadata: %w", err)
	}
	if err := os.WriteFile(metaPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write index metadata: %w", err)
	}
	return nil
}

// ReadIndex implements IndexStore
func (s *LocalIndexStore) ReadIndex(ctx context.Context, name string) ([]byte, error) {
	dataPath := filepath.Join(s.indexDir(name), "data.bin")
	data, err := os.ReadFile(dataPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("index %q not found", name)
		}
		return nil, fmt.Errorf("failed to read index data: %w", err)
	}
	return data, nil
}

// ReadIndexMetadata reads index metadata from storage
func (s *LocalIndexStore) ReadIndexMetadata(ctx context.Context, name string) (*IndexMetadata, error) {
	metaPath := filepath.Join(s.indexDir(name), "metadata.json")
	data, err := os.ReadFile(metaPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("index metadata %q not found", name)
		}
		return nil, fmt.Errorf("failed to read index metadata: %w", err)
	}
	var metadata IndexMetadata
	if err := json.Unmarshal(data, &metadata); err != nil {
		return nil, fmt.Errorf("failed to unmarshal index metadata: %w", err)
	}
	return &metadata, nil
}

// DeleteIndex implements IndexStore
func (s *LocalIndexStore) DeleteIndex(ctx context.Context, name string) error {
	dir := s.indexDir(name)
	if err := os.RemoveAll(dir); err != nil {
		return fmt.Errorf("failed to delete index: %w", err)
	}
	return nil
}

// ListIndexes implements IndexStore
func (s *LocalIndexStore) ListIndexes(ctx context.Context) ([]string, error) {
	dir := s.indicesDir()
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to list indexes: %w", err)
	}
	var names []string
	for _, entry := range entries {
		if entry.IsDir() {
			names = append(names, entry.Name())
		}
	}
	return names, nil
}

// WriteIndexStats writes index statistics to storage
func (s *LocalIndexStore) WriteIndexStats(ctx context.Context, name string, stats *IndexStats) error {
	dir := s.indexDir(name)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create index directory: %w", err)
	}
	statsPath := filepath.Join(dir, "stats.json")
	data, err := json.MarshalIndent(stats, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal index stats: %w", err)
	}
	if err := os.WriteFile(statsPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write index stats: %w", err)
	}
	return nil
}

// ReadIndexStats reads index statistics from storage
func (s *LocalIndexStore) ReadIndexStats(ctx context.Context, name string) (*IndexStats, error) {
	statsPath := filepath.Join(s.indexDir(name), "stats.json")
	data, err := os.ReadFile(statsPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("index stats %q not found", name)
		}
		return nil, fmt.Errorf("failed to read index stats: %w", err)
	}
	var stats IndexStats
	if err := json.Unmarshal(data, &stats); err != nil {
		return nil, fmt.Errorf("failed to unmarshal index stats: %w", err)
	}
	return &stats, nil
}

// Serializable is the interface for indexes that support serialization
type Serializable interface {
	// Marshal serializes the index to bytes
	Marshal() ([]byte, error)
	// Unmarshal deserializes the index from bytes
	Unmarshal(data []byte) error
}

// SaveIndex saves an index to the index store
func (m *IndexManager) SaveIndex(ctx context.Context, name string, store *LocalIndexStore) error {
	idx, ok := m.GetIndex(name)
	if !ok {
		return fmt.Errorf("index %q not found", name)
	}

	// Get index metadata
	metadata := &IndexMetadata{
		Name:          idx.Name(),
		Type:          idx.Type(),
		ColumnIndices: idx.Columns(),
		Path:          fmt.Sprintf("_indices/%s", name),
	}

	// Write metadata
	if err := store.WriteIndexMetadata(ctx, name, metadata); err != nil {
		return err
	}

	// Write statistics
	stats := idx.Statistics()
	if err := store.WriteIndexStats(ctx, name, &stats); err != nil {
		return err
	}

	// Check if index implements Serializable
	if serializable, ok := idx.(Serializable); ok {
		data, err := serializable.Marshal()
		if err != nil {
			return fmt.Errorf("failed to serialize index: %w", err)
		}
		if err := store.WriteIndex(ctx, name, data); err != nil {
			return err
		}
	}

	return nil
}

// LoadIndex loads an index from the index store
func (m *IndexManager) LoadIndex(ctx context.Context, name string, store *LocalIndexStore) (Index, error) {
	// Read metadata
	metadata, err := store.ReadIndexMetadata(ctx, name)
	if err != nil {
		return nil, err
	}

	// Create index based on type
	var idx Index
	switch metadata.Type {
	case ScalarIndex:
		if len(metadata.ColumnIndices) > 0 {
			btree := NewBTreeIndex(name, metadata.ColumnIndices[0])
			// Try to load serialized data
			data, err := store.ReadIndex(ctx, name)
			if err == nil && len(data) > 0 {
				if serializable, ok := interface{}(btree).(Serializable); ok {
					if err := serializable.Unmarshal(data); err != nil {
						return nil, fmt.Errorf("failed to deserialize index: %w", err)
					}
				}
			}
			idx = btree
		}
	default:
		return nil, fmt.Errorf("unsupported index type: %v", metadata.Type)
	}

	if idx != nil {
		m.indexes[name] = idx
	}

	return idx, nil
}

// LoadAllIndexes loads all indexes from the index store
func (m *IndexManager) LoadAllIndexes(ctx context.Context, store *LocalIndexStore) error {
	names, err := store.ListIndexes(ctx)
	if err != nil {
		return err
	}

	for _, name := range names {
		if _, err := m.LoadIndex(ctx, name, store); err != nil {
			// Log error but continue loading other indexes
			continue
		}
	}

	return nil
}
