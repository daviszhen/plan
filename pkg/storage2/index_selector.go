package storage2

import (
	"context"
	"path/filepath"

	"github.com/daviszhen/plan/pkg/chunk"
	"github.com/daviszhen/plan/pkg/common"
)

// IndexType extended constants
const (
	// BitmapIndex is a bitmap index for low-cardinality columns
	BitmapIndexType IndexType = iota + 100 // Start from 100 to avoid conflict
	// ZoneMapIndexType is a zone map index for range pruning
	ZoneMapIndexType
	// BloomFilterIndexType is a bloom filter for membership testing
	BloomFilterIndexType
)

// String returns string representation for extended index types
func ExtendedIndexTypeString(t IndexType) string {
	switch t {
	case BitmapIndexType:
		return "bitmap"
	case ZoneMapIndexType:
		return "zonemap"
	case BloomFilterIndexType:
		return "bloomfilter"
	default:
		return t.String()
	}
}

// IndexSelector selects the best index for a given query predicate
type IndexSelector struct {
	manager *IndexManager
	config  IndexSelectorConfig
}

// IndexSelectorConfig configures index selection behavior
type IndexSelectorConfig struct {
	// PreferBitmapForLowCardinality prefers bitmap index for low cardinality columns
	PreferBitmapForLowCardinality bool
	// LowCardinalityThreshold is the threshold for low cardinality (default 100)
	LowCardinalityThreshold uint64
	// UseZoneMapForRanges enables zone map for range queries
	UseZoneMapForRanges bool
	// UseBloomFilterForEquality enables bloom filter for equality queries
	UseBloomFilterForEquality bool
	// MaxIndexScanRatio is the maximum ratio of rows to scan using index (0.0-1.0)
	MaxIndexScanRatio float64
}

// DefaultIndexSelectorConfig returns default configuration
func DefaultIndexSelectorConfig() IndexSelectorConfig {
	return IndexSelectorConfig{
		PreferBitmapForLowCardinality: true,
		LowCardinalityThreshold:       100,
		UseZoneMapForRanges:           true,
		UseBloomFilterForEquality:     true,
		MaxIndexScanRatio:             0.3, // Use index if it filters > 70% of data
	}
}

// NewIndexSelector creates a new index selector
func NewIndexSelector(manager *IndexManager, config IndexSelectorConfig) *IndexSelector {
	if manager == nil {
		manager = NewIndexManager("", nil)
	}
	return &IndexSelector{
		manager: manager,
		config:  config,
	}
}

// IndexSelection represents the result of index selection
type IndexSelection struct {
	// Index is the selected index
	Index Index
	// Type is the type of index selected
	Type IndexType
	// ColumnIdx is the column index
	ColumnIdx int
	// EstimatedSelectivity is the estimated selectivity (0.0-1.0)
	EstimatedSelectivity float64
	// CanPrune indicates if this index can prune data
	CanPrune bool
	// PrunableFragments are fragment IDs that can be pruned
	PrunableFragments []uint64
	// FilterOp is the comparison operator from the predicate (for fragment-level pruning)
	FilterOp string
	// FilterValue is the comparison value from the predicate (for fragment-level pruning)
	FilterValue interface{}
}

// SelectIndexForPredicate selects the best index for a predicate
func (s *IndexSelector) SelectIndexForPredicate(ctx context.Context, pred FilterPredicate) (*IndexSelection, error) {
	// Extract column predicate
	colPred, ok := pred.(*ColumnPredicate)
	if !ok {
		// Handle compound predicates
		switch p := pred.(type) {
		case *AndPredicate:
			// Try to select index for left or right
			leftSel, err := s.SelectIndexForPredicate(ctx, p.Left)
			if err == nil && leftSel != nil {
				return leftSel, nil
			}
			return s.SelectIndexForPredicate(ctx, p.Right)
		case *OrPredicate:
			// For OR, we can't easily use a single index
			return nil, nil
		default:
			return nil, nil
		}
	}

	return s.selectIndexForColumnPredicate(ctx, colPred)
}

// selectIndexForColumnPredicate selects index for a column predicate
func (s *IndexSelector) selectIndexForColumnPredicate(ctx context.Context, pred *ColumnPredicate) (*IndexSelection, error) {
	columnIdx := pred.ColumnIndex
	op := pred.Op

	// Get available indexes for this column
	availableIndexes := s.getAvailableIndexes(columnIdx)

	if len(availableIndexes) == 0 {
		return nil, nil
	}

	// Select best index based on operation type
	switch op {
	case Eq:
		// Equality: prefer Bitmap (low cardinality) or BloomFilter (high cardinality)
		return s.selectForEquality(ctx, availableIndexes, columnIdx, pred)

	case Lt, Le, Gt, Ge:
		// Range: prefer ZoneMap
		return s.selectForRange(ctx, availableIndexes, columnIdx, pred)

	case Ne:
		// Not equal: less effective for most indexes
		return nil, nil

	default:
		return nil, nil
	}
}

// getAvailableIndexes returns available indexes for a column
func (s *IndexSelector) getAvailableIndexes(columnIdx int) []Index {
	var indexes []Index

	// Check all index types
	for _, idx := range s.manager.indexes {
		if len(idx.Columns()) == 1 && idx.Columns()[0] == columnIdx {
			indexes = append(indexes, idx)
		}
	}

	return indexes
}

// selectForEquality selects index for equality predicate
func (s *IndexSelector) selectForEquality(ctx context.Context, indexes []Index, columnIdx int, pred *ColumnPredicate) (*IndexSelection, error) {
	var bestSelection *IndexSelection

	for _, idx := range indexes {
		selection := &IndexSelection{
			Index:       idx,
			Type:        idx.Type(),
			ColumnIdx:   columnIdx,
			FilterOp:    pred.Op.String(),
			FilterValue: extractValueFromChunkValue(pred.Value),
		}

		switch index := idx.(type) {
		case *BitmapIndex:
			// Bitmap is good for equality on low cardinality
			cardinality := index.GetValueCardinality(extractValueFromChunkValue(pred.Value))
			stats := index.Statistics()

			// Skip if no data
			if stats.NumEntries == 0 {
				continue
			}

			// For bitmap, we can always use it for equality queries
			// even if cardinality is 0 (means value doesn't exist, can prune all)
			if s.config.PreferBitmapForLowCardinality {
				if cardinality == 0 {
					// Value doesn't exist - can prune everything
					selection.EstimatedSelectivity = 0
					selection.CanPrune = true
					return selection, nil
				}

				if cardinality <= s.config.LowCardinalityThreshold {
					selection.EstimatedSelectivity = float64(cardinality) / float64(stats.NumEntries)
					selection.CanPrune = true

					// For bitmap, always select if cardinality is low
					// The selectivity check is for cost-based optimization later
					if bestSelection == nil || selection.EstimatedSelectivity < bestSelection.EstimatedSelectivity {
						bestSelection = selection
					}
				}
			}

		case *BloomFilterIndex:
			// Bloom filter for equality on high cardinality
			if s.config.UseBloomFilterForEquality {
				// Check if value might be in the filter
				mightContain, err := index.MightContain(columnIdx, extractValueFromChunkValue(pred.Value))
				if err != nil {
					continue
				}

				if !mightContain {
					// Definitely not present - can prune everything
					selection.EstimatedSelectivity = 0
					selection.CanPrune = true
					selection.PrunableFragments = index.GetPrunableFragments(columnIdx, extractValueFromChunkValue(pred.Value))
					return selection, nil // Best possible outcome
				}

				// Might be present - estimate selectivity based on FPR
				filter := index.GetFilter(columnIdx)
				if filter != nil {
					selection.EstimatedSelectivity = filter.EstimatedFalsePositiveRate()
					selection.CanPrune = true
					selection.PrunableFragments = index.GetPrunableFragments(columnIdx, extractValueFromChunkValue(pred.Value))

					if bestSelection == nil || selection.EstimatedSelectivity < bestSelection.EstimatedSelectivity {
						bestSelection = selection
					}
				}
			}

		case *ZoneMapIndex:
			// ZoneMap can prune if value is outside min/max
			if s.config.UseZoneMapForRanges {
				canPrune := index.CanPrune(columnIdx, "=", extractValueFromChunkValue(pred.Value))
				if canPrune {
					selection.EstimatedSelectivity = 0
					selection.CanPrune = true
					return selection, nil
				}
			}
		}
	}

	// Return best selection if found
	// For bitmap and bloomfilter, we want to use them even if selectivity is higher
	// because they provide fast lookups
	if bestSelection != nil {
		return bestSelection, nil
	}

	return nil, nil
}

// selectForRange selects index for range predicate
func (s *IndexSelector) selectForRange(ctx context.Context, indexes []Index, columnIdx int, pred *ColumnPredicate) (*IndexSelection, error) {
	var bestSelection *IndexSelection

	for _, idx := range indexes {
		selection := &IndexSelection{
			Index:       idx,
			Type:        idx.Type(),
			ColumnIdx:   columnIdx,
			FilterOp:    pred.Op.String(),
			FilterValue: extractValueFromChunkValue(pred.Value),
		}

		switch index := idx.(type) {
		case *ZoneMapIndex:
			// ZoneMap is best for range queries
			if s.config.UseZoneMapForRanges {
				zm := index.GetZoneMap(columnIdx)
				if zm == nil || !zm.Initialized {
					continue
				}

				// Estimate selectivity based on overlap
				selectivity := estimateRangeSelectivity(zm, pred.Op, extractValueFromChunkValue(pred.Value))
				selection.EstimatedSelectivity = selectivity
				selection.CanPrune = selectivity < 1.0

				if selectivity == 0 {
					// No overlap - can prune everything
					return selection, nil
				}

				if bestSelection == nil || selection.EstimatedSelectivity < bestSelection.EstimatedSelectivity {
					bestSelection = selection
				}
			}

		case *BitmapIndex:
			// Bitmap is less effective for ranges, but can be used
			// Skip for now

		case *BloomFilterIndex:
			// BloomFilter is not useful for range queries
			continue
		}
	}

	if bestSelection != nil && bestSelection.EstimatedSelectivity <= s.config.MaxIndexScanRatio {
		return bestSelection, nil
	}

	return nil, nil
}

// estimateRangeSelectivity estimates selectivity for a range query
func estimateRangeSelectivity(zm *ZoneMap, op ComparisonOp, value interface{}) float64 {
	if !zm.Initialized || zm.RowCount == 0 {
		return 1.0
	}

	minVal := zm.MinValue
	maxVal := zm.MaxValue

	// Check if value is completely outside range
	switch op {
	case Lt, Le:
		if compareInterfaceValues(value, minVal) <= 0 {
			return 0.0 // All values are >= min
		}
		if compareInterfaceValues(value, maxVal) >= 0 {
			return 1.0 // All values are < max
		}
		// Estimate: (value - min) / (max - min)
		return estimateFraction(minVal, maxVal, value)

	case Gt, Ge:
		if compareInterfaceValues(value, maxVal) >= 0 {
			return 0.0 // All values are <= max
		}
		if compareInterfaceValues(value, minVal) <= 0 {
			return 1.0 // All values are > min
		}
		// Estimate: (max - value) / (max - min)
		return 1.0 - estimateFraction(minVal, maxVal, value)
	}

	return 1.0
}

// estimateFraction estimates the fraction of values in [min, value] relative to [min, max]
func estimateFraction(min, max, value interface{}) float64 {
	// Handle numeric types
	switch minVal := min.(type) {
	case int64:
		maxVal, ok1 := max.(int64)
		val, ok2 := value.(int64)
		if ok1 && ok2 && maxVal != minVal {
			return float64(val-minVal) / float64(maxVal-minVal)
		}
	case float64:
		maxVal, ok1 := max.(float64)
		val, ok2 := value.(float64)
		if ok1 && ok2 && maxVal != minVal {
			return (val - minVal) / (maxVal - minVal)
		}
	}

	// Default: assume 50% selectivity
	return 0.5
}

// compareInterfaceValues compares two interface{} values
func compareInterfaceValues(a, b interface{}) int {
	switch av := a.(type) {
	case int64:
		if bv, ok := b.(int64); ok {
			if av < bv {
				return -1
			} else if av > bv {
				return 1
			}
			return 0
		}
	case float64:
		if bv, ok := b.(float64); ok {
			if av < bv {
				return -1
			} else if av > bv {
				return 1
			}
			return 0
		}
	case string:
		if bv, ok := b.(string); ok {
			if av < bv {
				return -1
			} else if av > bv {
				return 1
			}
			return 0
		}
	}
	return 0
}

// extractValueFromChunkValue extracts a Go value from chunk.Value
func extractValueFromChunkValue(cv *chunk.Value) interface{} {
	if cv == nil {
		return nil
	}

	switch cv.Typ.Id {
	case common.LTID_BOOLEAN:
		return cv.Bool
	case common.LTID_TINYINT, common.LTID_SMALLINT, common.LTID_INTEGER, common.LTID_BIGINT:
		return cv.I64
	case common.LTID_UTINYINT, common.LTID_USMALLINT, common.LTID_UINTEGER, common.LTID_UBIGINT:
		return cv.U64
	case common.LTID_FLOAT, common.LTID_DOUBLE:
		return cv.F64
	case common.LTID_VARCHAR:
		return cv.Str
	default:
		return cv.I64
	}
}

// IndexAwareScanner wraps PushdownScanner with index-aware optimization
type IndexAwareScanner struct {
	*PushdownScanner
	selector *IndexSelector
	manager  *IndexManager
}

// NewIndexAwareScanner creates a new index-aware scanner
func NewIndexAwareScanner(basePath string, handler CommitHandler, version uint64, config *ScanConfig, manager *IndexManager) *IndexAwareScanner {
	if config == nil {
		config = DefaultScanConfig()
	}

	if manager == nil {
		manager = NewIndexManager(basePath, handler)
	}

	return &IndexAwareScanner{
		PushdownScanner: NewPushdownScanner(basePath, handler, version, config),
		selector:        NewIndexSelector(manager, DefaultIndexSelectorConfig()),
		manager:         manager,
	}
}

// ScanWithIndex performs a scan with index optimization
func (s *IndexAwareScanner) ScanWithIndex(ctx context.Context) ([]*chunk.Chunk, error) {
	// If no filter, use regular scan
	if s.config.Filter == nil {
		return s.Scan(ctx)
	}

	// Try to select an index
	selection, err := s.selector.SelectIndexForPredicate(ctx, s.config.Filter)
	if err != nil || selection == nil || !selection.CanPrune {
		// No suitable index, fall back to regular scan
		return s.Scan(ctx)
	}

	// Use index for pruning
	return s.scanWithIndexPruning(ctx, selection)
}

// scanWithIndexPruning performs scan with index-based pruning
func (s *IndexAwareScanner) scanWithIndexPruning(ctx context.Context, selection *IndexSelection) ([]*chunk.Chunk, error) {
	// Load manifest
	m, err := LoadManifest(ctx, s.basePath, s.handler, s.version)
	if err != nil {
		return nil, err
	}

	var results []*chunk.Chunk
	var totalRows int

	for _, f := range m.Fragments {
		if f == nil {
			continue
		}

		// Check if this fragment can be pruned
		fragmentID := f.Id
		if s.canPruneFragment(selection, fragmentID) {
			continue // Skip this fragment
		}

		for _, df := range f.Files {
			if df == nil || df.Path == "" {
				continue
			}

			fullPath := filepath.Join(s.basePath, df.Path)
			c, err := ReadChunkFromFile(fullPath)
			if err != nil {
				return nil, err
			}

			// Apply filter if present
			if s.config.Filter != nil && s.config.Filter.CanPushdown() {
				c, err = s.applyFilter(c, s.config.Filter)
				if err != nil {
					return nil, err
				}
			}

			// Apply projection if present
			if len(s.config.Columns) > 0 {
				c = s.applyProjection(c, s.config.Columns)
			}

			if c.Card() > 0 {
				results = append(results, c)
				totalRows += c.Card()
			}

			// Check limit
			if s.config.Limit > 0 && totalRows >= s.config.Limit {
				return s.truncateToLimit(results), nil
			}
		}
	}

	return results, nil
}

// canPruneFragment checks if a fragment can be pruned based on index selection
func (s *IndexAwareScanner) canPruneFragment(selection *IndexSelection, fragmentID uint64) bool {
	// Check if fragment is in prunable list
	for _, fid := range selection.PrunableFragments {
		if fid == fragmentID {
			return true
		}
	}

	// Check fragment-specific indexes
	switch idx := selection.Index.(type) {
	case *ZoneMapIndex:
		// Use ZoneMap fragment-level pruning with the filter op/value
		if selection.FilterOp != "" && selection.FilterValue != nil {
			return idx.CanPruneFragment(fragmentID, selection.ColumnIdx, selection.FilterOp, selection.FilterValue)
		}
		return false

	case *BloomFilterIndex:
		// Already handled via PrunableFragments
		return false

	case *BitmapIndex:
		// Bitmap doesn't support fragment-level pruning directly
		return false
	}

	return false
}

// IndexStatisticsCollector collects statistics for index creation decisions
type IndexStatisticsCollector struct {
	columnStats map[int]*ColumnStatistics
}

// ColumnStatistics contains statistics for a column
type ColumnStatistics struct {
	ColumnIndex    int
	DistinctCount  uint64
	TotalCount     uint64
	NullCount      uint64
	MinValue       interface{}
	MaxValue       interface{}
	HasMinmax      bool
	ValueFrequency map[interface{}]uint64
}

// NewIndexStatisticsCollector creates a new statistics collector
func NewIndexStatisticsCollector() *IndexStatisticsCollector {
	return &IndexStatisticsCollector{
		columnStats: make(map[int]*ColumnStatistics),
	}
}

// CollectFromChunk collects statistics from a chunk
func (c *IndexStatisticsCollector) CollectFromChunk(chunk *chunk.Chunk) {
	for colIdx := 0; colIdx < chunk.ColumnCount(); colIdx++ {
		stats, exists := c.columnStats[colIdx]
		if !exists {
			stats = &ColumnStatistics{
				ColumnIndex:    colIdx,
				ValueFrequency: make(map[interface{}]uint64),
			}
			c.columnStats[colIdx] = stats
		}

		col := chunk.Data[colIdx]
		for row := 0; row < chunk.Card(); row++ {
			val := col.GetValue(row)
			stats.TotalCount++

			if val == nil {
				stats.NullCount++
				continue
			}

			// Extract Go value for tracking
			goVal := extractValueFromChunkValue(val)

			// Track distinct values (with limit to avoid memory issues)
			if len(stats.ValueFrequency) < 10000 {
				stats.ValueFrequency[goVal]++
			}

			// Update min/max
			if !stats.HasMinmax {
				stats.MinValue = goVal
				stats.MaxValue = goVal
				stats.HasMinmax = true
			} else {
				if compareInterfaceValues(goVal, stats.MinValue) < 0 {
					stats.MinValue = goVal
				}
				if compareInterfaceValues(goVal, stats.MaxValue) > 0 {
					stats.MaxValue = goVal
				}
			}
		}

		stats.DistinctCount = uint64(len(stats.ValueFrequency))
	}
}

// RecommendIndexes recommends index types for each column
func (c *IndexStatisticsCollector) RecommendIndexes() map[int][]IndexType {
	recommendations := make(map[int][]IndexType)

	for colIdx, stats := range c.columnStats {
		var types []IndexType

		// Calculate cardinality ratio
		cardinalityRatio := float64(stats.DistinctCount) / float64(stats.TotalCount)

		// Recommend Bitmap for low cardinality
		if cardinalityRatio < 0.1 && stats.DistinctCount < 100 {
			types = append(types, BitmapIndexType)
		}

		// Always recommend ZoneMap for numeric columns
		if stats.HasMinmax {
			types = append(types, ZoneMapIndexType)
		}

		// Recommend BloomFilter for high cardinality
		if cardinalityRatio > 0.5 {
			types = append(types, BloomFilterIndexType)
		}

		if len(types) > 0 {
			recommendations[colIdx] = types
		}
	}

	return recommendations
}

// GetColumnStatistics returns statistics for a column
func (c *IndexStatisticsCollector) GetColumnStatistics(colIdx int) *ColumnStatistics {
	return c.columnStats[colIdx]
}

// GetAllStatistics returns all column statistics
func (c *IndexStatisticsCollector) GetAllStatistics() map[int]*ColumnStatistics {
	return c.columnStats
}
