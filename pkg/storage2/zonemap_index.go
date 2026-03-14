package storage2

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/daviszhen/plan/pkg/common"
)

// ZoneMap contains min/max statistics for a column or a data fragment
// It is used for query pruning - if a query's filter doesn't overlap with
// the zone's min/max range, the entire zone can be skipped.
type ZoneMap struct {
	// Minimum value in the zone
	MinValue interface{}
	// Maximum value in the zone
	MaxValue interface{}
	// Number of rows in the zone
	RowCount uint64
	// Number of null values
	NullCount uint64
	// Number of distinct values (approximate)
	DistinctCount uint64
	// Data type of the column
	DataType common.LType
	// Whether the zone has been initialized
	Initialized bool
}

// ZoneMapIndex manages zone maps for multiple columns
// It can be used at different granularities: per fragment, per file, or per dataset
type ZoneMapIndex struct {
	name      string
	indexType IndexType

	// Zone maps by column index
	zoneMaps map[int]*ZoneMap

	// Zone maps by fragment ID (for finer granularity)
	// fragmentID -> columnIdx -> ZoneMap
	fragmentZoneMaps map[uint64]map[int]*ZoneMap

	mu    sync.RWMutex
	stats IndexStats
}

// ZoneMapIndexOption configures ZoneMapIndex creation
type ZoneMapIndexOption func(*ZoneMapIndex)

// WithZoneMapIndexName sets the index name
func WithZoneMapIndexName(name string) ZoneMapIndexOption {
	return func(zmi *ZoneMapIndex) {
		zmi.name = name
	}
}

// NewZoneMapIndex creates a new zone map index
func NewZoneMapIndex(opts ...ZoneMapIndexOption) *ZoneMapIndex {
	idx := &ZoneMapIndex{
		name:             "zonemap_idx",
		indexType:        ScalarIndex,
		zoneMaps:         make(map[int]*ZoneMap),
		fragmentZoneMaps: make(map[uint64]map[int]*ZoneMap),
		stats: IndexStats{
			IndexType: "zonemap",
		},
	}

	for _, opt := range opts {
		opt(idx)
	}

	return idx
}

// Name returns the index name
func (zmi *ZoneMapIndex) Name() string {
	zmi.mu.RLock()
	defer zmi.mu.RUnlock()
	return zmi.name
}

// Type returns the index type
func (zmi *ZoneMapIndex) Type() IndexType {
	return zmi.indexType
}

// Columns returns the column indices this index covers
func (zmi *ZoneMapIndex) Columns() []int {
	zmi.mu.RLock()
	defer zmi.mu.RUnlock()

	cols := make([]int, 0, len(zmi.zoneMaps))
	for col := range zmi.zoneMaps {
		cols = append(cols, col)
	}
	sort.Ints(cols)
	return cols
}

// Search searches the index and returns matching row IDs
// For ZoneMap, this returns all rows if the zone overlaps with the query
func (zmi *ZoneMapIndex) Search(ctx context.Context, query interface{}, limit int) ([]uint64, error) {
	// ZoneMap is primarily for pruning, not for exact row lookup
	// This method is not typically used for ZoneMap indexes
	return nil, fmt.Errorf("zone map index does not support direct search")
}

// Statistics returns statistics about the index
func (zmi *ZoneMapIndex) Statistics() IndexStats {
	zmi.mu.RLock()
	defer zmi.mu.RUnlock()
	return zmi.stats
}

// RangeQuery checks if the zone overlaps with a range query
func (zmi *ZoneMapIndex) RangeQuery(ctx context.Context, start, end interface{}) ([]uint64, error) {
	// ZoneMap is for pruning, not for returning row IDs
	return nil, fmt.Errorf("zone map index does not support range query returning row IDs")
}

// EqualityQuery checks if a value exists within the zone
func (zmi *ZoneMapIndex) EqualityQuery(ctx context.Context, value interface{}) ([]uint64, error) {
	// ZoneMap is for pruning, not for exact lookup
	return nil, fmt.Errorf("zone map index does not support equality query returning row IDs")
}

// ZoneMap operations

// GetZoneMap returns the zone map for a column
func (zmi *ZoneMapIndex) GetZoneMap(columnIdx int) *ZoneMap {
	zmi.mu.RLock()
	defer zmi.mu.RUnlock()
	return zmi.zoneMaps[columnIdx]
}

// GetFragmentZoneMap returns the zone map for a specific fragment and column
func (zmi *ZoneMapIndex) GetFragmentZoneMap(fragmentID uint64, columnIdx int) *ZoneMap {
	zmi.mu.RLock()
	defer zmi.mu.RUnlock()

	if fragMaps, exists := zmi.fragmentZoneMaps[fragmentID]; exists {
		return fragMaps[columnIdx]
	}
	return nil
}

// UpdateZoneMap updates the zone map for a column with a new value
func (zmi *ZoneMapIndex) UpdateZoneMap(columnIdx int, value interface{}, isNull bool) error {
	zmi.mu.Lock()
	defer zmi.mu.Unlock()

	// Get or create zone map
	zm, exists := zmi.zoneMaps[columnIdx]
	if !exists {
		zm = &ZoneMap{
			RowCount:    0,
			NullCount:   0,
			Initialized: false,
		}
		zmi.zoneMaps[columnIdx] = zm
	}

	// Update statistics
	zm.RowCount++

	if isNull {
		zm.NullCount++
		return nil
	}

	// Update min/max
	if !zm.Initialized {
		zm.MinValue = value
		zm.MaxValue = value
		zm.DistinctCount = 1
		zm.Initialized = true

		// Infer data type from value
		zm.DataType = inferDataType(value)
	} else {
		if zoneMapCompareValues(value, zm.MinValue) < 0 {
			zm.MinValue = value
		}
		if zoneMapCompareValues(value, zm.MaxValue) > 0 {
			zm.MaxValue = value
		}
		// Note: DistinctCount is approximate, we don't track exact distinct values
	}

	zmi.updateStats()
	return nil
}

// UpdateFragmentZoneMap updates the zone map for a specific fragment
func (zmi *ZoneMapIndex) UpdateFragmentZoneMap(fragmentID uint64, columnIdx int, value interface{}, isNull bool) error {
	zmi.mu.Lock()
	defer zmi.mu.Unlock()

	// Get or create fragment zone maps
	fragMaps, exists := zmi.fragmentZoneMaps[fragmentID]
	if !exists {
		fragMaps = make(map[int]*ZoneMap)
		zmi.fragmentZoneMaps[fragmentID] = fragMaps
	}

	// Get or create zone map for this column
	zm, exists := fragMaps[columnIdx]
	if !exists {
		zm = &ZoneMap{
			RowCount:    0,
			NullCount:   0,
			Initialized: false,
		}
		fragMaps[columnIdx] = zm
	}

	// Update statistics
	zm.RowCount++

	if isNull {
		zm.NullCount++
		return nil
	}

	// Update min/max
	if !zm.Initialized {
		zm.MinValue = value
		zm.MaxValue = value
		zm.DistinctCount = 1
		zm.Initialized = true
		zm.DataType = inferDataType(value)
	} else {
		if zoneMapCompareValues(value, zm.MinValue) < 0 {
			zm.MinValue = value
		}
		if zoneMapCompareValues(value, zm.MaxValue) > 0 {
			zm.MaxValue = value
		}
	}

	zmi.updateStats()
	return nil
}

// UpdateZoneMapBatch updates the zone map with multiple values at once
func (zmi *ZoneMapIndex) UpdateZoneMapBatch(columnIdx int, values []interface{}, nullCount uint64) error {
	zmi.mu.Lock()
	defer zmi.mu.Unlock()

	if len(values) == 0 {
		return nil
	}

	// Get or create zone map
	zm, exists := zmi.zoneMaps[columnIdx]
	if !exists {
		zm = &ZoneMap{
			RowCount:    0,
			NullCount:   0,
			Initialized: false,
		}
		zmi.zoneMaps[columnIdx] = zm
	}

	// Find min/max in batch
	var batchMin, batchMax interface{}
	first := true

	for _, value := range values {
		if value == nil {
			continue
		}

		if first {
			batchMin = value
			batchMax = value
			first = false
		} else {
			if zoneMapCompareValues(value, batchMin) < 0 {
				batchMin = value
			}
			if zoneMapCompareValues(value, batchMax) > 0 {
				batchMax = value
			}
		}
	}

	// Update zone map
	zm.RowCount += uint64(len(values)) + nullCount
	zm.NullCount += nullCount

	if batchMin != nil {
		if !zm.Initialized {
			zm.MinValue = batchMin
			zm.MaxValue = batchMax
			zm.DistinctCount = uint64(len(values)) // Approximate
			zm.Initialized = true
			zm.DataType = inferDataType(batchMin)
		} else {
			if zoneMapCompareValues(batchMin, zm.MinValue) < 0 {
				zm.MinValue = batchMin
			}
			if zoneMapCompareValues(batchMax, zm.MaxValue) > 0 {
				zm.MaxValue = batchMax
			}
		}
	}

	zmi.updateStats()
	return nil
}

// Pruning operations

// CanPrune checks if a query filter can be pruned based on zone map
// Returns true if the zone definitely contains no matching rows
func (zmi *ZoneMapIndex) CanPrune(columnIdx int, op string, value interface{}) bool {
	zmi.mu.RLock()
	defer zmi.mu.RUnlock()

	zm, exists := zmi.zoneMaps[columnIdx]
	if !exists || !zm.Initialized {
		return false // Cannot prune without statistics
	}

	switch op {
	case "=":
		// Equality: prune if value is outside [min, max]
		return zoneMapCompareValues(value, zm.MinValue) < 0 || zoneMapCompareValues(value, zm.MaxValue) > 0

	case "<":
		// Less than: prune if value <= min
		return zoneMapCompareValues(value, zm.MinValue) <= 0

	case "<=":
		// Less than or equal: prune if value < min
		return zoneMapCompareValues(value, zm.MinValue) < 0

	case ">":
		// Greater than: prune if value >= max
		return zoneMapCompareValues(value, zm.MaxValue) >= 0

	case ">=":
		// Greater than or equal: prune if value > max
		return zoneMapCompareValues(value, zm.MaxValue) > 0

	case "between":
		// Between is handled as range [start, end]
		// This is a simplified check
		return false

	default:
		return false
	}
}

// CanPruneRange checks if a range query can be pruned
func (zmi *ZoneMapIndex) CanPruneRange(columnIdx int, start, end interface{}) bool {
	zmi.mu.RLock()
	defer zmi.mu.RUnlock()

	zm, exists := zmi.zoneMaps[columnIdx]
	if !exists || !zm.Initialized {
		return false
	}

	// Range [start, end] doesn't overlap with [min, max] if:
	// end < min OR start > max
	return zoneMapCompareValues(end, zm.MinValue) < 0 || zoneMapCompareValues(start, zm.MaxValue) > 0
}

// CanPruneFragment checks if a specific fragment can be pruned
func (zmi *ZoneMapIndex) CanPruneFragment(fragmentID uint64, columnIdx int, op string, value interface{}) bool {
	zmi.mu.RLock()
	defer zmi.mu.RUnlock()

	fragMaps, exists := zmi.fragmentZoneMaps[fragmentID]
	if !exists {
		return false
	}

	zm, exists := fragMaps[columnIdx]
	if !exists || !zm.Initialized {
		return false
	}

	switch op {
	case "=":
		return zoneMapCompareValues(value, zm.MinValue) < 0 || zoneMapCompareValues(value, zm.MaxValue) > 0
	case "<":
		return zoneMapCompareValues(value, zm.MinValue) <= 0
	case "<=":
		return zoneMapCompareValues(value, zm.MinValue) < 0
	case ">":
		return zoneMapCompareValues(value, zm.MaxValue) >= 0
	case ">=":
		return zoneMapCompareValues(value, zm.MaxValue) > 0
	default:
		return false
	}
}

// GetPrunableFragments returns fragment IDs that can be pruned for a given query
func (zmi *ZoneMapIndex) GetPrunableFragments(columnIdx int, op string, value interface{}) []uint64 {
	zmi.mu.RLock()
	defer zmi.mu.RUnlock()

	var prunable []uint64

	for fragID, fragMaps := range zmi.fragmentZoneMaps {
		zm, exists := fragMaps[columnIdx]
		if !exists || !zm.Initialized {
			continue
		}

		shouldPrune := false
		switch op {
		case "=":
			shouldPrune = zoneMapCompareValues(value, zm.MinValue) < 0 || zoneMapCompareValues(value, zm.MaxValue) > 0
		case "<":
			shouldPrune = zoneMapCompareValues(value, zm.MinValue) <= 0
		case "<=":
			shouldPrune = zoneMapCompareValues(value, zm.MinValue) < 0
		case ">":
			shouldPrune = zoneMapCompareValues(value, zm.MaxValue) >= 0
		case ">=":
			shouldPrune = zoneMapCompareValues(value, zm.MaxValue) > 0
		}

		if shouldPrune {
			prunable = append(prunable, fragID)
		}
	}

	return prunable
}

// Merge operations

// MergeZoneMaps merges multiple zone maps into one
func MergeZoneMaps(zms ...*ZoneMap) *ZoneMap {
	if len(zms) == 0 {
		return nil
	}

	result := &ZoneMap{
		Initialized: false,
	}

	for _, zm := range zms {
		if zm == nil {
			continue
		}

		result.RowCount += zm.RowCount
		result.NullCount += zm.NullCount

		if zm.Initialized {
			if !result.Initialized {
				result.MinValue = zm.MinValue
				result.MaxValue = zm.MaxValue
				result.DataType = zm.DataType
				result.Initialized = true
			} else {
				if zoneMapCompareValues(zm.MinValue, result.MinValue) < 0 {
					result.MinValue = zm.MinValue
				}
				if zoneMapCompareValues(zm.MaxValue, result.MaxValue) > 0 {
					result.MaxValue = zm.MaxValue
				}
			}
		}
	}

	return result
}

// Serialization

// MarshalBinary serializes the zone map index to binary format
func (zmi *ZoneMapIndex) MarshalBinary() ([]byte, error) {
	zmi.mu.RLock()
	defer zmi.mu.RUnlock()

	// Format:
	// [num_columns][column_entries][num_fragments][fragment_entries]

	var buf []byte

	// Number of columns (4 bytes)
	numCols := uint32(len(zmi.zoneMaps))
	colCountBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(colCountBuf, numCols)
	buf = append(buf, colCountBuf...)

	// Column entries
	for colIdx, zm := range zmi.zoneMaps {
		// Column index (4 bytes)
		colBuf := make([]byte, 4)
		binary.LittleEndian.PutUint32(colBuf, uint32(colIdx))
		buf = append(buf, colBuf...)

		// Zone map data
		zmData, err := zm.MarshalBinary()
		if err != nil {
			return nil, err
		}
		zmLenBuf := make([]byte, 4)
		binary.LittleEndian.PutUint32(zmLenBuf, uint32(len(zmData)))
		buf = append(buf, zmLenBuf...)
		buf = append(buf, zmData...)
	}

	// Number of fragments (4 bytes)
	numFrags := uint32(len(zmi.fragmentZoneMaps))
	fragCountBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(fragCountBuf, numFrags)
	buf = append(buf, fragCountBuf...)

	// Fragment entries
	for fragID, fragMaps := range zmi.fragmentZoneMaps {
		// Fragment ID (8 bytes)
		fragIDBuf := make([]byte, 8)
		binary.LittleEndian.PutUint64(fragIDBuf, fragID)
		buf = append(buf, fragIDBuf...)

		// Number of columns in this fragment (4 bytes)
		numFragCols := uint32(len(fragMaps))
		numFragColsBuf := make([]byte, 4)
		binary.LittleEndian.PutUint32(numFragColsBuf, numFragCols)
		buf = append(buf, numFragColsBuf...)

		// Fragment zone maps
		for colIdx, zm := range fragMaps {
			// Column index
			colBuf := make([]byte, 4)
			binary.LittleEndian.PutUint32(colBuf, uint32(colIdx))
			buf = append(buf, colBuf...)

			// Zone map data
			zmData, err := zm.MarshalBinary()
			if err != nil {
				return nil, err
			}
			zmLenBuf := make([]byte, 4)
			binary.LittleEndian.PutUint32(zmLenBuf, uint32(len(zmData)))
			buf = append(buf, zmLenBuf...)
			buf = append(buf, zmData...)
		}
	}

	return buf, nil
}

// UnmarshalBinary deserializes the zone map index from binary format
func (zmi *ZoneMapIndex) UnmarshalBinary(data []byte) error {
	zmi.mu.Lock()
	defer zmi.mu.Unlock()

	if len(data) < 4 {
		return fmt.Errorf("insufficient data for zone map index")
	}

	offset := 0

	// Number of columns
	numCols := binary.LittleEndian.Uint32(data[offset : offset+4])
	offset += 4

	zmi.zoneMaps = make(map[int]*ZoneMap)

	// Read column entries
	for i := uint32(0); i < numCols; i++ {
		if offset+4 > len(data) {
			return fmt.Errorf("unexpected end of data reading column index")
		}

		colIdx := int(binary.LittleEndian.Uint32(data[offset : offset+4]))
		offset += 4

		if offset+4 > len(data) {
			return fmt.Errorf("unexpected end of data reading zone map length")
		}

		zmLen := binary.LittleEndian.Uint32(data[offset : offset+4])
		offset += 4

		if offset+int(zmLen) > len(data) {
			return fmt.Errorf("unexpected end of data reading zone map")
		}

		zm := &ZoneMap{}
		if err := zm.UnmarshalBinary(data[offset : offset+int(zmLen)]); err != nil {
			return err
		}
		offset += int(zmLen)

		zmi.zoneMaps[colIdx] = zm
	}

	// Number of fragments
	if offset+4 > len(data) {
		return fmt.Errorf("unexpected end of data reading fragment count")
	}

	numFrags := binary.LittleEndian.Uint32(data[offset : offset+4])
	offset += 4

	zmi.fragmentZoneMaps = make(map[uint64]map[int]*ZoneMap)

	// Read fragment entries
	for i := uint32(0); i < numFrags; i++ {
		if offset+8 > len(data) {
			return fmt.Errorf("unexpected end of data reading fragment ID")
		}

		fragID := binary.LittleEndian.Uint64(data[offset : offset+8])
		offset += 8

		if offset+4 > len(data) {
			return fmt.Errorf("unexpected end of data reading fragment column count")
		}

		numFragCols := binary.LittleEndian.Uint32(data[offset : offset+4])
		offset += 4

		fragMaps := make(map[int]*ZoneMap)

		for j := uint32(0); j < numFragCols; j++ {
			if offset+4 > len(data) {
				return fmt.Errorf("unexpected end of data reading fragment column index")
			}

			colIdx := int(binary.LittleEndian.Uint32(data[offset : offset+4]))
			offset += 4

			if offset+4 > len(data) {
				return fmt.Errorf("unexpected end of data reading fragment zone map length")
			}

			zmLen := binary.LittleEndian.Uint32(data[offset : offset+4])
			offset += 4

			if offset+int(zmLen) > len(data) {
				return fmt.Errorf("unexpected end of data reading fragment zone map")
			}

			zm := &ZoneMap{}
			if err := zm.UnmarshalBinary(data[offset : offset+int(zmLen)]); err != nil {
				return err
			}
			offset += int(zmLen)

			fragMaps[colIdx] = zm
		}

		zmi.fragmentZoneMaps[fragID] = fragMaps
	}

	zmi.updateStats()
	return nil
}

// ZoneMap serialization

// MarshalBinary serializes a zone map to binary format
func (zm *ZoneMap) MarshalBinary() ([]byte, error) {
	// Format:
	// [initialized][row_count][null_count][distinct_count][data_type][min_value][max_value]

	var buf []byte

	// Initialized flag (1 byte)
	initialized := byte(0)
	if zm.Initialized {
		initialized = 1
	}
	buf = append(buf, initialized)

	// Row count (8 bytes)
	rowCountBuf := make([]byte, 8)
	binary.LittleEndian.PutUint64(rowCountBuf, zm.RowCount)
	buf = append(buf, rowCountBuf...)

	// Null count (8 bytes)
	nullCountBuf := make([]byte, 8)
	binary.LittleEndian.PutUint64(nullCountBuf, zm.NullCount)
	buf = append(buf, nullCountBuf...)

	// Distinct count (8 bytes)
	distinctBuf := make([]byte, 8)
	binary.LittleEndian.PutUint64(distinctBuf, zm.DistinctCount)
	buf = append(buf, distinctBuf...)

	// Data type (4 bytes for LType ID)
	dataTypeBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(dataTypeBuf, uint32(zm.DataType.Id))
	buf = append(buf, dataTypeBuf...)

	// Min/Max values (only if initialized)
	if zm.Initialized {
		minData, err := serializeValue(zm.MinValue)
		if err != nil {
			return nil, err
		}
		minLenBuf := make([]byte, 4)
		binary.LittleEndian.PutUint32(minLenBuf, uint32(len(minData)))
		buf = append(buf, minLenBuf...)
		buf = append(buf, minData...)

		maxData, err := serializeValue(zm.MaxValue)
		if err != nil {
			return nil, err
		}
		maxLenBuf := make([]byte, 4)
		binary.LittleEndian.PutUint32(maxLenBuf, uint32(len(maxData)))
		buf = append(buf, maxLenBuf...)
		buf = append(buf, maxData...)
	}

	return buf, nil
}

// UnmarshalBinary deserializes a zone map from binary format
func (zm *ZoneMap) UnmarshalBinary(data []byte) error {
	if len(data) < 25 { // minimum size
		return fmt.Errorf("insufficient data for zone map")
	}

	offset := 0

	// Initialized flag
	zm.Initialized = data[offset] != 0
	offset++

	// Row count
	zm.RowCount = binary.LittleEndian.Uint64(data[offset : offset+8])
	offset += 8

	// Null count
	zm.NullCount = binary.LittleEndian.Uint64(data[offset : offset+8])
	offset += 8

	// Distinct count
	zm.DistinctCount = binary.LittleEndian.Uint64(data[offset : offset+8])
	offset += 8

	// Data type
	typeID := binary.LittleEndian.Uint32(data[offset : offset+4])
	zm.DataType = common.MakeLType(common.LTypeId(typeID))
	offset += 4

	// Min/Max values
	if zm.Initialized {
		if offset+4 > len(data) {
			return fmt.Errorf("unexpected end of data reading min value length")
		}

		minLen := binary.LittleEndian.Uint32(data[offset : offset+4])
		offset += 4

		if offset+int(minLen) > len(data) {
			return fmt.Errorf("unexpected end of data reading min value")
		}

		var err error
		zm.MinValue, err = deserializeValue(data[offset:offset+int(minLen)], zm.DataType)
		if err != nil {
			return err
		}
		offset += int(minLen)

		if offset+4 > len(data) {
			return fmt.Errorf("unexpected end of data reading max value length")
		}

		maxLen := binary.LittleEndian.Uint32(data[offset : offset+4])
		offset += 4

		if offset+int(maxLen) > len(data) {
			return fmt.Errorf("unexpected end of data reading max value")
		}

		zm.MaxValue, err = deserializeValue(data[offset:offset+int(maxLen)], zm.DataType)
		if err != nil {
			return err
		}
	}

	return nil
}

// Helper functions

// updateStats updates index statistics
func (zmi *ZoneMapIndex) updateStats() {
	totalRows := uint64(0)
	totalSize := uint64(0)

	for _, zm := range zmi.zoneMaps {
		totalRows += zm.RowCount
		totalSize += 100 // Approximate size per zone map
	}

	for _, fragMaps := range zmi.fragmentZoneMaps {
		for range fragMaps {
			totalSize += 100
		}
	}

	zmi.stats.NumEntries = totalRows
	zmi.stats.SizeBytes = totalSize
}

// inferDataType infers the LType from a Go value
func inferDataType(value interface{}) common.LType {
	switch v := value.(type) {
	case bool:
		return common.MakeLType(common.LTID_BOOLEAN)
	case int8:
		return common.MakeLType(common.LTID_TINYINT)
	case int16:
		return common.MakeLType(common.LTID_SMALLINT)
	case int32:
		return common.MakeLType(common.LTID_INTEGER)
	case int64:
		return common.MakeLType(common.LTID_BIGINT)
	case float32:
		return common.MakeLType(common.LTID_FLOAT)
	case float64:
		return common.MakeLType(common.LTID_DOUBLE)
	case string:
		return common.MakeLType(common.LTID_VARCHAR)
	case time.Time:
		return common.MakeLType(common.LTID_DATE)
	default:
		_ = v // suppress unused variable warning
		return common.MakeLType(common.LTID_VARCHAR)
	}
}

// zoneMapCompareValues compares two values and returns:
// -1 if a < b
//
//	0 if a == b
//	1 if a > b
func zoneMapCompareValues(a, b interface{}) int {
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}

	switch av := a.(type) {
	case bool:
		bv, ok := b.(bool)
		if !ok {
			return 0
		}
		if av == bv {
			return 0
		}
		if !av && bv {
			return -1
		}
		return 1

	case int8:
		bv, ok := b.(int8)
		if !ok {
			return 0
		}
		if av < bv {
			return -1
		} else if av > bv {
			return 1
		}
		return 0

	case int16:
		bv, ok := b.(int16)
		if !ok {
			return 0
		}
		if av < bv {
			return -1
		} else if av > bv {
			return 1
		}
		return 0

	case int32:
		bv, ok := b.(int32)
		if !ok {
			return 0
		}
		if av < bv {
			return -1
		} else if av > bv {
			return 1
		}
		return 0

	case int64:
		bv, ok := b.(int64)
		if !ok {
			return 0
		}
		if av < bv {
			return -1
		} else if av > bv {
			return 1
		}
		return 0

	case uint8:
		bv, ok := b.(uint8)
		if !ok {
			return 0
		}
		if av < bv {
			return -1
		} else if av > bv {
			return 1
		}
		return 0

	case uint16:
		bv, ok := b.(uint16)
		if !ok {
			return 0
		}
		if av < bv {
			return -1
		} else if av > bv {
			return 1
		}
		return 0

	case uint32:
		bv, ok := b.(uint32)
		if !ok {
			return 0
		}
		if av < bv {
			return -1
		} else if av > bv {
			return 1
		}
		return 0

	case uint64:
		bv, ok := b.(uint64)
		if !ok {
			return 0
		}
		if av < bv {
			return -1
		} else if av > bv {
			return 1
		}
		return 0

	case float32:
		bv, ok := b.(float32)
		if !ok {
			return 0
		}
		if av < bv {
			return -1
		} else if av > bv {
			return 1
		}
		return 0

	case float64:
		bv, ok := b.(float64)
		if !ok {
			return 0
		}
		if av < bv {
			return -1
		} else if av > bv {
			return 1
		}
		return 0

	case string:
		bv, ok := b.(string)
		if !ok {
			return 0
		}
		if av < bv {
			return -1
		} else if av > bv {
			return 1
		}
		return 0

	case time.Time:
		bv, ok := b.(time.Time)
		if !ok {
			return 0
		}
		if av.Before(bv) {
			return -1
		} else if av.After(bv) {
			return 1
		}
		return 0

	default:
		// Fallback to string comparison
		as := fmt.Sprintf("%v", a)
		bs := fmt.Sprintf("%v", b)
		if as < bs {
			return -1
		} else if as > bs {
			return 1
		}
		return 0
	}
}

// serializeValue serializes a value to bytes
func serializeValue(value interface{}) ([]byte, error) {
	switch v := value.(type) {
	case bool:
		if v {
			return []byte{1}, nil
		}
		return []byte{0}, nil

	case int8:
		return []byte{byte(v)}, nil

	case int16:
		buf := make([]byte, 2)
		binary.LittleEndian.PutUint16(buf, uint16(v))
		return buf, nil

	case int32:
		buf := make([]byte, 4)
		binary.LittleEndian.PutUint32(buf, uint32(v))
		return buf, nil

	case int64:
		buf := make([]byte, 8)
		binary.LittleEndian.PutUint64(buf, uint64(v))
		return buf, nil

	case uint8:
		return []byte{v}, nil

	case uint16:
		buf := make([]byte, 2)
		binary.LittleEndian.PutUint16(buf, v)
		return buf, nil

	case uint32:
		buf := make([]byte, 4)
		binary.LittleEndian.PutUint32(buf, v)
		return buf, nil

	case uint64:
		buf := make([]byte, 8)
		binary.LittleEndian.PutUint64(buf, v)
		return buf, nil

	case float32:
		buf := make([]byte, 4)
		binary.LittleEndian.PutUint32(buf, math.Float32bits(v))
		return buf, nil

	case float64:
		buf := make([]byte, 8)
		binary.LittleEndian.PutUint64(buf, math.Float64bits(v))
		return buf, nil

	case string:
		return []byte(v), nil

	case time.Time:
		// Store as Unix timestamp
		buf := make([]byte, 8)
		binary.LittleEndian.PutUint64(buf, uint64(v.Unix()))
		return buf, nil

	default:
		return nil, fmt.Errorf("unsupported value type: %T", value)
	}
}

// deserializeValue deserializes a value from bytes
func deserializeValue(data []byte, dataType common.LType) (interface{}, error) {
	typeID := dataType.Id

	switch typeID {
	case common.LTID_BOOLEAN:
		if len(data) < 1 {
			return nil, fmt.Errorf("insufficient data for boolean")
		}
		return data[0] != 0, nil

	case common.LTID_TINYINT:
		if len(data) < 1 {
			return nil, fmt.Errorf("insufficient data for tinyint")
		}
		return int8(data[0]), nil

	case common.LTID_SMALLINT:
		if len(data) < 2 {
			return nil, fmt.Errorf("insufficient data for smallint")
		}
		return int16(binary.LittleEndian.Uint16(data)), nil

	case common.LTID_INTEGER:
		if len(data) < 4 {
			return nil, fmt.Errorf("insufficient data for integer")
		}
		return int32(binary.LittleEndian.Uint32(data)), nil

	case common.LTID_BIGINT:
		if len(data) < 8 {
			return nil, fmt.Errorf("insufficient data for bigint")
		}
		return int64(binary.LittleEndian.Uint64(data)), nil

	case common.LTID_FLOAT:
		if len(data) < 4 {
			return nil, fmt.Errorf("insufficient data for float")
		}
		return math.Float32frombits(binary.LittleEndian.Uint32(data)), nil

	case common.LTID_DOUBLE:
		if len(data) < 8 {
			return nil, fmt.Errorf("insufficient data for double")
		}
		return math.Float64frombits(binary.LittleEndian.Uint64(data)), nil

	case common.LTID_VARCHAR:
		return string(data), nil

	case common.LTID_DATE:
		if len(data) < 8 {
			return nil, fmt.Errorf("insufficient data for date")
		}
		ts := int64(binary.LittleEndian.Uint64(data))
		return time.Unix(ts, 0), nil

	default:
		return nil, fmt.Errorf("unsupported data type: %d", typeID)
	}
}

// Ensure ZoneMapIndex implements ScalarIndexImpl
var _ ScalarIndexImpl = (*ZoneMapIndex)(nil)
