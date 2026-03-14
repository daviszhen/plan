package storage2

import (
	"context"
	"encoding/binary"
	"fmt"
	"math/bits"
	"sort"
	"sync"
)

// BitmapIndex represents a bitmap index for efficient boolean and enumerated value filtering.
// It uses compressed bitmaps (similar to Roaring Bitmaps) for space efficiency.
type BitmapIndex struct {
	name      string
	columnIdx int
	indexType IndexType

	// For each distinct value, we store a compressed bitmap of row positions
	valueBitmaps map[string]*CompressedBitmap

	// Statistics
	rowCount       uint64
	distinctValues uint64
	nullCount      uint64
	hasNullBitmap  *CompressedBitmap // bitmap of rows with null values

	mu    sync.RWMutex
	stats IndexStats
}

// CompressedBitmap implements a simple compressed bitmap using 64-bit words
// This is a simplified version inspired by Roaring Bitmaps
type CompressedBitmap struct {
	// Array of 64-bit words representing the bitmap
	words []uint64

	// Number of set bits (cardinality)
	cardinality uint64

	// Maximum bit position set
	maxBit uint64
}

// BitmapIndexOption configures BitmapIndex creation
type BitmapIndexOption func(*BitmapIndex)

// WithBitmapIndexName sets the index name
func WithBitmapIndexName(name string) BitmapIndexOption {
	return func(bi *BitmapIndex) {
		bi.name = name
	}
}

// NewBitmapIndex creates a new bitmap index for a column
func NewBitmapIndex(columnIdx int, opts ...BitmapIndexOption) *BitmapIndex {
	idx := &BitmapIndex{
		name:         fmt.Sprintf("bitmap_idx_%d", columnIdx),
		columnIdx:    columnIdx,
		indexType:    ScalarIndex,
		valueBitmaps: make(map[string]*CompressedBitmap),
		stats: IndexStats{
			IndexType: "bitmap",
			DataType:  "boolean/enum", // Will be updated when data is added
		},
	}

	for _, opt := range opts {
		opt(idx)
	}

	return idx
}

// Name returns the index name
func (bi *BitmapIndex) Name() string {
	bi.mu.RLock()
	defer bi.mu.RUnlock()
	return bi.name
}

// Type returns the index type
func (bi *BitmapIndex) Type() IndexType {
	return bi.indexType
}

// Columns returns the column indices this index covers
func (bi *BitmapIndex) Columns() []int {
	return []int{bi.columnIdx}
}

// Search searches the index and returns matching row IDs
func (bi *BitmapIndex) Search(ctx context.Context, query interface{}, limit int) ([]uint64, error) {
	bi.mu.RLock()
	defer bi.mu.RUnlock()

	// For bitmap index, we interpret query as value equality
	queryStr := fmt.Sprintf("%v", query)

	bitmap, exists := bi.valueBitmaps[queryStr]
	if !exists {
		return nil, nil
	}

	rowIDs := bitmap.ToRowIDs()

	// Apply limit if specified
	if limit > 0 && len(rowIDs) > limit {
		rowIDs = rowIDs[:limit]
	}

	return rowIDs, nil
}

// Statistics returns statistics about the index
func (bi *BitmapIndex) Statistics() IndexStats {
	bi.mu.RLock()
	defer bi.mu.RUnlock()
	return bi.stats
}

// RangeQuery is not typically used for bitmap indexes
func (bi *BitmapIndex) RangeQuery(ctx context.Context, start, end interface{}) ([]uint64, error) {
	// Bitmap indexes are optimized for equality queries, not range queries
	// For range queries, other index types (B-tree, ZoneMap) are more appropriate
	return nil, fmt.Errorf("range query not supported for bitmap index")
}

// EqualityQuery performs an equality query on the index
func (bi *BitmapIndex) EqualityQuery(ctx context.Context, value interface{}) ([]uint64, error) {
	return bi.Search(ctx, value, 0)
}

// Insert adds a value and its row ID to the index
func (bi *BitmapIndex) Insert(value interface{}, rowID uint64) error {
	bi.mu.Lock()
	defer bi.mu.Unlock()

	valueStr := fmt.Sprintf("%v", value)

	// Handle null values specially
	if valueStr == "<nil>" {
		if bi.hasNullBitmap == nil {
			bi.hasNullBitmap = NewCompressedBitmap()
		}
		bi.hasNullBitmap.Set(rowID)
		bi.nullCount++
	} else {
		// Get or create bitmap for this value
		bitmap, exists := bi.valueBitmaps[valueStr]
		if !exists {
			bitmap = NewCompressedBitmap()
			bi.valueBitmaps[valueStr] = bitmap
			bi.distinctValues++
		}

		bitmap.Set(rowID)
	}

	// Update row count
	if rowID >= bi.rowCount {
		bi.rowCount = rowID + 1
	}

	bi.updateStats()
	return nil
}

// InsertBatch efficiently inserts multiple values at once
func (bi *BitmapIndex) InsertBatch(values []interface{}, rowIDs []uint64) error {
	if len(values) != len(rowIDs) {
		return fmt.Errorf("values and rowIDs length mismatch")
	}

	bi.mu.Lock()
	defer bi.mu.Unlock()

	for i, value := range values {
		valueStr := fmt.Sprintf("%v", value)
		rowID := rowIDs[i]

		if valueStr == "&lt;nil&gt;" {
			if bi.hasNullBitmap == nil {
				bi.hasNullBitmap = NewCompressedBitmap()
			}
			bi.hasNullBitmap.Set(rowID)
			bi.nullCount++
		} else {
			bitmap, exists := bi.valueBitmaps[valueStr]
			if !exists {
				bitmap = NewCompressedBitmap()
				bi.valueBitmaps[valueStr] = bitmap
				bi.distinctValues++
			}
			bitmap.Set(rowID)
		}

		if rowID >= bi.rowCount {
			bi.rowCount = rowID + 1
		}
	}

	bi.updateStats()
	return nil
}

// Bitmap operations for combining results

// And performs bitwise AND operation between two bitmaps
func (bi *BitmapIndex) And(bitmap1, bitmap2 *CompressedBitmap) *CompressedBitmap {
	if bitmap1 == nil || bitmap2 == nil {
		return NewCompressedBitmap()
	}

	result := NewCompressedBitmap()
	maxWords := len(bitmap1.words)
	if len(bitmap2.words) > maxWords {
		maxWords = len(bitmap2.words)
	}

	result.words = make([]uint64, maxWords)
	for i := 0; i < maxWords; i++ {
		word1 := uint64(0)
		word2 := uint64(0)
		if i < len(bitmap1.words) {
			word1 = bitmap1.words[i]
		}
		if i < len(bitmap2.words) {
			word2 = bitmap2.words[i]
		}
		result.words[i] = word1 & word2
		result.cardinality += uint64(bits.OnesCount64(result.words[i]))
	}

	result.maxBit = max(bitmap1.maxBit, bitmap2.maxBit)
	return result
}

// Or performs bitwise OR operation between two bitmaps
func (bi *BitmapIndex) Or(bitmap1, bitmap2 *CompressedBitmap) *CompressedBitmap {
	if bitmap1 == nil && bitmap2 == nil {
		return NewCompressedBitmap()
	}
	if bitmap1 == nil {
		return bitmap2.Clone()
	}
	if bitmap2 == nil {
		return bitmap1.Clone()
	}

	result := NewCompressedBitmap()
	maxWords := len(bitmap1.words)
	if len(bitmap2.words) > maxWords {
		maxWords = len(bitmap2.words)
	}

	result.words = make([]uint64, maxWords)
	for i := 0; i < maxWords; i++ {
		word1 := uint64(0)
		word2 := uint64(0)
		if i < len(bitmap1.words) {
			word1 = bitmap1.words[i]
		}
		if i < len(bitmap2.words) {
			word2 = bitmap2.words[i]
		}
		result.words[i] = word1 | word2
		result.cardinality += uint64(bits.OnesCount64(result.words[i]))
	}

	result.maxBit = max(bitmap1.maxBit, bitmap2.maxBit)
	return result
}

// Not performs bitwise NOT operation (complement)
func (bi *BitmapIndex) Not(bitmap *CompressedBitmap) *CompressedBitmap {
	if bitmap == nil {
		// Return bitmap with all bits set up to rowCount
		result := NewCompressedBitmap()
		for i := uint64(0); i < bi.rowCount; i++ {
			result.Set(i)
		}
		return result
	}

	result := NewCompressedBitmap()
	wordsNeeded := (bi.rowCount + 63) / 64

	result.words = make([]uint64, wordsNeeded)
	for i := uint64(0); i < wordsNeeded; i++ {
		word := uint64(0)
		if i < uint64(len(bitmap.words)) {
			word = bitmap.words[i]
		}
		result.words[i] = ^word
	}

	// Clear bits beyond rowCount
	if bi.rowCount%64 != 0 {
		lastWordIdx := wordsNeeded - 1
		mask := uint64(1)<<(bi.rowCount%64) - 1
		result.words[lastWordIdx] &= mask
	}

	// Calculate cardinality
	for _, word := range result.words {
		result.cardinality += uint64(bits.OnesCount64(word))
	}

	result.maxBit = bi.rowCount - 1
	return result
}

// GetBitmap returns the bitmap for a specific value
func (bi *BitmapIndex) GetBitmap(value interface{}) *CompressedBitmap {
	bi.mu.RLock()
	defer bi.mu.RUnlock()

	valueStr := fmt.Sprintf("%v", value)
	if bitmap, exists := bi.valueBitmaps[valueStr]; exists {
		return bitmap
	}
	return nil
}

// GetValueCardinality returns the number of rows for a specific value
func (bi *BitmapIndex) GetValueCardinality(value interface{}) uint64 {
	bitmap := bi.GetBitmap(value)
	if bitmap == nil {
		return 0
	}
	return bitmap.cardinality
}

// ListValues returns all distinct values in the index
func (bi *BitmapIndex) ListValues() []string {
	bi.mu.RLock()
	defer bi.mu.RUnlock()

	values := make([]string, 0, len(bi.valueBitmaps))
	for value := range bi.valueBitmaps {
		values = append(values, value)
	}
	sort.Strings(values)
	return values
}

// updateStats updates index statistics
func (bi *BitmapIndex) updateStats() {
	totalBits := uint64(0)
	totalSize := uint64(0)

	for _, bitmap := range bi.valueBitmaps {
		totalBits += bitmap.cardinality
		totalSize += uint64(len(bitmap.words) * 8) // 8 bytes per uint64
	}

	if bi.hasNullBitmap != nil {
		totalSize += uint64(len(bi.hasNullBitmap.words) * 8)
	}

	bi.stats.NumEntries = bi.rowCount
	bi.stats.SizeBytes = totalSize
	bi.stats.DistinctValues = bi.distinctValues
	bi.stats.NullCount = bi.nullCount

	// Estimate compression ratio
	if bi.rowCount > 0 {
		uncompressedSize := bi.rowCount / 8 // bits to bytes
		if uncompressedSize > 0 {
			compressionRatio := float64(uncompressedSize) / float64(totalSize)
			// For now, just update stats without storing ratio
			_ = compressionRatio // suppress unused variable warning
		}
	}
}

// Serialization methods

// MarshalBinary serializes the bitmap index to binary format
func (bi *BitmapIndex) MarshalBinary() ([]byte, error) {
	bi.mu.RLock()
	defer bi.mu.RUnlock()

	// Simple binary format:
	// [header][value_count][value_entries][null_bitmap]

	var buf []byte

	// Header: rowCount, distinctValues, nullCount (24 bytes)
	header := make([]byte, 24)
	binary.LittleEndian.PutUint64(header[0:8], bi.rowCount)
	binary.LittleEndian.PutUint64(header[8:16], bi.distinctValues)
	binary.LittleEndian.PutUint64(header[16:24], bi.nullCount)
	buf = append(buf, header...)

	// Value count (4 bytes)
	valueCount := uint32(len(bi.valueBitmaps))
	countBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(countBuf, valueCount)
	buf = append(buf, countBuf...)

	// Value entries: [value_len][value][bitmap_data_len][bitmap_data]
	for value, bitmap := range bi.valueBitmaps {
		// Value length and data
		valueLen := uint32(len(value))
		lenBuf := make([]byte, 4)
		binary.LittleEndian.PutUint32(lenBuf, valueLen)
		buf = append(buf, lenBuf...)
		buf = append(buf, []byte(value)...)

		// Bitmap data
		bitmapData, err := bitmap.MarshalBinary()
		if err != nil {
			return nil, err
		}
		dataLen := uint32(len(bitmapData))
		dataLenBuf := make([]byte, 4)
		binary.LittleEndian.PutUint32(dataLenBuf, dataLen)
		buf = append(buf, dataLenBuf...)
		buf = append(buf, bitmapData...)
	}

	// Null bitmap (if exists)
	if bi.hasNullBitmap != nil {
		nullData, err := bi.hasNullBitmap.MarshalBinary()
		if err != nil {
			return nil, err
		}
		nullFlag := []byte{1} // 1 byte flag indicating null bitmap exists
		buf = append(buf, nullFlag...)
		buf = append(buf, nullData...)
	} else {
		nullFlag := []byte{0} // 0 byte flag indicating no null bitmap
		buf = append(buf, nullFlag...)
	}

	return buf, nil
}

// UnmarshalBinary deserializes the bitmap index from binary format
func (bi *BitmapIndex) UnmarshalBinary(data []byte) error {
	bi.mu.Lock()
	defer bi.mu.Unlock()

	if len(data) < 28 { // minimum header size
		return fmt.Errorf("insufficient data for bitmap index")
	}

	// Parse header
	bi.rowCount = binary.LittleEndian.Uint64(data[0:8])
	bi.distinctValues = binary.LittleEndian.Uint64(data[8:16])
	bi.nullCount = binary.LittleEndian.Uint64(data[16:24])

	valueCount := binary.LittleEndian.Uint32(data[24:28])
	offset := 28

	bi.valueBitmaps = make(map[string]*CompressedBitmap)

	// Parse value entries
	for i := uint32(0); i < valueCount; i++ {
		if offset+4 > len(data) {
			return fmt.Errorf("unexpected end of data while reading value length")
		}

		valueLen := binary.LittleEndian.Uint32(data[offset : offset+4])
		offset += 4

		if offset+int(valueLen)+4 > len(data) {
			return fmt.Errorf("unexpected end of data while reading value")
		}

		value := string(data[offset : offset+int(valueLen)])
		offset += int(valueLen)

		dataLen := binary.LittleEndian.Uint32(data[offset : offset+4])
		offset += 4

		if offset+int(dataLen) > len(data) {
			return fmt.Errorf("unexpected end of data while reading bitmap")
		}

		bitmap := NewCompressedBitmap()
		if err := bitmap.UnmarshalBinary(data[offset : offset+int(dataLen)]); err != nil {
			return err
		}

		bi.valueBitmaps[value] = bitmap
		offset += int(dataLen)
	}

	// Parse null bitmap
	if offset >= len(data) {
		return fmt.Errorf("missing null bitmap flag")
	}

	hasNull := data[offset] != 0
	offset++

	if hasNull {
		if offset >= len(data) {
			return fmt.Errorf("missing null bitmap data")
		}
		bi.hasNullBitmap = NewCompressedBitmap()
		// Rest of data is null bitmap
		if err := bi.hasNullBitmap.UnmarshalBinary(data[offset:]); err != nil {
			return err
		}
	}

	bi.updateStats()
	return nil
}

// CompressedBitmap methods

// NewCompressedBitmap creates a new compressed bitmap
func NewCompressedBitmap() *CompressedBitmap {
	return &CompressedBitmap{
		words: make([]uint64, 0),
	}
}

// Set sets the bit at the given position
func (cb *CompressedBitmap) Set(bit uint64) {
	wordIdx := bit / 64
	bitIdx := bit % 64

	// Extend words slice if needed
	for uint64(len(cb.words)) <= wordIdx {
		cb.words = append(cb.words, 0)
	}

	// Set the bit
	if cb.words[wordIdx]&(1<<bitIdx) == 0 {
		cb.words[wordIdx] |= 1 << bitIdx
		cb.cardinality++
	}

	if bit > cb.maxBit {
		cb.maxBit = bit
	}
}

// Get returns whether the bit at the given position is set
func (cb *CompressedBitmap) Get(bit uint64) bool {
	wordIdx := bit / 64
	bitIdx := bit % 64

	if wordIdx >= uint64(len(cb.words)) {
		return false
	}

	return cb.words[wordIdx]&(1<<bitIdx) != 0
}

// ToRowIDs converts the bitmap to a sorted slice of row IDs
func (cb *CompressedBitmap) ToRowIDs() []uint64 {
	rowIDs := make([]uint64, 0, cb.cardinality)

	for wordIdx, word := range cb.words {
		if word == 0 {
			continue
		}

		for bitIdx := 0; bitIdx < 64; bitIdx++ {
			if word&(1<<uint(bitIdx)) != 0 {
				rowIDs = append(rowIDs, uint64(wordIdx)*64+uint64(bitIdx))
			}
		}
	}

	// Sort to ensure consistent order
	sort.Slice(rowIDs, func(i, j int) bool {
		return rowIDs[i] < rowIDs[j]
	})

	return rowIDs
}

// Clone creates a deep copy of the bitmap
func (cb *CompressedBitmap) Clone() *CompressedBitmap {
	clone := &CompressedBitmap{
		words:       make([]uint64, len(cb.words)),
		cardinality: cb.cardinality,
		maxBit:      cb.maxBit,
	}
	copy(clone.words, cb.words)
	return clone
}

// MarshalBinary serializes the compressed bitmap
func (cb *CompressedBitmap) MarshalBinary() ([]byte, error) {
	// Format: [cardinality][maxBit][word_count][words...]
	buf := make([]byte, 8+8+4+len(cb.words)*8)

	binary.LittleEndian.PutUint64(buf[0:8], cb.cardinality)
	binary.LittleEndian.PutUint64(buf[8:16], cb.maxBit)

	wordCount := uint32(len(cb.words))
	binary.LittleEndian.PutUint32(buf[16:20], wordCount)

	for i, word := range cb.words {
		binary.LittleEndian.PutUint64(buf[20+i*8:28+i*8], word)
	}

	return buf, nil
}

// UnmarshalBinary deserializes the compressed bitmap
func (cb *CompressedBitmap) UnmarshalBinary(data []byte) error {
	if len(data) < 20 {
		return fmt.Errorf("insufficient data for compressed bitmap")
	}

	cb.cardinality = binary.LittleEndian.Uint64(data[0:8])
	cb.maxBit = binary.LittleEndian.Uint64(data[8:16])

	wordCount := binary.LittleEndian.Uint32(data[16:20])
	if len(data) < 20+int(wordCount)*8 {
		return fmt.Errorf("insufficient data for bitmap words")
	}

	cb.words = make([]uint64, wordCount)
	for i := uint32(0); i < wordCount; i++ {
		cb.words[i] = binary.LittleEndian.Uint64(data[20+i*8 : 28+i*8])
	}

	return nil
}

// Utility functions

func max(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

// Ensure BitmapIndex implements ScalarIndexImpl
var _ ScalarIndexImpl = (*BitmapIndex)(nil)
