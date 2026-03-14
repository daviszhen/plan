package storage2

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"sort"
	"sync"
)

// BloomFilter implements a Bloom filter for fast membership testing
// A Bloom filter is a space-efficient probabilistic data structure that
// tests whether an element is a member of a set.
// False positive matches are possible, but false negatives are not.
type BloomFilter struct {
	// Bit array storing the filter state
	bits []uint64

	// Number of bits in the filter
	numBits uint64

	// Number of hash functions
	numHashes int

	// Number of items inserted
	numItems uint64

	// Target false positive rate
	falsePositiveRate float64

	mu sync.RWMutex
}

// BloomFilterIndex manages Bloom filters for multiple columns
type BloomFilterIndex struct {
	name      string
	indexType IndexType

	// Bloom filters by column index
	filters map[int]*BloomFilter

	// Bloom filters by fragment ID (for finer granularity)
	// fragmentID -> columnIdx -> BloomFilter
	fragmentFilters map[uint64]map[int]*BloomFilter

	mu    sync.RWMutex
	stats IndexStats
}

// BloomFilterConfig configures Bloom filter parameters
type BloomFilterConfig struct {
	// Expected number of items to insert
	ExpectedItems uint64

	// Target false positive rate (0.0 to 1.0)
	FalsePositiveRate float64

	// Maximum memory usage in bytes (0 = unlimited)
	MaxMemoryBytes uint64
}

// DefaultBloomFilterConfig returns default configuration
func DefaultBloomFilterConfig() BloomFilterConfig {
	return BloomFilterConfig{
		ExpectedItems:     10000,
		FalsePositiveRate: 0.01, // 1% false positive rate
		MaxMemoryBytes:    0,    // unlimited
	}
}

// BloomFilterIndexOption configures BloomFilterIndex creation
type BloomFilterIndexOption func(*BloomFilterIndex)

// WithBloomFilterIndexName sets the index name
func WithBloomFilterIndexName(name string) BloomFilterIndexOption {
	return func(bfi *BloomFilterIndex) {
		bfi.name = name
	}
}

// NewBloomFilter creates a new Bloom filter with the given configuration
func NewBloomFilter(config BloomFilterConfig) *BloomFilter {
	// Calculate optimal number of bits and hash functions
	// Using formulas:
	// numBits = -n * ln(p) / (ln(2)^2)
	// numHashes = (numBits / n) * ln(2)

	n := float64(config.ExpectedItems)
	p := config.FalsePositiveRate

	if p <= 0 {
		p = 0.01
	}
	if p >= 1 {
		p = 0.99
	}

	numBits := uint64(math.Ceil(-n * math.Log(p) / (math.Ln2 * math.Ln2)))
	numHashes := int(math.Ceil((float64(numBits) / n) * math.Ln2))

	// Ensure minimum values
	if numBits < 64 {
		numBits = 64
	}
	if numHashes < 1 {
		numHashes = 1
	}
	if numHashes > 20 {
		numHashes = 20 // Cap to avoid too many hash computations
	}

	// Check memory constraint
	if config.MaxMemoryBytes > 0 {
		maxBits := config.MaxMemoryBytes * 8
		if numBits > maxBits {
			numBits = maxBits
		}
	}

	// Allocate bit array (using uint64 words)
	numWords := (numBits + 63) / 64

	return &BloomFilter{
		bits:              make([]uint64, numWords),
		numBits:           numBits,
		numHashes:         numHashes,
		numItems:          0,
		falsePositiveRate: p,
	}
}

// NewBloomFilterIndex creates a new Bloom filter index
func NewBloomFilterIndex(opts ...BloomFilterIndexOption) *BloomFilterIndex {
	idx := &BloomFilterIndex{
		name:            "bloomfilter_idx",
		indexType:       ScalarIndex,
		filters:         make(map[int]*BloomFilter),
		fragmentFilters: make(map[uint64]map[int]*BloomFilter),
		stats: IndexStats{
			IndexType: "bloomfilter",
		},
	}

	for _, opt := range opts {
		opt(idx)
	}

	return idx
}

// Name returns the index name
func (bfi *BloomFilterIndex) Name() string {
	bfi.mu.RLock()
	defer bfi.mu.RUnlock()
	return bfi.name
}

// Type returns the index type
func (bfi *BloomFilterIndex) Type() IndexType {
	return bfi.indexType
}

// Columns returns the column indices this index covers
func (bfi *BloomFilterIndex) Columns() []int {
	bfi.mu.RLock()
	defer bfi.mu.RUnlock()

	cols := make([]int, 0, len(bfi.filters))
	for col := range bfi.filters {
		cols = append(cols, col)
	}
	sort.Ints(cols)
	return cols
}

// Search searches the index and returns matching row IDs
// For BloomFilter, this is not used as it's for pruning, not exact lookup
func (bfi *BloomFilterIndex) Search(ctx context.Context, query interface{}, limit int) ([]uint64, error) {
	return nil, fmt.Errorf("bloom filter index does not support direct search")
}

// Statistics returns statistics about the index
func (bfi *BloomFilterIndex) Statistics() IndexStats {
	bfi.mu.RLock()
	defer bfi.mu.RUnlock()
	return bfi.stats
}

// RangeQuery is not supported for Bloom filters
func (bfi *BloomFilterIndex) RangeQuery(ctx context.Context, start, end interface{}) ([]uint64, error) {
	return nil, fmt.Errorf("bloom filter index does not support range query")
}

// EqualityQuery is not supported for returning row IDs
func (bfi *BloomFilterIndex) EqualityQuery(ctx context.Context, value interface{}) ([]uint64, error) {
	return nil, fmt.Errorf("bloom filter index does not support equality query returning row IDs")
}

// BloomFilter operations

// Add inserts an item into the Bloom filter
func (bf *BloomFilter) Add(item []byte) {
	bf.mu.Lock()
	defer bf.mu.Unlock()

	hashes := bf.computeHashes(item)
	for _, h := range hashes {
		bf.setBit(h)
	}
	bf.numItems++
}

// AddString inserts a string item into the Bloom filter
func (bf *BloomFilter) AddString(item string) {
	bf.Add([]byte(item))
}

// AddInt64 inserts an int64 item into the Bloom filter
func (bf *BloomFilter) AddInt64(item int64) {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(item))
	bf.Add(buf)
}

// AddFloat64 inserts a float64 item into the Bloom filter
func (bf *BloomFilter) AddFloat64(item float64) {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, math.Float64bits(item))
	bf.Add(buf)
}

// AddValue inserts a value of any type into the Bloom filter
func (bf *BloomFilter) AddValue(value interface{}) error {
	data, err := serializeValueForBloom(value)
	if err != nil {
		return err
	}
	bf.Add(data)
	return nil
}

// MightContain checks if an item might be in the Bloom filter
// Returns true if the item might be in the set (with possible false positive)
// Returns false if the item is definitely not in the set
func (bf *BloomFilter) MightContain(item []byte) bool {
	bf.mu.RLock()
	defer bf.mu.RUnlock()

	hashes := bf.computeHashes(item)
	for _, h := range hashes {
		if !bf.getBit(h) {
			return false
		}
	}
	return true
}

// MightContainString checks if a string might be in the Bloom filter
func (bf *BloomFilter) MightContainString(item string) bool {
	return bf.MightContain([]byte(item))
}

// MightContainInt64 checks if an int64 might be in the Bloom filter
func (bf *BloomFilter) MightContainInt64(item int64) bool {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(item))
	return bf.MightContain(buf)
}

// MightContainFloat64 checks if a float64 might be in the Bloom filter
func (bf *BloomFilter) MightContainFloat64(item float64) bool {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, math.Float64bits(item))
	return bf.MightContain(buf)
}

// MightContainValue checks if a value of any type might be in the Bloom filter
func (bf *BloomFilter) MightContainValue(value interface{}) (bool, error) {
	data, err := serializeValueForBloom(value)
	if err != nil {
		return false, err
	}
	return bf.MightContain(data), nil
}

// computeHashes computes multiple hash values for an item
// Uses double hashing to generate multiple hash values from two base hashes
func (bf *BloomFilter) computeHashes(item []byte) []uint64 {
	// Compute two base hashes using FNV-1a
	h1 := fnvHash(item, 0)
	h2 := fnvHash(item, h1)

	hashes := make([]uint64, bf.numHashes)
	for i := 0; i < bf.numHashes; i++ {
		// Double hashing: h(i) = h1 + i * h2
		hashes[i] = (h1 + uint64(i)*h2) % bf.numBits
	}
	return hashes
}

// fnvHash computes FNV-1a hash with a seed
func fnvHash(data []byte, seed uint64) uint64 {
	h := uint64(2166136261) ^ seed
	for _, b := range data {
		h ^= uint64(b)
		h *= 16777619
	}
	return h
}

// setBit sets a bit in the filter
func (bf *BloomFilter) setBit(pos uint64) {
	wordIdx := pos / 64
	bitIdx := pos % 64
	bf.bits[wordIdx] |= 1 << bitIdx
}

// getBit checks if a bit is set in the filter
func (bf *BloomFilter) getBit(pos uint64) bool {
	wordIdx := pos / 64
	bitIdx := pos % 64
	return bf.bits[wordIdx]&(1<<bitIdx) != 0
}

// Size returns the size of the Bloom filter in bytes
func (bf *BloomFilter) Size() uint64 {
	bf.mu.RLock()
	defer bf.mu.RUnlock()
	return uint64(len(bf.bits) * 8)
}

// NumItems returns the number of items inserted
func (bf *BloomFilter) NumItems() uint64 {
	bf.mu.RLock()
	defer bf.mu.RUnlock()
	return bf.numItems
}

// EstimatedFalsePositiveRate estimates the current false positive rate
// based on the number of items inserted
func (bf *BloomFilter) EstimatedFalsePositiveRate() float64 {
	bf.mu.RLock()
	defer bf.mu.RUnlock()

	if bf.numItems == 0 {
		return 0
	}

	// Formula: (1 - e^(-k*n/m))^k
	// where k = numHashes, n = numItems, m = numBits
	k := float64(bf.numHashes)
	n := float64(bf.numItems)
	m := float64(bf.numBits)

	return math.Pow(1-math.Exp(-k*n/m), k)
}

// Clear resets the Bloom filter
func (bf *BloomFilter) Clear() {
	bf.mu.Lock()
	defer bf.mu.Unlock()

	for i := range bf.bits {
		bf.bits[i] = 0
	}
	bf.numItems = 0
}

// BloomFilterIndex operations

// GetFilter returns the Bloom filter for a column
func (bfi *BloomFilterIndex) GetFilter(columnIdx int) *BloomFilter {
	bfi.mu.RLock()
	defer bfi.mu.RUnlock()
	return bfi.filters[columnIdx]
}

// GetFragmentFilter returns the Bloom filter for a specific fragment and column
func (bfi *BloomFilterIndex) GetFragmentFilter(fragmentID uint64, columnIdx int) *BloomFilter {
	bfi.mu.RLock()
	defer bfi.mu.RUnlock()

	if fragFilters, exists := bfi.fragmentFilters[fragmentID]; exists {
		return fragFilters[columnIdx]
	}
	return nil
}

// CreateFilter creates a new Bloom filter for a column
func (bfi *BloomFilterIndex) CreateFilter(columnIdx int, config BloomFilterConfig) error {
	bfi.mu.Lock()
	defer bfi.mu.Unlock()

	bfi.filters[columnIdx] = NewBloomFilter(config)
	bfi.updateStats()
	return nil
}

// CreateFragmentFilter creates a new Bloom filter for a specific fragment
func (bfi *BloomFilterIndex) CreateFragmentFilter(fragmentID uint64, columnIdx int, config BloomFilterConfig) error {
	bfi.mu.Lock()
	defer bfi.mu.Unlock()

	fragFilters, exists := bfi.fragmentFilters[fragmentID]
	if !exists {
		fragFilters = make(map[int]*BloomFilter)
		bfi.fragmentFilters[fragmentID] = fragFilters
	}

	fragFilters[columnIdx] = NewBloomFilter(config)
	bfi.updateStats()
	return nil
}

// AddToFilter adds a value to a column's Bloom filter
func (bfi *BloomFilterIndex) AddToFilter(columnIdx int, value interface{}) error {
	bfi.mu.Lock()
	defer bfi.mu.Unlock()

	filter, exists := bfi.filters[columnIdx]
	if !exists {
		// Create filter with default config if not exists
		filter = NewBloomFilter(DefaultBloomFilterConfig())
		bfi.filters[columnIdx] = filter
	}

	err := filter.AddValue(value)
	if err != nil {
		return err
	}

	bfi.updateStats()
	return nil
}

// AddToFragmentFilter adds a value to a fragment's Bloom filter
func (bfi *BloomFilterIndex) AddToFragmentFilter(fragmentID uint64, columnIdx int, value interface{}) error {
	bfi.mu.Lock()
	defer bfi.mu.Unlock()

	fragFilters, exists := bfi.fragmentFilters[fragmentID]
	if !exists {
		fragFilters = make(map[int]*BloomFilter)
		bfi.fragmentFilters[fragmentID] = fragFilters
	}

	filter, exists := fragFilters[columnIdx]
	if !exists {
		filter = NewBloomFilter(DefaultBloomFilterConfig())
		fragFilters[columnIdx] = filter
	}

	err := filter.AddValue(value)
	if err != nil {
		return err
	}

	bfi.updateStats()
	return nil
}

// MightContain checks if a value might be in a column's Bloom filter
func (bfi *BloomFilterIndex) MightContain(columnIdx int, value interface{}) (bool, error) {
	bfi.mu.RLock()
	defer bfi.mu.RUnlock()

	filter, exists := bfi.filters[columnIdx]
	if !exists {
		return false, fmt.Errorf("no bloom filter for column %d", columnIdx)
	}

	return filter.MightContainValue(value)
}

// MightContainInFragment checks if a value might be in a fragment's Bloom filter
func (bfi *BloomFilterIndex) MightContainInFragment(fragmentID uint64, columnIdx int, value interface{}) (bool, error) {
	bfi.mu.RLock()
	defer bfi.mu.RUnlock()

	fragFilters, exists := bfi.fragmentFilters[fragmentID]
	if !exists {
		return false, nil // No filter means we can't prune
	}

	filter, exists := fragFilters[columnIdx]
	if !exists {
		return false, nil
	}

	return filter.MightContainValue(value)
}

// CanPrune checks if a fragment can be pruned based on Bloom filter
// Returns true if the value is definitely NOT in the fragment
func (bfi *BloomFilterIndex) CanPrune(fragmentID uint64, columnIdx int, value interface{}) (bool, error) {
	mightContain, err := bfi.MightContainInFragment(fragmentID, columnIdx, value)
	if err != nil {
		return false, err
	}
	return !mightContain, nil
}

// GetPrunableFragments returns fragment IDs that can be pruned for a given value
func (bfi *BloomFilterIndex) GetPrunableFragments(columnIdx int, value interface{}) []uint64 {
	bfi.mu.RLock()
	defer bfi.mu.RUnlock()

	var prunable []uint64

	for fragID, fragFilters := range bfi.fragmentFilters {
		filter, exists := fragFilters[columnIdx]
		if !exists {
			continue
		}

		mightContain, err := filter.MightContainValue(value)
		if err != nil {
			continue
		}

		if !mightContain {
			prunable = append(prunable, fragID)
		}
	}

	return prunable
}

// MergeFilters merges multiple Bloom filters into one
// This is done by OR-ing the bit arrays
func MergeFilters(filters ...*BloomFilter) (*BloomFilter, error) {
	if len(filters) == 0 {
		return nil, fmt.Errorf("no filters to merge")
	}

	// Check that all filters have the same size
	numBits := filters[0].numBits
	numHashes := filters[0].numHashes

	for _, f := range filters {
		if f.numBits != numBits {
			return nil, fmt.Errorf("cannot merge filters with different sizes")
		}
		if f.numHashes != numHashes {
			return nil, fmt.Errorf("cannot merge filters with different number of hashes")
		}
	}

	// Create new filter
	result := &BloomFilter{
		bits:              make([]uint64, len(filters[0].bits)),
		numBits:           numBits,
		numHashes:         numHashes,
		falsePositiveRate: filters[0].falsePositiveRate,
	}

	// OR all bit arrays
	for _, f := range filters {
		for i, word := range f.bits {
			result.bits[i] |= word
		}
		result.numItems += f.numItems
	}

	return result, nil
}

// Serialization

// MarshalBinary serializes the Bloom filter to binary format
func (bf *BloomFilter) MarshalBinary() ([]byte, error) {
	bf.mu.RLock()
	defer bf.mu.RUnlock()

	// Format:
	// [numBits][numHashes][numItems][falsePositiveRate][bits...]

	buf := make([]byte, 8+4+8+8+len(bf.bits)*8)

	binary.LittleEndian.PutUint64(buf[0:8], bf.numBits)
	binary.LittleEndian.PutUint32(buf[8:12], uint32(bf.numHashes))
	binary.LittleEndian.PutUint64(buf[12:20], bf.numItems)
	binary.LittleEndian.PutUint64(buf[20:28], math.Float64bits(bf.falsePositiveRate))

	for i, word := range bf.bits {
		binary.LittleEndian.PutUint64(buf[28+i*8:36+i*8], word)
	}

	return buf, nil
}

// UnmarshalBinary deserializes the Bloom filter from binary format
func (bf *BloomFilter) UnmarshalBinary(data []byte) error {
	if len(data) < 28 {
		return fmt.Errorf("insufficient data for bloom filter")
	}

	bf.numBits = binary.LittleEndian.Uint64(data[0:8])
	bf.numHashes = int(binary.LittleEndian.Uint32(data[8:12]))
	bf.numItems = binary.LittleEndian.Uint64(data[12:20])
	bf.falsePositiveRate = math.Float64frombits(binary.LittleEndian.Uint64(data[20:28]))

	numWords := (bf.numBits + 63) / 64
	if len(data) < 28+int(numWords)*8 {
		return fmt.Errorf("insufficient data for bloom filter bits")
	}

	bf.bits = make([]uint64, numWords)
	for i := uint64(0); i < numWords; i++ {
		bf.bits[i] = binary.LittleEndian.Uint64(data[28+i*8 : 36+i*8])
	}

	return nil
}

// MarshalBinary serializes the Bloom filter index to binary format
func (bfi *BloomFilterIndex) MarshalBinary() ([]byte, error) {
	bfi.mu.RLock()
	defer bfi.mu.RUnlock()

	// Similar format to ZoneMapIndex
	var buf []byte

	// Number of columns
	numCols := uint32(len(bfi.filters))
	colCountBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(colCountBuf, numCols)
	buf = append(buf, colCountBuf...)

	// Column filters
	for colIdx, filter := range bfi.filters {
		colBuf := make([]byte, 4)
		binary.LittleEndian.PutUint32(colBuf, uint32(colIdx))
		buf = append(buf, colBuf...)

		filterData, err := filter.MarshalBinary()
		if err != nil {
			return nil, err
		}
		filterLenBuf := make([]byte, 4)
		binary.LittleEndian.PutUint32(filterLenBuf, uint32(len(filterData)))
		buf = append(buf, filterLenBuf...)
		buf = append(buf, filterData...)
	}

	// Number of fragments
	numFrags := uint32(len(bfi.fragmentFilters))
	fragCountBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(fragCountBuf, numFrags)
	buf = append(buf, fragCountBuf...)

	// Fragment filters
	for fragID, fragFilters := range bfi.fragmentFilters {
		fragIDBuf := make([]byte, 8)
		binary.LittleEndian.PutUint64(fragIDBuf, fragID)
		buf = append(buf, fragIDBuf...)

		numFragCols := uint32(len(fragFilters))
		numFragColsBuf := make([]byte, 4)
		binary.LittleEndian.PutUint32(numFragColsBuf, numFragCols)
		buf = append(buf, numFragColsBuf...)

		for colIdx, filter := range fragFilters {
			colBuf := make([]byte, 4)
			binary.LittleEndian.PutUint32(colBuf, uint32(colIdx))
			buf = append(buf, colBuf...)

			filterData, err := filter.MarshalBinary()
			if err != nil {
				return nil, err
			}
			filterLenBuf := make([]byte, 4)
			binary.LittleEndian.PutUint32(filterLenBuf, uint32(len(filterData)))
			buf = append(buf, filterLenBuf...)
			buf = append(buf, filterData...)
		}
	}

	return buf, nil
}

// UnmarshalBinary deserializes the Bloom filter index from binary format
func (bfi *BloomFilterIndex) UnmarshalBinary(data []byte) error {
	bfi.mu.Lock()
	defer bfi.mu.Unlock()

	if len(data) < 4 {
		return fmt.Errorf("insufficient data for bloom filter index")
	}

	offset := 0

	// Number of columns
	numCols := binary.LittleEndian.Uint32(data[offset : offset+4])
	offset += 4

	bfi.filters = make(map[int]*BloomFilter)

	for i := uint32(0); i < numCols; i++ {
		colIdx := int(binary.LittleEndian.Uint32(data[offset : offset+4]))
		offset += 4

		filterLen := binary.LittleEndian.Uint32(data[offset : offset+4])
		offset += 4

		filter := &BloomFilter{}
		if err := filter.UnmarshalBinary(data[offset : offset+int(filterLen)]); err != nil {
			return err
		}
		offset += int(filterLen)

		bfi.filters[colIdx] = filter
	}

	// Number of fragments
	numFrags := binary.LittleEndian.Uint32(data[offset : offset+4])
	offset += 4

	bfi.fragmentFilters = make(map[uint64]map[int]*BloomFilter)

	for i := uint32(0); i < numFrags; i++ {
		fragID := binary.LittleEndian.Uint64(data[offset : offset+8])
		offset += 8

		numFragCols := binary.LittleEndian.Uint32(data[offset : offset+4])
		offset += 4

		fragFilters := make(map[int]*BloomFilter)

		for j := uint32(0); j < numFragCols; j++ {
			colIdx := int(binary.LittleEndian.Uint32(data[offset : offset+4]))
			offset += 4

			filterLen := binary.LittleEndian.Uint32(data[offset : offset+4])
			offset += 4

			filter := &BloomFilter{}
			if err := filter.UnmarshalBinary(data[offset : offset+int(filterLen)]); err != nil {
				return err
			}
			offset += int(filterLen)

			fragFilters[colIdx] = filter
		}

		bfi.fragmentFilters[fragID] = fragFilters
	}

	bfi.updateStats()
	return nil
}

// Helper functions

// serializeValueForBloom serializes a value for Bloom filter insertion
func serializeValueForBloom(value interface{}) ([]byte, error) {
	return serializeValue(value)
}

// updateStats updates index statistics
func (bfi *BloomFilterIndex) updateStats() {
	totalItems := uint64(0)
	totalSize := uint64(0)

	for _, filter := range bfi.filters {
		totalItems += filter.numItems
		totalSize += filter.Size()
	}

	for _, fragFilters := range bfi.fragmentFilters {
		for _, filter := range fragFilters {
			totalItems += filter.numItems
			totalSize += filter.Size()
		}
	}

	bfi.stats.NumEntries = totalItems
	bfi.stats.SizeBytes = totalSize
}

// Ensure BloomFilterIndex implements ScalarIndexImpl
var _ ScalarIndexImpl = (*BloomFilterIndex)(nil)
