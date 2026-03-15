package storage2

import (
	"encoding/binary"
	"fmt"
	"sort"
)

// RowIdSequence represents a sequence of row IDs, optimized for different patterns.
// This mirrors Lance's RowIdSequence structure for compatibility.
type RowIdSequence struct {
	Segments []U64Segment
}

// U64Segment represents a segment of u64 values with different encoding strategies.
type U64Segment interface {
	// Len returns the number of values in the segment
	Len() int
	// Get returns the value at index i, or false if out of bounds
	Get(i int) (uint64, bool)
	// Contains checks if a value is in the segment
	Contains(val uint64) bool
	// Position returns the position of a value, or -1 if not found
	Position(val uint64) int
	// Iter returns all values in the segment
	Iter() []uint64
}

// RangeSegment represents a contiguous range of u64 values [Start, End)
type RangeSegment struct {
	Start uint64
	End   uint64
}

func (r *RangeSegment) Len() int {
	return int(r.End - r.Start)
}

func (r *RangeSegment) Get(i int) (uint64, bool) {
	val := r.Start + uint64(i)
	if val >= r.End {
		return 0, false
	}
	return val, true
}

func (r *RangeSegment) Contains(val uint64) bool {
	return val >= r.Start && val < r.End
}

func (r *RangeSegment) Position(val uint64) int {
	if val >= r.Start && val < r.End {
		return int(val - r.Start)
	}
	return -1
}

func (r *RangeSegment) Iter() []uint64 {
	result := make([]uint64, r.End-r.Start)
	for i := range result {
		result[i] = r.Start + uint64(i)
	}
	return result
}

// RangeWithHolesSegment represents a range with some holes (missing values)
type RangeWithHolesSegment struct {
	Start uint64
	End   uint64
	Holes []uint64 // sorted list of hole values
}

func (r *RangeWithHolesSegment) Len() int {
	return int(r.End-r.Start) - len(r.Holes)
}

func (r *RangeWithHolesSegment) Get(i int) (uint64, bool) {
	count := 0
	for val := r.Start; val < r.End; val++ {
		if !r.isHole(val) {
			if count == i {
				return val, true
			}
			count++
		}
	}
	return 0, false
}

func (r *RangeWithHolesSegment) isHole(val uint64) bool {
	idx := sort.Search(len(r.Holes), func(i int) bool { return r.Holes[i] >= val })
	return idx < len(r.Holes) && r.Holes[idx] == val
}

func (r *RangeWithHolesSegment) Contains(val uint64) bool {
	if val < r.Start || val >= r.End {
		return false
	}
	return !r.isHole(val)
}

func (r *RangeWithHolesSegment) Position(val uint64) int {
	if !r.Contains(val) {
		return -1
	}
	// Count non-hole values before val
	pos := int(val - r.Start)
	for _, hole := range r.Holes {
		if hole < val {
			pos--
		}
	}
	return pos
}

func (r *RangeWithHolesSegment) Iter() []uint64 {
	result := make([]uint64, 0, r.Len())
	for val := r.Start; val < r.End; val++ {
		if !r.isHole(val) {
			result = append(result, val)
		}
	}
	return result
}

// RangeWithBitmapSegment represents a range with a bitmap indicating which values are present
type RangeWithBitmapSegment struct {
	Start  uint64
	End    uint64
	Bitmap []byte // 1 bit per value, 1 = present, 0 = missing
}

func (r *RangeWithBitmapSegment) Len() int {
	count := 0
	total := int(r.End - r.Start)
	for i := 0; i < total; i++ {
		if r.getBit(i) {
			count++
		}
	}
	return count
}

func (r *RangeWithBitmapSegment) getBit(i int) bool {
	if i >= len(r.Bitmap)*8 {
		return false
	}
	byteIdx := i / 8
	bitIdx := uint(i % 8)
	return (r.Bitmap[byteIdx] & (1 << bitIdx)) != 0
}

func (r *RangeWithBitmapSegment) Get(i int) (uint64, bool) {
	count := 0
	total := int(r.End - r.Start)
	for j := 0; j < total; j++ {
		if r.getBit(j) {
			if count == i {
				return r.Start + uint64(j), true
			}
			count++
		}
	}
	return 0, false
}

func (r *RangeWithBitmapSegment) Contains(val uint64) bool {
	if val < r.Start || val >= r.End {
		return false
	}
	offset := int(val - r.Start)
	return r.getBit(offset)
}

func (r *RangeWithBitmapSegment) Position(val uint64) int {
	if !r.Contains(val) {
		return -1
	}
	offset := int(val - r.Start)
	pos := 0
	for i := 0; i < offset; i++ {
		if r.getBit(i) {
			pos++
		}
	}
	return pos
}

func (r *RangeWithBitmapSegment) Iter() []uint64 {
	result := make([]uint64, 0, r.Len())
	total := int(r.End - r.Start)
	for i := 0; i < total; i++ {
		if r.getBit(i) {
			result = append(result, r.Start+uint64(i))
		}
	}
	return result
}

// SortedArraySegment represents a sorted array of u64 values
type SortedArraySegment struct {
	Values []uint64
}

func (s *SortedArraySegment) Len() int {
	return len(s.Values)
}

func (s *SortedArraySegment) Get(i int) (uint64, bool) {
	if i < 0 || i >= len(s.Values) {
		return 0, false
	}
	return s.Values[i], true
}

func (s *SortedArraySegment) Contains(val uint64) bool {
	idx := sort.Search(len(s.Values), func(i int) bool { return s.Values[i] >= val })
	return idx < len(s.Values) && s.Values[idx] == val
}

func (s *SortedArraySegment) Position(val uint64) int {
	idx := sort.Search(len(s.Values), func(i int) bool { return s.Values[i] >= val })
	if idx < len(s.Values) && s.Values[idx] == val {
		return idx
	}
	return -1
}

func (s *SortedArraySegment) Iter() []uint64 {
	return s.Values
}

// ArraySegment represents an unsorted array of u64 values
type ArraySegment struct {
	Values []uint64
}

func (a *ArraySegment) Len() int {
	return len(a.Values)
}

func (a *ArraySegment) Get(i int) (uint64, bool) {
	if i < 0 || i >= len(a.Values) {
		return 0, false
	}
	return a.Values[i], true
}

func (a *ArraySegment) Contains(val uint64) bool {
	for _, v := range a.Values {
		if v == val {
			return true
		}
	}
	return false
}

func (a *ArraySegment) Position(val uint64) int {
	for i, v := range a.Values {
		if v == val {
			return i
		}
	}
	return -1
}

func (a *ArraySegment) Iter() []uint64 {
	return a.Values
}

// NewRowIdSequenceFromRange creates a RowIdSequence from a contiguous range
func NewRowIdSequenceFromRange(start, end uint64) *RowIdSequence {
	return &RowIdSequence{
		Segments: []U64Segment{&RangeSegment{Start: start, End: end}},
	}
}

// NewRowIdSequenceFromSlice creates a RowIdSequence from a slice of values
func NewRowIdSequenceFromSlice(values []uint64) *RowIdSequence {
	if len(values) == 0 {
		return &RowIdSequence{}
	}

	// Check if sorted
	sorted := true
	for i := 1; i < len(values); i++ {
		if values[i] < values[i-1] {
			sorted = false
			break
		}
	}

	if sorted {
		return &RowIdSequence{
			Segments: []U64Segment{&SortedArraySegment{Values: values}},
		}
	}
	return &RowIdSequence{
		Segments: []U64Segment{&ArraySegment{Values: values}},
	}
}

// Len returns the total number of values in the sequence
func (r *RowIdSequence) Len() int {
	total := 0
	for _, seg := range r.Segments {
		total += seg.Len()
	}
	return total
}

// Get returns the value at index i
func (r *RowIdSequence) Get(i int) (uint64, bool) {
	offset := 0
	for _, seg := range r.Segments {
		segLen := seg.Len()
		if i < offset+segLen {
			return seg.Get(i - offset)
		}
		offset += segLen
	}
	return 0, false
}

// Contains checks if a value is in the sequence
func (r *RowIdSequence) Contains(val uint64) bool {
	for _, seg := range r.Segments {
		if seg.Contains(val) {
			return true
		}
	}
	return false
}

// Position returns the position of a value in the sequence
func (r *RowIdSequence) Position(val uint64) int {
	offset := 0
	for _, seg := range r.Segments {
		pos := seg.Position(val)
		if pos >= 0 {
			return offset + pos
		}
		offset += seg.Len()
	}
	return -1
}

// Iter returns all values in the sequence
func (r *RowIdSequence) Iter() []uint64 {
	var result []uint64
	for _, seg := range r.Segments {
		result = append(result, seg.Iter()...)
	}
	return result
}

// IsEmpty returns true if the sequence contains no values.
func (r *RowIdSequence) IsEmpty() bool {
	return len(r.Segments) == 0
}

// Extend appends another RowIdSequence to this one.
// If the tail of this sequence and the head of the other are adjacent Range segments,
// they are merged into a single Range (matching Lance Rust behaviour).
func (r *RowIdSequence) Extend(other *RowIdSequence) {
	if other == nil || len(other.Segments) == 0 {
		return
	}
	if len(r.Segments) > 0 {
		lastRange, okLast := r.Segments[len(r.Segments)-1].(*RangeSegment)
		firstRange, okFirst := other.Segments[0].(*RangeSegment)
		if okLast && okFirst && lastRange.End == firstRange.Start {
			r.Segments[len(r.Segments)-1] = &RangeSegment{Start: lastRange.Start, End: firstRange.End}
			r.Segments = append(r.Segments, other.Segments[1:]...)
			return
		}
	}
	r.Segments = append(r.Segments, other.Segments...)
}

// Delete removes the given row IDs from the sequence.
// Each segment that contains removed values is rebuilt using NewU64SegmentFromSlice
// so that the optimal encoding is re-selected (matching Lance Rust behaviour).
func (r *RowIdSequence) Delete(rowIDs []uint64) {
	if len(rowIDs) == 0 {
		return
	}
	toDelete := make(map[uint64]struct{}, len(rowIDs))
	for _, id := range rowIDs {
		toDelete[id] = struct{}{}
	}

	var newSegments []U64Segment
	for _, seg := range r.Segments {
		// Check whether this segment contains any of the deleted IDs.
		hasDeleted := false
		for _, id := range rowIDs {
			if seg.Contains(id) {
				hasDeleted = true
				break
			}
		}
		if !hasDeleted {
			newSegments = append(newSegments, seg)
			continue
		}
		// Rebuild segment without deleted values.
		var kept []uint64
		for _, v := range seg.Iter() {
			if _, del := toDelete[v]; !del {
				kept = append(kept, v)
			}
		}
		if len(kept) > 0 {
			newSegments = append(newSegments, NewU64SegmentFromSlice(kept))
		}
	}
	r.Segments = newSegments
}

// Slice returns a new RowIdSequence containing values [offset, offset+length).
// Matches Lance Rust RowIdSequence::slice semantics.
func (r *RowIdSequence) Slice(offset, length int) *RowIdSequence {
	if length <= 0 {
		return &RowIdSequence{}
	}
	var segs []U64Segment
	pos := 0
	remaining := length
	for _, seg := range r.Segments {
		segLen := seg.Len()
		// Skip segments entirely before offset.
		if pos+segLen <= offset {
			pos += segLen
			continue
		}
		// Start index within this segment.
		start := 0
		if pos < offset {
			start = offset - pos
		}
		// How many values to take from this segment.
		take := segLen - start
		if take > remaining {
			take = remaining
		}
		// Extract sub-slice of segment.
		vals := seg.Iter()
		segs = append(segs, NewU64SegmentFromSlice(vals[start:start+take]))
		remaining -= take
		pos += segLen
		if remaining <= 0 {
			break
		}
	}
	return &RowIdSequence{Segments: segs}
}

// NewU64SegmentFromSlice creates the optimal U64Segment encoding for the given values.
// It mirrors Lance Rust's U64Segment::from_iter / from_stats_and_sequence logic:
//
//	Sorted?
//	  Yes → Contiguous? → Range
//	         Dense?     → RangeWithBitmap / RangeWithHoles
//	         No         → SortedArray
//	  No  → Array
func NewU64SegmentFromSlice(values []uint64) U64Segment {
	n := len(values)
	if n == 0 {
		return &RangeSegment{Start: 0, End: 0}
	}
	// Compute stats.
	sorted := true
	minVal := values[0]
	maxVal := values[0]
	for i, v := range values {
		if v < minVal {
			minVal = v
		}
		if v > maxVal {
			maxVal = v
		}
		if sorted && i > 0 && v < values[i-1] {
			sorted = false
		}
	}
	if !sorted {
		cp := make([]uint64, n)
		copy(cp, values)
		return &ArraySegment{Values: cp}
	}
	// Contiguous?
	if maxVal-minVal+1 == uint64(n) {
		return &RangeSegment{Start: minVal, End: maxVal + 1}
	}
	// Dense decision: compare bitmap size vs sorted-array size.
	totalSlots := maxVal - minVal + 1
	bitmapBytes := 16 + (totalSlots+7)/8 // range + bitmap
	arrayBytes := 24 + 2*uint64(n)       // min + max + count + packed values (u16)
	nHoles := totalSlots - uint64(n)
	holesBytes := 16 + 4*nHoles // range + holes array (u32)

	if nHoles > 0 && holesBytes < bitmapBytes && holesBytes < arrayBytes {
		// RangeWithHoles
		existing := make(map[uint64]struct{}, n)
		for _, v := range values {
			existing[v] = struct{}{}
		}
		var holes []uint64
		for v := minVal; v <= maxVal; v++ {
			if _, ok := existing[v]; !ok {
				holes = append(holes, v)
			}
		}
		return &RangeWithHolesSegment{Start: minVal, End: maxVal + 1, Holes: holes}
	}
	if bitmapBytes < arrayBytes {
		// RangeWithBitmap
		bitmapLen := int(totalSlots+7) / 8
		bitmap := make([]byte, bitmapLen)
		for _, v := range values {
			off := v - minVal
			bitmap[off/8] |= 1 << (off % 8)
		}
		return &RangeWithBitmapSegment{Start: minVal, End: maxVal + 1, Bitmap: bitmap}
	}
	cp := make([]uint64, n)
	copy(cp, values)
	return &SortedArraySegment{Values: cp}
}

// ParseRowIdSequence parses a RowIdSequence from protobuf-encoded bytes.
// The format is compatible with Lance's RowIdSequence serialization.
func ParseRowIdSequence(data []byte) (*RowIdSequence, error) {
	if len(data) == 0 {
		return &RowIdSequence{}, nil
	}

	// Parse protobuf message
	// The protobuf format is defined in Lance's rowids.proto
	// For simplicity, we'll implement a basic parser

	// The wire format is:
	// - Field 1 (repeated): segments
	// Each segment is a nested message

	segments, err := parseSegments(data)
	if err != nil {
		return nil, fmt.Errorf("failed to parse segments: %w", err)
	}

	return &RowIdSequence{Segments: segments}, nil
}

// parseSegments parses the protobuf-encoded segments
func parseSegments(data []byte) ([]U64Segment, error) {
	var segments []U64Segment

	// Simple protobuf parsing
	// Field 1 is repeated, each segment is a length-delimited message
	i := 0
	for i < len(data) {
		if i >= len(data) {
			break
		}

		// Read field tag
		tag, n := binary.Uvarint(data[i:])
		if n <= 0 {
			return nil, fmt.Errorf("invalid protobuf tag at position %d", i)
		}
		i += n

		fieldNum := tag >> 3
		wireType := tag & 0x7

		if fieldNum != 1 || wireType != 2 {
			// Skip unknown field
			if wireType == 2 {
				// Length-delimited
				length, n := binary.Uvarint(data[i:])
				if n <= 0 {
					return nil, fmt.Errorf("invalid length at position %d", i)
				}
				i += n + int(length)
			} else {
				// Skip varint
				_, n := binary.Uvarint(data[i:])
				i += n
			}
			continue
		}

		// Read segment message
		length, n := binary.Uvarint(data[i:])
		if n <= 0 {
			return nil, fmt.Errorf("invalid segment length at position %d", i)
		}
		i += n

		if i+int(length) > len(data) {
			return nil, fmt.Errorf("segment data exceeds buffer")
		}

		segmentData := data[i : i+int(length)]
		segment, err := parseSegment(segmentData)
		if err != nil {
			return nil, fmt.Errorf("failed to parse segment: %w", err)
		}

		segments = append(segments, segment)
		i += int(length)
	}

	return segments, nil
}

// parseSegment parses a single U64Segment from protobuf data
func parseSegment(data []byte) (U64Segment, error) {
	// Segment format:
	// Field 1: Range (start, end)
	// Field 2: RangeWithHoles (start, end, holes)
	// Field 3: RangeWithBitmap (start, end, bitmap)
	// Field 4: SortedArray (encoded array)
	// Field 5: Array (encoded array)

	i := 0
	for i < len(data) {
		tag, n := binary.Uvarint(data[i:])
		if n <= 0 {
			return nil, fmt.Errorf("invalid tag at position %d", i)
		}
		i += n

		fieldNum := tag >> 3
		wireType := tag & 0x7

		switch fieldNum {
		case 1: // Range
			// Parse nested message
			if wireType != 2 {
				return nil, fmt.Errorf("expected length-delimited for Range")
			}
			length, n := binary.Uvarint(data[i:])
			if n <= 0 {
				return nil, fmt.Errorf("invalid Range length")
			}
			i += n
			rangeData := data[i : i+int(length)]
			start, end, err := parseRangeMessage(rangeData)
			if err != nil {
				return nil, err
			}
			return &RangeSegment{Start: start, End: end}, nil

		case 2: // RangeWithHoles
			if wireType != 2 {
				return nil, fmt.Errorf("expected length-delimited for RangeWithHoles")
			}
			length, n := binary.Uvarint(data[i:])
			if n <= 0 {
				return nil, fmt.Errorf("invalid RangeWithHoles length")
			}
			i += n
			rwhData := data[i : i+int(length)]
			start, end, holes, err := parseRangeWithHolesMessage(rwhData)
			if err != nil {
				return nil, err
			}
			return &RangeWithHolesSegment{Start: start, End: end, Holes: holes}, nil

		case 3: // RangeWithBitmap
			if wireType != 2 {
				return nil, fmt.Errorf("expected length-delimited for RangeWithBitmap")
			}
			length, n := binary.Uvarint(data[i:])
			if n <= 0 {
				return nil, fmt.Errorf("invalid RangeWithBitmap length")
			}
			i += n
			rwbData := data[i : i+int(length)]
			start, end, bitmap, err := parseRangeWithBitmapMessage(rwbData)
			if err != nil {
				return nil, err
			}
			return &RangeWithBitmapSegment{Start: start, End: end, Bitmap: bitmap}, nil

		case 4: // SortedArray
			if wireType != 2 {
				return nil, fmt.Errorf("expected length-delimited for array")
			}
			length, n := binary.Uvarint(data[i:])
			if n <= 0 {
				return nil, fmt.Errorf("invalid array length")
			}
			i += n
			arrayData := data[i : i+int(length)]
			arrayValues, err := parseEncodedArray(arrayData)
			if err != nil {
				return nil, err
			}
			return &SortedArraySegment{Values: arrayValues}, nil

		case 5: // Array
			if wireType != 2 {
				return nil, fmt.Errorf("expected length-delimited for array")
			}
			length, n := binary.Uvarint(data[i:])
			if n <= 0 {
				return nil, fmt.Errorf("invalid array length")
			}
			i += n
			arrayData := data[i : i+int(length)]
			arrayValues, err := parseEncodedArray(arrayData)
			if err != nil {
				return nil, err
			}
			return &ArraySegment{Values: arrayValues}, nil

		default:
			// Skip unknown field
			if wireType == 2 {
				length, n := binary.Uvarint(data[i:])
				if n <= 0 {
					return nil, fmt.Errorf("invalid skip length")
				}
				i += n + int(length)
			} else {
				_, n := binary.Uvarint(data[i:])
				i += n
			}
		}
	}

	// Default to empty range if no segment type found
	return &RangeSegment{Start: 0, End: 0}, nil
}

func parseRangeMessage(data []byte) (start, end uint64, err error) {
	i := 0
	for i < len(data) {
		tag, n := binary.Uvarint(data[i:])
		if n <= 0 {
			return 0, 0, fmt.Errorf("invalid tag in Range")
		}
		i += n

		fieldNum := tag >> 3
		wireType := tag & 0x7

		switch fieldNum {
		case 1: // start
			if wireType != 0 {
				return 0, 0, fmt.Errorf("expected varint for start")
			}
			start, n = binary.Uvarint(data[i:])
			i += n
		case 2: // end
			if wireType != 0 {
				return 0, 0, fmt.Errorf("expected varint for end")
			}
			end, n = binary.Uvarint(data[i:])
			i += n
		default:
			if wireType == 2 {
				length, n := binary.Uvarint(data[i:])
				i += n + int(length)
			} else {
				_, n := binary.Uvarint(data[i:])
				i += n
			}
		}
	}
	return start, end, nil
}

func parseRangeWithHolesMessage(data []byte) (start, end uint64, holes []uint64, err error) {
	i := 0
	for i < len(data) {
		tag, n := binary.Uvarint(data[i:])
		if n <= 0 {
			return 0, 0, nil, fmt.Errorf("invalid tag in RangeWithHoles")
		}
		i += n

		fieldNum := tag >> 3
		wireType := tag & 0x7

		switch fieldNum {
		case 1: // start
			start, n = binary.Uvarint(data[i:])
			i += n
		case 2: // end
			end, n = binary.Uvarint(data[i:])
			i += n
		case 3: // holes (encoded array)
			if wireType != 2 {
				return 0, 0, nil, fmt.Errorf("expected length-delimited for holes")
			}
			length, n := binary.Uvarint(data[i:])
			if n <= 0 {
				return 0, 0, nil, fmt.Errorf("invalid holes length")
			}
			i += n
			holesData := data[i : i+int(length)]
			holes, err = parseEncodedArray(holesData)
			if err != nil {
				return 0, 0, nil, err
			}
			i += int(length)
		default:
			if wireType == 2 {
				length, n := binary.Uvarint(data[i:])
				i += n + int(length)
			} else {
				_, n := binary.Uvarint(data[i:])
				i += n
			}
		}
	}
	return start, end, holes, nil
}

func parseRangeWithBitmapMessage(data []byte) (start, end uint64, bitmap []byte, err error) {
	i := 0
	for i < len(data) {
		tag, n := binary.Uvarint(data[i:])
		if n <= 0 {
			return 0, 0, nil, fmt.Errorf("invalid tag in RangeWithBitmap")
		}
		i += n

		fieldNum := tag >> 3
		wireType := tag & 0x7

		switch fieldNum {
		case 1: // start
			start, n = binary.Uvarint(data[i:])
			i += n
		case 2: // end
			end, n = binary.Uvarint(data[i:])
			i += n
		case 3: // bitmap
			if wireType != 2 {
				return 0, 0, nil, fmt.Errorf("expected length-delimited for bitmap")
			}
			length, n := binary.Uvarint(data[i:])
			if n <= 0 {
				return 0, 0, nil, fmt.Errorf("invalid bitmap length")
			}
			i += n
			bitmap = data[i : i+int(length)]
			i += int(length)
		default:
			if wireType == 2 {
				length, n := binary.Uvarint(data[i:])
				i += n + int(length)
			} else {
				_, n := binary.Uvarint(data[i:])
				i += n
			}
		}
	}
	return start, end, bitmap, nil
}

func parseEncodedArray(data []byte) ([]uint64, error) {
	// Encoded array format:
	// Field 1: U16Array (base, offsets)
	// Field 2: U32Array (base, offsets)
	// Field 3: U64Array (values)

	i := 0
	for i < len(data) {
		tag, n := binary.Uvarint(data[i:])
		if n <= 0 {
			return nil, fmt.Errorf("invalid tag in encoded array")
		}
		i += n

		fieldNum := tag >> 3
		wireType := tag & 0x7

		if wireType != 2 {
			return nil, fmt.Errorf("expected length-delimited for array type")
		}

		length, n := binary.Uvarint(data[i:])
		if n <= 0 {
			return nil, fmt.Errorf("invalid array length")
		}
		i += n
		arrayData := data[i : i+int(length)]

		switch fieldNum {
		case 1: // U16Array
			return parseU16Array(arrayData)
		case 2: // U32Array
			return parseU32Array(arrayData)
		case 3: // U64Array
			return parseU64Array(arrayData)
		}

		i += int(length)
	}

	return nil, fmt.Errorf("no array data found")
}

func parseU16Array(data []byte) ([]uint64, error) {
	var base uint64
	var offsets []uint16

	i := 0
	for i < len(data) {
		tag, n := binary.Uvarint(data[i:])
		if n <= 0 {
			return nil, fmt.Errorf("invalid tag in U16Array")
		}
		i += n

		fieldNum := tag >> 3
		wireType := tag & 0x7

		switch fieldNum {
		case 1: // base
			base, n = binary.Uvarint(data[i:])
			i += n
		case 2: // offsets
			if wireType != 2 {
				return nil, fmt.Errorf("expected length-delimited for offsets")
			}
			length, n := binary.Uvarint(data[i:])
			if n <= 0 {
				return nil, fmt.Errorf("invalid offsets length")
			}
			i += n
			offsetsData := data[i : i+int(length)]
			offsets = make([]uint16, len(offsetsData)/2)
			for j := range offsets {
				offsets[j] = binary.LittleEndian.Uint16(offsetsData[j*2 : (j+1)*2])
			}
			i += int(length)
		default:
			if wireType == 2 {
				length, n := binary.Uvarint(data[i:])
				i += n + int(length)
			} else {
				_, n := binary.Uvarint(data[i:])
				i += n
			}
		}
	}

	result := make([]uint64, len(offsets))
	for j, offset := range offsets {
		result[j] = base + uint64(offset)
	}
	return result, nil
}

func parseU32Array(data []byte) ([]uint64, error) {
	var base uint64
	var offsets []uint32

	i := 0
	for i < len(data) {
		tag, n := binary.Uvarint(data[i:])
		if n <= 0 {
			return nil, fmt.Errorf("invalid tag in U32Array")
		}
		i += n

		fieldNum := tag >> 3
		wireType := tag & 0x7

		switch fieldNum {
		case 1: // base
			base, n = binary.Uvarint(data[i:])
			i += n
		case 2: // offsets
			if wireType != 2 {
				return nil, fmt.Errorf("expected length-delimited for offsets")
			}
			length, n := binary.Uvarint(data[i:])
			if n <= 0 {
				return nil, fmt.Errorf("invalid offsets length")
			}
			i += n
			offsetsData := data[i : i+int(length)]
			offsets = make([]uint32, len(offsetsData)/4)
			for j := range offsets {
				offsets[j] = binary.LittleEndian.Uint32(offsetsData[j*4 : (j+1)*4])
			}
			i += int(length)
		default:
			if wireType == 2 {
				length, n := binary.Uvarint(data[i:])
				i += n + int(length)
			} else {
				_, n := binary.Uvarint(data[i:])
				i += n
			}
		}
	}

	result := make([]uint64, len(offsets))
	for j, offset := range offsets {
		result[j] = base + uint64(offset)
	}
	return result, nil
}

func parseU64Array(data []byte) ([]uint64, error) {
	i := 0
	for i < len(data) {
		tag, n := binary.Uvarint(data[i:])
		if n <= 0 {
			return nil, fmt.Errorf("invalid tag in U64Array")
		}
		i += n

		fieldNum := tag >> 3
		wireType := tag & 0x7

		switch fieldNum {
		case 1: // values
			if wireType != 2 {
				return nil, fmt.Errorf("expected length-delimited for values")
			}
			length, n := binary.Uvarint(data[i:])
			if n <= 0 {
				return nil, fmt.Errorf("invalid values length")
			}
			i += n
			valuesData := data[i : i+int(length)]
			values := make([]uint64, len(valuesData)/8)
			for j := range values {
				values[j] = binary.LittleEndian.Uint64(valuesData[j*8 : (j+1)*8])
			}
			return values, nil
		default:
			if wireType == 2 {
				length, n := binary.Uvarint(data[i:])
				i += n + int(length)
			} else {
				_, n := binary.Uvarint(data[i:])
				i += n
			}
		}
	}

	return nil, fmt.Errorf("no values found in U64Array")
}

// MarshalRowIdSequence serializes a RowIdSequence to protobuf-encoded bytes.
// The format is compatible with Lance's RowIdSequence serialization.
func MarshalRowIdSequence(seq *RowIdSequence) ([]byte, error) {
	if seq == nil || len(seq.Segments) == 0 {
		return nil, nil
	}
	var buf []byte
	for _, seg := range seq.Segments {
		segData, err := marshalSegment(seg)
		if err != nil {
			return nil, fmt.Errorf("marshal segment: %w", err)
		}
		// field 1, wire type 2 (length-delimited)
		buf = appendVarint(buf, (1<<3)|2)
		buf = appendVarint(buf, uint64(len(segData)))
		buf = append(buf, segData...)
	}
	return buf, nil
}

func marshalSegment(seg U64Segment) ([]byte, error) {
	switch s := seg.(type) {
	case *RangeSegment:
		inner := marshalRange(s.Start, s.End)
		var buf []byte
		buf = appendVarint(buf, (1<<3)|2) // field 1
		buf = appendVarint(buf, uint64(len(inner)))
		buf = append(buf, inner...)
		return buf, nil

	case *RangeWithHolesSegment:
		inner := marshalRangeWithHoles(s.Start, s.End, s.Holes)
		var buf []byte
		buf = appendVarint(buf, (2<<3)|2) // field 2
		buf = appendVarint(buf, uint64(len(inner)))
		buf = append(buf, inner...)
		return buf, nil

	case *RangeWithBitmapSegment:
		inner := marshalRangeWithBitmap(s.Start, s.End, s.Bitmap)
		var buf []byte
		buf = appendVarint(buf, (3<<3)|2) // field 3
		buf = appendVarint(buf, uint64(len(inner)))
		buf = append(buf, inner...)
		return buf, nil

	case *SortedArraySegment:
		inner := marshalEncodedU64Array(s.Values)
		var buf []byte
		buf = appendVarint(buf, (4<<3)|2) // field 4
		buf = appendVarint(buf, uint64(len(inner)))
		buf = append(buf, inner...)
		return buf, nil

	case *ArraySegment:
		inner := marshalEncodedU64Array(s.Values)
		var buf []byte
		buf = appendVarint(buf, (5<<3)|2) // field 5
		buf = appendVarint(buf, uint64(len(inner)))
		buf = append(buf, inner...)
		return buf, nil

	default:
		return nil, fmt.Errorf("unsupported segment type: %T", seg)
	}
}

func marshalRange(start, end uint64) []byte {
	var buf []byte
	if start != 0 {
		buf = appendVarint(buf, (1<<3)|0) // field 1, varint
		buf = appendVarint(buf, start)
	}
	if end != 0 {
		buf = appendVarint(buf, (2<<3)|0) // field 2, varint
		buf = appendVarint(buf, end)
	}
	return buf
}

func marshalRangeWithHoles(start, end uint64, holes []uint64) []byte {
	var buf []byte
	if start != 0 {
		buf = appendVarint(buf, (1<<3)|0)
		buf = appendVarint(buf, start)
	}
	if end != 0 {
		buf = appendVarint(buf, (2<<3)|0)
		buf = appendVarint(buf, end)
	}
	if len(holes) > 0 {
		inner := marshalEncodedU64Array(holes)
		buf = appendVarint(buf, (3<<3)|2)
		buf = appendVarint(buf, uint64(len(inner)))
		buf = append(buf, inner...)
	}
	return buf
}

func marshalRangeWithBitmap(start, end uint64, bitmap []byte) []byte {
	var buf []byte
	if start != 0 {
		buf = appendVarint(buf, (1<<3)|0)
		buf = appendVarint(buf, start)
	}
	if end != 0 {
		buf = appendVarint(buf, (2<<3)|0)
		buf = appendVarint(buf, end)
	}
	if len(bitmap) > 0 {
		buf = appendVarint(buf, (3<<3)|2) // field 3, length-delimited
		buf = appendVarint(buf, uint64(len(bitmap)))
		buf = append(buf, bitmap...)
	}
	return buf
}

// marshalEncodedU64Array serializes values using U64Array encoding (field 3).
func marshalEncodedU64Array(values []uint64) []byte {
	// Use U64Array (field 3): values as fixed 8-byte little-endian
	valBytes := make([]byte, len(values)*8)
	for i, v := range values {
		binary.LittleEndian.PutUint64(valBytes[i*8:(i+1)*8], v)
	}
	var inner []byte
	inner = appendVarint(inner, (1<<3)|2) // field 1, length-delimited
	inner = appendVarint(inner, uint64(len(valBytes)))
	inner = append(inner, valBytes...)

	var buf []byte
	buf = appendVarint(buf, (3<<3)|2) // field 3 = U64Array
	buf = appendVarint(buf, uint64(len(inner)))
	buf = append(buf, inner...)
	return buf
}

func appendVarint(buf []byte, v uint64) []byte {
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(tmp[:], v)
	return append(buf, tmp[:n]...)
}
