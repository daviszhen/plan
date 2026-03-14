package storage2

import (
	"testing"
)

func TestRangeSegment(t *testing.T) {
	seg := &RangeSegment{Start: 10, End: 20}

	if seg.Len() != 10 {
		t.Errorf("Len() = %d, want 10", seg.Len())
	}

	// Test Get
	if val, ok := seg.Get(0); !ok || val != 10 {
		t.Errorf("Get(0) = (%d, %v), want (10, true)", val, ok)
	}
	if val, ok := seg.Get(5); !ok || val != 15 {
		t.Errorf("Get(5) = (%d, %v), want (15, true)", val, ok)
	}
	if _, ok := seg.Get(10); ok {
		t.Error("Get(10) should return false")
	}

	// Test Contains
	if !seg.Contains(10) {
		t.Error("Contains(10) should be true")
	}
	if !seg.Contains(19) {
		t.Error("Contains(19) should be true")
	}
	if seg.Contains(9) {
		t.Error("Contains(9) should be false")
	}
	if seg.Contains(20) {
		t.Error("Contains(20) should be false")
	}

	// Test Position
	if pos := seg.Position(10); pos != 0 {
		t.Errorf("Position(10) = %d, want 0", pos)
	}
	if pos := seg.Position(15); pos != 5 {
		t.Errorf("Position(15) = %d, want 5", pos)
	}
	if pos := seg.Position(5); pos != -1 {
		t.Errorf("Position(5) = %d, want -1", pos)
	}

	// Test Iter
	vals := seg.Iter()
	if len(vals) != 10 {
		t.Errorf("Iter() returned %d values, want 10", len(vals))
	}
}

func TestRangeWithHolesSegment(t *testing.T) {
	seg := &RangeWithHolesSegment{
		Start: 10,
		End:   20,
		Holes: []uint64{12, 15, 18},
	}

	// Len: 10 - 3 = 7
	if seg.Len() != 7 {
		t.Errorf("Len() = %d, want 7", seg.Len())
	}

	// Test Contains
	if !seg.Contains(10) {
		t.Error("Contains(10) should be true")
	}
	if seg.Contains(12) {
		t.Error("Contains(12) should be false (hole)")
	}
	if seg.Contains(15) {
		t.Error("Contains(15) should be false (hole)")
	}
	if !seg.Contains(14) {
		t.Error("Contains(14) should be true")
	}

	// Test Position
	if pos := seg.Position(10); pos != 0 {
		t.Errorf("Position(10) = %d, want 0", pos)
	}
	if pos := seg.Position(11); pos != 1 {
		t.Errorf("Position(11) = %d, want 1", pos)
	}
	// 12 is a hole, so 13 is at position 1 (10, 11, 13)
	if pos := seg.Position(13); pos != 2 {
		t.Errorf("Position(13) = %d, want 2", pos)
	}
	if pos := seg.Position(12); pos != -1 {
		t.Errorf("Position(12) = %d, want -1", pos)
	}

	// Test Iter
	vals := seg.Iter()
	expected := []uint64{10, 11, 13, 14, 16, 17, 19}
	if len(vals) != len(expected) {
		t.Errorf("Iter() returned %d values, want %d", len(vals), len(expected))
	}
	for i, v := range vals {
		if v != expected[i] {
			t.Errorf("Iter()[%d] = %d, want %d", i, v, expected[i])
		}
	}
}

func TestRangeWithBitmapSegment(t *testing.T) {
	// Create bitmap: values 0, 2, 4, 6, 8 are present (every other value)
	// In binary: 01010101 = 0x55
	bitmap := []byte{0x55}
	seg := &RangeWithBitmapSegment{
		Start:  0,
		End:    8,
		Bitmap: bitmap,
	}

	// Len: 4 values present
	if seg.Len() != 4 {
		t.Errorf("Len() = %d, want 4", seg.Len())
	}

	// Test Contains
	if !seg.Contains(0) {
		t.Error("Contains(0) should be true")
	}
	if seg.Contains(1) {
		t.Error("Contains(1) should be false")
	}
	if !seg.Contains(2) {
		t.Error("Contains(2) should be true")
	}

	// Test Position
	if pos := seg.Position(0); pos != 0 {
		t.Errorf("Position(0) = %d, want 0", pos)
	}
	if pos := seg.Position(2); pos != 1 {
		t.Errorf("Position(2) = %d, want 1", pos)
	}
	if pos := seg.Position(4); pos != 2 {
		t.Errorf("Position(4) = %d, want 2", pos)
	}
	if pos := seg.Position(1); pos != -1 {
		t.Errorf("Position(1) = %d, want -1", pos)
	}
}

func TestSortedArraySegment(t *testing.T) {
	seg := &SortedArraySegment{Values: []uint64{1, 5, 10, 20, 100}}

	if seg.Len() != 5 {
		t.Errorf("Len() = %d, want 5", seg.Len())
	}

	// Test Get
	if val, ok := seg.Get(0); !ok || val != 1 {
		t.Errorf("Get(0) = (%d, %v), want (1, true)", val, ok)
	}
	if val, ok := seg.Get(4); !ok || val != 100 {
		t.Errorf("Get(4) = (%d, %v), want (100, true)", val, ok)
	}

	// Test Contains
	if !seg.Contains(5) {
		t.Error("Contains(5) should be true")
	}
	if seg.Contains(6) {
		t.Error("Contains(6) should be false")
	}

	// Test Position
	if pos := seg.Position(1); pos != 0 {
		t.Errorf("Position(1) = %d, want 0", pos)
	}
	if pos := seg.Position(100); pos != 4 {
		t.Errorf("Position(100) = %d, want 4", pos)
	}
	if pos := seg.Position(50); pos != -1 {
		t.Errorf("Position(50) = %d, want -1", pos)
	}
}

func TestArraySegment(t *testing.T) {
	seg := &ArraySegment{Values: []uint64{100, 5, 50, 1}}

	if seg.Len() != 4 {
		t.Errorf("Len() = %d, want 4", seg.Len())
	}

	// Test Contains
	if !seg.Contains(50) {
		t.Error("Contains(50) should be true")
	}
	if seg.Contains(99) {
		t.Error("Contains(99) should be false")
	}

	// Test Position
	if pos := seg.Position(100); pos != 0 {
		t.Errorf("Position(100) = %d, want 0", pos)
	}
	if pos := seg.Position(1); pos != 3 {
		t.Errorf("Position(1) = %d, want 3", pos)
	}
}

func TestRowIdSequence(t *testing.T) {
	// Create a sequence with multiple segments
	seq := &RowIdSequence{
		Segments: []U64Segment{
			&RangeSegment{Start: 0, End: 5}, // 0, 1, 2, 3, 4
			&SortedArraySegment{Values: []uint64{10, 20, 30}},
			&RangeSegment{Start: 100, End: 103}, // 100, 101, 102
		},
	}

	// Total length: 5 + 3 + 3 = 11
	if seq.Len() != 11 {
		t.Errorf("Len() = %d, want 11", seq.Len())
	}

	// Test Get across segments
	if val, ok := seq.Get(0); !ok || val != 0 {
		t.Errorf("Get(0) = (%d, %v), want (0, true)", val, ok)
	}
	if val, ok := seq.Get(5); !ok || val != 10 {
		t.Errorf("Get(5) = (%d, %v), want (10, true)", val, ok)
	}
	if val, ok := seq.Get(8); !ok || val != 100 {
		t.Errorf("Get(8) = (%d, %v), want (100, true)", val, ok)
	}

	// Test Contains
	if !seq.Contains(3) {
		t.Error("Contains(3) should be true")
	}
	if !seq.Contains(20) {
		t.Error("Contains(20) should be true")
	}
	if !seq.Contains(101) {
		t.Error("Contains(101) should be true")
	}
	if seq.Contains(50) {
		t.Error("Contains(50) should be false")
	}

	// Test Position
	if pos := seq.Position(0); pos != 0 {
		t.Errorf("Position(0) = %d, want 0", pos)
	}
	if pos := seq.Position(10); pos != 5 {
		t.Errorf("Position(10) = %d, want 5", pos)
	}
	if pos := seq.Position(100); pos != 8 {
		t.Errorf("Position(100) = %d, want 8", pos)
	}
	if pos := seq.Position(50); pos != -1 {
		t.Errorf("Position(50) = %d, want -1", pos)
	}

	// Test Iter
	vals := seq.Iter()
	if len(vals) != 11 {
		t.Errorf("Iter() returned %d values, want 11", len(vals))
	}
}

func TestNewRowIdSequenceFromRange(t *testing.T) {
	seq := NewRowIdSequenceFromRange(10, 20)
	if seq.Len() != 10 {
		t.Errorf("Len() = %d, want 10", seq.Len())
	}
	if !seq.Contains(15) {
		t.Error("Contains(15) should be true")
	}
}

func TestNewRowIdSequenceFromSlice(t *testing.T) {
	// Sorted slice
	sortedSeq := NewRowIdSequenceFromSlice([]uint64{1, 5, 10, 20})
	if sortedSeq.Len() != 4 {
		t.Errorf("Len() = %d, want 4", sortedSeq.Len())
	}

	// Unsorted slice
	unsortedSeq := NewRowIdSequenceFromSlice([]uint64{20, 5, 1, 10})
	if unsortedSeq.Len() != 4 {
		t.Errorf("Len() = %d, want 4", unsortedSeq.Len())
	}
}
