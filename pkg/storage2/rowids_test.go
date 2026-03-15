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

// ---- NewU64SegmentFromSlice ----

func TestNewU64SegmentFromSlice_Range(t *testing.T) {
	seg := NewU64SegmentFromSlice([]uint64{10, 11, 12, 13, 14})
	rs, ok := seg.(*RangeSegment)
	if !ok {
		t.Fatalf("expected RangeSegment, got %T", seg)
	}
	if rs.Start != 10 || rs.End != 15 {
		t.Errorf("RangeSegment = [%d,%d), want [10,15)", rs.Start, rs.End)
	}
}

func TestNewU64SegmentFromSlice_SortedArray(t *testing.T) {
	// Sparse sorted values should produce SortedArraySegment.
	seg := NewU64SegmentFromSlice([]uint64{1, 100, 200, 300})
	if _, ok := seg.(*SortedArraySegment); !ok {
		t.Fatalf("expected SortedArraySegment, got %T", seg)
	}
	if seg.Len() != 4 {
		t.Errorf("Len() = %d, want 4", seg.Len())
	}
}

func TestNewU64SegmentFromSlice_Array(t *testing.T) {
	// Unsorted values should produce ArraySegment.
	seg := NewU64SegmentFromSlice([]uint64{5, 2, 8, 1})
	if _, ok := seg.(*ArraySegment); !ok {
		t.Fatalf("expected ArraySegment, got %T", seg)
	}
	if seg.Len() != 4 {
		t.Errorf("Len() = %d, want 4", seg.Len())
	}
}

// ---- Extend ----

func TestRowIdSequenceExtend_Simple(t *testing.T) {
	a := NewRowIdSequenceFromSlice([]uint64{1, 2, 3})
	b := NewRowIdSequenceFromSlice([]uint64{10, 11, 12})
	a.Extend(b)
	if a.Len() != 6 {
		t.Errorf("Len() = %d, want 6", a.Len())
	}
}

func TestRowIdSequenceExtend_MergeAdjacentRanges(t *testing.T) {
	a := NewRowIdSequenceFromRange(0, 5)  // [0,5)
	b := NewRowIdSequenceFromRange(5, 10) // [5,10)
	a.Extend(b)
	if a.Len() != 10 {
		t.Errorf("Len() = %d, want 10", a.Len())
	}
	// Should have been merged into a single Range segment.
	if len(a.Segments) != 1 {
		t.Errorf("segments = %d, want 1 (merged)", len(a.Segments))
	}
	rs, ok := a.Segments[0].(*RangeSegment)
	if !ok {
		t.Fatalf("expected RangeSegment, got %T", a.Segments[0])
	}
	if rs.Start != 0 || rs.End != 10 {
		t.Errorf("merged range = [%d,%d), want [0,10)", rs.Start, rs.End)
	}
}

func TestRowIdSequenceExtend_NoMerge(t *testing.T) {
	a := NewRowIdSequenceFromRange(0, 5)
	b := NewRowIdSequenceFromRange(6, 10) // gap at 5
	a.Extend(b)
	if a.Len() != 9 {
		t.Errorf("Len() = %d, want 9", a.Len())
	}
	if len(a.Segments) != 2 {
		t.Errorf("segments = %d, want 2 (no merge due to gap)", len(a.Segments))
	}
}

func TestRowIdSequenceExtend_Empty(t *testing.T) {
	a := NewRowIdSequenceFromRange(0, 5)
	a.Extend(&RowIdSequence{})
	if a.Len() != 5 {
		t.Errorf("Len() = %d, want 5", a.Len())
	}
	empty := &RowIdSequence{}
	empty.Extend(NewRowIdSequenceFromRange(0, 3))
	if empty.Len() != 3 {
		t.Errorf("Len() = %d, want 3", empty.Len())
	}
}

// ---- Delete ----

func TestRowIdSequenceDelete(t *testing.T) {
	seq := NewRowIdSequenceFromRange(0, 10) // 0..9
	seq.Delete([]uint64{3, 7})
	if seq.Len() != 8 {
		t.Errorf("Len() = %d, want 8", seq.Len())
	}
	if seq.Contains(3) {
		t.Error("should not contain 3 after delete")
	}
	if seq.Contains(7) {
		t.Error("should not contain 7 after delete")
	}
	if !seq.Contains(0) || !seq.Contains(9) {
		t.Error("should still contain 0 and 9")
	}
}

func TestRowIdSequenceDelete_AllFromSegment(t *testing.T) {
	seq := NewRowIdSequenceFromRange(0, 3) // 0,1,2
	seq.Delete([]uint64{0, 1, 2})
	if seq.Len() != 0 {
		t.Errorf("Len() = %d, want 0", seq.Len())
	}
}

func TestRowIdSequenceDelete_NoOp(t *testing.T) {
	seq := NewRowIdSequenceFromRange(0, 5)
	seq.Delete([]uint64{99}) // not present
	if seq.Len() != 5 {
		t.Errorf("Len() = %d, want 5", seq.Len())
	}
}

// ---- Slice ----

func TestRowIdSequenceSlice(t *testing.T) {
	seq := NewRowIdSequenceFromRange(0, 10) // 0..9
	sliced := seq.Slice(3, 4)               // values at positions 3,4,5,6 → 3,4,5,6
	if sliced.Len() != 4 {
		t.Errorf("Len() = %d, want 4", sliced.Len())
	}
	vals := sliced.Iter()
	expected := []uint64{3, 4, 5, 6}
	for i, e := range expected {
		if vals[i] != e {
			t.Errorf("vals[%d] = %d, want %d", i, vals[i], e)
		}
	}
}

func TestRowIdSequenceSlice_MultiSegment(t *testing.T) {
	a := NewRowIdSequenceFromRange(0, 5)   // 0,1,2,3,4
	b := NewRowIdSequenceFromRange(10, 15) // 10,11,12,13,14
	a.Segments = append(a.Segments, b.Segments...)
	// total 10 values: 0,1,2,3,4,10,11,12,13,14
	sliced := a.Slice(3, 4) // positions 3,4,5,6 → values 3,4,10,11
	if sliced.Len() != 4 {
		t.Errorf("Len() = %d, want 4", sliced.Len())
	}
	vals := sliced.Iter()
	expected := []uint64{3, 4, 10, 11}
	for i, e := range expected {
		if vals[i] != e {
			t.Errorf("vals[%d] = %d, want %d", i, vals[i], e)
		}
	}
}

func TestRowIdSequenceSlice_Empty(t *testing.T) {
	seq := NewRowIdSequenceFromRange(0, 10)
	sliced := seq.Slice(0, 0)
	if sliced.Len() != 0 {
		t.Errorf("Len() = %d, want 0", sliced.Len())
	}
}

// TestRowIdSequenceRoundtrip verifies Marshal -> Parse roundtrip for all segment types.
func TestRowIdSequenceRoundtrip(t *testing.T) {
	tests := []struct {
		name string
		seq  *RowIdSequence
	}{
		{
			name: "Range",
			seq:  NewRowIdSequenceFromRange(10, 20),
		},
		{
			name: "SortedArray",
			seq:  &RowIdSequence{Segments: []U64Segment{&SortedArraySegment{Values: []uint64{1, 5, 10, 20, 100}}}},
		},
		{
			name: "Array",
			seq:  &RowIdSequence{Segments: []U64Segment{&ArraySegment{Values: []uint64{100, 5, 50, 1}}}},
		},
		{
			name: "MultiSegment",
			seq: &RowIdSequence{
				Segments: []U64Segment{
					&RangeSegment{Start: 0, End: 5},
					&SortedArraySegment{Values: []uint64{10, 20, 30}},
					&RangeSegment{Start: 100, End: 103},
				},
			},
		},
		{
			name: "RangeWithHoles",
			seq:  &RowIdSequence{Segments: []U64Segment{&RangeWithHolesSegment{Start: 10, End: 20, Holes: []uint64{12, 15, 18}}}},
		},
		{
			name: "RangeWithBitmap",
			seq:  &RowIdSequence{Segments: []U64Segment{&RangeWithBitmapSegment{Start: 0, End: 8, Bitmap: []byte{0x55}}}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := MarshalRowIdSequence(tt.seq)
			if err != nil {
				t.Fatalf("MarshalRowIdSequence: %v", err)
			}
			if len(data) == 0 {
				t.Fatal("MarshalRowIdSequence returned empty data")
			}

			restored, err := ParseRowIdSequence(data)
			if err != nil {
				t.Fatalf("ParseRowIdSequence: %v", err)
			}

			// Compare values
			origVals := tt.seq.Iter()
			restoredVals := restored.Iter()
			if len(origVals) != len(restoredVals) {
				t.Fatalf("value count: got %d want %d", len(restoredVals), len(origVals))
			}
			for i := range origVals {
				if origVals[i] != restoredVals[i] {
					t.Errorf("value[%d]: got %d want %d", i, restoredVals[i], origVals[i])
				}
			}
		})
	}
}

// TestU64SegmentEncodeDecode verifies that each segment type is correctly selected by NewU64SegmentFromSlice.
func TestU64SegmentEncodeDecode(t *testing.T) {
	// Contiguous range -> RangeSegment
	t.Run("ContiguousRange", func(t *testing.T) {
		seg := NewU64SegmentFromSlice([]uint64{10, 11, 12, 13, 14})
		rs, ok := seg.(*RangeSegment)
		if !ok {
			t.Fatalf("expected RangeSegment, got %T", seg)
		}
		if rs.Start != 10 || rs.End != 15 {
			t.Errorf("range: [%d,%d) want [10,15)", rs.Start, rs.End)
		}
	})

	// Sorted non-contiguous sparse -> SortedArraySegment
	t.Run("SortedArray", func(t *testing.T) {
		seg := NewU64SegmentFromSlice([]uint64{1, 100, 200, 300})
		if _, ok := seg.(*SortedArraySegment); !ok {
			t.Fatalf("expected SortedArraySegment, got %T", seg)
		}
	})

	// Unsorted -> ArraySegment
	t.Run("Unsorted", func(t *testing.T) {
		seg := NewU64SegmentFromSlice([]uint64{5, 2, 8, 1})
		if _, ok := seg.(*ArraySegment); !ok {
			t.Fatalf("expected ArraySegment, got %T", seg)
		}
	})

	// Single element -> RangeSegment
	t.Run("SingleElement", func(t *testing.T) {
		seg := NewU64SegmentFromSlice([]uint64{42})
		rs, ok := seg.(*RangeSegment)
		if !ok {
			t.Fatalf("expected RangeSegment for single element, got %T", seg)
		}
		if rs.Start != 42 || rs.End != 43 {
			t.Errorf("range: [%d,%d) want [42,43)", rs.Start, rs.End)
		}
	})

	// Empty -> RangeSegment{0,0}
	t.Run("Empty", func(t *testing.T) {
		seg := NewU64SegmentFromSlice(nil)
		if seg.Len() != 0 {
			t.Errorf("Len: got %d want 0", seg.Len())
		}
	})
}

// TestStableRowIdBasic verifies basic stable row ID operations:
// create, extend, delete, and verify positions remain consistent.
func TestStableRowIdBasic(t *testing.T) {
	// Create initial row IDs for a fragment with 100 rows
	seq := NewRowIdSequenceFromRange(0, 100)
	if seq.Len() != 100 {
		t.Errorf("initial Len: got %d want 100", seq.Len())
	}

	// Verify row IDs are stable
	for i := 0; i < 100; i++ {
		v, ok := seq.Get(i)
		if !ok || v != uint64(i) {
			t.Errorf("Get(%d): got (%d, %v) want (%d, true)", i, v, ok, i)
		}
	}

	// Append more rows (new fragment)
	seq2 := NewRowIdSequenceFromRange(100, 200)
	seq.Extend(seq2)
	if seq.Len() != 200 {
		t.Errorf("after extend Len: got %d want 200", seq.Len())
	}

	// Row IDs should still be stable after extend
	v, ok := seq.Get(0)
	if !ok || v != 0 {
		t.Error("row 0 should still be 0 after extend")
	}
	v, ok = seq.Get(150)
	if !ok || v != 150 {
		t.Errorf("row 150: got %d want 150", v)
	}

	// Delete some rows
	seq.Delete([]uint64{50, 51, 52})
	if seq.Len() != 197 {
		t.Errorf("after delete Len: got %d want 197", seq.Len())
	}
	if seq.Contains(50) || seq.Contains(51) || seq.Contains(52) {
		t.Error("deleted rows should not be contained")
	}
	// Remaining rows should still be addressable
	if !seq.Contains(0) || !seq.Contains(49) || !seq.Contains(53) || !seq.Contains(199) {
		t.Error("non-deleted rows should still be contained")
	}

	// Roundtrip the final sequence
	data, err := MarshalRowIdSequence(seq)
	if err != nil {
		t.Fatalf("MarshalRowIdSequence: %v", err)
	}
	restored, err := ParseRowIdSequence(data)
	if err != nil {
		t.Fatalf("ParseRowIdSequence: %v", err)
	}
	if restored.Len() != seq.Len() {
		t.Errorf("restored Len: got %d want %d", restored.Len(), seq.Len())
	}
}
