package storage2

import (
	"testing"
)

func TestComputeFragmentOffsets(t *testing.T) {
	m := NewManifest(1)
	m.Fragments = []*DataFragment{
		NewDataFragmentWithRows(0, 10, nil),
		NewDataFragmentWithRows(1, 20, nil),
		NewDataFragmentWithRows(2, 5, nil),
	}
	offsets := ComputeFragmentOffsets(m)
	if len(offsets) != 4 {
		t.Fatalf("len(offsets)=%d want 4", len(offsets))
	}
	if offsets[0] != 0 || offsets[1] != 10 || offsets[2] != 30 || offsets[3] != 35 {
		t.Errorf("offsets=%v", offsets)
	}
}

func TestComputeFragmentOffsetsEmpty(t *testing.T) {
	m := NewManifest(1)
	m.Fragments = nil
	if ComputeFragmentOffsets(m) != nil {
		t.Error("expected nil for empty fragments")
	}
}

func TestFragmentsByOffsetRange(t *testing.T) {
	m := NewManifest(1)
	m.Fragments = []*DataFragment{
		NewDataFragmentWithRows(0, 10, nil),
		NewDataFragmentWithRows(1, 20, nil),
		NewDataFragmentWithRows(2, 5, nil),
	}
	ranges := FragmentsByOffsetRange(m, 5, 25)
	if len(ranges) != 2 {
		t.Fatalf("len(ranges)=%d want 2", len(ranges))
	}
	if ranges[0].Start != 0 || ranges[0].End != 10 || ranges[1].Start != 10 || ranges[1].End != 30 {
		t.Errorf("ranges=%+v", ranges)
	}
}
