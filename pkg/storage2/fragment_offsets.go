package storage2

// ComputeFragmentOffsets returns the starting row offset of each fragment, plus the total row count at the end.
// So len(offsets) == len(manifest.Fragments)+1, and offsets[i] is the start of fragment i; offsets[len] is total rows.
// Uses PhysicalRows for each fragment (logical rows; does not subtract deletion file rows in this implementation).
func ComputeFragmentOffsets(manifest *Manifest) []uint64 {
	if manifest == nil || len(manifest.Fragments) == 0 {
		return nil
	}
	offsets := make([]uint64, 0, len(manifest.Fragments)+1)
	var cur uint64
	for _, f := range manifest.Fragments {
		offsets = append(offsets, cur)
		n := uint64(0)
		if f != nil {
			n = f.PhysicalRows
		}
		cur += n
	}
	offsets = append(offsets, cur)
	return offsets
}

// FragmentRange describes a fragment and its logical offset range [Start, End) in the dataset.
type FragmentRange struct {
	Fragment *DataFragment
	Start    uint64
	End      uint64
}

// FragmentsByOffsetRange returns fragments that overlap the logical row range [start, end).
// Offsets are computed from PhysicalRows; result is in fragment order.
func FragmentsByOffsetRange(manifest *Manifest, start, end uint64) []FragmentRange {
	offsets := ComputeFragmentOffsets(manifest)
	if offsets == nil {
		return nil
	}
	var out []FragmentRange
	for i := 0; i < len(manifest.Fragments); i++ {
		fStart := offsets[i]
		fEnd := offsets[i+1]
		if fEnd <= start || fStart >= end {
			continue
		}
		out = append(out, FragmentRange{
			Fragment: manifest.Fragments[i],
			Start:    fStart,
			End:      fEnd,
		})
	}
	return out
}
