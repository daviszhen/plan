package storage2

import (
	"context"
	"path/filepath"

	"github.com/daviszhen/plan/pkg/chunk"
	"github.com/daviszhen/plan/pkg/common"
)

// ScanChunks loads all data files referenced by the manifest of the given version.
// It returns their chunks in fragment/file order. This is a minimal scanner used
// for tests and simple verification; it does not apply predicates or projection.
func ScanChunks(ctx context.Context, basePath string, handler CommitHandler, version uint64) ([]*chunk.Chunk, error) {
	m, err := LoadManifest(ctx, basePath, handler, version)
	if err != nil {
		return nil, err
	}
	var out []*chunk.Chunk
	for _, f := range m.Fragments {
		if f == nil {
			continue
		}
		for _, df := range f.Files {
			if df == nil || df.Path == "" {
				continue
			}
			fullPath := filepath.Join(basePath, df.Path)
			c, err := ReadChunkFromFile(fullPath)
			if err != nil {
				return nil, err
			}
			out = append(out, c)
		}
	}
	return out, nil
}

// TakeRows returns a new chunk containing the rows at the given zero-based
// logical row indices for the specified version. It is a minimal implementation
// intended for tests; it currently assumes that each fragment has at least one
// data file and uses the first file per fragment.
func TakeRows(ctx context.Context, basePath string, handler CommitHandler, version uint64, indices []uint64) (*chunk.Chunk, error) {
	if len(indices) == 0 {
		return nil, nil
	}
	m, err := LoadManifest(ctx, basePath, handler, version)
	if err != nil {
		return nil, err
	}
	if len(m.Fragments) == 0 {
		return nil, nil
	}

	// Pre-compute offsets to map global row index -> fragment + local row index.
	offsets := ComputeFragmentOffsets(m)
	if offsets == nil {
		return nil, nil
	}

	// Group requested rows by fragment index.
	type rowRef struct {
		outRow   int
		localRow uint64
	}
	rowsByFrag := make(map[int][]rowRef)
	var totalRows uint64 = offsets[len(offsets)-1]
	for outRow, idx := range indices {
		if idx >= totalRows {
			continue
		}
		// find fragment i such that offsets[i] <= idx < offsets[i+1]
		var fragIdx int
		for i := 0; i < len(m.Fragments); i++ {
			if idx >= offsets[i] && idx < offsets[i+1] {
				fragIdx = i
				break
			}
		}
		local := idx - offsets[fragIdx]
		rowsByFrag[fragIdx] = append(rowsByFrag[fragIdx], rowRef{
			outRow:   outRow,
			localRow: local,
		})
	}

	// If no valid indices, return nil.
	if len(rowsByFrag) == 0 {
		return nil, nil
	}

	// Read first non-empty fragment to determine column types.
	var sampleChunk *chunk.Chunk
	for fragIdx := range rowsByFrag {
		frag := m.Fragments[fragIdx]
		if frag == nil || len(frag.Files) == 0 || frag.Files[0].Path == "" {
			continue
		}
		df := frag.Files[0]
		fullPath := filepath.Join(basePath, df.Path)
		sampleChunk, err = ReadChunkFromFile(fullPath)
		if err != nil {
			return nil, err
		}
		if sampleChunk != nil {
			break
		}
	}
	if sampleChunk == nil {
		return nil, nil
	}

	colCount := sampleChunk.ColumnCount()
	typs := make([]common.LType, colCount)
	for j := 0; j < colCount; j++ {
		typs[j] = sampleChunk.Data[j].Typ()
	}
	dst := &chunk.Chunk{}
	dst.Init(typs, len(indices))
	dst.SetCard(len(indices))

	// For each fragment, read its first data file and copy requested rows.
	for fragIdx, refs := range rowsByFrag {
		frag := m.Fragments[fragIdx]
		if frag == nil || len(frag.Files) == 0 || frag.Files[0].Path == "" {
			continue
		}
		df := frag.Files[0]
		fullPath := filepath.Join(basePath, df.Path)
		src, err := ReadChunkFromFile(fullPath)
		if err != nil {
			return nil, err
		}
		for _, r := range refs {
			if int(r.localRow) >= src.Card() {
				continue
			}
			for col := 0; col < colCount; col++ {
				val := src.Data[col].GetValue(int(r.localRow))
				dst.Data[col].SetValue(r.outRow, val)
			}
		}
	}
	return dst, nil
}

