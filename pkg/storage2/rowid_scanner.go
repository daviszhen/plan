package storage2

import (
	"context"
	"fmt"

	"github.com/daviszhen/plan/pkg/chunk"
	"github.com/daviszhen/plan/pkg/common"
	storage2pb "github.com/daviszhen/plan/pkg/storage2/proto"
)

// RowIdScanner provides random access by RowId instead of logical row indices.
// This requires the dataset to have stable row IDs enabled (feature flag 2).
type RowIdScanner struct {
	basePath string
	handler  CommitHandler
	version  uint64
	manifest *Manifest
}

// NewRowIdScanner creates a scanner for RowId-based random access.
func NewRowIdScanner(ctx context.Context, basePath string, handler CommitHandler, version uint64) (*RowIdScanner, error) {
	manifest, err := LoadManifest(ctx, basePath, handler, version)
	if err != nil {
		return nil, err
	}

	// Check if stable row IDs are enabled (feature flag 2)
	if manifest.ReaderFeatureFlags&2 == 0 {
		return nil, fmt.Errorf("stable row IDs not enabled for this dataset (feature flag 2 required)")
	}

	return &RowIdScanner{
		basePath: basePath,
		handler:  handler,
		version:  version,
		manifest: manifest,
	}, nil
}

// TakeByRowIds returns rows at the given RowIds for the current version.
// RowIds must be stable row IDs as stored in the fragment metadata.
// Returns a chunk with rows in the same order as the requested RowIds.
// If a RowId doesn't exist, that position in the result will be null.
func (r *RowIdScanner) TakeByRowIds(ctx context.Context, rowIds []uint64) (*chunk.Chunk, error) {
	if len(rowIds) == 0 {
		return nil, nil
	}

	// Map RowIds to (fragment_id, local_row_index) pairs
	rowLocations, err := r.mapRowIdsToLocations(rowIds)
	if err != nil {
		return nil, err
	}

	// Group by fragment for efficient reading
	rowsByFragment := make(map[uint64][]rowLocationRef)
	for i, loc := range rowLocations {
		if loc.found {
			rowsByFragment[loc.fragmentId] = append(rowsByFragment[loc.fragmentId],
				rowLocationRef{outIndex: i, localRowIndex: loc.localRowIndex})
		}
	}

	if len(rowsByFragment) == 0 {
		// No RowIds found - return empty chunk with correct schema
		return r.createEmptyResultChunk(len(rowIds))
	}

	// Read first fragment to determine column types
	var sampleChunk *chunk.Chunk
	for fragId := range rowsByFragment {
		frag := r.getFragmentById(fragId)
		if frag != nil && len(frag.Files) > 0 && frag.Files[0].Path != "" {
			fullPath := joinPath(r.basePath, frag.Files[0].Path)
			sampleChunk, err = ReadChunkFromFile(fullPath)
			if err != nil {
				return nil, err
			}
			if sampleChunk != nil {
				break
			}
		}
	}

	if sampleChunk == nil {
		return nil, fmt.Errorf("no readable data files found")
	}

	// Create result chunk
	result := r.createResultChunk(sampleChunk, len(rowIds))

	// Read data from each fragment
	for fragId, refs := range rowsByFragment {
		err := r.readFragmentRows(fragId, refs, result)
		if err != nil {
			return nil, err
		}
	}

	return result, nil
}

// rowLocation represents where a RowId is located
type rowLocation struct {
	found         bool
	fragmentId    uint64
	localRowIndex uint64
}

// rowLocationRef tracks where to place a row in the result
type rowLocationRef struct {
	outIndex      int    // index in output chunk
	localRowIndex uint64 // local row index within fragment
}

// mapRowIdsToLocations finds fragment and local row index for each RowId
func (r *RowIdScanner) mapRowIdsToLocations(rowIds []uint64) ([]rowLocation, error) {
	locations := make([]rowLocation, len(rowIds))

	for i, rowId := range rowIds {
		loc := r.findRowIdLocation(rowId)
		locations[i] = loc
	}

	return locations, nil
}

// findRowIdLocation looks up a single RowId in all fragments
func (r *RowIdScanner) findRowIdLocation(rowId uint64) rowLocation {
	for _, frag := range r.manifest.Fragments {
		if frag == nil {
			continue
		}

		// Get row IDs for this fragment
		rowIds, err := r.getRowIdsForFragment(frag)
		if err != nil || rowIds == nil {
			continue
		}

		// Search for the RowId
		for localIdx, id := range rowIds {
			if id == rowId {
				return rowLocation{
					found:         true,
					fragmentId:    frag.Id,
					localRowIndex: uint64(localIdx),
				}
			}
		}
	}

	return rowLocation{found: false}
}

// getRowIdsForFragment extracts RowIds from a fragment's metadata
func (r *RowIdScanner) getRowIdsForFragment(frag *DataFragment) ([]uint64, error) {
	switch seq := frag.RowIdSequence.(type) {
	case *storage2pb.DataFragment_InlineRowIds:
		if seq.InlineRowIds == nil {
			return nil, nil
		}
		// Parse inline RowIds using our RowIdSequence parser
		rowIdSeq, err := ParseRowIdSequence(seq.InlineRowIds)
		if err != nil {
			return nil, fmt.Errorf("failed to parse inline row ids: %w", err)
		}
		return rowIdSeq.Iter(), nil

	case *storage2pb.DataFragment_ExternalRowIds:
		if seq.ExternalRowIds == nil || seq.ExternalRowIds.Path == "" {
			return nil, nil
		}
		// Read external RowIds file
		// fullPath := joinPath(r.basePath, seq.ExternalRowIds.Path)
		// Would need to implement RowIds file format parsing
		return nil, fmt.Errorf("external RowIds parsing not implemented")

	default:
		return nil, nil
	}
}

// getFragmentById finds a fragment by ID
func (r *RowIdScanner) getFragmentById(id uint64) *DataFragment {
	for _, frag := range r.manifest.Fragments {
		if frag != nil && frag.Id == id {
			return frag
		}
	}
	return nil
}

// createEmptyResultChunk creates an empty chunk with the right schema
func (r *RowIdScanner) createEmptyResultChunk(card int) (*chunk.Chunk, error) {
	// This would need to determine schema from manifest.Fields
	// For now, return a simple placeholder
	return nil, fmt.Errorf("empty result chunk creation not fully implemented")
}

// createResultChunk creates a result chunk with the right schema
func (r *RowIdScanner) createResultChunk(sample *chunk.Chunk, card int) *chunk.Chunk {
	// Clone the sample chunk structure but with new cardinality
	result := &chunk.Chunk{}
	colCount := sample.ColumnCount()
	typs := make([]common.LType, colCount)
	for i := 0; i < colCount; i++ {
		typs[i] = sample.Data[i].Typ()
	}
	result.Init(typs, card)
	result.SetCard(card)
	return result
}

// readFragmentRows reads specified rows from a fragment into the result chunk
func (r *RowIdScanner) readFragmentRows(fragId uint64, refs []rowLocationRef, result *chunk.Chunk) error {
	frag := r.getFragmentById(fragId)
	if frag == nil || len(frag.Files) == 0 || frag.Files[0].Path == "" {
		return fmt.Errorf("fragment %d not found or has no data files", fragId)
	}

	fullPath := joinPath(r.basePath, frag.Files[0].Path)
	srcChunk, err := ReadChunkFromFile(fullPath)
	if err != nil {
		return err
	}

	// Copy requested rows
	for _, ref := range refs {
		if int(ref.localRowIndex) >= srcChunk.Card() {
			continue // Skip if local index out of bounds
		}

		for col := 0; col < srcChunk.ColumnCount(); col++ {
			val := srcChunk.Data[col].GetValue(int(ref.localRowIndex))
			result.Data[col].SetValue(ref.outIndex, val)
		}
	}

	return nil
}

// joinPath is a helper to construct file paths
func joinPath(base, rel string) string {
	// Simplified path joining - would need proper implementation
	return base + "/" + rel
}
