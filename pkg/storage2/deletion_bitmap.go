// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package storage2

import (
	"context"
	"encoding/binary"
	"fmt"

	storage2pb "github.com/daviszhen/plan/pkg/storage2/proto"
)

// DeletionBitmap tracks deleted row IDs using a bitmap.
// It supports both ARROW_ARRAY (sorted uint32 array) and BITMAP (bit-packed) formats,
// matching the Lance deletion file specification.
type DeletionBitmap struct {
	deleted map[uint64]struct{}
}

// NewDeletionBitmap creates an empty deletion bitmap.
func NewDeletionBitmap() *DeletionBitmap {
	return &DeletionBitmap{
		deleted: make(map[uint64]struct{}),
	}
}

// MarkDeleted marks a row as deleted.
func (db *DeletionBitmap) MarkDeleted(rowID uint64) {
	db.deleted[rowID] = struct{}{}
}

// IsDeleted checks whether a row is deleted.
func (db *DeletionBitmap) IsDeleted(rowID uint64) bool {
	_, ok := db.deleted[rowID]
	return ok
}

// Count returns the number of deleted rows.
func (db *DeletionBitmap) Count() uint64 {
	return uint64(len(db.deleted))
}

// Marshal serializes the deletion bitmap as a sorted uint32 array (ARROW_ARRAY format).
// Format: [uint32 count][uint32 rowID0][uint32 rowID1]...
func (db *DeletionBitmap) Marshal() ([]byte, error) {
	count := len(db.deleted)
	data := make([]byte, 4+count*4)
	binary.LittleEndian.PutUint32(data[0:4], uint32(count))

	// Collect and sort row IDs
	ids := make([]uint64, 0, count)
	for id := range db.deleted {
		ids = append(ids, id)
	}
	sortUint64s(ids)

	offset := 4
	for _, id := range ids {
		binary.LittleEndian.PutUint32(data[offset:offset+4], uint32(id))
		offset += 4
	}
	return data, nil
}

// Unmarshal deserializes a deletion bitmap from ARROW_ARRAY format.
func (db *DeletionBitmap) Unmarshal(data []byte) error {
	if len(data) < 4 {
		return fmt.Errorf("deletion bitmap data too short: %d bytes", len(data))
	}

	count := binary.LittleEndian.Uint32(data[0:4])
	expected := 4 + int(count)*4
	if len(data) < expected {
		return fmt.Errorf("deletion bitmap data truncated: got %d bytes, expected %d", len(data), expected)
	}

	db.deleted = make(map[uint64]struct{}, count)
	offset := 4
	for i := 0; i < int(count); i++ {
		rowID := binary.LittleEndian.Uint32(data[offset : offset+4])
		db.deleted[uint64(rowID)] = struct{}{}
		offset += 4
	}
	return nil
}

// UnmarshalBitmap deserializes a deletion bitmap from BITMAP format.
// Format: [uint64 maxRowID][packed bits...] where bit i set means row i is deleted.
func (db *DeletionBitmap) UnmarshalBitmap(data []byte) error {
	if len(data) < 8 {
		return fmt.Errorf("bitmap data too short: %d bytes", len(data))
	}

	maxRowID := binary.LittleEndian.Uint64(data[0:8])
	db.deleted = make(map[uint64]struct{})

	bitmapData := data[8:]
	for i := uint64(0); i <= maxRowID && int(i/8) < len(bitmapData); i++ {
		byteIdx := i / 8
		bitIdx := i % 8
		if bitmapData[byteIdx]&(1<<bitIdx) != 0 {
			db.deleted[i] = struct{}{}
		}
	}
	return nil
}

// sortUint64s sorts a uint64 slice in ascending order (insertion sort, good for small slices).
func sortUint64s(s []uint64) {
	for i := 1; i < len(s); i++ {
		key := s[i]
		j := i - 1
		for j >= 0 && s[j] > key {
			s[j+1] = s[j]
			j--
		}
		s[j+1] = key
	}
}

// LoadDeletionBitmap loads a deletion bitmap from storage at the given path.
func LoadDeletionBitmap(ctx context.Context, store ObjectStoreExt, path string, fileType storage2pb.DeletionFile_DeletionFileType) (*DeletionBitmap, error) {
	data, err := store.ReadRange(ctx, path, ReadOptions{})
	if err != nil {
		return nil, fmt.Errorf("read deletion file %s: %w", path, err)
	}

	db := NewDeletionBitmap()
	switch fileType {
	case storage2pb.DeletionFile_ARROW_ARRAY:
		if err := db.Unmarshal(data); err != nil {
			return nil, fmt.Errorf("unmarshal deletion bitmap (arrow_array): %w", err)
		}
	case storage2pb.DeletionFile_BITMAP:
		if err := db.UnmarshalBitmap(data); err != nil {
			return nil, fmt.Errorf("unmarshal deletion bitmap (bitmap): %w", err)
		}
	default:
		return nil, fmt.Errorf("unsupported deletion file type: %v", fileType)
	}

	return db, nil
}

// SaveDeletionBitmap saves a deletion bitmap to storage at the given path.
func SaveDeletionBitmap(ctx context.Context, store ObjectStore, path string, db *DeletionBitmap) error {
	data, err := db.Marshal()
	if err != nil {
		return fmt.Errorf("marshal deletion bitmap: %w", err)
	}
	return store.Write(path, data)
}

// LoadFragmentDeletionBitmap loads the deletion bitmap for a specific fragment.
// Returns nil if the fragment has no deletion file.
func LoadFragmentDeletionBitmap(ctx context.Context, store ObjectStoreExt, basePath string, frag *DataFragment) (*DeletionBitmap, error) {
	if frag.DeletionFile == nil || frag.DeletionFile.NumDeletedRows == 0 {
		return nil, nil
	}

	// Build the deletion file path: _deletions/{fragment_id}-{read_version}-{id}.{ext}
	ext := "arrow"
	if frag.DeletionFile.FileType == storage2pb.DeletionFile_BITMAP {
		ext = "bin"
	}
	path := fmt.Sprintf("%s/_deletions/%d-%d-%d.%s",
		basePath, frag.Id, frag.DeletionFile.ReadVersion, frag.DeletionFile.Id, ext)

	return LoadDeletionBitmap(ctx, store, path, frag.DeletionFile.FileType)
}
