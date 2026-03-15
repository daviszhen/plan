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
	"testing"
)

func TestDeletionBitmapBasic(t *testing.T) {
	db := NewDeletionBitmap()

	// Initially empty
	if db.Count() != 0 {
		t.Fatalf("expected count 0, got %d", db.Count())
	}
	if db.IsDeleted(0) {
		t.Fatal("row 0 should not be deleted")
	}

	// Mark some rows
	db.MarkDeleted(5)
	db.MarkDeleted(10)
	db.MarkDeleted(100)

	if db.Count() != 3 {
		t.Fatalf("expected count 3, got %d", db.Count())
	}
	if !db.IsDeleted(5) {
		t.Fatal("row 5 should be deleted")
	}
	if !db.IsDeleted(10) {
		t.Fatal("row 10 should be deleted")
	}
	if !db.IsDeleted(100) {
		t.Fatal("row 100 should be deleted")
	}
	if db.IsDeleted(6) {
		t.Fatal("row 6 should not be deleted")
	}

	// Idempotent marking
	db.MarkDeleted(5)
	if db.Count() != 3 {
		t.Fatalf("expected count 3 after re-marking, got %d", db.Count())
	}
}

func TestDeletionBitmapMarshalUnmarshal(t *testing.T) {
	db := NewDeletionBitmap()
	db.MarkDeleted(1)
	db.MarkDeleted(3)
	db.MarkDeleted(7)
	db.MarkDeleted(42)
	db.MarkDeleted(1000)

	data, err := db.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	db2 := NewDeletionBitmap()
	if err := db2.Unmarshal(data); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if db2.Count() != db.Count() {
		t.Fatalf("count mismatch: %d vs %d", db2.Count(), db.Count())
	}

	for _, id := range []uint64{1, 3, 7, 42, 1000} {
		if !db2.IsDeleted(id) {
			t.Fatalf("row %d should be deleted after unmarshal", id)
		}
	}

	for _, id := range []uint64{0, 2, 4, 5, 6, 8, 43, 999, 1001} {
		if db2.IsDeleted(id) {
			t.Fatalf("row %d should not be deleted after unmarshal", id)
		}
	}
}

func TestDeletionBitmapEmpty(t *testing.T) {
	db := NewDeletionBitmap()

	data, err := db.Marshal()
	if err != nil {
		t.Fatalf("marshal empty: %v", err)
	}

	db2 := NewDeletionBitmap()
	if err := db2.Unmarshal(data); err != nil {
		t.Fatalf("unmarshal empty: %v", err)
	}

	if db2.Count() != 0 {
		t.Fatalf("expected count 0, got %d", db2.Count())
	}
}

func TestDeletionBitmapUnmarshalBitmap(t *testing.T) {
	// Manually construct BITMAP format data:
	// [uint64 maxRowID=15][2 bytes of bitmap data]
	// Rows 0, 3, 7, 8, 15 are deleted
	data := make([]byte, 8+2)
	// maxRowID = 15
	data[0] = 15
	// byte 0: bits 0,3,7 -> 0b10001001 = 0x89
	data[8] = 0x89
	// byte 1: bits 0,7 (= rows 8, 15) -> 0b10000001 = 0x81
	data[9] = 0x81

	db := NewDeletionBitmap()
	if err := db.UnmarshalBitmap(data); err != nil {
		t.Fatalf("unmarshal bitmap: %v", err)
	}

	for _, id := range []uint64{0, 3, 7, 8, 15} {
		if !db.IsDeleted(id) {
			t.Fatalf("row %d should be deleted", id)
		}
	}

	for _, id := range []uint64{1, 2, 4, 5, 6, 9, 10, 11, 12, 13, 14} {
		if db.IsDeleted(id) {
			t.Fatalf("row %d should not be deleted", id)
		}
	}
}

func TestSortUint64s(t *testing.T) {
	tests := []struct {
		input    []uint64
		expected []uint64
	}{
		{nil, nil},
		{[]uint64{}, []uint64{}},
		{[]uint64{1}, []uint64{1}},
		{[]uint64{3, 1, 2}, []uint64{1, 2, 3}},
		{[]uint64{100, 1, 50, 25, 75}, []uint64{1, 25, 50, 75, 100}},
	}

	for _, tt := range tests {
		sortUint64s(tt.input)
		if len(tt.input) != len(tt.expected) {
			t.Fatalf("length mismatch")
		}
		for i := range tt.input {
			if tt.input[i] != tt.expected[i] {
				t.Fatalf("mismatch at %d: got %d, expected %d", i, tt.input[i], tt.expected[i])
			}
		}
	}
}
