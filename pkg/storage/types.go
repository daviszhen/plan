// Copyright 2023-2024 daviszhen
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package storage

import (
	"math"
	"unsafe"

	"github.com/daviszhen/plan/pkg/util"
)

const (
	SECTOR_SIZE       uint64  = 4096
	BLOCK_HEADER_SIZE uint64  = uint64(unsafe.Sizeof(uint64(0)))
	BLOCK_ALLOC_SIZE  uint64  = 1 << 18
	BLOCK_SIZE        uint64  = BLOCK_ALLOC_SIZE - BLOCK_HEADER_SIZE
	MAX_BLOCK         BlockID = 4611686018427388000

	UNDO_ENTRY_HEADER_SIZE           = 8
	MAX_ROW_ID               RowType = 4611686018427388000
	COLUMN_IDENTIFIER_ROW_ID IdxType = math.MaxUint64
	TRANSACTION_ID_START     uint64  = 4611686018427388000

	FILE_HEADER_SIZE uint64 = 4096
	BLOCK_START             = FILE_HEADER_SIZE * 3

	BlockIDSize             = uint64(8)
	MAXIMUM_QUERY_ID uint64 = math.MaxUint64
)

type BlockID int64

func AllocSize(sz uint64) uint64 {
	return util.AlignValue(sz+BLOCK_HEADER_SIZE, SECTOR_SIZE)
}

type TxnType uint64
type RowType int64
type IdxType uint64
