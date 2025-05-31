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

package compute

import (
	"testing"

	"github.com/daviszhen/plan/pkg/common"
)

const (
	NOT_CAST      = 0
	EXPLICIT_CAST = 1
	IMPLICIT_CAST = 2
)

var typeMatrix = [120][120]int{}

func fillTypeMatrix() {
	//blob
	typeMatrix[common.LTID_BLOB][common.LTID_BIT] = EXPLICIT_CAST
	typeMatrix[common.LTID_BLOB][common.LTID_VARCHAR] = EXPLICIT_CAST
	//interval
	typeMatrix[common.LTID_INTERVAL][common.LTID_VARCHAR] = IMPLICIT_CAST
	//date
	typeMatrix[common.LTID_DATE][common.LTID_TIMESTAMP] = IMPLICIT_CAST
	typeMatrix[common.LTID_DATE][common.LTID_VARCHAR] = IMPLICIT_CAST
	//time
	typeMatrix[common.LTID_TIME][common.LTID_VARCHAR] = IMPLICIT_CAST
	//timestamp
	typeMatrix[common.LTID_TIMESTAMP][common.LTID_DATE] = EXPLICIT_CAST
	typeMatrix[common.LTID_TIMESTAMP][common.LTID_TIME] = EXPLICIT_CAST
	typeMatrix[common.LTID_TIMESTAMP][common.LTID_VARCHAR] = IMPLICIT_CAST
	//boolean
	typeMatrix[common.LTID_BOOLEAN][common.LTID_BIT] = EXPLICIT_CAST
	typeMatrix[common.LTID_BOOLEAN][common.LTID_DOUBLE] = EXPLICIT_CAST
	typeMatrix[common.LTID_BOOLEAN][common.LTID_FLOAT] = EXPLICIT_CAST
	typeMatrix[common.LTID_BOOLEAN][common.LTID_HUGEINT] = EXPLICIT_CAST
	typeMatrix[common.LTID_BOOLEAN][common.LTID_BIGINT] = EXPLICIT_CAST
	typeMatrix[common.LTID_BOOLEAN][common.LTID_INTEGER] = EXPLICIT_CAST
	typeMatrix[common.LTID_BOOLEAN][common.LTID_SMALLINT] = EXPLICIT_CAST
	typeMatrix[common.LTID_BOOLEAN][common.LTID_TINYINT] = EXPLICIT_CAST
	typeMatrix[common.LTID_BOOLEAN][common.LTID_UBIGINT] = EXPLICIT_CAST
	typeMatrix[common.LTID_BOOLEAN][common.LTID_UINTEGER] = EXPLICIT_CAST
	typeMatrix[common.LTID_BOOLEAN][common.LTID_USMALLINT] = EXPLICIT_CAST
	typeMatrix[common.LTID_BOOLEAN][common.LTID_UTINYINT] = EXPLICIT_CAST
	typeMatrix[common.LTID_BOOLEAN][common.LTID_DECIMAL] = EXPLICIT_CAST
	typeMatrix[common.LTID_BOOLEAN][common.LTID_VARCHAR] = IMPLICIT_CAST
	//bit
	typeMatrix[common.LTID_BIT][common.LTID_BLOB] = EXPLICIT_CAST
	typeMatrix[common.LTID_BIT][common.LTID_DOUBLE] = EXPLICIT_CAST
	typeMatrix[common.LTID_BIT][common.LTID_FLOAT] = EXPLICIT_CAST
	typeMatrix[common.LTID_BIT][common.LTID_HUGEINT] = EXPLICIT_CAST
	typeMatrix[common.LTID_BIT][common.LTID_BIGINT] = EXPLICIT_CAST
	typeMatrix[common.LTID_BIT][common.LTID_INTEGER] = EXPLICIT_CAST
	typeMatrix[common.LTID_BIT][common.LTID_UBIGINT] = EXPLICIT_CAST
	typeMatrix[common.LTID_BIT][common.LTID_UINTEGER] = EXPLICIT_CAST
	typeMatrix[common.LTID_BIT][common.LTID_VARCHAR] = IMPLICIT_CAST
	//double
	typeMatrix[common.LTID_DOUBLE][common.LTID_BOOLEAN] = EXPLICIT_CAST
	typeMatrix[common.LTID_DOUBLE][common.LTID_BIT] = EXPLICIT_CAST
	typeMatrix[common.LTID_DOUBLE][common.LTID_VARCHAR] = IMPLICIT_CAST
	//float
	typeMatrix[common.LTID_FLOAT][common.LTID_BOOLEAN] = EXPLICIT_CAST
	typeMatrix[common.LTID_FLOAT][common.LTID_BIT] = EXPLICIT_CAST
	typeMatrix[common.LTID_FLOAT][common.LTID_DOUBLE] = IMPLICIT_CAST
	typeMatrix[common.LTID_FLOAT][common.LTID_VARCHAR] = IMPLICIT_CAST
	//hugeint
	typeMatrix[common.LTID_HUGEINT][common.LTID_BOOLEAN] = EXPLICIT_CAST
	typeMatrix[common.LTID_HUGEINT][common.LTID_BIT] = EXPLICIT_CAST
	typeMatrix[common.LTID_HUGEINT][common.LTID_DOUBLE] = IMPLICIT_CAST
	typeMatrix[common.LTID_HUGEINT][common.LTID_FLOAT] = IMPLICIT_CAST
	typeMatrix[common.LTID_HUGEINT][common.LTID_DECIMAL] = IMPLICIT_CAST
	typeMatrix[common.LTID_HUGEINT][common.LTID_VARCHAR] = IMPLICIT_CAST
	//bigint
	typeMatrix[common.LTID_BIGINT][common.LTID_BOOLEAN] = EXPLICIT_CAST
	typeMatrix[common.LTID_BIGINT][common.LTID_BIT] = EXPLICIT_CAST
	typeMatrix[common.LTID_BIGINT][common.LTID_DOUBLE] = IMPLICIT_CAST
	typeMatrix[common.LTID_BIGINT][common.LTID_FLOAT] = IMPLICIT_CAST
	typeMatrix[common.LTID_BIGINT][common.LTID_HUGEINT] = IMPLICIT_CAST
	typeMatrix[common.LTID_BIGINT][common.LTID_DECIMAL] = IMPLICIT_CAST
	typeMatrix[common.LTID_BIGINT][common.LTID_VARCHAR] = IMPLICIT_CAST
	//integer
	typeMatrix[common.LTID_INTEGER][common.LTID_BOOLEAN] = EXPLICIT_CAST
	typeMatrix[common.LTID_INTEGER][common.LTID_BIT] = EXPLICIT_CAST
	typeMatrix[common.LTID_INTEGER][common.LTID_DOUBLE] = IMPLICIT_CAST
	typeMatrix[common.LTID_INTEGER][common.LTID_FLOAT] = IMPLICIT_CAST
	typeMatrix[common.LTID_INTEGER][common.LTID_HUGEINT] = IMPLICIT_CAST
	typeMatrix[common.LTID_INTEGER][common.LTID_BIGINT] = IMPLICIT_CAST
	typeMatrix[common.LTID_INTEGER][common.LTID_DECIMAL] = IMPLICIT_CAST
	typeMatrix[common.LTID_INTEGER][common.LTID_VARCHAR] = IMPLICIT_CAST
	//smallint
	typeMatrix[common.LTID_SMALLINT][common.LTID_BOOLEAN] = EXPLICIT_CAST
	typeMatrix[common.LTID_SMALLINT][common.LTID_BIT] = EXPLICIT_CAST
	typeMatrix[common.LTID_SMALLINT][common.LTID_DOUBLE] = IMPLICIT_CAST
	typeMatrix[common.LTID_SMALLINT][common.LTID_FLOAT] = IMPLICIT_CAST
	typeMatrix[common.LTID_SMALLINT][common.LTID_HUGEINT] = IMPLICIT_CAST
	typeMatrix[common.LTID_SMALLINT][common.LTID_BIGINT] = IMPLICIT_CAST
	typeMatrix[common.LTID_SMALLINT][common.LTID_INTEGER] = IMPLICIT_CAST
	typeMatrix[common.LTID_SMALLINT][common.LTID_DECIMAL] = IMPLICIT_CAST
	typeMatrix[common.LTID_SMALLINT][common.LTID_VARCHAR] = IMPLICIT_CAST
	//tinyint
	typeMatrix[common.LTID_TINYINT][common.LTID_BOOLEAN] = EXPLICIT_CAST
	typeMatrix[common.LTID_TINYINT][common.LTID_BIT] = EXPLICIT_CAST
	typeMatrix[common.LTID_TINYINT][common.LTID_DOUBLE] = IMPLICIT_CAST
	typeMatrix[common.LTID_TINYINT][common.LTID_FLOAT] = IMPLICIT_CAST
	typeMatrix[common.LTID_TINYINT][common.LTID_HUGEINT] = IMPLICIT_CAST
	typeMatrix[common.LTID_TINYINT][common.LTID_BIGINT] = IMPLICIT_CAST
	typeMatrix[common.LTID_TINYINT][common.LTID_INTEGER] = IMPLICIT_CAST
	typeMatrix[common.LTID_TINYINT][common.LTID_SMALLINT] = IMPLICIT_CAST
	typeMatrix[common.LTID_TINYINT][common.LTID_DECIMAL] = IMPLICIT_CAST
	typeMatrix[common.LTID_TINYINT][common.LTID_VARCHAR] = IMPLICIT_CAST
	//ubigint
	typeMatrix[common.LTID_UBIGINT][common.LTID_BOOLEAN] = EXPLICIT_CAST
	typeMatrix[common.LTID_UBIGINT][common.LTID_BIT] = EXPLICIT_CAST
	typeMatrix[common.LTID_UBIGINT][common.LTID_DOUBLE] = IMPLICIT_CAST
	typeMatrix[common.LTID_UBIGINT][common.LTID_FLOAT] = IMPLICIT_CAST
	typeMatrix[common.LTID_UBIGINT][common.LTID_HUGEINT] = IMPLICIT_CAST
	typeMatrix[common.LTID_UBIGINT][common.LTID_BIGINT] = EXPLICIT_CAST
	typeMatrix[common.LTID_UBIGINT][common.LTID_INTEGER] = EXPLICIT_CAST
	typeMatrix[common.LTID_UBIGINT][common.LTID_SMALLINT] = EXPLICIT_CAST
	typeMatrix[common.LTID_UBIGINT][common.LTID_TINYINT] = EXPLICIT_CAST
	typeMatrix[common.LTID_UBIGINT][common.LTID_UINTEGER] = EXPLICIT_CAST
	typeMatrix[common.LTID_UBIGINT][common.LTID_USMALLINT] = EXPLICIT_CAST
	typeMatrix[common.LTID_UBIGINT][common.LTID_UTINYINT] = EXPLICIT_CAST
	typeMatrix[common.LTID_UBIGINT][common.LTID_DECIMAL] = IMPLICIT_CAST
	typeMatrix[common.LTID_UBIGINT][common.LTID_VARCHAR] = IMPLICIT_CAST
	//uinteger
	typeMatrix[common.LTID_UINTEGER][common.LTID_BOOLEAN] = EXPLICIT_CAST
	typeMatrix[common.LTID_UINTEGER][common.LTID_BIT] = EXPLICIT_CAST
	typeMatrix[common.LTID_UINTEGER][common.LTID_DOUBLE] = IMPLICIT_CAST
	typeMatrix[common.LTID_UINTEGER][common.LTID_FLOAT] = IMPLICIT_CAST
	typeMatrix[common.LTID_UINTEGER][common.LTID_HUGEINT] = IMPLICIT_CAST
	typeMatrix[common.LTID_UINTEGER][common.LTID_BIGINT] = IMPLICIT_CAST
	typeMatrix[common.LTID_UINTEGER][common.LTID_INTEGER] = EXPLICIT_CAST
	typeMatrix[common.LTID_UINTEGER][common.LTID_SMALLINT] = EXPLICIT_CAST
	typeMatrix[common.LTID_UINTEGER][common.LTID_TINYINT] = EXPLICIT_CAST
	typeMatrix[common.LTID_UINTEGER][common.LTID_UBIGINT] = IMPLICIT_CAST
	typeMatrix[common.LTID_UINTEGER][common.LTID_USMALLINT] = EXPLICIT_CAST
	typeMatrix[common.LTID_UINTEGER][common.LTID_UTINYINT] = EXPLICIT_CAST
	typeMatrix[common.LTID_UINTEGER][common.LTID_DECIMAL] = IMPLICIT_CAST
	typeMatrix[common.LTID_UINTEGER][common.LTID_VARCHAR] = IMPLICIT_CAST
	//usmallint
	typeMatrix[common.LTID_USMALLINT][common.LTID_BOOLEAN] = EXPLICIT_CAST
	typeMatrix[common.LTID_USMALLINT][common.LTID_BIT] = EXPLICIT_CAST
	typeMatrix[common.LTID_USMALLINT][common.LTID_DOUBLE] = IMPLICIT_CAST
	typeMatrix[common.LTID_USMALLINT][common.LTID_FLOAT] = IMPLICIT_CAST
	typeMatrix[common.LTID_USMALLINT][common.LTID_HUGEINT] = IMPLICIT_CAST
	typeMatrix[common.LTID_USMALLINT][common.LTID_BIGINT] = IMPLICIT_CAST
	typeMatrix[common.LTID_USMALLINT][common.LTID_INTEGER] = IMPLICIT_CAST
	typeMatrix[common.LTID_USMALLINT][common.LTID_SMALLINT] = EXPLICIT_CAST
	typeMatrix[common.LTID_USMALLINT][common.LTID_TINYINT] = EXPLICIT_CAST
	typeMatrix[common.LTID_USMALLINT][common.LTID_UBIGINT] = IMPLICIT_CAST
	typeMatrix[common.LTID_USMALLINT][common.LTID_UINTEGER] = IMPLICIT_CAST
	typeMatrix[common.LTID_USMALLINT][common.LTID_UTINYINT] = EXPLICIT_CAST
	typeMatrix[common.LTID_USMALLINT][common.LTID_DECIMAL] = IMPLICIT_CAST
	typeMatrix[common.LTID_USMALLINT][common.LTID_VARCHAR] = IMPLICIT_CAST
	//utinyint
	typeMatrix[common.LTID_UTINYINT][common.LTID_BOOLEAN] = EXPLICIT_CAST
	typeMatrix[common.LTID_UTINYINT][common.LTID_BIT] = EXPLICIT_CAST
	typeMatrix[common.LTID_UTINYINT][common.LTID_DOUBLE] = IMPLICIT_CAST
	typeMatrix[common.LTID_UTINYINT][common.LTID_FLOAT] = IMPLICIT_CAST
	typeMatrix[common.LTID_UTINYINT][common.LTID_HUGEINT] = IMPLICIT_CAST
	typeMatrix[common.LTID_UTINYINT][common.LTID_BIGINT] = IMPLICIT_CAST
	typeMatrix[common.LTID_UTINYINT][common.LTID_INTEGER] = IMPLICIT_CAST
	typeMatrix[common.LTID_UTINYINT][common.LTID_SMALLINT] = IMPLICIT_CAST
	typeMatrix[common.LTID_UTINYINT][common.LTID_TINYINT] = EXPLICIT_CAST
	typeMatrix[common.LTID_UTINYINT][common.LTID_UBIGINT] = IMPLICIT_CAST
	typeMatrix[common.LTID_UTINYINT][common.LTID_UINTEGER] = IMPLICIT_CAST
	typeMatrix[common.LTID_UTINYINT][common.LTID_USMALLINT] = IMPLICIT_CAST
	typeMatrix[common.LTID_UTINYINT][common.LTID_DECIMAL] = IMPLICIT_CAST
	typeMatrix[common.LTID_UTINYINT][common.LTID_VARCHAR] = IMPLICIT_CAST
	//decimal
	typeMatrix[common.LTID_DECIMAL][common.LTID_BOOLEAN] = EXPLICIT_CAST
	typeMatrix[common.LTID_DECIMAL][common.LTID_DOUBLE] = IMPLICIT_CAST
	typeMatrix[common.LTID_DECIMAL][common.LTID_FLOAT] = IMPLICIT_CAST
	typeMatrix[common.LTID_DECIMAL][common.LTID_HUGEINT] = EXPLICIT_CAST
	typeMatrix[common.LTID_DECIMAL][common.LTID_BIGINT] = EXPLICIT_CAST
	typeMatrix[common.LTID_DECIMAL][common.LTID_INTEGER] = EXPLICIT_CAST
	typeMatrix[common.LTID_DECIMAL][common.LTID_SMALLINT] = EXPLICIT_CAST
	typeMatrix[common.LTID_DECIMAL][common.LTID_VARCHAR] = IMPLICIT_CAST
	//uuid
	typeMatrix[common.LTID_UUID][common.LTID_VARCHAR] = IMPLICIT_CAST
}

func init() {
	fillTypeMatrix()
}

func Test_type(t *testing.T) {

}
