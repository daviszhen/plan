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

package plan

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParser(t *testing.T) {
	stmts, err := Parse("SELECT 42")
	assert.NoError(t, err)
	assert.Equal(t, 1, len(stmts))
	assert.Equal(t, int32(42), stmts[0].Stmt.GetSelectStmt().GetTargetList()[0].GetResTarget().GetVal().GetAConst().GetIval().Ival)
}

func TestTpchSqls(t *testing.T) {
	path := "../tpch/"
	for i := 0; i < 1; i++ {
		name := fmt.Sprintf("q%d.sql", i+1)
		sqlPath := path + name
		sql, err := os.ReadFile(sqlPath)
		assert.NoError(t, err)
		stmts, err := Parse(string(sql))
		assert.NoError(t, err)
		assert.Equal(t, len(stmts), 1)
		selectStmt := stmts[0].GetStmt().GetSelectStmt()
		assert.NotNil(t, selectStmt)
		from := selectStmt.GetFromClause()
		assert.NotNil(t, from)
	}
}
