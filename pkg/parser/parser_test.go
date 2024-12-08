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

package parser

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParser(t *testing.T) {
	stmts, err := Parse("SELECT 42")
	assert.NoError(t, err)
	assert.Equal(t, 1, len(stmts))
	assert.Equal(t, int32(42), stmts[0].Stmt.GetSelectStmt().GetTargetList()[0].GetResTarget().GetVal().GetAConst().GetIval().Ival)
}

func TestTpchSqls(t *testing.T) {
	path := "../../cases/tpch/query/"
	ids := []int{20}
	for _, id := range ids {
		name := fmt.Sprintf("q%d.sql", id)
		sqlPath := path + name
		sql, err := os.ReadFile(sqlPath)
		require.NoError(t, err)
		stmts, err := Parse(string(sql))
		require.NoError(t, err)
		require.Equal(t, 1, len(stmts))
		selectStmt := stmts[0].GetStmt().GetSelectStmt()
		require.NotNil(t, selectStmt)
		from := selectStmt.GetFromClause()
		require.NotNil(t, from)
	}

	//ddl
	name := "ddl.sql"
	sqlPath := path + name
	sql, err := os.ReadFile(sqlPath)
	require.NoError(t, err)
	stmts, err := Parse(string(sql))
	require.NoError(t, err)
	require.Equal(t, 8, len(stmts))
}

func TestSchema(t *testing.T) {
	schs := []string{
		"create schema s1",
		"create schema if not exists s1",
	}

	for _, sch := range schs {
		stmts, err := Parse(sch)
		require.NoError(t, err)
		require.Equal(t, 1, len(stmts))
	}
}
