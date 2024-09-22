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
