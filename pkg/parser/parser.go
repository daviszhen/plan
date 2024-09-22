package plan

import (
	pg_query "github.com/pganalyze/pg_query_go/v5"
)

func Parse(s string) ([]*pg_query.RawStmt, error) {
	result, err := pg_query.Parse(s)
	if err != nil {
		return nil, err
	}
	return result.Stmts, nil
}
