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
	pg_query "github.com/pganalyze/pg_query_go/v5"
)

func Parse(s string) ([]*pg_query.RawStmt, error) {
	result, err := pg_query.Parse(s)
	if err != nil {
		return nil, err
	}
	return result.Stmts, nil
}
