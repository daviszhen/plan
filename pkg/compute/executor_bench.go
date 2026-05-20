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
	"fmt"
	"os"
	"path"
	"path/filepath"
	"runtime/debug"
	"strings"
	"time"

	pg_query "github.com/pganalyze/pg_query_go/v5"
	"go.uber.org/zap"

	"github.com/daviszhen/plan/pkg/parser"
	"github.com/daviszhen/plan/pkg/storage"
	"github.com/daviszhen/plan/pkg/util"
)

type runResult struct {
	id   int
	dur  time.Duration
	succ bool
}

func (res *runResult) String() string {
	succ := "failed"
	if res.succ {
		succ = "success"
	}
	return fmt.Sprint("Query ", res.id, " took ", res.dur, " ", succ)
}

func Run(cfg *util.Config) error {
	if cfg == nil {
		return fmt.Errorf("config is nil")
	}

	start := time.Now()
	defer func() {
		fmt.Printf("Run took %s\n", time.Since(start))
	}()
	repeat := 1
	if cfg.Debug.Count > 0 {
		repeat = cfg.Debug.Count
	}
	if cfg.Tpch1g.Query.QueryId == 0 {
		maxQueryId := cfg.Tpch1g.Query.MaxQueryId
		if maxQueryId <= 0 {
			maxQueryId = 22 // default: all TPC-H queries
		}
		for r := 0; r < repeat; r++ {
			res := make([]runResult, 0)
			for i := 0; i < maxQueryId; i++ {
				id := i + 1
				fmt.Printf("[run] preparing query %d: parsing SQL\n", id)
				stmts, err := genStmts(cfg, id)
				if err != nil {
					return err
				}

				if len(stmts) != 1 || stmts[0] == nil {
					return fmt.Errorf("invalid statements")
				}

				fmt.Printf("[run] executing query %d\n", id)
				st := time.Now()
				err = execQuery(cfg, id, stmts[0].GetStmt().GetSelectStmt())
				if err != nil {
					util.Error("execQuery fail", zap.Int("queryId", id), zap.Error(err))
					res = append(res, runResult{id: id, dur: time.Since(st)})
				} else {
					res = append(res, runResult{id: id, dur: time.Since(st), succ: true})
				}
			}
			failed := make([]int, 0)
			for _, re := range res {
				fmt.Println(re.String())
				if !re.succ {
					failed = append(failed, re.id)
				}
			}
			if len(failed) > 0 {
				fmt.Printf("Failed query count: %d\n", len(failed))
				for _, i := range failed {
					fmt.Println("Query", i, "failed")
				}
			}
		}
	} else {
		id := cfg.Tpch1g.Query.QueryId
		maxQueryId := cfg.Tpch1g.Query.MaxQueryId
		if maxQueryId <= 0 {
			maxQueryId = 22
		}
		if id <= 0 || id > uint(maxQueryId) {
			return fmt.Errorf("invalid query Id:%d (max=%d)", id, maxQueryId)
		}
		re := runResult{
			id: int(id),
		}

		stmts, err := genStmts(cfg, int(id))
		if err != nil {
			return err
		}

		if len(stmts) != 1 || stmts[0] == nil || stmts[0].GetStmt().GetSelectStmt() == nil {
			return fmt.Errorf("invalid statements")
		}

		for i := 0; i < repeat; i++ {
			st := time.Now()
			err = execQuery(cfg, int(id), stmts[0].GetStmt().GetSelectStmt())
			if err != nil {
				util.Error("execQuery fail", zap.Uint("queryId", id), zap.Error(err))
				re.succ = false
			} else {
				re.succ = true
			}
			re.dur = time.Since(st)
			fmt.Println(re.String())
		}
	}
	return nil
}

func RunDDL(cfg *util.Config) error {
	var err error
	if cfg == nil {
		return fmt.Errorf("config is nil")
	}

	start := time.Now()
	defer func() {
		fmt.Printf("Run took %s\n", time.Since(start))
	}()

	var ddlStmts []*pg_query.RawStmt

	pathLen := len(cfg.Tpch1g.DDL.Path)
	ddlLen := len(cfg.Tpch1g.DDL.DDL)
	if pathLen != 0 && ddlLen != 0 ||
		pathLen == 0 && ddlLen == 0 {
		return fmt.Errorf("both ddl path and ddl or neither of them")
	} else if pathLen != 0 {
		ddlStmts, err = genDDLStmts(cfg, true)
		if err != nil {
			return err
		}
	} else {
		// ddlLen != 0
		ddlStmts, err = genDDLStmts(cfg, false)
		if err != nil {
			return err
		}
	}

	for _, ddl := range ddlStmts {
		err = runDDl(cfg, ddl)
		if err != nil {
			return err
		}
	}

	return nil
}

func execQuery(cfg *util.Config, id int, ast *pg_query.SelectStmt) (err error) {
	defer func() {
		if rErr := recover(); rErr != nil {
			err = util.ConvertPanicError(rErr)
			debug.PrintStack()
		}
	}()
	fmt.Printf("[execQuery] q%d: begin txn\n", id)
	txn, err := storage.GTxnMgr.NewTxn("runDDL")
	if err != nil {
		return err
	}
	storage.BeginQuery(txn)
	defer func() {
		if err != nil {
			fmt.Printf("[execQuery] q%d: rollback\n", id)
			storage.GTxnMgr.Rollback(txn)
		} else {
			fmt.Printf("[execQuery] q%d: commit\n", id)
			err = storage.GTxnMgr.Commit(txn)
		}
	}()

	fmt.Printf("[execQuery] q%d: generating physical plan\n", id)
	var root *PhysicalOperator
	root, err = genPhyPlan(txn, ast)
	if err != nil {
		return err
	}
	if root == nil {
		return fmt.Errorf("nil plan")
	}
	fname := fmt.Sprintf("q%d.txt", id)
	path := filepath.Join(cfg.Tpch1g.Result.Path, fname)
	fmt.Println("Execute query", path)
	var resFile *os.File
	if len(path) != 0 {
		resFile, err = os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
		if err != nil {
			return err
		}
		defer func() {
			resFile.Sync()
			resFile.Close()
		}()

		if cfg.Tpch1g.Result.NeedHeadLine {
			outputStrs := make([]string, 0)
			for _, outputExpr := range root.Outputs {
				outputStrs = append(outputStrs, outputExpr.Alias)
			}
			_, err = resFile.WriteString(fmt.Sprintf("#%s\n", strings.Join(outputStrs, "\t")))
			if err != nil {
				return err
			}
		}
	}

	return execOps(cfg, txn, nil, resFile, []*PhysicalOperator{root})
}

func genStmts(cfg *util.Config, id int) ([]*pg_query.RawStmt, error) {
	pattern := cfg.Tpch1g.Query.FilePattern
	if pattern == "" {
		pattern = "q%d.sql"
	}
	sqlPath := path.Join(cfg.Tpch1g.Query.Path, fmt.Sprintf(pattern, id))
	sqlBytes, err := os.ReadFile(sqlPath)
	if err != nil {
		return nil, err
	}
	stmts, err := parser.Parse(string(sqlBytes))
	if err != nil {
		return nil, err
	}
	return stmts, nil
}

func genDDLStmts(cfg *util.Config, usePath bool) ([]*pg_query.RawStmt, error) {
	var sql string
	if usePath {
		sqlPath := cfg.Tpch1g.DDL.Path
		sqlBytes, err := os.ReadFile(sqlPath)
		if err != nil {
			return nil, err
		}
		sql = string(sqlBytes)
	} else {
		sql = cfg.Tpch1g.DDL.DDL
	}
	stmts, err := parser.Parse(sql)
	if err != nil {
		return nil, err
	}
	return stmts, nil
}
