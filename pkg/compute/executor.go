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
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	wire "github.com/jeroenrinzema/psql-wire"
	"github.com/lib/pq/oid"
	pg_query "github.com/pganalyze/pg_query_go/v5"
	"go.uber.org/zap"

	"github.com/daviszhen/plan/pkg/chunk"
	"github.com/daviszhen/plan/pkg/common"
	"github.com/daviszhen/plan/pkg/parser"
	"github.com/daviszhen/plan/pkg/storage"
	"github.com/daviszhen/plan/pkg/util"
)

const (
	tpch1g22 = 22
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
		for r := 0; r < repeat; r++ {
			res := make([]runResult, 0)
			for i := 0; i < tpch1g22; i++ {
				id := i + 1
				stmts, err := genStmts(cfg, id)
				if err != nil {
					return err
				}

				if len(stmts) != 1 || stmts[0] == nil {
					return fmt.Errorf("invalid statements")
				}

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
		if id <= 0 || id > tpch1g22 {
			return fmt.Errorf("invalid query Id:%d", id)
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

func InitRunner(cfg *util.Config, txn *storage.Txn, query string) (*Runner, error) {
	if cfg == nil {
		return nil, fmt.Errorf("config is nil")
	}

	//parse
	stmts, err := parser.Parse(query)
	if err != nil {
		return nil, err
	}

	if len(stmts) != 1 {
		return nil, fmt.Errorf("multiple statements in one request")
	}

	//gen plan
	var root *PhysicalOperator
	root, err = genDDLPhyPlan(txn, stmts[0])
	if err != nil {
		return nil, err
	}
	if root == nil {
		return nil, fmt.Errorf("nil plan")
	}

	//gen runner
	run := &Runner{
		op:    root,
		state: &OperatorState{},
		cfg:   cfg,
		Txn:   txn,
	}
	err = run.Init()
	if err != nil {
		return nil, err
	}

	return run, nil
}

func genStmts(cfg *util.Config, id int) ([]*pg_query.RawStmt, error) {
	sqlPath := path.Join(cfg.Tpch1g.Query.Path, fmt.Sprintf("q%d.sql", id))
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

func execQuery(cfg *util.Config, id int, ast *pg_query.SelectStmt) (err error) {
	defer func() {
		if rErr := recover(); rErr != nil {
			err = errors.Join(err, util.ConvertPanicError(rErr))
		}
	}()
	txn, err := storage.GTxnMgr.NewTxn("runDDL")
	if err != nil {
		return err
	}
	storage.BeginQuery(txn)
	defer func() {
		if err != nil {
			storage.GTxnMgr.Rollback(txn)
		} else {
			err = storage.GTxnMgr.Commit(txn)
		}
	}()

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

func genPhyPlan(txn *storage.Txn, ast *pg_query.SelectStmt) (*PhysicalOperator, error) {
	builder := NewBuilder(txn)
	err := builder.buildSelect(ast, builder.rootCtx, 0)
	if err != nil {
		return nil, err
	}

	lp, err := builder.CreatePlan(builder.rootCtx, nil)
	if err != nil {
		return nil, err
	}
	if lp == nil {
		return nil, errors.New("nil plan")
	}
	checkExprIsValid(lp)
	lp, err = builder.Optimize(builder.rootCtx, lp)
	if err != nil {
		return nil, err
	}
	if lp == nil {
		return nil, errors.New("nil plan")
	}
	checkExprIsValid(lp)
	pp, err := builder.CreatePhyPlan(lp)
	if err != nil {
		return nil, err
	}
	if pp == nil {
		return nil, errors.New("nil physical plan")
	}
	return pp, nil
}

func execOps(
	conf *util.Config,
	txn *storage.Txn,
	serial util.Serialize,
	resFile *os.File,
	ops []*PhysicalOperator) error {
	var err error

	for _, op := range ops {
		if conf.Debug.PrintPlan {
			fmt.Println(op.String())
		}

		run := &Runner{
			op:    op,
			Txn:   txn,
			state: &OperatorState{},
			cfg:   conf,
		}
		err = run.Init()
		if err != nil {
			return err
		}

		rowCnt := 0
		for {
			if rowCnt >= conf.Debug.MaxOutputRowCount && conf.Debug.MaxOutputRowCount != -1 {
				break
			}
			output := &chunk.Chunk{}
			output.SetCap(util.DefaultVectorSize)
			result, err := run.Execute(nil, output, run.state)
			if err != nil {
				return err
			}
			if result == Done {
				break
			}
			if output.Card() > 0 {
				util.AssertFunc(output.Card() != 0)

				if serial != nil {
					err = output.Serialize(serial)
					if err != nil {
						return err
					}
				}

				if resFile != nil {
					err = output.SaveToFile(resFile)
					if err != nil {
						return err
					}
				}

				rowCnt += output.Card()
				if conf.Debug.PrintResult {
					output.Print()
				}
			}
		}
		if conf.Debug.PrintPlan {
			fmt.Println(op.String())
		}
		run.Close()
	}
	return nil
}

func wantOp(root *PhysicalOperator, pt POT) bool {
	if root == nil {
		return false
	}
	if root.Typ == pt {
		return true
	}
	return false
}

//func wantJoin(root *PhysicalOperator, jTyp LOT_JoinType) bool {
//	if root == nil {
//		return false
//	}
//	if root.Typ == POT_Join && root.JoinTyp == jTyp {
//		return true
//	}
//	return false
//}

func wantId(root *PhysicalOperator, id int) bool {
	if root == nil {
		return false
	}
	return root.Id == id
}

type Runner struct {
	cfg   *util.Config
	Txn   *storage.Txn
	op    *PhysicalOperator
	state *OperatorState

	//common
	children []*Runner
}

func (run *Runner) Columns() wire.Columns {
	cols := make(wire.Columns, 0)
	for _, output := range run.op.Outputs {
		col := wire.Column{
			//Name:  output.Name,
			Oid:   oid.T_varchar, //FIXME:
			Width: int16(output.DataTyp.Width),
		}
		cols = append(cols, col)
	}
	return cols
}

func (run *Runner) Run(
	ctx context.Context,
	writer wire.DataWriter) error {
	if run.cfg.Debug.PrintPlan {
		fmt.Println(run.op.String())
	}

	for {
		output := &chunk.Chunk{}
		output.SetCap(util.DefaultVectorSize)
		result, err := run.Execute(nil, output, run.state)
		if err != nil {
			return err
		}
		if result == Done {
			break
		}
		if output.Card() > 0 {
			util.AssertFunc(output.Card() != 0)
			err = output.SaveToWriter(writer)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (run *Runner) initChildren() error {
	run.children = []*Runner{}
	for _, child := range run.op.Children {
		childRun := &Runner{
			op:    child,
			Txn:   run.Txn,
			state: &OperatorState{},
			cfg:   run.cfg,
		}
		err := childRun.Init()
		if err != nil {
			return err
		}
		run.children = append(run.children, childRun)
	}
	return nil
}

func (run *Runner) initOutput() {
	for _, output := range run.op.Outputs {
		run.state.outputTypes = append(run.state.outputTypes, output.DataTyp)
		run.state.outputIndice = append(run.state.outputIndice, int(output.ColRef.column()))
	}
}

func (run *Runner) Init() error {
	run.initOutput()
	err := run.initChildren()
	if err != nil {
		return err
	}
	switch run.op.Typ {
	case POT_Scan:
		return run.scanInit()
	case POT_Project:
		return run.projInit()
	case POT_Join:
		return run.joinInit()
	case POT_Agg:
		return run.aggrInit()
	case POT_Filter:
		return run.filterInit()
	case POT_Order:
		return run.orderInit()
	case POT_Limit:
		return run.limitInit()
	case POT_Stub:
		return run.stubInit()
	case POT_CreateSchema:
		return run.createSchemaInit()
	case POT_CreateTable:
		return run.createTableInit()
	case POT_Insert:
		return run.insertInit()
	default:
		panic("usp")
	}
}

func (run *Runner) Execute(input, output *chunk.Chunk, state *OperatorState) (OperatorResult, error) {
	output.Init(run.state.outputTypes, util.DefaultVectorSize)
	defer func(start time.Time) {
		run.op.ExecStats._totalTime += time.Since(start)
	}(time.Now())
	switch run.op.Typ {
	case POT_Scan:
		return run.scanExec(output, state)
	case POT_Project:
		return run.projExec(output, state)
	case POT_Join:
		return run.joinExec(output, state)
	case POT_Agg:
		return run.aggrExec(output, state)
	case POT_Filter:
		return run.filterExec(output, state)
	case POT_Order:
		return run.orderExec(output, state)
	case POT_Limit:
		return run.limitExec(output, state)
	case POT_Stub:
		return run.stubExec(output, state)
	case POT_CreateSchema:
		return run.createSchemaExec(output, state)
	case POT_CreateTable:
		return run.createTableExec(output, state)
	case POT_Insert:
		return run.insertExec(output, state)
	default:
		panic("usp")
	}
}

func (run *Runner) execChild(child *Runner, output *chunk.Chunk, state *OperatorState) (OperatorResult, error) {
	defer func(start time.Time) {
		run.op.ExecStats._totalChildTime += time.Since(start)
	}(time.Now())
	for output.Card() == 0 {
		res, err := child.Execute(nil, output, child.state)
		if err != nil {
			return InvalidOpResult, err
		}
		switch res {
		case Done:
			return Done, nil
		case InvalidOpResult:
			return InvalidOpResult, nil
		default:
			return haveMoreOutput, nil
		}
	}
	return Done, nil
}

func (run *Runner) Close() error {
	for _, child := range run.children {
		err := child.Close()
		if err != nil {
			return err
		}
	}
	switch run.op.Typ {
	case POT_Scan:
		return run.scanClose()
	case POT_Project:
		return run.projClose()
	case POT_Join:
		return run.joinClose()
	case POT_Agg:
		return run.aggrClose()
	case POT_Filter:
		return run.filterClose()
	case POT_Order:
		return run.orderClose()
	case POT_Limit:
		return run.limitClose()
	case POT_Stub:
		return run.stubClose()
	case POT_CreateSchema:
		return run.createSchemaClose()
	case POT_CreateTable:
		return run.createTableClose()
	case POT_Insert:
		return run.insertClose()
	default:
		panic("usp")
	}
}

func (run *Runner) stubInit() error {
	deserial, err := util.NewFileDeserialize(run.op.Table)
	if err != nil {
		return err
	}
	run.state.deserial = deserial
	run.state.maxRowCnt = run.op.ChunkCount
	return nil
}

func (run *Runner) stubExec(output *chunk.Chunk, state *OperatorState) (OperatorResult, error) {
	if run.state.maxRowCnt != 0 && run.state.rowReadCnt >= run.state.maxRowCnt {
		return Done, nil
	}
	err := output.Deserialize(run.state.deserial)
	if err != nil {
		return InvalidOpResult, err
	}
	if output.Card() == 0 {
		return Done, nil
	}
	run.state.rowReadCnt += output.Card()
	return haveMoreOutput, nil
}

func (run *Runner) stubClose() error {
	return run.state.deserial.Close()
}

func (run *Runner) projInit() error {
	projTypes := make([]common.LType, 0)
	for _, proj := range run.op.Projects {
		projTypes = append(projTypes, proj.DataTyp)
	}

	run.state.projTypes = projTypes
	run.state.projExec = NewExprExec(run.op.Projects...)
	run.state.outputExec = NewExprExec(run.op.Outputs...)
	return nil
}

func (run *Runner) projExec(output *chunk.Chunk, state *OperatorState) (OperatorResult, error) {
	childChunk := &chunk.Chunk{}
	var res OperatorResult
	var err error
	if len(run.children) != 0 {
		res, err = run.execChild(run.children[0], childChunk, state)
		if err != nil {
			return 0, err
		}
		if res == InvalidOpResult {
			return InvalidOpResult, nil
		}
	}

	//project list
	projChunk := &chunk.Chunk{}
	projChunk.Init(run.state.projTypes, util.DefaultVectorSize)
	err = run.state.projExec.executeExprs([]*chunk.Chunk{childChunk, nil, nil}, projChunk)
	if err != nil {
		return 0, err
	}

	err = run.state.outputExec.executeExprs([]*chunk.Chunk{childChunk, nil, projChunk}, output)
	if err != nil {
		return 0, err
	}

	return res, nil
}
func (run *Runner) projClose() error {

	return nil
}
