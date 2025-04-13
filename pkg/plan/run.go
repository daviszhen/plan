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
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	wire "github.com/jeroenrinzema/psql-wire"
	"github.com/lib/pq/oid"
	pg_query "github.com/pganalyze/pg_query_go/v5"
	pqLocal "github.com/xitongsys/parquet-go-source/local"
	pqReader "github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/source"
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

func runDDl(cfg *util.Config, ddl *pg_query.RawStmt) error {
	var root *PhysicalOperator
	var err error
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

	root, err = genDDLPhyPlan(txn, ddl)
	if err != nil {
		return err
	}
	if root == nil {
		return fmt.Errorf("nil plan")
	}
	return execOps(cfg, txn, nil, nil, []*PhysicalOperator{root})
}

func genDDLPhyPlan(txn *storage.Txn, ddl *pg_query.RawStmt) (*PhysicalOperator, error) {
	builder := NewBuilder(txn)
	lp, err := builder.buildDDL(txn, ddl, builder.rootCtx, 0)
	if err != nil {
		return nil, err
	}
	if lp == nil {
		return nil, errors.New("nil plan")
	}
	pp, err := builder.CreatePhyPlan(lp)
	if err != nil {
		return nil, err
	}
	return pp, nil
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

type OperatorState struct {
	//order
	orderKeyExec *ExprExec
	keyTypes     []common.LType
	payloadTypes []common.LType

	projTypes  []common.LType
	projExec   *ExprExec
	outputExec *ExprExec

	//filter projExec used in aggr, filter, scan
	filterExec *ExprExec
	filterSel  *chunk.SelectVector

	//for aggregate
	referChildren         bool
	constGroupby          bool
	ungroupAggr           bool
	ungroupAggrDone       bool
	haScanState           *HashAggrScanState
	groupbyWithParamsExec *ExprExec
	groupbyExec           *ExprExec

	//for scan values list
	colScanState *ColumnDataScanState

	//for table scan
	tableScanState *storage.TableScanState

	showRaw bool
}

type OperatorResult int

const (
	InvalidOpResult OperatorResult = 0
	NeedMoreInput   OperatorResult = 1
	haveMoreOutput  OperatorResult = 2
	Done            OperatorResult = 3
)

type SourceResult int

const (
	SrcResHaveMoreOutput SourceResult = iota
	SrcResDone
)

type SinkResult int

const (
	SinkResNeedMoreInput SinkResult = iota
	SinkResDone
)

type ExecStats struct {
	_totalTime      time.Duration
	_totalChildTime time.Duration
}

func (stats ExecStats) String() string {
	if stats._totalTime == 0 {
		return fmt.Sprintf("total time is 0")
	}
	return fmt.Sprintf("time : total %v, this %v (%.2f) , child %v",
		stats._totalTime,
		stats._totalTime-stats._totalChildTime,
		float64(stats._totalTime-stats._totalChildTime)/float64(stats._totalTime),
		stats._totalChildTime,
	)
}

var _ OperatorExec = &Runner{}

type OperatorExec interface {
	Init() error
	Execute(input, output *chunk.Chunk, state *OperatorState) (OperatorResult, error)
	Close() error
}

type Runner struct {
	cfg   *util.Config
	Txn   *storage.Txn
	op    *PhysicalOperator
	state *OperatorState
	//for stub
	deserial   util.Deserialize
	maxRowCnt  int
	rowReadCnt int

	//for limit
	limit *Limit

	//for order
	localSort *LocalSort

	//for hash aggr
	hAggr *HashAggr

	//for cross product
	cross *CrossProduct
	//for hash join
	hjoin *HashJoin

	//for scan
	pqFile        source.ParquetFile
	pqReader      *pqReader.ParquetReader
	dataFile      *os.File
	reader        *csv.Reader
	colIndice     []int
	readedColTyps []common.LType
	tablePath     string
	//for test cross product
	maxRows int

	//common
	outputTypes  []common.LType
	outputIndice []int
	children     []*Runner

	//for insert
	insertChunk *chunk.Chunk

	//for table scan
	tabEnt *storage.CatalogEntry
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
		run.outputTypes = append(run.outputTypes, output.DataTyp)
		run.outputIndice = append(run.outputIndice, int(output.ColRef.column()))
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
	output.Init(run.outputTypes, util.DefaultVectorSize)
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

func (run *Runner) insertInit() error {
	run.insertChunk = &chunk.Chunk{}
	run.insertChunk.Init(run.op.InsertTypes, storage.STANDARD_VECTOR_SIZE)
	return nil
}

func (run *Runner) insertResolveDefaults(
	table *storage.CatalogEntry,
	data *chunk.Chunk,
	columnIndexMap []int,
	result *chunk.Chunk,
) {
	data.Flatten()

	result.Reset()
	result.SetCard(data.Card())

	if len(columnIndexMap) != 0 {
		//columns specified
		for colIdx := range table.GetColumns() {
			mappedIdx := columnIndexMap[colIdx]
			if mappedIdx == -1 {
				panic("usp default value")
			} else {
				util.AssertFunc(mappedIdx < data.ColumnCount())
				util.AssertFunc(result.Data[colIdx].Typ().Id ==
					data.Data[mappedIdx].Typ().Id)
				result.Data[colIdx].Reference(data.Data[mappedIdx])
			}
		}
	} else {
		//no columns specified
		for i := 0; i < result.ColumnCount(); i++ {
			util.AssertFunc(result.Data[i].Typ().Id ==
				data.Data[i].Typ().Id)
			result.Data[i].Reference(data.Data[i])
		}
	}
}

func (run *Runner) insertExec(output *chunk.Chunk, state *OperatorState) (OperatorResult, error) {
	var res OperatorResult
	var err error

	lAState := &storage.LocalAppendState{}
	table := run.op.TableEnt.GetStorage()
	table.InitLocalAppend(run.Txn, lAState)

	cnt := 0
	for {
		childChunk := &chunk.Chunk{}
		res, err = run.execChild(run.children[0], childChunk, state)
		if err != nil {
			return 0, err
		}
		if res == InvalidOpResult {
			return InvalidOpResult, nil
		}
		if res == Done {
			break
		}
		if childChunk.Card() == 0 {
			continue
		}

		//fmt.Println("child raw chunk")
		//childChunk.Print()

		cnt += childChunk.Card()

		run.insertResolveDefaults(
			run.op.TableEnt,
			childChunk,
			run.op.ColumnIndexMap,
			run.insertChunk)

		err = table.LocalAppend(
			run.Txn,
			lAState,
			run.insertChunk,
			false)
		if err != nil {
			return InvalidOpResult, err
		}
	}
	table.FinalizeLocalAppend(run.Txn, lAState)
	return Done, nil
}

func (run *Runner) insertClose() error {
	return nil
}

func (run *Runner) createTableInit() error {
	return nil
}

func (run *Runner) createTableExec(output *chunk.Chunk, state *OperatorState) (OperatorResult, error) {
	//step 1 : check schema
	schema := run.op.Database
	if len(schema) == 0 {
		schema = "public"
	}
	table := run.op.Table
	ifNotExists := run.op.IfNotExists
	//////////////////////////////////////
	tabEnt := storage.GCatalog.GetEntry(run.Txn, storage.CatalogTypeTable, schema, table)
	if tabEnt != nil {
		if ifNotExists {
			return Done, nil
		} else {
			return InvalidOpResult, fmt.Errorf("table %s already exits", table)
		}
	}
	info := storage.NewDataTableInfo3(schema, table, run.op.ColDefs, run.op.Constraints)
	_, err := storage.GCatalog.CreateTable(run.Txn, info)
	if err != nil {
		return 0, err
	}
	return Done, nil
}

func (run *Runner) createTableClose() error {
	return nil
}

func (run *Runner) createSchemaInit() error {
	return nil
}

func (run *Runner) createSchemaExec(output *chunk.Chunk, state *OperatorState) (OperatorResult, error) {
	name := run.op.Database
	ifNotExists := run.op.IfNotExists
	schEnt := storage.GCatalog.GetSchema(run.Txn, name)
	if schEnt != nil {
		if ifNotExists {
			return Done, nil
		} else {
			return InvalidOpResult, fmt.Errorf("schema %s already exists", name)
		}
	}
	_, err := storage.GCatalog.CreateSchema(run.Txn, name)
	if err != nil {
		return 0, err
	}
	return Done, nil
}

func (run *Runner) createSchemaClose() error {
	return nil
}

func (run *Runner) stubInit() error {
	deserial, err := util.NewFileDeserialize(run.op.Table)
	if err != nil {
		return err
	}
	run.deserial = deserial
	run.maxRowCnt = run.op.ChunkCount
	return nil
}

func (run *Runner) stubExec(output *chunk.Chunk, state *OperatorState) (OperatorResult, error) {
	if run.maxRowCnt != 0 && run.rowReadCnt >= run.maxRowCnt {
		return Done, nil
	}
	err := output.Deserialize(run.deserial)
	if err != nil {
		return InvalidOpResult, err
	}
	if output.Card() == 0 {
		return Done, nil
	}
	run.rowReadCnt += output.Card()
	return haveMoreOutput, nil
}

func (run *Runner) stubClose() error {
	return run.deserial.Close()
}

func (run *Runner) limitInit() error {
	//collect children output types
	childTypes := make([]common.LType, 0)
	for _, outputExpr := range run.op.Children[0].Outputs {
		childTypes = append(childTypes, outputExpr.DataTyp)
	}

	run.limit = NewLimit(childTypes, run.op.Limit, run.op.Offset)
	run.state = &OperatorState{
		outputExec: NewExprExec(run.op.Outputs...),
	}

	return nil
}

func (run *Runner) limitExec(output *chunk.Chunk, state *OperatorState) (OperatorResult, error) {
	var err error
	var res OperatorResult
	if run.limit._state == LIMIT_INIT {
		cnt := 0
		for {
			childChunk := &chunk.Chunk{}
			res, err = run.execChild(run.children[0], childChunk, state)
			if err != nil {
				return 0, err
			}
			if res == InvalidOpResult {
				return InvalidOpResult, nil
			}
			if res == Done {
				break
			}
			if childChunk.Card() == 0 {
				continue
			}

			//childChunk.print()

			ret := run.limit.Sink(childChunk)
			if ret == SinkResDone {
				break
			}
		}
		fmt.Println("limit total children count", cnt)
		run.limit._state = LIMIT_SCAN
	}

	if run.limit._state == LIMIT_SCAN {
		//get data from collection
		for {
			read := &chunk.Chunk{}
			read.Init(run.limit._childTypes, util.DefaultVectorSize)
			getRet := run.limit.GetData(read)
			if getRet == SrcResDone {
				break
			}

			//evaluate output
			err = run.state.outputExec.executeExprs([]*chunk.Chunk{read, nil, nil}, output)
			if err != nil {
				return InvalidOpResult, err
			}

			if output.Card() > 0 {
				return haveMoreOutput, nil
			}
		}

	}

	if output.Card() == 0 {
		return Done, nil
	}
	return haveMoreOutput, nil
}

func (run *Runner) limitClose() error {
	run.limit = nil
	return nil
}

func (run *Runner) orderInit() error {
	//TODO: asc or desc
	keyTypes := make([]common.LType, 0)
	realOrderByExprs := make([]*Expr, 0)
	for _, by := range run.op.OrderBys {
		child := by.Children[0]
		keyTypes = append(keyTypes, child.DataTyp)
		realOrderByExprs = append(realOrderByExprs, child)
	}

	payLoadTypes := make([]common.LType, 0)
	for _, output := range run.op.Outputs {
		payLoadTypes = append(payLoadTypes,
			output.DataTyp)
	}

	run.localSort = NewLocalSort(
		NewSortLayout(run.op.OrderBys),
		NewRowLayout(payLoadTypes, nil),
	)

	run.state = &OperatorState{
		keyTypes:     keyTypes,
		payloadTypes: payLoadTypes,
		orderKeyExec: NewExprExec(realOrderByExprs...),
		outputExec:   NewExprExec(run.op.Outputs...),
	}

	return nil
}

func (run *Runner) orderExec(output *chunk.Chunk, state *OperatorState) (OperatorResult, error) {
	var err error
	var res OperatorResult
	if run.localSort._sortState == SS_INIT {
		cnt := 0
		for {
			childChunk := &chunk.Chunk{}
			res, err = run.execChild(run.children[0], childChunk, state)
			if err != nil {
				return 0, err
			}
			if res == InvalidOpResult {
				return InvalidOpResult, nil
			}
			if res == Done {
				break
			}
			if childChunk.Card() == 0 {
				continue
			}

			//childChunk.print()

			//evaluate order by expr
			key := &chunk.Chunk{}
			key.Init(run.state.keyTypes, util.DefaultVectorSize)
			err = run.state.orderKeyExec.executeExprs(
				[]*chunk.Chunk{childChunk, nil, nil},
				key,
			)
			if err != nil {
				return 0, err
			}

			//key.print()

			//evaluate payload expr
			payload := &chunk.Chunk{}
			payload.Init(run.state.payloadTypes, util.DefaultVectorSize)

			err = run.state.outputExec.executeExprs(
				[]*chunk.Chunk{childChunk, nil, nil},
				payload,
			)
			if err != nil {
				return 0, err
			}

			util.AssertFunc(key.Card() != 0 && payload.Card() != 0)
			cnt += key.Card()
			util.AssertFunc(key.Card() == payload.Card())

			run.localSort.SinkChunk(key, payload)
		}
		fmt.Println("total count", cnt)
		run.localSort._sortState = SS_SORT
	}

	if run.localSort._sortState == SS_SORT {
		//get all chunks from child
		run.localSort.Sort(true)
		run.localSort._sortState = SS_SCAN
	}

	if run.localSort._sortState == SS_SCAN {
		if run.localSort._scanner != nil &&
			run.localSort._scanner.Remaining() == 0 {
			run.localSort._scanner = nil
		}

		if run.localSort._scanner == nil {
			run.localSort._scanner = NewPayloadScanner(
				run.localSort._sortedBlocks[0]._payloadData,
				run.localSort,
				true,
			)
		}

		run.localSort._scanner.Scan(output)
	}

	if output.Card() == 0 {
		return Done, nil
	}
	return haveMoreOutput, nil
}

func (run *Runner) orderClose() error {
	run.localSort = nil
	return nil
}

func (run *Runner) filterInit() error {
	var err error
	var filterExec *ExprExec
	filterExec, err = initFilterExec(run.op.Filters)
	if err != nil {
		return err
	}
	run.state = &OperatorState{
		filterExec: filterExec,
		filterSel:  chunk.NewSelectVector(util.DefaultVectorSize),
	}
	return nil
}

func initFilterExec(filters []*Expr) (*ExprExec, error) {
	//init filter
	//convert filters into "... AND ..."
	//var err error
	var andFilter *Expr
	if len(filters) > 0 {
		//var impl *Impl
		andFilter = filters[0]
		for i, filter := range filters {
			if i > 0 {
				if andFilter.DataTyp.Id != common.LTID_BOOLEAN ||
					filter.DataTyp.Id != common.LTID_BOOLEAN {
					return nil, fmt.Errorf("need boolean expr")
				}
				binder := FunctionBinder{}
				andFilter = binder.BindScalarFunc(
					ET_And.String(),
					[]*Expr{
						andFilter,
						filter,
					},
					ET_And,
					ET_And.isOperator(),
				)
			}
		}
	}
	return NewExprExec(andFilter), nil
}

func (run *Runner) runFilterExec(input *chunk.Chunk, output *chunk.Chunk, filterOnLocal bool) error {
	//filter
	var err error
	var count int
	//if !filterOnLocal {
	//	//fmt.Println("filter read child 4", input.card())
	//}
	if filterOnLocal {
		count, err = run.state.filterExec.executeSelect([]*chunk.Chunk{nil, nil, input}, run.state.filterSel)
		if err != nil {
			return err
		}
	} else {
		count, err = run.state.filterExec.executeSelect([]*chunk.Chunk{input, nil, nil}, run.state.filterSel)
		if err != nil {
			return err
		}
	}

	if count == input.Card() {
		//reference
		output.ReferenceIndice(input, run.outputIndice)
	} else {
		//slice
		output.SliceIndice(input, run.state.filterSel, count, 0, run.outputIndice)
	}
	//if !filterOnLocal {
	//	//fmt.Println("filter read child 5", output.card())
	//}
	return nil
}

func (run *Runner) filterExec(output *chunk.Chunk, state *OperatorState) (OperatorResult, error) {
	childChunk := &chunk.Chunk{}
	var res OperatorResult
	var err error
	if len(run.children) != 0 {
		for {
			//fmt.Println("filter read child 1")
			res, err = run.execChild(run.children[0], childChunk, state)
			if err != nil {
				return 0, err
			}
			if res == InvalidOpResult {
				return InvalidOpResult, nil
			}
			if res == Done {
				return res, nil
			}
			if childChunk.Card() > 0 {
				//fmt.Println("filter read child 2", childChunk.card())
				break
			}
		}
	}

	err = run.runFilterExec(childChunk, output, false)
	if err != nil {
		return 0, err
	}
	//fmt.Println("filter read child 3", childChunk.card())
	return haveMoreOutput, nil
}

func (run *Runner) filterClose() error {
	return nil
}

func (run *Runner) aggrInit() error {
	run.state = &OperatorState{}
	//if len(run.op.GroupBys) == 0 /*&& groupingSet*/ {
	//	run.hAggr = NewHashAggr(
	//		run.outputTypes,
	//		run.op.Aggs,
	//		nil,
	//		nil,
	//		nil,
	//	)
	//} else
	{

		if len(run.op.GroupBys) == 0 {
			//group by 1
			constExpr := &Expr{
				Typ:     ET_IConst,
				DataTyp: common.IntegerType(),
				Ivalue:  1,
			}
			run.op.GroupBys = append(run.op.GroupBys, constExpr)

			run.state.constGroupby = true
		}

		//children input types
		refChildrenOutput := make([]*Expr, 0)
		for i := 0; i < len(run.op.Children[0].Outputs); i++ {
			ref := run.op.Children[0].Outputs[i]
			refChildrenOutput = append(refChildrenOutput, &Expr{
				Typ:     ET_Column,
				DataTyp: ref.DataTyp,
				ColRef: ColumnBind{
					math.MaxUint64,
					uint64(i),
				},
			})
		}

		run.hAggr = NewHashAggr(
			run.outputTypes,
			run.op.Aggs,
			run.op.GroupBys,
			nil,
			nil,
			refChildrenOutput,
		)
		if run.op.Children[0].Typ == POT_Filter {
			run.hAggr._printHash = true
		}
		//groupby exprs + param exprs of aggr functions + reference to the output exprs of children
		groupExprs := make([]*Expr, 0)
		groupExprs = append(groupExprs, run.hAggr._groupedAggrData._groups...)
		groupExprs = append(groupExprs, run.hAggr._groupedAggrData._paramExprs...)
		groupExprs = append(groupExprs, run.hAggr._groupedAggrData._refChildrenOutput...)
		run.state.groupbyWithParamsExec = NewExprExec(groupExprs...)
		run.state.groupbyExec = NewExprExec(run.hAggr._groupedAggrData._groups...)
		run.state.filterExec = NewExprExec(run.op.Filters...)
		run.state.filterSel = chunk.NewSelectVector(util.DefaultVectorSize)
		run.state.outputExec = NewExprExec(run.op.Outputs...)

		//check output exprs have any colref refers the children node
		bSet := make(ColumnBindSet)
		collectColRefs2(bSet, run.op.Outputs...)

		for bind := range bSet {
			if int64(bind.table()) < 0 {
				run.state.referChildren = true
				break
			}
		}
		run.state.ungroupAggr = !run.state.referChildren && run.state.constGroupby

	}
	return nil
}

func (run *Runner) aggrExec(output *chunk.Chunk, state *OperatorState) (OperatorResult, error) {
	var err error
	var res OperatorResult
	if run.hAggr._has == HAS_INIT {
		cnt := 0
		for {
			childChunk := &chunk.Chunk{}
			res, err = run.execChild(run.children[0], childChunk, state)
			if err != nil {
				return 0, err
			}
			if res == InvalidOpResult {
				return InvalidOpResult, nil
			}
			if res == Done {
				break
			}
			if childChunk.Card() == 0 {
				continue
			}
			//if run.op.Children[0].Typ == POT_Filter {
			//

			//fmt.Println("child raw chunk")
			//childChunk.print()
			//}

			cnt += childChunk.Card()

			typs := make([]common.LType, 0)
			typs = append(typs, run.hAggr._groupedAggrData._groupTypes...)
			typs = append(typs, run.hAggr._groupedAggrData._payloadTypes...)
			typs = append(typs, run.hAggr._groupedAggrData._childrenOutputTypes...)
			groupChunk := &chunk.Chunk{}
			groupChunk.Init(typs, util.DefaultVectorSize)
			err = run.state.groupbyWithParamsExec.executeExprs([]*chunk.Chunk{childChunk, nil, nil}, groupChunk)
			if err != nil {
				return InvalidOpResult, err
			}

			//groupChunk.print()
			run.hAggr.Sink(groupChunk)

		}
		run.hAggr.Finalize()
		run.hAggr._has = HAS_SCAN
		fmt.Println("get build child cnt", cnt)
		fmt.Println("tuple collection size", run.hAggr._groupings[0]._tableData._finalizedHT._dataCollection._count)
	}
	if run.hAggr._has == HAS_SCAN {
		if run.state.haScanState == nil {
			run.state.haScanState = NewHashAggrScanState()
			err = run.initChildren()
			if err != nil {
				return InvalidOpResult, err
			}
		}

		for {

			if run.state.ungroupAggr {
				if run.state.ungroupAggrDone {
					return Done, nil
				}
				run.state.ungroupAggrDone = true
			}

			groupAddAggrTypes := make([]common.LType, 0)
			groupAddAggrTypes = append(groupAddAggrTypes, run.hAggr._groupedAggrData._groupTypes...)
			groupAddAggrTypes = append(groupAddAggrTypes, run.hAggr._groupedAggrData._aggrReturnTypes...)
			groupAndAggrChunk := &chunk.Chunk{}
			groupAndAggrChunk.Init(groupAddAggrTypes, util.DefaultVectorSize)
			util.AssertFunc(len(run.hAggr._groupedAggrData._groupingFuncs) == 0)
			childChunk := &chunk.Chunk{}
			childChunk.Init(run.hAggr._groupedAggrData._childrenOutputTypes, util.DefaultVectorSize)
			res = run.hAggr.GetData(run.state.haScanState, groupAndAggrChunk, childChunk)
			if res == InvalidOpResult {
				return InvalidOpResult, nil
			}
			if res == Done {
				break
			}

			x := childChunk.Card()

			//3.get group by + aggr states for the group

			//4.eval the filter on (child chunk + aggr states)

			//childChunk.print()

			//groupAndAggrChunk.print()

			//aggrStatesChunk.print()
			filterInputTypes := make([]common.LType, 0)
			filterInputTypes = append(filterInputTypes, run.hAggr._groupedAggrData._aggrReturnTypes...)
			filterInputChunk := &chunk.Chunk{}
			filterInputChunk.Init(filterInputTypes, util.DefaultVectorSize)
			for i := 0; i < len(run.hAggr._groupedAggrData._aggregates); i++ {
				filterInputChunk.Data[i].Reference(groupAndAggrChunk.Data[run.hAggr._groupedAggrData.GroupCount()+i])
			}
			filterInputChunk.SetCard(groupAndAggrChunk.Card())
			var count int
			count, err = state.filterExec.executeSelect([]*chunk.Chunk{childChunk, nil, filterInputChunk}, state.filterSel)
			if err != nil {
				return InvalidOpResult, err
			}

			if count == 0 {
				run.state.haScanState._filteredCnt1 += childChunk.Card() - count
				continue
			}

			var childChunk2 *chunk.Chunk
			var aggrStatesChunk2 *chunk.Chunk
			var filtered int
			if count == childChunk.Card() {
				childChunk2 = childChunk
				aggrStatesChunk2 = groupAndAggrChunk

				util.AssertFunc(childChunk.Card() == childChunk2.Card())
				util.AssertFunc(groupAndAggrChunk.Card() == aggrStatesChunk2.Card())
				util.AssertFunc(childChunk2.Card() == aggrStatesChunk2.Card())
			} else {
				filtered = childChunk.Card() - count
				run.state.haScanState._filteredCnt2 += filtered

				childChunkIndice := make([]int, 0)
				for i := 0; i < childChunk.ColumnCount(); i++ {
					childChunkIndice = append(childChunkIndice, i)
				}
				aggrStatesChunkIndice := make([]int, 0)
				for i := 0; i < groupAndAggrChunk.ColumnCount(); i++ {
					aggrStatesChunkIndice = append(aggrStatesChunkIndice, i)
				}
				childChunk2 = &chunk.Chunk{}
				childChunk2.Init(run.children[0].outputTypes, util.DefaultVectorSize)
				aggrStatesChunk2 = &chunk.Chunk{}
				aggrStatesChunk2.Init(groupAddAggrTypes, util.DefaultVectorSize)

				//slice
				childChunk2.SliceIndice(childChunk, state.filterSel, count, 0, childChunkIndice)
				aggrStatesChunk2.SliceIndice(groupAndAggrChunk, state.filterSel, count, 0, aggrStatesChunkIndice)

				util.AssertFunc(count == childChunk2.Card())
				util.AssertFunc(count == aggrStatesChunk2.Card())
				util.AssertFunc(childChunk2.Card() == aggrStatesChunk2.Card())
			}

			var aggrStatesChunk3 *chunk.Chunk
			if run.state.ungroupAggr {
				//remove const groupby expr
				aggrStatesTyps := make([]common.LType, 0)
				aggrStatesTyps = append(aggrStatesTyps, run.hAggr._groupedAggrData._aggrReturnTypes...)
				aggrStatesChunk3 = &chunk.Chunk{}
				aggrStatesChunk3.Init(aggrStatesTyps, util.DefaultVectorSize)

				for i := 0; i < len(run.hAggr._groupedAggrData._aggregates); i++ {
					aggrStatesChunk3.Data[i].Reference(aggrStatesChunk2.Data[run.hAggr._groupedAggrData.GroupCount()+i])
				}
				aggrStatesChunk3.SetCard(aggrStatesChunk2.Card())
			} else {
				aggrStatesChunk3 = aggrStatesChunk2
			}

			//5. eval the output
			err = run.state.outputExec.executeExprs([]*chunk.Chunk{childChunk2, nil, aggrStatesChunk3}, output)
			if err != nil {
				return InvalidOpResult, err
			}
			if filtered == 0 {
				util.AssertFunc(filtered == 0)
				util.AssertFunc(output.Card() == childChunk2.Card())
				util.AssertFunc(x >= childChunk2.Card())
			}
			util.AssertFunc(output.Card()+filtered == childChunk.Card())
			util.AssertFunc(x == childChunk.Card())
			util.AssertFunc(output.Card() == childChunk2.Card())

			run.state.haScanState._outputCnt += output.Card()
			run.state.haScanState._childCnt2 += childChunk.Card()
			run.state.haScanState._childCnt3 += x
			if output.Card() > 0 {

				//output.print()
				return haveMoreOutput, nil
			}
		}
	}
	fmt.Println("scan cnt",
		"childCnt",
		run.state.haScanState._childCnt,
		"childCnt2",
		run.state.haScanState._childCnt2,
		"childCnt3",
		run.state.haScanState._childCnt3,
		"outputCnt",
		run.state.haScanState._outputCnt,
		"filteredCnt1",
		run.state.haScanState._filteredCnt1,
		"filteredCnt2",
		run.state.haScanState._filteredCnt2,
	)
	return Done, nil
}

func (run *Runner) aggrClose() error {
	run.hAggr = nil
	return nil
}

func (run *Runner) joinInit() error {
	run.state = &OperatorState{
		outputExec: NewExprExec(run.op.Outputs...),
	}
	if len(run.op.OnConds) != 0 {
		run.hjoin = NewHashJoin(run.op, run.op.OnConds)
	} else {
		types := make([]common.LType, len(run.op.Children[1].Outputs))
		for i, e := range run.op.Children[1].Outputs {
			types[i] = e.DataTyp
		}
		//output pos -> [child,pos]
		outputPosMap := make(map[int]ColumnBind)
		for i, output := range run.op.Outputs {
			set := make(ColumnBindSet)
			collectColRefs(output, set)
			util.AssertFunc(!set.empty() && len(set) == 1)
			for bind := range set {
				outputPosMap[i] = bind
			}
		}
		run.cross = NewCrossProduct(types)
		run.cross._crossExec._outputExec = run.state.outputExec
		run.cross._crossExec._outputPosMap = outputPosMap
	}

	return nil
}

func (run *Runner) joinExec(output *chunk.Chunk, state *OperatorState) (OperatorResult, error) {
	if run.cross == nil {
		return run.hashJoinExec(output, state)
	} else {
		return run.crossProductExec(output, state)
	}
}

func (run *Runner) hashJoinExec(output *chunk.Chunk, state *OperatorState) (OperatorResult, error) {
	//1. Build Hash Table on the right child
	res, err := run.joinBuildHashTable(state)
	if err != nil {
		return InvalidOpResult, err
	}
	if res == InvalidOpResult {
		return InvalidOpResult, nil
	}
	//2. probe stage
	//probe
	if run.hjoin._hjs == HJS_BUILD || run.hjoin._hjs == HJS_PROBE {
		if run.hjoin._hjs == HJS_BUILD {
			run.hjoin._hjs = HJS_PROBE
		}

		//continue unfinished can
		if run.hjoin._scan != nil {
			nextChunk := chunk.Chunk{}
			nextChunk.Init(run.hjoin._scanNextTyps, util.DefaultVectorSize)
			run.hjoin._scan.Next(run.hjoin._joinKeys, run.hjoin._scan._leftChunk, &nextChunk)
			if nextChunk.Card() > 0 {
				err = run.evalJoinOutput(&nextChunk, output)
				if err != nil {
					return 0, err
				}
				return haveMoreOutput, nil
			}
			run.hjoin._scan = nil
		}

		//probe
		leftChunk := &chunk.Chunk{}
		res, err = run.execChild(run.children[0], leftChunk, state)
		if err != nil {
			return 0, err
		}
		switch res {
		case Done:
			return Done, nil
		case InvalidOpResult:
			return InvalidOpResult, nil
		}

		//fmt.Println("left chunk", leftChunk.card())
		//leftChunk.print()

		run.hjoin._joinKeys.Reset()
		err = run.hjoin._probExec.executeExprs([]*chunk.Chunk{leftChunk, nil, nil}, run.hjoin._joinKeys)
		if err != nil {
			return 0, err
		}
		run.hjoin._scan = run.hjoin._ht.Probe(run.hjoin._joinKeys)
		run.hjoin._scan._leftChunk = leftChunk
		nextChunk := chunk.Chunk{}
		nextChunk.Init(run.hjoin._scanNextTyps, util.DefaultVectorSize)
		run.hjoin._scan.Next(run.hjoin._joinKeys, run.hjoin._scan._leftChunk, &nextChunk)
		if nextChunk.Card() > 0 {
			err = run.evalJoinOutput(&nextChunk, output)
			if err != nil {
				return 0, err
			}
			return haveMoreOutput, nil
		} else {
			run.hjoin._scan = nil
		}
		return haveMoreOutput, nil
	}
	return 0, nil
}

func (run *Runner) crossProductExec(output *chunk.Chunk, state *OperatorState) (OperatorResult, error) {
	//1. Build Hash Table on the right child
	res, err := run.crossBuild(state)
	if err != nil {
		return InvalidOpResult, err
	}
	if res == InvalidOpResult {
		return InvalidOpResult, nil
	}
	//2. probe stage
	//probe
	if run.cross._crossStage == CROSS_BUILD || run.cross._crossStage == CROSS_PROBE {
		if run.cross._crossStage == CROSS_BUILD {
			run.cross._crossStage = CROSS_PROBE
		}

		nextInput := false

		//probe
		for {
			if run.cross._input == nil || nextInput {
				nextInput = false
				run.cross._input = &chunk.Chunk{}
				res, err = run.execChild(run.children[0], run.cross._input, state)
				if err != nil {
					return 0, err
				}
				switch res {
				case Done:
					return Done, nil
				case InvalidOpResult:
					return InvalidOpResult, nil
				}

				//run.cross._input.print()
			}

			res, err = run.cross.Execute(run.cross._input, output)
			if err != nil {
				return 0, err
			}
			switch res {
			case Done:
				return Done, nil
			case NeedMoreInput:
				nextInput = true
			case InvalidOpResult:
				return InvalidOpResult, nil
			}
			if !nextInput {
				break
			}
		}
		return res, nil
	}
	return 0, nil
}

func (run *Runner) crossBuild(state *OperatorState) (OperatorResult, error) {
	var err error
	var res OperatorResult
	if run.cross._crossStage == CROSS_INIT {
		run.cross._crossStage = CROSS_BUILD
		cnt := 0
		for {
			rightChunk := &chunk.Chunk{}
			res, err = run.execChild(run.children[1], rightChunk, state)
			if err != nil {
				return 0, err
			}
			if res == InvalidOpResult {
				return InvalidOpResult, nil
			}
			if res == Done {
				break
			}

			if rightChunk.Card() == 0 {
				continue
			}

			cnt += rightChunk.Card()

			//rightChunk.print()
			run.cross.Sink(rightChunk)
		}
		fmt.Println("right count", cnt)
		run.cross._crossStage = CROSS_PROBE
	}

	return Done, nil
}

func (run *Runner) evalJoinOutput(nextChunk, output *chunk.Chunk) (err error) {
	leftChunk := chunk.Chunk{}
	leftTyps := run.hjoin._scanNextTyps[:len(run.hjoin._leftIndice)]
	leftChunk.Init(leftTyps, util.DefaultVectorSize)
	leftChunk.ReferenceIndice(nextChunk, run.hjoin._leftIndice)

	rightChunk := chunk.Chunk{}
	rightChunk.Init(run.hjoin._buildTypes, util.DefaultVectorSize)
	rightChunk.ReferenceIndice(nextChunk, run.hjoin._rightIndice)

	var thisChunk *chunk.Chunk
	if run.op.JoinTyp == LOT_JoinTypeMARK || run.op.JoinTyp == LOT_JoinTypeAntiMARK {
		thisChunk = &chunk.Chunk{}
		markTyp := []common.LType{util.Back(run.hjoin._scanNextTyps)}
		thisChunk.Init(markTyp, util.DefaultVectorSize)
		thisChunk.ReferenceIndice(nextChunk, []int{run.hjoin._markIndex})
	}

	err = run.state.outputExec.executeExprs(
		[]*chunk.Chunk{
			&leftChunk,
			&rightChunk,
			thisChunk,
		},
		output,
	)
	return err
}

func (run *Runner) joinBuildHashTable(state *OperatorState) (OperatorResult, error) {
	var err error
	var res OperatorResult
	if run.hjoin._hjs == HJS_INIT {
		run.hjoin._hjs = HJS_BUILD
		cnt := 0
		for {
			rightChunk := &chunk.Chunk{}
			res, err = run.execChild(run.children[1], rightChunk, state)
			if err != nil {
				return 0, err
			}
			if res == InvalidOpResult {
				return InvalidOpResult, nil
			}
			if res == Done {
				run.hjoin._ht.Finalize()
				break
			}

			//fmt.Println("right child chunk")
			//rightChunk.print()

			cnt++
			err = run.hjoin.Build(rightChunk)
			if err != nil {
				return 0, err
			}
		}
		fmt.Println("right hash table count", run.hjoin._ht.count())
		run.hjoin._hjs = HJS_PROBE
	}

	return Done, nil
}

func (run *Runner) joinClose() error {
	run.hjoin = nil
	run.cross = nil
	return nil
}

func (run *Runner) projInit() error {
	projTypes := make([]common.LType, 0)
	for _, proj := range run.op.Projects {
		projTypes = append(projTypes, proj.DataTyp)
	}
	run.state = &OperatorState{
		projTypes:  projTypes,
		projExec:   NewExprExec(run.op.Projects...),
		outputExec: NewExprExec(run.op.Outputs...),
	}
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

func (run *Runner) scanInit() error {
	var err error
	switch run.op.ScanTyp {
	case ScanTypeTable:

		{
			tabEnt := storage.GCatalog.GetEntry(run.Txn, storage.CatalogTypeTable, run.op.Database, run.op.Table)
			if tabEnt == nil {
				return fmt.Errorf("no table %s in schema %s", run.op.Database, run.op.Table)
			}
			run.tabEnt = tabEnt
			col2Idx := tabEnt.GetColumn2Idx()
			typs := tabEnt.GetTypes()
			run.colIndice = make([]int, 0)
			for _, col := range run.op.Columns {
				if idx, has := col2Idx[col]; has {
					run.colIndice = append(run.colIndice, idx)
					run.readedColTyps = append(run.readedColTyps, typs[idx])
				} else {
					return fmt.Errorf("no such column %s in %s.%s", col, run.op.Database, run.op.Table)
				}
			}
		}
		{
			//read schema
			//cat, err := tpchCatalog().Table(run.op.Database, run.op.Table)
			//if err != nil {
			//	return err
			//}
			//run.colIndice = make([]int, 0)
			//for _, col := range run.op.Columns {
			//	if idx, has := cat.Column2Idx[col]; has {
			//		run.colIndice = append(run.colIndice, idx)
			//		run.readedColTyps = append(run.readedColTyps, cat.Types[idx])
			//	} else {
			//		return fmt.Errorf("no such column %s in %s.%s", col, run.op.Database, run.op.Table)
			//	}
			//}
			//
			////open data file
			//switch run.cfg.Tpch1g.Data.Format {
			//case "parquet":
			//	run.pqFile, err = pqLocal.NewLocalFileReader(run.cfg.Tpch1g.Data.Path + "/" + run.op.Table + ".parquet")
			//	if err != nil {
			//		return err
			//	}
			//
			//	run.pqReader, err = pqReader.NewParquetColumnReader(run.pqFile, 1)
			//	if err != nil {
			//		return err
			//	}
			//case "csv":
			//	run.tablePath = run.cfg.Tpch1g.Data.Path + "/" + run.op.Table + ".tbl"
			//	run.dataFile, err = os.OpenFile(run.tablePath, os.O_RDONLY, 0755)
			//	if err != nil {
			//		return err
			//	}
			//
			//	//init csv reader
			//	run.reader = csv.NewReader(run.dataFile)
			//	run.reader.Comma = '|'
			//default:
			//	panic("usp format")
			//}
		}

	case ScanTypeValuesList:
		run.colIndice = make([]int, 0)
		for _, col := range run.op.Columns {
			if idx, has := run.op.ColName2Idx[col]; has {
				run.colIndice = append(run.colIndice, idx)
				run.readedColTyps = append(run.readedColTyps, run.op.Types[idx])
			} else {
				return fmt.Errorf("no such column %s in %s.%s", col, run.op.Database, run.op.Table)
			}
		}
		run.readedColTyps = run.op.Types
	case ScanTypeCopyFrom:
		run.colIndice = run.op.ScanInfo.ColumnIds
		run.readedColTyps = run.op.ScanInfo.ReturnedTypes
		//open data file
		switch run.op.ScanInfo.Format {
		case "parquet":
			run.pqFile, err = pqLocal.NewLocalFileReader(run.op.ScanInfo.FilePath)
			if err != nil {
				return err
			}

			run.pqReader, err = pqReader.NewParquetColumnReader(run.pqFile, 1)
			if err != nil {
				return err
			}
		case "csv":
			run.tablePath = run.op.ScanInfo.FilePath
			run.dataFile, err = os.OpenFile(run.tablePath, os.O_RDONLY, 0755)
			if err != nil {
				return err
			}

			comma := ','
			if commaOpt := getFormatFun("delimiter", run.op.ScanInfo.Opts); commaOpt != nil {
				comma = int32(commaOpt.Opt[0])
			}

			//init csv reader
			run.reader = csv.NewReader(run.dataFile)
			run.reader.Comma = comma
		default:
			panic("usp format")
		}
	default:
		panic("usp")
	}
	var filterExec *ExprExec
	filterExec, err = initFilterExec(run.op.Filters)
	if err != nil {
		return err
	}

	run.state = &OperatorState{
		filterExec: filterExec,
		filterSel:  chunk.NewSelectVector(util.DefaultVectorSize),
		showRaw:    run.cfg.Debug.ShowRaw,
	}

	return nil
}

func (run *Runner) scanExec(output *chunk.Chunk, state *OperatorState) (OperatorResult, error) {

	for output.Card() == 0 {
		res, err := run.scanRows(output, state, util.DefaultVectorSize)
		if err != nil {
			return InvalidOpResult, err
		}
		if res {
			return Done, nil
		}
	}
	return haveMoreOutput, nil
}

func (run *Runner) scanRows(output *chunk.Chunk, state *OperatorState, maxCnt int) (bool, error) {
	if maxCnt == 0 {
		return false, nil
	}
	if run.cfg.Debug.EnableMaxScanRows {
		if run.maxRows > run.cfg.Debug.MaxScanRows {
			return true, nil
		}
	}

	readed := &chunk.Chunk{}
	readed.Init(run.readedColTyps, maxCnt)
	var err error

	switch run.op.ScanTyp {
	case ScanTypeTable:
		{
			if run.state.tableScanState == nil {
				run.state.tableScanState = storage.NewTableScanState()
				colIds := make([]storage.IdxType, 0)
				for _, colId := range run.colIndice {
					colIds = append(colIds, storage.IdxType(colId))
				}
				run.tabEnt.GetStorage().InitScan(
					run.Txn,
					run.state.tableScanState,
					colIds)
			}
			run.tabEnt.GetStorage().Scan(run.Txn, readed, run.state.tableScanState)
		}
		{
			//read table
			//switch run.cfg.Tpch1g.Data.Format {
			//case "parquet":
			//	err = run.readParquetTable(readed, state, maxCnt)
			//	if err != nil {
			//		return false, err
			//	}
			//case "csv":
			//	err = run.readCsvTable(readed, state, maxCnt)
			//	if err != nil {
			//		return false, err
			//	}
			//default:
			//	panic("usp format")
			//}
		}
	case ScanTypeValuesList:
		err = run.readValues(readed, state, maxCnt)
		if err != nil {
			return false, err
		}
	case ScanTypeCopyFrom:
		//read table
		switch run.op.ScanInfo.Format {
		case "parquet":
			err = run.readParquetTable(readed, state, maxCnt)
			if err != nil {
				return false, err
			}
		case "csv":
			err = run.readCsvTable(readed, state, maxCnt)
			if err != nil {
				return false, err
			}
		default:
			panic("usp format")
		}
	default:
		panic("usp")
	}

	if readed.Card() == 0 {
		return true, nil
	}

	if run.cfg.Debug.EnableMaxScanRows {
		run.maxRows += readed.Card()
	}

	err = run.runFilterExec(readed, output, true)
	if err != nil {
		return false, err
	}
	return false, nil
}

func (run *Runner) scanClose() error {
	switch run.op.ScanTyp {
	case ScanTypeTable:
		{

		}
		{
			//switch run.cfg.Tpch1g.Data.Format {
			//case "csv":
			//	run.reader = nil
			//	return run.dataFile.Close()
			//case "parquet":
			//	run.pqReader.ReadStop()
			//	return run.pqFile.Close()
			//default:
			//	panic("usp format")
			//}
		}

	case ScanTypeValuesList:
		return nil
	case ScanTypeCopyFrom:
		switch run.op.ScanInfo.Format {
		case "csv":
			run.reader = nil
			return run.dataFile.Close()
		case "parquet":
			run.pqReader.ReadStop()
			return run.pqFile.Close()
		default:
			panic("usp format")
		}
	default:
		panic("usp")
	}
	return nil
}
func (run *Runner) readParquetTable(output *chunk.Chunk, state *OperatorState, maxCnt int) error {
	rowCont := -1
	var err error
	var values []interface{}

	//fill field into vector
	for j, idx := range run.colIndice {
		values, _, _, err = run.pqReader.ReadColumnByIndex(int64(idx), int64(maxCnt))
		if err != nil {
			//EOF
			if errors.Is(err, io.EOF) {
				break
			}
			return err
		}

		if rowCont < 0 {
			rowCont = len(values)
		} else if len(values) != rowCont {
			return fmt.Errorf("column %d has different count of values %d with previous columns %d", idx, len(values), rowCont)
		}

		vec := output.Data[j]
		for i := 0; i < len(values); i++ {
			//[row i, col j]
			val, err := parquetColToValue(values[i], vec.Typ())
			if err != nil {
				return err
			}
			vec.SetValue(i, val)
			if state.showRaw {
				fmt.Print(values[i], " ")
			}
		}
		if state.showRaw {
			fmt.Println()
		}
	}
	output.SetCard(rowCont)
	return nil
}

func (run *Runner) readCsvTable(output *chunk.Chunk, state *OperatorState, maxCnt int) error {
	rowCont := 0
	for i := 0; i < maxCnt; i++ {
		//read line
		line, err := run.reader.Read()
		if err != nil {
			//EOF
			if errors.Is(err, io.EOF) {
				break
			}
			return err
		}
		//fill field into vector
		for j, idx := range run.colIndice {
			if idx >= len(line) {
				return errors.New("no enough fields in the line")
			}
			field := line[idx]
			//[row i, col j] = field
			vec := output.Data[j]
			val, err := fieldToValue(field, vec.Typ())
			if err != nil {
				return err
			}
			vec.SetValue(i, val)
			if state.showRaw {
				fmt.Print(field, " ")
			}
		}
		if state.showRaw {
			fmt.Println()
		}
		rowCont++
	}
	output.SetCard(rowCont)

	return nil
}

func (run *Runner) readValues(output *chunk.Chunk, state *OperatorState, maxCnt int) error {
	if run.op.collection.Count() == 0 {
		output.SetCap(0)
		return nil
	}

	if run.state.colScanState == nil {
		run.state.colScanState = &ColumnDataScanState{}
		run.op.collection.initScan(run.state.colScanState)
	}

	run.op.collection.Scan(run.state.colScanState, output)
	if state.showRaw {
		output.Print()
	}
	return nil
}

func fieldToValue(field string, lTyp common.LType) (*chunk.Value, error) {
	var err error
	val := &chunk.Value{
		Typ: lTyp,
	}
	switch lTyp.Id {
	case common.LTID_DATE:
		d, err := time.Parse(time.DateOnly, field)
		if err != nil {
			return nil, err
		}
		val.I64 = int64(d.Year())
		val.I64_1 = int64(d.Month())
		val.I64_2 = int64(d.Day())
	case common.LTID_INTEGER:
		val.I64, err = strconv.ParseInt(field, 10, 64)
		if err != nil {
			return nil, err
		}
	case common.LTID_VARCHAR:
		val.Str = field
	default:
		panic("usp")
	}
	return val, nil
}

func parquetColToValue(field any, lTyp common.LType) (*chunk.Value, error) {
	val := &chunk.Value{
		Typ: lTyp,
	}
	switch lTyp.Id {
	case common.LTID_DATE:
		if _, ok := field.(int32); !ok {
			panic("usp")
		}

		d := time.Date(1970, 1, int(1+field.(int32)), 0, 0, 0, 0, time.UTC)
		val.I64 = int64(d.Year())
		val.I64_1 = int64(d.Month())
		val.I64_2 = int64(d.Day())
	case common.LTID_INTEGER:
		switch fVal := field.(type) {
		case int32:
			val.I64 = int64(fVal)
		case int64:
			val.I64 = fVal
		default:
			panic("usp")
		}
	case common.LTID_BIGINT:
		switch fVal := field.(type) {
		case int32:
			val.I64 = int64(fVal)
		case int64:
			val.I64 = fVal
		default:
			panic("usp")
		}
	case common.LTID_VARCHAR:
		if _, ok := field.(string); !ok {
			panic("usp")
		}

		val.Str = field.(string)
	case common.LTID_DECIMAL:
		p10 := int64(1)
		for i := 0; i < lTyp.Scale; i++ {
			p10 *= 10
		}
		switch v := field.(type) {
		case int32:
			val.I64 = int64(v) / p10
			val.I64_1 = int64(v) % p10
		case int64:
			val.I64 = v / p10
			val.I64_1 = int64(v) % p10
		default:
			panic("usp")
		}

	default:
		panic("usp")
	}
	return val, nil
}
