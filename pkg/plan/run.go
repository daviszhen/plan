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
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"time"

	pqLocal "github.com/xitongsys/parquet-go-source/local"
	pqReader "github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/source"
	"go.uber.org/zap"

	"github.com/daviszhen/plan/pkg/util"
)

var tpch1gAsts = []*Ast{
	tpchQ1(),
	tpchQ2(),
	tpchQ3(),
	tpchQ4(),
	tpchQ5(),
	tpchQ6(),
	tpchQ7(),
	tpchQ8(),
	tpchQ9(),
	tpchQ10(),
	tpchQ11(),
	tpchQ12(),
	tpchQ13(),
	tpchQ14(),
	tpchQ15(),
	tpchQ16(),
	tpchQ17(),
	tpchQ18(),
	tpchQ19(),
	tpchQ20(),
	tpchQ21(),
	tpchQ22(),
}

func Run(cfg *util.Config) error {
	if cfg == nil {
		return fmt.Errorf("config is nil")
	}

	if cfg.Tpch1g.QueryId == 0 {
		failed := make([]int, 0)
		for i, ast := range tpch1gAsts {
			err := execQuery(cfg, i+1, ast)
			if err != nil {
				util.Error("execQuery failed", zap.Int("queryId", i+1))
				failed = append(failed, i+1)
			}
		}
		if len(failed) > 0 {
			fmt.Printf("Failed query count: %d\n", len(failed))
			for _, i := range failed {
				fmt.Println("Query", i, "failed")
			}
		}
	} else {
		id := cfg.Tpch1g.QueryId
		if id <= 0 || id > uint(len(tpch1gAsts)) {
			return fmt.Errorf("invalid query id:%d", id)
		}
		return execQuery(cfg, int(id), tpch1gAsts[id-1])
	}
	return nil
}

func execQuery(cfg *util.Config, id int, ast *Ast) (err error) {
	defer func() {
		if rErr := recover(); rErr != nil {
			err = errors.Join(err, errors.New(fmt.Sprint(rErr)))
		}
	}()
	var root *PhysicalOperator
	root, err = genPhyPlan(ast)
	if err != nil {
		return err
	}
	fname := fmt.Sprintf("q%d.result", id)
	path := filepath.Join(cfg.Tpch1g.Result.Path, fname)
	var resFile *os.File
	if len(path) != 0 {
		resFile, err = os.OpenFile(path, os.O_WRONLY|os.O_CREATE, 0644)
		if err != nil {
			return err
		}
		defer func() {
			resFile.Sync()
			resFile.Close()
		}()

		if cfg.Tpch1g.Result.NeedHeadLine {
			_, err = resFile.WriteString("#" + fname + "\n")
			if err != nil {
				return err
			}
		}
	}

	return execOps(cfg, nil, resFile, []*PhysicalOperator{root})
}

func genPhyPlan(ast *Ast) (*PhysicalOperator, error) {
	builder := NewBuilder()
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
	serial Serialize,
	resFile *os.File,
	ops []*PhysicalOperator) error {
	var err error

	for _, op := range ops {
		if conf.Debug.PrintPlan {
			fmt.Println(op.String())
		}

		run := &Runner{
			op:    op,
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
			output := &Chunk{}
			output.setCap(defaultVectorSize)
			result, err := run.Execute(nil, output, run.state)
			if err != nil {
				return err
			}
			if result == Done {
				break
			}
			if output.card() > 0 {
				assertFunc(output.card() != 0)

				if serial != nil {
					err = output.serialize(serial)
					if err != nil {
						return err
					}
				}

				if resFile != nil {
					err = output.saveToFile(resFile)
					if err != nil {
						return err
					}
				}

				rowCnt += output.card()
				if conf.Debug.PrintResult {
					output.print()
				}
			}
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
	keyTypes     []LType
	payloadTypes []LType

	projTypes  []LType
	projExec   *ExprExec
	outputExec *ExprExec

	//filter projExec used in aggr, filter, scan
	filterExec *ExprExec
	filterSel  *SelectVector

	//for aggregate
	referChildren         bool
	constGroupby          bool
	ungroupAggr           bool
	ungroupAggrDone       bool
	haScanState           *HashAggrScanState
	groupbyWithParamsExec *ExprExec
	groupbyExec           *ExprExec

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

var _ OperatorExec = &Runner{}

type OperatorExec interface {
	Init() error
	Execute(input, output *Chunk, state *OperatorState) (OperatorResult, error)
	Close() error
}

type Runner struct {
	cfg   *util.Config
	op    *PhysicalOperator
	state *OperatorState
	//for stub
	deserial   Deserialize
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
	readedColTyps []LType
	tablePath     string
	//for test cross product
	maxRows int

	//common
	outputTypes  []LType
	outputIndice []int
	children     []*Runner
}

func (run *Runner) initChildren() error {
	run.children = []*Runner{}
	for _, child := range run.op.Children {
		childRun := &Runner{
			op:    child,
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
		run.outputTypes = append(run.outputTypes, output.DataTyp.LTyp)
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
	default:
		panic("usp")
	}
}

func (run *Runner) Execute(input, output *Chunk, state *OperatorState) (OperatorResult, error) {
	output.init(run.outputTypes, defaultVectorSize)
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
	default:
		panic("usp")
	}
}

func (run *Runner) execChild(child *Runner, output *Chunk, state *OperatorState) (OperatorResult, error) {
	for output.card() == 0 {
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
	default:
		panic("usp")
	}
}

func (run *Runner) stubInit() error {
	deserial, err := NewFileDeserialize(run.op.Table)
	if err != nil {
		return err
	}
	run.deserial = deserial
	run.maxRowCnt = run.op.ChunkCount
	return nil
}

func (run *Runner) stubExec(output *Chunk, state *OperatorState) (OperatorResult, error) {
	if run.maxRowCnt != 0 && run.rowReadCnt >= run.maxRowCnt {
		return Done, nil
	}
	err := output.deserialize(run.deserial)
	if err != nil {
		return InvalidOpResult, err
	}
	if output.card() == 0 {
		return Done, nil
	}
	run.rowReadCnt += output.card()
	return haveMoreOutput, nil
}

func (run *Runner) stubClose() error {
	return run.deserial.Close()
}

func (run *Runner) limitInit() error {
	//collect children output types
	childTypes := make([]LType, 0)
	for _, outputExpr := range run.op.Children[0].Outputs {
		childTypes = append(childTypes, outputExpr.DataTyp.LTyp)
	}

	run.limit = NewLimit(childTypes, run.op.Limit, run.op.Offset)
	run.state = &OperatorState{
		outputExec: NewExprExec(run.op.Outputs...),
	}

	return nil
}

func (run *Runner) limitExec(output *Chunk, state *OperatorState) (OperatorResult, error) {
	var err error
	var res OperatorResult
	if run.limit._state == LIMIT_INIT {
		cnt := 0
		for {
			childChunk := &Chunk{}
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
			if childChunk.card() == 0 {
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
			read := &Chunk{}
			read.init(run.limit._childTypes, defaultVectorSize)
			getRet := run.limit.GetData(read)
			if getRet == SrcResDone {
				break
			}

			//evaluate output
			err = run.state.outputExec.executeExprs([]*Chunk{read, nil, nil}, output)
			if err != nil {
				return InvalidOpResult, err
			}

			if output.card() > 0 {
				return haveMoreOutput, nil
			}
		}

	}

	if output.card() == 0 {
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
	keyTypes := make([]LType, 0)
	realOrderByExprs := make([]*Expr, 0)
	for _, by := range run.op.OrderBys {
		child := by.Children[0]
		keyTypes = append(keyTypes, child.DataTyp.LTyp)
		realOrderByExprs = append(realOrderByExprs, child)
	}

	payLoadTypes := make([]LType, 0)
	for _, output := range run.op.Outputs {
		payLoadTypes = append(payLoadTypes,
			output.DataTyp.LTyp)
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

func (run *Runner) orderExec(output *Chunk, state *OperatorState) (OperatorResult, error) {
	var err error
	var res OperatorResult
	if run.localSort._sortState == SS_INIT {
		cnt := 0
		for {
			childChunk := &Chunk{}
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
			if childChunk.card() == 0 {
				continue
			}

			//childChunk.print()

			//evaluate order by expr
			key := &Chunk{}
			key.init(run.state.keyTypes, defaultVectorSize)
			err = run.state.orderKeyExec.executeExprs(
				[]*Chunk{childChunk, nil, nil},
				key,
			)
			if err != nil {
				return 0, err
			}

			//key.print()

			//evaluate payload expr
			payload := &Chunk{}
			payload.init(run.state.payloadTypes, defaultVectorSize)

			err = run.state.outputExec.executeExprs(
				[]*Chunk{childChunk, nil, nil},
				payload,
			)
			if err != nil {
				return 0, err
			}

			assertFunc(key.card() != 0 && payload.card() != 0)
			cnt += key.card()
			assertFunc(key.card() == payload.card())

			//key.print()

			//payload.print()

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

	if output.card() == 0 {
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
		filterSel:  NewSelectVector(defaultVectorSize),
	}
	return nil
}

func initFilterExec(filters []*Expr) (*ExprExec, error) {
	//init filter
	//convert filters into "... AND ..."
	var err error
	var andFilter *Expr
	if len(filters) > 0 {
		var impl *Impl
		andFilter = filters[0]
		for i, filter := range filters {
			if i > 0 {
				if andFilter.DataTyp.LTyp.id != LTID_BOOLEAN ||
					filter.DataTyp.LTyp.id != LTID_BOOLEAN {
					return nil, fmt.Errorf("need boolean expr")
				}
				argsTypes := []ExprDataType{
					andFilter.DataTyp,
					filter.DataTyp,
				}
				impl, err = GetFunctionImpl(
					AND,
					argsTypes)
				if err != nil {
					return nil, err
				}
				andFilter = &Expr{
					Typ:     ET_Func,
					SubTyp:  ET_And,
					DataTyp: impl.RetTypeDecider(argsTypes),
					FuncId:  AND,
					Children: []*Expr{
						andFilter,
						filter,
					},
				}
			}
		}
	}
	return NewExprExec(andFilter), nil
}

func (run *Runner) runFilterExec(input *Chunk, output *Chunk, filterOnLocal bool) error {
	//filter
	var err error
	var count int
	//if !filterOnLocal {
	//
	//	input.print()
	//}
	if filterOnLocal {
		count, err = run.state.filterExec.executeSelect([]*Chunk{nil, nil, input}, run.state.filterSel)
		if err != nil {
			return err
		}
	} else {
		count, err = run.state.filterExec.executeSelect([]*Chunk{input, nil, nil}, run.state.filterSel)
		if err != nil {
			return err
		}
	}

	//if !filterOnLocal {

	//}

	if count == input.card() {
		//reference
		output.referenceIndice(input, run.outputIndice)
	} else {
		//slice
		output.sliceIndice(input, run.state.filterSel, count, 0, run.outputIndice)
	}
	return nil
}

func (run *Runner) filterExec(output *Chunk, state *OperatorState) (OperatorResult, error) {
	childChunk := &Chunk{}
	var res OperatorResult
	var err error
	if len(run.children) != 0 {
		for {
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
			if childChunk.card() > 0 {
				break
			}
		}
	}

	err = run.runFilterExec(childChunk, output, false)
	if err != nil {
		return 0, err
	}
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
				Typ: ET_IConst,
				DataTyp: ExprDataType{
					LTyp: integer(),
				},
				Ivalue: 1,
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
		run.state.filterSel = NewSelectVector(defaultVectorSize)
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

func (run *Runner) aggrExec(output *Chunk, state *OperatorState) (OperatorResult, error) {
	var err error
	var res OperatorResult
	if run.hAggr._has == HAS_INIT {
		cnt := 0
		for {
			childChunk := &Chunk{}
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
			if childChunk.card() == 0 {
				continue
			}
			//if run.op.Children[0].Typ == POT_Filter {
			//

			//fmt.Println("child raw chunk")
			//childChunk.print()
			//}

			cnt += childChunk.card()

			typs := make([]LType, 0)
			typs = append(typs, run.hAggr._groupedAggrData._groupTypes...)
			typs = append(typs, run.hAggr._groupedAggrData._payloadTypes...)
			typs = append(typs, run.hAggr._groupedAggrData._childrenOutputTypes...)
			groupChunk := &Chunk{}
			groupChunk.init(typs, defaultVectorSize)
			err = run.state.groupbyWithParamsExec.executeExprs([]*Chunk{childChunk, nil, nil}, groupChunk)
			if err != nil {
				return InvalidOpResult, err
			}

			//groupChunk.print()
			run.hAggr.Sink(groupChunk)

		}
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

			groupAddAggrTypes := make([]LType, 0)
			groupAddAggrTypes = append(groupAddAggrTypes, run.hAggr._groupedAggrData._groupTypes...)
			groupAddAggrTypes = append(groupAddAggrTypes, run.hAggr._groupedAggrData._aggrReturnTypes...)
			groupAndAggrChunk := &Chunk{}
			groupAndAggrChunk.init(groupAddAggrTypes, defaultVectorSize)
			assertFunc(len(run.hAggr._groupedAggrData._groupingFuncs) == 0)
			childChunk := &Chunk{}
			childChunk.init(run.hAggr._groupedAggrData._childrenOutputTypes, defaultVectorSize)
			res = run.hAggr.GetData(run.state.haScanState, groupAndAggrChunk, childChunk)
			if res == InvalidOpResult {
				return InvalidOpResult, nil
			}
			if res == Done {
				break
			}

			x := childChunk.card()

			//3.get group by + aggr states for the group

			//4.eval the filter on (child chunk + aggr states)

			//childChunk.print()

			//groupAndAggrChunk.print()

			//aggrStatesChunk.print()
			filterInputTypes := make([]LType, 0)
			filterInputTypes = append(filterInputTypes, run.hAggr._groupedAggrData._aggrReturnTypes...)
			filterInputChunk := &Chunk{}
			filterInputChunk.init(filterInputTypes, defaultVectorSize)
			for i := 0; i < len(run.hAggr._groupedAggrData._aggregates); i++ {
				filterInputChunk._data[i].reference(groupAndAggrChunk._data[run.hAggr._groupedAggrData.GroupCount()+i])
			}
			filterInputChunk.setCard(groupAndAggrChunk.card())
			var count int
			count, err = state.filterExec.executeSelect([]*Chunk{childChunk, nil, filterInputChunk}, state.filterSel)
			if err != nil {
				return InvalidOpResult, err
			}

			if count == 0 {
				run.state.haScanState._filteredCnt1 += childChunk.card() - count
				continue
			}

			var childChunk2 *Chunk
			var aggrStatesChunk2 *Chunk
			var filtered int
			if count == childChunk.card() {
				childChunk2 = childChunk
				aggrStatesChunk2 = groupAndAggrChunk

				assertFunc(childChunk.card() == childChunk2.card())
				assertFunc(groupAndAggrChunk.card() == aggrStatesChunk2.card())
				assertFunc(childChunk2.card() == aggrStatesChunk2.card())
			} else {
				filtered = childChunk.card() - count
				run.state.haScanState._filteredCnt2 += filtered

				childChunkIndice := make([]int, 0)
				for i := 0; i < childChunk.columnCount(); i++ {
					childChunkIndice = append(childChunkIndice, i)
				}
				aggrStatesChunkIndice := make([]int, 0)
				for i := 0; i < groupAndAggrChunk.columnCount(); i++ {
					aggrStatesChunkIndice = append(aggrStatesChunkIndice, i)
				}
				childChunk2 = &Chunk{}
				childChunk2.init(run.children[0].outputTypes, defaultVectorSize)
				aggrStatesChunk2 = &Chunk{}
				aggrStatesChunk2.init(groupAddAggrTypes, defaultVectorSize)

				//slice
				childChunk2.sliceIndice(childChunk, state.filterSel, count, 0, childChunkIndice)
				aggrStatesChunk2.sliceIndice(groupAndAggrChunk, state.filterSel, count, 0, aggrStatesChunkIndice)

				assertFunc(count == childChunk2.card())
				assertFunc(count == aggrStatesChunk2.card())
				assertFunc(childChunk2.card() == aggrStatesChunk2.card())
			}

			var aggrStatesChunk3 *Chunk
			if run.state.ungroupAggr {
				//remove const groupby expr
				aggrStatesTyps := make([]LType, 0)
				aggrStatesTyps = append(aggrStatesTyps, run.hAggr._groupedAggrData._aggrReturnTypes...)
				aggrStatesChunk3 = &Chunk{}
				aggrStatesChunk3.init(aggrStatesTyps, defaultVectorSize)

				for i := 0; i < len(run.hAggr._groupedAggrData._aggregates); i++ {
					aggrStatesChunk3._data[i].reference(aggrStatesChunk2._data[run.hAggr._groupedAggrData.GroupCount()+i])
				}
				aggrStatesChunk3.setCard(aggrStatesChunk2.card())
			} else {
				aggrStatesChunk3 = aggrStatesChunk2
			}

			//5. eval the output
			err = run.state.outputExec.executeExprs([]*Chunk{childChunk2, nil, aggrStatesChunk3}, output)
			if err != nil {
				return InvalidOpResult, err
			}
			if filtered == 0 {
				assertFunc(filtered == 0)
				assertFunc(output.card() == childChunk2.card())
				assertFunc(x >= childChunk2.card())
			}
			assertFunc(output.card()+filtered == childChunk.card())
			assertFunc(x == childChunk.card())
			assertFunc(output.card() == childChunk2.card())

			run.state.haScanState._outputCnt += output.card()
			run.state.haScanState._childCnt2 += childChunk.card()
			run.state.haScanState._childCnt3 += x
			if output.card() > 0 {

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
		types := make([]LType, len(run.op.Children[1].Outputs))
		for i, e := range run.op.Children[1].Outputs {
			types[i] = e.DataTyp.LTyp
		}
		//output pos -> [child,pos]
		outputPosMap := make(map[int]ColumnBind)
		for i, output := range run.op.Outputs {
			set := make(ColumnBindSet)
			collectColRefs(output, set)
			assertFunc(!set.empty() && len(set) == 1)
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

func (run *Runner) joinExec(output *Chunk, state *OperatorState) (OperatorResult, error) {
	if run.cross == nil {
		return run.hashJoinExec(output, state)
	} else {
		return run.crossProductExec(output, state)
	}
}

func (run *Runner) hashJoinExec(output *Chunk, state *OperatorState) (OperatorResult, error) {
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
			nextChunk := Chunk{}
			nextChunk.init(run.hjoin._scanNextTyps, defaultVectorSize)
			run.hjoin._scan.Next(run.hjoin._joinKeys, run.hjoin._scan._leftChunk, &nextChunk)
			if nextChunk.card() > 0 {
				err = run.evalJoinOutput(&nextChunk, output)
				if err != nil {
					return 0, err
				}
				return haveMoreOutput, nil
			}
			run.hjoin._scan = nil
		}

		//probe
		leftChunk := &Chunk{}
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

		run.hjoin._joinKeys.reset()
		err = run.hjoin._probExec.executeExprs([]*Chunk{leftChunk, nil, nil}, run.hjoin._joinKeys)
		if err != nil {
			return 0, err
		}
		run.hjoin._scan = run.hjoin._ht.Probe(run.hjoin._joinKeys)
		run.hjoin._scan._leftChunk = leftChunk
		nextChunk := Chunk{}
		nextChunk.init(run.hjoin._scanNextTyps, defaultVectorSize)
		run.hjoin._scan.Next(run.hjoin._joinKeys, run.hjoin._scan._leftChunk, &nextChunk)
		if nextChunk.card() > 0 {
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

func (run *Runner) crossProductExec(output *Chunk, state *OperatorState) (OperatorResult, error) {
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
				run.cross._input = &Chunk{}
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
			rightChunk := &Chunk{}
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

			if rightChunk.card() == 0 {
				continue
			}

			cnt += rightChunk.card()

			//rightChunk.print()
			run.cross.Sink(rightChunk)
		}
		fmt.Println("right count", cnt)
		run.cross._crossStage = CROSS_PROBE
	}

	return Done, nil
}

func (run *Runner) evalJoinOutput(nextChunk, output *Chunk) (err error) {
	leftChunk := Chunk{}
	leftTyps := run.hjoin._scanNextTyps[:len(run.hjoin._leftIndice)]
	leftChunk.init(leftTyps, defaultVectorSize)
	leftChunk.referenceIndice(nextChunk, run.hjoin._leftIndice)

	rightChunk := Chunk{}
	rightChunk.init(run.hjoin._buildTypes, defaultVectorSize)
	rightChunk.referenceIndice(nextChunk, run.hjoin._rightIndice)

	var thisChunk *Chunk
	if run.op.JoinTyp == LOT_JoinTypeMARK || run.op.JoinTyp == LOT_JoinTypeAntiMARK {
		thisChunk = &Chunk{}
		markTyp := []LType{back(run.hjoin._scanNextTyps)}
		thisChunk.init(markTyp, defaultVectorSize)
		thisChunk.referenceIndice(nextChunk, []int{run.hjoin._markIndex})
	}

	err = run.state.outputExec.executeExprs(
		[]*Chunk{
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
			rightChunk := &Chunk{}
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
	projTypes := make([]LType, 0)
	for _, proj := range run.op.Projects {
		projTypes = append(projTypes, proj.DataTyp.LTyp)
	}
	run.state = &OperatorState{
		projTypes:  projTypes,
		projExec:   NewExprExec(run.op.Projects...),
		outputExec: NewExprExec(run.op.Outputs...),
	}
	return nil
}

func (run *Runner) projExec(output *Chunk, state *OperatorState) (OperatorResult, error) {
	childChunk := &Chunk{}
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
	projChunk := &Chunk{}
	projChunk.init(run.state.projTypes, defaultVectorSize)
	err = run.state.projExec.executeExprs([]*Chunk{childChunk, nil, nil}, projChunk)
	if err != nil {
		return 0, err
	}

	err = run.state.outputExec.executeExprs([]*Chunk{childChunk, nil, projChunk}, output)
	if err != nil {
		return 0, err
	}

	return res, nil
}
func (run *Runner) projClose() error {

	return nil
}

func (run *Runner) scanInit() error {
	//read schema
	cat, err := tpchCatalog().Table(run.op.Database, run.op.Table)
	if err != nil {
		return err
	}

	run.colIndice = make([]int, 0)
	for _, col := range run.op.Columns {
		if idx, has := cat.Column2Idx[col]; has {
			run.colIndice = append(run.colIndice, idx)
			run.readedColTyps = append(run.readedColTyps, cat.Types[idx].LTyp)
		} else {
			return fmt.Errorf("no such column %s in %s.%s", col, run.op.Database, run.op.Table)
		}
	}

	//open data file
	switch run.cfg.Tpch1g.Data.Format {
	case "parquet":
		run.pqFile, err = pqLocal.NewLocalFileReader(run.cfg.Tpch1g.Data.Path + "/" + run.op.Table + ".parquet")
		if err != nil {
			return err
		}

		run.pqReader, err = pqReader.NewParquetColumnReader(run.pqFile, 1)
		if err != nil {
			return err
		}
	case "csv":
		run.tablePath = run.cfg.Tpch1g.Data.Path + "/" + run.op.Table + ".tbl"
		run.dataFile, err = os.OpenFile(run.tablePath, os.O_RDONLY, 0755)
		if err != nil {
			return err
		}

		//init csv reader
		run.reader = csv.NewReader(run.dataFile)
		run.reader.Comma = '|'
	default:
		panic("usp format")
	}

	var filterExec *ExprExec
	filterExec, err = initFilterExec(run.op.Filters)
	if err != nil {
		return err
	}

	run.state = &OperatorState{
		filterExec: filterExec,
		filterSel:  NewSelectVector(defaultVectorSize),
		showRaw:    run.cfg.Debug.ShowRaw,
	}

	return nil
}

func (run *Runner) scanExec(output *Chunk, state *OperatorState) (OperatorResult, error) {

	for output.card() == 0 {
		res, err := run.scanRows(output, state, defaultVectorSize)
		if err != nil {
			return InvalidOpResult, err
		}
		if res {
			return Done, nil
		}
	}
	return haveMoreOutput, nil
}

func (run *Runner) scanRows(output *Chunk, state *OperatorState, maxCnt int) (bool, error) {
	if maxCnt == 0 {
		return false, nil
	}
	if run.cfg.Debug.EnableMaxScanRows {
		if run.maxRows > run.cfg.Debug.MaxScanRows {
			return true, nil
		}
	}

	readed := &Chunk{}
	readed.init(run.readedColTyps, maxCnt)
	var err error
	//read table
	switch run.cfg.Tpch1g.Data.Format {
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

	if readed.card() == 0 {
		return true, nil
	}

	if run.cfg.Debug.EnableMaxScanRows {
		run.maxRows += readed.card()
	}

	err = run.runFilterExec(readed, output, true)
	if err != nil {
		return false, err
	}
	return false, nil
}

func (run *Runner) scanClose() error {
	switch run.cfg.Tpch1g.Data.Format {
	case "csv":
		run.reader = nil
		return run.dataFile.Close()
	case "parquet":
		run.pqReader.ReadStop()
		return run.pqFile.Close()
	default:
		panic("usp format")
	}
}
func (run *Runner) readParquetTable(output *Chunk, state *OperatorState, maxCnt int) error {
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

		vec := output._data[j]
		for i := 0; i < len(values); i++ {
			//[row i, col j]
			val, err := parquetColToValue(values[i], vec.typ())
			if err != nil {
				return err
			}
			vec.setValue(i, val)
			if state.showRaw {
				fmt.Print(values[i], " ")
			}
		}
		if state.showRaw {
			fmt.Println()
		}
	}
	output.setCard(rowCont)

	return nil
}

func (run *Runner) readCsvTable(output *Chunk, state *OperatorState, maxCnt int) error {
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
			vec := output._data[j]
			val, err := fieldToValue(field, vec.typ())
			if err != nil {
				return err
			}
			vec.setValue(i, val)
			if state.showRaw {
				fmt.Print(field, " ")
			}
		}
		if state.showRaw {
			fmt.Println()
		}
		rowCont++
	}
	output.setCard(rowCont)

	return nil
}

func fieldToValue(field string, lTyp LType) (*Value, error) {
	var err error
	val := &Value{
		_typ: lTyp,
	}
	switch lTyp.id {
	case LTID_DATE:
		d, err := time.Parse(time.DateOnly, field)
		if err != nil {
			return nil, err
		}
		val._i64 = int64(d.Year())
		val._i64_1 = int64(d.Month())
		val._i64_2 = int64(d.Day())
	case LTID_INTEGER:
		val._i64, err = strconv.ParseInt(field, 10, 64)
		if err != nil {
			return nil, err
		}
	case LTID_VARCHAR:
		val._str = field
	default:
		panic("usp")
	}
	return val, nil
}

func parquetColToValue(field any, lTyp LType) (*Value, error) {
	val := &Value{
		_typ: lTyp,
	}
	switch lTyp.id {
	case LTID_DATE:
		if _, ok := field.(int32); !ok {
			panic("usp")
		}

		d := time.Date(1970, 1, int(1+field.(int32)), 0, 0, 0, 0, time.UTC)
		val._i64 = int64(d.Year())
		val._i64_1 = int64(d.Month())
		val._i64_2 = int64(d.Day())
	case LTID_INTEGER:
		switch fVal := field.(type) {
		case int32:
			val._i64 = int64(fVal)
		case int64:
			val._i64 = fVal
		default:
			panic("usp")
		}
	case LTID_VARCHAR:
		if _, ok := field.(string); !ok {
			panic("usp")
		}

		val._str = field.(string)
	case LTID_DECIMAL:
		p10 := int64(1)
		for i := 0; i < lTyp.scale; i++ {
			p10 *= 10
		}
		switch v := field.(type) {
		case int32:
			val._i64 = int64(v) / p10
			val._i64_1 = int64(v) % p10
		case int64:
			val._i64 = v / p10
			val._i64_1 = int64(v) % p10
		default:
			panic("usp")
		}

	default:
		panic("usp")
	}
	return val, nil
}
