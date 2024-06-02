package main

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"time"
)

type OperatorState struct {
	//for aggregate
	haScanstate           *HashAggrScanState
	groupbyWithParamsExec *ExprExec
	groupbyExec           *ExprExec
	filterExec            *ExprExec
	filterSel             *SelectVector
	aggrOutputExec        *ExprExec

	//for scan
	exec    *ExprExec
	sel     *SelectVector
	showRaw bool

	//for join (inner)
	outputExec *ExprExec
}

type OperatorResult int

const (
	InvalidOpResult OperatorResult = 0
	NeedMoreInput   OperatorResult = 1
	haveMoreOutput  OperatorResult = 2
	Done            OperatorResult = 3
)

var _ OperatorExec = &Runner{}

type OperatorExec interface {
	Init() error
	Execute(input, output *Chunk, state *OperatorState) (OperatorResult, error)
	Close() error
}

type Runner struct {
	op    *PhysicalOperator
	state *OperatorState

	//for hash aggr
	hAggr *HashAggr

	//for hash join
	hjoin    *HashJoin
	joinKeys *Chunk

	//for scan
	dataFile      *os.File
	reader        *csv.Reader
	colIndice     []int
	readedColTyps []LType
	tablePath     string

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
		}
		err := childRun.Init()
		if err != nil {
			return err
		}
		run.children = append(run.children, childRun)
	}
	return nil
}

func (run *Runner) Init() error {
	for _, output := range run.op.Outputs {
		run.outputTypes = append(run.outputTypes, output.DataTyp.LTyp)
		run.outputIndice = append(run.outputIndice, int(output.ColRef.column()))
	}
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
	default:
		panic("usp")
	}
	return nil
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
	default:
		panic("usp")
	}
	return Done, nil
}

func (run *Runner) execChild(child *Runner, output *Chunk, state *OperatorState) (OperatorResult, error) {
	cnt := 0
	for output.card() == 0 {
		res, err := child.Execute(nil, output, child.state)
		if err != nil {
			return InvalidOpResult, err
		}
		//fmt.Println("child result:", res, cnt)
		cnt++
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
	default:
		panic("usp")
	}
	return nil
}

func (run *Runner) aggrInit() error {
	run.state = &OperatorState{}
	if len(run.op.GroupBys) == 0 /*&& groupingSet*/ {
		run.hAggr = NewHashAggr(
			run.outputTypes,
			run.op.Aggs,
			nil,
			nil,
			nil,
		)
	} else {
		run.hAggr = NewHashAggr(
			run.outputTypes,
			run.op.Aggs,
			run.op.GroupBys,
			nil,
			nil,
		)
		//groupby exprs + param exprs of aggr functions
		groupExprs := make([]*Expr, 0)
		groupExprs = append(groupExprs, run.hAggr._groupedAggrData._groups...)
		groupExprs = append(groupExprs, run.hAggr._groupedAggrData._paramExprs...)
		run.state.groupbyWithParamsExec = NewExprExec(groupExprs...)
		run.state.groupbyExec = NewExprExec(run.hAggr._groupedAggrData._groups...)
		run.state.filterExec = NewExprExec(run.op.Filters...)
		run.state.filterSel = NewSelectVector(defaultVectorSize)
		run.state.aggrOutputExec = NewExprExec(run.op.Outputs...)
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
			//fmt.Println("build aggr", cnt, childChunk.card())
			//childChunk.print()
			cnt += childChunk.card()

			typs := make([]LType, 0)
			typs = append(typs, run.hAggr._groupedAggrData._groupTypes...)
			typs = append(typs, run.hAggr._groupedAggrData._payloadTypes...)
			groupChunk := &Chunk{}
			groupChunk.init(typs, defaultVectorSize)
			err = run.state.groupbyWithParamsExec.executeExprs([]*Chunk{childChunk, nil, nil}, groupChunk)
			if err != nil {
				return InvalidOpResult, err
			}
			run.hAggr.Sink(groupChunk)
		}
		run.hAggr._has = HAS_SCAN
		fmt.Println("child cnt", cnt)
	}
	if run.hAggr._has == HAS_SCAN {
		if run.state.haScanstate == nil {
			run.state.haScanstate = NewHashAggrScanState()
			err = run.initChildren()
			if err != nil {
				return InvalidOpResult, err
			}
		}

		for {
			//1.get child chunk from children[0]
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

			//fmt.Println("scan aggr", childChunk.card())
			//childChunk.print()
			x := childChunk.card()
			run.state.haScanstate._childCnt += childChunk.card()

			//2.eval the group by exprs for the child chunk
			typs := make([]LType, 0)
			typs = append(typs, run.hAggr._groupedAggrData._groupTypes...)
			groupChunk := &Chunk{}
			groupChunk.init(typs, defaultVectorSize)
			err = run.state.groupbyExec.executeExprs([]*Chunk{childChunk, nil, nil}, groupChunk)
			if err != nil {
				return InvalidOpResult, err
			}

			//3.get aggr states for the group
			aggrStatesChunk := &Chunk{}
			aggrStatesTyps := make([]LType, 0)
			aggrStatesTyps = append(aggrStatesTyps, run.hAggr._groupedAggrData._aggrReturnTypes...)
			aggrStatesChunk.init(aggrStatesTyps, defaultVectorSize)
			res = run.hAggr.FetechAggregates(run.state.haScanstate, groupChunk, aggrStatesChunk)
			if res == InvalidOpResult {
				return InvalidOpResult, nil
			}
			if res == Done {
				return res, nil
			}

			//4.eval the filter on (child chunk + aggr states)
			//fmt.Println("=============")
			//childChunk.print()
			//aggrStatesChunk.print()
			var count int
			count, err = state.filterExec.executeSelect2([]*Chunk{childChunk, nil, aggrStatesChunk}, state.filterSel)
			if err != nil {
				return InvalidOpResult, err
			}

			if count == 0 {
				run.state.haScanstate._filteredCnt1 += childChunk.card() - count
				continue
			}

			var childChunk2 *Chunk
			var aggrStatesChunk2 *Chunk
			var filtered int
			if count == childChunk.card() {
				childChunk2 = childChunk
				aggrStatesChunk2 = aggrStatesChunk
			} else {
				filtered = count - childChunk.card()
				run.state.haScanstate._filteredCnt2 += filtered

				childChunkIndice := make([]int, 0)
				for i := 0; i < childChunk.columnCount(); i++ {
					childChunkIndice = append(childChunkIndice, i)
				}
				aggrStatesChunkIndice := make([]int, 0)
				for i := 0; i < aggrStatesChunk.columnCount(); i++ {
					aggrStatesChunkIndice = append(aggrStatesChunkIndice, i)
				}
				childChunk2 = &Chunk{}
				childChunk2.init(run.children[0].outputTypes, defaultVectorSize)
				aggrStatesChunk2 = &Chunk{}
				aggrStatesChunk2.init(aggrStatesTyps, defaultVectorSize)

				//slice
				childChunk2.sliceIndice(childChunk, state.filterSel, count, 0, childChunkIndice)
				aggrStatesChunk2.sliceIndice(aggrStatesChunk, state.filterSel, count, 0, aggrStatesChunkIndice)

			}
			assertFunc(childChunk.card() == childChunk2.card())
			assertFunc(aggrStatesChunk.card() == aggrStatesChunk2.card())
			assertFunc(childChunk2.card() == aggrStatesChunk2.card())

			//5. eval the output
			err = run.state.aggrOutputExec.executeExprs([]*Chunk{childChunk2, nil, aggrStatesChunk2}, output)
			if err != nil {
				return InvalidOpResult, err
			}
			if filtered == 0 {
				assertFunc(filtered == 0)
				assertFunc(output.card() == childChunk2.card())
				assertFunc(x >= childChunk2.card())
			}
			assertFunc(output.card()+filtered == childChunk2.card())
			assertFunc(x == childChunk.card())
			assertFunc(output.card() == childChunk.card())

			run.state.haScanstate._outputCnt += output.card()
			run.state.haScanstate._childCnt2 += childChunk.card()
			run.state.haScanstate._childCnt3 += x
			if output.card() > 0 {
				return haveMoreOutput, nil
			}
		}
	}
	fmt.Println("scan cnt",
		"childCnt",
		run.state.haScanstate._childCnt,
		"childCnt2",
		run.state.haScanstate._childCnt2,
		"childCnt3",
		run.state.haScanstate._childCnt3,
		"outputCnt",
		run.state.haScanstate._outputCnt,
		"filteredCnt1",
		run.state.haScanstate._filteredCnt1,
		"filteredCnt2",
		run.state.haScanstate._filteredCnt2,
	)
	return Done, nil
}

func (run *Runner) aggrClose() error {
	run.hAggr = nil
	return nil
}

func (run *Runner) joinInit() error {
	run.hjoin = NewHashJoin(run.op, run.op.OnConds)
	run.state = &OperatorState{
		outputExec: NewExprExec(run.op.Outputs...),
	}
	return nil
}

func (run *Runner) joinExec(output *Chunk, state *OperatorState) (OperatorResult, error) {
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

func (run *Runner) evalJoinOutput(nextChunk, output *Chunk) (err error) {
	leftChunk := Chunk{}
	leftTyps := run.hjoin._scanNextTyps[:len(run.hjoin._leftIndice)]
	leftChunk.init(leftTyps, defaultVectorSize)
	leftChunk.referenceIndice(nextChunk, run.hjoin._leftIndice)

	rightChunk := Chunk{}
	rightChunk.init(run.hjoin._buildTypes, defaultVectorSize)
	rightChunk.referenceIndice(nextChunk, run.hjoin._rightIndice)
	err = run.state.outputExec.executeExprs(
		[]*Chunk{
			&leftChunk,
			&rightChunk,
			nil,
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
			//fmt.Println("build hash table", cnt)
			cnt++
			err = run.hjoin.Build(rightChunk)
			if err != nil {
				return 0, err
			}
		}
		run.hjoin._hjs = HJS_PROBE
	}

	return Done, nil
}

func (run *Runner) joinClose() error {
	run.hjoin = nil
	return nil
}

func (run *Runner) projInit() error {
	run.state = &OperatorState{
		exec: NewExprExec(run.op.Projects...),
		sel:  NewSelectVector(defaultVectorSize),
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
	err = run.state.exec.executeExprs([]*Chunk{childChunk, nil, nil}, output)
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
	run.tablePath = gConf.DataPath + "/" + run.op.Table + ".tbl"
	run.dataFile, err = os.OpenFile(run.tablePath, os.O_RDONLY, 0755)
	if err != nil {
		return err
	}

	//init csv reader
	run.reader = csv.NewReader(run.dataFile)
	run.reader.Comma = '|'

	//init filter
	//TODO: convert filters into "... AND ..."
	var andFilter *Expr
	if len(run.op.Filters) > 0 {
		var impl *Impl
		andFilter = run.op.Filters[0]
		for i, filter := range run.op.Filters {
			if i > 0 {
				if andFilter.DataTyp.LTyp.id != LTID_BOOLEAN ||
					filter.DataTyp.LTyp.id != LTID_BOOLEAN {
					return fmt.Errorf("need boolean expr")
				}
				argsTypes := []ExprDataType{
					andFilter.DataTyp,
					filter.DataTyp,
				}
				impl, err = GetFunctionImpl(
					AND,
					argsTypes)
				if err != nil {
					return err
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

	run.state = &OperatorState{
		exec:    NewExprExec(andFilter),
		sel:     NewSelectVector(defaultVectorSize),
		showRaw: gConf.ShowRaw,
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
	readed := &Chunk{}
	readed.init(run.readedColTyps, maxCnt)
	//read table
	err := run.readTable(readed, state, maxCnt)
	if err != nil {
		return false, err
	}

	if readed.card() == 0 {
		return true, nil
	}

	//filter
	var count int
	count, err = state.exec.executeSelect(readed, state.sel)
	if err != nil {
		return false, err
	}

	//TODO:remove unnessary row
	if count == readed.card() {
		//reference
		output.referenceIndice(readed, run.outputIndice)
	} else {
		//slice
		output.sliceIndice(readed, state.sel, count, 0, run.outputIndice)
	}
	return false, nil
}

func (run *Runner) scanClose() error {
	run.reader = nil
	return run.dataFile.Close()
}

func (run *Runner) readTable(output *Chunk, state *OperatorState, maxCnt int) error {
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
