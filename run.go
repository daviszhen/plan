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
	//for scan
	exec    *ExprExec
	sel     *SelectVector
	showRaw bool
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

func (run *Runner) Init() error {
	for _, output := range run.op.Outputs {
		run.outputTypes = append(run.outputTypes, output.DataTyp.LTyp)
		run.outputIndice = append(run.outputIndice, int(output.ColRef.column()))
	}
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
	switch run.op.Typ {
	case POT_Scan:
		return run.scanInit()
	case POT_Project:
		return run.projInit()
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
	default:
		panic("usp")
	}
	return Done, nil
}

func (run *Runner) execChild(child *Runner, output *Chunk, state *OperatorState) (OperatorResult, error) {
	for output.card() == 0 {
		res, err := child.Execute(nil, output, child.state)
		if err != nil {
			return InvalidOpResult, err
		}
		switch res {
		case Done:
			break
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
	default:
		panic("usp")
	}
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
