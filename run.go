package main

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
)

type OperatorState struct {
	//for scan
	exec *ExprExec
	sel  *SelectVector
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
	outputTypes   []LType
	outputIndice  []int
	tablePath     string
}

func (run *Runner) Init() error {
	switch run.op.Typ {
	case POT_Scan:
		return run.scanInit()
	default:
		panic("usp")
	}
	return nil
}

func (run *Runner) Execute(input, output *Chunk, state *OperatorState) (OperatorResult, error) {
	switch run.op.Typ {
	case POT_Scan:
		return run.scanExec(output, state)
	default:
		panic("usp")
	}
	return Done, nil
}

func (run *Runner) Close() error {
	switch run.op.Typ {
	case POT_Scan:
		return run.scanClose()
	default:
		panic("usp")
	}
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

	for _, output := range run.op.Outputs {
		run.outputTypes = append(run.outputTypes, output.DataTyp.LTyp)
		run.outputIndice = append(run.outputIndice, int(output.ColRef.column()))
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
		exec: NewExprExec(andFilter),
	}

	return nil
}

func (run *Runner) scanExec(output *Chunk, state *OperatorState) (OperatorResult, error) {
	output.init(run.outputTypes)
	readed := &Chunk{}
	readed.init(run.readedColTyps)
	//read table
	err := run.readTable(readed)
	if err != nil {
		return InvalidOpResult, err
	}

	if readed.card() == 0 {
		return Done, nil
	}

	//filter
	var count int
	count, err = state.exec.executeSelect(readed, state.sel)
	if err != nil {
		return InvalidOpResult, err
	}

	//TODO:remove unnessary row
	if count == readed.card() {
		//reference
		output.referenceIndice(readed, run.outputIndice)
	} else {
		//slice
		output.sliceIndice(readed, state.sel, count, 0, run.outputIndice)
	}
	return haveMoreOutput, nil
}

func (run *Runner) scanClose() error {
	run.reader = nil
	return run.dataFile.Close()
}

func (run *Runner) readTable(output *Chunk) error {
	rowCont := 0
	for i := 0; i < defaultVectorSize; i++ {
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
