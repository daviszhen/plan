package compute

import (
	"fmt"

	"github.com/daviszhen/plan/pkg/chunk"
	"github.com/daviszhen/plan/pkg/common"
	"github.com/daviszhen/plan/pkg/storage"
	"github.com/daviszhen/plan/pkg/util"
)

type filterExecutor struct {
	op           *PhysicalOperator
	filterExec   *ExprExec
	filterSel    *chunk.SelectVector
	outputIndice []int
	children     []OperatorExec
}

func newFilterExecutor(op *PhysicalOperator, cfg *util.Config, txn *storage.Txn, children []OperatorExec) (*filterExecutor, error) {
	return &filterExecutor{
		op:       op,
		children: children,
	}, nil
}

func (e *filterExecutor) Init() error {
	var err error
	var filterExec *ExprExec
	filterExec, err = initFilterExec(e.op.Filters)
	if err != nil {
		return err
	}

	e.filterExec = filterExec
	e.filterSel = chunk.NewSelectVector(util.DefaultVectorSize)
	for _, output := range e.op.Outputs {
		e.outputIndice = append(e.outputIndice, int(output.ColRef.column()))
	}

	return nil
}

func initFilterExec(filters []*Expr) (*ExprExec, error) {
	var andFilter *Expr
	if len(filters) > 0 {
		andFilter = filters[0]
		for i, filter := range filters {
			if i > 0 {
				if andFilter.DataTyp.Id != common.LTID_BOOLEAN ||
					filter.DataTyp.Id != common.LTID_BOOLEAN {
					return nil, fmt.Errorf("need boolean expr")
				}
				binder := FunctionBinder{}
				andFilter = binder.BindScalarFunc(
					FuncAnd,
					[]*Expr{
						andFilter,
						filter,
					},
					IsOperator(FuncAnd),
				)
			}
		}
	}
	return NewExprExec(andFilter), nil
}

func (e *filterExecutor) runFilterExec(input *chunk.Chunk, output *chunk.Chunk) error {
	count, err := e.filterExec.executeSelect([]*chunk.Chunk{input, nil, nil}, e.filterSel)
	if err != nil {
		return err
	}

	if count == input.Card() {
		output.ReferenceIndice(input, e.outputIndice)
	} else {
		output.SliceIndice(input, e.filterSel, count, 0, e.outputIndice)
	}
	return nil
}

func (e *filterExecutor) Execute(input, output *chunk.Chunk) (OperatorResult, error) {
	ensureOutputChunk(e.op, output)
	childChunk := &chunk.Chunk{}
	var res OperatorResult
	var err error
	if len(e.children) != 0 {
		for {
			res, err = e.children[0].Execute(nil, childChunk)
			if err != nil {
				return InvalidOpResult, err
			}
			if res == InvalidOpResult {
				return InvalidOpResult, nil
			}
			if res == Done {
				return Done, nil
			}
			if childChunk.Card() > 0 {
				break
			}
		}
	}

	err = e.runFilterExec(childChunk, output)
	if err != nil {
		return InvalidOpResult, err
	}
	if output.Card() == 0 {
		return haveMoreOutput, nil
	}
	return haveMoreOutput, nil
}

func (e *filterExecutor) Close() error {
	return nil
}
