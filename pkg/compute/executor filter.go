package compute

import (
	"fmt"

	"github.com/daviszhen/plan/pkg/chunk"
	"github.com/daviszhen/plan/pkg/common"
	"github.com/daviszhen/plan/pkg/util"
)

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
