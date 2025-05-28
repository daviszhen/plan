package executor

import (
	"fmt"

	"github.com/daviszhen/plan/pkg/chunk"
	"github.com/daviszhen/plan/pkg/common"
	"github.com/daviszhen/plan/pkg/util"
)

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
