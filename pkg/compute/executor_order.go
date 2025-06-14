package compute

import (
	"fmt"

	"github.com/daviszhen/plan/pkg/chunk"
	"github.com/daviszhen/plan/pkg/common"
	"github.com/daviszhen/plan/pkg/util"
)

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
