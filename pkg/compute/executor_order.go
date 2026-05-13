package compute

import (
	"fmt"

	"github.com/daviszhen/plan/pkg/chunk"
	"github.com/daviszhen/plan/pkg/common"
	"github.com/daviszhen/plan/pkg/storage"
	"github.com/daviszhen/plan/pkg/util"
)

type orderExecutor struct {
	op         *PhysicalOperator
	localSort  *LocalSort
	keyTypes   []common.LType
	payloadTypes []common.LType
	orderKeyExec *ExprExec
	outputExec   *ExprExec
	children     []OperatorExec
}

func newOrderExecutor(op *PhysicalOperator, cfg *util.Config, txn *storage.Txn, children []OperatorExec) (*orderExecutor, error) {
	return &orderExecutor{
		op:       op,
		children: children,
	}, nil
}

func (e *orderExecutor) Init() error {
	keyTypes := make([]common.LType, 0)
	realOrderByExprs := make([]*Expr, 0)
	for _, by := range e.op.getOrderBys() {
		child := by.Children[0]
		keyTypes = append(keyTypes, child.DataTyp)
		realOrderByExprs = append(realOrderByExprs, child)
	}

	payLoadTypes := make([]common.LType, 0)
	for _, output := range e.op.Outputs {
		payLoadTypes = append(payLoadTypes, output.DataTyp)
	}

	e.localSort = NewLocalSort(
		NewSortLayout(e.op.getOrderBys()),
		NewRowLayout(payLoadTypes, nil),
	)

	e.keyTypes = keyTypes
	e.payloadTypes = payLoadTypes
	e.orderKeyExec = NewExprExec(realOrderByExprs...)
	e.outputExec = NewExprExec(e.op.Outputs...)

	return nil
}

func (e *orderExecutor) Execute(input, output *chunk.Chunk) (OperatorResult, error) {
	ensureOutputChunk(e.op, output)
	var err error
	var res OperatorResult
	if e.localSort._sortState == SS_INIT {
		cnt := 0
		for {
			childChunk := &chunk.Chunk{}
			res, err = e.children[0].Execute(nil, childChunk)
			if err != nil {
				return InvalidOpResult, err
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

			key := &chunk.Chunk{}
			key.Init(e.keyTypes, util.DefaultVectorSize)
			err = e.orderKeyExec.executeExprs(
				[]*chunk.Chunk{childChunk, nil, nil},
				key,
			)
			if err != nil {
				return InvalidOpResult, err
			}

			payload := &chunk.Chunk{}
			payload.Init(e.payloadTypes, util.DefaultVectorSize)

			err = e.outputExec.executeExprs(
				[]*chunk.Chunk{childChunk, nil, nil},
				payload,
			)
			if err != nil {
				return InvalidOpResult, err
			}

			util.AssertFunc(key.Card() != 0 && payload.Card() != 0)
			cnt += key.Card()
			util.AssertFunc(key.Card() == payload.Card())

			e.localSort.SinkChunk(key, payload)
		}
		fmt.Println("total count", cnt)
		e.localSort._sortState = SS_SORT
	}

	if e.localSort._sortState == SS_SORT {
		e.localSort.Sort(true)
		e.localSort._sortState = SS_SCAN
	}

	if e.localSort._sortState == SS_SCAN {
		if len(e.localSort._sortedBlocks) == 0 {
			return Done, nil
		}
		if e.localSort._scanner != nil &&
			e.localSort._scanner.Remaining() == 0 {
			e.localSort._scanner = nil
		}

		if e.localSort._scanner == nil {
			e.localSort._scanner = NewPayloadScanner(
				e.localSort._sortedBlocks[0]._payloadData,
				e.localSort,
				true,
			)
		}

		e.localSort._scanner.Scan(output)
	}

	if output.Card() == 0 {
		return Done, nil
	}
	return haveMoreOutput, nil
}

func (e *orderExecutor) Close() error {
	e.localSort = nil
	return nil
}
