package compute

import (
	"fmt"

	"github.com/daviszhen/plan/pkg/chunk"
	"github.com/daviszhen/plan/pkg/common"
	"github.com/daviszhen/plan/pkg/storage"
	"github.com/daviszhen/plan/pkg/util"
)

type joinExecutor struct {
	op         *PhysicalOperator
	outputExec *ExprExec
	hjoin      *HashJoin
	cross      *CrossProduct
	children   []OperatorExec
}

func newJoinExecutor(op *PhysicalOperator, cfg *util.Config, txn *storage.Txn, children []OperatorExec) (*joinExecutor, error) {
	return &joinExecutor{
		op:       op,
		children: children,
	}, nil
}

func (e *joinExecutor) Init() error {
	e.outputExec = NewExprExec(e.op.Outputs...)

	if len(e.op.getOnConds()) != 0 {
		e.hjoin = NewHashJoin(e.op, e.op.getOnConds())
	} else {
		types := make([]common.LType, len(e.op.Children[1].Outputs))
		for i, e := range e.op.Children[1].Outputs {
			types[i] = e.DataTyp
		}
		outputPosMap := make(map[int]ColumnBind)
		for i, output := range e.op.Outputs {
			set := make(ColumnBindSet)
			collectColRefs(output, set)
			util.AssertFunc(!set.empty() && len(set) == 1)
			for bind := range set {
				outputPosMap[i] = bind
			}
		}
		e.cross = NewCrossProduct(types)
		e.cross._crossExec._outputExec = e.outputExec
		e.cross._crossExec._outputPosMap = outputPosMap
	}

	return nil
}

func (e *joinExecutor) Execute(input, output *chunk.Chunk) (OperatorResult, error) {
	ensureOutputChunk(e.op, output)
	if e.cross == nil {
		return e.hashJoinExec(output)
	}
	return e.crossProductExec(output)
}

func (e *joinExecutor) hashJoinExec(output *chunk.Chunk) (OperatorResult, error) {
	res, err := e.joinBuildHashTable()
	if err != nil {
		return InvalidOpResult, err
	}
	if res == InvalidOpResult {
		return InvalidOpResult, nil
	}

	if e.hjoin._hjs == HJS_BUILD || e.hjoin._hjs == HJS_PROBE {
		if e.hjoin._hjs == HJS_BUILD {
			e.hjoin._hjs = HJS_PROBE
		}

		if e.hjoin._scan != nil {
			nextChunk := chunk.Chunk{}
			nextChunk.Init(e.hjoin._scanNextTyps, util.DefaultVectorSize)
			e.hjoin._scan.Next(e.hjoin._joinKeys, e.hjoin._scan._leftChunk, &nextChunk)
			if nextChunk.Card() > 0 {
				err = e.evalJoinOutput(&nextChunk, output)
				if err != nil {
					return InvalidOpResult, err
				}
				return haveMoreOutput, nil
			}
			e.hjoin._scan = nil
		}

		leftChunk := &chunk.Chunk{}
		res, err = e.children[0].Execute(nil, leftChunk)
		if err != nil {
			return InvalidOpResult, err
		}
		switch res {
		case Done:
			return Done, nil
		case InvalidOpResult:
			return InvalidOpResult, nil
		}

		e.hjoin._joinKeys.Reset()
		err = e.hjoin._probExec.executeExprs([]*chunk.Chunk{leftChunk, nil, nil}, e.hjoin._joinKeys)
		if err != nil {
			return InvalidOpResult, err
		}
		e.hjoin._scan = e.hjoin._ht.Probe(e.hjoin._joinKeys)
		e.hjoin._scan._leftChunk = leftChunk
		nextChunk := chunk.Chunk{}
		nextChunk.Init(e.hjoin._scanNextTyps, util.DefaultVectorSize)
		e.hjoin._scan.Next(e.hjoin._joinKeys, e.hjoin._scan._leftChunk, &nextChunk)
		if nextChunk.Card() > 0 {
			err = e.evalJoinOutput(&nextChunk, output)
			if err != nil {
				return InvalidOpResult, err
			}
			return haveMoreOutput, nil
		}
		e.hjoin._scan = nil
		return haveMoreOutput, nil
	}
	return Done, nil
}

func (e *joinExecutor) crossProductExec(output *chunk.Chunk) (OperatorResult, error) {
	res, err := e.crossBuild()
	if err != nil {
		return InvalidOpResult, err
	}
	if res == InvalidOpResult {
		return InvalidOpResult, nil
	}

	if e.cross._crossStage == CROSS_BUILD || e.cross._crossStage == CROSS_PROBE {
		if e.cross._crossStage == CROSS_BUILD {
			e.cross._crossStage = CROSS_PROBE
		}

		nextInput := false

		for {
			if e.cross._input == nil || nextInput {
				nextInput = false
				e.cross._input = &chunk.Chunk{}
				res, err = e.children[0].Execute(nil, e.cross._input)
				if err != nil {
					return InvalidOpResult, err
				}
				switch res {
				case Done:
					return Done, nil
				case InvalidOpResult:
					return InvalidOpResult, nil
				}
			}

			res, err = e.cross.Execute(e.cross._input, output)
			if err != nil {
				return InvalidOpResult, err
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
	return Done, nil
}

func (e *joinExecutor) crossBuild() (OperatorResult, error) {
	if e.cross._crossStage == CROSS_INIT {
		e.cross._crossStage = CROSS_BUILD
		cnt := 0
		for {
			rightChunk := &chunk.Chunk{}
			res, err := e.children[1].Execute(nil, rightChunk)
			if err != nil {
				return InvalidOpResult, err
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
			e.cross.Sink(rightChunk)
		}
		fmt.Println("right count", cnt)
		e.cross._crossStage = CROSS_PROBE
	}

	return Done, nil
}

func (e *joinExecutor) evalJoinOutput(nextChunk, output *chunk.Chunk) error {
	leftChunk := chunk.Chunk{}
	leftTyps := e.hjoin._scanNextTyps[:len(e.hjoin._leftIndice)]
	leftChunk.Init(leftTyps, util.DefaultVectorSize)
	leftChunk.ReferenceIndice(nextChunk, e.hjoin._leftIndice)

	rightChunk := chunk.Chunk{}
	rightChunk.Init(e.hjoin._buildTypes, util.DefaultVectorSize)
	rightChunk.ReferenceIndice(nextChunk, e.hjoin._rightIndice)

	var thisChunk *chunk.Chunk
	if e.op.getJoinTyp() == LOT_JoinTypeMARK || e.op.getJoinTyp() == LOT_JoinTypeAntiMARK {
		thisChunk = &chunk.Chunk{}
		markTyp := []common.LType{util.Back(e.hjoin._scanNextTyps)}
		thisChunk.Init(markTyp, util.DefaultVectorSize)
		thisChunk.ReferenceIndice(nextChunk, []int{e.hjoin._markIndex})
	}

	return e.outputExec.executeExprs(
		[]*chunk.Chunk{
			&leftChunk,
			&rightChunk,
			thisChunk,
		},
		output,
	)
}

func (e *joinExecutor) joinBuildHashTable() (OperatorResult, error) {
	if e.hjoin._hjs == HJS_INIT {
		e.hjoin._hjs = HJS_BUILD
		for {
			rightChunk := &chunk.Chunk{}
			res, err := e.children[1].Execute(nil, rightChunk)
			if err != nil {
				return InvalidOpResult, err
			}
			if res == InvalidOpResult {
				return InvalidOpResult, nil
			}
			if res == Done {
				e.hjoin._ht.Finalize()
				break
			}

			err = e.hjoin.Build(rightChunk)
			if err != nil {
				return InvalidOpResult, err
			}
		}
		fmt.Println("right hash table count", e.hjoin._ht.count())
		e.hjoin._hjs = HJS_PROBE
	}

	return Done, nil
}

func (e *joinExecutor) Close() error {
	if e.hjoin != nil {
		e.hjoin = nil
	}
	if e.cross != nil {
		e.cross = nil
	}
	return nil
}
