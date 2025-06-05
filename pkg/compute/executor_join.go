package compute

import (
	"fmt"

	"github.com/daviszhen/plan/pkg/chunk"
	"github.com/daviszhen/plan/pkg/common"
	"github.com/daviszhen/plan/pkg/util"
)

func (run *Runner) joinInit() error {
	run.state.outputExec = NewExprExec(run.op.Outputs...)

	if len(run.op.OnConds) != 0 {
		run.state.hjoin = NewHashJoin(run.op, run.op.OnConds)
	} else {
		types := make([]common.LType, len(run.op.Children[1].Outputs))
		for i, e := range run.op.Children[1].Outputs {
			types[i] = e.DataTyp
		}
		//output pos -> [child,pos]
		outputPosMap := make(map[int]ColumnBind)
		for i, output := range run.op.Outputs {
			set := make(ColumnBindSet)
			collectColRefs(output, set)
			util.AssertFunc(!set.empty() && len(set) == 1)
			for bind := range set {
				outputPosMap[i] = bind
			}
		}
		run.state.cross = NewCrossProduct(types)
		run.state.cross._crossExec._outputExec = run.state.outputExec
		run.state.cross._crossExec._outputPosMap = outputPosMap
	}

	return nil
}

func (run *Runner) joinExec(output *chunk.Chunk, state *OperatorState) (OperatorResult, error) {
	if run.state.cross == nil {
		return run.hashJoinExec(output, state)
	} else {
		return run.crossProductExec(output, state)
	}
}

func (run *Runner) hashJoinExec(output *chunk.Chunk, state *OperatorState) (OperatorResult, error) {
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
	if run.state.hjoin._hjs == HJS_BUILD || run.state.hjoin._hjs == HJS_PROBE {
		if run.state.hjoin._hjs == HJS_BUILD {
			run.state.hjoin._hjs = HJS_PROBE
		}

		//continue unfinished can
		if run.state.hjoin._scan != nil {
			nextChunk := chunk.Chunk{}
			nextChunk.Init(run.state.hjoin._scanNextTyps, util.DefaultVectorSize)
			run.state.hjoin._scan.Next(run.state.hjoin._joinKeys, run.state.hjoin._scan._leftChunk, &nextChunk)
			if nextChunk.Card() > 0 {
				err = run.evalJoinOutput(&nextChunk, output)
				if err != nil {
					return 0, err
				}
				return haveMoreOutput, nil
			}
			run.state.hjoin._scan = nil
		}

		//probe
		leftChunk := &chunk.Chunk{}
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

		run.state.hjoin._joinKeys.Reset()
		err = run.state.hjoin._probExec.executeExprs([]*chunk.Chunk{leftChunk, nil, nil}, run.state.hjoin._joinKeys)
		if err != nil {
			return 0, err
		}
		run.state.hjoin._scan = run.state.hjoin._ht.Probe(run.state.hjoin._joinKeys)
		run.state.hjoin._scan._leftChunk = leftChunk
		nextChunk := chunk.Chunk{}
		nextChunk.Init(run.state.hjoin._scanNextTyps, util.DefaultVectorSize)
		run.state.hjoin._scan.Next(run.state.hjoin._joinKeys, run.state.hjoin._scan._leftChunk, &nextChunk)
		if nextChunk.Card() > 0 {
			err = run.evalJoinOutput(&nextChunk, output)
			if err != nil {
				return 0, err
			}
			return haveMoreOutput, nil
		} else {
			run.state.hjoin._scan = nil
		}
		return haveMoreOutput, nil
	}
	return 0, nil
}

func (run *Runner) crossProductExec(output *chunk.Chunk, state *OperatorState) (OperatorResult, error) {
	res, err := run.crossBuild(state)
	if err != nil {
		return InvalidOpResult, err
	}
	if res == InvalidOpResult {
		return InvalidOpResult, nil
	}
	//2. probe stage
	//probe
	if run.state.cross._crossStage == CROSS_BUILD || run.state.cross._crossStage == CROSS_PROBE {
		if run.state.cross._crossStage == CROSS_BUILD {
			run.state.cross._crossStage = CROSS_PROBE
		}

		nextInput := false

		//probe
		for {
			if run.state.cross._input == nil || nextInput {
				nextInput = false
				run.state.cross._input = &chunk.Chunk{}
				res, err = run.execChild(run.children[0], run.state.cross._input, state)
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

			res, err = run.state.cross.Execute(run.state.cross._input, output)
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
	if run.state.cross._crossStage == CROSS_INIT {
		run.state.cross._crossStage = CROSS_BUILD
		cnt := 0
		for {
			rightChunk := &chunk.Chunk{}
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

			if rightChunk.Card() == 0 {
				continue
			}

			cnt += rightChunk.Card()

			//rightChunk.print()
			run.state.cross.Sink(rightChunk)
		}
		fmt.Println("right count", cnt)
		run.state.cross._crossStage = CROSS_PROBE
	}

	return Done, nil
}

func (run *Runner) evalJoinOutput(nextChunk, output *chunk.Chunk) (err error) {
	leftChunk := chunk.Chunk{}
	leftTyps := run.state.hjoin._scanNextTyps[:len(run.state.hjoin._leftIndice)]
	leftChunk.Init(leftTyps, util.DefaultVectorSize)
	leftChunk.ReferenceIndice(nextChunk, run.state.hjoin._leftIndice)

	rightChunk := chunk.Chunk{}
	rightChunk.Init(run.state.hjoin._buildTypes, util.DefaultVectorSize)
	rightChunk.ReferenceIndice(nextChunk, run.state.hjoin._rightIndice)

	var thisChunk *chunk.Chunk
	if run.op.JoinTyp == LOT_JoinTypeMARK || run.op.JoinTyp == LOT_JoinTypeAntiMARK {
		thisChunk = &chunk.Chunk{}
		markTyp := []common.LType{util.Back(run.state.hjoin._scanNextTyps)}
		thisChunk.Init(markTyp, util.DefaultVectorSize)
		thisChunk.ReferenceIndice(nextChunk, []int{run.state.hjoin._markIndex})
	}

	err = run.state.outputExec.executeExprs(
		[]*chunk.Chunk{
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
	if run.state.hjoin._hjs == HJS_INIT {
		run.state.hjoin._hjs = HJS_BUILD
		cnt := 0
		for {
			rightChunk := &chunk.Chunk{}
			res, err = run.execChild(run.children[1], rightChunk, state)
			if err != nil {
				return 0, err
			}
			if res == InvalidOpResult {
				return InvalidOpResult, nil
			}
			if res == Done {
				run.state.hjoin._ht.Finalize()
				break
			}

			//fmt.Println("right child chunk")
			//rightChunk.print()

			cnt++
			err = run.state.hjoin.Build(rightChunk)
			if err != nil {
				return 0, err
			}
		}
		fmt.Println("right hash table count", run.state.hjoin._ht.count())
		run.state.hjoin._hjs = HJS_PROBE
	}

	return Done, nil
}

func (run *Runner) joinClose() error {
	run.state.hjoin = nil
	run.state.cross = nil
	return nil
}
