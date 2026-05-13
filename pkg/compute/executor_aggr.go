package compute

import (
	"math"

	"github.com/daviszhen/plan/pkg/chunk"
	"github.com/daviszhen/plan/pkg/common"
	"github.com/daviszhen/plan/pkg/storage"
	"github.com/daviszhen/plan/pkg/util"
)

type aggExecutor struct {
	op                    *PhysicalOperator
	txn                   *storage.Txn
	hAggr                 *HashAggr
	haScanState           *HashAggrScanState
	referChildren         bool
	constGroupby          bool
	ungroupAggr           bool
	ungroupAggrDone       bool
	groupbyWithParamsExec *ExprExec
	groupbyExec           *ExprExec
	filterExec            *ExprExec
	filterSel             *chunk.SelectVector
	outputExec            *ExprExec
	children              []OperatorExec
}

func newAggExecutor(op *PhysicalOperator, cfg *util.Config, txn *storage.Txn, children []OperatorExec) (*aggExecutor, error) {
	return &aggExecutor{
		op:       op,
		txn:      txn,
		children: children,
	}, nil
}

func (e *aggExecutor) Init() error {
	if len(e.op.getGroupBys()) == 0 {
		constExpr := &Expr{
			Typ:        ET_Const,
			DataTyp:    common.IntegerType(),
			ConstValue: NewIntegerConst(1),
		}
		if ai, ok := e.op.Info.(*AggOpInfo); ok {
			ai.GroupBys = append(ai.GroupBys, constExpr)
		}
		e.constGroupby = true
	}

	refChildrenOutput := make([]*Expr, 0)
	for i := 0; i < len(e.op.Children[0].Outputs); i++ {
		ref := e.op.Children[0].Outputs[i]
		refChildrenOutput = append(refChildrenOutput, &Expr{
			Typ:     ET_Column,
			DataTyp: ref.DataTyp,
			BaseInfo: BaseInfo{
				ColRef: ColumnBind{
					math.MaxUint64,
					uint64(i),
				},
			},
		})
	}

	outputTypes := make([]common.LType, 0)
	for _, output := range e.op.Outputs {
		outputTypes = append(outputTypes, output.DataTyp)
	}

	e.hAggr = NewHashAggr(
		outputTypes,
		e.op.getAggs(),
		e.op.getGroupBys(),
		nil,
		nil,
		refChildrenOutput,
	)
	if e.op.Children[0].Typ == POT_Filter {
		e.hAggr._printHash = true
	}

	groupExprs := make([]*Expr, 0)
	groupExprs = append(groupExprs, e.hAggr._groupedAggrData._groups...)
	groupExprs = append(groupExprs, e.hAggr._groupedAggrData._paramExprs...)
	groupExprs = append(groupExprs, e.hAggr._groupedAggrData._refChildrenOutput...)
	e.groupbyWithParamsExec = NewExprExec(groupExprs...)
	e.groupbyExec = NewExprExec(e.hAggr._groupedAggrData._groups...)
	e.filterExec = NewExprExec(e.op.Filters...)
	e.filterSel = chunk.NewSelectVector(util.DefaultVectorSize)
	e.outputExec = NewExprExec(e.op.Outputs...)

	bSet := make(ColumnBindSet)
	collectColRefs2(bSet, e.op.Outputs...)

	for bind := range bSet {
		if int64(bind.table()) < 0 {
			e.referChildren = true
			break
		}
	}
	e.ungroupAggr = !e.referChildren && e.constGroupby

	return nil
}

func (e *aggExecutor) Execute(input, output *chunk.Chunk) (OperatorResult, error) {
	ensureOutputChunk(e.op, output)
	var err error
	var res OperatorResult
	if e.hAggr._has == HAS_INIT {
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

			typs := make([]common.LType, 0)
			typs = append(typs, e.hAggr._groupedAggrData._groupTypes...)
			typs = append(typs, e.hAggr._groupedAggrData._payloadTypes...)
			typs = append(typs, e.hAggr._groupedAggrData._childrenOutputTypes...)
			groupChunk := &chunk.Chunk{}
			groupChunk.Init(typs, util.DefaultVectorSize)
			err = e.groupbyWithParamsExec.executeExprs([]*chunk.Chunk{childChunk, nil, nil}, groupChunk)
			if err != nil {
				return InvalidOpResult, err
			}

			e.hAggr.Sink(groupChunk)
		}
		e.hAggr.Finalize()
		e.hAggr._has = HAS_SCAN
	}
	if e.hAggr._has == HAS_SCAN {
		if e.haScanState == nil {
			e.haScanState = NewHashAggrScanState()
		}

		for {
			if e.ungroupAggr {
				if e.ungroupAggrDone {
					return Done, nil
				}
				e.ungroupAggrDone = true
			}

			groupAddAggrTypes := make([]common.LType, 0)
			groupAddAggrTypes = append(groupAddAggrTypes, e.hAggr._groupedAggrData._groupTypes...)
			groupAddAggrTypes = append(groupAddAggrTypes, e.hAggr._groupedAggrData._aggrReturnTypes...)
			groupAndAggrChunk := &chunk.Chunk{}
			groupAndAggrChunk.Init(groupAddAggrTypes, util.DefaultVectorSize)
			util.AssertFunc(len(e.hAggr._groupedAggrData._groupingFuncs) == 0)
			childChunk := &chunk.Chunk{}
			childChunk.Init(e.hAggr._groupedAggrData._childrenOutputTypes, util.DefaultVectorSize)
			res = e.hAggr.GetData(e.haScanState, groupAndAggrChunk, childChunk)
			if res == InvalidOpResult {
				return InvalidOpResult, nil
			}
			if res == Done {
				break
			}

			x := childChunk.Card()

			filterInputTypes := make([]common.LType, 0)
			filterInputTypes = append(filterInputTypes, e.hAggr._groupedAggrData._aggrReturnTypes...)
			filterInputChunk := &chunk.Chunk{}
			filterInputChunk.Init(filterInputTypes, util.DefaultVectorSize)
			for i := 0; i < len(e.hAggr._groupedAggrData._aggregates); i++ {
				filterInputChunk.Data[i].Reference(groupAndAggrChunk.Data[e.hAggr._groupedAggrData.GroupCount()+i])
			}
			filterInputChunk.SetCard(groupAndAggrChunk.Card())
			var count int
			count, err = e.filterExec.executeSelect([]*chunk.Chunk{childChunk, nil, filterInputChunk}, e.filterSel)
			if err != nil {
				return InvalidOpResult, err
			}

			if count == 0 {
				e.haScanState._filteredCnt1 += childChunk.Card() - count
				continue
			}

			var childChunk2 *chunk.Chunk
			var aggrStatesChunk2 *chunk.Chunk
			var filtered int
			if count == childChunk.Card() {
				childChunk2 = childChunk
				aggrStatesChunk2 = groupAndAggrChunk

				util.AssertFunc(childChunk.Card() == childChunk2.Card())
				util.AssertFunc(groupAndAggrChunk.Card() == aggrStatesChunk2.Card())
				util.AssertFunc(childChunk2.Card() == aggrStatesChunk2.Card())
			} else {
				filtered = childChunk.Card() - count
				e.haScanState._filteredCnt2 += filtered

				childChunkIndice := make([]int, 0)
				for i := 0; i < childChunk.ColumnCount(); i++ {
					childChunkIndice = append(childChunkIndice, i)
				}
				aggrStatesChunkIndice := make([]int, 0)
				for i := 0; i < groupAndAggrChunk.ColumnCount(); i++ {
					aggrStatesChunkIndice = append(aggrStatesChunkIndice, i)
				}
				childChunk2 = &chunk.Chunk{}
				childChunk2.Init(e.hAggr._groupedAggrData._childrenOutputTypes, util.DefaultVectorSize)
				aggrStatesChunk2 = &chunk.Chunk{}
				aggrStatesChunk2.Init(groupAddAggrTypes, util.DefaultVectorSize)

				childChunk2.SliceIndice(childChunk, e.filterSel, count, 0, childChunkIndice)
				aggrStatesChunk2.SliceIndice(groupAndAggrChunk, e.filterSel, count, 0, aggrStatesChunkIndice)

				util.AssertFunc(count == childChunk2.Card())
				util.AssertFunc(count == aggrStatesChunk2.Card())
				util.AssertFunc(childChunk2.Card() == aggrStatesChunk2.Card())
			}

			var aggrStatesChunk3 *chunk.Chunk
			if e.ungroupAggr {
				aggrStatesTyps := make([]common.LType, 0)
				aggrStatesTyps = append(aggrStatesTyps, e.hAggr._groupedAggrData._aggrReturnTypes...)
				aggrStatesChunk3 = &chunk.Chunk{}
				aggrStatesChunk3.Init(aggrStatesTyps, util.DefaultVectorSize)

				for i := 0; i < len(e.hAggr._groupedAggrData._aggregates); i++ {
					aggrStatesChunk3.Data[i].Reference(aggrStatesChunk2.Data[e.hAggr._groupedAggrData.GroupCount()+i])
				}
				aggrStatesChunk3.SetCard(aggrStatesChunk2.Card())
			} else {
				aggrStatesChunk3 = aggrStatesChunk2
			}

			err = e.outputExec.executeExprs([]*chunk.Chunk{childChunk2, nil, aggrStatesChunk3}, output)
			if err != nil {
				return InvalidOpResult, err
			}
			if filtered == 0 {
				util.AssertFunc(filtered == 0)
				util.AssertFunc(output.Card() == childChunk2.Card())
				util.AssertFunc(x >= childChunk2.Card())
			}
			util.AssertFunc(output.Card()+filtered == childChunk.Card())
			util.AssertFunc(x == childChunk.Card())
			util.AssertFunc(output.Card() == childChunk2.Card())

			e.haScanState._outputCnt += output.Card()
			e.haScanState._childCnt2 += childChunk.Card()
			e.haScanState._childCnt3 += x
			if output.Card() > 0 {
				return haveMoreOutput, nil
			}
		}
	}
	return Done, nil
}

func (e *aggExecutor) Close() error {
	if e.hAggr != nil {
		e.hAggr = nil
	}
	return nil
}
