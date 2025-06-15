package compute

import (
	"fmt"
	"math"

	"github.com/daviszhen/plan/pkg/chunk"
	"github.com/daviszhen/plan/pkg/common"
	"github.com/daviszhen/plan/pkg/util"
)

func (run *Runner) aggrInit() error {
	//if len(run.op.GroupBys) == 0 /*&& groupingSet*/ {
	//	run.hAggr = NewHashAggr(
	//		run.outputTypes,
	//		run.op.Aggs,
	//		nil,
	//		nil,
	//		nil,
	//	)
	//} else
	{

		if len(run.op.GroupBys) == 0 {
			//group by 1
			constExpr := &Expr{
				Typ:        ET_Const,
				DataTyp:    common.IntegerType(),
				ConstValue: NewIntegerConst(1),
			}
			run.op.GroupBys = append(run.op.GroupBys, constExpr)

			run.state.constGroupby = true
		}

		//children input types
		refChildrenOutput := make([]*Expr, 0)
		for i := 0; i < len(run.op.Children[0].Outputs); i++ {
			ref := run.op.Children[0].Outputs[i]
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

		run.state.hAggr = NewHashAggr(
			run.state.outputTypes,
			run.op.Aggs,
			run.op.GroupBys,
			nil,
			nil,
			refChildrenOutput,
		)
		if run.op.Children[0].Typ == POT_Filter {
			run.state.hAggr._printHash = true
		}
		//groupby exprs + param exprs of aggr functions + reference to the output exprs of children
		groupExprs := make([]*Expr, 0)
		groupExprs = append(groupExprs, run.state.hAggr._groupedAggrData._groups...)
		groupExprs = append(groupExprs, run.state.hAggr._groupedAggrData._paramExprs...)
		groupExprs = append(groupExprs, run.state.hAggr._groupedAggrData._refChildrenOutput...)
		run.state.groupbyWithParamsExec = NewExprExec(groupExprs...)
		run.state.groupbyExec = NewExprExec(run.state.hAggr._groupedAggrData._groups...)
		run.state.filterExec = NewExprExec(run.op.Filters...)
		run.state.filterSel = chunk.NewSelectVector(util.DefaultVectorSize)
		run.state.outputExec = NewExprExec(run.op.Outputs...)

		//check output exprs have any colref refers the children node
		bSet := make(ColumnBindSet)
		collectColRefs2(bSet, run.op.Outputs...)

		for bind := range bSet {
			if int64(bind.table()) < 0 {
				run.state.referChildren = true
				break
			}
		}
		run.state.ungroupAggr = !run.state.referChildren && run.state.constGroupby

	}
	return nil
}

func (run *Runner) aggrExec(output *chunk.Chunk, state *OperatorState) (OperatorResult, error) {
	var err error
	var res OperatorResult
	if run.state.hAggr._has == HAS_INIT {
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
			//if run.op.Children[0].Typ == POT_Filter {
			//

			//fmt.Println("child raw chunk")
			//childChunk.print()
			//}

			cnt += childChunk.Card()

			typs := make([]common.LType, 0)
			typs = append(typs, run.state.hAggr._groupedAggrData._groupTypes...)
			typs = append(typs, run.state.hAggr._groupedAggrData._payloadTypes...)
			typs = append(typs, run.state.hAggr._groupedAggrData._childrenOutputTypes...)
			groupChunk := &chunk.Chunk{}
			groupChunk.Init(typs, util.DefaultVectorSize)
			err = run.state.groupbyWithParamsExec.executeExprs([]*chunk.Chunk{childChunk, nil, nil}, groupChunk)
			if err != nil {
				return InvalidOpResult, err
			}

			//groupChunk.print()
			run.state.hAggr.Sink(groupChunk)

		}
		run.state.hAggr.Finalize()
		run.state.hAggr._has = HAS_SCAN
		fmt.Println("get build child cnt", cnt)
		fmt.Println("tuple collection size", run.state.hAggr._groupings[0]._tableData._finalizedHT._dataCollection._count)
	}
	if run.state.hAggr._has == HAS_SCAN {
		if run.state.haScanState == nil {
			run.state.haScanState = NewHashAggrScanState()
			err = run.initChildren()
			if err != nil {
				return InvalidOpResult, err
			}
		}

		for {

			if run.state.ungroupAggr {
				if run.state.ungroupAggrDone {
					return Done, nil
				}
				run.state.ungroupAggrDone = true
			}

			groupAddAggrTypes := make([]common.LType, 0)
			groupAddAggrTypes = append(groupAddAggrTypes, run.state.hAggr._groupedAggrData._groupTypes...)
			groupAddAggrTypes = append(groupAddAggrTypes, run.state.hAggr._groupedAggrData._aggrReturnTypes...)
			groupAndAggrChunk := &chunk.Chunk{}
			groupAndAggrChunk.Init(groupAddAggrTypes, util.DefaultVectorSize)
			util.AssertFunc(len(run.state.hAggr._groupedAggrData._groupingFuncs) == 0)
			childChunk := &chunk.Chunk{}
			childChunk.Init(run.state.hAggr._groupedAggrData._childrenOutputTypes, util.DefaultVectorSize)
			res = run.state.hAggr.GetData(run.state.haScanState, groupAndAggrChunk, childChunk)
			if res == InvalidOpResult {
				return InvalidOpResult, nil
			}
			if res == Done {
				break
			}

			x := childChunk.Card()

			//3.get group by + aggr states for the group

			//4.eval the filter on (child chunk + aggr states)

			//childChunk.print()

			//groupAndAggrChunk.print()

			//aggrStatesChunk.print()
			filterInputTypes := make([]common.LType, 0)
			filterInputTypes = append(filterInputTypes, run.state.hAggr._groupedAggrData._aggrReturnTypes...)
			filterInputChunk := &chunk.Chunk{}
			filterInputChunk.Init(filterInputTypes, util.DefaultVectorSize)
			for i := 0; i < len(run.state.hAggr._groupedAggrData._aggregates); i++ {
				filterInputChunk.Data[i].Reference(groupAndAggrChunk.Data[run.state.hAggr._groupedAggrData.GroupCount()+i])
			}
			filterInputChunk.SetCard(groupAndAggrChunk.Card())
			var count int
			count, err = state.filterExec.executeSelect([]*chunk.Chunk{childChunk, nil, filterInputChunk}, state.filterSel)
			if err != nil {
				return InvalidOpResult, err
			}

			if count == 0 {
				run.state.haScanState._filteredCnt1 += childChunk.Card() - count
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
				run.state.haScanState._filteredCnt2 += filtered

				childChunkIndice := make([]int, 0)
				for i := 0; i < childChunk.ColumnCount(); i++ {
					childChunkIndice = append(childChunkIndice, i)
				}
				aggrStatesChunkIndice := make([]int, 0)
				for i := 0; i < groupAndAggrChunk.ColumnCount(); i++ {
					aggrStatesChunkIndice = append(aggrStatesChunkIndice, i)
				}
				childChunk2 = &chunk.Chunk{}
				childChunk2.Init(run.children[0].state.outputTypes, util.DefaultVectorSize)
				aggrStatesChunk2 = &chunk.Chunk{}
				aggrStatesChunk2.Init(groupAddAggrTypes, util.DefaultVectorSize)

				//slice
				childChunk2.SliceIndice(childChunk, state.filterSel, count, 0, childChunkIndice)
				aggrStatesChunk2.SliceIndice(groupAndAggrChunk, state.filterSel, count, 0, aggrStatesChunkIndice)

				util.AssertFunc(count == childChunk2.Card())
				util.AssertFunc(count == aggrStatesChunk2.Card())
				util.AssertFunc(childChunk2.Card() == aggrStatesChunk2.Card())
			}

			var aggrStatesChunk3 *chunk.Chunk
			if run.state.ungroupAggr {
				//remove const groupby expr
				aggrStatesTyps := make([]common.LType, 0)
				aggrStatesTyps = append(aggrStatesTyps, run.state.hAggr._groupedAggrData._aggrReturnTypes...)
				aggrStatesChunk3 = &chunk.Chunk{}
				aggrStatesChunk3.Init(aggrStatesTyps, util.DefaultVectorSize)

				for i := 0; i < len(run.state.hAggr._groupedAggrData._aggregates); i++ {
					aggrStatesChunk3.Data[i].Reference(aggrStatesChunk2.Data[run.state.hAggr._groupedAggrData.GroupCount()+i])
				}
				aggrStatesChunk3.SetCard(aggrStatesChunk2.Card())
			} else {
				aggrStatesChunk3 = aggrStatesChunk2
			}

			//5. eval the output
			err = run.state.outputExec.executeExprs([]*chunk.Chunk{childChunk2, nil, aggrStatesChunk3}, output)
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

			run.state.haScanState._outputCnt += output.Card()
			run.state.haScanState._childCnt2 += childChunk.Card()
			run.state.haScanState._childCnt3 += x
			if output.Card() > 0 {

				//output.print()
				return haveMoreOutput, nil
			}
		}
	}
	fmt.Println("scan cnt",
		"childCnt",
		run.state.haScanState._childCnt,
		"childCnt2",
		run.state.haScanState._childCnt2,
		"childCnt3",
		run.state.haScanState._childCnt3,
		"outputCnt",
		run.state.haScanState._outputCnt,
		"filteredCnt1",
		run.state.haScanState._filteredCnt1,
		"filteredCnt2",
		run.state.haScanState._filteredCnt2,
	)
	return Done, nil
}

func (run *Runner) aggrClose() error {
	run.state.hAggr = nil
	return nil
}
