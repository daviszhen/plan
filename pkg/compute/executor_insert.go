package compute

import (
	"github.com/daviszhen/plan/pkg/chunk"
	"github.com/daviszhen/plan/pkg/storage"
	"github.com/daviszhen/plan/pkg/util"
)

func (run *Runner) insertInit() error {
	run.state.insertChunk = &chunk.Chunk{}
	run.state.insertChunk.Init(run.op.InsertTypes, storage.STANDARD_VECTOR_SIZE)
	return nil
}

func (run *Runner) insertResolveDefaults(
	table *storage.CatalogEntry,
	data *chunk.Chunk,
	columnIndexMap []int,
	result *chunk.Chunk,
) {
	data.Flatten()

	result.Reset()
	result.SetCard(data.Card())

	if len(columnIndexMap) != 0 {
		//columns specified
		for colIdx := range table.GetColumns() {
			mappedIdx := columnIndexMap[colIdx]
			if mappedIdx == -1 {
				panic("usp default value")
			} else {
				util.AssertFunc(mappedIdx < data.ColumnCount())
				util.AssertFunc(result.Data[colIdx].Typ().Id ==
					data.Data[mappedIdx].Typ().Id)
				result.Data[colIdx].Reference(data.Data[mappedIdx])
			}
		}
	} else {
		//no columns specified
		for i := 0; i < result.ColumnCount(); i++ {
			util.AssertFunc(result.Data[i].Typ().Id ==
				data.Data[i].Typ().Id)
			result.Data[i].Reference(data.Data[i])
		}
	}
}

func (run *Runner) insertExec(output *chunk.Chunk, state *OperatorState) (OperatorResult, error) {
	var res OperatorResult
	var err error

	lAState := &storage.LocalAppendState{}
	table := run.op.TableEnt.GetStorage()
	table.InitLocalAppend(run.Txn, lAState)

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

		//fmt.Println("child raw chunk")
		//childChunk.Print()

		cnt += childChunk.Card()

		run.insertResolveDefaults(
			run.op.TableEnt,
			childChunk,
			run.op.ColumnIndexMap,
			run.state.insertChunk)

		err = table.LocalAppend(
			run.Txn,
			lAState,
			run.state.insertChunk,
			false)
		if err != nil {
			return InvalidOpResult, err
		}
	}
	table.FinalizeLocalAppend(run.Txn, lAState)
	return Done, nil
}

func (run *Runner) insertClose() error {
	return nil
}
