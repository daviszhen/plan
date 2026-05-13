package compute

import (
	"fmt"

	"github.com/daviszhen/plan/pkg/chunk"
	"github.com/daviszhen/plan/pkg/storage"
	"github.com/daviszhen/plan/pkg/util"
)

type insertExecutor struct {
	op          *PhysicalOperator
	txn         *storage.Txn
	insertChunk *chunk.Chunk
	children    []OperatorExec
}

func newInsertExecutor(op *PhysicalOperator, cfg *util.Config, txn *storage.Txn, children []OperatorExec) (*insertExecutor, error) {
	return &insertExecutor{
		op:       op,
		txn:      txn,
		children: children,
	}, nil
}

func (e *insertExecutor) Init() error {
	info := e.op.Info.(*InsertOpInfo)
	e.insertChunk = &chunk.Chunk{}
	e.insertChunk.Init(info.ExpectedTypes, storage.STANDARD_VECTOR_SIZE)
	return nil
}

func (e *insertExecutor) insertResolveDefaults(
	table *storage.CatalogEntry,
	data *chunk.Chunk,
	columnIndexMap []int,
	result *chunk.Chunk,
) error {
	data.Flatten()

	result.Reset()
	result.SetCard(data.Card())

	if len(columnIndexMap) != 0 {
		for colIdx := range table.GetColumns() {
			mappedIdx := columnIndexMap[colIdx]
			if mappedIdx == -1 {
				return fmt.Errorf("default value not supported yet")
			} else {
				util.AssertFunc(mappedIdx < data.ColumnCount())
				util.AssertFunc(result.Data[colIdx].Typ().Id ==
					data.Data[mappedIdx].Typ().Id)
				result.Data[colIdx].Reference(data.Data[mappedIdx])
			}
		}
	} else {
		for i := 0; i < result.ColumnCount(); i++ {
			util.AssertFunc(result.Data[i].Typ().Id ==
				data.Data[i].Typ().Id)
			result.Data[i].Reference(data.Data[i])
		}
	}
	return nil
}

func (e *insertExecutor) Execute(input, output *chunk.Chunk) (OperatorResult, error) {
	var res OperatorResult
	var err error
	info := e.op.Info.(*InsertOpInfo)

	lAState := &storage.LocalAppendState{}
	table := info.TableEnt.GetStorage()
	table.InitLocalAppend(e.txn, lAState)

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

		e.insertResolveDefaults(
			info.TableEnt,
			childChunk,
			info.ColumnIndexMap,
			e.insertChunk)

		err = table.LocalAppend(
			e.txn,
			lAState,
			e.insertChunk,
			false)
		if err != nil {
			return InvalidOpResult, err
		}
	}
	table.FinalizeLocalAppend(e.txn, lAState)
	return Done, nil
}

func (e *insertExecutor) Close() error {
	return nil
}
