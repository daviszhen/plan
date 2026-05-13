package compute

import (
	"math"

	"github.com/daviszhen/plan/pkg/chunk"
	"github.com/daviszhen/plan/pkg/common"
	"github.com/daviszhen/plan/pkg/storage"
	"github.com/daviszhen/plan/pkg/util"
)

type limitExecutor struct {
	op         *PhysicalOperator
	limit      *Limit
	outputExec *ExprExec
	children   []OperatorExec
}

func newLimitExecutor(op *PhysicalOperator, cfg *util.Config, txn *storage.Txn, children []OperatorExec) (*limitExecutor, error) {
	return &limitExecutor{
		op:       op,
		children: children,
	}, nil
}

func (e *limitExecutor) Init() error {
	childTypes := make([]common.LType, 0)
	for _, outputExpr := range e.op.Children[0].Outputs {
		childTypes = append(childTypes, outputExpr.DataTyp)
	}

	e.limit = NewLimit(childTypes, e.op.getLimitExpr(), e.op.getOffsetExpr())
	e.outputExec = NewExprExec(e.op.Outputs...)

	return nil
}

func (e *limitExecutor) Execute(input, output *chunk.Chunk) (OperatorResult, error) {
	ensureOutputChunk(e.op, output)
	var err error
	var res OperatorResult
	if e.limit._state == LIMIT_INIT {
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

			ret := e.limit.Sink(childChunk)
			if ret == SinkResDone {
				break
			}
		}
		e.limit._state = LIMIT_SCAN
	}

	if e.limit._state == LIMIT_SCAN {
		for {
			read := &chunk.Chunk{}
			read.Init(e.limit._childTypes, util.DefaultVectorSize)
			getRet := e.limit.GetData(read)
			if getRet == SrcResDone {
				break
			}

			err = e.outputExec.executeExprs([]*chunk.Chunk{read, nil, nil}, output)
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

func (e *limitExecutor) Close() error {
	e.limit = nil
	return nil
}

type LimitState int

const (
	LIMIT_INIT LimitState = iota
	LIMIT_SCAN
)

type Limit struct {
	_state         LimitState
	_limit         uint64
	_offset        uint64
	_childTypes    []common.LType
	_data          *ColumnDataCollection
	_currentOffset uint64
	_reader        *LimitReader
}

type LimitReader struct {
	_currentOffset uint64
	_scanState     *ColumnDataScanState
}

func (limit *Limit) Sink(chunk *chunk.Chunk) SinkResult {
	var maxElement uint64
	if !limit.ComputeOffset(chunk, &maxElement) {
		return SinkResDone
	}

	maxCard := maxElement - limit._currentOffset
	if maxCard < uint64(chunk.Card()) {
		chunk.SetCard(int(maxCard))
	}

	limit._data.Append(chunk)
	limit._currentOffset += uint64(chunk.Card())
	if limit._currentOffset == maxElement {
		return SinkResDone
	}
	return SinkResNeedMoreInput
}

func (limit *Limit) ComputeOffset(
	chunk *chunk.Chunk,
	maxElement *uint64) bool {
	if limit._limit == math.MaxUint64 {
		*maxElement = math.MaxUint64
		return true
	}
	*maxElement = limit._limit + limit._offset
	if limit._limit == 0 ||
		limit._currentOffset >= *maxElement {
		return false
	}
	return true
}

func (limit *Limit) GetData(read *chunk.Chunk) SourceResult {
	if limit._reader == nil {
		limit._reader = &LimitReader{
			_currentOffset: 0,
			_scanState:     &ColumnDataScanState{},
		}
		limit._data.initScan(limit._reader._scanState)
	}
	for limit._reader._currentOffset <
		limit._limit+limit._offset {

		limit._data.Scan(limit._reader._scanState, read)
		if read.Card() == 0 {
			return SrcResDone
		}

		if limit.HandleOffset(read, &limit._reader._currentOffset) {
			break
		}
	}
	if read.Card() > 0 {
		return SrcResHaveMoreOutput
	} else {
		return SrcResDone
	}
}

func (limit *Limit) HandleOffset(
	input *chunk.Chunk,
	currentOffset *uint64,
) bool {
	maxElement := uint64(0)
	if limit._limit == math.MaxUint64 {
		maxElement = limit._limit
	} else {
		maxElement = limit._limit + limit._offset
	}

	inputSize := input.Card()
	if *currentOffset < limit._offset {
		if *currentOffset+uint64(input.Card()) >
			limit._offset {
			startPosition := limit._offset - *currentOffset
			chunkCount := min(limit._limit,
				uint64(input.Card())-startPosition)
			sel := chunk.NewSelectVector(util.DefaultVectorSize)
			for i := 0; i < int(chunkCount); i++ {
				sel.SetIndex(i, int(startPosition)+i)
			}
			input.Slice(input, sel, int(chunkCount), 0)
		} else {
			*currentOffset += uint64(input.Card())
			return false
		}
	} else {
		chunkCount := 0
		if *currentOffset+uint64(input.Card()) >= maxElement {
			chunkCount = int(maxElement - *currentOffset)
		} else {
			chunkCount = input.Card()
		}
		input.Reference(input)
		input.SetCard(chunkCount)
	}
	*currentOffset += uint64(inputSize)
	return true
}

func NewLimit(typs []common.LType, limitExpr, offsetExpr *Expr) *Limit {
	ret := &Limit{
		_childTypes: typs,
	}
	if limitExpr == nil {
		ret._limit = math.MaxUint64
	} else {
		ret._limit = uint64(limitExpr.ConstValue.Integer)
	}
	if offsetExpr == nil {
		ret._offset = 0
	} else {
		ret._offset = uint64(offsetExpr.ConstValue.Integer)
	}
	ret._data = NewColumnDataCollection(typs)
	return ret
}
