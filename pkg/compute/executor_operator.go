package compute

import (
	"fmt"
	"time"

	"github.com/daviszhen/plan/pkg/chunk"
	"github.com/daviszhen/plan/pkg/common"
	"github.com/daviszhen/plan/pkg/storage"
)

type OperatorState struct {
	//order
	orderKeyExec *ExprExec
	keyTypes     []common.LType
	payloadTypes []common.LType

	projTypes  []common.LType
	projExec   *ExprExec
	outputExec *ExprExec

	//filter projExec used in aggr, filter, scan
	filterExec *ExprExec
	filterSel  *chunk.SelectVector

	//for aggregate
	referChildren         bool
	constGroupby          bool
	ungroupAggr           bool
	ungroupAggrDone       bool
	haScanState           *HashAggrScanState
	groupbyWithParamsExec *ExprExec
	groupbyExec           *ExprExec

	//for scan values list
	colScanState *ColumnDataScanState

	//for table scan
	tableScanState *storage.TableScanState

	showRaw bool
}

type OperatorResult int

const (
	InvalidOpResult OperatorResult = 0
	NeedMoreInput   OperatorResult = 1
	haveMoreOutput  OperatorResult = 2
	Done            OperatorResult = 3
)

type SourceResult int

const (
	SrcResHaveMoreOutput SourceResult = iota
	SrcResDone
)

type SinkResult int

const (
	SinkResNeedMoreInput SinkResult = iota
	SinkResDone
)

type ExecStats struct {
	_totalTime      time.Duration
	_totalChildTime time.Duration
}

func (stats ExecStats) String() string {
	if stats._totalTime == 0 {
		return fmt.Sprintf("total time is 0")
	}
	return fmt.Sprintf("time : total %v, this %v (%.2f) , child %v",
		stats._totalTime,
		stats._totalTime-stats._totalChildTime,
		float64(stats._totalTime-stats._totalChildTime)/float64(stats._totalTime),
		stats._totalChildTime,
	)
}

var _ OperatorExec = &Runner{}

type OperatorExec interface {
	Init() error
	Execute(input, output *chunk.Chunk, state *OperatorState) (OperatorResult, error)
	Close() error
}
