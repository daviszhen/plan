package compute

import (
	"fmt"
	"time"

	"github.com/daviszhen/plan/pkg/chunk"
)

// OperatorResult is the result of executing an operator.
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

// OperatorExec is the execution interface for all physical operators.
type OperatorExec interface {
	Init() error
	Execute(input, output *chunk.Chunk) (OperatorResult, error)
	Close() error
}
