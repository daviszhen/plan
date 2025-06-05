package compute

import (
	"encoding/csv"
	"fmt"
	"os"
	"time"

	"github.com/daviszhen/plan/pkg/chunk"
	"github.com/daviszhen/plan/pkg/common"
	"github.com/daviszhen/plan/pkg/storage"
	"github.com/daviszhen/plan/pkg/util"
	pqReader "github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/source"
)

// 基础状态
type OprBaseState struct {
	outputTypes  []common.LType
	outputIndice []int
	outputExec   *ExprExec
}

type OprProjectState struct {
	projTypes []common.LType
	projExec  *ExprExec
}

// 扫描状态
type OprScanState struct {
	// 文件相关
	pqFile    source.ParquetFile
	pqReader  *pqReader.ParquetReader
	dataFile  *os.File
	reader    *csv.Reader
	tablePath string

	// 列信息
	colIndice     []int
	readedColTyps []common.LType
	tabEnt        *storage.CatalogEntry

	// 扫描状态
	//for scan table
	tableScanState *storage.TableScanState
	//for scan values list
	colScanState *ColumnDataScanState
	maxRows      int
	showRaw      bool
}

// 排序状态
type OprSortState struct {
	localSort    *LocalSort
	orderKeyExec *ExprExec
	keyTypes     []common.LType
	payloadTypes []common.LType
}

// 聚合状态
type OprAggrState struct {
	// 哈希聚合
	hAggr       *HashAggr
	haScanState *HashAggrScanState

	// 分组相关
	referChildren         bool
	constGroupby          bool
	ungroupAggr           bool
	ungroupAggrDone       bool
	groupbyWithParamsExec *ExprExec
	groupbyExec           *ExprExec
}

// 连接状态
type OprJoinState struct {
	// 交叉连接
	cross *CrossProduct

	// 哈希连接
	hjoin *HashJoin
}

// 限制状态
type OprLimitState struct {
	limit *Limit
}

// 过滤状态
type OprFilterState struct {
	filterExec *ExprExec
	filterSel  *chunk.SelectVector
}

// 插入状态
type OprInsertState struct {
	insertChunk *chunk.Chunk
}

type OprStubState struct {
	deserial   util.Deserialize
	maxRowCnt  int
	rowReadCnt int
}

type OperatorState struct {
	OprBaseState
	OprScanState
	OprProjectState
	OprJoinState
	OprAggrState
	OprFilterState
	OprSortState
	OprLimitState
	OprInsertState
	OprStubState
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
