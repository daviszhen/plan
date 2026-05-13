package compute

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"time"

	pqLocal "github.com/xitongsys/parquet-go-source/local"
	pqReader "github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/source"

	"github.com/daviszhen/plan/pkg/chunk"
	"github.com/daviszhen/plan/pkg/common"
	"github.com/daviszhen/plan/pkg/storage"
	"github.com/daviszhen/plan/pkg/util"
)

type scanExecutor struct {
	op            *PhysicalOperator
	txn           *storage.Txn
	cfg           *util.Config
	children      []OperatorExec

	// 列信息
	colIndice     []int
	readedColTyps []common.LType
	tabEnt        *storage.CatalogEntry
	outputIndice  []int

	// 文件相关
	pqFile        source.ParquetFile
	pqReader      *pqReader.ParquetReader
	dataFile      *os.File
	reader        *csv.Reader
	tablePath     string

	// 扫描状态
	tableScanState *storage.TableScanState
	colScanState   *ColumnDataScanState
	maxRows        int
	showRaw        bool

	// filter
	filterExec     *ExprExec
	filterSel      *chunk.SelectVector
}

func newScanExecutor(op *PhysicalOperator, cfg *util.Config, txn *storage.Txn, children []OperatorExec) (*scanExecutor, error) {
	return &scanExecutor{
		op:       op,
		cfg:      cfg,
		txn:      txn,
		children: children,
	}, nil
}

func (e *scanExecutor) Init() error {
	var err error
	switch e.op.getScanTyp() {
	case ScanTypeTable:
		tabEnt := storage.GCatalog.GetEntry(e.txn, storage.CatalogTypeTable, e.op.getScanDatabase(), e.op.getScanTable())
		if tabEnt == nil {
			return fmt.Errorf("no table %s in schema %s", e.op.getScanDatabase(), e.op.getScanTable())
		}
		e.tabEnt = tabEnt
		col2Idx := tabEnt.GetColumn2Idx()
		typs := tabEnt.GetTypes()
		e.colIndice = make([]int, 0)
		for _, col := range e.op.getScanColumns() {
			if idx, has := col2Idx[col]; has {
				e.colIndice = append(e.colIndice, idx)
				e.readedColTyps = append(e.readedColTyps, typs[idx])
			} else {
				return fmt.Errorf("no such column %s in %s.%s", col, e.op.getScanDatabase(), e.op.getScanTable())
			}
		}

	case ScanTypeValuesList:
		e.colIndice = make([]int, 0)
		for _, col := range e.op.getScanColumns() {
			if idx, has := e.op.getScanColName2Idx()[col]; has {
				e.colIndice = append(e.colIndice, idx)
				e.readedColTyps = append(e.readedColTyps, e.op.getScanTypes()[idx])
			} else {
				return fmt.Errorf("no such column %s in %s.%s", col, e.op.getScanDatabase(), e.op.getScanTable())
			}
		}
		e.readedColTyps = e.op.getScanTypes()
	case ScanTypeCopyFrom:
		e.colIndice = e.op.getScanConfig().ColumnIds
		e.readedColTyps = e.op.getScanConfig().ReturnedTypes
		switch e.op.getScanConfig().Format {
		case "parquet":
			e.pqFile, err = pqLocal.NewLocalFileReader(e.op.getScanConfig().FilePath)
			if err != nil {
				return err
			}

			e.pqReader, err = pqReader.NewParquetColumnReader(e.pqFile, 1)
			if err != nil {
				return err
			}
		case "csv":
			e.tablePath = e.op.getScanConfig().FilePath
			e.dataFile, err = os.OpenFile(e.tablePath, os.O_RDONLY, 0755)
			if err != nil {
				return err
			}

			comma := ','
			if commaOpt := getFormatFun("delimiter", e.op.getScanConfig().Opts); commaOpt != nil {
				comma = int32(commaOpt.Opt[0])
			}

			e.reader = csv.NewReader(e.dataFile)
			e.reader.Comma = comma
		default:
			return fmt.Errorf("unsupported scan format: %s", e.op.getScanConfig().Format)
		}
	default:
		return fmt.Errorf("unsupported scan type: %v", e.op.getScanTyp())
	}
	var filterExec *ExprExec
	filterExec, err = initFilterExec(e.op.Filters)
	if err != nil {
		return err
	}

	e.filterExec = filterExec
	e.filterSel = chunk.NewSelectVector(util.DefaultVectorSize)
	e.showRaw = e.cfg.Debug.ShowRaw

	for _, output := range e.op.Outputs {
		e.outputIndice = append(e.outputIndice, int(output.ColRef.column()))
	}

	return nil
}

func (e *scanExecutor) Execute(input, output *chunk.Chunk) (OperatorResult, error) {
	ensureOutputChunk(e.op, output)
	for output.Card() == 0 {
		res, err := e.scanRows(output, util.DefaultVectorSize)
		if err != nil {
			return InvalidOpResult, err
		}
		if res {
			return Done, nil
		}
	}
	return haveMoreOutput, nil
}

func (e *scanExecutor) scanRows(output *chunk.Chunk, maxCnt int) (bool, error) {
	if maxCnt == 0 {
		return false, nil
	}
	if e.cfg.Debug.EnableMaxScanRows {
		if e.maxRows > e.cfg.Debug.MaxScanRows {
			return true, nil
		}
	}

	readed := &chunk.Chunk{}
	readed.Init(e.readedColTyps, maxCnt)
	var err error

	switch e.op.getScanTyp() {
	case ScanTypeTable:
		if e.tableScanState == nil {
			e.tableScanState = storage.NewTableScanState()
			colIds := make([]storage.IdxType, 0)
			for _, colId := range e.colIndice {
				colIds = append(colIds, storage.IdxType(colId))
			}
			e.tabEnt.GetStorage().InitScan(
				e.txn,
				e.tableScanState,
				colIds)
		}
		e.tabEnt.GetStorage().Scan(e.txn, readed, e.tableScanState)
	case ScanTypeValuesList:
		err = e.readValues(readed, maxCnt)
		if err != nil {
			return false, err
		}
	case ScanTypeCopyFrom:
		switch e.op.getScanConfig().Format {
		case "parquet":
			err = e.readParquetTable(readed, maxCnt)
			if err != nil {
				return false, err
			}
		case "csv":
			err = e.readCsvTable(readed, maxCnt)
			if err != nil {
				return false, err
			}
		default:
			return false, fmt.Errorf("unsupported scan format: %s", e.op.getScanConfig().Format)
		}
	default:
		return false, fmt.Errorf("unsupported scan type: %v", e.op.getScanTyp())
	}

	if readed.Card() == 0 {
		return true, nil
	}

	if e.cfg.Debug.EnableMaxScanRows {
		e.maxRows += readed.Card()
	}

	err = e.runFilterExec(readed, output)
	if err != nil {
		return false, err
	}
	return false, nil
}

func (e *scanExecutor) runFilterExec(input *chunk.Chunk, output *chunk.Chunk) error {
	if e.filterExec == nil {
		output.ReferenceIndice(input, e.outputIndice)
		return nil
	}
	count, err := e.filterExec.executeSelect([]*chunk.Chunk{nil, nil, input}, e.filterSel)
	if err != nil {
		return err
	}

	if count == input.Card() {
		output.ReferenceIndice(input, e.outputIndice)
	} else {
		output.SliceIndice(input, e.filterSel, count, 0, e.outputIndice)
	}
	return nil
}

func (e *scanExecutor) Close() error {
	switch e.op.getScanTyp() {
	case ScanTypeTable:
		// nothing to close
	case ScanTypeValuesList:
		return nil
	case ScanTypeCopyFrom:
		switch e.op.getScanConfig().Format {
		case "csv":
			e.reader = nil
			if e.dataFile != nil {
				return e.dataFile.Close()
			}
		case "parquet":
			if e.pqReader != nil {
				e.pqReader.ReadStop()
			}
			if e.pqFile != nil {
				return e.pqFile.Close()
			}
		default:
			return fmt.Errorf("unsupported scan format: %s", e.op.getScanConfig().Format)
		}
	default:
		return fmt.Errorf("unsupported scan type: %v", e.op.getScanTyp())
	}
	return nil
}

func (e *scanExecutor) readParquetTable(output *chunk.Chunk, maxCnt int) error {
	rowCont := -1
	var err error
	var values []interface{}

	for j, idx := range e.colIndice {
		values, _, _, err = e.pqReader.ReadColumnByIndex(int64(idx), int64(maxCnt))
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return err
		}

		if rowCont < 0 {
			rowCont = len(values)
		} else if len(values) != rowCont {
			return fmt.Errorf("column %d has different count of values %d with previous columns %d", idx, len(values), rowCont)
		}

		vec := output.Data[j]
		for i := 0; i < len(values); i++ {
			val, err := parquetColToValue(values[i], vec.Typ())
			if err != nil {
				return err
			}
			vec.SetValue(i, val)
			if e.showRaw {
				fmt.Print(values[i], " ")
			}
		}
		if e.showRaw {
			fmt.Println()
		}
	}
	output.SetCard(rowCont)
	return nil
}

func (e *scanExecutor) readCsvTable(output *chunk.Chunk, maxCnt int) error {
	rowCont := 0
	for i := 0; i < maxCnt; i++ {
		line, err := e.reader.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return err
		}
		for j, idx := range e.colIndice {
			if idx >= len(line) {
				return errors.New("no enough fields in the line")
			}
			field := line[idx]
			vec := output.Data[j]
			val, err := fieldToValue(field, vec.Typ())
			if err != nil {
				return err
			}
			vec.SetValue(i, val)
			if e.showRaw {
				fmt.Print(field, " ")
			}
		}
		if e.showRaw {
			fmt.Println()
		}
		rowCont++
	}
	output.SetCard(rowCont)

	return nil
}

func (e *scanExecutor) readValues(output *chunk.Chunk, maxCnt int) error {
	if e.op.collection.Count() == 0 {
		output.SetCap(0)
		return nil
	}

	if e.colScanState == nil {
		e.colScanState = &ColumnDataScanState{}
		e.op.collection.initScan(e.colScanState)
	}

	e.op.collection.Scan(e.colScanState, output)
	if e.showRaw {
		output.Print()
	}
	return nil
}

func fieldToValue(field string, lTyp common.LType) (*chunk.Value, error) {
	var err error
	val := &chunk.Value{
		Typ: lTyp,
	}
	switch lTyp.Id {
	case common.LTID_DATE:
		d, err := time.Parse(time.DateOnly, field)
		if err != nil {
			return nil, err
		}
		val.I64 = int64(d.Year())
		val.I64_1 = int64(d.Month())
		val.I64_2 = int64(d.Day())
	case common.LTID_INTEGER:
		val.I64, err = strconv.ParseInt(field, 10, 64)
		if err != nil {
			return nil, err
		}
	case common.LTID_VARCHAR:
		val.Str = field
	default:
		return nil, fmt.Errorf("unsupported field type: %v", lTyp.Id)
	}
	return val, nil
}

func parquetColToValue(field any, lTyp common.LType) (*chunk.Value, error) {
	val := &chunk.Value{
		Typ: lTyp,
	}
	switch lTyp.Id {
	case common.LTID_DATE:
		if _, ok := field.(int32); !ok {
			return nil, fmt.Errorf("expected int32 for DATE, got %T", field)
		}

		d := time.Date(1970, 1, int(1+field.(int32)), 0, 0, 0, 0, time.UTC)
		val.I64 = int64(d.Year())
		val.I64_1 = int64(d.Month())
		val.I64_2 = int64(d.Day())
	case common.LTID_INTEGER:
		switch fVal := field.(type) {
		case int32:
			val.I64 = int64(fVal)
		case int64:
			val.I64 = fVal
		default:
			return nil, fmt.Errorf("expected int32/int64 for INTEGER, got %T", field)
		}
	case common.LTID_BIGINT:
		switch fVal := field.(type) {
		case int32:
			val.I64 = int64(fVal)
		case int64:
			val.I64 = fVal
		default:
			return nil, fmt.Errorf("expected int32/int64 for BIGINT, got %T", field)
		}
	case common.LTID_VARCHAR:
		if _, ok := field.(string); !ok {
			return nil, fmt.Errorf("expected string for VARCHAR, got %T", field)
		}
		val.Str = field.(string)
	case common.LTID_DECIMAL:
		p10 := int64(1)
		for i := 0; i < lTyp.Scale; i++ {
			p10 *= 10
		}
		switch v := field.(type) {
		case int32:
			val.I64 = int64(v) / p10
			val.I64_1 = int64(v) % p10
		case int64:
			val.I64 = v / p10
			val.I64_1 = v % p10
		default:
			return nil, fmt.Errorf("expected int32/int64 for DECIMAL, got %T", field)
		}
	default:
		return nil, fmt.Errorf("unsupported parquet type: %v", lTyp.Id)
	}
	return val, nil
}
