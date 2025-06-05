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

	"github.com/daviszhen/plan/pkg/chunk"
	"github.com/daviszhen/plan/pkg/common"
	"github.com/daviszhen/plan/pkg/storage"
	"github.com/daviszhen/plan/pkg/util"
)

func (run *Runner) scanInit() error {
	var err error
	switch run.op.ScanTyp {
	case ScanTypeTable:

		{
			tabEnt := storage.GCatalog.GetEntry(run.Txn, storage.CatalogTypeTable, run.op.Database, run.op.Table)
			if tabEnt == nil {
				return fmt.Errorf("no table %s in schema %s", run.op.Database, run.op.Table)
			}
			run.state.tabEnt = tabEnt
			col2Idx := tabEnt.GetColumn2Idx()
			typs := tabEnt.GetTypes()
			run.state.colIndice = make([]int, 0)
			for _, col := range run.op.Columns {
				if idx, has := col2Idx[col]; has {
					run.state.colIndice = append(run.state.colIndice, idx)
					run.state.readedColTyps = append(run.state.readedColTyps, typs[idx])
				} else {
					return fmt.Errorf("no such column %s in %s.%s", col, run.op.Database, run.op.Table)
				}
			}
		}

	case ScanTypeValuesList:
		run.state.colIndice = make([]int, 0)
		for _, col := range run.op.Columns {
			if idx, has := run.op.ColName2Idx[col]; has {
				run.state.colIndice = append(run.state.colIndice, idx)
				run.state.readedColTyps = append(run.state.readedColTyps, run.op.Types[idx])
			} else {
				return fmt.Errorf("no such column %s in %s.%s", col, run.op.Database, run.op.Table)
			}
		}
		run.state.readedColTyps = run.op.Types
	case ScanTypeCopyFrom:
		run.state.colIndice = run.op.ScanInfo.ColumnIds
		run.state.readedColTyps = run.op.ScanInfo.ReturnedTypes
		//open data file
		switch run.op.ScanInfo.Format {
		case "parquet":
			run.state.pqFile, err = pqLocal.NewLocalFileReader(run.op.ScanInfo.FilePath)
			if err != nil {
				return err
			}

			run.state.pqReader, err = pqReader.NewParquetColumnReader(run.state.pqFile, 1)
			if err != nil {
				return err
			}
		case "csv":
			run.state.tablePath = run.op.ScanInfo.FilePath
			run.state.dataFile, err = os.OpenFile(run.state.tablePath, os.O_RDONLY, 0755)
			if err != nil {
				return err
			}

			comma := ','
			if commaOpt := getFormatFun("delimiter", run.op.ScanInfo.Opts); commaOpt != nil {
				comma = int32(commaOpt.Opt[0])
			}

			//init csv reader
			run.state.reader = csv.NewReader(run.state.dataFile)
			run.state.reader.Comma = comma
		default:
			panic("usp format")
		}
	default:
		panic("usp")
	}
	var filterExec *ExprExec
	filterExec, err = initFilterExec(run.op.Filters)
	if err != nil {
		return err
	}

	run.state.filterExec = filterExec
	run.state.filterSel = chunk.NewSelectVector(util.DefaultVectorSize)
	run.state.showRaw = run.cfg.Debug.ShowRaw

	return nil
}

func (run *Runner) scanExec(output *chunk.Chunk, state *OperatorState) (OperatorResult, error) {

	for output.Card() == 0 {
		res, err := run.scanRows(output, state, util.DefaultVectorSize)
		if err != nil {
			return InvalidOpResult, err
		}
		if res {
			return Done, nil
		}
	}
	return haveMoreOutput, nil
}

func (run *Runner) scanRows(output *chunk.Chunk, state *OperatorState, maxCnt int) (bool, error) {
	if maxCnt == 0 {
		return false, nil
	}
	if run.cfg.Debug.EnableMaxScanRows {
		if run.state.maxRows > run.cfg.Debug.MaxScanRows {
			return true, nil
		}
	}

	readed := &chunk.Chunk{}
	readed.Init(run.state.readedColTyps, maxCnt)
	var err error

	switch run.op.ScanTyp {
	case ScanTypeTable:
		{
			if run.state.tableScanState == nil {
				run.state.tableScanState = storage.NewTableScanState()
				colIds := make([]storage.IdxType, 0)
				for _, colId := range run.state.colIndice {
					colIds = append(colIds, storage.IdxType(colId))
				}
				run.state.tabEnt.GetStorage().InitScan(
					run.Txn,
					run.state.tableScanState,
					colIds)
			}
			run.state.tabEnt.GetStorage().Scan(run.Txn, readed, run.state.tableScanState)
		}
		{
			//read table
			//switch run.cfg.Tpch1g.Data.Format {
			//case "parquet":
			//	err = run.readParquetTable(readed, state, maxCnt)
			//	if err != nil {
			//		return false, err
			//	}
			//case "csv":
			//	err = run.readCsvTable(readed, state, maxCnt)
			//	if err != nil {
			//		return false, err
			//	}
			//default:
			//	panic("usp format")
			//}
		}
	case ScanTypeValuesList:
		err = run.readValues(readed, state, maxCnt)
		if err != nil {
			return false, err
		}
	case ScanTypeCopyFrom:
		//read table
		switch run.op.ScanInfo.Format {
		case "parquet":
			err = run.readParquetTable(readed, state, maxCnt)
			if err != nil {
				return false, err
			}
		case "csv":
			err = run.readCsvTable(readed, state, maxCnt)
			if err != nil {
				return false, err
			}
		default:
			panic("usp format")
		}
	default:
		panic("usp")
	}

	if readed.Card() == 0 {
		return true, nil
	}

	if run.cfg.Debug.EnableMaxScanRows {
		run.state.maxRows += readed.Card()
	}

	err = run.runFilterExec(readed, output, true)
	if err != nil {
		return false, err
	}
	return false, nil
}

func (run *Runner) scanClose() error {
	switch run.op.ScanTyp {
	case ScanTypeTable:
		{

		}
		{
			//switch run.cfg.Tpch1g.Data.Format {
			//case "csv":
			//	run.reader = nil
			//	return run.dataFile.Close()
			//case "parquet":
			//	run.pqReader.ReadStop()
			//	return run.pqFile.Close()
			//default:
			//	panic("usp format")
			//}
		}

	case ScanTypeValuesList:
		return nil
	case ScanTypeCopyFrom:
		switch run.op.ScanInfo.Format {
		case "csv":
			run.state.reader = nil
			return run.state.dataFile.Close()
		case "parquet":
			run.state.pqReader.ReadStop()
			return run.state.pqFile.Close()
		default:
			panic("usp format")
		}
	default:
		panic("usp")
	}
	return nil
}
func (run *Runner) readParquetTable(output *chunk.Chunk, state *OperatorState, maxCnt int) error {
	rowCont := -1
	var err error
	var values []interface{}

	//fill field into vector
	for j, idx := range run.state.colIndice {
		values, _, _, err = run.state.pqReader.ReadColumnByIndex(int64(idx), int64(maxCnt))
		if err != nil {
			//EOF
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
			//[row i, col j]
			val, err := parquetColToValue(values[i], vec.Typ())
			if err != nil {
				return err
			}
			vec.SetValue(i, val)
			if state.showRaw {
				fmt.Print(values[i], " ")
			}
		}
		if state.showRaw {
			fmt.Println()
		}
	}
	output.SetCard(rowCont)
	return nil
}

func (run *Runner) readCsvTable(output *chunk.Chunk, state *OperatorState, maxCnt int) error {
	rowCont := 0
	for i := 0; i < maxCnt; i++ {
		//read line
		line, err := run.state.reader.Read()
		if err != nil {
			//EOF
			if errors.Is(err, io.EOF) {
				break
			}
			return err
		}
		//fill field into vector
		for j, idx := range run.state.colIndice {
			if idx >= len(line) {
				return errors.New("no enough fields in the line")
			}
			field := line[idx]
			//[row i, col j] = field
			vec := output.Data[j]
			val, err := fieldToValue(field, vec.Typ())
			if err != nil {
				return err
			}
			vec.SetValue(i, val)
			if state.showRaw {
				fmt.Print(field, " ")
			}
		}
		if state.showRaw {
			fmt.Println()
		}
		rowCont++
	}
	output.SetCard(rowCont)

	return nil
}

func (run *Runner) readValues(output *chunk.Chunk, state *OperatorState, maxCnt int) error {
	if run.op.collection.Count() == 0 {
		output.SetCap(0)
		return nil
	}

	if run.state.colScanState == nil {
		run.state.colScanState = &ColumnDataScanState{}
		run.op.collection.initScan(run.state.colScanState)
	}

	run.op.collection.Scan(run.state.colScanState, output)
	if state.showRaw {
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
		panic("usp")
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
			panic("usp")
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
			panic("usp")
		}
	case common.LTID_BIGINT:
		switch fVal := field.(type) {
		case int32:
			val.I64 = int64(fVal)
		case int64:
			val.I64 = fVal
		default:
			panic("usp")
		}
	case common.LTID_VARCHAR:
		if _, ok := field.(string); !ok {
			panic("usp")
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
			val.I64_1 = int64(v) % p10
		default:
			panic("usp")
		}

	default:
		panic("usp")
	}
	return val, nil
}
