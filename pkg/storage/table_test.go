package storage

import (
	"errors"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/daviszhen/plan/pkg/chunk"
	"github.com/daviszhen/plan/pkg/common"
	"github.com/daviszhen/plan/pkg/util"
)

const (
	testVectorSize = 8
)

func Test_table1(t *testing.T) {
	colDefs := []*ColumnDefinition{
		{
			Name: "a",
			Type: common.IntegerType(),
		},
		{
			Name: "b",
			Type: common.IntegerType(),
		},
		{
			Name: "c",
			Type: common.IntegerType(),
		},
	}
	table := NewDataTable("test", "t1", colDefs)
	txnMgr := GTxnMgr
	txn, err := txnMgr.NewTxn("txn1")
	require.NoError(t, err)
	lAState := &LocalAppendState{}
	table.InitLocalAppend(txn, lAState)

	data := &chunk.Chunk{}
	data.Init(table.GetTypes(), testVectorSize)
	for i := 0; i < len(colDefs); i++ {
		data.Data[i] = NewInt32ConstVector(int32(10+i), false)
	}
	data.Count = testVectorSize
	for i := 0; i < 6; i++ {
		table.LocalAppend(txn, lAState, data, false)
	}

	//1. append
	data1 := &chunk.Chunk{}
	data1.Init(table.GetTypes(), testVectorSize/2)
	for i := 0; i < len(colDefs); i++ {
		data1.Data[i] = NewInt32ConstVector(int32(20+i), false)
	}
	data1.Count = testVectorSize / 2
	for i := 0; i < 5; i++ {
		table.LocalAppend(txn, lAState, data1, false)
	}

	table.FinalizeLocalAppend(txn, lAState)

	colTyps := make([]common.LType, 0)
	colTyps = append(colTyps, common.BigintType())
	colTyps = append(colTyps, table.GetTypes()...)

	rowIds := chunk.NewFlatVector(common.BigintType(), testVectorSize)
	rowIdCount := 0
	rowIdSlice := chunk.GetSliceInPhyFormatFlat[RowType](rowIds)

	saveRowids := func(result *chunk.Chunk) {
		if rowIdCount < testVectorSize/2 {
			rowIdVec := result.Data[0]
			for rowIdCount < testVectorSize/2 {
				val := rowIdVec.GetValue(rowIdCount)
				rowIdSlice[rowIdCount] = RowType(val.I64)
				rowIdCount++
			}
		}
	}

	//2. scan
	tCount := readTable(table, txn, testVectorSize, saveRowids)
	fmt.Println("total count: ", tCount)

	//3.delete
	delCnt := table.Delete(txn, rowIds, IdxType(rowIdCount))
	require.Equal(t, IdxType(rowIdCount), delCnt)

	fmt.Println("delete count:", delCnt)
	rowIds.Print(int(delCnt))

	//4.scan again
	rowIdCount = 0

	tCount = readTable(table, txn, testVectorSize, saveRowids)
	fmt.Println("total count 2: ", tCount)

	//5.update
	for i := 0; i < len(colDefs); i++ {
		colids := []IdxType{IdxType(i)}

		updates := &chunk.Chunk{}
		updates.Init([]common.LType{table.GetTypes()[i]}, testVectorSize)
		updates.SetCard(rowIdCount)
		newVal := int32(100 + i)
		for j := 0; j < rowIdCount; j++ {
			fmt.Println("row ", rowIdSlice[j], " col ", i, " update to ", newVal)
		}
		updates.Data[0] = NewInt32ConstVector(newVal, false)
		table.Update(txn, rowIds, colids, updates)

		fmt.Println("after update col", i)
		readTable(table, txn, testVectorSize, nil)
	}

	err = txnMgr.Commit(txn)
	require.NoError(t, err)
}

func Test_table2(t *testing.T) {
	colDefs := []*ColumnDefinition{
		{
			Name: "a",
			Type: common.IntegerType(),
		},
		{
			Name: "b",
			Type: common.IntegerType(),
		},
		{
			Name: "c",
			Type: common.IntegerType(),
		},
	}
	table := NewDataTable("test", "t1", colDefs)
	txnMgr := GTxnMgr

	//txn0
	txn0, err := txnMgr.NewTxn("txn0")
	require.NoError(t, err)
	txn0Do(table, txn0, colDefs)

	tCount := readTable(table, txn0, math.MaxInt32, nil)
	fmt.Println("total count: ", txn0, tCount)

	err = txnMgr.Commit(txn0)
	require.NoError(t, err)

	//txn_1
	txn_1, err := txnMgr.NewTxn("txn_1")
	require.NoError(t, err)

	wg := errgroup.Group{}

	const txnCount = 2
	txns := make([]*Txn, txnCount)

	for i := 0; i < txnCount; i++ {
		txns[i], err = txnMgr.NewTxn(fmt.Sprintf("txn%d", i+1))
		require.NoError(t, err)
	}

	for i := 0; i < txnCount; i++ {
		wg.Go(func() (retErr error) {
			time.Sleep(1)
			defer func() {
				if xre := recover(); xre != nil {
					retErr = util.ConvertPanicError(xre)
				}
			}()
			txn1Do(table, txns[i], int64(i*testVectorSize), (i+1)*100, colDefs)
			return
		})
	}
	err = wg.Wait()
	assert.NoError(t, err)

	fmt.Println("txn1,txn2,..., read after txn1,txn2,...,done but no commit")
	for i := 0; i < txnCount; i++ {
		readTable(table, txns[i], math.MaxInt32, nil)
	}

	fmt.Println("txn_1 read after txn1,txn2,...,done but no commit")
	//txn_1 read
	//expect: as txn0 read
	readTable(table, txn_1, math.MaxInt32, nil)

	//commit all txn
	for i := 0; i < txnCount; i++ {
		err = txnMgr.Commit(txns[i])
		assert.NoError(t, err)
	}

	fmt.Println("txn_1 read after txn1,txn2,..., commit")
	//txn_1 read
	//expect as txn0 read
	readTable(table, txn_1, math.MaxInt32, nil)

	err = txnMgr.Commit(txn_1)
	assert.NoError(t, err)

	txn_after, err := txnMgr.NewTxn("txn_after")
	require.NoError(t, err)

	fmt.Println("txn_after read after all commit")
	//expect as [txn1,txn2,...,] committed
	readTable(table, txn_after, math.MaxInt32, nil)

	err = txnMgr.Commit(txn_after)
	assert.NoError(t, err)
}

func Test_table3(t *testing.T) {
	colDefs := []*ColumnDefinition{
		{
			Name: "a",
			Type: common.IntegerType(),
		},
		{
			Name: "b",
			Type: common.IntegerType(),
		},
		{
			Name: "c",
			Type: common.IntegerType(),
		},
	}
	table := NewDataTable("test", "t1", colDefs)
	txnMgr := GTxnMgr

	//txn0
	txn0, err := txnMgr.NewTxn("txn0")
	require.NoError(t, err)
	txn0Do(table, txn0, colDefs)

	tCount := readTable(table, txn0, math.MaxInt32, nil)
	fmt.Println("total count: ", txn0, tCount)

	err = txnMgr.Commit(txn0)
	require.NoError(t, err)

	//txn_1
	txn_1, err := txnMgr.NewTxn("txn_1")
	require.NoError(t, err)

	wg := errgroup.Group{}

	const txnCount = 2
	txns := make([]*Txn, txnCount)

	for i := 0; i < txnCount; i++ {
		txns[i], err = txnMgr.NewTxn(fmt.Sprintf("txn%d", i+1))
		require.NoError(t, err)
	}

	for i := 0; i < txnCount; i++ {
		wg.Go(func() (retErr error) {
			time.Sleep(1)
			defer func() {
				if xre := recover(); xre != nil {
					retErr = util.ConvertPanicError(xre)
				}
			}()
			txn1Do1(table, txns[i], int64(i*testVectorSize), (i+1)*100, colDefs)
			return
		})
	}
	err = wg.Wait()
	assert.NoError(t, err)

	fmt.Println("txn1,txn2,..., read after txn1,txn2,...,done but no commit")
	for i := 0; i < txnCount; i++ {
		readTable(table, txns[i], math.MaxInt32, nil)
	}

	fmt.Println("txn_1 read after txn1,txn2,...,done but no commit")
	//txn_1 read
	//expect: as txn0 read
	readTable(table, txn_1, math.MaxInt32, nil)

	testCommitError := func() {
		util.Open(util.FAULTS_SCOPE_TXN)
		defer util.Close(util.FAULTS_SCOPE_TXN)
		util.Register(util.FAULTS_SCOPE_TXN,
			"return_err_after_storage_commit",
			nil, func(strings []string) error {
				return errors.New("return err after storage commit")
			})

		//commit error and rollback all txn
		for i := 0; i < txnCount; i++ {
			err = txnMgr.Commit(txns[i])
			assert.Error(t, err)
		}
	}
	testCommitError()

	fmt.Println("txn_1 read after txn1,txn2,..., rollback")
	//txn_1 read
	//expect as txn0 read
	readTable(table, txn_1, math.MaxInt32, nil)

	err = txnMgr.Commit(txn_1)
	assert.NoError(t, err)

	txn_after, err := txnMgr.NewTxn("txn_after")
	require.NoError(t, err)

	fmt.Println("txn_after read after all rollback")
	//expect as [txn1,txn2,...,] committed
	readTable(table, txn_after, math.MaxInt32, nil)

	err = txnMgr.Commit(txn_after)
	assert.NoError(t, err)
}

//func Test_table4(t *testing.T) {
//	var table *DataTable
//	if len(DefDbs) == 0 {
//		colDefs := []*ColumnDefinition{
//			{
//				Name: "a",
//				Type: common.IntegerType(),
//			},
//			{
//				Name: "b",
//				Type: common.IntegerType(),
//			},
//			{
//				Name: "c",
//				Type: common.IntegerType(),
//			},
//		}
//		table = NewDataTable("test", "t1", colDefs)
//		DefDbs = []*Database{
//			{
//				_name: "test",
//				_tables: []*DataTable{
//					table,
//				},
//			},
//		}
//	} else {
//		table = GetTable("test", "t1")
//	}
//	colDefs := table._colDefs
//
//	txnMgr := GTxnMgr
//
//	//txn_2
//	txn_2, err := txnMgr.NewTxn("txn_2")
//	require.NoError(t, err)
//
//	tCount := readTable(table, txn_2, math.MaxInt32, nil)
//	fmt.Println("total count: ", txn_2, tCount)
//
//	err = txnMgr.Commit(txn_2)
//	require.NoError(t, err)
//
//	//txn0
//	txn0, err := txnMgr.NewTxn("txn0")
//	require.NoError(t, err)
//	txn0Do(table, txn0, colDefs)
//
//	tCount = readTable(table, txn0, math.MaxInt32, nil)
//	fmt.Println("total count: ", txn0, tCount)
//
//	err = txnMgr.Commit(txn0)
//	require.NoError(t, err)
//
//	//txn_1
//	txn_1, err := txnMgr.NewTxn("txn_1")
//	require.NoError(t, err)
//
//	wg := errgroup.Group{}
//
//	const txnCount = 2
//	txns := make([]*Txn, txnCount)
//
//	for i := 0; i < txnCount; i++ {
//		txns[i], err = txnMgr.NewTxn(fmt.Sprintf("txn%d", i+1))
//		require.NoError(t, err)
//	}
//
//	for i := 0; i < txnCount; i++ {
//		wg.Go(func() (retErr error) {
//			time.Sleep(1)
//			defer func() {
//				if xre := recover(); xre != nil {
//					retErr = util.ConvertPanicError(xre)
//				}
//			}()
//			txn1Do1(table, txns[i], int64(i*testVectorSize), (i+1)*100, colDefs)
//			return
//		})
//	}
//	err = wg.Wait()
//	require.NoError(t, err)
//
//	fmt.Println("txn1,txn2,..., read after txn1,txn2,...,done but no commit")
//	for i := 0; i < txnCount; i++ {
//		readTable(table, txns[i], math.MaxInt32, nil)
//	}
//
//	fmt.Println("txn_1 read after txn1,txn2,...,done but no commit")
//	//txn_1 read
//	//expect: as txn0 read
//	readTable(table, txn_1, math.MaxInt32, nil)
//
//	//commit all txn
//	for i := 0; i < txnCount; i++ {
//		err = txnMgr.Commit(txns[i])
//		assert.NoError(t, err)
//	}
//
//	fmt.Println("txn_1 read after txn1,txn2,..., commit")
//	//txn_1 read
//	//expect as txn0 read
//	readTable(table, txn_1, math.MaxInt32, nil)
//
//	err = txnMgr.Commit(txn_1)
//	assert.NoError(t, err)
//
//	txn_after, err := txnMgr.NewTxn("txn_after")
//	require.NoError(t, err)
//
//	fmt.Println("txn_after read after all commit")
//	//expect as [txn1,txn2,...,] committed
//	tCount = readTable(table, txn_after, math.MaxInt32, nil)
//	fmt.Println("txn_after count", tCount)
//
//	err = txnMgr.Commit(txn_after)
//	assert.NoError(t, err)
//}

func Test_catalog_database(t *testing.T) {
	txnMgr := GTxnMgr

	//txn_2
	txn_2, err := txnMgr.NewTxn("txn_2")
	require.NoError(t, err)

	catSch := GCatalog.GetSchema(txn_2, "catalog")
	util.AssertFunc(catSch != nil)
	util.AssertFunc(catSch._name == "catalog")

	tables := make([]*CatalogEntry, 3)
	tables[0] = catSch.GetEntry(txn_2, CatalogTypeTable, "database")
	tables[1] = catSch.GetEntry(txn_2, CatalogTypeTable, "tables")
	tables[2] = catSch.GetEntry(txn_2, CatalogTypeTable, "columns")

	util.AssertFunc(len(tables) >= 3)
	util.AssertFunc(tables[0]._name == "database")
	util.AssertFunc(tables[1]._name == "tables")
	util.AssertFunc(tables[2]._name == "columns")

	var table *DataTable
	table = tables[0]._storage
	colDefs := table._colDefs

	table1 := tables[1]._storage
	table2 := tables[2]._storage

	tCount := readTable(table, txn_2, math.MaxInt32, nil)
	fmt.Println(table._info._table, "total count: ", txn_2, tCount)

	tCount = readTable(table1, txn_2, math.MaxInt32, nil)
	fmt.Println(table1._info._table, "total count: ", txn_2, tCount)

	tCount = readTable(table2, txn_2, math.MaxInt32, nil)
	fmt.Println(table2._info._table, "total count: ", txn_2, tCount)

	err = txnMgr.Commit(txn_2)
	require.NoError(t, err)

	//txn0
	txn0, err := txnMgr.NewTxn("txn0")
	require.NoError(t, err)
	err = fillCatalogDatabase(table, txn0, colDefs)
	require.NoError(t, err)

	tCount = readTable(table, txn0, math.MaxInt32, nil)
	fmt.Println("total count: ", txn0, tCount)

	err = txnMgr.Commit(txn0)
	require.NoError(t, err)

	txn_after, err := txnMgr.NewTxn("txn_after")
	require.NoError(t, err)

	fmt.Println("txn_after read after all commit")
	//expect as [txn1,txn2,...,] committed
	tCount = readTable(table, txn_after, math.MaxInt32, nil)
	fmt.Println("txn_after count", tCount)

	err = txnMgr.Commit(txn_after)
	assert.NoError(t, err)
}

func Test_read_tpch1g_tables(t *testing.T) {
	txnMgr := GTxnMgr

	//txn_2
	txn_2, err := txnMgr.NewTxn("txn_2")
	require.NoError(t, err)

	tabEnt := GCatalog.GetEntry(txn_2, CatalogTypeTable, "public", "customer")
	require.NotNil(t, tabEnt)

	var table *DataTable
	table = tabEnt._storage
	//colDefs := table._colDefs

	tCount := readTable(table, txn_2, math.MaxInt32, nil)
	fmt.Println(table._info._table, "total count: ", txn_2, tCount)

	err = txnMgr.Commit(txn_2)
	require.NoError(t, err)

	////txn0
	//txn0, err := txnMgr.NewTxn("txn0")
	//require.NoError(t, err)
	//err = fillCatalogDatabase(table, txn0, colDefs)
	//require.NoError(t, err)
	//
	//tCount = readTable(table, txn0, math.MaxInt32, nil)
	//fmt.Println("total count: ", txn0, tCount)
	//
	//err = txnMgr.Commit(txn0)
	//require.NoError(t, err)
	//
	//txn_after, err := txnMgr.NewTxn("txn_after")
	//require.NoError(t, err)
	//
	//fmt.Println("txn_after read after all commit")
	////expect as [txn1,txn2,...,] committed
	//tCount = readTable(table, txn_after, math.MaxInt32, nil)
	//fmt.Println("txn_after count", tCount)
	//
	//err = txnMgr.Commit(txn_after)
	//assert.NoError(t, err)
}

func Test_read_tpch1g_lineitem(t *testing.T) {
	txnMgr := GTxnMgr

	//txn_2
	txn_2, err := txnMgr.NewTxn("txn_2")
	require.NoError(t, err)

	tabEnt := GCatalog.GetEntry(txn_2, CatalogTypeTable, "public", "lineitem")
	require.NotNil(t, tabEnt)

	var table *DataTable
	table = tabEnt._storage
	//colDefs := table._colDefs

	tCount := ReadTable(table, txn_2, math.MaxInt32, nil)
	fmt.Println(table._info._table, "total count: ", txn_2, tCount)

	err = txnMgr.Commit(txn_2)
	require.NoError(t, err)
}

func txn0Do(table *DataTable, txn *Txn, colDefs []*ColumnDefinition) {
	lAState := &LocalAppendState{}
	table.InitLocalAppend(txn, lAState)

	data := &chunk.Chunk{}
	data.Init(table.GetTypes(), testVectorSize)
	for i := 0; i < len(colDefs); i++ {
		data.Data[i] = NewInt32ConstVector(int32(10+i), false)
	}
	data.Count = testVectorSize
	for i := 0; i < 2; i++ {
		table.LocalAppend(txn, lAState, data, false)
	}

	//1. append
	data1 := &chunk.Chunk{}
	data1.Init(table.GetTypes(), testVectorSize/2)
	for i := 0; i < len(colDefs); i++ {
		data1.Data[i] = NewInt32ConstVector(int32(20+i), false)
	}
	data1.Count = testVectorSize / 2
	for i := 0; i < 2; i++ {
		table.LocalAppend(txn, lAState, data1, false)
	}

	table.FinalizeLocalAppend(txn, lAState)
}

func txn1Do(table *DataTable, txn *Txn, startRowId int64, baseValue int, colDefs []*ColumnDefinition) {
	colTyps := make([]common.LType, 0)
	colTyps = append(colTyps, common.BigintType())
	colTyps = append(colTyps, table.GetTypes()...)

	rowIds := chunk.NewFlatVector(common.BigintType(), testVectorSize)
	rowIdCount := 0
	rowIdSlice := chunk.GetSliceInPhyFormatFlat[RowType](rowIds)

	saveRowids := func(result *chunk.Chunk) {
		if rowIdCount < testVectorSize/2 {
			rowIdVec := result.Data[0]
			for i := 0; i < result.Card() && rowIdCount < testVectorSize/2; i++ {
				val := rowIdVec.GetValue(i)
				if val.I64 < startRowId {
					continue
				}
				rowIdSlice[rowIdCount] = RowType(val.I64)
				rowIdCount++
			}
		}
	}

	util.Info("read table 1: ",
		zap.String("txn", txn.String()))
	//2. scan
	tCount := readTable(table, txn, testVectorSize, saveRowids)
	util.Info("total table 1: ",
		zap.String("txn", txn.String()),
		zap.Int("tCount", tCount),
		zap.Int64("startRowId", startRowId))

	//3.delete
	delCnt := table.Delete(txn, rowIds, IdxType(rowIdCount))
	if IdxType(rowIdCount) != delCnt {
		panic("not equal")
	}

	util.Info("delete count after delete:",
		zap.String("txn", txn.String()),
		zap.Int64("delCnt", int64(delCnt)))
	rowIds.Print2(txn.String()+" del ", int(delCnt))

	//4.scan again
	rowIdCount = 0

	util.Info("read table after delete: ",
		zap.String("txn", txn.String()))

	tCount = readTable(table, txn, testVectorSize, saveRowids)
	util.Info("read table after delete: ",
		zap.String("txn", txn.String()),
		zap.Int("tCount", tCount))

	//5.update
	for i := 0; i < len(colDefs); i++ {
		colids := []IdxType{IdxType(i)}

		updates := &chunk.Chunk{}
		updates.Init([]common.LType{table.GetTypes()[i]}, testVectorSize)
		updates.SetCard(rowIdCount)
		newVal := int32(baseValue + i)
		for j := 0; j < rowIdCount; j++ {
			util.Info("update",
				zap.String("txn", txn.String()),
				zap.Uint64("row ", uint64(rowIdSlice[j])),
				zap.Int(" col ", i),
				zap.Int32(" update to ", newVal))
		}
		updates.Data[0] = NewInt32ConstVector(newVal, false)
		table.Update(txn, rowIds, colids, updates)

		//util.Info("update",
		//	zap.String("txn", txn.String()),
		//	zap.Int("after update col", i))
		//readTable(table, txn, testVectorSize, nil)
	}
}

func txn1Do1(table *DataTable, txn *Txn, startRowId int64, baseValue int, colDefs []*ColumnDefinition) {
	colTyps := make([]common.LType, 0)
	colTyps = append(colTyps, common.BigintType())
	colTyps = append(colTyps, table.GetTypes()...)

	rowIds := chunk.NewFlatVector(common.BigintType(), testVectorSize)
	rowIdCount := 0
	rowIdSlice := chunk.GetSliceInPhyFormatFlat[RowType](rowIds)

	saveRowids := func(result *chunk.Chunk) {
		if rowIdCount < testVectorSize/2 {
			rowIdVec := result.Data[0]
			for i := 0; i < result.Card() && rowIdCount < testVectorSize/2; i++ {
				val := rowIdVec.GetValue(i)
				if val.I64 < startRowId {
					continue
				}
				rowIdSlice[rowIdCount] = RowType(val.I64)
				rowIdCount++
			}
		}
	}

	util.Info("read table 1: ",
		zap.String("txn", txn.String()))
	//2. scan
	tCount := readTable(table, txn, testVectorSize, saveRowids)
	util.Info("total table 1: ",
		zap.String("txn", txn.String()),
		zap.Int("tCount", tCount),
		zap.Int64("startRowId", startRowId))

	//3.delete
	delCnt := table.Delete(txn, rowIds, IdxType(rowIdCount))
	if IdxType(rowIdCount) != delCnt {
		panic("not equal")
	}

	util.Info("delete count after delete:",
		zap.String("txn", txn.String()),
		zap.Int64("delCnt", int64(delCnt)))
	rowIds.Print2(txn.String()+" del ", int(delCnt))

	//4.scan again
	rowIdCount = 0

	util.Info("read table after delete: ",
		zap.String("txn", txn.String()))

	tCount = readTable(table, txn, testVectorSize, saveRowids)
	util.Info("read table after delete: ",
		zap.String("txn", txn.String()),
		zap.Int("tCount", tCount))

	//5.update
	for i := 0; i < len(colDefs); i++ {
		colids := []IdxType{IdxType(i)}

		updates := &chunk.Chunk{}
		updates.Init([]common.LType{table.GetTypes()[i]}, testVectorSize)
		updates.SetCard(rowIdCount)
		newVal := int32(baseValue + i)
		for j := 0; j < rowIdCount; j++ {
			util.Info("update",
				zap.String("txn", txn.String()),
				zap.Uint64("row ", uint64(rowIdSlice[j])),
				zap.Int(" col ", i),
				zap.Int32(" update to ", newVal))
		}
		updates.Data[0] = NewInt32ConstVector(newVal, false)
		table.Update(txn, rowIds, colids, updates)

		//util.Info("update",
		//	zap.String("txn", txn.String()),
		//	zap.Int("after update col", i))
		//readTable(table, txn, testVectorSize, nil)
	}

	lAState := &LocalAppendState{}
	table.InitLocalAppend(txn, lAState)

	//1. append
	data1 := &chunk.Chunk{}
	data1.Init(table.GetTypes(), testVectorSize/2)
	for i := 0; i < len(colDefs); i++ {
		data1.Data[i] = NewInt32ConstVector(int32(baseValue+20+i), false)
	}
	data1.Count = testVectorSize / 2
	for i := 0; i < 1; i++ {
		table.LocalAppend(txn, lAState, data1, false)
	}

	table.FinalizeLocalAppend(txn, lAState)
}

func fillCatalogDatabase(table *DataTable, txn *Txn, colDefs []*ColumnDefinition) error {
	lAState := &LocalAppendState{}
	table.InitLocalAppend(txn, lAState)

	data := &chunk.Chunk{}
	data.Init(table.GetTypes(), testVectorSize)
	for i := 0; i < len(colDefs); i++ {
		switch colDefs[i].Type.Id {
		case common.LTID_UBIGINT:
			values := make([]uint64, testVectorSize)
			for j := 0; j < testVectorSize; j++ {
				values[j] = uint64(10 + i + j)
			}
			data.Data[i] = NewUbigintFlatVector(values, testVectorSize)
		case common.LTID_VARCHAR:
			values := make([]string, testVectorSize)
			if i > 3 {
				buf := make([]byte, STRING_BLOCK_LIMIT)
				for k := IdxType(0); k < STRING_BLOCK_LIMIT; k++ {
					buf[k] = 1
				}
				for j := 0; j < testVectorSize; j++ {
					prefix := fmt.Sprintf("db_%d", 10+i+j)
					copy(buf, prefix)
					values[j] = string(buf)
				}
			} else {
				for j := 0; j < testVectorSize; j++ {
					values[j] = fmt.Sprintf("db_%d", 10+i+j)
				}
			}

			data.Data[i] = NewVarcharFlatVector(values, testVectorSize)
		default:
			panic("not support type")
		}

	}
	data.Count = testVectorSize

	data.Print()
	err := table.LocalAppend(txn, lAState, data, false)
	if err != nil {
		return err
	}
	table.FinalizeLocalAppend(txn, lAState)
	return nil
}

func readTable(table *DataTable, txn *Txn, limit int, callback func(result *chunk.Chunk)) int {
	scanState := NewTableScanState()
	colIdx := make([]IdxType, 1+len(table._colDefs))
	colIdx[0] = COLUMN_IDENTIFIER_ROW_ID
	for i := 0; i < len(table._colDefs); i++ {
		colIdx[i+1] = IdxType(i)
	}
	table.InitScan(txn, scanState, colIdx)
	tCount := 0
	colTyps := make([]common.LType, 0)
	colTyps = append(colTyps, common.BigintType())
	colTyps = append(colTyps, table.GetTypes()...)
	txnInfo := txn.String()
	//util.Info(txnInfo + " enter readTale")
	//defer func() {
	//	util.Info(txnInfo + " exit readTale")
	//}()
	//cnt := 0
	for {
		result := &chunk.Chunk{}
		result.Init(colTyps, testVectorSize)
		//util.Info(txnInfo, zap.Int("read 1", cnt))
		table.Scan(txn, result, scanState)
		//util.Info(txnInfo, zap.Int("read 2", cnt))
		if result.Card() == 0 {
			break
		}
		if tCount < limit {
			result.Print2(txnInfo)
		}
		tCount += result.Card()

		if callback != nil {
			callback(result)
		}
	}
	return tCount
}

func NewUbigintFlatVector(v []uint64, sz int) *chunk.Vector {
	vec := chunk.NewFlatVector(common.UbigintType(), sz)
	data := chunk.GetSliceInPhyFormatFlat[uint64](vec)
	copy(data, v)
	return vec
}

func NewVarcharFlatVector(v []string, sz int) *chunk.Vector {
	vec := chunk.NewFlatVector(common.VarcharType(), sz)
	data := chunk.GetSliceInPhyFormatFlat[common.String](vec)
	for i := 0; i < len(v); i++ {
		dstMem := util.CMalloc(len(v[i]))
		dst := util.PointerToSlice[byte](dstMem, len(v[i]))
		copy(dst, v[i])
		data[i] = common.String{
			Data: dstMem,
			Len:  len(dst),
		}
	}
	return vec
}

func NewInt32ConstVector(v int32, null bool) *chunk.Vector {
	vec := chunk.NewConstVector(common.IntegerType())
	data := chunk.GetSliceInPhyFormatConst[int32](vec)
	data[0] = v
	chunk.SetNullInPhyFormatConst(vec, null)
	return vec
}
