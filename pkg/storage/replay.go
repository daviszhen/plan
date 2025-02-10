package storage

import (
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/daviszhen/plan/pkg/chunk"
	"github.com/daviszhen/plan/pkg/common"
	"github.com/daviszhen/plan/pkg/util"
)

type ReplayState struct {
	_source          util.Deserialize
	_currentTable    *CatalogEntry
	_deserializeOnly bool
	_checkpointId    BlockID
}

func NewReplayState(src util.Deserialize) *ReplayState {
	return &ReplayState{
		_source:       src,
		_checkpointId: -1,
	}
}

func (state *ReplayState) ReplayEntry(txn *Txn, walTyp uint8) error {
	switch walTyp {
	case WAL_CREATE_TABLE:
		return state.replayCreateTable(txn)
	case WAL_CREATE_SCHEMA:
		return state.replayCreateSchema(txn)
	case WAL_USE_TABLE:
		return state.replayUseTable(txn)
	case WAL_INSERT_TUPLE:
		return state.replayInsert(txn)
	case WAL_DELETE_TUPLE:
		return state.replayDelete(txn)
	case WAL_UPDATE_TUPLE:
		return state.replayUpdate(txn)
	case WAL_CHECKPOINT:
		return state.replayCheckpoint(txn)
	default:
		panic("usp")
	}
	return nil
}

func (state *ReplayState) replayUseTable(txn *Txn) error {
	schema, err := util.ReadString(state._source)
	if err != nil {
		return err
	}
	table, err := util.ReadString(state._source)
	if err != nil {
		return err
	}
	if state._deserializeOnly {
		return nil
	}
	//FIXME: create data table
	state._currentTable = GCatalog.GetEntry(txn, CatalogTypeTable, schema, table)
	return nil
}

func (state *ReplayState) replayInsert(txn *Txn) error {
	data := &chunk.Chunk{}
	err := data.Deserialize(state._source)
	if err != nil {
		return err
	}
	if state._deserializeOnly {
		return nil
	}
	if state._currentTable == nil {
		panic("insert without table")
	}
	fmt.Println("replay insert", txn.String(), "row count", data.Card())
	//data.Print()
	state._currentTable._storage.LocalAppend2(txn, data)
	return nil
}

func (state *ReplayState) replayDelete(txn *Txn) error {
	data := &chunk.Chunk{}
	err := data.Deserialize(state._source)
	if err != nil {
		return err
	}
	if state._deserializeOnly {
		return nil
	}
	if state._currentTable == nil {
		panic("delete without table")
	}

	fmt.Println("replay delete", txn.String(), "row count", data.Card())
	//data.Print()
	vec := chunk.NewConstVector(common.BigintType())

	srcIds := chunk.GetSliceInPhyFormatFlat[RowType](data.Data[0])
	for i := 0; i < data.Card(); i++ {
		rowIds := chunk.GetSliceInPhyFormatConst[RowType](vec)
		rowIds[0] = srcIds[i]
		state._currentTable._storage.Delete(txn, vec, 1)
	}
	return nil
}

func (state *ReplayState) replayUpdate(txn *Txn) error {
	colPath := make([]IdxType, 0)
	cnt := IdxType(0)
	err := util.Read[IdxType](&cnt, state._source)
	if err != nil {
		return err
	}
	for i := 0; i < int(cnt); i++ {
		colId := IdxType(0)
		err = util.Read[IdxType](&colId, state._source)
		if err != nil {
			return err
		}
		colPath = append(colPath, colId)
	}

	data := &chunk.Chunk{}
	err = data.Deserialize(state._source)
	if err != nil {
		return err
	}
	if state._deserializeOnly {
		return nil
	}
	if state._currentTable == nil {
		panic("delete without table")
	}

	fmt.Println("replay update", txn.String(), "row count", data.Card())
	//data.Print()
	if colPath[0] >= IdxType(len(state._currentTable._colDefs)) {
		panic("column index count more than that in table")
	}

	//remove row id vector
	rowIdsVec := util.Back(data.Data)
	data.Data = data.Data[:len(data.Data)-1]

	state._currentTable._storage.UpdateColumn(txn, rowIdsVec, colPath, data)

	return nil
}

func (state *ReplayState) replayCheckpoint(txn *Txn) error {
	id := BlockID(0)
	err := util.Read[BlockID](&id, state._source)
	if err != nil {
		return err
	}
	state._checkpointId = id
	return nil
}

func (state *ReplayState) replayCreateSchema(txn *Txn) error {
	schema, err := util.ReadString(state._source)
	if err != nil {
		return err
	}
	if state._deserializeOnly {
		return nil
	}
	_, err = GCatalog.CreateSchema(txn, schema)
	return err
}

func (state *ReplayState) replayCreateTable(txn *Txn) error {
	tabEnt := &CatalogEntry{}
	err := tabEnt.Deserialize(state._source)
	if err != nil {
		return err
	}
	if state._deserializeOnly {
		return nil
	}
	info := NewDataTableInfo()
	info._schema = tabEnt._schName
	info._table = tabEnt._name
	info._colDefs = tabEnt._colDefs
	info._constraints = tabEnt._constraints
	_, err = GCatalog.CreateTable(
		txn,
		info,
	)
	return err
}

func Replay(path string) (bool, error) {
	fmt.Println("Replay...")
	start := time.Now()
	defer func() {
		fmt.Println("Replay done", time.Since(start))
	}()
	reader, err := NewBufferedFileReader(path)
	if err != nil {
		return false, err
	}
	defer reader.Close()

	id := 0
	txn, err := GTxnMgr.NewTxn(fmt.Sprintf("replay-%d", id))
	if err != nil {
		return false, err
	}

	//1. find checkpoint
	ckpState := NewReplayState(reader)
	ckpState._deserializeOnly = true
	cnt := 0
	for {
		walTyp := uint8(0)
		err = util.Read[uint8](&walTyp, reader)
		//fmt.Println("find ckp stage", cnt, walTyp, walType(walTyp), err)
		cnt++
		if err != nil {
			if errors.Is(err, io.EOF) {
				//done
				err = nil
				break
			}
		}
		if walTyp == WAL_FLUSH {
		} else {
			err = ckpState.ReplayEntry(txn, walTyp)
			if err != nil {
				return false, err
			}
		}
	}
	if ckpState._checkpointId != -1 {
		if GStorageMgr.IsCheckpointClean(ckpState._checkpointId) {
			err = GTxnMgr.Commit(txn)
			if err != nil {
				return false, err
			}
			return true, nil
		}
	}

	reader.Close()

	reader2, err := NewBufferedFileReader(path)
	if err != nil {
		return false, err
	}

	//2. replay wal
	state := NewReplayState(reader2)
	defer reader2.Close()

	for {
		walTyp := uint8(0)
		err = util.Read[uint8](&walTyp, reader2)
		//fmt.Println("replay wal", txn.String(), walTyp, walType(walTyp), err)
		if err != nil {
			if errors.Is(err, io.EOF) {
				//done
				err = nil
				GTxnMgr.Rollback(txn)
				break
			}
			return false, err
		}
		if walTyp == WAL_FLUSH {
			err = GTxnMgr.Commit(txn)
			if err != nil {
				return false, err
			}
			id++
			txn, err = GTxnMgr.NewTxn(fmt.Sprintf("replay-%d", id))
			if err != nil {
				return false, err
			}
		} else {
			//replay
			err = state.ReplayEntry(txn, walTyp)
			if err != nil {
				return false, err
			}
		}
	}
	return false, nil
}
