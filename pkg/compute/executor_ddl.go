package compute

import (
	"errors"
	"fmt"

	"github.com/daviszhen/plan/pkg/chunk"
	"github.com/daviszhen/plan/pkg/storage"
	"github.com/daviszhen/plan/pkg/util"
	pg_query "github.com/pganalyze/pg_query_go/v5"
)

func runDDl(cfg *util.Config, ddl *pg_query.RawStmt) error {
	var root *PhysicalOperator
	var err error
	txn, err := storage.GTxnMgr.NewTxn("runDDL")
	if err != nil {
		return err
	}
	storage.BeginQuery(txn)
	defer func() {
		if err != nil {
			storage.GTxnMgr.Rollback(txn)
		} else {
			err = storage.GTxnMgr.Commit(txn)
		}
	}()

	root, err = genDDLPhyPlan(txn, ddl)
	if err != nil {
		return err
	}
	if root == nil {
		return fmt.Errorf("nil plan")
	}
	return execOps(cfg, txn, nil, nil, []*PhysicalOperator{root})
}

func genDDLPhyPlan(txn *storage.Txn, ddl *pg_query.RawStmt) (*PhysicalOperator, error) {
	builder := NewBuilder(txn)
	lp, err := builder.buildDDL(txn, ddl, builder.rootCtx, 0)
	if err != nil {
		return nil, err
	}
	if lp == nil {
		return nil, errors.New("nil plan")
	}
	pp, err := builder.CreatePhyPlan(lp)
	if err != nil {
		return nil, err
	}
	return pp, nil
}

type createTableExecutor struct {
	op  *PhysicalOperator
	txn *storage.Txn
}

func newCreateTableExecutor(op *PhysicalOperator, cfg *util.Config, txn *storage.Txn, children []OperatorExec) (*createTableExecutor, error) {
	return &createTableExecutor{op: op, txn: txn}, nil
}

func (e *createTableExecutor) Init() error {
	return nil
}

func (e *createTableExecutor) Execute(input, output *chunk.Chunk) (OperatorResult, error) {
	ctInfo := e.op.Info.(*CreateTableOpInfo)
	schema := ctInfo.Database
	if len(schema) == 0 {
		schema = "public"
	}
	table := ctInfo.Table
	ifNotExists := ctInfo.IfNotExists
	tabEnt := storage.GCatalog.GetEntry(e.txn, storage.CatalogTypeTable, schema, table)
	if tabEnt != nil {
		if ifNotExists {
			return Done, nil
		}
		return InvalidOpResult, fmt.Errorf("table %s already exits", table)
	}
	info := storage.NewDataTableInfo3(schema, table, ctInfo.ColDefs, ctInfo.Constraints)
	_, err := storage.GCatalog.CreateTable(e.txn, info)
	if err != nil {
		return InvalidOpResult, err
	}
	return Done, nil
}

func (e *createTableExecutor) Close() error {
	return nil
}

type createSchemaExecutor struct {
	op  *PhysicalOperator
	txn *storage.Txn
}

func newCreateSchemaExecutor(op *PhysicalOperator, cfg *util.Config, txn *storage.Txn, children []OperatorExec) (*createSchemaExecutor, error) {
	return &createSchemaExecutor{op: op, txn: txn}, nil
}

func (e *createSchemaExecutor) Init() error {
	return nil
}

func (e *createSchemaExecutor) Execute(input, output *chunk.Chunk) (OperatorResult, error) {
	csInfo := e.op.Info.(*CreateSchemaOpInfo)
	name := csInfo.Database
	ifNotExists := csInfo.IfNotExists
	schEnt := storage.GCatalog.GetSchema(e.txn, name)
	if schEnt != nil {
		if ifNotExists {
			return Done, nil
		}
		return InvalidOpResult, fmt.Errorf("schema %s already exists", name)
	}
	_, err := storage.GCatalog.CreateSchema(e.txn, name)
	if err != nil {
		return InvalidOpResult, err
	}
	return Done, nil
}

func (e *createSchemaExecutor) Close() error {
	return nil
}
