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

func (run *Runner) createTableInit() error {
	return nil
}

func (run *Runner) createTableExec(output *chunk.Chunk, state *OperatorState) (OperatorResult, error) {
	//step 1 : check schema
	schema := run.op.Database
	if len(schema) == 0 {
		schema = "public"
	}
	table := run.op.Table
	ifNotExists := run.op.IfNotExists
	//////////////////////////////////////
	tabEnt := storage.GCatalog.GetEntry(run.Txn, storage.CatalogTypeTable, schema, table)
	if tabEnt != nil {
		if ifNotExists {
			return Done, nil
		} else {
			return InvalidOpResult, fmt.Errorf("table %s already exits", table)
		}
	}
	info := storage.NewDataTableInfo3(schema, table, run.op.ColDefs, run.op.Constraints)
	_, err := storage.GCatalog.CreateTable(run.Txn, info)
	if err != nil {
		return 0, err
	}
	return Done, nil
}

func (run *Runner) createTableClose() error {
	return nil
}

func (run *Runner) createSchemaInit() error {
	return nil
}

func (run *Runner) createSchemaExec(output *chunk.Chunk, state *OperatorState) (OperatorResult, error) {
	name := run.op.Database
	ifNotExists := run.op.IfNotExists
	schEnt := storage.GCatalog.GetSchema(run.Txn, name)
	if schEnt != nil {
		if ifNotExists {
			return Done, nil
		} else {
			return InvalidOpResult, fmt.Errorf("schema %s already exists", name)
		}
	}
	_, err := storage.GCatalog.CreateSchema(run.Txn, name)
	if err != nil {
		return 0, err
	}
	return Done, nil
}

func (run *Runner) createSchemaClose() error {
	return nil
}
