package compute

import (
	"errors"
	"fmt"
	"strings"

	"github.com/daviszhen/plan/pkg/common"
	"github.com/daviszhen/plan/pkg/storage"
	pg_query "github.com/pganalyze/pg_query_go/v5"
)

func (b *Builder) createPhyCreateSchema(root *LogicalOperator, children []*PhysicalOperator) (*PhysicalOperator, error) {
	return &PhysicalOperator{
		Typ:         POT_CreateSchema,
		Database:    root.Database,
		IfNotExists: root.IfNotExists,
		Children:    children}, nil
}

func (b *Builder) createPhyCreateTable(root *LogicalOperator, children []*PhysicalOperator) (*PhysicalOperator, error) {
	return &PhysicalOperator{
		Typ:         POT_CreateTable,
		Database:    root.Database,
		Table:       root.Table,
		IfNotExists: root.IfNotExists,
		ColDefs:     root.ColDefs,
		Constraints: root.Constraints,
		Children:    children,
	}, nil
}

func (b *Builder) buildDDL(txn *storage.Txn, ddl *pg_query.RawStmt, ctx *BindContext, depth int) (*LogicalOperator, error) {
	switch impl := ddl.GetStmt().GetNode().(type) {
	case *pg_query.Node_CreateSchemaStmt:
		return b.buildCreateSchema(
			txn,
			impl.CreateSchemaStmt,
			ctx,
			depth)
	case *pg_query.Node_CreateStmt:
		return b.buildCreateTable(
			txn,
			impl.CreateStmt,
			ctx,
			depth)
	case *pg_query.Node_InsertStmt:
		return b.buildInsert(txn, impl.InsertStmt, ctx, depth)
	case *pg_query.Node_CopyStmt:
		return b.buildCopy(txn, impl.CopyStmt, ctx, depth)
	case *pg_query.Node_SelectStmt:
		err := b.buildSelect(impl.SelectStmt, b.rootCtx, 0)
		if err != nil {
			return nil, err
		}

		lp, err := b.CreatePlan(b.rootCtx, nil)
		if err != nil {
			return nil, err
		}
		if lp == nil {
			return nil, errors.New("nil plan")
		}
		checkExprIsValid(lp)
		lp, err = b.Optimize(b.rootCtx, lp)
		if err != nil {
			return nil, err
		}
		if lp == nil {
			return nil, errors.New("nil plan")
		}
		checkExprIsValid(lp)
		return lp, nil
	default:
		return nil, fmt.Errorf("unsupport statement right now")
	}
	return nil, nil
}

func (b *Builder) buildCreateSchema(
	txn *storage.Txn,
	stmt *pg_query.CreateSchemaStmt,
	ctx *BindContext,
	depth int) (*LogicalOperator, error) {
	name := stmt.Schemaname
	return &LogicalOperator{
		Typ:         LOT_CreateSchema,
		Database:    name,
		IfNotExists: stmt.IfNotExists,
	}, nil
}

func (b *Builder) buildCreateTable(
	txn *storage.Txn,
	stmt *pg_query.CreateStmt,
	ctx *BindContext,
	depth int) (*LogicalOperator, error) {

	schema := stmt.GetRelation().GetSchemaname()
	name := stmt.GetRelation().GetRelname()

	ret := &LogicalOperator{
		Typ:         LOT_CreateTable,
		Database:    schema,
		Table:       name,
		IfNotExists: stmt.GetIfNotExists(),
	}

	colDefs := make([]*storage.ColumnDefinition, 0)
	tableCons := make([]*storage.Constraint, 0)
	for colIdx, node := range stmt.GetTableElts() {
		switch nodeImpl := node.GetNode().(type) {
		case *pg_query.Node_ColumnDef:
			colDef := nodeImpl.ColumnDef
			colDefExpr := &storage.ColumnDefinition{}

			//column name
			colDefExpr.Name = colDef.Colname
			//column type
			typName := ""
			switch len(colDef.TypeName.Names) {
			case 2:
				typName = colDef.TypeName.Names[1].GetString_().GetSval()
			case 1:
				typName = colDef.TypeName.Names[0].GetString_().GetSval()
			default:
				panic("usp")
			}
			switch strings.ToLower(typName) {
			case "int4":
				colDefExpr.Type = common.IntegerType()
			case "int8":
				colDefExpr.Type = common.BigintType()
			case "varchar":
				colDefExpr.Type = common.VarcharType()
			case "numeric":
				typMods := colDef.TypeName.GetTypmods()
				width := typMods[0].GetAConst().GetIval().GetIval()
				pres := typMods[1].GetAConst().GetIval().GetIval()
				colDefExpr.Type = common.DecimalType(int(width), int(pres))
			case "date":
				colDefExpr.Type = common.DateType()
			default:
				panic("")
			}

			//column constraint
			colCons := make([]*storage.Constraint, 0)
			for _, cons := range colDef.Constraints {
				consImpl := cons.GetConstraint()
				if consImpl != nil {
					switch consImpl.Contype {
					case pg_query.ConstrType_CONSTR_NOTNULL:
						colCons = append(colCons, storage.NewNotNullConstraint(colIdx))
					case pg_query.ConstrType_CONSTR_UNIQUE:
						colCons = append(colCons, storage.NewUniqueIndexConstraint(colIdx, false))
					case pg_query.ConstrType_CONSTR_PRIMARY:
						colCons = append(colCons, storage.NewUniqueIndexConstraint(colIdx, true))
					default:
						panic("")
					}
				}
			}
			colDefExpr.Constraints = colCons
			colDefs = append(colDefs, colDefExpr)
		case *pg_query.Node_Constraint: //table constraint
			cons := nodeImpl.Constraint
			pkNames := make([]string, 0)
			switch cons.GetContype() {
			case pg_query.ConstrType_CONSTR_PRIMARY:
				for _, key := range cons.GetKeys() {
					kname := key.GetString_().GetSval()
					pkNames = append(pkNames, kname)
				}
			default:
				panic("usp")
			}
			tableCons = append(tableCons, storage.NewUniqueIndexConstraint2(pkNames, true))
		default:
			panic("usp")
		}
	}
	ret.ColDefs = colDefs
	ret.Constraints = tableCons

	return ret, nil
}
