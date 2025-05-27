// Copyright 2023-2024 daviszhen
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"errors"
	"fmt"
	"strings"

	"github.com/daviszhen/plan/pkg/common"
	"github.com/daviszhen/plan/pkg/storage"
	"github.com/daviszhen/plan/pkg/util"
	pg_query "github.com/pganalyze/pg_query_go/v5"
)

func collectTags(root *LogicalOperator, set map[uint64]bool) {
	if root.Index != 0 {
		set[root.Index] = true
	}
	if root.Index2 != 0 {
		set[root.Index2] = true
	}
	for _, child := range root.Children {
		collectTags(child, set)
	}
}

const (
	NoneSide       = 0
	LeftSide       = 1 << 1
	RightSide      = 1 << 2
	BothSide       = LeftSide | RightSide
	CorrelatedSide = 1 << 3
)

func (b *Builder) createPhyLimit(root *LogicalOperator, children []*PhysicalOperator) (*PhysicalOperator, error) {
	return &PhysicalOperator{
		Typ:      POT_Limit,
		Outputs:  root.Outputs,
		Limit:    root.Limit,
		Offset:   root.Offset,
		Children: children}, nil
}

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

func (b *Builder) buildInsert(
	txn *storage.Txn,
	stmt *pg_query.InsertStmt,
	ctx *BindContext,
	depth int) (*LogicalOperator, error) {
	schema := stmt.GetRelation().GetSchemaname()
	if schema == "" {
		schema = "public"
	}
	name := stmt.GetRelation().GetRelname()

	return b.buildInsertInternal(
		txn,
		schema,
		name,
		stmt.Cols,
		stmt.GetSelectStmt().GetSelectStmt(),
		ctx, depth,
	)
}

func (b *Builder) buildInsertInternal(
	txn *storage.Txn,
	schema string,
	name string,
	Cols []*pg_query.Node,
	subSelect *pg_query.SelectStmt,
	ctx *BindContext,
	depth int) (*LogicalOperator, error) {
	//step 0: get table
	tabEnt := storage.GCatalog.GetEntry(
		txn,
		storage.CatalogTypeTable,
		schema,
		name,
	)
	if tabEnt == nil {
		return nil, fmt.Errorf("no table %s in schema '%s'",
			name, schema)
	}
	insert := &LogicalOperator{
		Typ:        LOT_Insert,
		Database:   schema,
		Table:      name,
		TableEnt:   tabEnt,
		TableIndex: b.GetTag(),
	}

	//step 1: process columns
	//column seq no -> column idx in table
	namedColumnMap := make([]int, 0)
	if len(Cols) != 0 {
		getNameFun := func(col *pg_query.Node) string {
			resTar := col.GetResTarget()
			if resTar != nil {
				return resTar.GetName()
			}
			str := col.GetString_()
			if str != nil {
				return str.GetSval()
			}
			panic("usp")
		}
		//insert into specified columns
		//column name in stmt -> column seq no in stmt
		columnNameMap := make(map[string]int)
		for i, col := range Cols {
			colName := strings.ToLower(getNameFun(col))
			if _, has := columnNameMap[colName]; has {
				return nil, fmt.Errorf("duplicate column name %s in INSERT", colName)
			}
			columnNameMap[colName] = i
			colIdx := tabEnt.GetColumnIndex(colName)
			if colIdx == -1 {
				return nil, fmt.Errorf("invalid column %d", colIdx)
			}
			colDef := tabEnt.GetColumn(colIdx)
			insert.ExpectedTypes = append(insert.ExpectedTypes, colDef.Type)
			namedColumnMap = append(namedColumnMap, colIdx)
		}
		//
		for _, colDef := range tabEnt.GetColumns() {
			if seqNo, has := columnNameMap[colDef.Name]; !has {
				insert.ColumnIndexMap = append(insert.ColumnIndexMap, -1)
			} else {
				insert.ColumnIndexMap = append(insert.ColumnIndexMap, seqNo)
			}
		}
	} else {
		//no specified columns
		for i, colDef := range tabEnt.GetColumns() {
			namedColumnMap = append(namedColumnMap, i)
			insert.ExpectedTypes = append(insert.ExpectedTypes,
				colDef.Type)
		}
	}

	//step 2 : process default values
	//TODO:

	if subSelect == nil {
		return insert, nil
	}

	//step 3: process select stmt
	subBuilder := NewBuilder(b.txn)
	subBuilder.tag = b.tag
	subBuilder.rootCtx.parent = ctx

	expectedColumns := 0
	if len(Cols) == 0 {
		expectedColumns = len(tabEnt.GetColumns())
	} else {
		expectedColumns = len(Cols)
	}

	//value list
	if len(subSelect.ValuesLists) != 0 {
		expectedTypes := make([]common.LType, expectedColumns)
		expectedNames := make([]string, expectedColumns)

		resultColumns := len(subSelect.ValuesLists[0].GetList().GetItems())
		if expectedColumns != resultColumns {
			return nil, fmt.Errorf("table %s has %d columns but %d values were supplied",
				name,
				expectedColumns,
				resultColumns,
			)
		}

		//value list
		for colIdx := 0; colIdx < expectedColumns; colIdx++ {
			tableColIdx := namedColumnMap[colIdx]
			colDef := tabEnt.GetColumn(tableColIdx)
			expectedTypes[colIdx] = colDef.Type
			expectedNames[colIdx] = colDef.Name

			//TODO: process default value
		}
		subBuilder.expectedTypes = expectedTypes
		subBuilder.expectedNames = expectedNames
	}

	err := subBuilder.buildSelect(
		subSelect,
		subBuilder.rootCtx, 0)
	if err != nil {
		return nil, err
	}

	if len(subBuilder.projectExprs) == 0 {
		panic("select in Insert must have project list")
	}

	if expectedColumns != len(subBuilder.projectExprs) {
		return nil, fmt.Errorf("table %s has %d columns but %d values were supplied",
			name,
			expectedColumns,
			len(subBuilder.projectExprs),
		)
	}

	//build plan
	lp, err := subBuilder.CreatePlan(subBuilder.rootCtx, nil)
	if err != nil {
		return nil, err
	}
	if lp == nil {
		return nil, errors.New("sub select in insert has nil plan")
	}

	//cast types
	lp, err = b.CastLogicalOperatorToTypes(insert.ExpectedTypes, lp)
	if err != nil {
		return nil, err
	}

	checkExprIsValid(lp)
	lp, err = subBuilder.Optimize(subBuilder.rootCtx, lp)
	if err != nil {
		return nil, err
	}
	if lp == nil {
		return nil, errors.New("nil plan")
	}
	checkExprIsValid(lp)

	insert.Children = append(insert.Children, lp)

	return insert, nil
}

func (b *Builder) buildValuesLists(
	lists []*pg_query.Node,
	ctx *BindContext,
	depth int) (*Expr, error) {
	var err error
	resultTypes := b.expectedTypes
	resultNames := b.expectedNames
	resultValues := make([][]*Expr, 0)
	var retExpr *Expr
	for _, exprList := range lists {
		items := exprList.GetList().GetItems()
		if len(resultNames) == 0 {
			for i := 0; i < len(exprList.GetList().GetItems()); i++ {
				resultNames = append(resultNames, fmt.Sprintf("col%d", i))
			}
		}
		list := make([]*Expr, 0)
		for valIdx, value := range items {
			retExpr, err = b.bindExpr(ctx, IWC_VALUES, value, depth)
			if err != nil {
				return nil, err
			}
			retExpr, err = AddCastToType(retExpr, resultTypes[valIdx], false)
			if err != nil {
				return nil, err
			}
			list = append(list, retExpr)
		}
		resultValues = append(resultValues, list)
	}

	if len(resultTypes) == 0 && len(lists) != 0 {
		panic("usp")
	}

	alias := "valueslist"
	bind := &Binding{
		typ:     BT_TABLE,
		alias:   alias,
		index:   uint64(b.GetTag()),
		typs:    util.CopyTo(resultTypes),
		names:   util.CopyTo(resultNames),
		nameMap: make(map[string]int),
	}
	for idx, name := range bind.names {
		bind.nameMap[name] = idx
	}
	err = ctx.AddBinding(alias, bind)
	if err != nil {
		return nil, err
	}

	return &Expr{
		Typ:         ET_ValuesList,
		Index:       bind.index,
		Database:    "",
		Table:       alias,
		Alias:       alias,
		BelongCtx:   ctx,
		Types:       resultTypes,
		Names:       resultNames,
		Values:      resultValues,
		ColName2Idx: bind.nameMap,
	}, err
}

func (b *Builder) CastLogicalOperatorToTypes(
	targetTyps []common.LType,
	root *LogicalOperator) (*LogicalOperator, error) {
	//srcTyps := make([]common.LType)
	//for i, output := range lp.Outputs {
	//
	//}
	var err error
	if root.Typ == LOT_Project {
		util.AssertFunc(len(root.Projects) == len(targetTyps))
		for i, proj := range root.Projects {
			if proj.DataTyp.Id != targetTyps[i].Id {
				//add cast
				root.Projects[i], err = AddCastToType(
					proj,
					targetTyps[i],
					false)
				if err != nil {
					return nil, err
				}
			}
		}
		return root, nil
	} else {
		//add cast project
		panic("usp")
	}
	return root, nil
}

func (b *Builder) createPhyInsert(
	root *LogicalOperator,
	children []*PhysicalOperator) (*PhysicalOperator, error) {
	//list := storage.NewDependList()
	//list.AddDepend(root.TableEnt)

	ret := &PhysicalOperator{
		Typ:            POT_Insert,
		TableEnt:       root.TableEnt,
		ColumnIndexMap: root.ColumnIndexMap,
		InsertTypes:    root.TableEnt.GetTypes(),
		Children:       children,
	}
	return ret, nil
}

func (b *Builder) buildCopy(
	txn *storage.Txn,
	stmt *pg_query.CopyStmt,
	ctx *BindContext,
	depth int) (*LogicalOperator, error) {
	if stmt.IsFrom {
		return b.buildCopyFrom(txn, stmt, ctx, depth)
	} else {
		return b.buildCopyTo(txn, stmt, ctx, depth)
	}
}

func (b *Builder) buildCopyFrom(
	txn *storage.Txn,
	stmt *pg_query.CopyStmt,
	ctx *BindContext,
	depth int) (*LogicalOperator, error) {
	schema := stmt.GetRelation().GetSchemaname()
	if schema == "" {
		schema = "public"
	}
	name := stmt.GetRelation().GetRelname()
	insert, err := b.buildInsertInternal(
		txn,
		schema,
		name,
		stmt.GetAttlist(),
		nil,
		ctx,
		depth,
	)
	if err != nil {
		return nil, err
	}
	tabEnt := storage.GCatalog.GetEntry(txn, storage.CatalogTypeTable, schema, name)
	if tabEnt == nil {
		return nil, fmt.Errorf("no table %s in schema %s", name, schema)
	}
	var expectedNames []string
	if len(insert.ColumnIndexMap) != 0 {
		expectedNames = make([]string, len(insert.ExpectedTypes))
		for i, colDef := range tabEnt.GetColumns() {
			if insert.ColumnIndexMap[i] != -1 {
				expectedNames[insert.ColumnIndexMap[i]] = colDef.Name
			}
		}
	} else {
		for _, colDef := range tabEnt.GetColumns() {
			expectedNames = append(expectedNames, colDef.Name)
		}
	}

	getOpts := func() []*ScanOption {
		opts := make([]*ScanOption, 0)
		for _, node := range stmt.GetOptions() {
			opt := &ScanOption{}
			opt.Kind = node.GetDefElem().GetDefname()
			opt.Opt = node.GetDefElem().GetArg().GetString_().GetSval()
			opts = append(opts, opt)
		}
		return opts
	}

	opts := getOpts()

	formatOpt := getFormatFun("format", opts)
	if formatOpt == nil {
		return nil, fmt.Errorf("no format option in copy from")
	}

	scanInfo := &ScanInfo{
		ReturnedTypes: insert.ExpectedTypes,
		Names:         expectedNames,
		FilePath:      stmt.GetFilename(),
		Opts:          opts,
		Format:        formatOpt.Opt,
	}

	for i := 0; i < len(insert.ExpectedTypes); i++ {
		scanInfo.ColumnIds = append(scanInfo.ColumnIds, i)
	}

	name2IdxMap := make(map[string]int)
	for i, colName := range expectedNames {
		name2IdxMap[colName] = i
	}

	scanOp := &LogicalOperator{
		Typ:         LOT_Scan,
		Index:       uint64(b.GetTag()),
		ScanTyp:     ScanTypeCopyFrom,
		ScanInfo:    scanInfo,
		ColName2Idx: name2IdxMap,
	}

	projOp := &LogicalOperator{
		Typ:      LOT_Project,
		Children: []*LogicalOperator{scanOp},
	}

	for i := 0; i < len(scanInfo.Names); i++ {
		expr := &Expr{
			Typ:     ET_Column,
			DataTyp: scanInfo.ReturnedTypes[i],
			Name:    scanInfo.Names[i],
			Alias:   scanInfo.Names[i],
			ColRef:  ColumnBind{scanOp.Index, uint64(i)},
		}
		projOp.Projects = append(projOp.Projects, expr)
	}

	tempBuilder := NewBuilder(b.txn)
	projOp, err = tempBuilder.Optimize(tempBuilder.rootCtx, projOp)
	if err != nil {
		return nil, err
	}
	if projOp == nil {
		return nil, errors.New("nil plan")
	}
	checkExprIsValid(projOp)

	insert.Children = append(insert.Children, projOp)
	return insert, nil
}

func getFormatFun(kind string, opts []*ScanOption) *ScanOption {
	for _, opt := range opts {
		if opt.Kind == kind {
			return opt
		}
	}
	return nil
}

func (b *Builder) buildCopyTo(
	txn *storage.Txn,
	stmt *pg_query.CopyStmt,
	ctx *BindContext,
	depth int) (*LogicalOperator, error) {
	return nil, fmt.Errorf("usp copy to")
}
