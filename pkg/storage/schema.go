package storage

import (
	"slices"

	"github.com/daviszhen/plan/pkg/chunk"
	"github.com/daviszhen/plan/pkg/common"
)

// catalog.database holds all databases
func createCatalogDatabase() *DataTable {
	dbColDefs := []*ColumnDefinition{
		{
			Name: "account_id",
			Type: common.UbigintType(),
		},
		{
			Name: "dat_name",
			Type: common.VarcharType(),
		},
		{
			Name: "dat_id",
			Type: common.UbigintType(),
		},
		{
			Name: "dat_catalog_name",
			Type: common.VarcharType(),
		},
		{
			Name: "dat_type",
			Type: common.VarcharType(),
		},
		{
			Name: "owner",
			Type: common.UbigintType(),
		},
		{
			Name: "creator",
			Type: common.UbigintType(),
		},
		{
			Name: "created_time",
			Type: common.VarcharType(),
		},
	}
	dbTable := NewDataTable(
		"catalog",
		"database",
		dbColDefs)
	dbTable._info._constraints = []Constraint{
		{
			_typ:         ConstraintTypeUnique,
			_uniqueIndex: -1,
			_uniqueNames: []string{
				dbColDefs[0].Name,
				dbColDefs[1].Name,
			},
			_isPrimaryKey: true,
		},
		{
			_typ:          ConstraintTypeNotNull,
			_notNullIndex: 0,
		},
		{
			_typ:          ConstraintTypeNotNull,
			_notNullIndex: 1,
		},
	}
	pkey := NewIndex(
		IndexTypeBPlus,
		GStorageMgr._blockMgr,
		[]IdxType{0, 1},
		[]common.LType{
			dbColDefs[0].Type,
			dbColDefs[1].Type},
		IndexConstraintTypePrimary,
		nil,
	)
	dbTable._info._indexes.AddIndex(pkey)
	return dbTable
}

func getInfoOfCatalogDatabase() *DataTableInfo {
	info := NewDataTableInfo()
	info._schema = "catalog"
	info._table = "database"
	info._colDefs = []*ColumnDefinition{
		{
			Name: "account_id",
			Type: common.UbigintType(),
		},
		{
			Name: "dat_name",
			Type: common.VarcharType(),
		},
		{
			Name: "dat_id",
			Type: common.UbigintType(),
		},
		{
			Name: "dat_catalog_name",
			Type: common.VarcharType(),
		},
		{
			Name: "dat_type",
			Type: common.VarcharType(),
		},
		{
			Name: "owner",
			Type: common.UbigintType(),
		},
		{
			Name: "creator",
			Type: common.UbigintType(),
		},
		{
			Name: "created_time",
			Type: common.VarcharType(),
		},
	}
	info._constraints = []Constraint{
		{
			_typ:         ConstraintTypeUnique,
			_uniqueIndex: -1,
			_uniqueNames: []string{
				info._colDefs[0].Name,
				info._colDefs[1].Name,
			},
			_isPrimaryKey: true,
		},
		{
			_typ:          ConstraintTypeNotNull,
			_notNullIndex: 0,
		},
		{
			_typ:          ConstraintTypeNotNull,
			_notNullIndex: 1,
		},
	}
	return info
}

// catalog.tables holds all tables
func createCatalogTables() *DataTable {
	tablesColDefs := []*ColumnDefinition{
		{
			Name: "account_id",
			Type: common.UbigintType(),
		},
		{
			Name: "reldatabase",
			Type: common.VarcharType(),
		},
		{
			Name: "relname",
			Type: common.VarcharType(),
		},
		{
			Name: "reldatabase_id",
			Type: common.UbigintType(),
		},
		{
			Name: "rel_id",
			Type: common.UbigintType(),
		},
		{
			Name: "relpersistence",
			Type: common.VarcharType(),
		},
		{
			Name: "relkind",
			Type: common.VarcharType(),
		},
		{
			Name: "rel_comment",
			Type: common.VarcharType(),
		},
		{
			Name: "rel_createsql",
			Type: common.VarcharType(),
		},
		{
			Name: "partitioned",
			Type: common.UbigintType(),
		},
		{
			Name: "partition_info",
			Type: common.VarcharType(),
		},
		{
			Name: "viewdef",
			Type: common.VarcharType(),
		},
		{
			Name: "constraint",
			Type: common.VarcharType(),
		},
		{
			Name: "rel_version",
			Type: common.UbigintType(),
		},
		{
			Name: "catalog_version",
			Type: common.UbigintType(),
		},
		{
			Name: "owner",
			Type: common.UbigintType(),
		},
		{
			Name: "creator",
			Type: common.UbigintType(),
		},
		{
			Name: "created_time",
			Type: common.VarcharType(),
		},
	}
	tablesTable := NewDataTable(
		"catalog",
		"tables",
		tablesColDefs)
	tablesTable._info._constraints = []Constraint{
		{
			_typ:         ConstraintTypeUnique,
			_uniqueIndex: -1,
			_uniqueNames: []string{
				tablesColDefs[0].Name,
				tablesColDefs[1].Name,
				tablesColDefs[2].Name,
			},
			_isPrimaryKey: true,
		},
		{
			_typ:          ConstraintTypeNotNull,
			_notNullIndex: 0,
		},
		{
			_typ:          ConstraintTypeNotNull,
			_notNullIndex: 1,
		},
		{
			_typ:          ConstraintTypeNotNull,
			_notNullIndex: 2,
		},
	}
	pkey := NewIndex(
		IndexTypeBPlus,
		GStorageMgr._blockMgr,
		[]IdxType{0, 1, 2},
		[]common.LType{
			tablesColDefs[0].Type,
			tablesColDefs[1].Type,
			tablesColDefs[2].Type,
		},
		IndexConstraintTypePrimary,
		nil,
	)
	tablesTable._info._indexes.AddIndex(pkey)
	return tablesTable
}

func getInfoOfCatalogTables() *DataTableInfo {
	info := NewDataTableInfo()
	info._schema = "catalog"
	info._table = "tables"
	info._colDefs = []*ColumnDefinition{
		{
			Name: "account_id",
			Type: common.UbigintType(),
		},
		{
			Name: "reldatabase",
			Type: common.VarcharType(),
		},
		{
			Name: "relname",
			Type: common.VarcharType(),
		},
		{
			Name: "reldatabase_id",
			Type: common.UbigintType(),
		},
		{
			Name: "rel_id",
			Type: common.UbigintType(),
		},
		{
			Name: "relpersistence",
			Type: common.VarcharType(),
		},
		{
			Name: "relkind",
			Type: common.VarcharType(),
		},
		{
			Name: "rel_comment",
			Type: common.VarcharType(),
		},
		{
			Name: "rel_createsql",
			Type: common.VarcharType(),
		},
		{
			Name: "partitioned",
			Type: common.UbigintType(),
		},
		{
			Name: "partition_info",
			Type: common.VarcharType(),
		},
		{
			Name: "viewdef",
			Type: common.VarcharType(),
		},
		{
			Name: "constraint",
			Type: common.VarcharType(),
		},
		{
			Name: "rel_version",
			Type: common.UbigintType(),
		},
		{
			Name: "catalog_version",
			Type: common.UbigintType(),
		},
		{
			Name: "owner",
			Type: common.UbigintType(),
		},
		{
			Name: "creator",
			Type: common.UbigintType(),
		},
		{
			Name: "created_time",
			Type: common.VarcharType(),
		},
	}
	info._constraints = []Constraint{
		{
			_typ:         ConstraintTypeUnique,
			_uniqueIndex: -1,
			_uniqueNames: []string{
				info._colDefs[0].Name,
				info._colDefs[1].Name,
				info._colDefs[2].Name,
			},
			_isPrimaryKey: true,
		},
		{
			_typ:          ConstraintTypeNotNull,
			_notNullIndex: 0,
		},
		{
			_typ:          ConstraintTypeNotNull,
			_notNullIndex: 1,
		},
		{
			_typ:          ConstraintTypeNotNull,
			_notNullIndex: 2,
		},
	}
	return info
}

// catalog.columns holds all tables
func createCatalogColumns() *DataTable {
	columnsColDefs := []*ColumnDefinition{
		{
			Name: "account_id",
			Type: common.UbigintType(),
		},
		{
			Name: "att_database",
			Type: common.VarcharType(),
		},
		{
			Name: "att_relname",
			Type: common.VarcharType(),
		},
		{
			Name: "attname",
			Type: common.VarcharType(),
		},
		{
			Name: "att_uniq_name",
			Type: common.VarcharType(),
		},
		{
			Name: "att_database_id",
			Type: common.UbigintType(),
		},
		{
			Name: "att_relname_id",
			Type: common.UbigintType(),
		},
		{
			Name: "atttyp",
			Type: common.VarcharType(),
		},
		{
			Name: "attnum",
			Type: common.UbigintType(),
		},
		{
			Name: "att_length",
			Type: common.UbigintType(),
		},
		{
			Name: "attnotnull",
			Type: common.UbigintType(),
		},
		{
			Name: "atthasdef",
			Type: common.UbigintType(),
		},
		{
			Name: "att_default",
			Type: common.VarcharType(),
		},
		{
			Name: "attisdropped",
			Type: common.UbigintType(),
		},
		{
			Name: "att_constraint_type",
			Type: common.VarcharType(),
		},
		{
			Name: "att_is_unsigned",
			Type: common.UbigintType(),
		},
		{
			Name: "att_is_auto_increment",
			Type: common.UbigintType(),
		},
		{
			Name: "att_comment",
			Type: common.VarcharType(),
		},
		{
			Name: "att_is_hidden",
			Type: common.UbigintType(),
		},
		{
			Name: "attr_has_update",
			Type: common.UbigintType(),
		},
		{
			Name: "attr_update",
			Type: common.VarcharType(),
		},
		{
			Name: "attr_seqnum",
			Type: common.UbigintType(),
		},
		{
			Name: "attr_enum",
			Type: common.VarcharType(),
		},
	}
	columnsTable := NewDataTable(
		"catalog",
		"columns",
		columnsColDefs)
	columnsTable._info._constraints = []Constraint{
		{
			_typ:         ConstraintTypeUnique,
			_uniqueIndex: -1,
			_uniqueNames: []string{
				columnsColDefs[0].Name,
				columnsColDefs[1].Name,
				columnsColDefs[2].Name,
				columnsColDefs[3].Name,
			},
			_isPrimaryKey: true,
		},
		{
			_typ:          ConstraintTypeNotNull,
			_notNullIndex: 0,
		},
		{
			_typ:          ConstraintTypeNotNull,
			_notNullIndex: 1,
		},
		{
			_typ:          ConstraintTypeNotNull,
			_notNullIndex: 2,
		},
		{
			_typ:          ConstraintTypeNotNull,
			_notNullIndex: 3,
		},
	}
	pkey := NewIndex(
		IndexTypeBPlus,
		GStorageMgr._blockMgr,
		[]IdxType{0, 1, 2, 3},
		[]common.LType{
			columnsColDefs[0].Type,
			columnsColDefs[1].Type,
			columnsColDefs[2].Type,
			columnsColDefs[3].Type,
		},
		IndexConstraintTypePrimary,
		nil,
	)
	columnsTable._info._indexes.AddIndex(pkey)
	return columnsTable
}

func getInfoOfCatalogColumns() *DataTableInfo {
	info := NewDataTableInfo()
	info._schema = "catalog"
	info._table = "columns"
	info._colDefs = []*ColumnDefinition{
		{
			Name: "account_id",
			Type: common.UbigintType(),
		},
		{
			Name: "att_database",
			Type: common.VarcharType(),
		},
		{
			Name: "att_relname",
			Type: common.VarcharType(),
		},
		{
			Name: "attname",
			Type: common.VarcharType(),
		},
		{
			Name: "att_uniq_name",
			Type: common.VarcharType(),
		},
		{
			Name: "att_database_id",
			Type: common.UbigintType(),
		},
		{
			Name: "att_relname_id",
			Type: common.UbigintType(),
		},
		{
			Name: "atttyp",
			Type: common.VarcharType(),
		},
		{
			Name: "attnum",
			Type: common.UbigintType(),
		},
		{
			Name: "att_length",
			Type: common.UbigintType(),
		},
		{
			Name: "attnotnull",
			Type: common.UbigintType(),
		},
		{
			Name: "atthasdef",
			Type: common.UbigintType(),
		},
		{
			Name: "att_default",
			Type: common.VarcharType(),
		},
		{
			Name: "attisdropped",
			Type: common.UbigintType(),
		},
		{
			Name: "att_constraint_type",
			Type: common.VarcharType(),
		},
		{
			Name: "att_is_unsigned",
			Type: common.UbigintType(),
		},
		{
			Name: "att_is_auto_increment",
			Type: common.UbigintType(),
		},
		{
			Name: "att_comment",
			Type: common.VarcharType(),
		},
		{
			Name: "att_is_hidden",
			Type: common.UbigintType(),
		},
		{
			Name: "attr_has_update",
			Type: common.UbigintType(),
		},
		{
			Name: "attr_update",
			Type: common.VarcharType(),
		},
		{
			Name: "attr_seqnum",
			Type: common.UbigintType(),
		},
		{
			Name: "attr_enum",
			Type: common.VarcharType(),
		},
	}
	info._constraints = []Constraint{
		{
			_typ:         ConstraintTypeUnique,
			_uniqueIndex: -1,
			_uniqueNames: []string{
				info._colDefs[0].Name,
				info._colDefs[1].Name,
				info._colDefs[2].Name,
				info._colDefs[3].Name,
			},
			_isPrimaryKey: true,
		},
		{
			_typ:          ConstraintTypeNotNull,
			_notNullIndex: 0,
		},
		{
			_typ:          ConstraintTypeNotNull,
			_notNullIndex: 1,
		},
		{
			_typ:          ConstraintTypeNotNull,
			_notNullIndex: 2,
		},
		{
			_typ:          ConstraintTypeNotNull,
			_notNullIndex: 3,
		},
	}
	return info
}

func CreateTable(
	schema, tabName string,
	colDefs []*ColumnDefinition,
	constraints []*Constraint,
) *DataTable {
	dbTable := NewDataTable(
		schema,
		tabName,
		colDefs)

	////collect constraints
	////check primary key
	//pkColNames := make([]string, 0)
	//for i, cons := range constraints {
	//	if cons._isPrimaryKey {
	//		pkColNames = append(pkColNames, cons._uniqueNames...)
	//	}
	//}
	//dbTable._info._constraints = constraints
	//pkey := NewIndex(
	//	IndexTypeBPlus,
	//	GStorageMgr._blockMgr,
	//	[]IdxType{0, 1},
	//	[]common.LType{
	//		dbColDefs[0].Type,
	//		dbColDefs[1].Type},
	//	IndexConstraintTypePrimary,
	//	nil,
	//)
	//dbTable._info._indexes.AddIndex(pkey)
	return dbTable
}

func CreateInternalSchemas() error {
	//txn0
	txn0, err := GTxnMgr.NewTxn("txn0")
	if err != nil {
		return err
	}
	BeginQuery(txn0)
	_, err = GCatalog.CreateSchema(txn0, "catalog")
	if err != nil {
		return err
	}

	var dbEnt *CatalogEntry
	dbEnt, err = GCatalog.CreateTable(txn0, getInfoOfCatalogDatabase())
	if err != nil {
		return err
	}

	var tablesEnt *CatalogEntry
	tablesEnt, err = GCatalog.CreateTable(txn0, getInfoOfCatalogTables())
	if err != nil {
		return err
	}

	var columnsEnt *CatalogEntry
	columnsEnt, err = GCatalog.CreateTable(txn0, getInfoOfCatalogColumns())
	if err != nil {
		return err
	}

	err = fillCatalogDBTable(dbEnt._storage, txn0)
	if err != nil {
		return err
	}
	err = fillCatalogTablesTable(tablesEnt._storage, txn0)
	if err != nil {
		return err
	}
	err = fillCatalogColumnsTable(columnsEnt._storage,
		dbEnt._storage,
		tablesEnt._storage, txn0)
	if err != nil {
		return err
	}
	err = GTxnMgr.Commit(txn0)
	if err != nil {
		return err
	}
	return err
}

func fillCatalogDBTable(table *DataTable, txn *Txn) error {
	lAState := &LocalAppendState{}
	table.InitLocalAppend(txn, lAState)

	data := &chunk.Chunk{}
	data.Init(table.GetTypes(), STANDARD_VECTOR_SIZE)

	//1. record of catalog

	//account_id
	col0Vec := make([]uint64, STANDARD_VECTOR_SIZE)
	//dat_name
	col1Vec := make([]string, STANDARD_VECTOR_SIZE)
	col1Vec[0] = "catalog"
	col1Vec[1] = "public"
	//dat_id
	col2Vec := make([]uint64, STANDARD_VECTOR_SIZE)
	col2Vec[0] = 0
	col2Vec[1] = 1
	//dat_catalog_name
	col3Vec := make([]string, STANDARD_VECTOR_SIZE)
	//dat_type
	col4Vec := make([]string, STANDARD_VECTOR_SIZE)
	//owner
	col5Vec := make([]uint64, STANDARD_VECTOR_SIZE)
	//creator
	col6Vec := make([]uint64, STANDARD_VECTOR_SIZE)
	//created_time
	col7Vec := make([]string, STANDARD_VECTOR_SIZE)

	data.Data[0] = chunk.NewUbigintFlatVector(col0Vec, STANDARD_VECTOR_SIZE)
	data.Data[1] = chunk.NewVarcharFlatVector(col1Vec, STANDARD_VECTOR_SIZE)
	data.Data[2] = chunk.NewUbigintFlatVector(col2Vec, STANDARD_VECTOR_SIZE)
	data.Data[3] = chunk.NewVarcharFlatVector(col3Vec, STANDARD_VECTOR_SIZE)
	data.Data[4] = chunk.NewVarcharFlatVector(col4Vec, STANDARD_VECTOR_SIZE)
	data.Data[5] = chunk.NewUbigintFlatVector(col5Vec, STANDARD_VECTOR_SIZE)
	data.Data[6] = chunk.NewUbigintFlatVector(col6Vec, STANDARD_VECTOR_SIZE)
	data.Data[7] = chunk.NewVarcharFlatVector(col7Vec, STANDARD_VECTOR_SIZE)

	data.SetCard(2)

	err := table.LocalAppend(txn, lAState, data, false)
	if err != nil {
		return err
	}
	table.FinalizeLocalAppend(txn, lAState)
	return err
}

func SaveToCatalogDBTable(table *DataTable, txn *Txn, schema string) error {
	lAState := &LocalAppendState{}
	table.InitLocalAppend(txn, lAState)

	data := &chunk.Chunk{}
	data.Init(table.GetTypes(), STANDARD_VECTOR_SIZE)

	//1. record of catalog

	//account_id
	col0Vec := make([]uint64, STANDARD_VECTOR_SIZE)
	//dat_name
	col1Vec := make([]string, STANDARD_VECTOR_SIZE)
	col1Vec[0] = schema
	//dat_id
	col2Vec := make([]uint64, STANDARD_VECTOR_SIZE)
	col2Vec[0] = 0
	//col2Vec[1] = 1
	//dat_catalog_name
	col3Vec := make([]string, STANDARD_VECTOR_SIZE)
	//dat_type
	col4Vec := make([]string, STANDARD_VECTOR_SIZE)
	//owner
	col5Vec := make([]uint64, STANDARD_VECTOR_SIZE)
	//creator
	col6Vec := make([]uint64, STANDARD_VECTOR_SIZE)
	//created_time
	col7Vec := make([]string, STANDARD_VECTOR_SIZE)

	data.Data[0] = chunk.NewUbigintFlatVector(col0Vec, STANDARD_VECTOR_SIZE)
	data.Data[1] = chunk.NewVarcharFlatVector(col1Vec, STANDARD_VECTOR_SIZE)
	data.Data[2] = chunk.NewUbigintFlatVector(col2Vec, STANDARD_VECTOR_SIZE)
	data.Data[3] = chunk.NewVarcharFlatVector(col3Vec, STANDARD_VECTOR_SIZE)
	data.Data[4] = chunk.NewVarcharFlatVector(col4Vec, STANDARD_VECTOR_SIZE)
	data.Data[5] = chunk.NewUbigintFlatVector(col5Vec, STANDARD_VECTOR_SIZE)
	data.Data[6] = chunk.NewUbigintFlatVector(col6Vec, STANDARD_VECTOR_SIZE)
	data.Data[7] = chunk.NewVarcharFlatVector(col7Vec, STANDARD_VECTOR_SIZE)

	data.SetCard(1)

	err := table.LocalAppend(txn, lAState, data, false)
	if err != nil {
		return err
	}
	table.FinalizeLocalAppend(txn, lAState)
	return err
}

func fillCatalogTablesTable(table *DataTable, txn *Txn) error {
	lAState := &LocalAppendState{}
	table.InitLocalAppend(txn, lAState)

	data := &chunk.Chunk{}
	data.Init(table.GetTypes(), STANDARD_VECTOR_SIZE)

	//1. record of tables

	//account_id
	col0Vec := make([]uint64, STANDARD_VECTOR_SIZE)
	//reldatabase
	col1Vec := make([]string, STANDARD_VECTOR_SIZE)
	col1Vec[0] = "catalog"
	col1Vec[1] = "catalog"
	col1Vec[2] = "catalog"
	//relname
	col2Vec := make([]string, STANDARD_VECTOR_SIZE)
	col2Vec[0] = "database"
	col2Vec[1] = "tables"
	col2Vec[2] = "columns"
	//reldatabase_id
	col3Vec := make([]uint64, STANDARD_VECTOR_SIZE)
	col3Vec[0] = 0
	col3Vec[1] = 0
	col3Vec[2] = 0
	//rel_id
	col4Vec := make([]uint64, STANDARD_VECTOR_SIZE)
	col4Vec[0] = 0
	col4Vec[1] = 1
	col4Vec[2] = 2
	//relpersistence
	col5Vec := make([]string, STANDARD_VECTOR_SIZE)
	col5Vec[0] = "persist"
	col5Vec[1] = "persist"
	col5Vec[2] = "persist"
	//relkind
	col6Vec := make([]string, STANDARD_VECTOR_SIZE)
	col6Vec[0] = "table"
	col6Vec[1] = "table"
	col6Vec[2] = "table"
	//rel_comment
	col7Vec := make([]string, STANDARD_VECTOR_SIZE)
	col7Vec[0] = "catalog.database"
	col7Vec[1] = "catalog.tables"
	col7Vec[2] = "catalog.columns"
	//rel_createsql
	col8Vec := make([]string, STANDARD_VECTOR_SIZE)
	//partitioned
	col9Vec := make([]uint64, STANDARD_VECTOR_SIZE)
	//partition_info
	col10Vec := make([]string, STANDARD_VECTOR_SIZE)
	//viewdef
	col11Vec := make([]string, STANDARD_VECTOR_SIZE)
	//constraint
	col12Vec := make([]string, STANDARD_VECTOR_SIZE)
	//rel_version
	col13Vec := make([]uint64, STANDARD_VECTOR_SIZE)
	//catalog_version
	col14Vec := make([]uint64, STANDARD_VECTOR_SIZE)
	//owner
	col15Vec := make([]uint64, STANDARD_VECTOR_SIZE)
	//creator
	col16Vec := make([]uint64, STANDARD_VECTOR_SIZE)
	//created_time
	col17Vec := make([]string, STANDARD_VECTOR_SIZE)

	data.Data[0] = chunk.NewUbigintFlatVector(col0Vec, STANDARD_VECTOR_SIZE)
	data.Data[1] = chunk.NewVarcharFlatVector(col1Vec, STANDARD_VECTOR_SIZE)
	data.Data[2] = chunk.NewVarcharFlatVector(col2Vec, STANDARD_VECTOR_SIZE)
	data.Data[3] = chunk.NewUbigintFlatVector(col3Vec, STANDARD_VECTOR_SIZE)
	data.Data[4] = chunk.NewUbigintFlatVector(col4Vec, STANDARD_VECTOR_SIZE)
	data.Data[5] = chunk.NewVarcharFlatVector(col5Vec, STANDARD_VECTOR_SIZE)
	data.Data[6] = chunk.NewVarcharFlatVector(col6Vec, STANDARD_VECTOR_SIZE)
	data.Data[7] = chunk.NewVarcharFlatVector(col7Vec, STANDARD_VECTOR_SIZE)
	data.Data[8] = chunk.NewVarcharFlatVector(col8Vec, STANDARD_VECTOR_SIZE)
	data.Data[9] = chunk.NewUbigintFlatVector(col9Vec, STANDARD_VECTOR_SIZE)
	data.Data[10] = chunk.NewVarcharFlatVector(col10Vec, STANDARD_VECTOR_SIZE)
	data.Data[11] = chunk.NewVarcharFlatVector(col11Vec, STANDARD_VECTOR_SIZE)
	data.Data[12] = chunk.NewVarcharFlatVector(col12Vec, STANDARD_VECTOR_SIZE)
	data.Data[13] = chunk.NewUbigintFlatVector(col13Vec, STANDARD_VECTOR_SIZE)
	data.Data[14] = chunk.NewUbigintFlatVector(col14Vec, STANDARD_VECTOR_SIZE)
	data.Data[15] = chunk.NewUbigintFlatVector(col15Vec, STANDARD_VECTOR_SIZE)
	data.Data[16] = chunk.NewUbigintFlatVector(col16Vec, STANDARD_VECTOR_SIZE)
	data.Data[17] = chunk.NewVarcharFlatVector(col17Vec, STANDARD_VECTOR_SIZE)

	data.SetCard(3)

	err := table.LocalAppend(txn, lAState, data, false)
	if err != nil {
		return err
	}
	table.FinalizeLocalAppend(txn, lAState)
	return err
}

func SaveToCatalogTables(
	table *DataTable,
	txn *Txn,
	schema, tabName string) error {
	lAState := &LocalAppendState{}
	table.InitLocalAppend(txn, lAState)

	data := &chunk.Chunk{}
	data.Init(table.GetTypes(), STANDARD_VECTOR_SIZE)

	//1. record of tables

	//account_id
	col0Vec := make([]uint64, STANDARD_VECTOR_SIZE)
	//reldatabase
	col1Vec := make([]string, STANDARD_VECTOR_SIZE)
	col1Vec[0] = schema
	//relname
	col2Vec := make([]string, STANDARD_VECTOR_SIZE)
	col2Vec[0] = tabName
	//reldatabase_id
	col3Vec := make([]uint64, STANDARD_VECTOR_SIZE)
	col3Vec[0] = 0
	//rel_id
	col4Vec := make([]uint64, STANDARD_VECTOR_SIZE)
	col4Vec[0] = 0
	//relpersistence
	col5Vec := make([]string, STANDARD_VECTOR_SIZE)
	col5Vec[0] = "persist"
	//relkind
	col6Vec := make([]string, STANDARD_VECTOR_SIZE)
	col6Vec[0] = "table"
	//rel_comment
	col7Vec := make([]string, STANDARD_VECTOR_SIZE)
	col7Vec[0] = schema + "." + tabName
	//rel_createsql
	col8Vec := make([]string, STANDARD_VECTOR_SIZE)
	//partitioned
	col9Vec := make([]uint64, STANDARD_VECTOR_SIZE)
	//partition_info
	col10Vec := make([]string, STANDARD_VECTOR_SIZE)
	//viewdef
	col11Vec := make([]string, STANDARD_VECTOR_SIZE)
	//constraint
	col12Vec := make([]string, STANDARD_VECTOR_SIZE)
	//rel_version
	col13Vec := make([]uint64, STANDARD_VECTOR_SIZE)
	//catalog_version
	col14Vec := make([]uint64, STANDARD_VECTOR_SIZE)
	//owner
	col15Vec := make([]uint64, STANDARD_VECTOR_SIZE)
	//creator
	col16Vec := make([]uint64, STANDARD_VECTOR_SIZE)
	//created_time
	col17Vec := make([]string, STANDARD_VECTOR_SIZE)

	data.Data[0] = chunk.NewUbigintFlatVector(col0Vec, STANDARD_VECTOR_SIZE)
	data.Data[1] = chunk.NewVarcharFlatVector(col1Vec, STANDARD_VECTOR_SIZE)
	data.Data[2] = chunk.NewVarcharFlatVector(col2Vec, STANDARD_VECTOR_SIZE)
	data.Data[3] = chunk.NewUbigintFlatVector(col3Vec, STANDARD_VECTOR_SIZE)
	data.Data[4] = chunk.NewUbigintFlatVector(col4Vec, STANDARD_VECTOR_SIZE)
	data.Data[5] = chunk.NewVarcharFlatVector(col5Vec, STANDARD_VECTOR_SIZE)
	data.Data[6] = chunk.NewVarcharFlatVector(col6Vec, STANDARD_VECTOR_SIZE)
	data.Data[7] = chunk.NewVarcharFlatVector(col7Vec, STANDARD_VECTOR_SIZE)
	data.Data[8] = chunk.NewVarcharFlatVector(col8Vec, STANDARD_VECTOR_SIZE)
	data.Data[9] = chunk.NewUbigintFlatVector(col9Vec, STANDARD_VECTOR_SIZE)
	data.Data[10] = chunk.NewVarcharFlatVector(col10Vec, STANDARD_VECTOR_SIZE)
	data.Data[11] = chunk.NewVarcharFlatVector(col11Vec, STANDARD_VECTOR_SIZE)
	data.Data[12] = chunk.NewVarcharFlatVector(col12Vec, STANDARD_VECTOR_SIZE)
	data.Data[13] = chunk.NewUbigintFlatVector(col13Vec, STANDARD_VECTOR_SIZE)
	data.Data[14] = chunk.NewUbigintFlatVector(col14Vec, STANDARD_VECTOR_SIZE)
	data.Data[15] = chunk.NewUbigintFlatVector(col15Vec, STANDARD_VECTOR_SIZE)
	data.Data[16] = chunk.NewUbigintFlatVector(col16Vec, STANDARD_VECTOR_SIZE)
	data.Data[17] = chunk.NewVarcharFlatVector(col17Vec, STANDARD_VECTOR_SIZE)

	data.SetCard(1)

	err := table.LocalAppend(txn, lAState, data, false)
	if err != nil {
		return err
	}
	table.FinalizeLocalAppend(txn, lAState)
	return err
}

func fillColumnsChunk(
	txn *Txn,
	dstTable *DataTable,
	lAState *LocalAppendState,
	srcTable *DataTable) error {

	//1. record of columns

	//account_id
	col0Vec := make([]uint64, STANDARD_VECTOR_SIZE)
	//att_database
	col1Vec := make([]string, STANDARD_VECTOR_SIZE)
	//att_relname
	col2Vec := make([]string, STANDARD_VECTOR_SIZE)
	//attname
	col3Vec := make([]string, STANDARD_VECTOR_SIZE)
	//att_uniq_name
	col4Vec := make([]string, STANDARD_VECTOR_SIZE)
	//att_database_id
	col5Vec := make([]uint64, STANDARD_VECTOR_SIZE)
	//att_relname_id
	col6Vec := make([]uint64, STANDARD_VECTOR_SIZE)
	//atttyp
	col7Vec := make([]string, STANDARD_VECTOR_SIZE)
	//attnum
	col8Vec := make([]uint64, STANDARD_VECTOR_SIZE)
	//att_length
	col9Vec := make([]uint64, STANDARD_VECTOR_SIZE)
	//attnotnull
	col10Vec := make([]uint64, STANDARD_VECTOR_SIZE)
	//atthasdef
	col11Vec := make([]uint64, STANDARD_VECTOR_SIZE)
	//att_default
	col12Vec := make([]string, STANDARD_VECTOR_SIZE)
	//attisdropped
	col13Vec := make([]uint64, STANDARD_VECTOR_SIZE)
	//att_constraint_type
	col14Vec := make([]string, STANDARD_VECTOR_SIZE)
	//att_is_unsigned
	col15Vec := make([]uint64, STANDARD_VECTOR_SIZE)
	//att_is_auto_increment
	col16Vec := make([]uint64, STANDARD_VECTOR_SIZE)
	//att_comment
	col17Vec := make([]string, STANDARD_VECTOR_SIZE)
	//att_is_hidden
	col18Vec := make([]uint64, STANDARD_VECTOR_SIZE)
	//attr_has_update
	col19Vec := make([]uint64, STANDARD_VECTOR_SIZE)
	//attr_update
	col20Vec := make([]string, STANDARD_VECTOR_SIZE)
	//attr_seqnum
	col21Vec := make([]uint64, STANDARD_VECTOR_SIZE)
	//attr_enum
	col22Vec := make([]string, STANDARD_VECTOR_SIZE)

	data := &chunk.Chunk{}
	for i, colDef := range srcTable._colDefs {
		res := i % STANDARD_VECTOR_SIZE
		if res == 0 { //0,8,16
			//init vec
			data.Init(dstTable.GetTypes(), STANDARD_VECTOR_SIZE)
		}

		//fill vec
		//account_id
		col0Vec[res] = 0
		//att_database
		col1Vec[res] = srcTable._info._schema
		//att_relname
		col2Vec[res] = srcTable._info._table
		//attname
		col3Vec[res] = colDef.Name
		//att_uniq_name
		col4Vec[res] = colDef.Name
		//att_database_id
		col5Vec[res] = 0
		//att_relname_id
		switch srcTable._info._table {
		case "database":
			col6Vec[res] = 0
		case "tables":
			col6Vec[res] = 1
		case "columns":
			col6Vec[res] = 2
		}
		//atttyp
		col7Vec[res] = colDef.Type.String()
		//attnum
		col8Vec[res] = uint64(i + 1)
		//att_length
		col9Vec[res] = 0
		//attnotnull
		if slices.Contains(srcTable._info._indexes._indexes[0]._columnIds, IdxType(i)) {
			//if it is in the primary key, it is not null
			col10Vec[res] = 1
		} else {
			col10Vec[res] = 0
		}

		//atthasdef
		col11Vec[res] = 0
		//att_default
		col12Vec[res] = ""
		//attisdropped
		col13Vec[res] = 0
		//att_constraint_type
		col14Vec[res] = "n"
		//att_is_unsigned
		col15Vec[res] = 0
		//att_is_auto_increment
		col16Vec[res] = 0
		//att_comment
		col17Vec[res] = ""
		//att_is_hidden
		col18Vec[res] = 0
		//attr_has_update
		col19Vec[res] = 0
		//attr_update
		col20Vec[res] = ""
		//attr_seqnum
		col21Vec[res] = uint64(i)
		//attr_enum
		col22Vec[res] = ""

		//append to table
		if res == STANDARD_VECTOR_SIZE-1 ||
			i == len(srcTable._colDefs)-1 {

			data.Data[0] = chunk.NewUbigintFlatVector(col0Vec, STANDARD_VECTOR_SIZE)
			data.Data[1] = chunk.NewVarcharFlatVector(col1Vec, STANDARD_VECTOR_SIZE)
			data.Data[2] = chunk.NewVarcharFlatVector(col2Vec, STANDARD_VECTOR_SIZE)
			data.Data[3] = chunk.NewVarcharFlatVector(col3Vec, STANDARD_VECTOR_SIZE)
			data.Data[4] = chunk.NewVarcharFlatVector(col4Vec, STANDARD_VECTOR_SIZE)
			data.Data[5] = chunk.NewUbigintFlatVector(col5Vec, STANDARD_VECTOR_SIZE)
			data.Data[6] = chunk.NewUbigintFlatVector(col6Vec, STANDARD_VECTOR_SIZE)
			data.Data[7] = chunk.NewVarcharFlatVector(col7Vec, STANDARD_VECTOR_SIZE)
			data.Data[8] = chunk.NewUbigintFlatVector(col8Vec, STANDARD_VECTOR_SIZE)
			data.Data[9] = chunk.NewUbigintFlatVector(col9Vec, STANDARD_VECTOR_SIZE)
			data.Data[10] = chunk.NewUbigintFlatVector(col10Vec, STANDARD_VECTOR_SIZE)
			data.Data[11] = chunk.NewUbigintFlatVector(col11Vec, STANDARD_VECTOR_SIZE)
			data.Data[12] = chunk.NewVarcharFlatVector(col12Vec, STANDARD_VECTOR_SIZE)
			data.Data[13] = chunk.NewUbigintFlatVector(col13Vec, STANDARD_VECTOR_SIZE)
			data.Data[14] = chunk.NewVarcharFlatVector(col14Vec, STANDARD_VECTOR_SIZE)
			data.Data[15] = chunk.NewUbigintFlatVector(col15Vec, STANDARD_VECTOR_SIZE)
			data.Data[16] = chunk.NewUbigintFlatVector(col16Vec, STANDARD_VECTOR_SIZE)
			data.Data[17] = chunk.NewVarcharFlatVector(col17Vec, STANDARD_VECTOR_SIZE)
			data.Data[18] = chunk.NewUbigintFlatVector(col18Vec, STANDARD_VECTOR_SIZE)
			data.Data[19] = chunk.NewUbigintFlatVector(col19Vec, STANDARD_VECTOR_SIZE)
			data.Data[20] = chunk.NewVarcharFlatVector(col20Vec, STANDARD_VECTOR_SIZE)
			data.Data[21] = chunk.NewUbigintFlatVector(col21Vec, STANDARD_VECTOR_SIZE)
			data.Data[22] = chunk.NewVarcharFlatVector(col22Vec, STANDARD_VECTOR_SIZE)

			data.SetCard(res + 1)
			//data.Print()

			err := dstTable.LocalAppend(txn, lAState, data, false)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func fillCatalogColumnsTable(table *DataTable,
	db, tables *DataTable, txn *Txn) error {
	lAState := &LocalAppendState{}
	table.InitLocalAppend(txn, lAState)

	err := fillColumnsChunk(txn, table, lAState, db)
	if err != nil {
		return err
	}
	err = fillColumnsChunk(txn, table, lAState, tables)
	if err != nil {
		return err
	}
	err = fillColumnsChunk(txn, table, lAState, table)
	if err != nil {
		return err
	}

	table.FinalizeLocalAppend(txn, lAState)
	return err
}

func fillColumnsChunk2(
	txn *Txn,
	dstTable *DataTable,
	lAState *LocalAppendState,
	schema, tabName string,
	colDefs []*ColumnDefinition,
	tableCons []*Constraint) error {

	//1. record of columns

	//account_id
	col0Vec := make([]uint64, STANDARD_VECTOR_SIZE)
	//att_database
	col1Vec := make([]string, STANDARD_VECTOR_SIZE)
	//att_relname
	col2Vec := make([]string, STANDARD_VECTOR_SIZE)
	//attname
	col3Vec := make([]string, STANDARD_VECTOR_SIZE)
	//att_uniq_name
	col4Vec := make([]string, STANDARD_VECTOR_SIZE)
	//att_database_id
	col5Vec := make([]uint64, STANDARD_VECTOR_SIZE)
	//att_relname_id
	col6Vec := make([]uint64, STANDARD_VECTOR_SIZE)
	//atttyp
	col7Vec := make([]string, STANDARD_VECTOR_SIZE)
	//attnum
	col8Vec := make([]uint64, STANDARD_VECTOR_SIZE)
	//att_length
	col9Vec := make([]uint64, STANDARD_VECTOR_SIZE)
	//attnotnull
	col10Vec := make([]uint64, STANDARD_VECTOR_SIZE)
	//atthasdef
	col11Vec := make([]uint64, STANDARD_VECTOR_SIZE)
	//att_default
	col12Vec := make([]string, STANDARD_VECTOR_SIZE)
	//attisdropped
	col13Vec := make([]uint64, STANDARD_VECTOR_SIZE)
	//att_constraint_type
	col14Vec := make([]string, STANDARD_VECTOR_SIZE)
	//att_is_unsigned
	col15Vec := make([]uint64, STANDARD_VECTOR_SIZE)
	//att_is_auto_increment
	col16Vec := make([]uint64, STANDARD_VECTOR_SIZE)
	//att_comment
	col17Vec := make([]string, STANDARD_VECTOR_SIZE)
	//att_is_hidden
	col18Vec := make([]uint64, STANDARD_VECTOR_SIZE)
	//attr_has_update
	col19Vec := make([]uint64, STANDARD_VECTOR_SIZE)
	//attr_update
	col20Vec := make([]string, STANDARD_VECTOR_SIZE)
	//attr_seqnum
	col21Vec := make([]uint64, STANDARD_VECTOR_SIZE)
	//attr_enum
	col22Vec := make([]string, STANDARD_VECTOR_SIZE)

	data := &chunk.Chunk{}
	for i, colDef := range colDefs {
		res := i % STANDARD_VECTOR_SIZE
		if res == 0 { //0,8,16
			//init vec
			data.Init(dstTable.GetTypes(), STANDARD_VECTOR_SIZE)
		}

		//fill vec
		//account_id
		col0Vec[res] = 0
		//att_database
		col1Vec[res] = schema
		//att_relname
		col2Vec[res] = tabName
		//attname
		col3Vec[res] = colDef.Name
		//att_uniq_name
		col4Vec[res] = colDef.Name
		//att_database_id
		col5Vec[res] = 0
		//att_relname_id
		col6Vec[res] = 3
		//atttyp
		col7Vec[res] = colDef.Type.String()
		//attnum
		col8Vec[res] = uint64(i + 1)
		//att_length
		col9Vec[res] = 0
		//attnotnull
		col10Vec[res] = 0
		for _, cons := range colDef.Constraints {
			//if it is in the primary key, it is not null
			if cons._isPrimaryKey ||
				cons._typ == ConstraintTypeNotNull {
				col10Vec[res] = 1
			}
		}
		for _, cons := range tableCons {
			if cons._isPrimaryKey &&
				slices.Contains(cons._uniqueNames, colDef.Name) {
				col10Vec[res] = 1
			}
		}
		//atthasdef
		col11Vec[res] = 0
		//att_default
		col12Vec[res] = ""
		//attisdropped
		col13Vec[res] = 0
		//att_constraint_type
		col14Vec[res] = "n"
		for _, cons := range colDef.Constraints {
			//it is in the primary key
			if cons._isPrimaryKey {
				col14Vec[res] = "p"
			}
		}
		for _, cons := range tableCons {
			if cons._isPrimaryKey &&
				slices.Contains(cons._uniqueNames, colDef.Name) {
				col14Vec[res] = "p"
			}
		}
		//att_is_unsigned
		col15Vec[res] = 0
		//att_is_auto_increment
		col16Vec[res] = 0
		//att_comment
		col17Vec[res] = ""
		//att_is_hidden
		col18Vec[res] = 0
		//attr_has_update
		col19Vec[res] = 0
		//attr_update
		col20Vec[res] = ""
		//attr_seqnum
		col21Vec[res] = uint64(i)
		//attr_enum
		col22Vec[res] = ""

		//append to table
		if res == STANDARD_VECTOR_SIZE-1 ||
			i == len(colDefs)-1 {

			data.Data[0] = chunk.NewUbigintFlatVector(col0Vec, STANDARD_VECTOR_SIZE)
			data.Data[1] = chunk.NewVarcharFlatVector(col1Vec, STANDARD_VECTOR_SIZE)
			data.Data[2] = chunk.NewVarcharFlatVector(col2Vec, STANDARD_VECTOR_SIZE)
			data.Data[3] = chunk.NewVarcharFlatVector(col3Vec, STANDARD_VECTOR_SIZE)
			data.Data[4] = chunk.NewVarcharFlatVector(col4Vec, STANDARD_VECTOR_SIZE)
			data.Data[5] = chunk.NewUbigintFlatVector(col5Vec, STANDARD_VECTOR_SIZE)
			data.Data[6] = chunk.NewUbigintFlatVector(col6Vec, STANDARD_VECTOR_SIZE)
			data.Data[7] = chunk.NewVarcharFlatVector(col7Vec, STANDARD_VECTOR_SIZE)
			data.Data[8] = chunk.NewUbigintFlatVector(col8Vec, STANDARD_VECTOR_SIZE)
			data.Data[9] = chunk.NewUbigintFlatVector(col9Vec, STANDARD_VECTOR_SIZE)
			data.Data[10] = chunk.NewUbigintFlatVector(col10Vec, STANDARD_VECTOR_SIZE)
			data.Data[11] = chunk.NewUbigintFlatVector(col11Vec, STANDARD_VECTOR_SIZE)
			data.Data[12] = chunk.NewVarcharFlatVector(col12Vec, STANDARD_VECTOR_SIZE)
			data.Data[13] = chunk.NewUbigintFlatVector(col13Vec, STANDARD_VECTOR_SIZE)
			data.Data[14] = chunk.NewVarcharFlatVector(col14Vec, STANDARD_VECTOR_SIZE)
			data.Data[15] = chunk.NewUbigintFlatVector(col15Vec, STANDARD_VECTOR_SIZE)
			data.Data[16] = chunk.NewUbigintFlatVector(col16Vec, STANDARD_VECTOR_SIZE)
			data.Data[17] = chunk.NewVarcharFlatVector(col17Vec, STANDARD_VECTOR_SIZE)
			data.Data[18] = chunk.NewUbigintFlatVector(col18Vec, STANDARD_VECTOR_SIZE)
			data.Data[19] = chunk.NewUbigintFlatVector(col19Vec, STANDARD_VECTOR_SIZE)
			data.Data[20] = chunk.NewVarcharFlatVector(col20Vec, STANDARD_VECTOR_SIZE)
			data.Data[21] = chunk.NewUbigintFlatVector(col21Vec, STANDARD_VECTOR_SIZE)
			data.Data[22] = chunk.NewVarcharFlatVector(col22Vec, STANDARD_VECTOR_SIZE)

			data.SetCard(res + 1)
			//data.Print()

			err := dstTable.LocalAppend(txn, lAState, data, false)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func SaveToCatalogColumns(
	table *DataTable,
	txn *Txn,
	schema, tabName string,
	colDefs []*ColumnDefinition,
	tableCons []*Constraint,
) error {
	lAState := &LocalAppendState{}
	table.InitLocalAppend(txn, lAState)

	err := fillColumnsChunk2(txn, table, lAState, schema, tabName, colDefs, tableCons)
	if err != nil {
		return err
	}

	table.FinalizeLocalAppend(txn, lAState)
	return err
}

// ReadCatalogTable read catalog table:
// catalog.database, catalog.tables,catalog.columns
func ReadCatalogTable(
	table *DataTable,
	txn *Txn,
	limit int,
	callback func(scanned *chunk.Chunk)) int {
	scanState := NewTableScanState()
	//extract primary key
	pkey := table._info._indexes._indexes[0]

	colIdx := make([]IdxType, 1+len(pkey._columnIds))
	colIdx[0] = COLUMN_IDENTIFIER_ROW_ID
	for i, colId := range pkey._columnIds {
		colIdx[i+1] = colId
	}
	table.InitScan(txn, scanState, colIdx)
	cnt := 0
	colTyps := make([]common.LType, 0)
	colTyps = append(colTyps, common.BigintType())
	colTyps = append(colTyps, pkey._logicalTypes...)
	for {
		scanned := &chunk.Chunk{}
		scanned.Init(colTyps, STANDARD_VECTOR_SIZE)
		table.Scan(txn, scanned, scanState)
		if scanned.Card() == 0 {
			break
		}
		cnt += scanned.Card()

		if callback != nil {
			callback(scanned)
		}
	}
	return cnt
}
