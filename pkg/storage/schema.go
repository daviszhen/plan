package storage

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
