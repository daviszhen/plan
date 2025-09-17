package v2

import (
	"github.com/daviszhen/plan/pkg/common"
	"github.com/google/uuid"
)

type TableDef struct {
	_id     string
	_name   string
	_schema string
	_cols   *Schema
}

func NewTableDef(name, schema string, cols *Schema) *TableDef {
	id, _ := uuid.NewV7()
	return &TableDef{
		_id:     id.String(),
		_name:   name,
		_schema: schema,
		_cols:   cols,
	}
}

type Schema struct {
	_colDefs []*ColumnDef
}

func NewSchema(colDefs []*ColumnDef) *Schema {
	return &Schema{
		_colDefs: colDefs,
	}
}

type ColumnDef struct {
	_id   string
	_name string
	_type common.LType
}

func NewColumnDef(name string, typ common.LType) *ColumnDef {
	id, _ := uuid.NewV7()
	return &ColumnDef{
		_id:   id.String(),
		_name: name,
		_type: typ,
	}
}
