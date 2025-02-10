package storage

import (
	"fmt"

	"github.com/daviszhen/plan/pkg/util"
)

type Database struct {
	_name   string
	_tables []*DataTable
}

func (db *Database) Serialize(serial util.Serialize) error {
	writer := NewFieldWriter(serial)
	err := WriteString(db._name, writer)
	if err != nil {
		return err
	}
	return writer.Finalize()
}

func (db *Database) Deserialize(deserial util.Deserialize) error {
	reader, err := NewFieldReader(deserial)
	if err != nil {
		return err
	}
	db._name, err = ReadString(reader)
	if err != nil {
		return err
	}
	reader.Finalize()
	return nil
}

func (db *Database) GetTable(name string) *DataTable {
	for _, table := range db._tables {
		if table._info._table == name {
			return table
		}
	}
	panic(fmt.Sprintf("no table %s ", name))
}
