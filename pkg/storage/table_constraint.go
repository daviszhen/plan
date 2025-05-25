package storage

import (
	"fmt"
	"strings"

	"github.com/daviszhen/plan/pkg/util"
)

const (
	ConstraintTypeInvalid    uint8 = 0
	ConstraintTypeNotNull    uint8 = 1
	ConstraintTypeCheck      uint8 = 2
	ConstraintTypeUnique     uint8 = 3
	ConstraintTypeForeignKey uint8 = 4
)

type Constraint struct {
	_typ          uint8
	_notNullIndex int
	_uniqueIndex  int      //single unique column
	_uniqueNames  []string // multiple unique columns
	_isPrimaryKey bool     //primary or unique
}

func NewNotNullConstraint(
	index int,
) *Constraint {
	return &Constraint{
		_typ:          ConstraintTypeNotNull,
		_notNullIndex: index,
	}
}

func NewUniqueIndexConstraint(
	index int,
	pk bool,
) *Constraint {
	return &Constraint{
		_typ:          ConstraintTypeUnique,
		_uniqueIndex:  index,
		_isPrimaryKey: pk,
	}
}

func NewUniqueIndexConstraint2(
	names []string,
	pk bool,
) *Constraint {
	return &Constraint{
		_typ:          ConstraintTypeUnique,
		_uniqueNames:  names,
		_isPrimaryKey: pk,
	}
}

func (cons *Constraint) Serialize(serial util.Serialize) error {
	writer := NewFieldWriter(serial)
	err := WriteField[uint8](cons._typ, writer)
	if err != nil {
		return err
	}
	//
	err = cons.Serialize2(writer)
	if err != nil {
		return err
	}
	return writer.Finalize()
}

func (cons *Constraint) Serialize2(writer *FieldWriter) error {
	switch cons._typ {
	case ConstraintTypeNotNull:
		return WriteField[int](cons._notNullIndex, writer)
	case ConstraintTypeUnique:
		err := WriteField[bool](cons._isPrimaryKey, writer)
		if err != nil {
			return err
		}
		err = WriteField[int](cons._uniqueIndex, writer)
		if err != nil {
			return err
		}
		err = WriteStrings(cons._uniqueNames, writer)
		if err != nil {
			return err
		}
		return err
	default:
		panic("usp")
	}
}

func (cons *Constraint) Deserialize(source util.Deserialize) error {
	reader, err := NewFieldReader(source)
	if err != nil {
		return err
	}
	err = ReadRequired[uint8](&cons._typ, reader)
	if err != nil {
		return err
	}
	switch cons._typ {
	case ConstraintTypeNotNull:
		return ReadRequired[int](&cons._notNullIndex, reader)
	case ConstraintTypeUnique:
		err = ReadRequired[bool](&cons._isPrimaryKey, reader)
		if err != nil {
			return err
		}
		err = ReadRequired[int](&cons._uniqueIndex, reader)
		if err != nil {
			return err
		}
		cons._uniqueNames, err = ReadStrings(reader)
		if err != nil {
			return err
		}
	default:
		panic("usp")
	}
	reader.Finalize()
	return err
}

func (cons *Constraint) String() string {
	bb := strings.Builder{}
	bb.WriteString("{")
	switch cons._typ {
	case ConstraintTypeInvalid:
		bb.WriteString("invalid")
	case ConstraintTypeNotNull:
		bb.WriteString("not null")
	case ConstraintTypeCheck:
		bb.WriteString("check")
	case ConstraintTypeUnique:
		if cons._isPrimaryKey {
			bb.WriteString("primary key")
		} else {
			bb.WriteString("unique")
		}
		if len(cons._uniqueNames) > 0 {
			bb.WriteString("(")
			bb.WriteString(fmt.Sprintf("%s", strings.Join(cons._uniqueNames, ",")))
			bb.WriteString(")")
		}
	case ConstraintTypeForeignKey:
		bb.WriteString("foreign key")
	}

	bb.WriteString("}")
	return bb.String()
}
