package common

import (
	"bytes"
	"unsafe"

	"github.com/daviszhen/plan/pkg/util"
)

type String struct {
	Len  int
	Data unsafe.Pointer
}

func (s *String) DataSlice() []byte {
	return util.PointerToSlice[byte](s.Data, s.Len)
}

func (s *String) DataPtr() unsafe.Pointer {
	return s.Data
}

func (s *String) String() string {
	t := s.DataSlice()
	return string(t)
}

func (s *String) Equal(o *String) bool {
	if s.Len != o.Len {
		return false
	}
	sSlice := util.PointerToSlice[byte](s.Data, s.Len)
	oSlice := util.PointerToSlice[byte](o.Data, o.Len)
	return bytes.Equal(sSlice, oSlice)
}

func (s *String) Less(o *String) bool {
	sSlice := util.PointerToSlice[byte](s.Data, s.Len)
	oSlice := util.PointerToSlice[byte](o.Data, o.Len)
	return bytes.Compare(sSlice, oSlice) < 0
}

func (s *String) Length() int {
	return s.Len
}

func (s String) NullLen() int {
	return 0
}

func WriteString(val String, serial util.Serialize) error {
	err := util.Write[uint32](uint32(val.Length()), serial)
	if err != nil {
		return err
	}

	if val.Length() > 0 {
		err = serial.WriteData(val.DataSlice(), val.Length())
		if err != nil {
			return err
		}
	}
	return err
}

func ReadString(val *String, deserial util.Deserialize) error {
	var len uint32
	err := util.Read[uint32](&len, deserial)
	if err != nil {
		return err
	}
	if len > 0 {
		val.Data = util.CMalloc(int(len))
		val.Len = int(len)
		err = deserial.ReadData(val.DataSlice(), val.Length())
		if err != nil {
			return err
		}
	}
	return err
}
