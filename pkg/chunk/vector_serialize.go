package chunk

import (
	"github.com/daviszhen/plan/pkg/common"
	"github.com/daviszhen/plan/pkg/util"
)

// Serialization and deserialization methods for Vector
func (vec *Vector) Serialize(count int, serial util.Serialize) error {
	var vdata UnifiedFormat
	vec.ToUnifiedFormat(count, &vdata)
	writeValidity := (count > 0) && !vdata.Mask.AllValid()
	err := util.Write[bool](writeValidity, serial)
	if err != nil {
		return err
	}
	if writeValidity {
		flatMask := &util.Bitmap{}
		flatMask.Init(count)
		for i := 0; i < count; i++ {
			rowIdx := vdata.Sel.GetIndex(i)
			flatMask.Set(uint64(i), vdata.Mask.RowIsValid(uint64(rowIdx)))
		}
		err = serial.WriteData(flatMask.Data(), flatMask.Bytes(count))
		if err != nil {
			return err
		}
	}
	typ := vec.Typ()
	if typ.GetInternalType().IsConstant() {
		writeSize := typ.GetInternalType().Size() * count
		buff := util.GAlloc.Alloc(writeSize)
		defer util.GAlloc.Free(buff)
		WriteToStorage(vec, count, util.BytesSliceToPointer(buff))
		err = serial.WriteData(buff, writeSize)
		if err != nil {
			return err
		}
	} else {
		switch typ.GetInternalType() {
		case common.VARCHAR:
			strSlice := GetSliceInPhyFormatUnifiedFormat[common.String](&vdata)
			for i := 0; i < count; i++ {
				idx := vdata.Sel.GetIndex(i)
				if !vdata.Mask.RowIsValid(uint64(idx)) {
					nVal := StringScatterOp{}.NullValue()
					err = common.WriteString(nVal, serial)
					if err != nil {
						return err
					}
				} else {
					val := strSlice[idx]
					err = common.WriteString(val, serial)
					if err != nil {
						return err
					}
				}
			}
		default:
			panic("usp")
		}
	}
	return err
}

func (vec *Vector) Deserialize(count int, deserial util.Deserialize) error {
	var mask *util.Bitmap
	switch vec.PhyFormat() {
	case PF_CONST:
		mask = GetMaskInPhyFormatConst(vec)
	case PF_FLAT:
		mask = GetMaskInPhyFormatFlat(vec)
	case PF_DICT:
		panic("usp")
	}
	mask.Reset()
	hasMask := false
	err := util.Read[bool](&hasMask, deserial)
	if err != nil {
		return err
	}
	if hasMask {
		mask.Init(count)
		err = deserial.ReadData(mask.Data(), mask.Bytes(count))
		if err != nil {
			return err
		}
	}

	typ := vec.Typ()
	if typ.GetInternalType().IsConstant() {
		readSize := typ.GetInternalType().Size() * count
		buf := util.GAlloc.Alloc(readSize)
		defer util.GAlloc.Free(buf)
		err = deserial.ReadData(buf, readSize)
		if err != nil {
			return err
		}
		ReadFromStorage(util.BytesSliceToPointer(buf), count, vec)
	} else {
		switch typ.GetInternalType() {
		case common.VARCHAR:
			strSlice := GetSliceInPhyFormatFlat[common.String](vec)
			for i := 0; i < count; i++ {
				var str common.String
				err = common.ReadString(&str, deserial)
				if err != nil {
					return err
				}
				if mask.RowIsValid(uint64(i)) {
					strSlice[i] = str
				}
			}
		default:
			panic("usp")
		}
	}
	return nil
}
