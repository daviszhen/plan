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

package common

import (
	"bytes"
	"crypto/rand"
	"errors"
	"io"
	"math"
	"os"
	"testing"

	dec "github.com/govalues/decimal"
	"github.com/stretchr/testify/assert"

	"github.com/daviszhen/plan/pkg/util"
)

func runSerial(
	t *testing.T,
	name string,
	run func(t *testing.T, fname string) error) error {
	tempFile, err := os.CreateTemp("", name)
	if err != nil {
		return err
	}
	defer func(name string) {
		_ = os.Remove(name)
	}(tempFile.Name())
	fname := tempFile.Name()
	_ = tempFile.Close()
	if run != nil {
		return run(t, fname)
	}
	return nil
}

var _ util.Serialize = new(testSerialize)
var _ util.Deserialize = new(testSerialize)

type testSerialize struct {
	data *bytes.Buffer
}

func (serial *testSerialize) ReadData(buffer []byte, len int) error {
	_, err := serial.data.Read(buffer)
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil
		}
		return err
	}
	return nil
}

func (serial *testSerialize) WriteData(buffer []byte, len int) error {
	serial.data.Write(buffer[:len])
	return nil
}

func (serial *testSerialize) Close() error {
	return nil
}

func Test_serialize(t *testing.T) {
	tSerial := new(testSerialize)
	tSerial.data = &bytes.Buffer{}

	//write
	err := util.Write[bool](true, tSerial)
	assert.NoError(t, err)
	err = util.Write[uint64](math.MaxUint64/2, tSerial)
	assert.NoError(t, err)

	err = util.Write[float64](math.MaxFloat64, tSerial)
	assert.NoError(t, err)
	s := String{
		Data: util.CMalloc(10),
		Len:  10,
	}
	sSlice := s.DataSlice()
	for i := 0; i < 10; i++ {
		sSlice[i] = byte('0' + i)
	}
	err = util.WriteString(s.String(), tSerial)
	assert.NoError(t, err)
	err = util.Write[Hugeint](
		Hugeint{Lower: math.MaxUint64 / 2, Upper: math.MaxInt64 / 2},
		tSerial,
	)
	assert.NoError(t, err)
	err = util.Write[Date](
		Date{
			Year:  2024,
			Month: 9,
			Day:   8,
		},
		tSerial,
	)
	assert.NoError(t, err)

	err = util.Write[Decimal](
		Decimal{
			Decimal: dec.MustNew(199, 2),
		},
		tSerial,
	)
	assert.NoError(t, err)

	//read
	b := false
	err = util.Read[bool](&b, tSerial)
	assert.NoError(t, err)
	assert.True(t, b)

	u64 := uint64(0)
	err = util.Read[uint64](&u64, tSerial)
	assert.NoError(t, err)
	assert.Equal(t, uint64(math.MaxUint64/2), u64)

	f64 := float64(0)
	err = util.Read[float64](&f64, tSerial)
	assert.NoError(t, err)
	assert.Equal(t, float64(math.MaxFloat64), f64)

	//s1 := String{}
	//err = ReadString(&s1, tSerial)
	//assert.NoError(t, err)
	//assert.Truef(t, equalStrOp{}.operation(&s, &s1), "string differ")

	hi := Hugeint{}
	err = util.Read[Hugeint](&hi, tSerial)
	assert.NoError(t, err)
	assert.Equal(t, Hugeint{Lower: math.MaxUint64 / 2, Upper: math.MaxInt64 / 2}, hi)

	d1 := Date{}
	err = util.Read[Date](&d1, tSerial)
	assert.NoError(t, err)
	assert.Equal(t, Date{
		Year:  2024,
		Month: 9,
		Day:   8,
	}, d1)

	dec1 := Decimal{}
	err = util.Read[Decimal](&dec1, tSerial)
	assert.NoError(t, err)
	//assert.Truef(t, equalDecimalOp{}.operation(&Decimal{
	//	Decimal: dec.MustNew(199, 2),
	//}, &dec1), "decimal differ")
}

func TestNewFileSerialize(t *testing.T) {
	run := func(t *testing.T, fname string) error {
		serial, err := util.NewFileSerialize(fname)
		assert.NoError(t, err, fname)
		assert.NotNil(t, serial)
		cnt := 1000
		buflen := 5723
		bufs := make([][]byte, cnt)
		for i := 0; i < cnt; i++ {
			bufs[i] = make([]byte, buflen)
			_, err = rand.Read(bufs[i])
			assert.NoError(t, err, "rand gen buffer failed")
		}

		for i := 0; i < cnt; i++ {
			err = serial.WriteData(bufs[i], buflen)
			assert.NoError(t, err, "serial write failed")
		}
		_ = serial.Close()

		deserial, err := util.NewFileDeserialize(fname)
		assert.NoError(t, err, fname)
		assert.NotNil(t, deserial)
		readBufs := make([][]byte, cnt)
		for i := 0; i < cnt; i++ {
			readBufs[i] = make([]byte, buflen)
			err = deserial.ReadData(readBufs[i], buflen)
			assert.NoError(t, err, "deserial read failed")
			assert.Equal(t, bufs[i], readBufs[i])
		}
		_ = deserial.Close()
		return nil
	}
	err := runSerial(t, "serial", run)
	assert.NoError(t, err)
}

//func Test_vectorSerialize(t *testing.T) {
//	type args struct {
//		typ  LType
//		pf   PhyFormat
//		vec  *Vector
//		read *Vector
//	}
//	typs := []LType{
//		IntegerType(),
//		Decimal(DecimalMaxWidthInt64, 2),
//		VarcharType(),
//	}
//	pfs := []PhyFormat{
//		PF_FLAT,
//		PF_CONST,
//	}
//	kases := make([]args, 0)
//	for _, typ := range typs {
//		for _, pf := range pfs {
//			vec := randomVector(typ, pf, 0.2)
//
//			//vec.print(defaultVectorSize)
//			kases = append(kases, args{
//				typ:  typ,
//				pf:   pf,
//				vec:  vec,
//				read: NewEmptyVector(typ, pf, defaultVectorSize),
//			})
//		}
//	}
//	run := func(t *testing.T, fname string) error {
//		serial, err := NewFileSerialize(fname)
//		assert.NoError(t, err, fname)
//		assert.NotNil(t, serial)
//		for _, kase := range kases {
//			vec := kase.vec
//			err = vec.serialize(defaultVectorSize, serial)
//			if err != nil {
//				return err
//			}
//		}
//		_ = serial.Close()
//
//		deserial, err := NewFileDeserialize(fname)
//		assert.NoError(t, err, fname)
//		assert.NotNil(t, deserial)
//
//		for _, kase := range kases {
//			read := kase.read
//			err = read.deserialize(defaultVectorSize, deserial)
//			if err != nil {
//				return err
//			}
//			same0 := isSameVector(kase.vec, kase.vec, defaultVectorSize)
//			assert.True(t, same0, "self equal self")
//			same := isSameVector(kase.vec, kase.read, defaultVectorSize)
//			assert.Truef(t, same, "type %s phy_format %s", kase.typ.String(), kase.pf.String())
//			if !same {
//				fmt.Println("src vec")
//				kase.vec.print(defaultVectorSize)
//				fmt.Println("read vec")
//				kase.read.print(defaultVectorSize)
//			}
//		}
//
//		_ = deserial.Close()
//		return nil
//	}
//	err := runSerial(t, "serial-vector", run)
//	assert.NoError(t, err)
//}
//
//func Test_chunkSerialize(t *testing.T) {
//	type args struct {
//		pf   PhyFormat
//		src  *Chunk
//		read *Chunk
//	}
//	typs := []LType{
//		IntegerType(),
//		Decimal(DecimalMaxWidthInt64, 2),
//		VarcharType(),
//	}
//	pfs := []PhyFormat{
//		PF_FLAT,
//		PF_CONST,
//	}
//	kases := make([]args, 0)
//	for _, pf := range pfs {
//		chunk := &Chunk{}
//		chunk.init(typs, defaultVectorSize)
//		for i, typ := range typs {
//			vec := randomVector(typ, pf, 0.2)
//			chunk._data[i] = vec
//		}
//		chunk.setCard(defaultVectorSize)
//
//		//chunk.print()
//
//		kases = append(kases, args{
//			pf:   pf,
//			src:  chunk,
//			read: &Chunk{},
//		})
//	}
//	run := func(t *testing.T, fname string) error {
//		serial, err := NewFileSerialize(fname)
//		assert.NoError(t, err, fname)
//		assert.NotNil(t, serial)
//		for _, kase := range kases {
//			err = kase.src.serialize(serial)
//			if err != nil {
//				return err
//			}
//		}
//		_ = serial.Close()
//
//		deserial, err := NewFileDeserialize(fname)
//		assert.NoError(t, err, fname)
//		assert.NotNil(t, deserial)
//
//		for _, kase := range kases {
//			read := kase.read
//			err = read.deserialize(deserial)
//			if err != nil {
//				return err
//			}
//
//			same0 := isSameChunk(kase.src, kase.src, defaultVectorSize)
//			assert.True(t, same0, "self equal self")
//			same := isSameChunk(kase.src, kase.read, defaultVectorSize)
//			if !same {
//				fmt.Println("src chunk")
//				kase.src.print()
//				fmt.Println("read chunk")
//				kase.read.print()
//			}
//		}
//
//		_ = deserial.Close()
//		return nil
//	}
//	err := runSerial(t, "serial-chunk", run)
//	assert.NoError(t, err)
//
//}
