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
	"os"
	"unsafe"
)

func Write[T any](value T, serial Serialize) error {
	cnt := int(unsafe.Sizeof(value))
	buf := pointerToSlice[byte](unsafe.Pointer(&value), cnt)
	return serial.WriteData(buf, cnt)
}

func WriteString(val String, serial Serialize) error {
	err := Write[uint32](uint32(val.len()), serial)
	if err != nil {
		return err
	}

	if val.len() > 0 {
		err = serial.WriteData(val.dataSlice(), val.len())
		if err != nil {
			return err
		}
	}
	return err
}

func Read[T any](value *T, deserial Deserialize) error {
	cnt := int(unsafe.Sizeof(*value))
	buf := pointerToSlice[byte](unsafe.Pointer(value), cnt)
	err := deserial.ReadData(buf, cnt)
	if err != nil {
		return err
	}
	return nil
}

func ReadString(val *String, deserial Deserialize) error {
	var len uint32
	err := Read[uint32](&len, deserial)
	if err != nil {
		return err
	}
	if len > 0 {
		val._data = cMalloc(int(len))
		val._len = int(len)
		err = deserial.ReadData(val.dataSlice(), val.len())
		if err != nil {
			return err
		}
	}
	return err
}

var _ Serialize = new(FileSerialize)

type FileSerialize struct {
	file *os.File
}

func (serial *FileSerialize) Close() error {
	_ = serial.file.Sync()
	_ = serial.file.Close()
	return nil
}

func NewFileSerialize(name string) (*FileSerialize, error) {
	var err error
	ret := &FileSerialize{}
	ret.file, err = os.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_SYNC, 0775)
	if err != nil {
		return nil, err
	}
	return ret, nil
}

func (serial *FileSerialize) WriteData(buffer []byte, len int) error {
	var wlen int
	var n int
	var err error
	for wlen < len {
		n, err = serial.file.Write(buffer[wlen:len])
		if err != nil {
			return err
		}
		wlen += n
	}
	return nil
}

var _ Deserialize = new(FileDeserialize)

type FileDeserialize struct {
	file *os.File
}

func NewFileDeserialize(name string) (*FileDeserialize, error) {
	var err error
	ret := &FileDeserialize{}
	ret.file, err = os.OpenFile(name, os.O_RDONLY|os.O_CREATE|os.O_SYNC, 0775)
	if err != nil {
		return nil, err
	}
	return ret, nil
}

func (deserial *FileDeserialize) ReadData(buffer []byte, len int) error {
	var rlen int
	var n int
	var err error
	for rlen < len {
		n, err = deserial.file.Read(buffer[rlen:len])
		if err != nil {
			return err
		}
		rlen += n
	}
	return nil
}

func (deserial *FileDeserialize) Close() error {
	_ = deserial.file.Close()
	return nil
}
