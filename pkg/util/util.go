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

package util

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"runtime"
	"unsafe"
)

func AlignValue[T ~uint64 | ~uint32 | ~uint16](value, align T) T {
	return (value + (align - 1)) & ^(align - 1)
}

func AlignValue8(value int) int {
	return (value + 7) & (^7)
}

func AssertFunc(b bool) {
	if !b {
		panic("assertion failed")
	}
}

type Pair[K any, V any] struct {
	First  K
	Second V
}

func FileIsValid(path string) bool {
	stat, err := os.Stat(path)
	if err != nil {
		return false
	}
	return !stat.IsDir()
}

func ConvertPanicError(v interface{}) error {
	return fmt.Errorf("panic %v: %+v", v, Callers(3))
}

type Stack []uintptr

// Callers makes the depth customizable.
func Callers(depth int) *Stack {
	const numFrames = 32
	var pcs [numFrames]uintptr
	n := runtime.Callers(2+depth, pcs[:])
	var st Stack = pcs[0:n]
	return &st
}

func NextPowerOfTwo(v uint64) uint64 {
	v--
	v |= v >> 1
	v |= v >> 2
	v |= v >> 4
	v |= v >> 8
	v |= v >> 16
	v |= v >> 32
	v++
	return v
}

func IsPowerOfTwo(v uint64) bool {
	return (v & (v - 1)) == 0
}

func GreaterFloat[T ~float32 | ~float64](lhs, rhs T) bool {
	lIsNan := math.IsNaN(float64(lhs))
	rIsNan := math.IsNaN(float64(rhs))
	if rIsNan {
		return false
	}
	if lIsNan {
		return true
	}
	return lhs > rhs
}

func ToJson(root any, path string) error {
	data, err := json.Marshal(root)
	if err != nil {
		return err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0755)
	if err != nil {
		return err
	}
	defer file.Close()
	_, err = file.Write(data)
	if err != nil {
		return err
	}
	return nil
}

type Serialize interface {
	WriteData(buffer []byte, len int) error
	Close() error
}

type Deserialize interface {
	ReadData(buffer []byte, len int) error
	Close() error
}

const (
	DefaultVectorSize = 2048
)

func UnsafeStringToBytes(s string) []byte {
	return unsafe.Slice(unsafe.StringData(s), len(s))
}

func Abs[T int32](val T) T {
	if val > 0 {
		return val
	}
	return -val
}

func FlagIsSet[T uint8 | uint16 | uint32 | uint64](val, flag T) bool {
	return (val & flag) != 0
}
