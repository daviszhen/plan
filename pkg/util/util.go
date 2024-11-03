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
	"fmt"
	"os"
	"runtime"
	"unsafe"
)

func AlignValue[T ~uint64](value, align T) T {
	return (value + (align - 1)) & ^(align - 1)
}

func PointerAdd(base unsafe.Pointer, offset int) unsafe.Pointer {
	return unsafe.Add(base, offset)
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
