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

package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"testing"
)

type DedupSet map[string]bool

func (set DedupSet) insert(s string) {
	set[s] = true
}

func (set DedupSet) count() int {
	return len(set)
}

func analyze(path string) error {
	file, err := os.OpenFile(path, os.O_RDONLY, 0755)
	if err != nil {
		return err
	}
	defer file.Close()
	reader := csv.NewReader(file)
	reader.Comma = '|'
	lines := uint64(0)

	var cols []DedupSet

	for {
		records, err := reader.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		if len(cols) == 0 {
			cols = make([]DedupSet, len(records)*2)
			for i := range cols {
				cols[i] = make(DedupSet)
			}
		}

		for i, record := range records {
			cols[i].insert(record)
		}

		lines++
	}

	fmt.Println(path)
	fmt.Println("lines", lines)
	for i, col := range cols {
		fmt.Println("col", i, col.count())
	}
	return nil
}

func Test_analyzeTpch1g(t *testing.T) {
	//path := "/Users/pengzhen/Documents/GitHub/mo-tpch/data/1/customer.tbl"
	//err := analyze(path)
	//assertFunc.NoError(t, err)
}

func TestT1(t *testing.T) {
	var x ColumnBind

	dfun := func(x ColumnBind) {
		x[0] = 1
		x[1] = 2
		fmt.Println(x)
	}
	dfun2 := func(x *ColumnBind) {
		x[0] = 1
		x[1] = 2
		fmt.Println(x)
	}
	dfun(x)
	fmt.Println(x)
	dfun2(&x)
	fmt.Println(x)
}
