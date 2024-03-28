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
	//assert.NoError(t, err)
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
