package main

import (
	"bytes"
	"encoding/json"
	"os"
	"testing"
)

func TestTpchQ2(t *testing.T) {
	q2 := tpchQ2()
	data, err := json.Marshal(q2)
	if err != nil {
		t.Fatal(err)
	}
	bb := &bytes.Buffer{}
	json.Indent(bb, data, "", "  ")
	bb.WriteTo(os.Stdout)
}
