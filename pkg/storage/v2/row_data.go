package v2

import "bytes"

type T_COL []byte

func (col T_COL) Less(other T_COL) bool {
	return bytes.Compare(col, other) < 0
}

type T_ROW []T_COL
