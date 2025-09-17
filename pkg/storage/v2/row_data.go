package v2

import "bytes"

type T_PK []byte

func (pk T_PK) Less(other T_PK) bool {
	return bytes.Compare(pk, other) < 0
}

type T_COL []byte
type T_ROW []T_COL
