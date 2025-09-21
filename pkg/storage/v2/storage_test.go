package v2

import (
	"fmt"
	"testing"

	"github.com/tidwall/btree"
)

func Test_Btree(t *testing.T) {
	db := btree.NewBTreeGOptions(func(a, b int) bool {
		return a < b
	}, btree.Options{
		Degree:  10,
		NoLocks: true,
	})
	db.Set(1)
	db.Set(2)
	db.Set(3)
	db.Set(4)
	db.Set(5)

	db.Set(7)
	db.Set(8)

	db.AscendMut(5,
		func(item int) bool {
			fmt.Println(item)
			if item <= 5 {
				return true
			} else if item > 7 {
				db.Set(6)
				return false
			}
			return true
		})
}
