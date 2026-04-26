package storage2

import "testing"

func TestTransactionRoundTrip(t *testing.T) {
	txn := NewTransactionAppend(1, "test-uuid", []*DataFragment{
		NewDataFragment(0, []*DataFile{NewDataFile("f.parquet", []int32{0, 1}, 1, 0)}),
	})
	data, err := MarshalTransaction(txn)
	if err != nil {
		t.Fatal(err)
	}
	txn2, err := UnmarshalTransaction(data)
	if err != nil {
		t.Fatal(err)
	}
	if txn2.ReadVersion != txn.ReadVersion || txn2.GetAppend() == nil {
		t.Errorf("round trip lost data")
	}
}
