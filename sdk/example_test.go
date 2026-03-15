package sdk_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/daviszhen/plan/pkg/chunk"
	"github.com/daviszhen/plan/pkg/common"
	"github.com/daviszhen/plan/pkg/storage2"
	"github.com/daviszhen/plan/pkg/util"
	"github.com/daviszhen/plan/sdk"
)

// Example_createAndAppend demonstrates creating a dataset and appending a fragment (data file uses pkg/chunk).
func Example_createAndAppend() {
	ctx := context.Background()
	dir, _ := os.MkdirTemp("", "sdk_example_")
	defer os.RemoveAll(dir)
	basePath := filepath.Join(dir, "dataset")
	_ = os.MkdirAll(basePath, 0755)

	handler := sdk.NewLocalRenameCommitHandler()
	dataset, err := sdk.CreateDataset(ctx, basePath).WithCommitHandler(handler).Build()
	if err != nil {
		fmt.Println("create failed:", err)
		return
	}
	defer dataset.Close()

	dataPath := filepath.Join(basePath, "data", "0.dat")
	numRows := 100
	typs := []common.LType{
		common.MakeLType(common.LTID_INTEGER),
		common.MakeLType(common.LTID_BIGINT),
	}
	c := &chunk.Chunk{}
	c.Init(typs, util.DefaultVectorSize)
	c.SetCard(numRows)
	for i := 0; i < numRows; i++ {
		c.Data[0].SetValue(i, &chunk.Value{Typ: typs[0], I64: int64(i)})
		c.Data[1].SetValue(i, &chunk.Value{Typ: typs[1], I64: int64(i * 100)})
	}
	if err := storage2.WriteChunkToFile(dataPath, c); err != nil {
		fmt.Println("write chunk failed:", err)
		return
	}

	dataFile := sdk.NewDataFile("data/0.dat", []int32{0, 1}, 1, 0)
	fragment := sdk.NewDataFragmentWithRows(0, uint64(numRows), []*sdk.DataFile{dataFile})
	if err := dataset.Append(ctx, []*sdk.DataFragment{fragment}); err != nil {
		fmt.Println("append failed:", err)
		return
	}

	version := dataset.Version()
	count, _ := dataset.CountRows()
	fmt.Printf("Version: %d, Rows: %d\n", version, count)
	// Output: Version: 1, Rows: 100
}
