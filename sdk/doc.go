// Package sdk provides a high-level API for the storage2 versioned table engine.
//
// Open or create a dataset with OpenDataset / CreateDataset, then use Dataset
// to Append, Delete, Overwrite, and read Version / CountRows. Data files use
// pkg/chunk: write with storage2.WriteChunkToFile, read with storage2.ReadChunkFromFile.
//
// Example:
//
//	dataset, err := sdk.CreateDataset(ctx, basePath).WithCommitHandler(handler).Build()
//	defer dataset.Close()
//	// ... write chunk to file, then:
//	dataFile := sdk.NewDataFile("data/0.dat", []int32{0, 1}, 1, 0)
//	fragment := sdk.NewDataFragmentWithRows(0, 100, []*sdk.DataFile{dataFile})
//	dataset.Append(ctx, []*sdk.DataFragment{fragment})
package sdk
