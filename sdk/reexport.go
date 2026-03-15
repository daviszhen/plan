package sdk

import (
	"github.com/daviszhen/plan/pkg/storage2"
)

// ErrConflict is returned when a transaction conflicts with an already committed one.
var ErrConflict = storage2.ErrConflict

// CommitHandler is the same as storage2.CommitHandler.
type CommitHandler = storage2.CommitHandler

// DataFragment is the same as storage2.DataFragment.
type DataFragment = storage2.DataFragment

// DataFile is the same as storage2.DataFile.
type DataFile = storage2.DataFile

// NewLocalRenameCommitHandler returns a CommitHandler for local filesystem.
func NewLocalRenameCommitHandler() *storage2.LocalRenameCommitHandler {
	return storage2.NewLocalRenameCommitHandler()
}

// NewDataFile constructs a DataFile (path relative to dataset base, field IDs, major/minor version).
func NewDataFile(path string, fields []int32, fileMajorVersion, fileMinorVersion uint32) *DataFile {
	return storage2.NewDataFile(path, fields, fileMajorVersion, fileMinorVersion)
}

// NewDataFragmentWithRows constructs a DataFragment with the given id, physical row count, and files.
func NewDataFragmentWithRows(id, physicalRows uint64, files []*DataFile) *DataFragment {
	return storage2.NewDataFragmentWithRows(id, physicalRows, files)
}
