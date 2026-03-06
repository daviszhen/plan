package storage2

import (
	"github.com/daviszhen/plan/pkg/storage2/proto"
)

// DataFragment is a logical data block (id, files, deletion_file, physical_rows, etc.).
type DataFragment = storage2pb.DataFragment

// DataFile is physical file metadata (path, fields, column_indices, version, size, base_id).
type DataFile = storage2pb.DataFile

// DeletionFile describes deletion vectors for a fragment.
type DeletionFile = storage2pb.DeletionFile

// NewDataFragment creates a fragment with the given id and data files.
func NewDataFragment(id uint64, files []*DataFile) *DataFragment {
	if files == nil {
		files = []*DataFile{}
	}
	return &DataFragment{
		Id:    id,
		Files: files,
	}
}

// NewDataFile creates a data file record with path and field ids.
func NewDataFile(path string, fields []int32, fileMajorVersion, fileMinorVersion uint32) *DataFile {
	if fields == nil {
		fields = []int32{}
	}
	return &DataFile{
		Path:              path,
		Fields:            fields,
		FileMajorVersion:  fileMajorVersion,
		FileMinorVersion:  fileMinorVersion,
	}
}
