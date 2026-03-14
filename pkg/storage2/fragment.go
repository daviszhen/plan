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

// NewDataFragmentWithRows creates a fragment with id, physical row count, and optional data files.
func NewDataFragmentWithRows(id, physicalRows uint64, files []*DataFile) *DataFragment {
	f := NewDataFragment(id, files)
	f.PhysicalRows = physicalRows
	return f
}

// NewDataFile creates a data file record with path and field ids.
func NewDataFile(path string, fields []int32, fileMajorVersion, fileMinorVersion uint32) *DataFile {
	if fields == nil {
		fields = []int32{}
	}
	return &DataFile{
		Path:             path,
		Fields:           fields,
		FileMajorVersion: fileMajorVersion,
		FileMinorVersion: fileMinorVersion,
	}
}

// BasePath is the proto BasePath (id, path, name, is_dataset_root).
type BasePath = storage2pb.BasePath

// NewBasePath creates a BasePath for multi-base or shallow clone.
func NewBasePath(id uint32, path string, name *string, isDatasetRoot bool) *BasePath {
	return &BasePath{
		Id:            id,
		Path:          path,
		Name:          name,
		IsDatasetRoot: isDatasetRoot,
	}
}

// NewDeletionFile creates a DeletionFile (file_type, read_version, id, num_deleted_rows).
func NewDeletionFile(fileType storage2pb.DeletionFile_DeletionFileType, readVersion, id, numDeletedRows uint64) *DeletionFile {
	return &DeletionFile{
		FileType:       fileType,
		ReadVersion:    readVersion,
		Id:             id,
		NumDeletedRows: numDeletedRows,
	}
}
