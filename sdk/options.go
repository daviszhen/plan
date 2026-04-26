package sdk

// ReadOptions holds options for opening a dataset (e.g. version, block size).
type ReadOptions struct {
	Version   *uint64 // version to open; nil means latest
	BlockSize *int    // I/O block size in bytes (reserved)
}

// ReadOption is a functional option for ReadOptions.
type ReadOption func(*ReadOptions)

// WithVersion sets the manifest version to open.
func WithVersion(v uint64) ReadOption {
	return func(opts *ReadOptions) {
		opts.Version = &v
	}
}

// WithBlockSize sets the I/O block size (reserved for future use).
func WithBlockSize(size int) ReadOption {
	return func(opts *ReadOptions) {
		opts.BlockSize = &size
	}
}

// WriteMode is the mode for write operations.
type WriteMode int

const (
	WriteModeCreate WriteMode = iota
	WriteModeAppend
	WriteModeOverwrite
)

// WriteOptions holds options for write operations (reserved for future use).
type WriteOptions struct {
	MaxRowsPerFile  *uint64
	MaxBytesPerFile *uint64
	Mode            WriteMode
}
