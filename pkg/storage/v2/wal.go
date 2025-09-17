package v2

type WAL struct {
	_path string
}

func NewWAL(path string) *WAL {
	return &WAL{
		_path: path,
	}
}

func (wal *WAL) Close() {

}
