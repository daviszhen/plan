package storage

import (
	"fmt"
	"io"
	"os"

	"github.com/daviszhen/plan/pkg/chunk"
	"github.com/daviszhen/plan/pkg/util"
)

var defWalog *WriteAheadLog
var defWalog2 *WriteAheadLog
var defLogPath string = defaultDbPath + ".wal"
var defLogPath2 string = defaultDbPath + "2.wal"

func init() {
	//var err error
	//defWalog, err = NewWriteAheadLog(defLogPath)
	//if err != nil {
	//	panic(fmt.Sprintf("init default.wal failed"))
	//}
	//defWalog2, err = NewWriteAheadLog(defLogPath2)
	//if err != nil {
	//	panic(fmt.Sprintf("init default2.wal failed"))
	//}
}

const (
	WAL_CREATE_TABLE  uint8 = 1
	WAL_CREATE_SCHEMA uint8 = 3
	WAL_USE_TABLE     uint8 = 25
	WAL_INSERT_TUPLE  uint8 = 26
	WAL_DELETE_TUPLE  uint8 = 27
	WAL_UPDATE_TUPLE  uint8 = 28
	WAL_CHECKPOINT    uint8 = 99
	WAL_FLUSH         uint8 = 100
)

func walType(walTyp uint8) string {
	switch walTyp {
	case WAL_CREATE_SCHEMA:
		return "WAL_CREATE_SCHEMA"
	case WAL_USE_TABLE:
		return "WAL_USE_TABLE"
	case WAL_INSERT_TUPLE:
		return "WAL_INSERT_TUPLE"
	case WAL_DELETE_TUPLE:
		return "WAL_DELETE_TUPLE"
	case WAL_UPDATE_TUPLE:
		return "WAL_UPDATE_TUPLE"
	case WAL_CHECKPOINT:
		return "WAL_CHECKPOINT"
	case WAL_FLUSH:
		return "WAL_FLUSH"
	default:
		return "unknown wal type"
	}
}

type WriteAheadLog struct {
	_path        string
	_writer      *BufferedFileWriter
	_skipWriting bool
}

func NewWriteAheadLog(path string) (*WriteAheadLog, error) {
	log := &WriteAheadLog{
		_path: path,
	}
	writer, err := NewBufferedFileWriter(path)
	if err != nil {
		return nil, err
	}
	log._writer = writer
	return log, nil
}

func (log *WriteAheadLog) Close() error {
	err := log._writer.Close()
	if err != nil {
		return err
	}
	return nil
}

func (log *WriteAheadLog) WriteSetTable(schema string, table string) error {
	if log._skipWriting {
		return nil
	}
	err := util.Write[uint8](WAL_USE_TABLE, log._writer)
	if err != nil {
		return err
	}
	err = util.WriteString(schema, log._writer)
	if err != nil {
		return err
	}
	err = util.WriteString(table, log._writer)
	if err != nil {
		return err
	}
	return err
}

func (log *WriteAheadLog) WriteInsert(data *chunk.Chunk) error {
	if log._skipWriting {
		return nil
	}
	err := util.Write[uint8](WAL_INSERT_TUPLE, log._writer)
	if err != nil {
		return err
	}
	return data.Serialize(log._writer)
}

func (log *WriteAheadLog) WriteDelete(data *chunk.Chunk) error {
	if log._skipWriting {
		return nil
	}
	err := util.Write[uint8](WAL_DELETE_TUPLE, log._writer)
	if err != nil {
		return err
	}
	return data.Serialize(log._writer)
}

func (log *WriteAheadLog) WriteUpdate(data *chunk.Chunk, colIdx []IdxType) error {
	if log._skipWriting {
		return nil
	}

	err := util.Write[uint8](WAL_UPDATE_TUPLE, log._writer)
	if err != nil {
		return err
	}
	err = util.Write[IdxType](IdxType(len(colIdx)), log._writer)
	if err != nil {
		return err
	}
	for _, idx := range colIdx {
		err = util.Write[IdxType](idx, log._writer)
		if err != nil {
			return err
		}
	}
	return data.Serialize(log._writer)
}

func (log *WriteAheadLog) Flush() error {
	if log._skipWriting {
		return nil
	}

	err := util.Write[uint8](WAL_FLUSH, log._writer)
	if err != nil {
		return err
	}
	return log._writer.Sync()
}

func (log *WriteAheadLog) Truncate(sz int64) error {
	return log._writer.Truncate(uint64(sz))
}

func (log *WriteAheadLog) Delete() error {
	if log._writer == nil {
		return nil
	}
	err := log._writer.Close()
	if err != nil {
		return err
	}
	log._writer = nil
	return os.Remove(log._path)
}

func (log *WriteAheadLog) GetWalSize() int64 {
	sz, _ := log._writer.GetFileSize()
	return sz
}

func (log *WriteAheadLog) WriteCheckpoint(block BlockID) error {
	err := util.Write[uint8](WAL_CHECKPOINT, log._writer)
	if err != nil {
		return err
	}
	err = util.Write[BlockID](block, log._writer)
	if err != nil {
		return err
	}
	return nil
}

func (log *WriteAheadLog) WriteCreateSchema(ent *CatalogEntry) error {
	if log._skipWriting {
		return nil
	}
	err := util.Write[uint8](WAL_CREATE_SCHEMA, log._writer)
	if err != nil {
		return err
	}
	return util.WriteString(ent._name, log._writer)
}

func (log *WriteAheadLog) WriteCreateTable(ent *CatalogEntry) error {
	if log._skipWriting {
		return nil
	}
	err := util.Write[uint8](WAL_CREATE_TABLE, log._writer)
	if err != nil {
		return err
	}
	return ent.Serialize(log._writer)
}

var _ util.Serialize = new(BufferedFileWriter)

type BufferedFileWriter struct {
	_path string
	_file *os.File
}

func NewBufferedFileWriter(path string) (*BufferedFileWriter, error) {
	var err error
	writer := &BufferedFileWriter{}
	writer._path = path

	//FIXME:test mode with trunc
	writer._file, err = os.OpenFile(path,
		os.O_CREATE|os.O_RDWR|os.O_APPEND, 0755)
	if err != nil {
		return nil, err
	}
	return writer, nil
}

func (writer *BufferedFileWriter) WriteData(buffer []byte, len int) error {
	w := 0
	for w < len {
		n, err := writer._file.Write(buffer[w:len])
		if err != nil {
			return err
		}
		w += n
	}
	return nil
}

func (writer *BufferedFileWriter) Flush() error {
	return nil
}

func (writer *BufferedFileWriter) Sync() error {
	err := writer.Flush()
	if err != nil {
		return err
	}
	return writer._file.Sync()
}

func (writer *BufferedFileWriter) Truncate(sz uint64) error {
	err := writer.Flush()
	if err != nil {
		return err
	}
	return writer._file.Truncate(int64(sz))
}

func (writer *BufferedFileWriter) GetFileSize() (int64, error) {
	stat, err := writer._file.Stat()
	if err != nil {
		return 0, err
	}
	return stat.Size(), nil
}

func (writer *BufferedFileWriter) Close() error {
	err := writer._file.Sync()
	if err != nil {
		return err
	}
	return writer._file.Close()
}

var _ util.Deserialize = new(BufferedFileReader)

type BufferedFileReader struct {
	_path string
	_file *os.File
}

func NewBufferedFileReader(path string) (*BufferedFileReader, error) {
	var err error
	reader := &BufferedFileReader{}
	reader._path = path

	reader._file, err = os.OpenFile(path, os.O_RDONLY, 0755)
	if err != nil {
		return nil, err
	}
	finfo, _ := reader._file.Stat()
	fmt.Println(path, finfo.Size())
	reader._file.Seek(0, io.SeekStart)
	return reader, nil
}

func (reader *BufferedFileReader) ReadData(buffer []byte, len int) error {
	r := 0
	for r < len {
		n, err := reader._file.Read(buffer[r:len])
		if err != nil {
			return err
		}
		r += n
	}
	return nil
}

func (reader *BufferedFileReader) Close() error {
	return reader._file.Close()
}
