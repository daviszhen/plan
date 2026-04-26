// Data file layer using pkg/chunk: write/read Chunk via chunk.Serialize and chunk.Deserialize.
// File format is the same as produced by chunk.Chunk.Serialize (row count, column count, types, column data).
// See STORAGE2_DEVELOPMENT_PLAN.md section 4.1.

package storage2

import (
	"io"
	"os"
	"path/filepath"

	"github.com/daviszhen/plan/pkg/chunk"
)

// fileSerial implements util.Serialize by writing to an os.File.
type fileSerial struct {
	f *os.File
}

func (s *fileSerial) WriteData(buffer []byte, len int) error {
	n, err := s.f.Write(buffer[:len])
	if err != nil {
		return err
	}
	if n != len {
		return io.ErrShortWrite
	}
	return nil
}

func (s *fileSerial) Close() error {
	if s.f == nil {
		return nil
	}
	err := s.f.Close()
	s.f = nil
	return err
}

// fileDeserial implements util.Deserialize by reading from an os.File.
type fileDeserial struct {
	f *os.File
}

func (d *fileDeserial) ReadData(buffer []byte, len int) error {
	_, err := io.ReadFull(d.f, buffer[:len])
	return err
}

func (d *fileDeserial) Close() error {
	if d.f == nil {
		return nil
	}
	err := d.f.Close()
	d.f = nil
	return err
}

// WriteChunkToFile writes c to path using chunk's serialization format.
// The file layout is that of chunk.Chunk.Serialize (row count, column count, column types, column data).
func WriteChunkToFile(path string, c *chunk.Chunk) error {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	ser := &fileSerial{f: f}
	err = c.Serialize(ser)
	closeErr := ser.Close()
	if err != nil {
		return err
	}
	return closeErr
}

// ReadChunkFromFile reads a chunk from path (format produced by chunk.Chunk.Serialize).
// The returned chunk uses util.DefaultVectorSize for capacity.
func ReadChunkFromFile(path string) (*chunk.Chunk, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	des := &fileDeserial{f: f}
	c := &chunk.Chunk{}
	err = c.Deserialize(des)
	closeErr := des.Close()
	if err != nil {
		return nil, err
	}
	if closeErr != nil {
		return nil, closeErr
	}
	return c, nil
}
