package storage2

import (
	"testing"
)

func TestHeaderRoundTrip(t *testing.T) {
	var buf [headerSize]byte
	WriteHeader(buf[:], 100, 3)
	numRows, numCols, err := ReadHeader(buf[:])
	if err != nil {
		t.Fatal(err)
	}
	if numRows != 100 || numCols != 3 {
		t.Errorf("got %d rows %d cols", numRows, numCols)
	}
}

func TestHeaderInvalidMagic(t *testing.T) {
	var buf [headerSize]byte
	WriteHeader(buf[:], 1, 1)
	buf[0] = 'x'
	_, _, err := ReadHeader(buf[:])
	if err == nil {
		t.Fatal("expected error for bad magic")
	}
}

func TestFooterRoundTrip(t *testing.T) {
	lengths := []uint64{40, 80, 120}
	footerLen := FooterSize(uint32(len(lengths)))
	footer := make([]byte, footerLen)
	WriteFooter(footer, lengths)
	out, err := ReadFooter(footer, uint32(len(lengths)))
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 3 || out[0] != 40 || out[1] != 80 || out[2] != 120 {
		t.Errorf("got %v", out)
	}
}

func TestColumnTypeFixedSize(t *testing.T) {
	if ColTypeInt32.FixedSize() != 4 || ColTypeInt64.FixedSize() != 8 {
		t.Error("fixed sizes wrong")
	}
	if ColTypeBytes.FixedSize() != 0 {
		t.Error("Bytes should be variable")
	}
}
