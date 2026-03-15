package storage2

import (
	"fmt"
	"path/filepath"
	"testing"
)

func TestManifestPath(t *testing.T) {
	p := ManifestPath(1)
	if p != filepath.Join(VersionsDir, "1.manifest") {
		t.Errorf("ManifestPath(1) = %s", p)
	}
}

func TestTransactionPath(t *testing.T) {
	p := TransactionPath(1, "abc-def")
	if p != filepath.Join(TransactionsDir, "1-abc-def.txn") {
		t.Errorf("TransactionPath = %s", p)
	}
}

func TestParseVersion(t *testing.T) {
	v, err := ParseVersion("3.manifest")
	if err != nil || v != 3 {
		t.Errorf("ParseVersion(3.manifest) = %d, %v", v, err)
	}
	_, err = ParseVersion("x.manifest")
	if err == nil {
		t.Fatal("expected error for invalid version")
	}
	_, err = ParseVersion("1.txt")
	if err == nil {
		t.Fatal("expected error for wrong extension")
	}
}

func TestManifestNamingScheme(t *testing.T) {
	// V1: _versions/{version}.manifest
	v1Path := ManifestPathV1(42)
	if v1Path != filepath.Join(VersionsDir, "42.manifest") {
		t.Errorf("V1 path: got %q want %q", v1Path, filepath.Join(VersionsDir, "42.manifest"))
	}

	// V2: inverted version numbers (ManifestNamingV2Max - version), 20-digit zero-padded
	v2Path := ManifestPathV2(42)
	inverted := ManifestNamingV2Max - 42
	expected := filepath.Join(VersionsDir, fmt.Sprintf("%020d.manifest", inverted))
	if v2Path != expected {
		t.Errorf("V2 path: got %q want %q", v2Path, expected)
	}

	// Parse V1
	v, scheme, err := ParseVersionEx("42.manifest")
	if err != nil {
		t.Fatalf("ParseVersionEx V1: %v", err)
	}
	if v != 42 || scheme != ManifestNamingV1 {
		t.Errorf("ParseVersionEx V1: got v=%d scheme=%v", v, scheme)
	}

	// Parse V2: use the inverted filename
	v2Filename := fmt.Sprintf("%020d.manifest", ManifestNamingV2Max-42)
	v, scheme, err = ParseVersionEx(v2Filename)
	if err != nil {
		t.Fatalf("ParseVersionEx V2: %v", err)
	}
	if v != 42 || scheme != ManifestNamingV2 {
		t.Errorf("ParseVersionEx V2: got v=%d scheme=%v", v, scheme)
	}

	// ManifestPath should default to V1 for backward compatibility
	defaultPath := ManifestPath(42)
	if defaultPath != v1Path {
		t.Errorf("ManifestPath default should be V1: got %q", defaultPath)
	}

	// V2 roundtrip: ManifestPathV2 -> extract filename -> ParseVersionEx
	v2Full := ManifestPathV2(100)
	v2Base := filepath.Base(v2Full)
	vParsed, schemeParsed, err := ParseVersionEx(v2Base)
	if err != nil {
		t.Fatalf("V2 roundtrip parse: %v", err)
	}
	if vParsed != 100 || schemeParsed != ManifestNamingV2 {
		t.Errorf("V2 roundtrip: got v=%d scheme=%v", vParsed, schemeParsed)
	}
}
