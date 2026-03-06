package storage2

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLocalObjectStore(t *testing.T) {
	dir := t.TempDir()
	store := NewLocalObjectStore(dir)

	if err := store.MkdirAll("a/b"); err != nil {
		t.Fatal(err)
	}
	if err := store.Write("a/b/f.txt", []byte("hello")); err != nil {
		t.Fatal(err)
	}
	data, err := store.Read("a/b/f.txt")
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "hello" {
		t.Errorf("got %q", data)
	}
	names, err := store.List("a/b")
	if err != nil {
		t.Fatal(err)
	}
	if len(names) != 1 || names[0] != "f.txt" {
		t.Errorf("List: got %v", names)
	}
	if _, err := store.List("nonexistent"); err != nil {
		t.Errorf("List nonexistent should return nil,nil: %v", err)
	}
}

func TestLocalObjectStoreListMissingDir(t *testing.T) {
	dir := t.TempDir()
	store := NewLocalObjectStore(dir)
	names, err := store.List("missing")
	if err != nil {
		t.Fatal(err)
	}
	if names != nil {
		t.Errorf("expected nil names for missing dir, got %v", names)
	}
}

func TestLocalObjectStoreWriteCreatesDir(t *testing.T) {
	dir := t.TempDir()
	store := NewLocalObjectStore(dir)
	if err := store.Write("x/y/z.txt", []byte("data")); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "x/y/z.txt")); err != nil {
		t.Fatal(err)
	}
}
