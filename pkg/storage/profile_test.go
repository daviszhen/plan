package storage

import (
	"os"
	"runtime"
	"runtime/debug"
	"runtime/pprof"
	"testing"
)

func TestProfileLoadDatabase(t *testing.T) {
	dbPath := os.Getenv("DB_PATH")
	if dbPath == "" {
		t.Skip("DB_PATH not set, skipping")
	}

	f, err := os.Create("/tmp/cpu_load.prof")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	mgr := NewStorageMgr(dbPath, false)
	GStorageMgr = mgr

	prev := debug.SetGCPercent(-1)
	pprof.StartCPUProfile(f)
	err = mgr.LoadDatabase()
	pprof.StopCPUProfile()
	debug.SetGCPercent(prev)
	runtime.GC()

	if err != nil {
		t.Fatal(err)
	}
	t.Log("profile written to /tmp/cpu_load.prof")
}
