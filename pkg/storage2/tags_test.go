package storage2

import (
	"context"
	"testing"
)

func TestListTagsAndResolveTagVersion(t *testing.T) {
	ctx := context.Background()
	basePath := t.TempDir()
	handler := NewLocalRenameCommitHandler()

	// version 0: empty manifest without tag
	m0 := NewManifest(0)
	m0.Fragments = []*DataFragment{}
	if err := handler.Commit(ctx, basePath, 0, m0); err != nil {
		t.Fatal(err)
	}

	// v1: append with tag "v1"
	txn1 := NewTransactionAppend(0, "txn-v1", []*DataFragment{
		NewDataFragment(0, nil),
	})
	txn1.Tag = "v1"
	if err := CommitTransaction(ctx, basePath, handler, txn1); err != nil {
		t.Fatal(err)
	}

	// v2: append with tag "v2"
	txn2 := NewTransactionAppend(1, "txn-v2", []*DataFragment{
		NewDataFragment(1, nil),
	})
	txn2.Tag = "v2"
	if err := CommitTransaction(ctx, basePath, handler, txn2); err != nil {
		t.Fatal(err)
	}

	// v3: another append also tagged "v1" (later version)
	txn3 := NewTransactionAppend(2, "txn-v1b", []*DataFragment{
		NewDataFragment(2, nil),
	})
	txn3.Tag = "v1"
	if err := CommitTransaction(ctx, basePath, handler, txn3); err != nil {
		t.Fatal(err)
	}

	tags, err := ListTags(ctx, basePath, handler)
	if err != nil {
		t.Fatal(err)
	}
	if len(tags["v1"]) != 2 || tags["v1"][0] != 1 || tags["v1"][1] != 3 {
		t.Errorf("v1 versions = %v, want [1 3]", tags["v1"])
	}
	if len(tags["v2"]) != 1 || tags["v2"][0] != 2 {
		t.Errorf("v2 versions = %v, want [2]", tags["v2"])
	}

	v, ok, err := ResolveTagVersion(ctx, basePath, handler, "v1")
	if err != nil {
		t.Fatal(err)
	}
	if !ok || v != 3 {
		t.Errorf("ResolveTagVersion(v1) want (3,true) got (%d,%v)", v, ok)
	}

	v2, ok2, err := ResolveTagVersion(ctx, basePath, handler, "v2")
	if err != nil {
		t.Fatal(err)
	}
	if !ok2 || v2 != 2 {
		t.Errorf("ResolveTagVersion(v2) want (2,true) got (%d,%v)", v2, ok2)
	}

	vMissing, okMissing, err := ResolveTagVersion(ctx, basePath, handler, "missing-tag")
	if err != nil {
		t.Fatal(err)
	}
	if okMissing || vMissing != 0 {
		t.Errorf("ResolveTagVersion(missing) want (0,false) got (%d,%v)", vMissing, okMissing)
	}
}

