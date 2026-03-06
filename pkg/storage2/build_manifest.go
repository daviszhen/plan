package storage2

import (
	"fmt"

	protobuf "google.golang.org/protobuf/proto"

	"github.com/daviszhen/plan/pkg/storage2/proto"
)

// BuildManifest produces the next manifest from current and the given transaction.
// Supports Append, Delete, and Overwrite operations.
func BuildManifest(current *Manifest, txn *Transaction) (*Manifest, error) {
	if current == nil {
		return nil, fmt.Errorf("current manifest is nil")
	}
	if txn == nil {
		return nil, fmt.Errorf("transaction is nil")
	}
	if txn.ReadVersion != current.Version {
		return nil, fmt.Errorf("transaction read_version %d does not match current version %d", txn.ReadVersion, current.Version)
	}

	switch op := txn.Operation.(type) {
	case *storage2pb.Transaction_Append_:
		return buildManifestAppend(current, op.Append)
	case *storage2pb.Transaction_Delete_:
		return buildManifestDelete(current, op.Delete)
	case *storage2pb.Transaction_Overwrite_:
		return buildManifestOverwrite(current, op.Overwrite)
	default:
		return nil, fmt.Errorf("unsupported operation type %T", txn.Operation)
	}
}

func buildManifestAppend(current *Manifest, appendOp *storage2pb.Transaction_Append) (*Manifest, error) {
	next := protobuf.Clone(current).(*Manifest)
	next.Version = current.Version + 1

	existing := current.Fragments
	newFrags := appendOp.GetFragments()
	if newFrags == nil {
		newFrags = []*DataFragment{}
	}

	// Assign fragment IDs to new fragments (Lance: start from max_fragment_id+1).
	var nextID uint64
	if current.MaxFragmentId != nil {
		nextID = uint64(*current.MaxFragmentId) + 1
	}
	clonedNew := make([]*DataFragment, len(newFrags))
	for i, f := range newFrags {
		clonedNew[i] = protobuf.Clone(f).(*DataFragment)
		clonedNew[i].Id = nextID
		nextID++
	}

	next.Fragments = append(append([]*DataFragment{}, existing...), clonedNew...)
	next.MaxFragmentId = ptrUint32(uint32(nextID - 1))
	if len(clonedNew) == 0 {
		next.MaxFragmentId = current.MaxFragmentId
	}
	return next, nil
}

func buildManifestDelete(current *Manifest, deleteOp *storage2pb.Transaction_Delete) (*Manifest, error) {
	next := protobuf.Clone(current).(*Manifest)
	next.Version = current.Version + 1

	updatedByID := make(map[uint64]*DataFragment)
	for _, f := range deleteOp.GetUpdatedFragments() {
		updatedByID[f.Id] = f
	}
	deletedSet := make(map[uint64]struct{})
	for _, id := range deleteOp.GetDeletedFragmentIds() {
		deletedSet[id] = struct{}{}
	}

	var out []*DataFragment
	for _, f := range current.Fragments {
		if _, ok := deletedSet[f.Id]; ok {
			continue
		}
		if u := updatedByID[f.Id]; u != nil {
			out = append(out, protobuf.Clone(u).(*DataFragment))
			continue
		}
		out = append(out, protobuf.Clone(f).(*DataFragment))
	}
	next.Fragments = out
	return next, nil
}

func buildManifestOverwrite(current *Manifest, overwriteOp *storage2pb.Transaction_Overwrite) (*Manifest, error) {
	next := &Manifest{
		Version:   current.Version + 1,
		Fragments: []*DataFragment{},
		Fields:    overwriteOp.GetSchema(),
		Config:    overwriteOp.GetConfigUpsertValues(),
	}
	if next.Config == nil {
		next.Config = make(map[string]string)
	}
	// Copy schema_metadata if present.
	if overwriteOp.SchemaMetadata != nil {
		next.SchemaMetadata = make(map[string][]byte)
		for k, v := range overwriteOp.SchemaMetadata {
			next.SchemaMetadata[k] = append([]byte(nil), v...)
		}
	}

	frags := overwriteOp.GetFragments()
	if frags == nil {
		frags = []*DataFragment{}
	}
	// Assign fragment IDs 0, 1, ...
	for i, f := range frags {
		cloned := protobuf.Clone(f).(*DataFragment)
		cloned.Id = uint64(i)
		next.Fragments = append(next.Fragments, cloned)
	}
	if len(next.Fragments) > 0 {
		next.MaxFragmentId = ptrUint32(uint32(len(next.Fragments) - 1))
	}
	return next, nil
}

func ptrUint32(u uint32) *uint32 { return &u }
