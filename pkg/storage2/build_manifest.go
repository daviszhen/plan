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
	case *storage2pb.Transaction_UpdateConfig_:
		return buildManifestUpdateConfig(current, op.UpdateConfig)
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
	if overwriteOp.SchemaMetadata != nil {
		next.SchemaMetadata = make(map[string][]byte)
		for k, v := range overwriteOp.SchemaMetadata {
			next.SchemaMetadata[k] = append([]byte(nil), v...)
		}
	}
	if len(overwriteOp.GetInitialBases()) > 0 {
		next.BasePaths = append([]*storage2pb.BasePath{}, overwriteOp.GetInitialBases()...)
	} else if current != nil && len(current.BasePaths) > 0 {
		next.BasePaths = append([]*storage2pb.BasePath{}, current.BasePaths...)
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

// buildManifestUpdateConfig applies UpdateConfig to the current manifest.
// For now we support only the deprecated upsert_values/delete_keys fields
// to update Manifest.Config, ignoring table/schema/field metadata.
func buildManifestUpdateConfig(current *Manifest, update *storage2pb.Transaction_UpdateConfig) (*Manifest, error) {
	if update == nil {
		return nil, fmt.Errorf("update config is nil")
	}
	next := protobuf.Clone(current).(*Manifest)
	next.Version = current.Version + 1
	if next.Config == nil {
		next.Config = make(map[string]string)
	}
	// Apply upserts.
	for k, v := range update.GetUpsertValues() {
		next.Config[k] = v
	}
	// Apply deletes.
	for _, k := range update.GetDeleteKeys() {
		delete(next.Config, k)
	}

	// Apply new-style config updates.
	applyUpdateMapToStringMap(next.Config, update.GetConfigUpdates())

	// Apply table metadata updates.
	if next.TableMetadata == nil {
		next.TableMetadata = make(map[string]string)
	}
	applyUpdateMapToStringMap(next.TableMetadata, update.GetTableMetadataUpdates())

	// Apply schema metadata updates (legacy schema_metadata map merged with updates).
	if next.SchemaMetadata == nil {
		next.SchemaMetadata = make(map[string][]byte)
	}
	// First merge legacy schema_metadata string map if present.
	for k, v := range update.GetSchemaMetadata() {
		next.SchemaMetadata[k] = []byte(v)
	}
	// Then apply UpdateMap-style schema metadata updates.
	if smUpdates := update.GetSchemaMetadataUpdates(); smUpdates != nil {
		entries := smUpdates.GetUpdateEntries()
		if smUpdates.GetReplace() {
			// Replace entire map.
			next.SchemaMetadata = make(map[string][]byte)
		}
		for _, e := range entries {
			key := e.GetKey()
			if e.Value == nil {
				delete(next.SchemaMetadata, key)
				continue
			}
			next.SchemaMetadata[key] = []byte(e.GetValue())
		}
	}

	return next, nil
}

// applyUpdateMapToStringMap applies an UpdateMap to a map[string]string.
// If upd.Replace is true, the target map is replaced entirely by entries
// whose value is non-nil. If false, entries with non-nil value upsert keys,
// and entries with nil value delete keys.
func applyUpdateMapToStringMap(target map[string]string, upd *storage2pb.Transaction_UpdateMap) {
	if upd == nil {
		return
	}
	entries := upd.GetUpdateEntries()
	if upd.GetReplace() {
		for k := range target {
			delete(target, k)
		}
	}
	for _, e := range entries {
		key := e.GetKey()
		if e.Value == nil {
			delete(target, key)
			continue
		}
		target[key] = e.GetValue()
	}
}

func ptrUint32(u uint32) *uint32 { return &u }
