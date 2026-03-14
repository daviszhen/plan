package storage2

import (
	"fmt"

	protobuf "google.golang.org/protobuf/proto"

	"github.com/daviszhen/plan/pkg/storage2/proto"
)

// BuildManifest produces the next manifest from current and the given transaction.
// Supports Append, Delete, Overwrite, UpdateConfig, Rewrite, Update, CreateIndex, and DataReplacement operations.
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
	case *storage2pb.Transaction_Rewrite_:
		return buildManifestRewrite(current, op.Rewrite)
	case *storage2pb.Transaction_Project_:
		return buildManifestProject(current, op.Project)
	case *storage2pb.Transaction_Merge_:
		return buildManifestMerge(current, op.Merge)
	case *storage2pb.Transaction_Clone_:
		return buildManifestClone(current, op.Clone)
	case *storage2pb.Transaction_Update_:
		return buildManifestUpdate(current, op.Update)
	case *storage2pb.Transaction_CreateIndex_:
		return buildManifestCreateIndex(current, op.CreateIndex)
	case *storage2pb.Transaction_DataReplacement_:
		return buildManifestDataReplacement(current, op.DataReplacement)
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

// buildManifestRewrite applies Rewrite operation (compaction) to the current manifest.
// It replaces old fragments with new compacted fragments.
func buildManifestRewrite(current *Manifest, rewriteOp *storage2pb.Transaction_Rewrite) (*Manifest, error) {
	next := protobuf.Clone(current).(*Manifest)
	next.Version = current.Version + 1

	// Create maps for quick lookup
	oldFragmentIDs := make(map[uint64]bool)
	for _, group := range rewriteOp.GetGroups() {
		for _, oldFrag := range group.GetOldFragments() {
			oldFragmentIDs[oldFrag.Id] = true
		}
	}

	// Filter out old fragments
	var remainingFragments []*DataFragment
	for _, frag := range current.Fragments {
		if !oldFragmentIDs[frag.Id] {
			remainingFragments = append(remainingFragments, frag)
		}
	}

	// Add new fragments
	var nextFragmentID uint64
	if current.MaxFragmentId != nil {
		nextFragmentID = uint64(*current.MaxFragmentId) + 1
	}

	for _, group := range rewriteOp.GetGroups() {
		for _, newFrag := range group.GetNewFragments() {
			cloned := protobuf.Clone(newFrag).(*DataFragment)
			cloned.Id = nextFragmentID
			remainingFragments = append(remainingFragments, cloned)
			nextFragmentID++
		}
	}

	next.Fragments = remainingFragments
	next.MaxFragmentId = ptrUint32(uint32(nextFragmentID - 1))

	return next, nil
}

// buildManifestProject applies a Project operation (drop columns) to the current manifest.
// It replaces the schema with the projected schema.
func buildManifestProject(current *Manifest, projectOp *storage2pb.Transaction_Project) (*Manifest, error) {
	next := protobuf.Clone(current).(*Manifest)
	next.Version = current.Version + 1

	// Replace schema with the projected schema
	next.Fields = projectOp.GetSchema()

	// Update fragments: remove field references for dropped columns
	projectedFieldIDs := make(map[int32]bool)
	for _, field := range projectOp.GetSchema() {
		projectedFieldIDs[field.Id] = true
	}

	for _, frag := range next.Fragments {
		var updatedFiles []*storage2pb.DataFile
		for _, file := range frag.Files {
			var newFields []int32
			for _, fieldID := range file.Fields {
				if projectedFieldIDs[fieldID] {
					newFields = append(newFields, fieldID)
				}
			}
			if len(newFields) > 0 {
				file.Fields = newFields
				updatedFiles = append(updatedFiles, file)
			}
		}
		frag.Files = updatedFiles
	}

	return next, nil
}

// buildManifestMerge applies a Merge operation (add/alter columns) to the current manifest.
// It updates the schema and fragments with the merged column data.
func buildManifestMerge(current *Manifest, mergeOp *storage2pb.Transaction_Merge) (*Manifest, error) {
	next := protobuf.Clone(current).(*Manifest)
	next.Version = current.Version + 1

	// Replace schema with the merged schema
	next.Fields = mergeOp.GetSchema()

	// Update schema metadata if provided
	if mergeOp.SchemaMetadata != nil {
		if next.SchemaMetadata == nil {
			next.SchemaMetadata = make(map[string][]byte)
		}
		for k, v := range mergeOp.SchemaMetadata {
			next.SchemaMetadata[k] = append([]byte(nil), v...)
		}
	}

	// Update fragments with new column data
	mergedFragments := mergeOp.GetFragments()
	mergedByID := make(map[uint64]*DataFragment)
	for _, f := range mergedFragments {
		mergedByID[f.Id] = f
	}

	for i, frag := range next.Fragments {
		if merged, ok := mergedByID[frag.Id]; ok {
			// Add new data files from the merged fragment
			for _, newFile := range merged.Files {
				next.Fragments[i].Files = append(next.Fragments[i].Files, protobuf.Clone(newFile).(*storage2pb.DataFile))
			}
		}
	}

	return next, nil
}

// buildManifestClone applies a Clone operation to the current manifest.
// For shallow clone, it copies manifest metadata and adds base paths for data reference.
func buildManifestClone(current *Manifest, cloneOp *storage2pb.Transaction_Clone) (*Manifest, error) {
	next := protobuf.Clone(current).(*Manifest)
	next.Version = current.Version + 1

	if cloneOp.GetIsShallow() {
		// Shallow clone: add base path for the source dataset
		refPath := cloneOp.GetRefPath()
		var nextBaseID uint32
		for _, bp := range next.BasePaths {
			if bp.Id >= nextBaseID {
				nextBaseID = bp.Id + 1
			}
		}

		newBasePath := &storage2pb.BasePath{
			Id:            nextBaseID,
			Path:          refPath,
			Name:          cloneOp.RefName,
			IsDatasetRoot: true,
		}
		next.BasePaths = append(next.BasePaths, newBasePath)

		// Update all fragments to reference the new base path
		for _, frag := range next.Fragments {
			for _, file := range frag.Files {
				baseID := nextBaseID
				file.BaseId = &baseID
			}
		}
	}

	return next, nil
}

func ptrUint32(u uint32) *uint32 { return &u }
