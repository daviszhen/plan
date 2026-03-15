package storage2

import (
	"fmt"

	protobuf "google.golang.org/protobuf/proto"

	"github.com/daviszhen/plan/pkg/storage2/proto"
)

// Transaction is the write transaction (read_version, uuid, operation).
type Transaction = storage2pb.Transaction

// MarshalTransaction serializes the transaction to Proto binary (.txn format).
func MarshalTransaction(t *Transaction) ([]byte, error) {
	if t == nil {
		return nil, fmt.Errorf("transaction is nil")
	}
	return protobuf.Marshal(t)
}

// UnmarshalTransaction deserializes the transaction from Proto binary.
func UnmarshalTransaction(data []byte) (*Transaction, error) {
	t := new(Transaction)
	if err := protobuf.Unmarshal(data, t); err != nil {
		return nil, err
	}
	return t, nil
}

// NewTransactionAppend builds a transaction with Append operation.
func NewTransactionAppend(readVersion uint64, uuid string, fragments []*DataFragment) *Transaction {
	if fragments == nil {
		fragments = []*DataFragment{}
	}
	return &Transaction{
		ReadVersion: readVersion,
		Uuid:        uuid,
		Operation:   &storage2pb.Transaction_Append_{Append: &storage2pb.Transaction_Append{Fragments: fragments}},
	}
}

// NewTransactionDelete builds a transaction with Delete operation.
func NewTransactionDelete(readVersion uint64, uuid string, updatedFragments []*DataFragment, deletedFragmentIds []uint64, predicate string) *Transaction {
	if updatedFragments == nil {
		updatedFragments = []*DataFragment{}
	}
	if deletedFragmentIds == nil {
		deletedFragmentIds = []uint64{}
	}
	return &Transaction{
		ReadVersion: readVersion,
		Uuid:        uuid,
		Operation: &storage2pb.Transaction_Delete_{Delete: &storage2pb.Transaction_Delete{
			UpdatedFragments:   updatedFragments,
			DeletedFragmentIds: deletedFragmentIds,
			Predicate:          predicate,
		}},
	}
}

// NewTransactionOverwrite builds a transaction with Overwrite operation (create or replace table).
func NewTransactionOverwrite(readVersion uint64, uuid string, fragments []*DataFragment, schema []*storage2pb.Field, configUpsert map[string]string) *Transaction {
	if fragments == nil {
		fragments = []*DataFragment{}
	}
	if schema == nil {
		schema = []*storage2pb.Field{}
	}
	return &Transaction{
		ReadVersion: readVersion,
		Uuid:        uuid,
		Operation: &storage2pb.Transaction_Overwrite_{Overwrite: &storage2pb.Transaction_Overwrite{
			Fragments:          fragments,
			Schema:             schema,
			ConfigUpsertValues: configUpsert,
		}},
	}
}

// NewTransactionProject builds a transaction with Project operation (drop columns).
// The projected schema contains only the columns to keep.
func NewTransactionProject(readVersion uint64, uuid string, schema []*storage2pb.Field) *Transaction {
	if schema == nil {
		schema = []*storage2pb.Field{}
	}
	return &Transaction{
		ReadVersion: readVersion,
		Uuid:        uuid,
		Operation: &storage2pb.Transaction_Project_{Project: &storage2pb.Transaction_Project{
			Schema: schema,
		}},
	}
}

// NewTransactionMerge builds a transaction with Merge operation (add/alter columns).
func NewTransactionMerge(readVersion uint64, uuid string, fragments []*DataFragment, schema []*storage2pb.Field, schemaMetadata map[string][]byte) *Transaction {
	if fragments == nil {
		fragments = []*DataFragment{}
	}
	if schema == nil {
		schema = []*storage2pb.Field{}
	}
	return &Transaction{
		ReadVersion: readVersion,
		Uuid:        uuid,
		Operation: &storage2pb.Transaction_Merge_{Merge: &storage2pb.Transaction_Merge{
			Fragments:      fragments,
			Schema:         schema,
			SchemaMetadata: schemaMetadata,
		}},
	}
}

// NewTransactionClone builds a transaction with Clone operation.
func NewTransactionClone(readVersion uint64, uuid string, isShallow bool, refName *string, refVersion uint64, refPath string) *Transaction {
	return &Transaction{
		ReadVersion: readVersion,
		Uuid:        uuid,
		Operation: &storage2pb.Transaction_Clone_{Clone: &storage2pb.Transaction_Clone{
			IsShallow:  isShallow,
			RefName:    refName,
			RefVersion: refVersion,
			RefPath:    refPath,
		}},
	}
}

// NewTransactionRewrite builds a transaction with Rewrite operation (compaction).
func NewTransactionRewrite(readVersion uint64, uuid string, oldFragments, newFragments []*DataFragment) *Transaction {
	if oldFragments == nil {
		oldFragments = []*DataFragment{}
	}
	if newFragments == nil {
		newFragments = []*DataFragment{}
	}

	groups := []*storage2pb.Transaction_Rewrite_RewriteGroup{
		{
			OldFragments: oldFragments,
			NewFragments: newFragments,
		},
	}

	return &Transaction{
		ReadVersion: readVersion,
		Uuid:        uuid,
		Operation: &storage2pb.Transaction_Rewrite_{Rewrite: &storage2pb.Transaction_Rewrite{
			Groups: groups,
		}},
	}
}

// MergedGeneration is the protobuf type for merged generation tracking
type MergedGeneration = storage2pb.MergedGeneration

// NewTransactionUpdateMemWalState builds a transaction with UpdateMemWalState operation.
// This is used during merge-insert to atomically record which generations have been
// merged to the base table.
func NewTransactionUpdateMemWalState(readVersion uint64, uuid string, mergedGenerations []*MergedGeneration) *Transaction {
	if mergedGenerations == nil {
		mergedGenerations = []*MergedGeneration{}
	}
	return &Transaction{
		ReadVersion: readVersion,
		Uuid:        uuid,
		Operation: &storage2pb.Transaction_UpdateMemWalState_{UpdateMemWalState: &storage2pb.Transaction_UpdateMemWalState{
			MergedGenerations: mergedGenerations,
		}},
	}
}
