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
			Fragments:         fragments,
			Schema:            schema,
			ConfigUpsertValues: configUpsert,
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
			OldFragments:  oldFragments,
			NewFragments:  newFragments,
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
