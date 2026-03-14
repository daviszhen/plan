package storage2

import (
	"fmt"

	protobuf "google.golang.org/protobuf/proto"

	"github.com/daviszhen/plan/pkg/storage2/proto"
)

// ErrConflict is returned when a transaction conflicts with an already committed one.
var ErrConflict = fmt.Errorf("transaction conflict")

// OpKind identifies the operation type of a transaction.
type OpKind int

const (
	OpAppend OpKind = iota
	OpDelete
	OpOverwrite
	OpUpdate
	OpCreateIndex
	OpDataReplacement
	OpOther
)

// OpKindOf returns the operation kind of the transaction.
func OpKindOf(txn *Transaction) OpKind {
	if txn == nil {
		return OpOther
	}
	switch txn.Operation.(type) {
	case *storage2pb.Transaction_Append_:
		return OpAppend
	case *storage2pb.Transaction_Delete_:
		return OpDelete
	case *storage2pb.Transaction_Overwrite_:
		return OpOverwrite
	case *storage2pb.Transaction_Update_:
		return OpUpdate
	case *storage2pb.Transaction_CreateIndex_:
		return OpCreateIndex
	case *storage2pb.Transaction_DataReplacement_:
		return OpDataReplacement
	default:
		return OpOther
	}
}

// CheckConflict returns true if myTxn conflicts with the already-committed otherTxn (which produced otherVersion).
// Matches Lance conflict matrix: Append↔Append ok, Append↔Delete ok, Append↔Overwrite conflict, etc.
func CheckConflict(myTxn *Transaction, otherTxn *Transaction, otherVersion uint64) bool {
	if myTxn == nil || otherTxn == nil {
		return true
	}
	myOp := OpKindOf(myTxn)
	otherOp := OpKindOf(otherTxn)

	// Overwrite conflicts with everything (including another Overwrite).
	if myOp == OpOverwrite || otherOp == OpOverwrite {
		return true
	}
	// Append vs Append: compatible.
	if myOp == OpAppend && otherOp == OpAppend {
		return false
	}
	// Append vs Delete: compatible.
	if (myOp == OpAppend && otherOp == OpDelete) || (myOp == OpDelete && otherOp == OpAppend) {
		return false
	}
	// Delete vs Delete: conflict if they touch the same fragments.
	if myOp == OpDelete && otherOp == OpDelete {
		return deleteDeleteConflict(myTxn, otherTxn)
	}
	// Update vs other operations: use specific conflict check
	if myOp == OpUpdate {
		return CheckUpdateConflict(myTxn, otherTxn)
	}
	if otherOp == OpUpdate {
		// Check conflict from the other direction
		return CheckUpdateConflict(otherTxn, myTxn)
	}
	// CreateIndex vs other operations: use specific conflict check
	if myOp == OpCreateIndex {
		return CheckCreateIndexConflict(myTxn, otherTxn)
	}
	if otherOp == OpCreateIndex {
		// Check conflict from the other direction
		return CheckCreateIndexConflict(otherTxn, myTxn)
	}
	// DataReplacement vs other operations: use specific conflict check
	if myOp == OpDataReplacement {
		return CheckDataReplacementConflict(myTxn, otherTxn)
	}
	if otherOp == OpDataReplacement {
		// Check conflict from the other direction
		return CheckDataReplacementConflict(otherTxn, myTxn)
	}
	// Unsupported or other ops: treat as conflict.
	return true
}

func deleteDeleteConflict(a, b *Transaction) bool {
	delA := a.GetDelete()
	delB := b.GetDelete()
	if delA == nil || delB == nil {
		return true
	}
	idsA := setFromUint64(delA.DeletedFragmentIds)
	for _, id := range delA.GetUpdatedFragments() {
		idsA[id.Id] = struct{}{}
	}
	for _, id := range delB.DeletedFragmentIds {
		if _, ok := idsA[id]; ok {
			return true
		}
	}
	for _, f := range delB.GetUpdatedFragments() {
		if _, ok := idsA[f.Id]; ok {
			return true
		}
	}
	return false
}

func setFromUint64(s []uint64) map[uint64]struct{} {
	m := make(map[uint64]struct{}, len(s))
	for _, v := range s {
		m[v] = struct{}{}
	}
	return m
}

// Rebase returns a new transaction with read_version set to newReadVersion and the same operation.
// Caller must ensure the transaction is compatible (e.g. after CheckConflict returned false).
func Rebase(txn *Transaction, newReadVersion uint64) *Transaction {
	if txn == nil {
		return nil
	}
	next := protobuf.Clone(txn).(*Transaction)
	next.ReadVersion = newReadVersion
	return next
}
