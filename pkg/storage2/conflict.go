package storage2

import (
	"fmt"

	protobuf "google.golang.org/protobuf/proto"

	storage2pb "github.com/daviszhen/plan/pkg/storage2/proto"
)

// ErrConflict is returned when a transaction conflicts with an already committed one.
var ErrConflict = fmt.Errorf("transaction conflict")

// OpKind identifies the operation type of a transaction.
type OpKind int

const (
	OpAppend OpKind = iota
	OpDelete
	OpOverwrite
	OpRestore
	OpUpdate
	OpCreateIndex
	OpDataReplacement
	OpRewrite
	OpMerge
	OpProject
	OpUpdateMemWalState
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
	case *storage2pb.Transaction_Restore_:
		return OpRestore
	case *storage2pb.Transaction_Update_:
		return OpUpdate
	case *storage2pb.Transaction_CreateIndex_:
		return OpCreateIndex
	case *storage2pb.Transaction_DataReplacement_:
		return OpDataReplacement
	case *storage2pb.Transaction_Rewrite_:
		return OpRewrite
	case *storage2pb.Transaction_Merge_:
		return OpMerge
	case *storage2pb.Transaction_Project_:
		return OpProject
	case *storage2pb.Transaction_UpdateMemWalState_:
		return OpUpdateMemWalState
	default:
		return OpOther
	}
}

// CheckConflict returns true if myTxn conflicts with the already-committed otherTxn (which produced otherVersion).
// Matches Lance conflict matrix from conflict_resolver.rs.
//
// Lance Rust conflict matrix (columns = committed, rows = current):
//
//	|                  | Append | Delete/Update | Overwrite | CreateIndex | Rewrite | Merge | Project |
//	|------------------|--------|---------------|-----------|-------------|---------|-------|---------|
//	| Append           | ✅     | ✅            | ❌        | ✅          | ✅      | ❌    | ❌      |
//	| Delete/Update    | ✅     | 1️⃣            | ❌        | ✅          | 1️⃣      | ❌    | ❌      |
//	| Overwrite        | ✅     | ✅            | ✅        | ✅          | ✅      | ✅    | ✅      |
//	| CreateIndex      | ✅     | ✅            | ❌        | ✅          | ✅      | ✅    | ✅      |
//	| Rewrite          | ✅     | 1️⃣            | ❌        | ❌          | 1️⃣      | ❌    | ❌      |
//	| Merge            | ❌     | ❌            | ❌        | ❌          | ✅      | ❌    | ❌      |
//	| Project          | ✅     | ✅            | ❌        | ❌          | ✅      | ❌    | ✅      |
//
// 1️⃣ = conflict only if touching same fragments
func CheckConflict(myTxn *Transaction, otherTxn *Transaction, otherVersion uint64) bool {
	if myTxn == nil || otherTxn == nil {
		return true
	}
	myOp := OpKindOf(myTxn)
	otherOp := OpKindOf(otherTxn)

	// Committed Overwrite/Restore replaces everything, so most subsequent ops conflict.
	if otherOp == OpOverwrite || otherOp == OpRestore {
		if myOp == OpOverwrite {
			return false // Overwrite can follow Overwrite
		}
		return true
	}

	// Current Overwrite doesn't depend on prior state — compatible with anything.
	if myOp == OpOverwrite {
		return false
	}

	// Delegate to per-operation-type checkers.
	switch myOp {
	case OpAppend:
		return checkAppendConflict(otherOp)
	case OpDelete:
		return checkDeleteConflict(myTxn, otherTxn, otherOp)
	case OpCreateIndex:
		return checkCreateIndexConflict2(otherOp)
	case OpRewrite:
		return checkRewriteConflict(myTxn, otherTxn, otherOp)
	case OpMerge:
		return checkMergeConflict(otherOp)
	case OpProject:
		return checkProjectConflict(otherOp)
	case OpUpdate:
		return CheckUpdateConflict(myTxn, otherTxn)
	case OpDataReplacement:
		return CheckDataReplacementConflict(myTxn, otherTxn)
	case OpUpdateMemWalState:
		return checkUpdateMemWalStateConflict(myTxn, otherTxn, otherOp)
	default:
		// For committed Update/DataReplacement, check from their perspective.
		if otherOp == OpUpdate {
			return CheckUpdateConflict(otherTxn, myTxn)
		}
		if otherOp == OpDataReplacement {
			return CheckDataReplacementConflict(otherTxn, myTxn)
		}
		return true
	}
}

// checkAppendConflict: Append row of the matrix.
//
//	Append | Delete | CreateIndex | Rewrite | Merge | Project
//	  ✅       ✅        ✅          ✅       ❌      ❌
func checkAppendConflict(otherOp OpKind) bool {
	switch otherOp {
	case OpAppend, OpDelete, OpCreateIndex, OpRewrite:
		return false
	default:
		return true // Merge, Project, unknown → conflict
	}
}

// checkDeleteConflict: Delete/Update row of the matrix.
//
//	Append | Delete | CreateIndex | Rewrite | Merge | Project
//	  ✅      1️⃣        ✅         1️⃣       ❌      ❌
func checkDeleteConflict(myTxn, otherTxn *Transaction, otherOp OpKind) bool {
	switch otherOp {
	case OpAppend, OpCreateIndex:
		return false
	case OpDelete:
		return deleteDeleteConflict(myTxn, otherTxn)
	case OpRewrite:
		return deleteRewriteConflict(myTxn, otherTxn)
	default:
		return true // Merge, Project, unknown → conflict
	}
}

// checkCreateIndexConflict2: CreateIndex row of the matrix.
//
//	Append | Delete | CreateIndex | Rewrite | Merge | Project
//	  ✅       ✅        ✅          ✅       ✅      ✅
func checkCreateIndexConflict2(otherOp OpKind) bool {
	switch otherOp {
	case OpAppend, OpDelete, OpCreateIndex, OpRewrite, OpMerge, OpProject:
		return false
	default:
		return true // unknown → conflict
	}
}

// checkRewriteConflict: Rewrite row of the matrix.
//
//	Append | Delete  | CreateIndex | Rewrite | Merge | Project
//	  ✅      1️⃣         ❌          1️⃣       ❌      ❌
func checkRewriteConflict(myTxn, otherTxn *Transaction, otherOp OpKind) bool {
	switch otherOp {
	case OpAppend:
		return false
	case OpDelete:
		return deleteRewriteConflict(otherTxn, myTxn) // swap: delete is otherTxn
	case OpRewrite:
		return rewriteRewriteConflict(myTxn, otherTxn)
	default:
		return true // CreateIndex, Merge, Project, unknown → conflict
	}
}

// checkMergeConflict: Merge row of the matrix.
//
//	Append | Delete | CreateIndex | Rewrite | Merge | Project
//	  ❌       ❌        ❌          ✅       ❌      ❌
func checkMergeConflict(otherOp OpKind) bool {
	switch otherOp {
	case OpRewrite:
		return false
	default:
		return true // everything else conflicts
	}
}

// checkProjectConflict: Project row of the matrix.
//
//	Append | Delete | CreateIndex | Rewrite | Merge | Project
//	  ✅       ✅        ❌          ✅       ❌      ✅
func checkProjectConflict(otherOp OpKind) bool {
	switch otherOp {
	case OpAppend, OpDelete, OpRewrite, OpProject:
		return false
	default:
		return true // CreateIndex, Merge, unknown → conflict
	}
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

// deleteRewriteConflict checks if a Delete transaction conflicts with a Rewrite transaction.
// They conflict if they touch the same fragments.
func deleteRewriteConflict(delTxn, rewriteTxn *Transaction) bool {
	del := delTxn.GetDelete()
	rewrite := rewriteTxn.GetRewrite()
	if del == nil || rewrite == nil {
		return true
	}

	// Collect all fragment IDs touched by Delete
	delFragments := setFromUint64(del.DeletedFragmentIds)
	for _, f := range del.GetUpdatedFragments() {
		delFragments[f.Id] = struct{}{}
	}

	// Check if Rewrite touches any of those fragments
	for _, group := range rewrite.GetGroups() {
		for _, f := range group.GetOldFragments() {
			if _, ok := delFragments[f.Id]; ok {
				return true
			}
		}
	}
	return false
}

// rewriteRewriteConflict checks if two Rewrite transactions conflict.
// They conflict if they touch the same fragments.
func rewriteRewriteConflict(a, b *Transaction) bool {
	rewriteA := a.GetRewrite()
	rewriteB := b.GetRewrite()
	if rewriteA == nil || rewriteB == nil {
		return true
	}

	// Collect all fragment IDs from transaction A
	fragmentsA := make(map[uint64]struct{})
	for _, group := range rewriteA.GetGroups() {
		for _, f := range group.GetOldFragments() {
			fragmentsA[f.Id] = struct{}{}
		}
	}

	// Check if transaction B touches any of those fragments
	for _, group := range rewriteB.GetGroups() {
		for _, f := range group.GetOldFragments() {
			if _, ok := fragmentsA[f.Id]; ok {
				return true
			}
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

// checkUpdateMemWalStateConflict: UpdateMemWalState row of the conflict matrix.
// UpdateMemWalState is a metadata-only operation that updates WAL merged generations.
// It conflicts with:
// - Merge: Both modify WAL-related state
// - Another UpdateMemWalState: May have overlapping generations
// It is compatible with:
// - Append, Delete, CreateIndex, Rewrite, Project: Data operations don't affect WAL state
func checkUpdateMemWalStateConflict(myTxn, otherTxn *Transaction, otherOp OpKind) bool {
	switch otherOp {
	case OpAppend, OpDelete, OpCreateIndex, OpRewrite, OpProject:
		return false
	case OpMerge:
		return true // Merge also updates WAL state
	case OpUpdateMemWalState:
		// Check if they touch the same generations
		return updateMemWalStateOverlap(myTxn, otherTxn)
	default:
		return true // unknown → conflict
	}
}

// updateMemWalStateOverlap checks if two UpdateMemWalState transactions overlap.
func updateMemWalStateOverlap(a, b *Transaction) bool {
	walA := a.GetUpdateMemWalState()
	walB := b.GetUpdateMemWalState()
	if walA == nil || walB == nil {
		return true
	}

	// Build a set of region/generation keys from transaction A
	// Key format: hex(uuid) + ":" + generation
	gensA := make(map[string]struct{})
	for _, mg := range walA.GetMergedGenerations() {
		if mg.GetRegionId() != nil {
			key := fmt.Sprintf("%x:%d", mg.GetRegionId().GetUuid(), mg.GetGeneration())
			gensA[key] = struct{}{}
		}
	}

	// Check if transaction B touches any of those region/generation pairs
	for _, mg := range walB.GetMergedGenerations() {
		if mg.GetRegionId() != nil {
			key := fmt.Sprintf("%x:%d", mg.GetRegionId().GetUuid(), mg.GetGeneration())
			if _, ok := gensA[key]; ok {
				return true
			}
		}
	}
	return false
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
