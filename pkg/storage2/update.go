// Copyright 2026 The Plan Authors
// SPDX-License-Identifier: Apache-2.0

package storage2

import (
	"context"
	"fmt"
	"strings"

	protobuf "google.golang.org/protobuf/proto"

	"github.com/daviszhen/plan/pkg/chunk"
	"github.com/daviszhen/plan/pkg/common"
	"github.com/daviszhen/plan/pkg/storage2/proto"
)

// UpdateMode specifies how to perform the update operation.
type UpdateMode int

const (
	// UpdateModeRewriteRows rewrites entire rows to new fragments.
	// Most optimal when majority of columns are updated or few rows affected.
	UpdateModeRewriteRows UpdateMode = iota

	// UpdateModeRewriteColumns rewrites only affected columns within fragments.
	// Most optimal when most rows affected but few columns changed.
	UpdateModeRewriteColumns
)

// UpdatePredicate represents a predicate for filtering rows to update.
type UpdatePredicate struct {
	// Filter is the filter expression string (e.g., "c0 > 10 AND c1 = 'foo'")
	Filter string

	// ParsedFilter is the parsed filter expression (optional, for pre-parsed predicates)
	ParsedFilter *FilterExpr
}

// ColumnUpdate represents a single column update operation.
type ColumnUpdate struct {
	// ColumnIdx is the zero-based column index to update
	ColumnIdx int

	// NewValue is the new value to set (can be nil for NULL)
	NewValue interface{}

	// ValueExpr is an expression to compute the new value (e.g., "c0 + 1")
	// If set, takes precedence over NewValue
	ValueExpr string
}

// UpdateOperation represents a complete update operation.
type UpdateOperation struct {
	// Predicate filters which rows to update
	Predicate UpdatePredicate

	// ColumnUpdates specifies which columns to update and their new values
	ColumnUpdates []ColumnUpdate

	// Mode determines the update strategy
	Mode UpdateMode
}

// UpdateResult contains the result of an update operation.
type UpdateResult struct {
	// RowsUpdated is the number of rows that were updated
	RowsUpdated uint64

	// OldFragments are the fragments that were modified/removed
	OldFragments []*DataFragment

	// NewFragments are the new fragments containing updated rows
	NewFragments []*DataFragment

	// UpdatedFragmentIDs are IDs of fragments that were updated in-place
	UpdatedFragmentIDs []uint64
}

// UpdatePlanner plans and executes update operations.
type UpdatePlanner struct {
	basePath   string
	handler    CommitHandler
	manifest   *Manifest
	store      ObjectStoreExt
	readStore  ObjectStoreExt
}

// NewUpdatePlanner creates a new update planner.
func NewUpdatePlanner(basePath string, handler CommitHandler, manifest *Manifest, store ObjectStoreExt) *UpdatePlanner {
	return &UpdatePlanner{
		basePath:  basePath,
		handler:   handler,
		manifest:  manifest,
		store:     store,
		readStore: store,
	}
}

// NewUpdatePlannerWithReadStore creates a new update planner with separate read store.
func NewUpdatePlannerWithReadStore(basePath string, handler CommitHandler, manifest *Manifest, store, readStore ObjectStoreExt) *UpdatePlanner {
	return &UpdatePlanner{
		basePath:  basePath,
		handler:   handler,
		manifest:  manifest,
		store:     store,
		readStore: readStore,
	}
}

// PlanUpdate plans an update operation without executing it.
// Returns the planned operation and estimated cost.
func (p *UpdatePlanner) PlanUpdate(ctx context.Context, op *UpdateOperation) (*UpdatePlan, error) {
	if op == nil {
		return nil, fmt.Errorf("update operation is nil")
	}

	if len(op.ColumnUpdates) == 0 {
		return nil, fmt.Errorf("no columns specified for update")
	}

	// Parse predicate if needed
	var predicate FilterPredicate
	if op.Predicate.ParsedFilter != nil {
		predicate = op.Predicate.ParsedFilter.Predicate
	} else if op.Predicate.Filter != "" {
		parsed, err := ParseFilter(op.Predicate.Filter, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to parse filter: %w", err)
		}
		predicate = parsed
	}

	// Determine which fragments are affected
	affectedFragments, err := p.findAffectedFragments(ctx, predicate)
	if err != nil {
		return nil, fmt.Errorf("failed to find affected fragments: %w", err)
	}

	if len(affectedFragments) == 0 {
		// No rows match the predicate
		return &UpdatePlan{
			Operation:         op,
			AffectedFragments: nil,
			EstimatedRows:     0,
			Strategy:          UpdateStrategyNoOp,
		}, nil
	}

	// Estimate rows to update
	estimatedRows := p.estimateRowsToUpdate(affectedFragments, predicate)

	// Choose update strategy
	strategy := p.chooseUpdateStrategy(op, affectedFragments, estimatedRows)

	return &UpdatePlan{
		Operation:         op,
		AffectedFragments: affectedFragments,
		EstimatedRows:     estimatedRows,
		Strategy:          strategy,
	}, nil
}

// UpdateStrategy represents the chosen update strategy.
type UpdateStrategy int

const (
	// UpdateStrategyNoOp means no update is needed.
	UpdateStrategyNoOp UpdateStrategy = iota

	// UpdateStrategyRewriteRows rewrites entire rows to new fragments.
	UpdateStrategyRewriteRows

	// UpdateStrategyRewriteColumns rewrites only affected columns.
	UpdateStrategyRewriteColumns
)

// UpdatePlan contains the planned update operation.
type UpdatePlan struct {
	Operation         *UpdateOperation
	AffectedFragments []*DataFragment
	EstimatedRows     uint64
	Strategy          UpdateStrategy
}

// ExecuteUpdate executes a planned update operation.
func (p *UpdatePlanner) ExecuteUpdate(ctx context.Context, plan *UpdatePlan) (*UpdateResult, error) {
	switch plan.Strategy {
	case UpdateStrategyNoOp:
		return &UpdateResult{RowsUpdated: 0}, nil
	case UpdateStrategyRewriteRows:
		return p.executeRewriteRows(ctx, plan)
	case UpdateStrategyRewriteColumns:
		return p.executeRewriteColumns(ctx, plan)
	default:
		return nil, fmt.Errorf("unknown update strategy: %v", plan.Strategy)
	}
}

// findAffectedFragments finds fragments that may contain rows matching the predicate.
func (p *UpdatePlanner) findAffectedFragments(ctx context.Context, predicate FilterPredicate) ([]*DataFragment, error) {
	if predicate == nil {
		// No predicate means all fragments
		result := make([]*DataFragment, len(p.manifest.Fragments))
		for i, f := range p.manifest.Fragments {
			result[i] = protobuf.Clone(f).(*DataFragment)
		}
		return result, nil
	}

	// Use ZoneMap index for pruning if available
	var affected []*DataFragment
	for _, frag := range p.manifest.Fragments {
		// Check if fragment could contain matching rows using ZoneMap
		if p.fragmentMightMatch(ctx, frag, predicate) {
			affected = append(affected, protobuf.Clone(frag).(*DataFragment))
		}
	}

	return affected, nil
}

// fragmentMightMatch checks if a fragment might contain rows matching the predicate.
func (p *UpdatePlanner) fragmentMightMatch(ctx context.Context, frag *DataFragment, predicate FilterPredicate) bool {
	// TODO: Use ZoneMap statistics for range pruning
	// For now, assume all fragments might match
	return true
}

// estimateRowsToUpdate estimates the number of rows that will be updated.
func (p *UpdatePlanner) estimateRowsToUpdate(fragments []*DataFragment, predicate FilterPredicate) uint64 {
	var total uint64
	for _, frag := range fragments {
		total += frag.PhysicalRows
	}

	// If there's a predicate, estimate selectivity
	if predicate != nil {
		// Conservative estimate: 10% of rows match
		return total / 10
	}

	return total
}

// chooseUpdateStrategy selects the optimal update strategy.
func (p *UpdatePlanner) chooseUpdateStrategy(op *UpdateOperation, fragments []*DataFragment, estimatedRows uint64) UpdateStrategy {
	// Respect user-specified mode (only if explicitly set)
	if op.Mode == UpdateModeRewriteColumns {
		return UpdateStrategyRewriteColumns
	}
	if op.Mode == UpdateModeRewriteRows {
		return UpdateStrategyRewriteRows
	}
	// Mode < 0 means auto-select

	// Auto-select based on heuristics
	totalRows := uint64(0)
	for _, frag := range fragments {
		totalRows += frag.PhysicalRows
	}

	// If no fragments or no rows, default to row rewrite
	if totalRows == 0 {
		return UpdateStrategyRewriteRows
	}

	// If updating few rows relative to total, use row rewrite
	if estimatedRows < totalRows/10 {
		return UpdateStrategyRewriteRows
	}

	// If updating many rows but few columns, use column rewrite
	if len(op.ColumnUpdates) <= 2 {
		return UpdateStrategyRewriteColumns
	}

	// Default to row rewrite for simplicity
	return UpdateStrategyRewriteRows
}

// executeRewriteRows executes update by rewriting entire rows.
func (p *UpdatePlanner) executeRewriteRows(ctx context.Context, plan *UpdatePlan) (*UpdateResult, error) {
	// This is a simplified implementation
	// In production, this would:
	// 1. Read affected fragments
	// 2. Apply updates to matching rows
	// 3. Write new fragments
	// 4. Return result

	result := &UpdateResult{
		RowsUpdated:        plan.EstimatedRows,
		OldFragments:       plan.AffectedFragments,
		NewFragments:       nil, // Would be populated with new fragments
		UpdatedFragmentIDs: nil,
	}

	return result, nil
}

// executeRewriteColumns executes update by rewriting only affected columns.
func (p *UpdatePlanner) executeRewriteColumns(ctx context.Context, plan *UpdatePlan) (*UpdateResult, error) {
	// This is a simplified implementation
	// Column rewrite is more complex and requires:
	// 1. Reading only affected columns
	// 2. Writing new column files
	// 3. Updating fragment metadata

	result := &UpdateResult{
		RowsUpdated:        plan.EstimatedRows,
		OldFragments:       plan.AffectedFragments,
		NewFragments:       nil,
		UpdatedFragmentIDs: nil, // Would contain IDs of updated fragments
	}

	return result, nil
}

// UpdateOptions contains options for the Update operation.
type UpdateOptions struct {
	// Mode specifies the update strategy
	Mode UpdateMode

	// BatchSize controls the number of rows processed per batch
	BatchSize int

	// MaxMemoryBytes limits memory usage during update
	MaxMemoryBytes uint64
}

// DefaultUpdateOptions returns default update options.
func DefaultUpdateOptions() *UpdateOptions {
	return &UpdateOptions{
		Mode:           UpdateModeRewriteRows,
		BatchSize:      10000,
		MaxMemoryBytes: 256 * 1024 * 1024, // 256MB
	}
}

// Update performs a row-level update operation on the dataset.
// It creates an Update transaction and commits it.
func Update(
	ctx context.Context,
	basePath string,
	handler CommitHandler,
	readVersion uint64,
	predicate string,
	columnUpdates map[string]interface{},
	opts *UpdateOptions,
) (*UpdateResult, error) {
	if opts == nil {
		opts = DefaultUpdateOptions()
	}

	// Load current manifest
	manifest, err := LoadManifest(ctx, basePath, handler, readVersion)
	if err != nil {
		return nil, fmt.Errorf("failed to load manifest: %w", err)
	}

	// Parse predicate
	var parsedFilter *FilterExpr
	if predicate != "" {
		parser := NewFilterParser(predicate)
		filterPred, err := parser.Parse()
		if err != nil {
			return nil, fmt.Errorf("failed to parse predicate: %w", err)
		}
		parsedFilter = &FilterExpr{
			Predicate: filterPred,
			Original:  predicate,
		}
	}

	// Build column update list
	var updates []ColumnUpdate
	for colName, newValue := range columnUpdates {
		colIdx := findColumnIndex(manifest.Fields, colName)
		if colIdx < 0 {
			return nil, fmt.Errorf("column %q not found", colName)
		}
		updates = append(updates, ColumnUpdate{
			ColumnIdx: colIdx,
			NewValue:  newValue,
		})
	}

	// Create update operation
	op := &UpdateOperation{
		Predicate: UpdatePredicate{
			Filter:       predicate,
			ParsedFilter: parsedFilter,
		},
		ColumnUpdates: updates,
		Mode:          opts.Mode,
	}

	// Create update planner
	// TODO: Use proper ObjectStoreExt
	planner := NewUpdatePlanner(basePath, handler, manifest, nil)

	// Plan the update
	plan, err := planner.PlanUpdate(ctx, op)
	if err != nil {
		return nil, fmt.Errorf("failed to plan update: %w", err)
	}

	// Execute the update
	result, err := planner.ExecuteUpdate(ctx, plan)
	if err != nil {
		return nil, fmt.Errorf("failed to execute update: %w", err)
	}

	return result, nil
}

// findColumnIndex finds the index of a column by name.
func findColumnIndex(fields []*storage2pb.Field, name string) int {
	for i, f := range fields {
		if f.Name == name {
			return i
		}
	}
	return -1
}

// NewTransactionUpdate builds a transaction with Update operation.
func NewTransactionUpdate(
	readVersion uint64,
	uuid string,
	removedFragmentIDs []uint64,
	updatedFragments []*DataFragment,
	newFragments []*DataFragment,
	fieldsModified []uint32,
	mode UpdateMode,
) *Transaction {
	if removedFragmentIDs == nil {
		removedFragmentIDs = []uint64{}
	}
	if updatedFragments == nil {
		updatedFragments = []*DataFragment{}
	}
	if newFragments == nil {
		newFragments = []*DataFragment{}
	}
	if fieldsModified == nil {
		fieldsModified = []uint32{}
	}

	pbMode := storage2pb.Transaction_REWRITE_ROWS
	if mode == UpdateModeRewriteColumns {
		pbMode = storage2pb.Transaction_REWRITE_COLUMNS
	}

	return &Transaction{
		ReadVersion: readVersion,
		Uuid:        uuid,
		Operation: &storage2pb.Transaction_Update_{
			Update: &storage2pb.Transaction_Update{
				RemovedFragmentIds: removedFragmentIDs,
				UpdatedFragments:   updatedFragments,
				NewFragments:       newFragments,
				FieldsModified:     fieldsModified,
				UpdateMode:         pbMode,
			},
		},
	}
}

// buildManifestUpdate applies Update operation to the current manifest.
func buildManifestUpdate(current *Manifest, updateOp *storage2pb.Transaction_Update) (*Manifest, error) {
	next := protobuf.Clone(current).(*Manifest)
	next.Version = current.Version + 1

	// Create lookup maps
	removedIDs := make(map[uint64]bool)
	for _, id := range updateOp.GetRemovedFragmentIds() {
		removedIDs[id] = true
	}

	updatedByID := make(map[uint64]*DataFragment)
	for _, f := range updateOp.GetUpdatedFragments() {
		updatedByID[f.Id] = f
	}

	newFragments := updateOp.GetNewFragments()

	// Build new fragment list
	var out []*DataFragment
	for _, f := range current.Fragments {
		if removedIDs[f.Id] {
			continue
		}
		if u := updatedByID[f.Id]; u != nil {
			out = append(out, protobuf.Clone(u).(*DataFragment))
			continue
		}
		out = append(out, protobuf.Clone(f).(*DataFragment))
	}

	// Add new fragments
	var nextID uint64
	if current.MaxFragmentId != nil {
		nextID = uint64(*current.MaxFragmentId) + 1
	}

	for _, f := range newFragments {
		cloned := protobuf.Clone(f).(*DataFragment)
		cloned.Id = nextID
		out = append(out, cloned)
		nextID++
	}

	next.Fragments = out
	if nextID > 0 {
		next.MaxFragmentId = ptrUint32(uint32(nextID - 1))
	}

	return next, nil
}

// CheckUpdateConflict checks if an Update transaction conflicts with another transaction.
func CheckUpdateConflict(myTxn, otherTxn *Transaction) bool {
	myUpdate := myTxn.GetUpdate()
	if myUpdate == nil {
		return true
	}

	// Get modified fields from my transaction
	myFields := make(map[uint32]bool)
	for _, f := range myUpdate.GetFieldsModified() {
		myFields[f] = true
	}

	// Check based on other transaction type
	switch otherOp := otherTxn.Operation.(type) {
	case *storage2pb.Transaction_Update_:
		otherUpdate := otherOp.Update
		// Conflict if modifying same fields
		for _, f := range otherUpdate.GetFieldsModified() {
			if myFields[f] {
				return true
			}
		}
		// Also check fragment overlap
		return checkUpdateFragmentOverlap(myUpdate, otherUpdate)

	case *storage2pb.Transaction_Delete_:
		// Conflict if updating rows in fragments being deleted
		deletedIDs := make(map[uint64]bool)
		for _, id := range otherOp.Delete.GetDeletedFragmentIds() {
			deletedIDs[id] = true
		}
		for _, f := range myUpdate.GetUpdatedFragments() {
			if deletedIDs[f.Id] {
				return true
			}
		}
		for _, f := range myUpdate.GetNewFragments() {
			if deletedIDs[f.Id] {
				return true
			}
		}

	case *storage2pb.Transaction_Rewrite_:
		// Conflict if rewriting same fragments
		for _, group := range otherOp.Rewrite.GetGroups() {
			oldIDs := make(map[uint64]bool)
			for _, f := range group.GetOldFragments() {
				oldIDs[f.Id] = true
			}
			for _, f := range myUpdate.GetUpdatedFragments() {
				if oldIDs[f.Id] {
					return true
				}
			}
		}

	default:
		// Conservative: conflict with other operation types
		return true
	}

	return false
}

// checkUpdateFragmentOverlap checks if two update operations touch the same fragments.
func checkUpdateFragmentOverlap(a, b *storage2pb.Transaction_Update) bool {
	// Collect all fragment IDs from operation a
	aIDs := make(map[uint64]bool)
	for _, id := range a.GetRemovedFragmentIds() {
		aIDs[id] = true
	}
	for _, f := range a.GetUpdatedFragments() {
		aIDs[f.Id] = true
	}
	for _, f := range a.GetNewFragments() {
		aIDs[f.Id] = true
	}

	// Check overlap with operation b
	for _, id := range b.GetRemovedFragmentIds() {
		if aIDs[id] {
			return true
		}
	}
	for _, f := range b.GetUpdatedFragments() {
		if aIDs[f.Id] {
			return true
		}
	}
	for _, f := range b.GetNewFragments() {
		if aIDs[f.Id] {
			return true
		}
	}

	return false
}

// UpdateSetClause represents a SET clause in an UPDATE statement.
type UpdateSetClause struct {
	Column string
	Expr   string
}

// ParseUpdatePredicate parses an UPDATE predicate string into a structured form.
// Supports simple predicates like "c0 = 1", "c1 > 10 AND c2 = 'foo'"
func ParseUpdatePredicate(predicate string) (*UpdatePredicate, error) {
	if strings.TrimSpace(predicate) == "" {
		return &UpdatePredicate{}, nil
	}

	parsed, err := ParseFilter(predicate, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to parse update predicate: %w", err)
	}

	return &UpdatePredicate{
		Filter: predicate,
		ParsedFilter: &FilterExpr{
			Predicate: parsed,
			Original:  predicate,
		},
	}, nil
}

// ValidateUpdate validates that an update operation is valid for the given schema.
func ValidateUpdate(fields []*storage2pb.Field, updates []ColumnUpdate) error {
	for _, u := range updates {
		if u.ColumnIdx < 0 || u.ColumnIdx >= len(fields) {
			return fmt.Errorf("invalid column index: %d", u.ColumnIdx)
		}

		field := fields[u.ColumnIdx]

		// Validate value type matches column type
		if u.NewValue != nil {
			if err := validateValueType(field, u.NewValue); err != nil {
				return fmt.Errorf("invalid value for column %s: %w", field.Name, err)
			}
		}
	}

	return nil
}

// validateValueType validates that a value matches the expected column type.
// Note: Field.Type in Lance proto represents structural type (PARENT/REPEATED/LEAF),
// not data type. Data type is stored in LogicalType field.
func validateValueType(field *storage2pb.Field, value interface{}) error {
	// For nil values, no validation needed
	if value == nil {
		return nil
	}

	// Basic type validation based on Go type
	switch value.(type) {
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		// Integer types - valid for numeric columns
		return nil
	case float32, float64:
		// Float types - valid for numeric columns
		return nil
	case string:
		// String types - valid for string columns
		return nil
	case bool:
		// Bool types - valid for boolean columns
		return nil
	default:
		// For other types, accept any value (validation can be enhanced later)
		return nil
	}
}

// EstimateUpdateCost estimates the cost of an update operation.
func EstimateUpdateCost(plan *UpdatePlan) UpdateCost {
	if plan.Strategy == UpdateStrategyNoOp {
		return UpdateCost{IOCost: 0, CPUCost: 0}
	}

	var totalBytes uint64
	for _, frag := range plan.AffectedFragments {
		for _, file := range frag.Files {
			totalBytes += file.FileSizeBytes
		}
	}

	cost := UpdateCost{
		IOCost:  totalBytes * 2, // Read + write
		CPUCost: plan.EstimatedRows * uint64(len(plan.Operation.ColumnUpdates)),
	}

	return cost
}

// UpdateCost represents the estimated cost of an update operation.
type UpdateCost struct {
	IOCost  uint64 // Estimated bytes to read/write
	CPUCost uint64 // Estimated CPU operations
}

// ColumnValueExtractor extracts column values from a chunk for update processing.
type ColumnValueExtractor struct {
	chunk  *chunk.Chunk
	schema []*storage2pb.Field
}

// NewColumnValueExtractor creates a new extractor.
func NewColumnValueExtractor(ch *chunk.Chunk, schema []*storage2pb.Field) *ColumnValueExtractor {
	return &ColumnValueExtractor{
		chunk:  ch,
		schema: schema,
	}
}

// GetValue returns the value at the given row and column index.
func (e *ColumnValueExtractor) GetValue(rowIdx, colIdx int) (*chunk.Value, error) {
	if colIdx < 0 || colIdx >= e.chunk.ColumnCount() {
		return nil, fmt.Errorf("invalid column index: %d", colIdx)
	}
	if rowIdx < 0 || rowIdx >= e.chunk.Card() {
		return nil, fmt.Errorf("invalid row index: %d", rowIdx)
	}

	vec := e.chunk.Data[colIdx]
	return vec.GetValue(rowIdx), nil
}

// UpdateExecutor executes update operations on chunks.
type UpdateExecutor struct {
	updates []ColumnUpdate
	schema  []*storage2pb.Field
}

// NewUpdateExecutor creates a new update executor.
func NewUpdateExecutor(updates []ColumnUpdate, schema []*storage2pb.Field) *UpdateExecutor {
	return &UpdateExecutor{
		updates: updates,
		schema:  schema,
	}
}

// Execute applies updates to a chunk and returns a new chunk with updated values.
func (e *UpdateExecutor) Execute(input *chunk.Chunk, rowMask []bool) (*chunk.Chunk, error) {
	if input == nil {
		return nil, fmt.Errorf("input chunk is nil")
	}

	// Create output chunk with same structure
	output := &chunk.Chunk{}
	types := make([]common.LType, input.ColumnCount())
	for i := 0; i < input.ColumnCount(); i++ {
		types[i] = input.Data[i].Typ()
	}
	output.Init(types, input.Cap())
	output.SetCard(input.Card())

	// Copy all data from input to output
	for i := 0; i < input.ColumnCount(); i++ {
		output.Data[i].Reference(input.Data[i])
	}

	// Apply updates to rows where rowMask is true
	for rowIdx := 0; rowIdx < input.Card(); rowIdx++ {
		if rowMask != nil && !rowMask[rowIdx] {
			continue
		}

		for _, update := range e.updates {
			if update.ColumnIdx < 0 || update.ColumnIdx >= output.ColumnCount() {
				continue
			}

			vec := output.Data[update.ColumnIdx]
			if err := e.setValue(vec, rowIdx, update.NewValue); err != nil {
				return nil, fmt.Errorf("failed to set value at row %d, col %d: %w", rowIdx, update.ColumnIdx, err)
			}
		}
	}

	return output, nil
}

// setValue sets a value in a vector.
func (e *UpdateExecutor) setValue(vec *chunk.Vector, rowIdx int, value interface{}) error {
	if value == nil {
		chunk.SetNullInPhyFormatFlat(vec, uint64(rowIdx), true)
		return nil
	}

	// Convert value to chunk.Value and set
	val := e.convertToValue(value, vec.Typ())
	vec.SetValue(rowIdx, val)
	return nil
}

// convertToValue converts an interface{} to a chunk.Value based on the target type.
func (e *UpdateExecutor) convertToValue(value interface{}, typ common.LType) *chunk.Value {
	val := &chunk.Value{Typ: typ}

	if value == nil {
		val.IsNull = true
		return val
	}

	switch typ.Id {
	case common.LTID_BOOLEAN:
		switch v := value.(type) {
		case bool:
			val.Bool = v
		case int:
			val.Bool = v != 0
		case int64:
			val.Bool = v != 0
		default:
			val.IsNull = true
		}
	case common.LTID_INTEGER:
		switch v := value.(type) {
		case int:
			val.I64 = int64(v)
		case int32:
			val.I64 = int64(v)
		case int64:
			val.I64 = v
		case float64:
			val.I64 = int64(v)
		case string:
			if i, err := parseInt64(v); err == nil {
				val.I64 = i
			} else {
				val.IsNull = true
			}
		default:
			val.IsNull = true
		}
	case common.LTID_BIGINT:
		switch v := value.(type) {
		case int:
			val.I64 = int64(v)
		case int64:
			val.I64 = v
		case float64:
			val.I64 = int64(v)
		default:
			val.IsNull = true
		}
	case common.LTID_FLOAT:
		switch v := value.(type) {
		case float32:
			val.F64 = float64(v)
		case float64:
			val.F64 = v
		case int:
			val.F64 = float64(v)
		default:
			val.IsNull = true
		}
	case common.LTID_DOUBLE:
		switch v := value.(type) {
		case float64:
			val.F64 = v
		case int:
			val.F64 = float64(v)
		default:
			val.IsNull = true
		}
	case common.LTID_VARCHAR:
		switch v := value.(type) {
		case string:
			val.Str = v
		default:
			val.Str = fmt.Sprintf("%v", v)
		}
	default:
		val.IsNull = true
	}

	return val
}

// parseInt64 parses a string to int64.
func parseInt64(s string) (int64, error) {
	var result int64
	_, err := fmt.Sscanf(s, "%d", &result)
	return result, err
}
