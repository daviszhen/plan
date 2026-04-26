package storage2

import (
	"testing"

	"github.com/daviszhen/plan/pkg/chunk"
	"github.com/daviszhen/plan/pkg/common"
	"github.com/stretchr/testify/require"
)

func TestFilterParser_Comparison(t *testing.T) {
	tests := []struct {
		name     string
		filter   string
		wantErr  bool
		wantType string
	}{
		{
			name:     "simple equals",
			filter:   "c0 = 1",
			wantErr:  false,
			wantType: "*storage2.ColumnPredicate",
		},
		{
			name:     "greater than",
			filter:   "c1 > 10",
			wantErr:  false,
			wantType: "*storage2.ColumnPredicate",
		},
		{
			name:     "less than or equal",
			filter:   "c2 <= 100",
			wantErr:  false,
			wantType: "*storage2.ColumnPredicate",
		},
		{
			name:     "not equals",
			filter:   "c0 != 5",
			wantErr:  false,
			wantType: "*storage2.ColumnPredicate",
		},
		{
			name:     "string comparison",
			filter:   "c0 = 'foo'",
			wantErr:  false,
			wantType: "*storage2.ColumnPredicate",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pred, err := ParseFilter(tt.filter, nil)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, pred)
			require.Contains(t, pred.String(), "col[")
		})
	}
}

func TestFilterParser_BooleanOperators(t *testing.T) {
	tests := []struct {
		name    string
		filter  string
		wantErr bool
	}{
		{
			name:    "AND expression",
			filter:  "c0 > 10 AND c1 < 100",
			wantErr: false,
		},
		{
			name:    "OR expression",
			filter:  "c0 = 1 OR c0 = 2",
			wantErr: false,
		},
		{
			name:    "NOT expression",
			filter:  "NOT c0 = 1",
			wantErr: false,
		},
		{
			name:    "complex expression",
			filter:  "(c0 > 10 OR c1 > 10) AND c2 < 100",
			wantErr: false,
		},
		{
			name:    "nested AND/OR",
			filter:  "c0 = 1 AND (c1 = 2 OR c1 = 3)",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pred, err := ParseFilter(tt.filter, nil)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, pred)
		})
	}
}

func TestFilterParser_NullChecks(t *testing.T) {
	tests := []struct {
		name    string
		filter  string
		wantErr bool
	}{
		{
			name:    "IS NULL",
			filter:  "c0 IS NULL",
			wantErr: false,
		},
		{
			name:    "IS NOT NULL",
			filter:  "c1 IS NOT NULL",
			wantErr: false,
		},
		{
			name:    "NULL with AND",
			filter:  "c0 IS NOT NULL AND c1 > 10",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pred, err := ParseFilter(tt.filter, nil)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, pred)
		})
	}
}

func TestFilterParser_InExpression(t *testing.T) {
	tests := []struct {
		name    string
		filter  string
		wantErr bool
	}{
		{
			name:    "IN with integers",
			filter:  "c0 IN (1, 2, 3)",
			wantErr: false,
		},
		{
			name:    "IN with strings",
			filter:  "c0 IN ('foo', 'bar', 'baz')",
			wantErr: false,
		},
		{
			name:    "NOT IN simulation",
			filter:  "NOT c0 IN (1, 2, 3)",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pred, err := ParseFilter(tt.filter, nil)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, pred)
		})
	}
}

func TestFilterParser_LikeExpression(t *testing.T) {
	tests := []struct {
		name    string
		filter  string
		wantErr bool
	}{
		{
			name:    "LIKE with prefix",
			filter:  "c0 LIKE 'foo%'",
			wantErr: false,
		},
		{
			name:    "LIKE with suffix",
			filter:  "c0 LIKE '%bar'",
			wantErr: false,
		},
		{
			name:    "LIKE with contains",
			filter:  "c0 LIKE '%baz%'",
			wantErr: false,
		},
		{
			name:    "LIKE with single char",
			filter:  "c0 LIKE '_oo'",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pred, err := ParseFilter(tt.filter, nil)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, pred)
		})
	}
}

func TestFilterParser_Empty(t *testing.T) {
	pred, err := ParseFilter("", nil)
	require.NoError(t, err)
	require.Nil(t, pred)
}

func TestFilterParser_Errors(t *testing.T) {
	tests := []struct {
		name   string
		filter string
	}{
		{
			name:   "missing operator",
			filter: "c0 1",
		},
		{
			name:   "invalid operator",
			filter: "c0 ?? 1",
		},
		{
			name:   "unclosed parenthesis",
			filter: "(c0 = 1",
		},
		{
			name:   "unclosed string",
			filter: "name = 'foo",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ParseFilter(tt.filter, nil)
			require.Error(t, err)
		})
	}
}

func TestLikePattern(t *testing.T) {
	tests := []struct {
		pattern string
		input   string
		want    bool
	}{
		{"foo%", "foobar", true},
		{"foo%", "foo", true},
		{"foo%", "barfoo", false},
		{"%bar", "foobar", true},
		{"%bar", "bar", true},
		{"%bar", "barfoo", false},
		{"%baz%", "foobazbar", true},
		{"%baz%", "baz", true},
		{"%baz%", "foobar", false},
		{"_oo", "foo", true},
		{"_oo", "bar", false},
		{"f_o%", "foobar", true},
		{"%", "", true},
		{"_", "a", true},
		{"_", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.pattern+"_"+tt.input, func(t *testing.T) {
			segments := likeToRegex(tt.pattern)
			got := matchLike(tt.input, segments)
			require.Equal(t, tt.want, got, "pattern=%q, input=%q", tt.pattern, tt.input)
		})
	}
}

func TestTokenize(t *testing.T) {
	tests := []struct {
		input    string
		expected []token
	}{
		{
			input: "c0 = 1",
			expected: []token{
				{typ: tokenIdent, value: "c0"},
				{typ: tokenOp, value: "="},
				{typ: tokenNumber, value: "1"},
				{typ: tokenEOF},
			},
		},
		{
			input: "name = 'foo'",
			expected: []token{
				{typ: tokenIdent, value: "name"},
				{typ: tokenOp, value: "="},
				{typ: tokenString, value: "'foo'"},
				{typ: tokenEOF},
			},
		},
		{
			input: "(c0 > 10 AND c1 < 100)",
			expected: []token{
				{typ: tokenLParen, value: "("},
				{typ: tokenIdent, value: "c0"},
				{typ: tokenOp, value: ">"},
				{typ: tokenNumber, value: "10"},
				{typ: tokenIdent, value: "AND"},
				{typ: tokenIdent, value: "c1"},
				{typ: tokenOp, value: "<"},
				{typ: tokenNumber, value: "100"},
				{typ: tokenRParen, value: ")"},
				{typ: tokenEOF},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			tokens := tokenize(tt.input)
			require.Equal(t, len(tt.expected), len(tokens), "token count mismatch")
			for i, exp := range tt.expected {
				require.Equal(t, exp.typ, tokens[i].typ, "token %d type mismatch", i)
				require.Equal(t, exp.value, tokens[i].value, "token %d value mismatch", i)
			}
		})
	}
}

// TestNullPredicateEvaluate tests NullPredicate.Evaluate method.
func TestNullPredicateEvaluate(t *testing.T) {
	c := createTestChunkWithNulls(t)

	pred := &NullPredicate{ColumnIndex: 0}
	mask, err := pred.Evaluate(c)
	require.NoError(t, err)
	require.Equal(t, 4, len(mask))

	// Check NULL detection (index 1 and 3 are nil)
	expected := []bool{false, true, false, true}
	for i, want := range expected {
		require.Equal(t, want, mask[i], "row %d", i)
	}

	// Test CanPushdown
	require.True(t, pred.CanPushdown())

	// Test String
	require.Contains(t, pred.String(), "IS NULL")
}

// TestNotNullPredicateEvaluate tests NotNullPredicate.Evaluate method.
func TestNotNullPredicateEvaluate(t *testing.T) {
	c := createTestChunkWithNulls(t)

	pred := &NotNullPredicate{ColumnIndex: 0}
	mask, err := pred.Evaluate(c)
	require.NoError(t, err)
	require.Equal(t, 4, len(mask))

	// Check NOT NULL detection (index 0 and 2 are not nil)
	expected := []bool{true, false, true, false}
	for i, want := range expected {
		require.Equal(t, want, mask[i], "row %d", i)
	}

	// Test CanPushdown
	require.True(t, pred.CanPushdown())

	// Test String
	require.Contains(t, pred.String(), "IS NOT NULL")
}

// TestInPredicateEvaluate tests InPredicate.Evaluate method.
func TestInPredicateEvaluate(t *testing.T) {
	c := createTestChunkForIn(t)

	// Test IN (1, 3)
	pred := &InPredicate{
		ColumnIndex: 0,
		Values:      createChunkValues([]int64{1, 3}),
	}
	mask, err := pred.Evaluate(c)
	require.NoError(t, err)
	require.Equal(t, 5, len(mask))

	// Values: 1, 2, 3, 4, 5 -> IN (1, 3) = true, false, true, false, false
	expected := []bool{true, false, true, false, false}
	for i, want := range expected {
		require.Equal(t, want, mask[i], "row %d", i)
	}

	// Test CanPushdown
	require.True(t, pred.CanPushdown())

	// Test String
	require.Contains(t, pred.String(), "IN")
}

// TestLikePredicateEvaluate tests LikePredicate.Evaluate method.
func TestLikePredicateEvaluate(t *testing.T) {
	c := createTestChunkForLike(t)

	// Test LIKE 'foo%'
	pred := &LikePredicate{
		ColumnIndex: 0,
		Pattern:     "foo%",
	}
	mask, err := pred.Evaluate(c)
	require.NoError(t, err)
	require.Equal(t, 4, len(mask))

	// Strings: "foobar", "bar", "football", nil -> LIKE 'foo%' = true, false, true, false
	expected := []bool{true, false, true, false}
	for i, want := range expected {
		require.Equal(t, want, mask[i], "row %d", i)
	}

	// Test CanPushdown
	require.True(t, pred.CanPushdown())

	// Test String
	require.Contains(t, pred.String(), "LIKE")
	require.Contains(t, pred.String(), "foo%")
}

// TestPredicateOutOfRange tests predicates with invalid column index.
func TestPredicateOutOfRange(t *testing.T) {
	c := createTestChunkWithNulls(t)

	// Test NullPredicate with invalid column
	nullPred := &NullPredicate{ColumnIndex: 99}
	_, err := nullPred.Evaluate(c)
	require.Error(t, err)
	require.Contains(t, err.Error(), "out of range")

	// Test NotNullPredicate with invalid column
	notNullPred := &NotNullPredicate{ColumnIndex: -1}
	_, err = notNullPred.Evaluate(c)
	require.Error(t, err)
	require.Contains(t, err.Error(), "out of range")

	// Test InPredicate with invalid column
	inPred := &InPredicate{ColumnIndex: 99, Values: nil}
	_, err = inPred.Evaluate(c)
	require.Error(t, err)
	require.Contains(t, err.Error(), "out of range")

	// Test LikePredicate with invalid column
	likePred := &LikePredicate{ColumnIndex: 99, Pattern: "%"}
	_, err = likePred.Evaluate(c)
	require.Error(t, err)
	require.Contains(t, err.Error(), "out of range")
}

// Helper function to create a test chunk with null values
func createTestChunkWithNulls(t *testing.T) *chunk.Chunk {
	t.Helper()
	typs := []common.LType{common.MakeLType(common.LTID_INTEGER)}
	c := &chunk.Chunk{}
	c.Init(typs, 4)
	c.SetCard(4)

	// Set values: 10, nil, 20, nil
	col := c.Data[0]
	col.SetValue(0, &chunk.Value{Typ: typs[0], I64: 10})
	col.SetValue(1, &chunk.Value{Typ: typs[0], I64: 0}) // Set a placeholder
	chunk.SetNullInPhyFormatFlat(col, 1, true)          // Mark as null
	col.SetValue(2, &chunk.Value{Typ: typs[0], I64: 20})
	col.SetValue(3, &chunk.Value{Typ: typs[0], I64: 0}) // Set a placeholder
	chunk.SetNullInPhyFormatFlat(col, 3, true)          // Mark as null

	return c
}

// Helper function to create a test chunk for IN tests
func createTestChunkForIn(t *testing.T) *chunk.Chunk {
	t.Helper()
	typs := []common.LType{common.MakeLType(common.LTID_INTEGER)}
	c := &chunk.Chunk{}
	c.Init(typs, 5)
	c.SetCard(5)

	col := c.Data[0]
	for i := 0; i < 5; i++ {
		col.SetValue(i, &chunk.Value{Typ: typs[0], I64: int64(i + 1)}) // 1, 2, 3, 4, 5
	}

	return c
}

// Helper function to create a test chunk for LIKE tests
func createTestChunkForLike(t *testing.T) *chunk.Chunk {
	t.Helper()
	typs := []common.LType{common.MakeLType(common.LTID_VARCHAR)}
	c := &chunk.Chunk{}
	c.Init(typs, 4)
	c.SetCard(4)

	col := c.Data[0]
	col.SetValue(0, &chunk.Value{Typ: typs[0], Str: "foobar"})
	col.SetValue(1, &chunk.Value{Typ: typs[0], Str: "bar"})
	col.SetValue(2, &chunk.Value{Typ: typs[0], Str: "football"})
	col.SetValue(3, &chunk.Value{Typ: typs[0], Str: ""}) // Set placeholder
	chunk.SetNullInPhyFormatFlat(col, 3, true)           // Mark as null

	return c
}

// Helper function to create chunk values for IN predicate
func createChunkValues(values []int64) []*chunk.Value {
	typ := common.MakeLType(common.LTID_INTEGER)
	result := make([]*chunk.Value, len(values))
	for i, v := range values {
		result[i] = &chunk.Value{Typ: typ, I64: v}
	}
	return result
}
