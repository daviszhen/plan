package storage2

import (
	"testing"

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
