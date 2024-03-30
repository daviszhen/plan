package main

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_andWithNull(t *testing.T) {
	tests := []struct {
		name  string
		input [4]bool
		want  [2]bool
	}{
		{
			name:  "TRUE  AND TRUE   = TRUE",
			input: [4]bool{true, true, false, false},
			want:  [2]bool{false, true},
		},
		{
			name:  "TRUE  AND FALSE  = FALSE",
			input: [4]bool{true, false, false, false},
			want:  [2]bool{false, false},
		},
		{
			name:  "FALSE AND TRUE   = FALSE",
			input: [4]bool{false, true, false, false},
			want:  [2]bool{false, false},
		},
		{
			name:  "FALSE AND FALSE  = FALSE",
			input: [4]bool{false, false, false, false},
			want:  [2]bool{false, false},
		},
		{
			name:  "FALSE AND NULL   = FALSE",
			input: [4]bool{false, false, false, true},
			want:  [2]bool{false, false},
		},
		{
			name:  "NULL  AND FALSE  = FALSE",
			input: [4]bool{false, false, true, false},
			want:  [2]bool{false, false},
		},
		{
			name:  "TRUE  AND NULL   = NULL",
			input: [4]bool{true, false, false, true},
			want:  [2]bool{true, true},
		},
		{
			name:  "NULL  AND TRUE   = NULL",
			input: [4]bool{true, true, true, false},
			want:  [2]bool{true, true},
		},
		{
			name:  "NULL  AND NULL   = NULL",
			input: [4]bool{true, true, true, true},
			want:  [2]bool{true, true},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotNull, gotResult := gAndOp.opWithNull(tt.input[0], tt.input[1], tt.input[2], tt.input[3])
			assert.Equalf(t, tt.want[0], gotNull, "andWithNull(%v, %v, %v, %v)", tt.input[0], tt.input[1], tt.input[2], tt.input[3])
			assert.Equalf(t, tt.want[1], gotResult, "andWithNull(%v, %v, %v, %v)", tt.input[0], tt.input[1], tt.input[2], tt.input[3])
		})
	}
}
