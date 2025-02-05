// Copyright 2023-2024 daviszhen
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

//func Test_andWithNull(t *testing.T) {
//	tests := []struct {
//		name  string
//		input [4]bool
//		want  [2]bool
//	}{
//		{
//			name:  "TRUE  AND TRUE   = TRUE",
//			input: [4]bool{true, true, false, false},
//			want:  [2]bool{false, true},
//		},
//		{
//			name:  "TRUE  AND FALSE  = FALSE",
//			input: [4]bool{true, false, false, false},
//			want:  [2]bool{false, false},
//		},
//		{
//			name:  "FALSE AND TRUE   = FALSE",
//			input: [4]bool{false, true, false, false},
//			want:  [2]bool{false, false},
//		},
//		{
//			name:  "FALSE AND FALSE  = FALSE",
//			input: [4]bool{false, false, false, false},
//			want:  [2]bool{false, false},
//		},
//		{
//			name:  "FALSE AND NULL   = FALSE",
//			input: [4]bool{false, false, false, true},
//			want:  [2]bool{false, false},
//		},
//		{
//			name:  "NULL  AND FALSE  = FALSE",
//			input: [4]bool{false, false, true, false},
//			want:  [2]bool{false, false},
//		},
//		{
//			name:  "TRUE  AND NULL   = NULL",
//			input: [4]bool{true, false, false, true},
//			want:  [2]bool{true, true},
//		},
//		{
//			name:  "NULL  AND TRUE   = NULL",
//			input: [4]bool{true, true, true, false},
//			want:  [2]bool{true, true},
//		},
//		{
//			name:  "NULL  AND NULL   = NULL",
//			input: [4]bool{true, true, true, true},
//			want:  [2]bool{true, true},
//		},
//	}
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			gotNull, gotResult := gAndOp.opWithNull(tt.input[0], tt.input[1], tt.input[2], tt.input[3])
//			assert.Equalf(t, tt.want[0], gotNull, "andWithNull(%v, %v, %v, %v)", tt.input[0], tt.input[1], tt.input[2], tt.input[3])
//			assert.Equalf(t, tt.want[1], gotResult, "andWithNull(%v, %v, %v, %v)", tt.input[0], tt.input[1], tt.input[2], tt.input[3])
//		})
//	}
//}
//
//func Test_selectOperation(t *testing.T) {
//	type args struct {
//		left        *chunk.Vector
//		right       *chunk.Vector
//		sel         *chunk.SelectVector
//		count       int
//		trueSel     *chunk.SelectVector
//		falseSel    *chunk.SelectVector
//		subTyp      ET_SubTyp
//		checkResult func(t *testing.T, arg *args, ret int)
//	}
//	tests := []struct {
//		name string
//		args args
//	}{
//		{
//			name: "t1 : int32 == int32",
//			args: args{
//				left:     chunk.newInt32FlatVectorEven(false, util.DefaultVectorSize),
//				right:    chunk.newInt32FlatVectorEven(false, util.DefaultVectorSize),
//				sel:      nil,
//				count:    util.DefaultVectorSize,
//				trueSel:  nil,
//				falseSel: chunk.NewSelectVector(util.DefaultVectorSize),
//				subTyp:   ET_Equal,
//				checkResult: func(t *testing.T, arg *args, ret int) {
//					assert.Equal(t, ret, util.DefaultVectorSize)
//				},
//			},
//		},
//		{
//			name: "t2 : int32 == int32",
//			args: args{
//				left:     chunk.newInt32FlatVectorEven(false, util.DefaultVectorSize),
//				right:    chunk.newInt32FlatVectorEven(true, util.DefaultVectorSize),
//				sel:      nil,
//				count:    util.DefaultVectorSize,
//				trueSel:  nil,
//				falseSel: chunk.NewSelectVector(util.DefaultVectorSize),
//				subTyp:   ET_Equal,
//				checkResult: func(t *testing.T, arg *args, ret int) {
//					assert.Equal(t, ret, util.DefaultVectorSize/2)
//					for i := 0; i < util.DefaultVectorSize/2; i++ {
//						fidx := arg.falseSel.GetIndex(i)
//						assert.Equal(t, fidx%2, 0)
//					}
//				},
//			},
//		},
//		{
//			name: "t3 : int32 == int32",
//			args: args{
//				left:     chunk.newInt32FlatVectorOdd(true, util.DefaultVectorSize),
//				right:    chunk.newInt32FlatVectorEven(true, util.DefaultVectorSize),
//				sel:      nil,
//				count:    util.DefaultVectorSize,
//				trueSel:  chunk.NewSelectVector(util.DefaultVectorSize),
//				falseSel: nil,
//				subTyp:   ET_Equal,
//				checkResult: func(t *testing.T, arg *args, ret int) {
//					assert.Equal(t, 0, ret)
//				},
//			},
//		},
//	}
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			ret := selectOperation(tt.args.left, tt.args.right, tt.args.sel, tt.args.count, tt.args.trueSel, tt.args.falseSel, tt.args.subTyp)
//			if tt.args.checkResult != nil {
//				tt.args.checkResult(t, &tt.args, ret)
//			}
//		})
//	}
//}
