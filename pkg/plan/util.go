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

import (
	"fmt"
	"strings"

	"github.com/xlab/treeprint"

	"github.com/daviszhen/plan/pkg/storage"
	"github.com/daviszhen/plan/pkg/util"
)

const (
	defaultIndent = 4
)

type FormatCtx struct {
	buf    strings.Builder
	line   strings.Builder
	offset Padding
	indent Padding
}

func (fc *FormatCtx) AddOffset() {
	fc.offset.AddPad()
}

func (fc *FormatCtx) RestoreOffset() {
	fc.offset.RestorePad()
}

func (fc *FormatCtx) fillOffset() {
	for i := 0; i < fc.offset.pad; i++ {
		fc.buf.WriteByte(' ')
	}
}

func (fc *FormatCtx) fillIndent() {
	for i := 0; i < fc.indent.pad; i++ {
		fc.buf.WriteByte(' ')
	}
}

func (fc *FormatCtx) writeString(s string) {
	for _, c := range s {
		fc.line.WriteRune(c)
		if c == '\n' {
			fc.buf.WriteString(fc.line.String())
			fc.fillOffset()
			fc.line.Reset()
		}
	}
	fc.buf.WriteString(fc.line.String())
	fc.line.Reset()
}

func (fc *FormatCtx) String() string {
	return fc.buf.String()
}

func (fc *FormatCtx) Write(s string) {
	fc.writeString(s)
}

func (fc *FormatCtx) Writef(f string, args ...any) {
	fc.writeString(fmt.Sprintf(f, args...))
}

func (fc *FormatCtx) Writefln(f string, args ...any) {
	fc.writeString(fmt.Sprintf(f+"\n", args...))
}

func (fc *FormatCtx) Writeln(args ...any) {
	fc.writeString(fmt.Sprintln(args...))
}

func (fc *FormatCtx) WriteStrings(ss []string) {
	for _, s := range ss {
		fc.Writeln(s)
	}
}

func WriteMap[K comparable, V any](ctx *FormatCtx, m map[K]V) {
	for k, v := range m {
		ctx.Writeln(k, ":", v)
	}
}

func WriteExprs(ctx *FormatCtx, exprs []*Expr) {
	for _, e := range exprs {
		e.Format(ctx)
		ctx.Writeln()
	}
}

func WriteExpr(ctx *FormatCtx, expr *Expr) {
	expr.Format(ctx)
	ctx.Writeln()
}

func WriteMapTree[K comparable, V any](tree treeprint.Tree, m map[K]V) {
	for k, v := range m {
		tree.AddNode(fmt.Sprintf("%v : %v", k, v))
	}
}

func WriteExprsTree(tree treeprint.Tree, exprs []*Expr) {
	for i, e := range exprs {
		p := tree.AddBranch(fmt.Sprintf("%d", i))
		e.Print(p, "")
	}
}

func WriteExprTree(tree treeprint.Tree, expr *Expr) {
	expr.Print(tree, "")
}

type Format interface {
	Format(*FormatCtx)
}

type Padding struct {
	pad      int
	lastPads []int
}

func (fc *Padding) addPad(d int) {
	if fc.pad+d < 0 {
		fc.pad = 0
	} else {
		fc.pad += d
	}
}

func (fc *Padding) AddPad() {
	fc.lastPads = append(fc.lastPads, fc.pad)
	fc.addPad(defaultIndent)
}

func (fc *Padding) RestorePad() {
	if len(fc.lastPads) <= 0 {
		fc.addPad(-fc.pad)
		return
	}
	fc.pad = fc.lastPads[len(fc.lastPads)-1]
	fc.lastPads = util.Pop(fc.lastPads)
}

func listExprs(bb *strings.Builder, exprs []*Expr) *strings.Builder {
	for i, expr := range exprs {
		if expr == nil {
			continue
		}
		bb.WriteString(fmt.Sprintf("\n  %d: ", i))
		if len(expr.Alias) != 0 {
			bb.WriteString(expr.Alias)
			bb.WriteByte(' ')
		}
		bb.WriteString(expr.String())

	}
	return bb
}

func listExprsToTree(tree treeprint.Tree, exprs []*Expr) {
	for i, expr := range exprs {
		if expr == nil {
			continue
		}
		alias := ""
		if len(expr.Alias) != 0 {
			alias = expr.Alias
		}
		meta := ""
		if expr.Typ == ET_Orderby {
			asc := ""
			if expr.Desc {
				asc = "desc"
			} else {
				asc = "asc"
			}
			meta = fmt.Sprintf("%v %v %v", i, alias, asc)
		} else {
			meta = fmt.Sprintf("%v %v", i, alias)
		}
		expr.Print(tree, meta)
	}
}

func listColDefsToTree(tree treeprint.Tree, colDefs []*storage.ColumnDefinition) {
	for _, colDef := range colDefs {
		consStrs := make([]string, 0)
		for _, cons := range colDef.Constraints {
			consStrs = append(consStrs, cons.String())
		}
		tree.AddMetaNode(
			fmt.Sprintf("%v %v", colDef.Name, colDef.Type),
			strings.Join(consStrs, ","),
		)
	}
}

func isDisjoint(a, b map[uint64]bool) bool {
	for k := range a {
		if _, has := b[k]; has {
			return false
		}
	}
	return true
}

func EntriesPerBlock(width int) int {
	return BLOCK_SIZE / width
}
