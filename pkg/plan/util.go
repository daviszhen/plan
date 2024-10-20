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

import "C"
import (
	"bytes"
	"fmt"
	"math"
	"strings"
	"unsafe"

	"github.com/xlab/treeprint"
)

//#include <stdio.h>
//#include <stdlib.h>
import "C"

func swap[T any](a []T, i, j int) {
	if i >= 0 && i < len(a) && j >= 0 && j < len(a) {
		t := a[i]
		a[i] = a[j]
		a[j] = t
	}
}

func pop[T any](a []T) []T {
	if len(a) > 0 {
		return a[:len(a)-1]
	}
	return a
}

func copyTo[T any](src []T) []T {
	dst := make([]T, len(src))
	copy(dst, src)
	return dst
}

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
	fc.lastPads = pop(fc.lastPads)
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

func Min[T int](a, b T) T {
	if a > b {
		return b
	}
	return a
}

func isDisjoint(a, b map[uint64]bool) bool {
	for k := range a {
		if _, has := b[k]; has {
			return false
		}
	}
	return true
}

func erase[T any](a []T, i int) []T {
	if i < 0 || i >= len(a) {
		return a
	}
	a[i], a[len(a)-1] = a[len(a)-1], a[i]
	return a[:len(a)-1]
}

func assertFunc(b bool) {
	if !b {
		panic("need true")
	}
}

type BytesAllocator interface {
	Alloc(sz int) []byte
	Free([]byte)
}

var gAlloc BytesAllocator = &DefaultAllocator{}

type DefaultAllocator struct {
}

func (alloc *DefaultAllocator) Alloc(sz int) []byte {
	return make([]byte, sz)
}

func (alloc *DefaultAllocator) Free(bytes []byte) {
}

func nextPowerOfTwo(v uint64) uint64 {
	v--
	v |= v >> 1
	v |= v >> 2
	v |= v >> 4
	v |= v >> 8
	v |= v >> 16
	v |= v >> 32
	v++
	return v
}

func isPowerOfTwo(v uint64) bool {
	return (v & (v - 1)) == 0
}

func load[T any](ptr unsafe.Pointer) T {
	return *(*T)(ptr)
}

func store[T any](val T, ptr unsafe.Pointer) {
	*(*T)(ptr) = val
}

func memset(ptr unsafe.Pointer, val byte, size int) {
	for i := 0; i < size; i++ {
		store[byte](val, pointerAdd(ptr, i))
	}
}

func fill[T any](data []T, count int, val T) {
	for i := 0; i < count; i++ {
		data[i] = val
	}
}

func toSlice[T any](data []byte, pSize int) []T {
	slen := len(data) / pSize
	return unsafe.Slice((*T)(unsafe.Pointer(unsafe.SliceData(data))), slen)
}

func bytesSliceToPointer(data []byte) unsafe.Pointer {
	return unsafe.Pointer(unsafe.SliceData(data))
}

func pointerAdd(base unsafe.Pointer, offset int) unsafe.Pointer {
	return unsafe.Add(base, offset)
}

func pointerLess(lhs, rhs unsafe.Pointer) bool {
	return uintptr(lhs) < uintptr(rhs)
}

func pointerLessEqual(lhs, rhs unsafe.Pointer) bool {
	return uintptr(lhs) <= uintptr(rhs)
}

func pointerSub(lhs, rhs unsafe.Pointer) int64 {
	a := uint64(uintptr(lhs))
	b := uint64(uintptr(rhs))
	//uint64
	ret0 := a - b
	ret := int64(ret0)
	if a < b {
		ret = -ret
	}
	return ret
}

func pointerToSlice[T any](base unsafe.Pointer, len int) []T {
	return unsafe.Slice((*T)(base), len)
}

func pointerCopy(dst, src unsafe.Pointer, len int) {
	dstSlice := pointerToSlice[byte](dst, len)
	srcSlice := pointerToSlice[byte](src, len)
	copy(dstSlice, srcSlice)
}

func pointerValid(ptr unsafe.Pointer) bool {
	return uintptr(ptr) != 0
}

func pointerMemcmp(lAddr, rAddr unsafe.Pointer, len int) int {
	lSlice := pointerToSlice[byte](lAddr, len)
	rSlice := pointerToSlice[byte](rAddr, len)
	ret := bytes.Compare(lSlice, rSlice)

	if len == 12 {
		ls := string(lSlice[1:])
		rs := string(rSlice[1:])
		if ls == "MOROCCO" && rs == "IRAQ" ||
			ls == "IRAQ" && rs == "MOROCCO" {
			fmt.Println("memcmp",
				ret,
				lSlice[0], ls,
				rSlice[0], rs)
		}

	}

	return ret
}

func invertBits(base unsafe.Pointer, offset int) {
	ptr := pointerAdd(base, offset)
	b := load[byte](ptr)
	b = ^b
	store[byte](b, ptr)
}

func findIf[T ~*Expr | ~string | ~int](data []T, pred func(t T) bool) int {
	for i, ele := range data {
		if pred(ele) {
			return i
		}
	}
	return -1
}

func cMalloc(sz int) unsafe.Pointer {
	return C.malloc(C.size_t(sz))
}

func cFree(ptr unsafe.Pointer) {
	C.free(ptr)
}

func cRealloc(ptr unsafe.Pointer, sz int) unsafe.Pointer {
	return C.realloc(ptr, C.size_t(sz))
}

func EntriesPerBlock(width int) int {
	return BLOCK_SIZE / width
}

func back[T any](data []T) T {
	l := len(data)
	if l == 0 {
		panic("empty slice")
	} else if l == 1 {
		return data[0]
	}
	return data[l-1]
}

func size[T any](data []T) int {
	return len(data)
}

func empty[T any](data []T) bool {
	return size(data) == 0
}

// removeIf removes the one that pred is true.
func removeIf[T any](data []T, pred func(t T) bool) []T {
	if len(data) == 0 {
		return data
	}
	res := 0
	for i := 0; i < len(data); i++ {
		if !pred(data[i]) {
			if res != i {
				data[res] = data[i]
			}
			res++
		}
	}
	return data[:res]
}

func greaterFloat[T ~float32 | ~float64](lhs, rhs T) bool {
	lIsNan := math.IsNaN(float64(lhs))
	rIsNan := math.IsNaN(float64(rhs))
	if rIsNan {
		return false
	}
	if lIsNan {
		return true
	}
	return lhs > rhs
}

func alignValue(value int) int {
	return (value + 7) & (^7)
}
