package main

import "testing"

func TestAst(t *testing.T) {
	var a *Ast = &Ast{}
	a.Typ = AstTypeSelect
}
