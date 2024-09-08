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

// (a and b1) or (a and b2) or (a and b3) => a and (b1 or b2 or b3)
func distributeExpr(expr *Expr) *Expr {
	if expr == nil {
		return nil
	}
	orExprs := splitExprByOr(expr)
	candidates := splitExprByAnd(orExprs[0])
	for i := 1; i < len(orExprs); i++ {
		next := splitExprByAnd(orExprs[i])
		intersect := make([]*Expr, 0)
		for _, cand := range candidates {
			if hasExpr(cand, next) {
				intersect = append(intersect, cand)
			}
		}
		candidates = intersect
	}
	if len(candidates) == 0 {
		//no common expr. return original
		return expr
	}

	//remove cand from children in original exprs
	resChildren := make([]*Expr, 0)
	for _, orExpr := range orExprs {
		elems := splitExprByAnd(orExpr)
		var retElems []*Expr
		for _, cand := range candidates {
			retElems = removeIf[*Expr](elems, func(t *Expr) bool {
				return t == nil || cand.equal(t)
			})
		}
		if len(retElems) > 0 {
			resChildren = append(resChildren, combineExprsByAnd(retElems...))
		}
	}
	var newRoot *Expr
	//assertFunc(len(resChildren) > 0)
	//result: candidates && (resChildren[0] || resChildren[1] ...)
	if len(resChildren) == 1 {
		newRoot = andExpr(combineExprsByAnd(candidates...), resChildren[0])
	} else if len(resChildren) > 1 {
		newCandidates := combineExprsByAnd(candidates...)
		newChildren := combineExprsByOr(resChildren...)
		newRoot = andExpr(newCandidates, newChildren)
	} else {
		newRoot = combineExprsByAnd(candidates...)
	}
	if len(newRoot.Children) == 1 {
		return newRoot.Children[0]
	}
	return newRoot
}

func hasExpr(expr *Expr, exprList []*Expr) bool {
	for _, e := range exprList {
		if expr.equal(e) {
			return true
		}
	}
	return false
}

func distributeExprs(exprs ...*Expr) []*Expr {
	res := make([]*Expr, 0)
	for _, expr := range exprs {
		res = append(res, distributeExpr(expr))
	}
	return res
}
