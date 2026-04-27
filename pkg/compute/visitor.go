// Copyright 2023-2026 daviszhen
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

package compute

// LogicalOperatorVisitor defines the visitor pattern for LogicalOperator trees.
// Mirrors DuckDB's LogicalOperatorVisitor. All methods are virtual (overridable).
type LogicalOperatorVisitor interface {
	VisitOperator(op *LogicalOperator)
	VisitOperatorChildren(op *LogicalOperator)
	VisitOperatorExpressions(op *LogicalOperator)
	VisitExpression(expr **Expr)
	VisitExpressionChildren(expr *Expr)
	VisitReplace(expr *Expr) *Expr
}

// BaseVisitor provides default implementations for LogicalOperatorVisitor.
// Embed it and set Self to the outermost struct to get virtual dispatch.
type BaseVisitor struct {
	Self LogicalOperatorVisitor
}

func (v *BaseVisitor) VisitOperator(op *LogicalOperator) {
	DefaultVisitOperator(v.Self, op)
}

func (v *BaseVisitor) VisitOperatorChildren(op *LogicalOperator) {
	DefaultVisitOperatorChildren(v.Self, op)
}

func (v *BaseVisitor) VisitOperatorExpressions(op *LogicalOperator) {
	DefaultVisitOperatorExpressions(v.Self, op)
}

func (v *BaseVisitor) VisitExpression(expr **Expr) {
	DefaultVisitExpression(v.Self, expr)
}

func (v *BaseVisitor) VisitExpressionChildren(expr *Expr) {
	DefaultVisitExpressionChildren(v.Self, expr)
}

func (v *BaseVisitor) VisitReplace(expr *Expr) *Expr {
	return nil
}

// --- Default implementations (accept self for virtual dispatch) ---

func DefaultVisitOperator(self LogicalOperatorVisitor, op *LogicalOperator) {
	self.VisitOperatorChildren(op)
	self.VisitOperatorExpressions(op)
}

func DefaultVisitOperatorChildren(self LogicalOperatorVisitor, op *LogicalOperator) {
	for _, child := range op.Children {
		self.VisitOperator(child)
	}
}

func DefaultVisitOperatorExpressions(self LogicalOperatorVisitor, op *LogicalOperator) {
	EnumerateExpressions(op, func(expr **Expr) {
		self.VisitExpression(expr)
	})
}

func DefaultVisitExpression(self LogicalOperatorVisitor, expr **Expr) {
	result := self.VisitReplace(*expr)
	if result != nil {
		*expr = result
	} else {
		self.VisitExpressionChildren(*expr)
	}
}

func DefaultVisitExpressionChildren(self LogicalOperatorVisitor, expr *Expr) {
	EnumerateExprChildren(expr, func(child **Expr) {
		self.VisitExpression(child)
	})
}

// --- Static enumeration functions ---

// EnumerateExpressions enumerates all expression pointer locations on a LogicalOperator.
// Mirrors DuckDB LogicalOperatorVisitor::EnumerateExpressions (static).
func EnumerateExpressions(op *LogicalOperator, callback func(expr **Expr)) {
	for i := range op.Filters {
		callback(&op.Filters[i])
	}
	for i := range op.Projects {
		callback(&op.Projects[i])
	}
	for i := range op.Outputs {
		callback(&op.Outputs[i])
	}
	switch info := op.Info.(type) {
	case *JoinOpInfo:
		for i := range info.OnConds {
			callback(&info.OnConds[i])
		}
	case *AggOpInfo:
		for i := range info.GroupBys {
			callback(&info.GroupBys[i])
		}
		for i := range info.Aggs {
			callback(&info.Aggs[i])
		}
	case *OrderOpInfo:
		for i := range info.OrderBys {
			callback(&info.OrderBys[i])
		}
	case *LimitOpInfo:
		if info.Limit != nil {
			callback(&info.Limit)
		}
		if info.Offset != nil {
			callback(&info.Offset)
		}
	}
}

// EnumerateExprChildren enumerates all child expression pointer locations of an Expr.
// Mirrors DuckDB ExpressionIterator::EnumerateChildren.
func EnumerateExprChildren(expr *Expr, callback func(child **Expr)) {
	if expr == nil {
		return
	}
	for i := range expr.Children {
		callback(&expr.Children[i])
	}
}
