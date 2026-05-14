# Plan Expression 重构方案

## 1. 现状分析

### 1.1 DuckDB Expression 架构

DuckDB 采用经典的 C++ 继承体系：

```
BaseExpression (parser)
  ├── type: ExpressionType
  ├── expression_class: ExpressionClass
  ├── alias, query_location
  └── 虚方法: IsAggregate(), IsWindow(), HasSubquery(), IsScalar(), HasParameter()
      GetName(), ToString(), Hash(), Equals(), Verify()

Expression (planner) extends BaseExpression
  ├── return_type: LogicalType
  ├── verification_stats
  └── 虚方法: IsVolatile(), IsConsistent(), PropagatesNullValues(), IsFoldable(), CanThrow()
      Copy() [纯虚], Serialize(), Deserialize()

各子类 (BoundConstantExpression, BoundColumnRefExpression, BoundFunctionExpression, BoundAggregateExpression, ...)
  └── 按需覆盖上述虚方法
```

**核心设计：递归属性计算**

DuckDB 的 Expression 基类为所有属性提供**默认递归实现**：

```cpp
bool Expression::IsFoldable() const {
    bool is_foldable = true;
    ExpressionIterator::EnumerateChildren(*this, [&](const Expression &child) {
        if (!child.IsFoldable()) is_foldable = false;
    });
    return is_foldable;
}
```

子类只需覆盖**叶子节点**的特殊行为：
- `BoundConstantExpression`: 总是 `IsFoldable()=true`
- `BoundColumnRefExpression`: 总是 `IsFoldable()=false`
- `BoundFunctionExpression`: 取决于 `function.GetStability()`
- `BoundAggregateExpression`: 总是 `IsFoldable()=false`

### 1.2 Plan Expr 现状

Plan 采用 Go struct + `any Info` 的扁平化设计：

```go
type Expr struct {
    BaseInfo              // Database, Table, Name, Alias, ColRef, Depth, BelongCtx
    ConstValue ConstValue // 常量值
    Typ        ET         // 表达式类型枚举
    DataTyp    common.LType
    Index      uint64
    Children   []*Expr
    Info       any        // FunctionInfo | SubqueryInfo | JoinInfo | ...
}
```

**已有能力：**
- `equal()` — 结构深比较（函数名、聚合类型、连接类型等）
- `copy()` — 结构深拷贝（递归复制 Children 和 Info）
- `String()` — 通过 `explainExpr` 格式化输出
- `copyExpr()` — 使用 `go-clone` 库做反射深拷贝

**关键缺失：**

| 能力 | DuckDB | Plan | 影响 |
|------|--------|------|------|
| `Hash()` | 有，每个子类实现 | ❌ 无 | 无法用于 hash map（去重、memoization） |
| `Equals()` | 有，公共虚方法 | ⚠️ 仅有私有 `equal()` | 外部无法比较表达式 |
| `Copy()` | 有，公共纯虚方法 | ⚠️ 仅有私有 `copy()` | 外部无法安全复制 |
| `IsConstant()` | 通过 `IsFoldable()` 推断 | ❌ 无 | 无法判断表达式是否为常量 |
| `IsFoldable()` | 递归虚方法 | ❌ 仅有 stub matcher | 常量折叠规则无法识别可折叠表达式 |
| `IsVolatile()` | 递归虚方法 | ❌ 仅有 stub matcher | 无法判断表达式是否稳定 |
| `IsConsistent()` | 递归虚方法 | ❌ 无 | 缓存/物化视图无法判断表达式稳定性 |
| `IsScalar()` | 递归虚方法 | ❌ 无 | 无法判断是否为纯标量（无列引用） |
| `IsAggregate()` | 递归虚方法 | ❌ 无 | 需手动检查 `FunImpl._funcTyp` |
| `IsWindow()` | 递归虚方法 | ❌ 无 | 不支持窗口函数分析 |
| `HasSubquery()` | 递归虚方法 | ❌ 无 | 需手动遍历检查 |
| `HasParameter()` | 递归虚方法 | ❌ 无 | 不支持参数化查询分析 |
| `PropagatesNullValues()` | 递归虚方法 | ❌ 无 | 空值传播优化无法实施 |
| `CanThrow()` | 递归虚方法 | ❌ 无 | 错误处理优化受限 |
| `Verify()` | 虚方法 | ❌ 无 | 缺少表达式合法性校验 |
| `ExpressionClass` | 有（BOUND_CONSTANT, BOUND_FUNCTION, ...） | ⚠️ 仅有 `ET` | matcher 框架用 `ET` 区分，粒度不够 |
| 函数稳定性 | `FunctionStability` (VOLATILE/CONSISTENT/STABLE) | ⚠️ 仅有 `FuncSideEffects` | 无法区分 volatile 和 consistent |
| 函数抛错能力 | `FunctionErrors` (CAN_THROW_RUNTIME_ERROR) | ❌ 无 | `CanThrow()` 无法实施 |
| ExpressionIterator | 有通用迭代器 | ❌ 无 | 所有递归遍历都是 ad-hoc |

---

## 2. 重构目标

使 Plan `Expr` 支持 DuckDB Expression 的完整能力，具体包括：

1. **公共 API 化** — `Equal`, `Hash`, `Copy` 成为公共方法
2. **属性查询体系** — 实现 `IsConstant`, `IsFoldable`, `IsVolatile`, `IsScalar`, `IsAggregate`, `HasSubquery` 等递归属性方法
3. **函数元数据扩展** — 补充 `FunctionStability`、`FunctionErrors` 等函数属性
4. **通用迭代器** — 实现 `ExprIterator` 替代所有 ad-hoc 递归
5. **表达式重写框架** — 在 matcher 基础上添加 `ExprRewriter` 规则引擎

---

## 3. 详细重构方案

### 3.1 Phase 1: 公共 API 化 (低风险)

**目标：** 将现有的 `equal()` 和 `copy()` 提升为公共方法，新增 `Hash()`。

```go
// expr.go — 现有方法改为公共方法

// Equal 报告两个表达式是否结构相等（深度比较）。
func (e *Expr) Equal(o *Expr) bool { /* 现有 equal() 逻辑 */ }

// Copy 返回表达式的深度拷贝。
func (e *Expr) Copy() *Expr { /* 现有 copy() 逻辑 */ }

// Hash 返回表达式的 64-bit 哈希值。
// 要求: Equal(a, b) => Hash(a) == Hash(b)
func (e *Expr) Hash() uint64 {
    h := fnv.New64a()
    e.writeHash(h)
    return h.Sum64()
}

func (e *Expr) writeHash(h hash.Hash64) {
    binary.Write(h, binary.LittleEndian, int32(e.Typ))
    binary.Write(h, binary.LittleEndian, int32(e.DataTyp.Id))
    // ... 各类型具体哈希逻辑
}
```

**兼容性处理：**
- 保留现有的私有方法作为内部调用（或直接用公共方法替换）
- `copyExpr(e)` 可以改为直接调用 `e.Copy()`

**测试要求：**
- `Equal` 的自反性、对称性、传递性
- `Hash` 与 `Equal` 的一致性（相同表达式必有相同哈希）
- `Copy` 后的表达式与原表达式 `Equal` 但指针不同

---

### 3.2 Phase 2: 属性查询体系 (中风险)

**目标：** 在 `Expr` 上实现 DuckDB 风格的递归属性查询。

Go 没有虚方法，但通过 `switch e.Typ` 可以实现类似效果：

```go
// expr_properties.go — 新增文件

package compute

// IsConstant 返回表达式是否为常量（不含列引用、子查询、参数）。
// 等价于 DuckDB 的 IsScalar() && !HasSubquery() && !HasParameter() && IsFoldable()
func (e *Expr) IsConstant() bool {
    if e == nil {
        return true
    }
    switch e.Typ {
    case ET_Column:
        return false
    case ET_Subquery:
        return false
    case ET_Func:
        // 函数是否稳定且所有子节点都是常量
        if e.isVolatileFunc() {
            return false
        }
        for _, child := range e.Children {
            if !child.IsConstant() {
                return false
            }
        }
        return true
    case ET_Const:
        return true
    default:
        for _, child := range e.Children {
            if !child.IsConstant() {
                return false
            }
        }
        return true
    }
}

// IsFoldable 返回表达式是否可以在编译期折叠为常量。
// 与 IsConstant 的区别：允许引用已绑定为常量的列（如 LIMIT 参数）。
func (e *Expr) IsFoldable() bool {
    if e == nil {
        return true
    }
    switch e.Typ {
    case ET_Column:
        return false
    case ET_Subquery:
        return false
    case ET_Func:
        if e.isVolatileFunc() {
            return false
        }
        for _, child := range e.Children {
            if !child.IsFoldable() {
                return false
            }
        }
        return true
    case ET_Const:
        return true
    default:
        for _, child := range e.Children {
            if !child.IsFoldable() {
                return false
            }
        }
        return true
    }
}

// IsVolatile 返回表达式是否包含 volatile 函数（如 random(), now()）。
func (e *Expr) IsVolatile() bool {
    if e == nil {
        return false
    }
    switch e.Typ {
    case ET_Func:
        if e.isVolatileFunc() {
            return true
        }
        for _, child := range e.Children {
            if child.IsVolatile() {
                return true
            }
        }
        return false
    default:
        for _, child := range e.Children {
            if child.IsVolatile() {
                return true
            }
        }
        return false
    }
}

// IsScalar 返回表达式是否为标量（不含列引用、聚合、窗口函数）。
func (e *Expr) IsScalar() bool {
    if e == nil {
        return true
    }
    switch e.Typ {
    case ET_Column:
        return false
    case ET_Func:
        if e.isAggregateFunc() || e.isWindowFunc() {
            return false
        }
        for _, child := range e.Children {
            if !child.IsScalar() {
                return false
            }
        }
        return true
    default:
        for _, child := range e.Children {
            if !child.IsScalar() {
                return false
            }
        }
        return true
    }
}

// IsAggregate 返回表达式是否包含聚合函数。
func (e *Expr) IsAggregate() bool {
    if e == nil {
        return false
    }
    switch e.Typ {
    case ET_Func:
        if e.isAggregateFunc() {
            return true
        }
        for _, child := range e.Children {
            if child.IsAggregate() {
                return true
            }
        }
        return false
    default:
        for _, child := range e.Children {
            if child.IsAggregate() {
                return true
            }
        }
        return false
    }
}

// HasSubquery 返回表达式是否包含子查询。
func (e *Expr) HasSubquery() bool {
    if e == nil {
        return false
    }
    if e.Typ == ET_Subquery {
        return true
    }
    for _, child := range e.Children {
        if child.HasSubquery() {
            return true
        }
    }
    return false
}

// PropagatesNullValues 返回表达式是否传播空值。
// 对于大部分函数/运算符，如果任一输入为 NULL 则输出 NULL。
// 特殊情况：IS NULL, IS NOT NULL, COALESCE, CASE, AND, OR 等不传播。
func (e *Expr) PropagatesNullValues() bool {
    if e == nil {
        return true
    }
    switch e.Typ {
    case ET_Func:
        name := e.GetFuncInfo().FunImpl._name
        switch name {
        case FuncAnd, FuncOr, FuncBetween, FuncCase, FuncCaseWhen:
            return false
        }
        // 函数级别的 null handling
        if e.GetFuncInfo().FunImpl._nullHandling == SpecialHandling {
            return false
        }
        for _, child := range e.Children {
            if !child.PropagatesNullValues() {
                return false
            }
        }
        return true
    default:
        for _, child := range e.Children {
            if !child.PropagatesNullValues() {
                return false
            }
        }
        return true
    }
}
```

**内部辅助方法：**

```go
// expr.go — 新增内部辅助

func (e *Expr) isVolatileFunc() bool {
    if e.Typ != ET_Func || e.GetFuncInfo().FunImpl == nil {
        return false
    }
    return e.GetFuncInfo().FunImpl._stability == VolatileFunction
}

func (e *Expr) isAggregateFunc() bool {
    if e.Typ != ET_Func || e.GetFuncInfo().FunImpl == nil {
        return false
    }
    return e.GetFuncInfo().FunImpl._funcTyp == AggregateFuncType
}

func (e *Expr) isWindowFunc() bool {
    if e.Typ != ET_Func || e.GetFuncInfo().FunImpl == nil {
        return false
    }
    // TODO: 当添加 WindowFuncType 时更新
    return false
}
```

**对现有代码的影响：**
- `StableExpressionMatcher` 可以从 stub 改为使用 `e.IsVolatile()`
- `FoldableConstantMatcher` 可以从 stub 改为使用 `e.IsFoldable()`
- `AggregateExprMatcher` 可以使用 `e.IsAggregate()` 做前置检查
- 消除 `expr_exec.go` 等处对 `ET_Subquery` 的 ad-hoc 检查

---

### 3.3 Phase 3: 函数元数据扩展 (中风险)

**目标：** 为 `Function` 结构体添加 DuckDB 风格的稳定性和抛错属性。

```go
// function.go — 扩展现有枚举

// FunctionStability 表示函数在不同调用间返回相同结果的能力。
type FunctionStability int

const (
    // VolatileFunction: 每次调用可能返回不同结果（如 random(), now()）。
    // 不可折叠，不可缓存，不可下推。
    VolatileFunction FunctionStability = iota
    // StableFunction: 单次查询内返回相同结果（如 current_database()）。
    // 可在查询级别折叠，但不能跨查询缓存。
    StableFunction
    // ConsistentFunction: 给定相同输入永远返回相同结果（如 +, sin(), upper()）。
    // 可安全折叠和缓存。
    ConsistentFunction
)

// FunctionErrorMode 表示函数是否可能抛出运行时错误。
type FunctionErrorMode int

const (
    // NoRuntimeError: 函数不会抛出运行时错误（如 +, -）。
    NoRuntimeError FunctionErrorMode = iota
    // CanThrowRuntimeError: 函数可能因输入抛出错误（如 cast(), / by zero）。
    CanThrowRuntimeError
)

// Function 结构体扩展
type Function struct {
    _name         string
    _args         []common.LType
    _retType      common.LType
    _funcTyp      FuncType
    _sideEffects  FuncSideEffects
    _nullHandling FuncNullHandling
    _aggrType     AggrType

    // === 新增字段 ===
    _stability   FunctionStability   // 默认 ConsistentFunction
    _errorMode   FunctionErrorMode   // 默认 NoRuntimeError
    // ===============

    _scalar        ScalarFunc
    _bind          bindScalarFunc
    _boundCastInfo *BoundCastInfo
    _stateSize     aggrStateSize
    _init          aggrInit
    _update        aggrUpdate
    _combine       aggrCombine
    _finalize      aggrFinalize
    _simpleUpdate  aggrSimpleUpdate
}
```

**为现有函数注册默认稳定性：**

```go
// function_scalar.go — 在注册函数时设置稳定性

// 算术函数 — Consistent
set := NewFunctionSet(FuncAdd, ScalarFuncType)
set.Add(&Function{
    _name:    FuncAdd,
    _funcTyp: ScalarFuncType,
    _stability: ConsistentFunction,  // 新增
    _errorMode: NoRuntimeError,      // 新增
    // ...
})

// 随机函数 — Volatile（如果未来添加）
// set.Add(&Function{
//     _name:    "random",
//     _stability: VolatileFunction,
// })

// Cast 函数 — CanThrowRuntimeError
set = NewFunctionSet(FuncCast, ScalarFuncType)
set.Add(&Function{
    _name:      FuncCast,
    _stability: ConsistentFunction,
    _errorMode: CanThrowRuntimeError, // cast 可能失败
})
```

**新增 Function 查询方法：**

```go
// function.go

func (fun *Function) GetStability() FunctionStability {
    if fun == nil {
        return ConsistentFunction
    }
    return fun._stability
}

func (fun *Function) GetErrorMode() FunctionErrorMode {
    if fun == nil {
        return NoRuntimeError
    }
    return fun._errorMode
}

func (fun *Function) IsVolatile() bool {
    return fun.GetStability() == VolatileFunction
}

func (fun *Function) CanThrow() bool {
    return fun.GetErrorMode() == CanThrowRuntimeError
}
```

---

### 3.4 Phase 4: 通用表达式迭代器 (低风险)

**目标：** 替代所有 ad-hoc 的递归遍历。

```go
// expr_iterator.go — 新增文件

package compute

// ExprIterator 提供 Expression 树的通用遍历。
type ExprIterator struct{}

// EnumerateChildren 对 expr 的所有直接子节点调用 f。
func EnumerateChildren(expr *Expr, f func(child *Expr)) {
    if expr == nil {
        return
    }
    for _, child := range expr.Children {
        f(child)
    }
}

// EnumerateExpressions 递归遍历 expr 树中的所有节点（前序）。
func EnumerateExpressions(expr *Expr, f func(node *Expr)) {
    if expr == nil {
        return
    }
    f(expr)
    for _, child := range expr.Children {
        EnumerateExpressions(child, f)
    }
}

// EnumerateLeafs 递归遍历 expr 树中的所有叶子节点。
func EnumerateLeafs(expr *Expr, f func(leaf *Expr)) {
    if expr == nil {
        return
    }
    if len(expr.Children) == 0 {
        f(expr)
        return
    }
    for _, child := range expr.Children {
        EnumerateLeafs(child, f)
    }
}

// TransformExpressions 对表达式树做自底向上变换。
// f 返回非 nil 时替换当前节点，返回 nil 时保留原节点。
func TransformExpressions(expr *Expr, f func(node *Expr) *Expr) *Expr {
    if expr == nil {
        return nil
    }
    for i, child := range expr.Children {
        expr.Children[i] = TransformExpressions(child, f)
    }
    if replacement := f(expr); replacement != nil {
        return replacement
    }
    return expr
}
```

**替换现有 ad-hoc 递归：**

可以用 `EnumerateExpressions` 替换以下函数中的手动递归：
- `collectColRefs`
- `collectTableRefers`
- `referTo`
- `onlyReferTo`
- `decideSide`
- `replaceColRef`
- `restoreExpr`
- `checkColRefPos`

---

### 3.5 Phase 5: 表达式重写框架 (中高风险)

**目标：** 在 matcher 基础上实现 DuckDB 风格的 `ExpressionRewriter`。

```go
// expr_rewriter.go — 新增文件

package compute

// RewriteRule 表示一个表达式重写规则。
type RewriteRule interface {
    // Root 返回该规则的根 matcher。
    Root() ExprMatcher
    // Apply 对匹配到的表达式执行重写。返回新的表达式（或 nil 表示无变化）。
    // bindings[0] 是根表达式，后续为子 matcher 捕获的绑定。
    Apply(root *Expr, bindings []*Expr) (*Expr, bool)
}

// ExpressionRewriter 应用一组重写规则到表达式树。
type ExpressionRewriter struct {
    rules []RewriteRule
}

func NewExpressionRewriter(rules []RewriteRule) *ExpressionRewriter {
    return &ExpressionRewriter{rules: rules}
}

// Rewrite 对表达式树应用所有规则，直到到达不动点。
func (rw *ExpressionRewriter) Rewrite(expr *Expr) *Expr {
    changed := true
    for changed {
        changed = false
        expr = TransformExpressions(expr, func(node *Expr) *Expr {
            for _, rule := range rw.rules {
                var bindings []*Expr
                if ok, newBindings := rule.Root().Match(node, bindings); ok {
                    if result, modified := rule.Apply(node, newBindings); modified {
                        changed = true
                        return result
                    }
                }
            }
            return nil
        })
    }
    return expr
}
```

**示例规则实现（常量折叠）：**

```go
// rule_constant_folding.go — 新增文件

type ConstantFoldingRule struct{}

func (r *ConstantFoldingRule) Root() ExprMatcher {
    // 匹配任何可折叠的表达式（非 ET_Const 本身）
    return &foldableConstantMatcher{}
}

func (r *ConstantFoldingRule) Apply(root *Expr, bindings []*Expr) (*Expr, bool) {
    if root.Typ == ET_Const {
        return nil, false // 已经是常量，无需折叠
    }
    if !root.IsFoldable() {
        return nil, false
    }
    // TODO: 使用 ExpressionExecutor 执行表达式并提取结果值
    // value := EvaluateScalar(root)
    // return &Expr{Typ: ET_Const, ConstValue: value, DataTyp: root.DataTyp}, true
    return nil, false
}
```

---

## 4. 实施优先级

| 优先级 | 阶段 | 工作量 | 价值 | 依赖 |
|--------|------|--------|------|------|
| P0 | Phase 1: 公共 API 化 | 小 | 高 | 无 |
| P0 | Phase 3: 函数元数据扩展 | 中 | 高 | 无 |
| P1 | Phase 2: 属性查询体系 | 中 | 高 | Phase 3 |
| P1 | Phase 4: 通用迭代器 | 小 | 中 | 无 |
| P2 | Phase 5: 重写框架 | 大 | 高 | Phase 1+2 |

**推荐实施顺序：**

1. **先完成 Phase 1 + Phase 3**（1-2 天）
   - 将 `equal/copy` 公共化
   - 添加 `Hash()`
   - 为 `Function` 添加 `_stability` / `_errorMode`
   - 为所有现有函数设置正确的默认值

2. **然后 Phase 2**（2-3 天）
   - 实现所有属性查询方法
   - 替换 `FoldableConstantMatcher` / `StableExpressionMatcher` 的 stub 实现
   - 编写属性方法的单元测试

3. **随后 Phase 4**（1 天）
   - 实现 `ExprIterator`
   - 逐步替换现有 ad-hoc 递归

4. **最后 Phase 5**（3-5 天）
   - 实现 `ExpressionRewriter`
   - 迁移常量折叠、算术简化等规则
   - 与 matcher 框架集成测试

---

## 5. 关键设计决策

### 5.1 为什么不用 Go 接口继承？

DuckDB 使用 C++ 虚方法实现多态。在 Go 中可以有三种选择：

1. **接口 + 类型断言**（类似 visitor 模式）
   - 优点：类型安全，可扩展
   - 缺点：每个操作都要做类型断言，代码分散

2. **单一 struct + switch Typ**（当前方案）
   - 优点：简单，所有逻辑集中
   - 缺点：`any Info` 失去类型安全，switch 可能变长

3. **接口 + 每个 ET 一个实现类型**
   - 优点：最类型安全
   - 缺点：改动巨大，与现有代码不兼容

**建议：** 保持当前的 "单一 struct + switch" 模式，但通过以下方式改善：
- 将 `any Info` 的访问封装在 getter 中（已部分完成）
- 属性方法使用 `switch e.Typ` 做分发（类似 DuckDB 的虚表）
- 未来如果 ET 数量继续增长，再考虑接口化

### 5.2 Hash 函数选择

DuckDB 使用自定义的 `hash_t`（通常为 64-bit）和 `CombineHash`。在 Go 中：

```go
import "hash/fnv"

func (e *Expr) Hash() uint64 {
    h := fnv.New64a()
    e.writeHash(h)
    return h.Sum64()
}
```

或使用更简单的方式：

```go
func hashCombine(h uint64, v uint64) uint64 {
    return h ^ (v + 0x9e3779b97f4a7c15 + (h<<6) + (h>>2))
}
```

### 5.3 与现有 matcher 框架的关系

Phase 2 的属性方法**不是替代 matcher**，而是补充：

- **Matcher**：用于**模式匹配**（"这个表达式是不是 `+(const, col)` 的形式？"）
- **属性方法**：用于**属性查询**（"这个表达式能不能被折叠？"）

两者协作：
```go
// 常量折叠规则
func (r *ConstantFoldingRule) Apply(root *Expr, bindings []*Expr) (*Expr, bool) {
    if !root.IsFoldable() {  // 属性方法检查
        return nil, false
    }
    // 执行折叠...
}
```

---

## 6. 文件变更计划

| 文件 | 变更 |
|------|------|
| `expr.go` | `equal()` → `Equal()`, `copy()` → `Copy()`, 新增 `Hash()`, 新增 `isVolatileFunc()`/`isAggregateFunc()`/`isWindowFunc()` |
| `expr_properties.go` | **新增** — `IsConstant()`, `IsFoldable()`, `IsVolatile()`, `IsScalar()`, `IsAggregate()`, `HasSubquery()`, `PropagatesNullValues()`, `CanThrow()` |
| `expr_iterator.go` | **新增** — `EnumerateChildren()`, `EnumerateExpressions()`, `EnumerateLeafs()`, `TransformExpressions()` |
| `expr_rewriter.go` | **新增** — `RewriteRule`, `ExpressionRewriter` |
| `function.go` | 新增 `FunctionStability`, `FunctionErrorMode` 枚举；`Function` 结构体扩展 `_stability`/`_errorMode`；新增 getter 方法 |
| `function_scalar.go` | 为所有函数注册设置 `_stability`/`_errorMode` |
| `matcher.go` | `FoldableConstantMatcher` 使用 `IsFoldable()`；`StableExpressionMatcher` 使用 `IsVolatile()` |
| `expr_test.go` | **新增** — Equal/Hash/Copy 的单元测试；属性方法的单元测试 |
| `expr_iterator_test.go` | **新增** — 迭代器的单元测试 |

---

## 7. 附录：DuckDB ↔ Plan 概念映射

| DuckDB | Plan |
|--------|------|
| `ExpressionClass` | `ET` (类型粒度不同，DuckDB 更细) |
| `ExpressionType` | `ET` + 函数名 |
| `LogicalType return_type` | `common.LType DataTyp` |
| `BaseExpression::Equals()` | `Expr.equal()` → `Expr.Equal()` |
| `BaseExpression::Hash()` | ❌ → `Expr.Hash()` |
| `Expression::Copy()` | `Expr.copy()` → `Expr.Copy()` |
| `Expression::IsConstant()` | ❌ → `Expr.IsConstant()` |
| `Expression::IsFoldable()` | `FoldableConstantMatcher` (stub) → `Expr.IsFoldable()` |
| `Expression::IsVolatile()` | `StableExpressionMatcher` (stub) → `Expr.IsVolatile()` |
| `Expression::IsScalar()` | ❌ → `Expr.IsScalar()` |
| `Expression::IsAggregate()` | 手动检查 `_funcTyp` → `Expr.IsAggregate()` |
| `Expression::HasSubquery()` | 手动遍历 → `Expr.HasSubquery()` |
| `Expression::PropagatesNullValues()` | ❌ → `Expr.PropagatesNullValues()` |
| `Expression::CanThrow()` | ❌ → `Expr.CanThrow()` |
| `FunctionStability` | ❌ → `Function._stability` |
| `FunctionErrors` | ❌ → `Function._errorMode` |
| `ExpressionIterator` | ad-hoc 递归 → `ExprIterator` |
| `ExpressionRewriter` | ❌ → `ExpressionRewriter` |
