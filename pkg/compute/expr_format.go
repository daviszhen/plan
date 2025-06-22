package compute

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/daviszhen/plan/pkg/common"
)

type ExplainBuffer struct {
	lines []string
}

func NewExplainBuffer() *ExplainBuffer {
	return &ExplainBuffer{
		lines: make([]string, 0),
	}
}

func (eb *ExplainBuffer) getOffset(level int) int {
	if level <= 0 {
		return 2
	} else {
		return 2 + level*6
	}
}

func (eb *ExplainBuffer) AppendLine(line string, level int, newNode bool) {
	prefix := ""
	if level <= 0 {
		if newNode {
			prefix = ""
		} else {
			prefix = "  "
		}
	} else {
		offset := eb.getOffset(level)
		if newNode {
			pad := offset - 6
			if pad < 0 {
				pad = 0
			}
			prefix = strings.Repeat(" ", pad) + "->  "
		} else {
			prefix = strings.Repeat(" ", offset)
		}
	}
	eb.lines = append(eb.lines, prefix+line)
}

func (eb *ExplainBuffer) AppendTitle(title string) {
	eb.lines = append(eb.lines, title)
}

func (eb *ExplainBuffer) String() string {
	return strings.Join(eb.lines, "\n")
}

type ExplainPosition struct {
	level  int
	indent int
}

const (
	ExplainOptionAnalyze     = 1
	ExplainOptionVerbose     = 2
	ExplainOptionCosts       = 3
	ExplainOptionSettings    = 4
	ExplainOptionGenericPlan = 5
	ExplainOptionBuffers     = 6
	ExplainOptionSerialize   = 7
	ExplainOptionWal         = 8
	ExplainOptionTiming      = 9
	ExplainOptionSummary     = 10
	ExplainOptionMemory      = 11
	ExplainOptionFormat      = 12

	ExplainSerializeNone   = 0
	ExplainSerializeText   = 1
	ExplainSerializeBinary = 2

	ExplainFormatText = 0
	ExplainFormatJSON = 1
	ExplainFormatYAML = 2
	ExplainFormatXML  = 3
)

var ExplainOptionNames = []string{
	"Invalid Explain Option",
	"Analyze",
	"Verbose",
	"Costs",
	"Settings",
	"GenericPlan",
	"Buffers",
	"Serialize",
	"Wal",
	"Timing",
	"Summary",
	"Memory",
	"Format",
}

type ExplainOption struct {
	enabled   bool
	serialize int
	format    int
}

type ExplainOptions struct {
	options [ExplainOptionFormat + 1]ExplainOption
}

func NewExplainOptions() *ExplainOptions {
	ret := &ExplainOptions{
		options: [ExplainOptionFormat + 1]ExplainOption{},
	}
	ret.SetDefaultValues()
	return ret
}

func (eopt *ExplainOptions) String() string {
	buf := bytes.NewBuffer(nil)
	for i := 1; i <= ExplainOptionFormat; i++ {
		if !eopt.options[i].enabled {
			continue
		}
		if buf.Len() > 0 {
			buf.WriteString(", ")
		}
		switch i {
		case ExplainOptionFormat:
			buf.WriteString("Format: ")
			switch eopt.options[i].format {
			case ExplainFormatText:
				buf.WriteString("text")
			case ExplainFormatJSON:
				buf.WriteString("json")
			case ExplainFormatYAML:
				buf.WriteString("yaml")
			case ExplainFormatXML:
				buf.WriteString("xml")
			}

		case ExplainOptionSerialize:
			buf.WriteString("Serialize: ")
			switch eopt.options[i].serialize {
			case ExplainSerializeNone:
				buf.WriteString("none")
			case ExplainSerializeText:
				buf.WriteString("text")
			case ExplainSerializeBinary:
				buf.WriteString("binary")
			}
		default:

			buf.WriteString(ExplainOptionNames[i])
			buf.WriteString(": true")
		}
	}
	return buf.String()
}

func (eopt *ExplainOptions) SetDefaultValues() {
	eopt.SetBooleanOption(ExplainOptionAnalyze, false)
	eopt.SetBooleanOption(ExplainOptionVerbose, true)
	eopt.SetBooleanOption(ExplainOptionCosts, true)
	eopt.SetBooleanOption(ExplainOptionSettings, false)
	eopt.SetBooleanOption(ExplainOptionGenericPlan, false)
	eopt.SetBooleanOption(ExplainOptionBuffers, false)
	eopt.SetBooleanOption(ExplainOptionSerialize, true)
	eopt.SetSerializeOption(ExplainOptionSerialize, ExplainSerializeNone)
	eopt.SetBooleanOption(ExplainOptionWal, false)
	eopt.SetBooleanOption(ExplainOptionTiming, true)
	eopt.SetBooleanOption(ExplainOptionSummary, false)
	eopt.SetBooleanOption(ExplainOptionMemory, false)
	eopt.SetBooleanOption(ExplainOptionFormat, true)
	eopt.SetFormatOption(ExplainOptionFormat, ExplainFormatText)
}

func (eopt *ExplainOptions) SetBooleanOption(option int, enabled bool) {
	eopt.options[option].enabled = enabled
}

func (eopt *ExplainOptions) SetSerializeOption(option int, serialize int) {
	eopt.options[option].serialize = serialize
}

func (eopt *ExplainOptions) SetFormatOption(option int, format int) {
	eopt.options[option].format = format
}

type ExplainCtx struct {
	pos     *ExplainPosition
	buffer  *ExplainBuffer
	options *ExplainOptions
}

func ExplainLogicalPlan(root *LogicalOperator) (string, error) {
	explainCtx := &ExplainCtx{
		pos: &ExplainPosition{
			level:  0,
			indent: 2,
		},
		buffer:  NewExplainBuffer(),
		options: NewExplainOptions(),
	}

	err := doExplainLogicalPlan(root, explainCtx)
	if err != nil {
		return "", err
	}
	return explainCtx.buffer.String(), nil
}

func doExplainLogicalPlan(root *LogicalOperator, ctx *ExplainCtx) error {
	if root == nil {
		return nil
	}

	if err := root.explainOperator(ctx); err != nil {
		return err
	}

	ctx.pos.level++
	defer func() {
		ctx.pos.level--
	}()

	for _, child := range root.Children {
		if err := doExplainLogicalPlan(child, ctx); err != nil {
			return err
		}
	}

	return nil
}

func (lo *LogicalOperator) explainOperator(ctx *ExplainCtx) error {
	if ctx.options.options[ExplainOptionFormat].format == ExplainFormatText {
		//step 1: basic info
		ctx.buffer.AppendLine(
			lo.getBasicInfo(ctx.options),
			ctx.pos.level,
			true)
		//step 2: verbose info
		if ctx.options.options[ExplainOptionVerbose].enabled {
			//outputs,counts,colRefToPos,exprs
			lines := lo.explainOutputs(ctx)
			for _, line := range lines {
				ctx.buffer.AppendLine(line, ctx.pos.level, false)
			}
			//step 3: extra info
			switch lo.Typ {
			case LOT_Project:
				projs := explainExprs(lo.Projects, ctx)
				ctx.buffer.AppendLine("exprs:"+projs, ctx.pos.level, false)
			case LOT_Filter:
				filters := explainExprs(lo.Filters, ctx)
				ctx.buffer.AppendLine("exprs:"+filters, ctx.pos.level, false)
			case LOT_Scan:
				tableInfo := ""
				if len(lo.Alias) != 0 && lo.Alias != lo.Table {
					tableInfo = fmt.Sprintf("%v.%v %v", lo.Database, lo.Table, lo.Alias)
				} else {
					tableInfo = fmt.Sprintf("%v.%v", lo.Database, lo.Table)
				}
				ctx.buffer.AppendLine(fmt.Sprintf("table: %v", tableInfo), ctx.pos.level, false)
				printColumns := func(col2Idx map[string]int, typs []common.LType, cols []string) string {
					lines := make([]string, 0)
					if len(col2Idx) > 0 {
						for i, col := range cols {
							idx := col2Idx[col]
							lines = append(lines, fmt.Sprintf("%v#%d::%v", col, i, typs[idx]))
						}
					} else {
						for i, col := range cols {
							if i >= len(typs) {
								lines = append(lines, fmt.Sprintf("%v#%d", col, i))
							} else {
								lines = append(lines, fmt.Sprintf("%v#%d::%v", col, i, typs[i]))
							}
						}
					}

					return strings.Join(lines, ", ")
				}
				if len(lo.Columns) > 0 {
					ctx.buffer.AppendLine("columns:"+printColumns(lo.ColName2Idx, lo.Types, lo.Columns), ctx.pos.level, false)
				}
				filters := explainExprs(lo.Filters, ctx)
				ctx.buffer.AppendLine("filters:"+filters, ctx.pos.level, false)
			case LOT_JOIN:
				ctx.buffer.AppendLine("type:"+lo.JoinTyp.String(), ctx.pos.level, false)
				filters := explainExprs(lo.OnConds, ctx)
				ctx.buffer.AppendLine("on:"+filters, ctx.pos.level, false)
			case LOT_AggGroup:
				groupBys := explainExprs(lo.GroupBys, ctx)
				ctx.buffer.AppendLine(fmt.Sprintf("groupExprs[#%d]:", lo.Index)+groupBys, ctx.pos.level, false)
				aggExprs := explainExprs(lo.Aggs, ctx)
				ctx.buffer.AppendLine(fmt.Sprintf("aggExprs[#%d]:", lo.Index2)+aggExprs, ctx.pos.level, false)
				filters := explainExprs(lo.Filters, ctx)
				ctx.buffer.AppendLine("filters:"+filters, ctx.pos.level, false)
			case LOT_Order:
				orderBys := explainExprs(lo.OrderBys, ctx)
				ctx.buffer.AppendLine("exprs:"+orderBys, ctx.pos.level, false)
			case LOT_Limit:
				limit := explainExprs([]*Expr{lo.Limit}, ctx)
				ctx.buffer.AppendLine("limit:"+limit, ctx.pos.level, false)
			case LOT_CreateSchema:
				ctx.buffer.AppendLine(fmt.Sprintf("CreateSchema: %v %v", lo.Database, lo.IfNotExists), ctx.pos.level, false)
			case LOT_CreateTable:
				ctx.buffer.AppendLine(fmt.Sprintf("CreateTable: %v %v %v", lo.Database, lo.Table, lo.IfNotExists), ctx.pos.level, false)
			}
		}
	} else {
		return fmt.Errorf("unsupported format: %d", ctx.options.options[ExplainOptionFormat].format)
	}
	return nil
}

func (lo *LogicalOperator) getBasicInfo(options *ExplainOptions) string {
	if lo == nil {
		return ""
	}
	//step 1: name
	buf := bytes.NewBuffer(nil)
	name := lo.Typ.String()
	//step 2: schema info & costs
	if options.options[ExplainOptionFormat].format == ExplainFormatText {
		buf.WriteString(name)
		switch lo.Typ {
		case LOT_Scan:
			buf.WriteString(" on ")
			tableInfo := ""
			if len(lo.Alias) != 0 && lo.Alias != lo.Table {
				tableInfo = fmt.Sprintf("%v.%v %v", lo.Database, lo.Table, lo.Alias)
			} else {
				tableInfo = fmt.Sprintf("%v.%v", lo.Database, lo.Table)
			}
			buf.WriteString(tableInfo)
		}
		buf.WriteString(fmt.Sprintf(" (Id %d) ", lo.Index))
		//TODO: costs info
	}

	return buf.String()
}

func (lo *LogicalOperator) explainOutputs(ctx *ExplainCtx) []string {
	lines := make([]string, 0)
	if lo == nil {
		return nil
	}

	if len(lo.Outputs) != 0 {
		buf := bytes.NewBuffer(nil)
		buf.WriteString("outputs:")
		ret := explainExprs(lo.Outputs, ctx)
		buf.WriteString(ret)
		lines = append(lines, buf.String())
	}

	if len(lo.Counts) != 0 {
		buf := bytes.NewBuffer(nil)
		buf.WriteString("counts:")
		j := 0
		for bind, i := range lo.Counts {
			if j > 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(fmt.Sprintf("%v %v", bind, i))
			j++
		}
		lines = append(lines, buf.String())
	}

	if len(lo.ColRefToPos) != 0 {
		buf := bytes.NewBuffer(nil)
		buf.WriteString("colRefToPos:")
		binds := lo.ColRefToPos.sortByColumnBind()
		j := 0
		for i, bind := range binds {
			if j > 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(fmt.Sprintf("%v %v", bind, i))
			j++
		}
		lines = append(lines, buf.String())
	}
	return lines
}

func ExplainPhysicalPlan(root *PhysicalOperator) (string, error) {
	explainCtx := &ExplainCtx{
		pos: &ExplainPosition{
			level:  0,
			indent: 2,
		},
		buffer:  NewExplainBuffer(),
		options: NewExplainOptions(),
	}

	err := doExplainPhysicalPlan(root, explainCtx)
	if err != nil {
		return "", err
	}
	return explainCtx.buffer.String(), nil
}

func doExplainPhysicalPlan(root *PhysicalOperator, ctx *ExplainCtx) error {
	if root == nil {
		return nil
	}

	if err := root.explainOperator(ctx); err != nil {
		return err
	}

	ctx.pos.level++
	defer func() {
		ctx.pos.level--
	}()

	for _, child := range root.Children {
		if err := doExplainPhysicalPlan(child, ctx); err != nil {
			return err
		}
	}

	return nil
}

func (po *PhysicalOperator) explainOperator(ctx *ExplainCtx) error {
	if ctx.options.options[ExplainOptionFormat].format == ExplainFormatText {
		//step 1: basic info
		ctx.buffer.AppendLine(
			po.getBasicInfo(ctx.options),
			ctx.pos.level,
			true)
		//step 2: verbose info
		if ctx.options.options[ExplainOptionVerbose].enabled {
			//outputs,counts,colRefToPos,exprs
			outputs := po.explainOutputs(ctx)
			ctx.buffer.AppendLine(outputs, ctx.pos.level, false)

			//step 3: extra info
			switch po.Typ {
			case POT_Project:
				projs := explainExprs(po.Projects, ctx)
				ctx.buffer.AppendLine("exprs:"+projs, ctx.pos.level, false)
			case POT_Filter:
				filters := explainExprs(po.Filters, ctx)
				ctx.buffer.AppendLine("exprs:"+filters, ctx.pos.level, false)
			case POT_Scan:
				if po.ScanTyp == ScanTypeTable && len(po.Columns) > 0 {
					ctx.buffer.AppendLine("columns:"+explainColumns(po.Columns), ctx.pos.level, false)
				}

				filters := explainExprs(po.Filters, ctx)
				ctx.buffer.AppendLine("filters:"+filters, ctx.pos.level, false)
			case POT_Join:
				ctx.buffer.AppendLine("type:"+po.JoinTyp.String(), ctx.pos.level, false)
				filters := explainExprs(po.OnConds, ctx)
				ctx.buffer.AppendLine("on:"+filters, ctx.pos.level, false)
			case POT_Agg:
				groupBys := explainExprs(po.GroupBys, ctx)
				ctx.buffer.AppendLine(fmt.Sprintf("groupExprs[#%d]:", po.Index)+groupBys, ctx.pos.level, false)
				aggExprs := explainExprs(po.Aggs, ctx)
				ctx.buffer.AppendLine(fmt.Sprintf("aggExprs[#%d]:", po.Index2)+aggExprs, ctx.pos.level, false)
				filters := explainExprs(po.Filters, ctx)
				ctx.buffer.AppendLine("filters:"+filters, ctx.pos.level, false)
			case POT_Order:
				orderBys := explainExprs(po.OrderBys, ctx)
				ctx.buffer.AppendLine("exprs:"+orderBys, ctx.pos.level, false)
			case POT_Limit:
				limit := explainExprs([]*Expr{po.Limit}, ctx)
				ctx.buffer.AppendLine("limit:"+limit, ctx.pos.level, false)
			case POT_Stub:
				ctx.buffer.AppendLine(fmt.Sprintf("Stub: %v %v", po.Table, po.ChunkCount), ctx.pos.level, false)
			case POT_CreateSchema:
				ctx.buffer.AppendLine(fmt.Sprintf("CreateSchema: %v %v", po.Database, po.IfNotExists), ctx.pos.level, false)
			case POT_CreateTable:
				ctx.buffer.AppendLine(fmt.Sprintf("CreateTable: %v %v %v", po.Database, po.Table, po.IfNotExists), ctx.pos.level, false)
				// colDefs := explainColDefs(po.ColDefs, ctx)
			case POT_Insert:
				ctx.buffer.AppendLine(fmt.Sprintf("Insert: %v %v", po.Database, po.Table), ctx.pos.level, false)
			}
		}

	} else {
		return fmt.Errorf("unsupported format: %d", ctx.options.options[ExplainOptionFormat].format)
	}
	return nil
}

func explainColumns(cols []string) string {
	t := strings.Builder{}
	for i, col := range cols {
		if i > 0 {
			t.WriteString(", ")
		}
		t.WriteString(fmt.Sprintf("%v#%d", col, i))
	}
	return t.String()
}

func (po *PhysicalOperator) getBasicInfo(options *ExplainOptions) string {
	if po == nil {
		return ""
	}
	//step 1: name
	buf := bytes.NewBuffer(nil)
	name := po.Typ.String()
	//step 2: schema info & costs
	if options.options[ExplainOptionFormat].format == ExplainFormatText {
		buf.WriteString(name)
		switch po.Typ {
		case POT_Scan:
			buf.WriteString(" on ")
			tableInfo := ""
			if len(po.Alias) != 0 && po.Alias != po.Table {
				tableInfo = fmt.Sprintf("%v.%v %v", po.Database, po.Table, po.Alias)
			} else {
				tableInfo = fmt.Sprintf("%v.%v", po.Database, po.Table)
			}
			buf.WriteString(tableInfo)
		}
		buf.WriteString(fmt.Sprintf(" (Id %d ", po.Id))
		buf.WriteString(fmt.Sprintf("estCard %d)", po.estimatedCard))
		//TODO: costs info
	}

	return buf.String()
}

func (po *PhysicalOperator) explainOutputs(ctx *ExplainCtx) string {
	if po == nil {
		return ""
	}
	buf := bytes.NewBuffer(nil)
	if len(po.Outputs) != 0 {
		buf.WriteString("outputs:")
		ret := explainExprs(po.Outputs, ctx)
		buf.WriteString(ret)
	}

	return buf.String()
}

func explainExprs(exprs []*Expr, ctx *ExplainCtx) string {
	buf := bytes.NewBuffer(nil)
	for i, expr := range exprs {
		if i > 0 {
			buf.WriteString(", ")
		}
		explainExpr(expr, ctx.options, buf)
		alias := ""
		if len(expr.Alias) != 0 {
			alias = expr.Alias
			buf.WriteString(fmt.Sprintf(" as %v", alias))
		}
		if expr.Typ == ET_Orderby {
			if expr.Desc {
				buf.WriteString(" desc")
			}

		}
	}
	return buf.String()
}

func explainExpr(e *Expr, options *ExplainOptions, buf *bytes.Buffer) {
	if e == nil {
		return
	}
	switch e.Typ {
	case ET_Column:
		if e.Depth != 0 {
			buf.WriteString(fmt.Sprintf("%s.%s%v,%d",
				e.Table, e.Name,
				e.ColRef, e.Depth))
		} else {
			buf.WriteString(fmt.Sprintf("%s.%s%v",
				e.Table, e.Name,
				e.ColRef))
		}
	case ET_Const:
		var value string
		switch e.ConstValue.Type {
		case ConstTypeString:
			value = fmt.Sprintf("'%s'", e.ConstValue.String)
		case ConstTypeInteger:
			value = fmt.Sprintf("%d", e.ConstValue.Integer)
		case ConstTypeDate:
			value = fmt.Sprintf("'%s'", e.ConstValue.Date)
		case ConstTypeInterval:
			value = fmt.Sprintf("INTERVAL %d %s", e.ConstValue.Interval.Value, e.ConstValue.Interval.Unit)
		case ConstTypeBoolean:
			value = fmt.Sprintf("%v", e.ConstValue.Boolean)
		case ConstTypeFloat:
			value = fmt.Sprintf("%f", e.ConstValue.Float)
		case ConstTypeDecimal:
			value = fmt.Sprintf("DECIMAL(%s %d %d)",
				e.ConstValue.Decimal,
				e.DataTyp.Width,
				e.DataTyp.Scale)
		case ConstTypeNull:
			value = "NULL"
		}
		buf.WriteString(value)
	case ET_TABLE:
		buf.WriteString(fmt.Sprintf("%s.%s", e.Database, e.Table))
	case ET_Join:
		typStr := ""
		switch e.JoinTyp {
		case ET_JoinTypeCross:
			typStr = "cross"
		case ET_JoinTypeLeft:
			typStr = "left"
		default:
			panic(fmt.Sprintf("usp join type %d", e.JoinTyp))
		}
		buf.WriteString(typStr)
		buf.WriteString("(")
		explainExpr(e.Children[0], options, buf)
		buf.WriteString(",")
		explainExpr(e.Children[1], options, buf)
		buf.WriteString(")")
	case ET_Func:
		if e.FunImpl.IsFunction() {
			dist := ""
			if e.FunctionInfo.FunImpl._aggrType == DISTINCT {
				dist = " distinct "
			}
			buf.WriteString(e.FunImpl._name)
			buf.WriteString("(")
			if dist != "" {
				buf.WriteString(dist)
				buf.WriteString(" ")
			}

			if e.FunImpl._name == FuncCast {
				explainExpr(e.Children[0], options, buf)
				buf.WriteString(" as ")
				buf.WriteString(e.DataTyp.String())
			} else {
				for j, child := range e.Children {
					if j > 0 {
						buf.WriteString(", ")
					}
					explainExpr(child, options, buf)
				}
			}

			buf.WriteString(")")
		} else {
			switch e.FunImpl._name {
			case FuncBetween:
				explainExpr(e.Children[0], options, buf)
				buf.WriteString(" ")
				buf.WriteString(e.FunImpl._name)
				buf.WriteString(" ")
				explainExpr(e.Children[1], options, buf)
				buf.WriteString(" and ")
				explainExpr(e.Children[2], options, buf)
			case FuncCase:
				buf.WriteString(e.FunImpl._name)
				if e.Children[0] != nil {
					explainExpr(e.Children[0], options, buf)
				}
				buf.WriteString(" when ")
				for i := 1; i < len(e.Children); i += 2 {
					explainExpr(e.Children[i], options, buf)
					buf.WriteString(" then ")
					explainExpr(e.Children[i+1], options, buf)
				}
				if e.Children[0] != nil {
					buf.WriteString(" else ")
					explainExpr(e.Children[0], options, buf)
				}
				buf.WriteString(" end")
			case FuncIn, FuncNotIn:
				explainExpr(e.Children[0], options, buf)
				buf.WriteString(" ")
				buf.WriteString(e.FunImpl._name)
				buf.WriteString(" (")
				for j, child := range e.Children {
					if j == 0 {
						continue
					}
					if j > 0 {
						buf.WriteString(",")
					}
					explainExpr(child, options, buf)
				}
				buf.WriteString(")")
			case FuncExists:
				buf.WriteString(e.FunImpl._name)
				buf.WriteString("(")
				explainExpr(e.Children[0], options, buf)
				buf.WriteString(")")
			default:
				//binary operator
				buf.WriteString("(")
				explainExpr(e.Children[0], options, buf)
				buf.WriteString(" ")
				buf.WriteString(e.FunImpl._name)
				buf.WriteString(" ")
				explainExpr(e.Children[1], options, buf)
				buf.WriteString(")")
			}
		}

	case ET_Subquery:
		buf.WriteString("subquery(")
		// e.SubBuilder.Print(buf)
		buf.WriteString(")")
	case ET_Orderby:
		explainExpr(e.Children[0], options, buf)

	default:
		panic(fmt.Sprintf("usp expr type %d", e.Typ))
	}
}
