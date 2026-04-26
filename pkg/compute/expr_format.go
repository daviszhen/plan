package compute

import (
	"bytes"
	"fmt"
)

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
		case ConstTypeFloat:
			value = fmt.Sprintf("%f", e.ConstValue.Float)
		case ConstTypeBoolean:
			value = fmt.Sprintf("%t", e.ConstValue.Boolean)
		case ConstTypeNull:
			value = "NULL"
		case ConstTypeDate:
			value = fmt.Sprintf("cast('%s' as DATE)", e.ConstValue.String)
		case ConstTypeInterval:
			value = fmt.Sprintf("cast('%d %s' as INTERVAL)", e.ConstValue.Integer, e.ConstValue.String)
		default:
			panic(fmt.Sprintf("usp const type %d", e.ConstValue.Type))
		}
		buf.WriteString(value)
	case ET_Join:
		typStr := ""
		switch e.GetJoinInfo().JoinTyp {
		case ET_JoinTypeCross:
			typStr = "cross"
		case ET_JoinTypeLeft:
			typStr = "left"
		default:
			panic(fmt.Sprintf("usp join type %d", e.GetJoinInfo().JoinTyp))
		}
		buf.WriteString(typStr)
		buf.WriteString("(")
		explainExpr(e.Children[0], options, buf)
		buf.WriteString(",")
		explainExpr(e.Children[1], options, buf)
		buf.WriteString(")")
	case ET_Func:
		if e.GetFuncInfo().FunImpl.IsFunction() {
			dist := ""
			if e.GetFuncInfo().FunImpl._aggrType == DISTINCT {
				dist = " distinct "
			}
			buf.WriteString(e.GetFuncInfo().FunImpl._name)
			buf.WriteString("(")
			if dist != "" {
				buf.WriteString(dist)
				buf.WriteString(" ")
			}

			if e.GetFuncInfo().FunImpl._name == FuncCast {
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
			switch e.GetFuncInfo().FunImpl._name {
			case FuncBetween:
				explainExpr(e.Children[0], options, buf)
				buf.WriteString(" ")
				buf.WriteString(e.GetFuncInfo().FunImpl._name)
				buf.WriteString(" ")
				explainExpr(e.Children[1], options, buf)
				buf.WriteString(" and ")
				explainExpr(e.Children[2], options, buf)
			case FuncCase:
				buf.WriteString(e.GetFuncInfo().FunImpl._name)
				if e.Children[0] != nil {
					explainExpr(e.Children[0], options, buf)
				}
				for j := 1; j < len(e.Children); j += 2 {
					buf.WriteString(" when ")
					explainExpr(e.Children[j], options, buf)
					buf.WriteString(" then ")
					explainExpr(e.Children[j+1], options, buf)
				}
				if len(e.Children)%2 == 0 {
					buf.WriteString(" else ")
					explainExpr(e.Children[len(e.Children)-1], options, buf)
				}
				buf.WriteString(" end")
			case FuncIn, FuncNotIn:
				explainExpr(e.Children[0], options, buf)
				buf.WriteString(" ")
				buf.WriteString(e.GetFuncInfo().FunImpl._name)
				buf.WriteString(" (")
				for j, child := range e.Children {
					if j == 0 {
						continue
					}
					if j > 1 {
						buf.WriteString(",")
					}
					explainExpr(child, options, buf)
				}
				buf.WriteString(")")
			case FuncExists:
				buf.WriteString(e.GetFuncInfo().FunImpl._name)
				buf.WriteString("(")
				explainExpr(e.Children[0], options, buf)
				buf.WriteString(")")
			default:
				//binary operator
				buf.WriteString("(")
				explainExpr(e.Children[0], options, buf)
				buf.WriteString(" ")
				buf.WriteString(e.GetFuncInfo().FunImpl._name)
				buf.WriteString(" ")
				explainExpr(e.Children[1], options, buf)
				buf.WriteString(")")
			}
		}
	case ET_Orderby:
		explainExpr(e.Children[0], options, buf)
	case ET_Subquery:
		buf.WriteString("subquery")
	default:
		panic(fmt.Sprintf("usp expr type %d", e.Typ))
	}
}
