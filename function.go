package main

import "fmt"

type FuncId int

type Function struct {
	Id FuncId

	// all implementation versions
	Impls []Impl

	// decide which implementation should be used
	ImplDecider func(*Function, []ExprDataType) (int, []ExprDataType)
}

type FunctionBody func() error

type Impl struct {
	Desc           string
	Idx            int
	Args           []ExprDataType
	RetTypeDecider func([]ExprDataType) ExprDataType
	Body           func() FunctionBody
	IsAgg          bool
	IsWindow       bool
}

const (
	MIN FuncId = iota
)

var funcName2Id = map[string]FuncId{
	"min": MIN,
}

var allFunctions = map[FuncId]Function{}

var aggFuncs = map[string]int{
	"min": 1,
}

func IsAgg(name string) bool {
	if _, ok := aggFuncs[name]; ok {
		return ok
	}
	return false
}

func GetFunctionId(name string) (FuncId, error) {
	if id, ok := funcName2Id[name]; ok {
		return id, nil
	}
	return 0, fmt.Errorf("no function %s", name)
}

func GetFunctionImpl(id FuncId, argsTypes []ExprDataType) (FunctionBody, error) {
	if _, ok := allFunctions[id]; ok {
		panic("usp")
	}
	return nil, fmt.Errorf("no body of function %d", id)
}

var operators = []Function{}

var aggs = []Function{
	{
		Id: MIN,
		Impls: []Impl{
			{
				Desc: "min",
				Idx:  0,
				Args: []ExprDataType{
					{
						Typ: DataTypeDecimal,
					},
				},
				RetTypeDecider: func(types []ExprDataType) ExprDataType {
					if types[0].Typ == DataTypeDecimal {
						return types[0]
					}
					panic("usp")
				},
				Body: func() FunctionBody {
					return func() error {
						return fmt.Errorf("usp")
					}
				},
				IsAgg: true,
			},
		},
		ImplDecider: func(*Function, []ExprDataType) (int, []ExprDataType) {
			panic("usp")
		},
	},
}

func init() {
	for _, oper := range operators {
		allFunctions[oper.Id] = oper
	}

	for _, agg := range aggs {
		allFunctions[agg.Id] = agg
	}
}
