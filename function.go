package main

import (
	"fmt"
)

type FuncId int

type Function struct {
	Id FuncId

	// all implementation versions
	Impls []*Impl

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
	INVALID_FUNC FuncId = iota
	MIN
	DATE_ADD
	COUNT
	EXTRACT
	SUM
	MAX
	AVG
	SUBSTRING
	CAST
	IN
	BETWEEN
)

var funcName2Id = map[string]FuncId{
	"min":       MIN,
	"date_add":  DATE_ADD,
	"count":     COUNT,
	"extract":   EXTRACT,
	"sum":       SUM,
	"max":       MAX,
	"avg":       AVG,
	"substring": SUBSTRING,
	"cast":      CAST,
	"in":        IN,
	"between":   BETWEEN,
}

var allFunctions = map[FuncId]*Function{}

var aggNames = map[string]int{
	"min":   1,
	"count": 1,
	"sum":   1,
	"max":   1,
	"avg":   1,
}

func IsAgg(name string) bool {
	if _, ok := aggNames[name]; ok {
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

func GetFunctionImpl(id FuncId, argsTypes []ExprDataType) (*Impl, error) {
	if fun, ok := allFunctions[id]; !ok {
		panic(fmt.Sprintf("no function %v", id))
	} else {
		if fun.ImplDecider == nil {
			panic("usp")
		}
		implIdx, _ := fun.ImplDecider(fun, argsTypes)
		if implIdx < 0 {
			//no right impl
			panic("no right impl")
		} else {
			//right impl
			impl := fun.Impls[implIdx]
			return impl, nil
		}
	}
	panic("usp")
}

func exactImplDecider(fun *Function, argsTypes []ExprDataType) (int, []ExprDataType) {
	for i, impl := range fun.Impls {
		if len(argsTypes) != len(impl.Args) {
			continue
		}
		equalTyp := true
		for j, arg := range impl.Args {
			if !arg.equal(argsTypes[j]) &&
				!arg.include(argsTypes[j]) {
				equalTyp = false
				break
			}
		}
		if equalTyp {
			return i, impl.Args
		}
	}
	return -1, nil
}

func opInImplDecider(fun *Function, argsTypes []ExprDataType) (int, []ExprDataType) {
	if len(argsTypes) < 1 {
		return -1, nil
	}
	for i, impl := range fun.Impls {
		equalTyp := true
		for _, arg := range argsTypes {
			if !impl.Args[0].equal(arg) &&
				!impl.Args[0].include(arg) {
				equalTyp = false
				break
			}
		}
		if equalTyp {
			return i, impl.Args
		}
	}
	return -1, nil
}

func ignoreTypesImplDecider(fun *Function, argsTypes []ExprDataType) (int, []ExprDataType) {
	return 0, nil
}

func decideNull(types []ExprDataType) bool {
	for _, typ := range types {
		if !typ.NotNull {
			return true
		}
	}
	return false
}

func registerFunctions(funs []*Function) {
	for _, fun := range funs {
		if _, ok := allFunctions[fun.Id]; ok {
			panic(fmt.Sprintf("function %v already exists", fun.Id))
		}
		allFunctions[fun.Id] = fun
	}
}

func init() {
	registerFunctions(operators)

	registerFunctions(aggFuncs)

	registerFunctions(funcs)
}

var operators = []*Function{}

var aggFuncs = []*Function{
	{
		Id: MIN,
		Impls: []*Impl{
			{
				Desc: "min",
				Idx:  0,
				Args: []ExprDataType{
					{
						LTyp: decimal(DecimalMaxWidthInt64, 0),
					},
				},
				RetTypeDecider: func(types []ExprDataType) ExprDataType {
					if types[0].LTyp.id == LTID_DECIMAL {
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
	{
		Id: SUM,
		Impls: []*Impl{
			{
				Desc: "sum",
				Idx:  0,
				Args: []ExprDataType{
					{
						LTyp: decimal(DecimalMaxWidthInt64, 0),
					},
				},
				RetTypeDecider: func(types []ExprDataType) ExprDataType {
					if types[0].LTyp.id == LTID_DECIMAL {
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
		ImplDecider: exactImplDecider,
	},
	{
		Id: COUNT,
		Impls: []*Impl{
			{
				Desc: "count",
				Idx:  0,
				Args: []ExprDataType{
					{
						LTyp: decimal(DecimalMaxWidthInt64, 0),
					},
				},
				RetTypeDecider: func(types []ExprDataType) ExprDataType {
					return ExprDataType{
						LTyp:    integer(),
						NotNull: true,
					}
				},
				Body: func() FunctionBody {
					return func() error {
						return fmt.Errorf("usp")
					}
				},
				IsAgg: true,
			},
		},
		ImplDecider: ignoreTypesImplDecider,
	},
	{
		Id: AVG,
		Impls: []*Impl{
			{
				Desc: "avg",
				Idx:  0,
				Args: []ExprDataType{
					{
						LTyp: decimal(DecimalMaxWidthInt64, 0),
					},
				},
				RetTypeDecider: func(types []ExprDataType) ExprDataType {
					return ExprDataType{
						LTyp:    types[0].LTyp,
						NotNull: decideNull(types),
					}
				},
				Body: func() FunctionBody {
					return func() error {
						return fmt.Errorf("usp")
					}
				},
				IsAgg: true,
			},
		},
		ImplDecider: exactImplDecider,
	},
}

var funcs = []*Function{
	{
		Id: DATE_ADD,
		Impls: []*Impl{
			{
				Desc: "date_add",
				Idx:  0,
				Args: []ExprDataType{
					{LTyp: dateLTyp()},
					{LTyp: intervalLType()},
				},
				RetTypeDecider: func(types []ExprDataType) ExprDataType {
					panic("usp")
				},
				Body: func() FunctionBody {
					return func() error {
						return fmt.Errorf("usp")
					}
				},
			},
		},
	},
	{
		Id: EXTRACT,
		Impls: []*Impl{
			{
				Desc: "extract",
				Idx:  0,
				Args: []ExprDataType{
					{LTyp: intervalLType()},
					{LTyp: dateLTyp()},
				},
				RetTypeDecider: func(types []ExprDataType) ExprDataType {
					panic("usp")
				},
				Body: func() FunctionBody {
					return func() error {
						return fmt.Errorf("usp")
					}
				},
			},
		},
	},
	{
		Id: CAST,
		Impls: []*Impl{
			{
				Desc: "cast",
				Idx:  0,
				Args: []ExprDataType{
					{
						LTyp: decimal(DecimalMaxWidthInt64, 0),
					},
					{
						LTyp: integer(),
					},
				},
				RetTypeDecider: func(types []ExprDataType) ExprDataType {
					panic("usp")
				},
				Body: func() FunctionBody {
					return func() error {
						return fmt.Errorf("usp")
					}
				},
				IsAgg: false,
			},
		},
		ImplDecider: func(*Function, []ExprDataType) (int, []ExprDataType) {
			panic("usp")
		},
	},
	{
		Id: SUBSTRING,
		Impls: []*Impl{
			{
				Desc: "substring",
				Idx:  0,
				Args: []ExprDataType{
					{
						LTyp: varchar(),
					},
					{
						LTyp: integer(),
					},
					{
						LTyp: integer(),
					},
				},
				RetTypeDecider: func(types []ExprDataType) ExprDataType {
					return ExprDataType{
						LTyp:    varchar(),
						NotNull: decideNull(types),
					}
				},
				Body: func() FunctionBody {
					return func() error {
						return fmt.Errorf("usp")
					}
				},
				IsAgg: false,
			},
		},
		ImplDecider: exactImplDecider,
	},
	{
		Id: IN,
		Impls: []*Impl{
			{
				Desc: "in",
				Idx:  0,
				Args: []ExprDataType{
					{
						LTyp: varchar(),
					},
				},
				RetTypeDecider: func(types []ExprDataType) ExprDataType {
					return ExprDataType{LTyp: boolean(), NotNull: decideNull(types)}
				},
				Body: func() FunctionBody {
					return func() error {
						return fmt.Errorf("usp")
					}
				},
				IsAgg: false,
			},
		},
		ImplDecider: opInImplDecider,
	},
	{
		Id: BETWEEN,
		Impls: []*Impl{
			{
				Desc: "between",
				Idx:  0,
				Args: []ExprDataType{
					{
						LTyp: float(),
					},
					{
						LTyp: float(),
					},
					{
						LTyp: float(),
					},
				},
				RetTypeDecider: func(types []ExprDataType) ExprDataType {
					return ExprDataType{LTyp: boolean(), NotNull: decideNull(types)}
				},
				Body: func() FunctionBody {
					return func() error {
						return fmt.Errorf("usp")
					}
				},
				IsAgg: false,
			},
		},
		ImplDecider: exactImplDecider,
	},
}
