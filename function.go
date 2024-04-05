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

type FunctionBody func(chunk *Chunk, state *ExprState, count int, result *Vector) error

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
	//agg
	MIN
	COUNT
	SUM
	MAX
	AVG
	//operator
	EQUAL
	NOT_EQUAL
	IN
	NOT_IN
	BETWEEN
	AND
	OR
	ADD
	SUB
	MUL
	DIV
	GREAT_EQUAL
	GREAT
	LESS_EQUAL
	LESS
	LIKE
	NOT_LIKE
	CASE
	EXISTS
	NOT_EXISTS
	//functions
	DATE_ADD
	EXTRACT
	SUBSTRING
	CAST
)

var funcName2Id = map[string]FuncId{
	"min":        MIN,
	"date_add":   DATE_ADD,
	"count":      COUNT,
	"extract":    EXTRACT,
	"sum":        SUM,
	"max":        MAX,
	"avg":        AVG,
	"substring":  SUBSTRING,
	"cast":       CAST,
	"=":          EQUAL,
	"<>":         NOT_EQUAL,
	"!=":         NOT_EQUAL,
	"in":         IN,
	"not in":     NOT_IN,
	"between":    BETWEEN,
	"and":        AND,
	"or":         OR,
	"+":          ADD,
	"-":          SUB,
	"*":          MUL,
	"/":          DIV,
	">=":         GREAT_EQUAL,
	">":          GREAT,
	"<=":         LESS_EQUAL,
	"<":          LESS,
	"like":       LIKE,
	"not like":   NOT_LIKE,
	"case":       CASE,
	"exists":     EXISTS,
	"not exists": NOT_EXISTS,
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

func registerFunctions(funs []*Function, needAgg bool) {
	for _, fun := range funs {
		if _, ok := allFunctions[fun.Id]; ok {
			panic(fmt.Sprintf("function %v already exists", fun.Id))
		}
		if len(fun.Impls) == 0 {
			panic(fmt.Sprintf("function %v need impl", fun.Id))
		}
		for i := 1; i < len(fun.Impls); i++ {
			if fun.Impls[i].Idx != i {
				panic(fmt.Sprintf("function %v impl %d has wrong index", fun.Id, i))
			}
		}
		if needAgg {
			hasAgg := false
			for _, impl := range fun.Impls {
				if impl.IsAgg {
					hasAgg = true
					break
				}
			}
			if !hasAgg {
				panic(fmt.Sprintf("function %v need agg impl", fun.Id))
			}
		}
		allFunctions[fun.Id] = fun
	}
}

func init() {
	registerFunctions(operators, false)

	registerFunctions(aggFuncs, true)

	registerFunctions(funcs, false)
}

var operators = []*Function{
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
					return func(chunk *Chunk, state *ExprState, count int, result *Vector) error {
						return fmt.Errorf("usp")
					}
				},
			},
			{
				Desc: "in",
				Idx:  1,
				Args: []ExprDataType{
					{
						LTyp: integer(),
					},
				},
				RetTypeDecider: func(types []ExprDataType) ExprDataType {
					return ExprDataType{LTyp: boolean(), NotNull: decideNull(types)}
				},
				Body: func() FunctionBody {
					return func(chunk *Chunk, state *ExprState, count int, result *Vector) error {
						binaryExecSwitch[int32, bool](chunk._data[0], chunk._data[1], result, count, gBinInt32Equal, nil, gBinInt32BoolSingleOpWrapper)
						return nil
					}
				},
			},
		},
		ImplDecider: opInImplDecider,
	},
	{
		Id: NOT_IN,
		Impls: []*Impl{
			{
				Desc: "not in",
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
					return func(chunk *Chunk, state *ExprState, count int, result *Vector) error {
						return fmt.Errorf("usp")
					}
				},
			},
			{
				Desc: "not in",
				Idx:  1,
				Args: []ExprDataType{
					{
						LTyp: integer(),
					},
				},
				RetTypeDecider: func(types []ExprDataType) ExprDataType {
					return ExprDataType{LTyp: boolean(), NotNull: decideNull(types)}
				},
				Body: func() FunctionBody {
					return func(chunk *Chunk, state *ExprState, count int, result *Vector) error {
						return fmt.Errorf("usp")
					}
				},
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
					return func(chunk *Chunk, state *ExprState, count int, result *Vector) error {
						return fmt.Errorf("usp")
					}
				},
			},
			{
				Desc: "between",
				Idx:  1,
				Args: []ExprDataType{
					{
						LTyp: integer(),
					},
					{
						LTyp: integer(),
					},
					{
						LTyp: integer(),
					},
				},
				RetTypeDecider: func(types []ExprDataType) ExprDataType {
					return ExprDataType{LTyp: boolean(), NotNull: decideNull(types)}
				},
				Body: func() FunctionBody {
					return func(chunk *Chunk, state *ExprState, count int, result *Vector) error {
						return fmt.Errorf("usp")
					}
				},
			},
			{
				Desc: "between",
				Idx:  2,
				Args: []ExprDataType{
					{
						LTyp: dateLTyp(),
					},
					{
						LTyp: dateLTyp(),
					},
					{
						LTyp: dateLTyp(),
					},
				},
				RetTypeDecider: func(types []ExprDataType) ExprDataType {
					return ExprDataType{LTyp: boolean(), NotNull: decideNull(types)}
				},
				Body: func() FunctionBody {
					return func(chunk *Chunk, state *ExprState, count int, result *Vector) error {
						return fmt.Errorf("usp")
					}
				},
			},
		},
		ImplDecider: exactImplDecider,
	},
	{
		Id: ADD,
		Impls: []*Impl{
			{
				Desc: "+",
				Idx:  0,
				Args: []ExprDataType{
					{
						LTyp: dateLTyp(),
					},
					{
						LTyp: dateLTyp(),
					},
				},
				RetTypeDecider: func(types []ExprDataType) ExprDataType {
					return ExprDataType{LTyp: types[0].LTyp, NotNull: decideNull(types)}
				},
				Body: func() FunctionBody {
					return func(chunk *Chunk, state *ExprState, count int, result *Vector) error {
						return fmt.Errorf("usp")
					}
				},
			},
			{
				Desc: "+",
				Idx:  1,
				Args: []ExprDataType{
					{
						LTyp: float(),
					},
					{
						LTyp: float(),
					},
				},
				RetTypeDecider: func(types []ExprDataType) ExprDataType {
					return ExprDataType{LTyp: types[0].LTyp, NotNull: decideNull(types)}
				},
				Body: func() FunctionBody {
					return func(chunk *Chunk, state *ExprState, count int, result *Vector) error {
						return fmt.Errorf("usp")
					}
				},
			},
			{
				Desc: "+",
				Idx:  2,
				Args: []ExprDataType{
					{
						LTyp: decimal(DecimalMaxWidthInt64, 0),
					},
					{
						LTyp: decimal(DecimalMaxWidthInt64, 0),
					},
				},
				RetTypeDecider: func(types []ExprDataType) ExprDataType {
					return ExprDataType{LTyp: types[0].LTyp, NotNull: decideNull(types)}
				},
				Body: func() FunctionBody {
					return func(chunk *Chunk, state *ExprState, count int, result *Vector) error {
						return fmt.Errorf("usp")
					}
				},
			},
		},
		ImplDecider: exactImplDecider,
	},
	{
		Id: SUB,
		Impls: []*Impl{
			{
				Desc: "-",
				Idx:  0,
				Args: []ExprDataType{
					{
						LTyp: float(),
					},
					{
						LTyp: float(),
					},
				},
				RetTypeDecider: func(types []ExprDataType) ExprDataType {
					return ExprDataType{LTyp: types[0].LTyp, NotNull: decideNull(types)}
				},
				Body: func() FunctionBody {
					return func(chunk *Chunk, state *ExprState, count int, result *Vector) error {
						return fmt.Errorf("usp")
					}
				},
			},
			{
				Desc: "-",
				Idx:  1,
				Args: []ExprDataType{
					{
						LTyp: decimal(DecimalMaxWidthInt64, 0),
					},
					{
						LTyp: decimal(DecimalMaxWidthInt64, 0),
					},
				},
				RetTypeDecider: func(types []ExprDataType) ExprDataType {
					return ExprDataType{LTyp: types[0].LTyp, NotNull: decideNull(types)}
				},
				Body: func() FunctionBody {
					return func(chunk *Chunk, state *ExprState, count int, result *Vector) error {
						return fmt.Errorf("usp")
					}
				},
			},
			{
				Desc: "-",
				Idx:  2,
				Args: []ExprDataType{
					{
						LTyp: dateLTyp(),
					},
					{
						LTyp: dateLTyp(),
					},
				},
				RetTypeDecider: func(types []ExprDataType) ExprDataType {
					return ExprDataType{LTyp: types[0].LTyp, NotNull: decideNull(types)}
				},
				Body: func() FunctionBody {
					return func(chunk *Chunk, state *ExprState, count int, result *Vector) error {
						return fmt.Errorf("usp")
					}
				},
			},
		},
		ImplDecider: exactImplDecider,
	},
	{
		Id: MUL,
		Impls: []*Impl{
			{
				Desc: "*",
				Idx:  0,
				Args: []ExprDataType{
					{
						LTyp: decimal(DecimalMaxWidthInt64, 0),
					},
					{
						LTyp: decimal(DecimalMaxWidthInt64, 0),
					},
				},
				RetTypeDecider: func(types []ExprDataType) ExprDataType {
					return ExprDataType{LTyp: types[0].LTyp, NotNull: decideNull(types)}
				},
				Body: func() FunctionBody {
					return func(chunk *Chunk, state *ExprState, count int, result *Vector) error {
						return fmt.Errorf("usp")
					}
				},
			},
			{
				Desc: "*",
				Idx:  1,
				Args: []ExprDataType{
					{
						LTyp: float(),
					},
					{
						LTyp: float(),
					},
				},
				RetTypeDecider: func(types []ExprDataType) ExprDataType {
					return ExprDataType{LTyp: types[0].LTyp, NotNull: decideNull(types)}
				},
				Body: func() FunctionBody {
					return func(chunk *Chunk, state *ExprState, count int, result *Vector) error {
						binaryExecSwitch[float32, float32](chunk._data[0], chunk._data[1], result, count, gBinFloat32Multi, nil, gBinFloat32Float32SingleOpWrapper)
						return nil
					}
				},
			},
		},
		ImplDecider: exactImplDecider,
	},
	{
		Id: DIV,
		Impls: []*Impl{
			{
				Desc: "/",
				Idx:  0,
				Args: []ExprDataType{
					{
						LTyp: decimal(DecimalMaxWidthInt64, 0),
					},
					{
						LTyp: decimal(DecimalMaxWidthInt64, 0),
					},
				},
				RetTypeDecider: func(types []ExprDataType) ExprDataType {
					return ExprDataType{LTyp: types[0].LTyp, NotNull: decideNull(types)}
				},
				Body: func() FunctionBody {
					return func(chunk *Chunk, state *ExprState, count int, result *Vector) error {
						return fmt.Errorf("usp")
					}
				},
			},
			{
				Desc: "/",
				Idx:  1,
				Args: []ExprDataType{
					{
						LTyp: float(),
					},
					{
						LTyp: float(),
					},
				},
				RetTypeDecider: func(types []ExprDataType) ExprDataType {
					return ExprDataType{LTyp: types[0].LTyp, NotNull: decideNull(types)}
				},
				Body: func() FunctionBody {
					return func(chunk *Chunk, state *ExprState, count int, result *Vector) error {
						return fmt.Errorf("usp")
					}
				},
			},
		},
		ImplDecider: exactImplDecider,
	},
	{
		Id: EQUAL,
		Impls: []*Impl{
			{
				Desc: "=",
				Idx:  0,
				Args: []ExprDataType{
					{
						LTyp: integer(),
					},
					{
						LTyp: integer(),
					},
				},
				RetTypeDecider: func(types []ExprDataType) ExprDataType {
					return ExprDataType{LTyp: boolean(), NotNull: decideNull(types)}
				},
				Body: func() FunctionBody {
					return func(chunk *Chunk, state *ExprState, count int, result *Vector) error {
						binaryExecSwitch[int32, bool](chunk._data[0], chunk._data[1], result, count, gBinInt32Equal, nil, gBinInt32BoolSingleOpWrapper)
						return nil
					}
				},
			},
			{
				Desc: "=",
				Idx:  1,
				Args: []ExprDataType{
					{
						LTyp: varchar(),
					},
					{
						LTyp: varchar(),
					},
				},
				RetTypeDecider: func(types []ExprDataType) ExprDataType {
					return ExprDataType{LTyp: boolean(), NotNull: decideNull(types)}
				},
				Body: func() FunctionBody {
					return func(chunk *Chunk, state *ExprState, count int, result *Vector) error {
						return fmt.Errorf("usp")
					}
				},
			},
			{
				Desc: "=",
				Idx:  2,
				Args: []ExprDataType{
					{
						LTyp: decimal(DecimalMaxWidthInt64, 0),
					},
					{
						LTyp: decimal(DecimalMaxWidthInt64, 0),
					},
				},
				RetTypeDecider: func(types []ExprDataType) ExprDataType {
					return ExprDataType{LTyp: boolean(), NotNull: decideNull(types)}
				},
				Body: func() FunctionBody {
					return func(chunk *Chunk, state *ExprState, count int, result *Vector) error {
						return fmt.Errorf("usp")
					}
				},
			},
		},
		ImplDecider: exactImplDecider,
	},
	{
		Id: NOT_EQUAL,
		Impls: []*Impl{
			{
				Desc: "<>",
				Idx:  0,
				Args: []ExprDataType{
					{
						LTyp: integer(),
					},
					{
						LTyp: integer(),
					},
				},
				RetTypeDecider: func(types []ExprDataType) ExprDataType {
					return ExprDataType{LTyp: boolean(), NotNull: decideNull(types)}
				},
				Body: func() FunctionBody {
					return func(chunk *Chunk, state *ExprState, count int, result *Vector) error {
						return fmt.Errorf("usp")
					}
				},
			},
			{
				Desc: "<>",
				Idx:  1,
				Args: []ExprDataType{
					{
						LTyp: varchar(),
					},
					{
						LTyp: varchar(),
					},
				},
				RetTypeDecider: func(types []ExprDataType) ExprDataType {
					return ExprDataType{LTyp: boolean(), NotNull: decideNull(types)}
				},
				Body: func() FunctionBody {
					return func(chunk *Chunk, state *ExprState, count int, result *Vector) error {
						return fmt.Errorf("usp")
					}
				},
			},
		},
		ImplDecider: exactImplDecider,
	},
	{
		Id: GREAT_EQUAL,
		Impls: []*Impl{
			{
				Desc: ">=",
				Idx:  0,
				Args: []ExprDataType{
					{
						LTyp: dateLTyp(),
					},
					{
						LTyp: dateLTyp(),
					},
				},
				RetTypeDecider: func(types []ExprDataType) ExprDataType {
					return ExprDataType{LTyp: boolean(), NotNull: decideNull(types)}
				},
				Body: func() FunctionBody {
					return func(chunk *Chunk, state *ExprState, count int, result *Vector) error {
						return fmt.Errorf("usp")
					}
				},
			},
			{
				Desc: ">=",
				Idx:  1,
				Args: []ExprDataType{
					{
						LTyp: integer(),
					},
					{
						LTyp: integer(),
					},
				},
				RetTypeDecider: func(types []ExprDataType) ExprDataType {
					return ExprDataType{LTyp: boolean(), NotNull: decideNull(types)}
				},
				Body: func() FunctionBody {
					return func(chunk *Chunk, state *ExprState, count int, result *Vector) error {
						return fmt.Errorf("usp")
					}
				},
			},
		},
		ImplDecider: exactImplDecider,
	},
	{
		Id: GREAT,
		Impls: []*Impl{
			{
				Desc: ">",
				Idx:  0,
				Args: []ExprDataType{
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
					return func(chunk *Chunk, state *ExprState, count int, result *Vector) error {
						binaryExecSwitch[float32, bool](chunk._data[0], chunk._data[1], result, count, gBinFloat32Great, nil, gBinFloat32BoolSingleOpWrapper)
						return nil
					}
				},
			},
			{
				Desc: ">",
				Idx:  1,
				Args: []ExprDataType{
					{
						LTyp: decimal(DecimalMaxWidthInt64, 0),
					},
					{
						LTyp: decimal(DecimalMaxWidthInt64, 0),
					},
				},
				RetTypeDecider: func(types []ExprDataType) ExprDataType {
					return ExprDataType{LTyp: boolean(), NotNull: decideNull(types)}
				},
				Body: func() FunctionBody {
					return func(chunk *Chunk, state *ExprState, count int, result *Vector) error {
						return fmt.Errorf("usp")
					}
				},
			},
			{
				Desc: ">",
				Idx:  2,
				Args: []ExprDataType{
					{
						LTyp: dateLTyp(),
					},
					{
						LTyp: dateLTyp(),
					},
				},
				RetTypeDecider: func(types []ExprDataType) ExprDataType {
					return ExprDataType{LTyp: boolean(), NotNull: decideNull(types)}
				},
				Body: func() FunctionBody {
					return func(chunk *Chunk, state *ExprState, count int, result *Vector) error {
						return fmt.Errorf("usp")
					}
				},
			},
			{
				Desc: ">",
				Idx:  3,
				Args: []ExprDataType{
					{
						LTyp: integer(),
					},
					{
						LTyp: integer(),
					},
				},
				RetTypeDecider: func(types []ExprDataType) ExprDataType {
					return ExprDataType{LTyp: boolean(), NotNull: decideNull(types)}
				},
				Body: func() FunctionBody {
					return func(chunk *Chunk, state *ExprState, count int, result *Vector) error {
						binaryExecSwitch[int32, bool](chunk._data[0], chunk._data[1], result, count, gBinInt32Great, nil, gBinInt32BoolSingleOpWrapper)
						return nil
					}
				},
			},
		},
		ImplDecider: exactImplDecider,
	},
	{
		Id: LESS_EQUAL,
		Impls: []*Impl{
			{
				Desc: "<=",
				Idx:  0,
				Args: []ExprDataType{
					{
						LTyp: dateLTyp(),
					},
					{
						LTyp: dateLTyp(),
					},
				},
				RetTypeDecider: func(types []ExprDataType) ExprDataType {
					return ExprDataType{LTyp: boolean(), NotNull: decideNull(types)}
				},
				Body: func() FunctionBody {
					return func(chunk *Chunk, state *ExprState, count int, result *Vector) error {
						return fmt.Errorf("usp")
					}
				},
			},
			{
				Desc: "<=",
				Idx:  1,
				Args: []ExprDataType{
					{
						LTyp: integer(),
					},
					{
						LTyp: integer(),
					},
				},
				RetTypeDecider: func(types []ExprDataType) ExprDataType {
					return ExprDataType{LTyp: boolean(), NotNull: decideNull(types)}
				},
				Body: func() FunctionBody {
					return func(chunk *Chunk, state *ExprState, count int, result *Vector) error {
						return fmt.Errorf("usp")
					}
				},
			},
		},
		ImplDecider: exactImplDecider,
	},
	{
		Id: LESS,
		Impls: []*Impl{
			{
				Desc: "<",
				Idx:  0,
				Args: []ExprDataType{
					{
						LTyp: dateLTyp(),
					},
					{
						LTyp: dateLTyp(),
					},
				},
				RetTypeDecider: func(types []ExprDataType) ExprDataType {
					return ExprDataType{LTyp: boolean(), NotNull: decideNull(types)}
				},
				Body: func() FunctionBody {
					return func(chunk *Chunk, state *ExprState, count int, result *Vector) error {
						return fmt.Errorf("usp")
					}
				},
			},
			{
				Desc: "<",
				Idx:  1,
				Args: []ExprDataType{
					{
						LTyp: integer(),
					},
					{
						LTyp: integer(),
					},
				},
				RetTypeDecider: func(types []ExprDataType) ExprDataType {
					return ExprDataType{LTyp: boolean(), NotNull: decideNull(types)}
				},
				Body: func() FunctionBody {
					return func(chunk *Chunk, state *ExprState, count int, result *Vector) error {
						return fmt.Errorf("usp")
					}
				},
			},
			{
				Desc: "<",
				Idx:  2,
				Args: []ExprDataType{
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
					return func(chunk *Chunk, state *ExprState, count int, result *Vector) error {
						return fmt.Errorf("usp")
					}
				},
			},
		},
		ImplDecider: exactImplDecider,
	},
	{
		Id: AND,
		Impls: []*Impl{
			{
				Desc: "and",
				Idx:  0,
				Args: []ExprDataType{
					{
						LTyp: boolean(),
					},
					{
						LTyp: boolean(),
					},
				},
				RetTypeDecider: func(types []ExprDataType) ExprDataType {
					return ExprDataType{LTyp: boolean(), NotNull: decideNull(types)}
				},
				Body: func() FunctionBody {
					return func(chunk *Chunk, state *ExprState, count int, result *Vector) error {
						return fmt.Errorf("usp")
					}
				},
			},
		},
		ImplDecider: exactImplDecider,
	},
	{
		Id: OR,
		Impls: []*Impl{
			{
				Desc: "or",
				Idx:  0,
				Args: []ExprDataType{
					{
						LTyp: boolean(),
					},
					{
						LTyp: boolean(),
					},
				},
				RetTypeDecider: func(types []ExprDataType) ExprDataType {
					return ExprDataType{LTyp: boolean(), NotNull: decideNull(types)}
				},
				Body: func() FunctionBody {
					return func(chunk *Chunk, state *ExprState, count int, result *Vector) error {
						return fmt.Errorf("usp")
					}
				},
			},
		},
		ImplDecider: exactImplDecider,
	},
	{
		Id: LIKE,
		Impls: []*Impl{
			{
				Desc: "like",
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
					return func(chunk *Chunk, state *ExprState, count int, result *Vector) error {
						return fmt.Errorf("usp")
					}
				},
			},
		},
		ImplDecider: opInImplDecider,
	},
	{
		Id: NOT_LIKE,
		Impls: []*Impl{
			{
				Desc: "not like",
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
					return func(chunk *Chunk, state *ExprState, count int, result *Vector) error {
						return fmt.Errorf("usp")
					}
				},
			},
		},
		ImplDecider: opInImplDecider,
	},
	{
		Id: CASE,
		Impls: []*Impl{
			{
				Desc: "case",
				Idx:  0,
				Args: []ExprDataType{
					{
						LTyp: null(),
					},
					{
						LTyp: decimal(DecimalMaxWidthInt64, 0),
					},
					{
						LTyp: boolean(),
					},
					{
						LTyp: decimal(DecimalMaxWidthInt64, 0),
					},
				},
				RetTypeDecider: func(types []ExprDataType) ExprDataType {
					return ExprDataType{LTyp: types[3].LTyp, NotNull: decideNull(types)}
				},
				Body: func() FunctionBody {
					return func(chunk *Chunk, state *ExprState, count int, result *Vector) error {
						return fmt.Errorf("usp")
					}
				},
			},
			{
				Desc: "case",
				Idx:  1,
				Args: []ExprDataType{
					{
						LTyp: null(),
					},
					{
						LTyp: integer(),
					},
					{
						LTyp: boolean(),
					},
					{
						LTyp: integer(),
					},
				},
				RetTypeDecider: func(types []ExprDataType) ExprDataType {
					return ExprDataType{LTyp: types[3].LTyp, NotNull: decideNull(types)}
				},
				Body: func() FunctionBody {
					return func(chunk *Chunk, state *ExprState, count int, result *Vector) error {
						return fmt.Errorf("usp")
					}
				},
			},
		},
		ImplDecider: exactImplDecider,
	},
	{
		Id: EXISTS,
		Impls: []*Impl{
			{
				Desc: "exists",
				Idx:  0,
				Args: []ExprDataType{
					{
						LTyp: integer(),
					},
				},
				RetTypeDecider: func(types []ExprDataType) ExprDataType {
					return ExprDataType{LTyp: boolean(), NotNull: decideNull(types)}
				},
				Body: func() FunctionBody {
					return func(chunk *Chunk, state *ExprState, count int, result *Vector) error {
						return fmt.Errorf("usp")
					}
				},
			},
		},
		ImplDecider: exactImplDecider,
	},
	{
		Id: NOT_EXISTS,
		Impls: []*Impl{
			{
				Desc: "not exists",
				Idx:  0,
				Args: []ExprDataType{
					{
						LTyp: boolean(),
					},
				},
				RetTypeDecider: func(types []ExprDataType) ExprDataType {
					return ExprDataType{LTyp: boolean(), NotNull: decideNull(types)}
				},
				Body: func() FunctionBody {
					return func(chunk *Chunk, state *ExprState, count int, result *Vector) error {
						return fmt.Errorf("usp")
					}
				},
			},
			{
				Desc: "not exists",
				Idx:  1,
				Args: []ExprDataType{
					{
						LTyp: integer(),
					},
				},
				RetTypeDecider: func(types []ExprDataType) ExprDataType {
					return ExprDataType{LTyp: boolean(), NotNull: decideNull(types)}
				},
				Body: func() FunctionBody {
					return func(chunk *Chunk, state *ExprState, count int, result *Vector) error {
						return fmt.Errorf("usp")
					}
				},
			},
		},
		ImplDecider: exactImplDecider,
	},
}

var aggFuncs = []*Function{
	{
		Id: MAX,
		Impls: []*Impl{
			{
				Desc: "max",
				Idx:  0,
				Args: []ExprDataType{
					{
						LTyp: decimal(DecimalMaxWidthInt64, 0),
					},
				},
				RetTypeDecider: func(types []ExprDataType) ExprDataType {
					return ExprDataType{LTyp: types[0].LTyp, NotNull: decideNull(types)}
				},
				Body: func() FunctionBody {
					return func(chunk *Chunk, state *ExprState, count int, result *Vector) error {
						return fmt.Errorf("usp")
					}
				},
				IsAgg: true,
			},
		},
		ImplDecider: exactImplDecider,
	},
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
					return ExprDataType{LTyp: types[0].LTyp, NotNull: decideNull(types)}
				},
				Body: func() FunctionBody {
					return func(chunk *Chunk, state *ExprState, count int, result *Vector) error {
						return fmt.Errorf("usp")
					}
				},
				IsAgg: true,
			},
		},
		ImplDecider: exactImplDecider,
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
					return ExprDataType{LTyp: types[0].LTyp, NotNull: decideNull(types)}
				},
				Body: func() FunctionBody {
					return func(chunk *Chunk, state *ExprState, count int, result *Vector) error {
						return fmt.Errorf("usp")
					}
				},
				IsAgg: true,
			},
			{
				Desc: "sum",
				Idx:  1,
				Args: []ExprDataType{
					{
						LTyp: integer(),
					},
				},
				RetTypeDecider: func(types []ExprDataType) ExprDataType {
					return ExprDataType{LTyp: integer(), NotNull: decideNull(types)}
				},
				Body: func() FunctionBody {
					return func(chunk *Chunk, state *ExprState, count int, result *Vector) error {
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
					return func(chunk *Chunk, state *ExprState, count int, result *Vector) error {
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
					return func(chunk *Chunk, state *ExprState, count int, result *Vector) error {
						return fmt.Errorf("usp")
					}
				},
				IsAgg: true,
			},
			{
				Desc: "avg",
				Idx:  1,
				Args: []ExprDataType{
					{
						LTyp: integer(),
					},
				},
				RetTypeDecider: func(types []ExprDataType) ExprDataType {
					return ExprDataType{
						LTyp:    types[0].LTyp,
						NotNull: decideNull(types),
					}
				},
				Body: func() FunctionBody {
					return func(chunk *Chunk, state *ExprState, count int, result *Vector) error {
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
					return ExprDataType{LTyp: dateLTyp(), NotNull: decideNull(types)}
				},
				Body: func() FunctionBody {
					return func(chunk *Chunk, state *ExprState, count int, result *Vector) error {
						return fmt.Errorf("usp")
					}
				},
			},
		},
		ImplDecider: exactImplDecider,
	},
	{
		Id: EXTRACT,
		Impls: []*Impl{
			{
				Desc: "extract",
				Idx:  0,
				Args: []ExprDataType{
					{LTyp: varchar()},
					{LTyp: dateLTyp()},
				},
				RetTypeDecider: func(types []ExprDataType) ExprDataType {
					return ExprDataType{LTyp: dateLTyp(), NotNull: decideNull(types)}
				},
				Body: func() FunctionBody {
					return func(chunk *Chunk, state *ExprState, count int, result *Vector) error {
						return fmt.Errorf("usp")
					}
				},
			},
		},
		ImplDecider: exactImplDecider,
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
					return ExprDataType{LTyp: types[1].LTyp, NotNull: decideNull(types)}
				},
				Body: func() FunctionBody {
					return func(chunk *Chunk, state *ExprState, count int, result *Vector) error {
						return fmt.Errorf("usp")
					}
				},
			},
			{
				Desc: "cast",
				Idx:  1,
				Args: []ExprDataType{
					{
						LTyp: integer(),
					},
					{
						LTyp: integer(),
					},
				},
				RetTypeDecider: func(types []ExprDataType) ExprDataType {
					return ExprDataType{LTyp: types[1].LTyp, NotNull: decideNull(types)}
				},
				Body: func() FunctionBody {
					return func(chunk *Chunk, state *ExprState, count int, result *Vector) error {
						castExec(chunk._data[0], result, count)
						return nil
					}
				},
			},
			{
				Desc: "cast",
				Idx:  2,
				Args: []ExprDataType{
					{
						LTyp: integer(),
					},
					{
						LTyp: float(),
					},
				},
				RetTypeDecider: func(types []ExprDataType) ExprDataType {
					return ExprDataType{LTyp: types[1].LTyp, NotNull: decideNull(types)}
				},
				Body: func() FunctionBody {
					return func(chunk *Chunk, state *ExprState, count int, result *Vector) error {
						castExec(chunk._data[0], result, count)
						return nil
					}
				},
			},
			{
				Desc: "cast",
				Idx:  3,
				Args: []ExprDataType{
					{
						LTyp: float(),
					},
					{
						LTyp: integer(),
					},
				},
				RetTypeDecider: func(types []ExprDataType) ExprDataType {
					return ExprDataType{LTyp: types[1].LTyp, NotNull: decideNull(types)}
				},
				Body: func() FunctionBody {
					return func(chunk *Chunk, state *ExprState, count int, result *Vector) error {
						castExec(chunk._data[0], result, count)
						return nil
					}
				},
			},
		},
		ImplDecider: exactImplDecider,
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
					return func(chunk *Chunk, state *ExprState, count int, result *Vector) error {
						return fmt.Errorf("usp")
					}
				},
			},
		},
		ImplDecider: exactImplDecider,
	},
}
