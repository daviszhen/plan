package chunk

import (
	"fmt"
	"math"
	"math/big"
	"time"

	"github.com/daviszhen/plan/pkg/common"
	"github.com/govalues/decimal"
)

type Value struct {
	Typ    common.LType
	IsNull bool
	//value
	Bool  bool
	I64   int64
	I64_1 int64
	I64_2 int64
	U64   uint64
	F64   float64
	Str   string
}

func (val Value) String() string {
	if val.IsNull {
		return "NULL"
	}
	switch val.Typ.Id {
	case common.LTID_INTEGER:
		return fmt.Sprintf("%d", val.I64)
	case common.LTID_BOOLEAN:
		return fmt.Sprintf("%v", val.Bool)
	case common.LTID_VARCHAR:
		return val.Str
	case common.LTID_DECIMAL:
		if len(val.Str) != 0 {
			return val.Str
		} else {
			d, err := decimal.NewFromInt64(val.I64, val.I64_1, val.Typ.Scale)
			if err != nil {
				panic(err)
			}
			return d.String()
		}
	case common.LTID_DATE:
		dat := time.Date(int(val.I64), time.Month(val.I64_1), int(val.I64_2),
			0, 0, 0, 0, time.UTC)
		return dat.Format(time.DateOnly)
	case common.LTID_BIGINT:
		return fmt.Sprintf("%d", val.I64)
	case common.LTID_UBIGINT:
		return fmt.Sprintf("0x%x %d", val.I64, val.I64)
	case common.LTID_DOUBLE:
		return fmt.Sprintf("%v", val.F64)
	case common.LTID_FLOAT:
		return fmt.Sprintf("%v", val.F64)
	case common.LTID_POINTER:
		return fmt.Sprintf("0x%x", val.I64)
	case common.LTID_HUGEINT:
		h := big.NewInt(val.I64)
		l := big.NewInt(val.I64_1)
		h.Lsh(h, 64)
		h.Add(h, l)
		return fmt.Sprintf("%v", h.String())
	default:
		panic("usp")
	}
}

var (
	POWERS_OF_TEN = []int64{
		1,
		10,
		100,
		1000,
		10000,
		100000,
		1000000,
		10000000,
		100000000,
		1000000000,
		10000000000,
		100000000000,
		1000000000000,
		10000000000000,
		100000000000000,
		1000000000000000,
		10000000000000000,
		100000000000000000,
		1000000000000000000,
	}
)

func MaxValue(typ common.LType) *Value {
	ret := &Value{
		Typ: typ,
	}
	switch typ.Id {
	case common.LTID_BOOLEAN:
		ret.Bool = true
	case common.LTID_INTEGER:
		ret.I64 = math.MaxInt32
	case common.LTID_BIGINT:
		ret.I64 = math.MaxInt64
	case common.LTID_UBIGINT:
		ret.U64 = math.MaxUint64
	case common.LTID_DECIMAL:
		ret.I64 = POWERS_OF_TEN[typ.Width] - 1
	case common.LTID_DATE:
		ret.I64 = 5881580
		ret.I64_1 = 7
		ret.I64_2 = 10
	default:
		panic("usp")
	}
	return ret
}

func MinValue(typ common.LType) *Value {
	ret := &Value{
		Typ: typ,
	}
	switch typ.Id {
	case common.LTID_BOOLEAN:
		ret.Bool = false
	case common.LTID_INTEGER:
		ret.I64 = math.MinInt32
	case common.LTID_BIGINT:
		ret.I64 = math.MinInt64
	case common.LTID_UBIGINT:
		ret.I64 = 0
	case common.LTID_DECIMAL:
		ret.I64 = -POWERS_OF_TEN[typ.Width] + 1
	case common.LTID_DATE:
		ret.I64 = -5877641
		ret.I64_1 = 6
		ret.I64_2 = 25
	default:
		panic("usp")
	}
	return ret
}
