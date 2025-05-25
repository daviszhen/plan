package common

import (
	decimal2 "github.com/govalues/decimal"
)

type Decimal struct {
	decimal2.Decimal
}

func (dec *Decimal) Equal(o *Decimal) bool {
	return dec.Decimal.Cmp(o.Decimal) == 0
}

func (dec *Decimal) String() string {
	return dec.Decimal.String()
}

func (dec *Decimal) Add(lhs *Decimal, rhs *Decimal) {
	res, err := lhs.Decimal.Add(rhs.Decimal)
	if err != nil {
		panic(err)
	}
	lhs.Decimal = res
}

func (dec *Decimal) Mul(lhs *Decimal, rhs *Decimal) {
	res, err := lhs.Decimal.Mul(rhs.Decimal)
	if err != nil {
		panic(err)
	}
	lhs.Decimal = res
}

func (dec *Decimal) Less(lhs, rhs *Decimal) bool {
	return lhs.Decimal.Cmp(rhs.Decimal) < 0
}

func (dec *Decimal) Greater(lhs, rhs *Decimal) bool {
	return lhs.Decimal.Cmp(rhs.Decimal) > 0
}

func NegateDecimal(input *Decimal, result *Decimal) {
	result.Decimal = input.Decimal.Neg()
}
