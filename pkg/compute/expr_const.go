// Package compute provides core computation functionality
package compute

import (
	"fmt"
)

// ConstType represents the type of a constant value
type ConstType int

const (
	ConstTypeInvalid ConstType = iota
	ConstTypeInteger
	ConstTypeDecimal
	ConstTypeString
	ConstTypeFloat
	ConstTypeDate
	ConstTypeInterval
	ConstTypeBoolean
	ConstTypeNull
)

// ConstValue represents a constant value with its type and actual value
type ConstValue struct {
	Type ConstType

	// 每种类型一个具体字段
	Integer  int64
	Decimal  string
	String   string
	Float    float64
	Date     string
	Interval struct {
		Value int64
		Unit  string
	}
	Boolean bool
	// Null类型不需要值字段
}

// NewIntegerConst creates a new integer constant
func NewIntegerConst(value int64) ConstValue {
	return ConstValue{
		Type:    ConstTypeInteger,
		Integer: value,
	}
}

// NewDecimalConst creates a new decimal constant
func NewDecimalConst(value string) ConstValue {
	return ConstValue{
		Type:    ConstTypeDecimal,
		Decimal: value,
	}
}

// NewStringConst creates a new string constant
func NewStringConst(value string) ConstValue {
	return ConstValue{
		Type:   ConstTypeString,
		String: value,
	}
}

// NewFloatConst creates a new float constant
func NewFloatConst(value float64) ConstValue {
	return ConstValue{
		Type:  ConstTypeFloat,
		Float: value,
	}
}

// NewDateConst creates a new date constant
func NewDateConst(value string) ConstValue {
	return ConstValue{
		Type: ConstTypeDate,
		Date: value,
	}
}

// NewIntervalConst creates a new interval constant
func NewIntervalConst(value int64, unit string) ConstValue {
	return ConstValue{
		Type: ConstTypeInterval,
		Interval: struct {
			Value int64
			Unit  string
		}{value, unit},
	}
}

// NewBooleanConst creates a new boolean constant
func NewBooleanConst(value bool) ConstValue {
	return ConstValue{
		Type:    ConstTypeBoolean,
		Boolean: value,
	}
}

// NewNullConst creates a new null constant
func NewNullConst() ConstValue {
	return ConstValue{
		Type: ConstTypeNull,
	}
}

// GetInteger returns the integer value of the constant
func (c ConstValue) GetInteger() (int64, error) {
	if c.Type != ConstTypeInteger {
		return 0, fmt.Errorf("not an integer constant")
	}
	return c.Integer, nil
}

// GetDecimal returns the decimal value of the constant
func (c ConstValue) GetDecimal() (string, error) {
	if c.Type != ConstTypeDecimal {
		return "", fmt.Errorf("not a decimal constant")
	}
	return c.Decimal, nil
}

// GetString returns the string value of the constant
func (c ConstValue) GetString() (string, error) {
	if c.Type != ConstTypeString {
		return "", fmt.Errorf("not a string constant")
	}
	return c.String, nil
}

// GetFloat returns the float value of the constant
func (c ConstValue) GetFloat() (float64, error) {
	if c.Type != ConstTypeFloat {
		return 0, fmt.Errorf("not a float constant")
	}
	return c.Float, nil
}

// GetDate returns the date value of the constant
func (c ConstValue) GetDate() (string, error) {
	if c.Type != ConstTypeDate {
		return "", fmt.Errorf("not a date constant")
	}
	return c.Date, nil
}

// GetInterval returns the interval value and unit of the constant
func (c ConstValue) GetInterval() (int64, string, error) {
	if c.Type != ConstTypeInterval {
		return 0, "", fmt.Errorf("not an interval constant")
	}
	return c.Interval.Value, c.Interval.Unit, nil
}

// GetBoolean returns the boolean value of the constant
func (c ConstValue) GetBoolean() (bool, error) {
	if c.Type != ConstTypeBoolean {
		return false, fmt.Errorf("not a boolean constant")
	}
	return c.Boolean, nil
}

// IsNull returns whether the constant is null
func (c ConstValue) IsNull() bool {
	return c.Type == ConstTypeNull
}

// equal compares two constant values for equality
func (c ConstValue) equal(o ConstValue) bool {
	if c.Type != o.Type {
		return false
	}
	switch c.Type {
	case ConstTypeInteger:
		return c.Integer == o.Integer
	case ConstTypeDecimal:
		return c.Decimal == o.Decimal
	case ConstTypeString:
		return c.String == o.String
	case ConstTypeFloat:
		return c.Float == o.Float
	case ConstTypeDate:
		return c.Date == o.Date
	case ConstTypeInterval:
		return c.Interval.Value == o.Interval.Value && c.Interval.Unit == o.Interval.Unit
	case ConstTypeBoolean:
		return c.Boolean == o.Boolean
	case ConstTypeNull:
		return true
	default:
		return true
	}
}

// copy returns a copy of the constant value
func (c ConstValue) copy() ConstValue {
	return c // 由于是值类型，直接返回即可
}
