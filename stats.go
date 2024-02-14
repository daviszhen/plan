package main

import "fmt"

type BaseStats struct {
    typ           DataType
    hasNull       bool
    hasNoNull     bool
    distinctCount uint64
}

func (bs BaseStats) getDistinctCount() uint64 {
    return bs.distinctCount
}

func (bs BaseStats) Copy() *BaseStats {
    return &BaseStats{
        typ:           bs.typ,
        hasNull:       bs.hasNull,
        hasNoNull:     bs.hasNoNull,
        distinctCount: bs.distinctCount,
    }
}

func (bs BaseStats) String() string {
    return fmt.Sprintf("%v %v %v %v", bs.typ, bs.hasNull, bs.hasNoNull, bs.distinctCount)
}
