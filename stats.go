package main

type BaseStats struct {
    typ           DataType
    hasNull       bool
    hasNoNull     bool
    distinctCount uint64
}

func (bs BaseStats) getDistinctCount() uint64 {
    return bs.distinctCount
}
