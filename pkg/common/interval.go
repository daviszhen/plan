package common

import (
	"time"
)

//lint:ignore U1000
type Interval struct {
	Months int32
	Days   int32
	Micros int32

	Unit string
	Year int32
}

func (i *Interval) Equal(o *Interval) bool {
	return i.Months == o.Months &&
		i.Days == o.Days &&
		i.Micros == o.Micros
}

func (i *Interval) Less(o *Interval) bool {
	panic("usp")
}

func (i *Interval) Milli() int64 {
	switch i.Unit {
	case "year":
		d := time.Date(
			int(1970+i.Year),
			1,
			1,
			0, 0, 0, 0, time.UTC)
		return d.UnixMilli()
	case "month":
		d := time.Date(
			1970,
			time.Month(1+i.Months),
			1,
			0, 0, 0, 0, time.UTC)
		return d.UnixMilli()
	case "day":
		d := time.Date(
			1970,
			1,
			int(1+i.Days),
			0, 0, 0, 0, time.UTC)
		return d.UnixMilli()
	default:
		panic("usp")
	}
}
