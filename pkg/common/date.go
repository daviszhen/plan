package common

import (
	"time"
)

//lint:ignore U1000
type Date struct {
	Year  int32
	Month int32
	Day   int32
}

func (d *Date) Equal(o *Date) bool {
	return d.Year == o.Year && d.Month == o.Month && d.Day == o.Day
}

func (d *Date) Less(o *Date) bool {
	d1 := d.ToDate()
	o1 := o.ToDate()
	return d1.Before(o1)
}

func (d *Date) ToDate() time.Time {
	return time.Date(int(d.Year), time.Month(d.Month), int(d.Day), 0, 0, 0, 0, time.UTC)
}

func (d *Date) AddInterval(rhs *Interval) Date {
	lhsD := d.ToDate()
	resD := lhsD.AddDate(int(rhs.Year), int(rhs.Months), int(rhs.Days))
	y, m, day := resD.Date()
	return Date{
		Year:  int32(y),
		Month: int32(m),
		Day:   int32(day),
	}
}

func (d *Date) SubInterval(rhs *Interval) Date {
	lhsD := d.ToDate()
	resD := lhsD.AddDate(int(-rhs.Year), int(-rhs.Months), int(-rhs.Days))
	y, m, day := resD.Date()
	return Date{
		Year:  int32(y),
		Month: int32(m),
		Day:   int32(day),
	}
}
