package compute

type OrderType int

const (
	OT_INVALID OrderType = iota
	OT_DEFAULT
	OT_ASC
	OT_DESC
)

type OrderByNullType int

const (
	OBNT_INVALID OrderByNullType = iota
	OBNT_DEFAULT
	OBNT_NULLS_FIRST
	OBNT_NULLS_LAST
)

const (
	VALUES_PER_RADIX              = 256
	MSD_RADIX_LOCATIONS           = VALUES_PER_RADIX + 1
	INSERTION_SORT_THRESHOLD      = 24
	MSD_RADIX_SORT_SIZE_THRESHOLD = 4
)

type SortState int

const (
	SS_INIT SortState = iota
	SS_SORT
	SS_SCAN
)

type SortedDataType int

const (
	SDT_BLOB    SortedDataType = 0
	SDT_PAYLOAD SortedDataType = 1
)
