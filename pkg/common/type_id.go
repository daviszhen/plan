package common

import "fmt"

type LTypeId int

const (
	LTID_INVALID         LTypeId = 0
	LTID_NULL            LTypeId = 1
	LTID_UNKNOWN         LTypeId = 2
	LTID_ANY             LTypeId = 3
	LTID_USER            LTypeId = 4
	LTID_BOOLEAN         LTypeId = 10
	LTID_TINYINT         LTypeId = 11
	LTID_SMALLINT        LTypeId = 12
	LTID_INTEGER         LTypeId = 13
	LTID_BIGINT          LTypeId = 14
	LTID_DATE            LTypeId = 15
	LTID_TIME            LTypeId = 16
	LTID_TIMESTAMP_SEC   LTypeId = 17
	LTID_TIMESTAMP_MS    LTypeId = 18
	LTID_TIMESTAMP       LTypeId = 19
	LTID_TIMESTAMP_NS    LTypeId = 20
	LTID_DECIMAL         LTypeId = 21
	LTID_FLOAT           LTypeId = 22
	LTID_DOUBLE          LTypeId = 23
	LTID_CHAR            LTypeId = 24
	LTID_VARCHAR         LTypeId = 25
	LTID_BLOB            LTypeId = 26
	LTID_INTERVAL        LTypeId = 27
	LTID_UTINYINT        LTypeId = 28
	LTID_USMALLINT       LTypeId = 29
	LTID_UINTEGER        LTypeId = 30
	LTID_UBIGINT         LTypeId = 31
	LTID_TIMESTAMP_TZ    LTypeId = 32
	LTID_TIME_TZ         LTypeId = 34
	LTID_BIT             LTypeId = 36
	LTID_HUGEINT         LTypeId = 50
	LTID_POINTER         LTypeId = 51
	LTID_VALIDITY        LTypeId = 53
	LTID_UUID            LTypeId = 54
	LTID_STRUCT          LTypeId = 100
	LTID_LIST            LTypeId = 101
	LTID_MAP             LTypeId = 102
	LTID_TABLE           LTypeId = 103
	LTID_ENUM            LTypeId = 104
	LTID_AGGREGATE_STATE LTypeId = 105
	LTID_LAMBDA          LTypeId = 106
	LTID_UNION           LTypeId = 107
)

var lTypeIdToStr = map[LTypeId]string{
	LTID_INVALID:         "LTID_INVALID",
	LTID_NULL:            "LTID_NULL",
	LTID_UNKNOWN:         "LTID_UNKNOWN",
	LTID_ANY:             "LTID_ANY",
	LTID_USER:            "LTID_USER",
	LTID_BOOLEAN:         "LTID_BOOLEAN",
	LTID_TINYINT:         "LTID_TINYINT",
	LTID_SMALLINT:        "LTID_SMALLINT",
	LTID_INTEGER:         "LTID_INTEGER",
	LTID_BIGINT:          "LTID_BIGINT",
	LTID_DATE:            "LTID_DATE",
	LTID_TIME:            "LTID_TIME",
	LTID_TIMESTAMP_SEC:   "LTID_TIMESTAMP_SEC",
	LTID_TIMESTAMP_MS:    "LTID_TIMESTAMP_MS",
	LTID_TIMESTAMP:       "LTID_TIMESTAMP",
	LTID_TIMESTAMP_NS:    "LTID_TIMESTAMP_NS",
	LTID_DECIMAL:         "LTID_DECIMAL",
	LTID_FLOAT:           "LTID_FLOAT",
	LTID_DOUBLE:          "LTID_DOUBLE",
	LTID_CHAR:            "LTID_CHAR",
	LTID_VARCHAR:         "LTID_VARCHAR",
	LTID_BLOB:            "LTID_BLOB",
	LTID_INTERVAL:        "LTID_INTERVAL",
	LTID_UTINYINT:        "LTID_UTINYINT",
	LTID_USMALLINT:       "LTID_USMALLINT",
	LTID_UINTEGER:        "LTID_UINTEGER",
	LTID_UBIGINT:         "LTID_UBIGINT",
	LTID_TIMESTAMP_TZ:    "LTID_TIMESTAMP_TZ",
	LTID_TIME_TZ:         "LTID_TIME_TZ",
	LTID_BIT:             "LTID_BIT",
	LTID_HUGEINT:         "LTID_HUGEINT",
	LTID_POINTER:         "LTID_POINTER",
	LTID_VALIDITY:        "LTID_VALIDITY",
	LTID_UUID:            "LTID_UUID",
	LTID_STRUCT:          "LTID_STRUCT",
	LTID_LIST:            "LTID_LIST",
	LTID_MAP:             "LTID_MAP",
	LTID_TABLE:           "LTID_TABLE",
	LTID_ENUM:            "LTID_ENUM",
	LTID_AGGREGATE_STATE: "LTID_AGGREGATE_STATE",
	LTID_LAMBDA:          "LTID_LAMBDA",
	LTID_UNION:           "LTID_UNION",
}

func (id LTypeId) String() string {
	if s, has := lTypeIdToStr[id]; has {
		return s
	}
	panic(fmt.Sprintf("usp %d", id))
}
