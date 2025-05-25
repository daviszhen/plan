package storage

import (
	"bytes"
	"math"
	"sync"
	"sync/atomic"

	hll "github.com/axiomhq/hyperloglog"
	dec "github.com/govalues/decimal"

	"github.com/daviszhen/plan/pkg/chunk"
	"github.com/daviszhen/plan/pkg/common"
	"github.com/daviszhen/plan/pkg/util"
)

type StatsInfo uint8

const (
	StatsInfoCanHaveNullValues         StatsInfo = 0
	StatsInfoCanNotHaveNullValues      StatsInfo = 1
	StatsInfoCanHaveValidValues        StatsInfo = 2
	StatsInfoCanNotHaveValidValues     StatsInfo = 3
	StatsInfoCanHaveNullAndValidValues StatsInfo = 4
)

type StatsType uint8

const (
	StatsTypeNumeric StatsType = 0
	StatsTypeString  StatsType = 1
	StatsTypeBase    StatsType = 4
)

type NumericValueUnion struct {
	_value struct {
		_bool    bool
		_int8    int8
		_int16   int16
		_int32   int32
		_int64   int64
		_uint8   uint8
		_uint16  uint16
		_uint32  uint32
		_uint64  uint64
		_hugeint common.Hugeint
		_decimal common.Decimal
		_date    common.Date
		_float   float32
		_double  float64
	}
}

type NumericStatsData struct {
	_hasMin bool
	_hasMax bool
	_min    NumericValueUnion
	_max    NumericValueUnion
}

type NumericStats struct {
}

const (
	MAX_STRING_MINMAX_SIZE uint32 = 8
)

type StringStatsData struct {
	_min                [MAX_STRING_MINMAX_SIZE]byte
	_max                [MAX_STRING_MINMAX_SIZE]byte
	_hasUnicode         bool
	_hasMaxStringLength bool
	_maxStringLength    uint32
}

type StringStats struct {
}

type BaseStats struct {
	_typ           common.LType
	_hasNull       bool
	_hasNoNull     bool
	_distinctCount uint64
	_totalCount    uint64
	_numericData   NumericStatsData
	_stringData    StringStatsData
	_childStats    []*BaseStats
}

func (stats *BaseStats) Set(info StatsInfo) {
	switch info {
	case StatsInfoCanHaveNullValues:
		stats._hasNull = true
	case StatsInfoCanNotHaveNullValues:
		stats._hasNull = false
	case StatsInfoCanHaveValidValues:
		stats._hasNoNull = true
	case StatsInfoCanNotHaveValidValues:
		stats._hasNoNull = false
	case StatsInfoCanHaveNullAndValidValues:
		stats._hasNull = true
		stats._hasNoNull = false
	}
}
func (stats *BaseStats) Count() uint64 {
	return stats._totalCount
}

func (stats *BaseStats) InitEmpty() {
	stats._hasNull = false
	stats._hasNoNull = true
}

func setNumericValue(
	input *chunk.Value,
	typ common.LType,
	val *NumericValueUnion,
	hasVal *bool) {
	if input.IsNull {
		*hasVal = false
		return
	}
	if input.Typ.GetInternalType() != typ.GetInternalType() {
		panic("different physical type")
	}
	*hasVal = true
	switch typ.GetInternalType() {
	case common.BOOL:
		val._value._bool = input.Bool
	case common.INT32:
		val._value._int32 = int32(input.I64)
	case common.INT64:
		val._value._int64 = input.I64
	case common.UINT64:
		val._value._uint64 = input.U64
	case common.DECIMAL:
		decval, err := dec.NewFromInt64(input.I64, 0, typ.Scale)
		if err != nil {
			panic("make decimal failed")
		}
		val._value._decimal = common.Decimal{
			Decimal: decval,
		}
	case common.DATE:
		val._value._date = common.Date{
			Year:  int32(input.I64),
			Month: int32(input.I64_1),
			Day:   int32(input.I64_2),
		}
	default:
		panic("usp")
	}
}

func (stats *BaseStats) SetMin(val *chunk.Value) {
	setNumericValue(
		val,
		stats._typ,
		&stats._numericData._min,
		&stats._numericData._hasMin)
}

func (stats *BaseStats) SetMax(val *chunk.Value) {
	setNumericValue(
		val,
		stats._typ,
		&stats._numericData._max,
		&stats._numericData._hasMax)
}

func numericStatsHasMin(stats *BaseStats) bool {
	if stats._typ.Id == common.LTID_NULL {
		return false
	}
	return stats._numericData._hasMin
}

func numericStatsHasMax(stats *BaseStats) bool {
	if stats._typ.Id == common.LTID_NULL {
		return false
	}
	return stats._numericData._hasMax
}

func numericStatsUpdateMin(stats, other *BaseStats) {
	util.AssertFunc(stats._typ.Id == other._typ.Id)
	minVal := stats._numericData._min._value
	oMinVal := other._numericData._min._value
	switch stats._typ.GetInternalType() {
	case common.BOOL:
		//false < true
		if !oMinVal._bool && minVal._bool {
			minVal._bool = false
		}
	case common.INT32:
		if oMinVal._int32 < minVal._int32 {
			minVal._int32 = oMinVal._int32
		}
	case common.INT64:
		if oMinVal._int64 < minVal._int64 {
			minVal._int64 = oMinVal._int64
		}
	case common.UINT64:
		if oMinVal._uint64 < minVal._uint64 {
			minVal._uint64 = oMinVal._uint64
		}
	case common.DECIMAL:
		if oMinVal._decimal.Decimal.Cmp(minVal._decimal.Decimal) < 0 {
			minVal._decimal.Decimal = oMinVal._decimal.Decimal
		}
	case common.DATE:
		if oMinVal._date.Less(&minVal._date) {
			minVal._date = oMinVal._date
		}
	default:
		panic("usp")
	}
}

func numericStatsUpdateMax(stats, other *BaseStats) {
	util.AssertFunc(stats._typ.Id == other._typ.Id)
	minVal := stats._numericData._min._value
	oMinVal := other._numericData._min._value
	switch stats._typ.GetInternalType() {
	case common.BOOL:
		//false < true
		if oMinVal._bool && !minVal._bool {
			minVal._bool = true
		}
	case common.INT32:
		if oMinVal._int32 > minVal._int32 {
			minVal._int32 = oMinVal._int32
		}
	case common.INT64:
		if oMinVal._int64 > minVal._int64 {
			minVal._int64 = oMinVal._int64
		}
	case common.UINT64:
		if oMinVal._uint64 > minVal._uint64 {
			minVal._uint64 = oMinVal._uint64
		}
	case common.DECIMAL:
		if oMinVal._decimal.Decimal.Cmp(minVal._decimal.Decimal) > 0 {
			minVal._decimal.Decimal = oMinVal._decimal.Decimal
		}
	case common.DATE:
		if minVal._date.Less(&oMinVal._date) {
			minVal._date = oMinVal._date
		}
	default:
		panic("usp")
	}
}

func numericStatsMerge(stats, other *BaseStats) {
	if other._typ.Id == common.LTID_VALIDITY {
		return
	}
	util.AssertFunc(stats._typ.Id == other._typ.Id)
	if numericStatsHasMin(stats) && numericStatsHasMin(other) {
		numericStatsUpdateMin(stats, other)
	} else {
		stats.SetMin(&chunk.Value{Typ: common.Null(), IsNull: true})
	}

	if numericStatsHasMax(stats) && numericStatsHasMax(other) {
		numericStatsUpdateMax(stats, other)
	} else {
		stats.SetMax(&chunk.Value{Typ: common.Null(), IsNull: true})
	}
}

func stringStatsMerge(stats, other *BaseStats) {
	if other._typ.Id == common.LTID_VALIDITY {
		return
	}
	sdata := stats._stringData
	osdata := other._stringData
	if bytes.Compare(osdata._min[:], sdata._min[:]) < 0 {
		copy(sdata._min[:], osdata._min[:])
	}
	if bytes.Compare(osdata._max[:], sdata._max[:]) > 0 {
		copy(sdata._max[:], osdata._max[:])
	}
	sdata._hasUnicode = sdata._hasUnicode || osdata._hasUnicode
	sdata._hasMaxStringLength = sdata._hasMaxStringLength &&
		osdata._hasMaxStringLength
	sdata._maxStringLength = max(sdata._maxStringLength,
		osdata._maxStringLength)
}

// Merge other to me
func (stats *BaseStats) Merge(other *BaseStats) {
	stats._hasNull = stats._hasNull || other._hasNull
	stats._hasNoNull = stats._hasNoNull || other._hasNoNull
	switch GetStatsType(stats._typ) {
	case StatsTypeNumeric:
		numericStatsMerge(stats, other)
	case StatsTypeString:
		stringStatsMerge(stats, other)
	}
}

func (stats *BaseStats) Copy() BaseStats {
	return *stats
}

func (stats *BaseStats) Serialize(serial util.Serialize) error {
	writer := NewFieldWriter(serial)
	if err := WriteField[bool](stats._hasNull, writer); err != nil {
		return err
	}
	if err := WriteField[bool](stats._hasNoNull, writer); err != nil {
		return err
	}
	if err := WriteField[uint64](stats._distinctCount, writer); err != nil {
		return err
	}
	err := stats.Serialize2(writer)
	if err != nil {
		return err
	}
	return writer.Finalize()
}

func (stats *BaseStats) Serialize2(writer *FieldWriter) error {
	switch GetStatsType(stats._typ) {
	case StatsTypeNumeric:
		return numericStatsSerialize(stats, writer)
	case StatsTypeString:
		return stringStatsSerialize(stats, writer)
	}
	return nil
}

func (stats *BaseStats) Deserialize(deserial util.Deserialize, lType common.LType) error {
	stats._typ = lType
	reader, err := NewFieldReader(deserial)
	if err != nil {
		return err
	}
	err = ReadRequired[bool](&stats._hasNull, reader)
	if err != nil {
		return err
	}
	err = ReadRequired[bool](&stats._hasNoNull, reader)
	if err != nil {
		return err
	}
	err = ReadRequired[uint64](&stats._distinctCount, reader)
	if err != nil {
		return err
	}
	switch GetStatsType(lType) {
	case StatsTypeNumeric:
		err = numericStatsDeserialize(stats, reader, lType)
	case StatsTypeString:
		err = stringStatsDeserialize(stats, reader, lType)
	}
	reader.Finalize()
	return err
}

func (stats *BaseStats) DistinctCount() uint64 {
	return stats._distinctCount
}

func (stats *BaseStats) HasNull() bool {
	return stats._hasNull
}

func (stats *BaseStats) HasNoNull() bool {
	return stats._hasNoNull
}

func stringStatsDeserialize(stats *BaseStats, reader *FieldReader, ltyp common.LType) error {
	stats._typ = ltyp
	sdata := stats._stringData
	err := ReadBlob(sdata._min[:], reader)
	if err != nil {
		return err
	}
	err = ReadBlob(sdata._max[:], reader)
	if err != nil {
		return err
	}
	err = ReadRequired[bool](&sdata._hasUnicode, reader)
	if err != nil {
		return err
	}
	err = ReadRequired[bool](&sdata._hasMaxStringLength, reader)
	if err != nil {
		return err
	}
	err = ReadRequired[uint32](&sdata._maxStringLength, reader)
	if err != nil {
		return err
	}
	return err
}

func deserializeNumericStatsValue(
	typ common.LType,
	reader *FieldReader,
	val *NumericValueUnion,
	hasStats *bool,
) error {
	isNull := false
	err := ReadRequired[bool](&isNull, reader)
	if err != nil {
		return err
	}
	if isNull {
		*hasStats = false
		return nil
	}
	switch typ.GetInternalType() {
	case common.BOOL:
		err = ReadRequired[bool](&val._value._bool, reader)
	case common.INT32:
		err = ReadRequired[int32](&val._value._int32, reader)
	case common.INT64:
		err = ReadRequired[int64](&val._value._int64, reader)
	case common.UINT64:
		err = ReadRequired[uint64](&val._value._uint64, reader)
	case common.DECIMAL:
		err = ReadRequired[common.Decimal](&val._value._decimal, reader)
	case common.DATE:
		err = ReadRequired[common.Date](&val._value._date, reader)
	default:
		panic("usp")
	}
	return err
}

func numericStatsDeserialize(stats *BaseStats, reader *FieldReader, ltyp common.LType) error {
	ndata := stats._numericData
	stats._typ = ltyp
	err := deserializeNumericStatsValue(ltyp, reader, &ndata._min, &ndata._hasMin)
	if err != nil {
		return err
	}
	err = deserializeNumericStatsValue(ltyp, reader, &ndata._max, &ndata._hasMax)
	if err != nil {
		return err
	}
	return err
}

func serializeNumericStatsValue(
	typ common.LType,
	val *NumericValueUnion,
	hasValue bool,
	writer *FieldWriter,
) error {
	err := WriteField[bool](!hasValue, writer)
	if err != nil {
		return err
	}
	if !hasValue {
		return nil
	}

	switch typ.GetInternalType() {
	case common.BOOL:
		err = WriteField[bool](val._value._bool, writer)
	case common.INT32:
		err = WriteField[int32](val._value._int32, writer)
	case common.INT64:
		err = WriteField[int64](val._value._int64, writer)
	case common.UINT64:
		err = WriteField[uint64](val._value._uint64, writer)
	case common.DECIMAL:
		err = WriteField[common.Decimal](val._value._decimal, writer)
	case common.DATE:
		err = WriteField[common.Date](val._value._date, writer)
	default:
		panic("usp")
	}
	return err
}

func numericStatsSerialize(stats *BaseStats, writer *FieldWriter) error {
	nstats := stats._numericData
	err := serializeNumericStatsValue(stats._typ, &nstats._min, nstats._hasMin, writer)
	if err != nil {
		return err
	}
	err = serializeNumericStatsValue(stats._typ, &nstats._max, nstats._hasMax, writer)
	if err != nil {
		return err
	}
	return err
}

func stringStatsSerialize(stats *BaseStats, writer *FieldWriter) error {
	data := stats._stringData
	err := WriteBlob(data._min[:], writer)
	if err != nil {
		return err
	}
	err = WriteBlob(data._max[:], writer)
	if err != nil {
		return err
	}
	err = WriteField[bool](data._hasUnicode, writer)
	if err != nil {
		return err
	}
	err = WriteField[bool](data._hasMaxStringLength, writer)
	if err != nil {
		return err
	}
	err = WriteField[uint32](data._maxStringLength, writer)
	if err != nil {
		return err
	}
	return nil
}

func newBaseStats(typ common.LType) BaseStats {
	ret := BaseStats{
		_typ: typ,
	}
	return ret
}

func GetStatsType(typ common.LType) StatsType {
	if typ.Id == common.LTID_NULL {
		return StatsTypeBase
	}
	switch typ.GetInternalType() {
	case common.BOOL:
		fallthrough
	case common.INT8:
		fallthrough
	case common.INT16:
		fallthrough
	case common.INT32:
		fallthrough
	case common.INT64:
		fallthrough
	case common.UINT8:
		fallthrough
	case common.UINT16:
		fallthrough
	case common.UINT32:
		fallthrough
	case common.UINT64:
		fallthrough
	case common.INT128:
		fallthrough
	case common.FLOAT:
		fallthrough
	case common.DOUBLE:
		return StatsTypeNumeric
	case common.VARCHAR:
		return StatsTypeString
	case common.BIT:
		fallthrough
	case common.INTERVAL:
		fallthrough
	default:
		return StatsTypeBase
	}
}

func newEmptyNumericStats(typ common.LType) BaseStats {
	ret := newBaseStats(typ)
	ret.InitEmpty()
	ret.SetMin(chunk.MaxValue(typ))
	ret.SetMax(chunk.MinValue(typ))
	return ret
}

func newEmptyStringStats(typ common.LType) BaseStats {
	ret := newBaseStats(typ)
	ret.InitEmpty()
	for i := 0; i < int(MAX_STRING_MINMAX_SIZE); i++ {
		ret._stringData._min[i] = 0xFF
		ret._stringData._max[i] = 0
	}
	ret._stringData._maxStringLength = 0
	ret._stringData._hasMaxStringLength = true
	ret._stringData._hasUnicode = false
	return ret
}

func newEmptyBaseStats(typ common.LType) BaseStats {
	switch GetStatsType(typ) {
	case StatsTypeNumeric:
		return newEmptyNumericStats(typ)
	case StatsTypeString:
		return newEmptyStringStats(typ)
	default:
		return newBaseStats(typ)
	}
}

func NewEmptyBaseStats(typ common.LType) BaseStats {
	if typ.Id == common.LTID_BIT {
		ret := newBaseStats(typ)
		ret.Set(StatsInfoCanNotHaveNullValues)
		ret.Set(StatsInfoCanNotHaveValidValues)
		return ret
	}
	ret := newEmptyBaseStats(typ)
	ret.InitEmpty()
	return ret
}

type SegmentStats struct {
	_stats BaseStats
}

func NewSegmentStats(typ common.LType) *SegmentStats {
	ret := &SegmentStats{
		_stats: NewEmptyBaseStats(typ),
	}
	return ret
}

func NewSegmentStats2(stats *BaseStats) *SegmentStats {
	ret := &SegmentStats{
		_stats: *stats,
	}
	return ret
}

const (
	SAMPLE_RATE float64 = 0.1
)

type DistinctStats struct {
	_log         *hll.Sketch
	_sampleCount atomic.Uint64
	_totalCount  atomic.Uint64
}

func NewDistinctStats() *DistinctStats {
	ret := &DistinctStats{
		_log: hll.New14(),
	}
	return ret
}

func (stats *DistinctStats) Update(
	vec *chunk.Vector,
	count int,
	sample bool) {
	if count == 0 {
		return
	}
	stats._totalCount.Add(uint64(count))
	if sample {
		mval := int(float64(max(STANDARD_VECTOR_SIZE, count)) * SAMPLE_RATE)
		count = min(mval, count)
	}
	stats._sampleCount.Add(uint64(count))
	result := chunk.NewFlatVector(
		common.HashType(),
		STANDARD_VECTOR_SIZE)
	//evaluate hash first
	chunk.HashTypeSwitch(vec, result, nil, count, false)
	hashes := chunk.GetSliceInPhyFormatFlat[uint64](result)
	for i := 0; i < count; i++ {
		stats._log.InsertHash(hashes[i])
	}
}

func (stats *DistinctStats) Count() uint64 {
	if stats._sampleCount.Load() == 0 ||
		stats._totalCount.Load() == 0 {
		return 0
	}
	cnt := stats._log.Estimate()
	u := float64(min(cnt, stats._sampleCount.Load()))
	s := float64(stats._sampleCount.Load())
	n := float64(stats._totalCount.Load())
	u1 := math.Pow(u/s, 2) * u
	est := u + u1/s*(n-s)
	return min(uint64(est), stats._totalCount.Load())

}

func (stats *DistinctStats) Copy() *DistinctStats {
	ret := &DistinctStats{
		_log: stats._log.Clone(),
	}
	ret._sampleCount.Store(stats._sampleCount.Load())
	ret._totalCount.Store(stats._totalCount.Load())

	return ret
}

func (stats *DistinctStats) Merge(other *DistinctStats) {
	_ = stats._log.Merge(other._log)
	stats._sampleCount.Add(other._sampleCount.Load())
	stats._totalCount.Add(other._totalCount.Load())
}

func (stats *DistinctStats) Serialize(serial util.Serialize) error {
	writer := NewFieldWriter(serial)
	err := stats.Serialize2(writer)
	if err != nil {
		return err
	}
	return writer.Finalize()
}

func (stats *DistinctStats) Serialize2(writer *FieldWriter) error {
	err := WriteField[uint64](stats._sampleCount.Load(), writer)
	if err != nil {
		return err
	}
	err = WriteField[uint64](stats._totalCount.Load(), writer)
	if err != nil {
		return err
	}

	//serial log
	logData, err := stats._log.MarshalBinary()
	if err != nil {
		return err
	}
	err = WriteField[uint32](uint32(len(logData)), writer)
	if err != nil {
		return err
	}
	//fmt.Println("distinct stats ",
	//	stats._sampleCount.Load(),
	//	stats._totalCount.Load(),
	//	len(logData),
	//)
	err = WriteBlob(logData, writer)
	if err != nil {
		return err
	}
	return err
}

func (stats *DistinctStats) Deserialize(deserial util.Deserialize) error {
	reader, err := NewFieldReader(deserial)
	if err != nil {
		return err
	}
	err = stats.Deserialize2(reader)
	if err != nil {
		return err
	}
	reader.Finalize()
	return err
}

func (stats *DistinctStats) Deserialize2(reader *FieldReader) error {
	var scount, tcount uint64
	err := ReadRequired[uint64](&scount, reader)
	if err != nil {
		return err
	}
	err = ReadRequired[uint64](&tcount, reader)
	if err != nil {
		return err
	}
	stats._sampleCount.Store(scount)
	stats._totalCount.Store(tcount)
	var logLen uint32
	err = ReadRequired[uint32](&logLen, reader)
	if err != nil {
		return err
	}
	logData := make([]byte, logLen)
	err = ReadBlob(logData, reader)
	if err != nil {
		return err
	}
	return stats._log.UnmarshalBinary(logData)
}

type ColumnStats struct {
	_stats         BaseStats
	_distinctStats *DistinctStats
}

func (stat *ColumnStats) UpdateDistinctStats(
	vec *chunk.Vector,
	count int) {
	if stat._distinctStats == nil {
		return
	}
	stat._distinctStats.Update(vec, count, true)
}

func (stat *ColumnStats) HasDistinctStats() bool {
	return stat._distinctStats != nil
}

func (stat *ColumnStats) Copy() *ColumnStats {
	ret := &ColumnStats{
		_stats: stat._stats.Copy(),
	}

	if stat._distinctStats != nil {
		ret._distinctStats = stat._distinctStats.Copy()
	}

	return ret
}

func (stat *ColumnStats) Merge(other *ColumnStats) {
	stat._stats.Merge(&other._stats)
	if stat._distinctStats != nil {
		stat._distinctStats.Merge(other._distinctStats)
	}
}

func (stat *ColumnStats) Serialize(serial util.Serialize) error {
	err := stat._stats.Serialize(serial)
	if err != nil {
		return err
	}
	return util.WriteOptional(
		func() bool {
			return stat._distinctStats != nil
		},
		func(serial util.Serialize) error {
			return stat._distinctStats.Serialize(serial)
		},
		serial,
	)
}

func (stat *ColumnStats) Deserialize(deserial util.Deserialize, lType common.LType) error {
	err := stat._stats.Deserialize(deserial, lType)
	if err != nil {
		return err
	}
	return util.ReadOptional(
		func(deserial util.Deserialize) error {
			return stat._distinctStats.Deserialize(deserial)
		},
		deserial,
	)
}

func (stat *ColumnStats) Count() uint64 {
	return stat._stats._totalCount
}

func (stat *ColumnStats) HasNull() bool {
	return stat._stats._hasNull
}

func (stat *ColumnStats) HasNoNull() bool {
	return stat._stats._hasNoNull
}

func (stat *ColumnStats) DistinctCount() uint64 {
	return stat._stats._distinctCount
}

func NewEmptyColumnStats(lType common.LType) *ColumnStats {
	ret := &ColumnStats{
		_stats:         NewEmptyBaseStats(lType),
		_distinctStats: NewDistinctStats(),
	}
	return ret
}

type TableStatsLock struct {
	_lock sync.Locker
}

type TableStats struct {
	_lock        sync.Mutex
	_columnStats []*ColumnStats
}

func (stats *TableStats) InitEmpty(types []common.LType) {
	stats._columnStats = nil
	for _, lType := range types {
		stats._columnStats = append(stats._columnStats,
			NewEmptyColumnStats(lType))
	}
}

func (stats *TableStats) GetStats(i int) *ColumnStats {
	return stats._columnStats[i]
}
func (stats *TableStats) GetStats2() []*ColumnStats {
	return stats._columnStats
}

func (stats *TableStats) CopyStats(idx int) *BaseStats {
	stats._lock.Lock()
	defer stats._lock.Unlock()
	result := stats._columnStats[idx]._stats.Copy()
	if stats._columnStats[idx].HasDistinctStats() {
		result._distinctCount = stats._columnStats[idx]._distinctStats.Count()
		result._totalCount = stats._columnStats[idx]._distinctStats._totalCount.Load()
	}
	return &result
}

// copy these stats to other
func (stats *TableStats) CopyStats2(other *TableStats) {
	for _, stat := range stats._columnStats {
		other._columnStats = append(other._columnStats,
			stat.Copy())
	}
}

func (stats *TableStats) MergeStats(
	other *TableStats) {
	stats._lock.Lock()
	defer stats._lock.Unlock()
	util.AssertFunc(len(stats._columnStats) == len(other._columnStats))
	for i := 0; i < len(stats._columnStats); i++ {
		stats._columnStats[i].Merge(other._columnStats[i])
	}
}

func (stats *TableStats) MergeStats2(
	id int,
	other *BaseStats) {
	stats._columnStats[id]._stats.Merge(other)
}

func (stats *TableStats) Serialize(serial util.Serialize) error {
	for _, stat := range stats._columnStats {
		err := stat.Serialize(serial)
		if err != nil {
			return err
		}
	}
	return nil
}

func (stats *TableStats) Deserialize(
	deserial util.Deserialize,
	defs []*ColumnDefinition) error {
	for _, def := range defs {
		colStats := NewEmptyColumnStats(def.Type)
		err := colStats.Deserialize(deserial, def.Type)
		if err != nil {
			return err
		}
		stats._columnStats = append(stats._columnStats, colStats)
	}
	return nil
}

func (stats *TableStats) Init(types []common.LType, data *PersistentTableData) {
	stats._columnStats = data._tableStats._columnStats
}

type StatsOp[T any] interface {
	Update(stats *BaseStats, newValue *T)
}

type BitStatsOp struct {
}

func (BitStatsOp) Update(stats *BaseStats, newValue *bool) {
	minVal := &stats._numericData._min._value._bool
	maxVal := &stats._numericData._max._value._bool
	//false < true
	if !*newValue && *minVal {
		*minVal = *newValue
	}
	//true > false
	if *newValue && *maxVal {
		*maxVal = *newValue
	}
}

type Int32StatsOp struct {
}

func (Int32StatsOp) Update(stats *BaseStats, newValue *int32) {
	minVal := &stats._numericData._min._value._int32
	maxVal := &stats._numericData._max._value._int32
	if *newValue < *minVal {
		*minVal = *newValue
	}
	if *newValue > *maxVal {
		*maxVal = *newValue
	}
}

type Int64StatsOp struct {
}

func (Int64StatsOp) Update(stats *BaseStats, newValue *int64) {
	minVal := &stats._numericData._min._value._int64
	maxVal := &stats._numericData._max._value._int64
	if *newValue < *minVal {
		*minVal = *newValue
	}
	if *newValue > *maxVal {
		*maxVal = *newValue
	}
}

type Uint64StatsOp struct {
}

func (Uint64StatsOp) Update(stats *BaseStats, newValue *uint64) {
	minVal := &stats._numericData._min._value._uint64
	maxVal := &stats._numericData._max._value._uint64
	if *newValue < *minVal {
		*minVal = *newValue
	}
	if *newValue > *maxVal {
		*maxVal = *newValue
	}
}

type DecimalStatsOp struct {
}

func (DecimalStatsOp) Update(stats *BaseStats, newValue *common.Decimal) {
	minVal := &stats._numericData._min._value._decimal
	maxVal := &stats._numericData._max._value._decimal
	if newValue.Decimal.Cmp(minVal.Decimal) < 0 {
		*minVal = *newValue
	}
	if newValue.Decimal.Cmp(maxVal.Decimal) > 0 {
		*maxVal = *newValue
	}
}

type DateStatsOp struct {
}

func (DateStatsOp) Update(stats *BaseStats, newValue *common.Date) {
	minVal := &stats._numericData._min._value._date
	maxVal := &stats._numericData._max._value._date
	if newValue.Less(minVal) {
		*minVal = *newValue
	}
	if maxVal.Less(newValue) {
		*maxVal = *newValue
	}
}

type StringStatsOp struct {
}

func (StringStatsOp) Update(stats *BaseStats, newValue *common.String) {
	data := newValue.DataSlice()
	dlen := newValue.Length()

	var target [MAX_STRING_MINMAX_SIZE]byte

	sz := min(int(MAX_STRING_MINMAX_SIZE), dlen)
	copy(target[:], data[:sz])
	for i := sz; i < int(MAX_STRING_MINMAX_SIZE); i++ {
		target[i] = 0
	}
	if bytes.Compare(target[:], stats._stringData._min[:]) < 0 {
		copy(stats._stringData._min[:], target[:])
	}
	if bytes.Compare(target[:], stats._stringData._max[:]) > 0 {
		copy(stats._stringData._max[:], target[:])
	}
	if uint32(dlen) >= stats._stringData._maxStringLength {
		stats._stringData._maxStringLength = uint32(dlen)
	}
}
