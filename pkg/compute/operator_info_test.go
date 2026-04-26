package compute

import (
	"testing"

	"github.com/daviszhen/plan/pkg/common"
	"github.com/stretchr/testify/assert"
)

func TestLogicalOperator_Accessors(t *testing.T) {
	t.Run("LimitInfo", func(t *testing.T) {
		lo := &LogicalOperator{Typ: LOT_Limit, Info: &LimitOpInfo{Limit: intConst(10), Offset: intConst(5)}}
		li := lo.getLimitInfo()
		assert.NotNil(t, li)
		v, _ := li.Limit.ConstValue.GetInteger()
		assert.Equal(t, int64(10), v)
	})
	t.Run("OrderInfo", func(t *testing.T) {
		lo := &LogicalOperator{Typ: LOT_Order, Info: &OrderOpInfo{OrderBys: []*Expr{intConst(1)}}}
		assert.Len(t, lo.getOrderBys(), 1)
	})
	t.Run("JoinInfo", func(t *testing.T) {
		lo := &LogicalOperator{Typ: LOT_JOIN, Info: &JoinOpInfo{JoinTyp: LOT_JoinTypeInner, OnConds: []*Expr{intConst(1)}}}
		assert.Equal(t, LOT_JoinTypeInner, lo.getJoinTyp())
		assert.Len(t, lo.getOnConds(), 1)
	})
	t.Run("AggInfo", func(t *testing.T) {
		lo := &LogicalOperator{Typ: LOT_AggGroup, Info: &AggOpInfo{AggTag: 99, Aggs: []*Expr{intConst(1)}, GroupBys: []*Expr{intConst(2)}}}
		assert.Equal(t, uint64(99), lo.getAggTag())
		assert.Len(t, lo.getAggs(), 1)
		assert.Len(t, lo.getGroupBys(), 1)
	})
	t.Run("CreateSchemaInfo", func(t *testing.T) {
		lo := &LogicalOperator{Typ: LOT_CreateSchema, Info: &CreateSchemaOpInfo{Database: "mydb", IfNotExists: true}}
		info := lo.Info.(*CreateSchemaOpInfo)
		assert.Equal(t, "mydb", info.Database)
		assert.True(t, info.IfNotExists)
	})
	t.Run("InsertInfo", func(t *testing.T) {
		lo := &LogicalOperator{Typ: LOT_Insert, Info: &InsertOpInfo{TableIndex: 5, ExpectedTypes: []common.LType{common.IntegerType()}}}
		info := lo.Info.(*InsertOpInfo)
		assert.Equal(t, 5, info.TableIndex)
	})
	t.Run("ScanInfo", func(t *testing.T) {
		lo := &LogicalOperator{Typ: LOT_Scan, Info: &ScanOpInfo{Database: "public", Table: "t1", Columns: []string{"a", "b"}}}
		assert.Equal(t, "public", lo.getScanDatabase())
		assert.Equal(t, "t1", lo.getScanTable())
		assert.Len(t, lo.getScanColumns(), 2)
	})
}

func TestLogicalOperator_PushFilter(t *testing.T) {
	scan := &LogicalOperator{Typ: LOT_Scan, Index: 1}
	result := pushFilter(scan, intConst(1))
	assert.Equal(t, LOT_Filter, result.Typ)
	assert.Len(t, result.Filters, 1)
	result2 := pushFilter(result, intConst(2))
	assert.Equal(t, result, result2)
	assert.Len(t, result2.Filters, 2)
}

func TestLogicalOperator_SetOnConds(t *testing.T) {
	lo := &LogicalOperator{Typ: LOT_JOIN, Info: &JoinOpInfo{JoinTyp: LOT_JoinTypeInner}}
	lo.setOnConds([]*Expr{intConst(1)})
	assert.Len(t, lo.Info.(*JoinOpInfo).OnConds, 1)
}
