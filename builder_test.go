package main

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBuildSelect(t *testing.T) {
	q2 := tpchQ2()
	builder := NewBuilder()
	err := builder.buildSelect(q2, builder.rootCtx, 0)
	assert.NoError(t, err)

	fmt.Println(builder.String())
	assert.Greater(t, builder.tag, 0)
	assert.NotNil(t, builder.whereExpr)

	lp, err := builder.CreatePlan(builder.rootCtx, nil)
	assert.NoError(t, err)
	assert.NotNil(t, lp)
	fmt.Println(lp.String())
}
