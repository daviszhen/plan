package compute

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExplainLogicalPlan_SimpleScan(t *testing.T) {
	plan := buildSimpleScanPlan()
	result, err := ExplainLogicalPlan(plan)
	require.NoError(t, err)
	assert.Contains(t, result, "Project (Id 3)")
	assert.Contains(t, result, "->  Filter (Id 2)")
	assert.Contains(t, result, "->  Scan on public.nation")
	assert.Contains(t, result, "outputs:")
	assert.Contains(t, result, "filters:")
}

func TestExplainLogicalPlan_Join(t *testing.T) {
	plan := buildJoinPlan()
	result, err := ExplainLogicalPlan(plan)
	require.NoError(t, err)
	assert.Contains(t, result, "Project (Id 4)")
	assert.Contains(t, result, "->  Join (Id 3)")
	assert.Contains(t, result, "type:inner")
	assert.Contains(t, result, "->  Scan on public.nation")
	assert.Contains(t, result, "->  Scan on public.region")
}
