package pcc

import (
	"testing"

	"github.com/lance6716/plan-change-capturer/pkg/compare"
	"github.com/lance6716/plan-change-capturer/pkg/source"
	"github.com/stretchr/testify/require"
)

func TestTopNSumLatencyPlans(t *testing.T) {
	got := topNSumLatencyPlans(nil, 5)
	require.Nil(t, got)
	r5 := &compare.PlanCmpResult{OldVersionInfo: &source.StmtSummary{SumLatency: 5}}

	got = topNSumLatencyPlans([]*compare.PlanCmpResult{r5}, 5)
	require.Equal(t, []*compare.PlanCmpResult{r5}, got)

	r4 := &compare.PlanCmpResult{OldVersionInfo: &source.StmtSummary{SumLatency: 4}}
	r3 := &compare.PlanCmpResult{OldVersionInfo: &source.StmtSummary{SumLatency: 3}}
	r2 := &compare.PlanCmpResult{OldVersionInfo: &source.StmtSummary{SumLatency: 2}}
	r1 := &compare.PlanCmpResult{OldVersionInfo: &source.StmtSummary{SumLatency: 1}}

	got = topNSumLatencyPlans([]*compare.PlanCmpResult{r1, r3, r2, r5, r4}, 10)
	require.Equal(t, []*compare.PlanCmpResult{r5, r4, r3, r2, r1}, got)

	got = topNSumLatencyPlans([]*compare.PlanCmpResult{r1, r3, r2, r5, r4}, 3)
	require.Equal(t, []*compare.PlanCmpResult{r5, r4, r3}, got)
}
