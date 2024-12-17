package compare

import (
	"testing"

	"github.com/lance6716/plan-change-capturer/pkg/plan"
	"github.com/stretchr/testify/require"
)

func TestCmpPlan(t *testing.T) {
	a := plan.MustNewOp("Projection_4")
	a.Children = []*plan.Op{plan.MustNewOp("TableReader_7")}
	a.Children[0].Children = []*plan.Op{plan.MustNewOp("Selection_6")}
	a.Children[0].Children[0].Children = []*plan.Op{plan.MustNewOp("TableRangeScan_5")}

	b := plan.MustNewOp("TableReader_11")
	b.Children = []*plan.Op{plan.MustNewOp("Projection_5")}
	b.Children[0].Children = []*plan.Op{plan.MustNewOp("Selection_10")}
	b.Children[0].Children[0].Children = []*plan.Op{plan.MustNewOp("TableRangeScan_9")}

	require.Equal(t, Same, CmpPlan(a, b))
}

func TestRemoveProj(t *testing.T) {
	// test projection at root
	input := plan.MustNewOp("Projection_4")
	input.Children = []*plan.Op{plan.MustNewOp("TableReader_7")}
	input.Children[0].Children = []*plan.Op{plan.MustNewOp("Selection_6")}
	input.Children[0].Children[0].Children = []*plan.Op{plan.MustNewOp("TableRangeScan_5")}

	expected := plan.MustNewOp("TableReader_7")
	expected.Children = []*plan.Op{plan.MustNewOp("Selection_6")}
	expected.Children[0].Children = []*plan.Op{plan.MustNewOp("TableRangeScan_5")}
	removeProj(input)

	require.Equal(t, expected, input)

	// test projection at middle
	input = plan.MustNewOp("TableReader_7")
	input.Children = []*plan.Op{plan.MustNewOp("Projection_4")}
	input.Children[0].Children = []*plan.Op{plan.MustNewOp("Selection_6")}
	input.Children[0].Children[0].Children = []*plan.Op{plan.MustNewOp("TableRangeScan_5")}

	expected = plan.MustNewOp("TableReader_7")
	expected.Children = []*plan.Op{plan.MustNewOp("Selection_6")}
	expected.Children[0].Children = []*plan.Op{plan.MustNewOp("TableRangeScan_5")}
	removeProj(input)

	require.Equal(t, expected, input)
}
