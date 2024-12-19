package compare

import (
	"testing"

	"github.com/lance6716/plan-change-capturer/pkg/plan"
	"github.com/stretchr/testify/require"
)

func TestCmpPlan(t *testing.T) {
	a := plan.NewOp4Test("Projection_4")
	a.Children = []*plan.Op{plan.NewOp4Test("TableReader_7")}
	a.Children[0].Children = []*plan.Op{plan.NewOp4Test("Selection_6")}
	a.Children[0].Children[0].Children = []*plan.Op{plan.NewOp4Test("TableRangeScan_5")}

	b := plan.NewOp4Test("TableReader_11")
	b.Children = []*plan.Op{plan.NewOp4Test("Projection_5")}
	b.Children[0].Children = []*plan.Op{plan.NewOp4Test("Selection_10")}
	b.Children[0].Children[0].Children = []*plan.Op{plan.NewOp4Test("TableRangeScan_9")}

	result, err := CmpPlan("SELECT c FROM t WHERE id > 1 AND c2 > 1", a, b)
	require.NoError(t, err)
	require.Equal(t, Same, result)
}

func TestRemoveProj(t *testing.T) {
	// test projection at root
	input := plan.NewOp4Test("Projection_4")
	input.Children = []*plan.Op{plan.NewOp4Test("TableReader_7")}
	input.Children[0].Children = []*plan.Op{plan.NewOp4Test("Selection_6")}
	input.Children[0].Children[0].Children = []*plan.Op{plan.NewOp4Test("TableRangeScan_5")}

	expected := plan.NewOp4Test("TableReader_7")
	expected.Children = []*plan.Op{plan.NewOp4Test("Selection_6")}
	expected.Children[0].Children = []*plan.Op{plan.NewOp4Test("TableRangeScan_5")}
	removeProj(input)

	require.Equal(t, expected, input)

	// test projection at middle
	input = plan.NewOp4Test("TableReader_7")
	input.Children = []*plan.Op{plan.NewOp4Test("Projection_4")}
	input.Children[0].Children = []*plan.Op{plan.NewOp4Test("Selection_6")}
	input.Children[0].Children[0].Children = []*plan.Op{plan.NewOp4Test("TableRangeScan_5")}

	expected = plan.NewOp4Test("TableReader_7")
	expected.Children = []*plan.Op{plan.NewOp4Test("Selection_6")}
	expected.Children[0].Children = []*plan.Op{plan.NewOp4Test("TableRangeScan_5")}
	removeProj(input)

	require.Equal(t, expected, input)
}

func TestNormalizeTableNameAlias(t *testing.T) {
	// this is a manually composed test, not sure if it will happen in real world
	a := plan.NewOp4Test("HashJoin_23")
	a.Children = []*plan.Op{
		plan.NewOp4Test("IndexReader_44(Build)"),
		plan.NewOp4Test("HashJoin_37(Probe)"),
	}
	a.Children[0].Children = []*plan.Op{plan.NewOp4Test("IndexFullScan_43|table:t, index:idx(c2)")}
	a.Children[1].Children = []*plan.Op{
		plan.NewOp4Test("TableReader_40(Build)"),
		plan.NewOp4Test("IndexReader_42(Probe)"),
	}
	a.Children[1].Children[0].Children = []*plan.Op{plan.NewOp4Test("Selection_39")}
	a.Children[1].Children[0].Children[0].Children = []*plan.Op{
		plan.NewOp4Test("TableFullScan_38|table:foo"),
	}
	a.Children[1].Children[1].Children = []*plan.Op{
		plan.NewOp4Test("IndexFullScan_41|table:t2, index:idx(c2)"),
	}

	b := a.Clone()
	b.Children[0].Children[0].AccessObject = &plan.AccessObject{Table: "t3", Index: "idx(c2)"}
	b.Children[1].Children[0].Children[0].Children[0].AccessObject = &plan.AccessObject{Table: "t1"}

	require.EqualValues(t, Diff, cmpPlan(a, b))

	sql := `SELECT t.c2 
		FROM t1 foo, t2, t3 t 
		WHERE foo.c1 = t2.c1 
		  	AND foo.c1 = t.c1 
		  	AND t.c2 < t2.c2`
	err := normalizeTableNameAlias(sql, a, b)
	require.NoError(t, err)
	require.EqualValues(t, Same, cmpPlan(a, b))
}
