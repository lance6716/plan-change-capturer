package parse

import (
	"slices"
	"strings"
	"testing"

	"github.com/lance6716/plan-change-capturer/pkg/plan"
	"github.com/stretchr/testify/require"
)

// parseBatchModeResult processes the result from `mysql ... --batch` command, to
// output a map of column name to rows. It reruns a slice of [3]string, where
// the columns are [id, task, access object].
func parseBatchModeResult(t *testing.T, result string) [][3]string {
	lines := strings.Split(result, "\n")
	cols := strings.Split(lines[0], "\t")
	idColIdx := slices.Index(cols, "id")
	taskColIdx := slices.Index(cols, "task")
	accessObjColIdx := slices.Index(cols, "access object")
	ret := make([][3]string, 0, len(lines)-1)

	for i := 1; i < len(lines); i++ {
		fields := strings.Split(lines[i], "\t")
		require.Equal(
			t, len(cols), len(fields),
			"column count mismatch at line %d\nfirst line: %s\nmismatch line: %s",
			i, lines[0], lines[i],
		)
		ret = append(ret, [3]string{
			fields[idColIdx], fields[taskColIdx], fields[accessObjColIdx],
		})
	}

	return ret
}

func TestExample(t *testing.T) {
	sqlResult := `id	estRows	task	access object	operator info
HashJoin_23	15609.38	root		inner join, equal:[eq(test.t1.c1, test.t3.c1)], other cond:lt(test.t3.c2, test.t2.c2)
├─IndexReader_44(Build)	9990.00	root		index:IndexFullScan_43
│ └─IndexFullScan_43	9990.00	cop[tikv]	table:t, index:idx(c2)	keep order:false, stats:pseudo
└─HashJoin_37(Probe)	12487.50	root		inner join, equal:[eq(test.t1.c1, test.t2.c1)]
  ├─TableReader_40(Build)	9990.00	root		data:Selection_39
  │ └─Selection_39	9990.00	cop[tikv]		not(isnull(test.t1.c1))
  │   └─TableFullScan_38	10000.00	cop[tikv]	table:foo	keep order:false, stats:pseudo
  └─IndexReader_42(Probe)	9990.00	root		index:IndexFullScan_41
    └─IndexFullScan_41	9990.00	cop[tikv]	table:t2, index:idx(c2)	keep order:false, stats:pseudo`
	result := parseBatchModeResult(t, sqlResult)

	p, err := NewPlanFromSQLResultRow(result)
	require.NoError(t, err)

	expected := &plan.Op{
		Type: "HashJoin", ID: "23", Task: "root",
		Children: []*plan.Op{
			{
				Type: "IndexReader", ID: "44", Label: "(Build)", Task: "root",
				Children: []*plan.Op{
					{
						Type: "IndexFullScan", ID: "43", Task: "cop[tikv]",
						AccessObject: &plan.AccessObject{Table: "t", Index: "idx(c2)"},
					},
				},
			},
			{
				Type: "HashJoin", ID: "37", Label: "(Probe)", Task: "root",
				Children: []*plan.Op{
					{
						Type: "TableReader", ID: "40", Label: "(Build)", Task: "root",
						Children: []*plan.Op{
							{
								Type: "Selection", ID: "39", Task: "cop[tikv]",
								Children: []*plan.Op{
									{
										Type: "TableFullScan", ID: "38", Task: "cop[tikv]",
										AccessObject: &plan.AccessObject{Table: "foo"},
									},
								},
							},
						},
					},
					{
						Type: "IndexReader", ID: "42", Label: "(Probe)", Task: "root",
						Children: []*plan.Op{
							{
								Type: "IndexFullScan", ID: "41", Task: "cop[tikv]",
								AccessObject: &plan.AccessObject{Table: "t2", Index: "idx(c2)"},
							},
						},
					},
				},
			},
		},
	}

	require.Equal(t, expected, p)
}
