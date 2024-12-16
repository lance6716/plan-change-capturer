package parse

import (
	"strings"
	"testing"

	"github.com/lance6716/plan-change-capturer/pkg/plan"
	"github.com/stretchr/testify/require"
)

// parseBatchModeResult processes the result from `mysql ... --batch` command, to
// output a map of column name to rows.
func parseBatchModeResult(t *testing.T, result string) map[string][]string {
	lines := strings.Split(result, "\n")
	cols := strings.Split(lines[0], "\t")
	ret := make(map[string][]string, len(cols))

	for i := 1; i < len(lines); i++ {
		fields := strings.Split(lines[i], "\t")
		require.Equal(
			t, len(cols), len(fields),
			"column count mismatch at line %d\nfirst line: %s\nmismatch line: %s",
			i, lines[0], lines[i],
		)
		for j, f := range fields {
			ret[cols[j]] = append(ret[cols[j]], f)
		}
	}

	return ret
}

func TestExample(t *testing.T) {
	result := `id	estRows	task	access object	operator info
HashJoin_8	12487.50	root		inner join, equal:[eq(test.t1.a, test.t2.b)]
├─TableReader_15(Build)	9990.00	root		data:Selection_14
│ └─Selection_14	9990.00	cop[tikv]		not(isnull(test.t2.b))
│   └─TableFullScan_13	10000.00	cop[tikv]	table:t2	keep order:false, stats:pseudo
└─TableReader_12(Probe)	9990.00	root		data:Selection_11
  └─Selection_11	9990.00	cop[tikv]		not(isnull(test.t1.a))
    └─TableFullScan_10	10000.00	cop[tikv]	table:t1	keep order:false, stats:pseudo`
	resultMap := parseBatchModeResult(t, result)

	p, err := ParseSQLResultRow(resultMap["id"])
	require.NoError(t, err)

	expected := &plan.Op{
		FullName: "HashJoin_8",
		Children: []*plan.Op{
			{
				FullName: "TableReader_15(Build)",
				Children: []*plan.Op{
					{
						FullName: "Selection_14",
						Children: []*plan.Op{
							{
								FullName: "TableFullScan_13",
							},
						},
					},
				},
			},
			{
				FullName: "TableReader_12(Probe)",
				Children: []*plan.Op{
					{
						FullName: "Selection_11",
						Children: []*plan.Op{
							{
								FullName: "TableFullScan_10",
							},
						},
					},
				},
			},
		},
	}

	require.Equal(t, expected, p)
}
