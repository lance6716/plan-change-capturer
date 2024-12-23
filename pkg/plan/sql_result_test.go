package plan

import (
	"slices"
	"strings"
	"testing"

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

	p, err := newPlanFromSQLResultRow(result)
	require.NoError(t, err)

	expected := &Op{
		Type: "HashJoin", ID: "23", Task: "root",
		Children: []*Op{
			{
				Type: "IndexReader", ID: "44", Label: "(Build)", Task: "root",
				Children: []*Op{
					{
						Type: "IndexFullScan", ID: "43", Task: "cop[tikv]",
						AccessObject: &AccessObject{Table: "t", Index: "idx(c2)"},
					},
				},
			},
			{
				Type: "HashJoin", ID: "37", Label: "(Probe)", Task: "root",
				Children: []*Op{
					{
						Type: "TableReader", ID: "40", Label: "(Build)", Task: "root",
						Children: []*Op{
							{
								Type: "Selection", ID: "39", Task: "cop[tikv]",
								Children: []*Op{
									{
										Type: "TableFullScan", ID: "38", Task: "cop[tikv]",
										AccessObject: &AccessObject{Table: "foo"},
									},
								},
							},
						},
					},
					{
						Type: "IndexReader", ID: "42", Label: "(Probe)", Task: "root",
						Children: []*Op{
							{
								Type: "IndexFullScan", ID: "41", Task: "cop[tikv]",
								AccessObject: &AccessObject{Table: "t2", Index: "idx(c2)"},
							},
						},
					},
				},
			},
		},
	}

	require.Equal(t, expected, p)
}

func TestFromStmtSummaryPlan(t *testing.T) {
	// This is a real example from plan-change-capturer/stmt-summary file.
	input := "\tid                  \ttask     \testRows\toperator info                                                                                                                                                                                                                                                                                                                                                                                             \tactRows\texecution info                                                                                                                                                            \tmemory \tdisk\n\tProjection_4        \troot     \t3333.33\tinformation_schema.cluster_statements_summary_history.schema_name, information_schema.cluster_statements_summary_history.query_sample_text, information_schema.cluster_statements_summary_history.table_names, information_schema.cluster_statements_summary_history.plan, information_schema.cluster_statements_summary_history.digest, information_schema.cluster_statements_summary_history.plan_digest\t4      \ttime:1.38ms, loops:2, Concurrency:5                                                                                                                                       \t47.1 KB\tN/A\n\t└─TableReader_7     \troot     \t3333.33\tdata:Selection_6                                                                                                                                                                                                                                                                                                                                                                                          \t4      \ttime:1.32ms, loops:2, cop_task: {num: 1, max: 1.27ms, proc_keys: 0, copr_cache_hit_ratio: 0.00, max_distsql_concurrency: 1}, rpc_info:{Cop:{num_rpc:1, total_time:1.25ms}}\t9.07 KB\tN/A\n\t  └─Selection_6     \tcop[tidb]\t3333.33\tgt(information_schema.cluster_statements_summary_history.exec_count, 1)                                                                                                                                                                                                                                                                                                                                   \t0      \t                                                                                                                                                                          \tN/A    \tN/A\n\t    └─MemTableScan_5\tcop[tidb]\t10000  \ttable:CLUSTER_STATEMENTS_SUMMARY_HISTORY,                                                                                                                                                                                                                                                                                                                                                                 \t0      \t                                                                                                                                                                          \tN/A    \tN/A"
	op, err := NewPlanFromStmtSummaryPlan(input)
	require.NoError(t, err)
	expected := &Op{
		Type: "Projection", ID: "4", Task: "root",
		Children: []*Op{
			{
				Type: "TableReader", ID: "7", Task: "root",
				Children: []*Op{
					{
						Type: "Selection", ID: "6", Task: "cop[tidb]",
						Children: []*Op{
							{
								Type: "MemTableScan", ID: "5", Task: "cop[tidb]",
								AccessObject: &AccessObject{Table: "CLUSTER_STATEMENTS_SUMMARY_HISTORY"},
							},
						},
					},
				},
			},
		},
	}

	require.Equal(t, expected, op)
}
