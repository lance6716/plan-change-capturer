package report

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRender(t *testing.T) {
	r := &Report{
		TaskInfoItems: [][2]string{
			{"key1", "value1"},
		},
		WorkloadInfoItems: [][2]string{
			{"key2", "value2"},
			{"key3", "value3"},
		},
		ExecutionInfoItems: [][2]string{
			{"key4", "value4"},
		},
		Summary: Summary{
			Overall: ChangeCount{
				SQL:  2,
				Plan: 1,
			},
			Unchanged: ChangeCount{
				SQL:  2,
				Plan: 1,
			},
		},
		TopSQLs: Table{
			Header: []string{"SQLDigest", "SumLatency"},
			Data: [][]string{
				{"digest1", "100"},
				{"digest2", "200"},
			},
		},
		Details: []Details{
			{
				Header: "header1",
				Labels: [][2]string{
					{"label1", "value1"},
					{"label2", "value2"},
				},
				Source: &Plan{
					Labels: [][2]string{
						{"source1", "value1"},
					},
					Text: "Sort_6\n└─Projection_8\n  └─HashAgg_18\n    └─IndexLookUp_19\n      ├─IndexRangeScan_16(Build)\n      └─HashAgg_10(Probe)\n        └─TableRowIDScan_17",
				},
				Target: &Plan{
					Text: "Sort_6\n└─Projection_8\n  └─HashAgg_14\n    └─TableReader_15\n      └─HashAgg_9\n        └─Selection_13\n          └─TableFullScan_12",
				},
			},
			{
				Header: "header2",
				Source: &Plan{
					Text: "Sort_6\n└─Projection_8\n  └─HashAgg_18\n    └─IndexLookUp_19\n      ├─IndexRangeScan_16(Build)\n      └─HashAgg_10(Probe)\n        └─TableRowIDScan_17",
				},
			},
		},
	}

	file, err := os.Create("/tmp/report.html")
	require.NoError(t, err)
	err = render(r, file)
	require.NoError(t, err)
}
