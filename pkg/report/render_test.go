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
	}

	file, err := os.Create("/tmp/report.html")
	require.NoError(t, err)
	err = render(r, file)
	require.NoError(t, err)
}
