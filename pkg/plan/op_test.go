package plan

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAccessObject(t *testing.T) {
	cases := []struct {
		str      string
		expected *AccessObject
	}{
		{
			str:      "table:t1",
			expected: &AccessObject{Table: "t1"},
		},
		{
			str:      "table:t1, partition:p0,p1,p2",
			expected: &AccessObject{Table: "t1", Partitions: []string{"p0", "p1", "p2"}},
		},
		{
			str:      "table:t4, index:idx(a, b)",
			expected: &AccessObject{Table: "t4", Index: "idx(a, b)"},
		},
	}

	for _, c := range cases {
		got, err := parseAccessObject(c.str)
		require.NoError(t, err)
		require.Equal(t, c.expected, got)
	}
}
