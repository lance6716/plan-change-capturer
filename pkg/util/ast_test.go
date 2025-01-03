package util

import (
	"testing"

	"github.com/pingcap/tidb/pkg/parser"
	_ "github.com/pingcap/tidb/pkg/parser/test_driver"
	"github.com/stretchr/testify/require"
)

func TestExtractTableNames(t *testing.T) {
	currDB := "test"
	cases := []struct {
		sql      string
		expected [][2]string
	}{
		{
			sql:      "SELECT * FROM t",
			expected: [][2]string{{"test", "t"}},
		},
		{
			sql:      "CREATE TABLE t LIKE test2.t2",
			expected: [][2]string{{"test", "t"}, {"test2", "t2"}},
		},
		{
			sql:      "CREATE TABLE t AS SELECT * FROM test2.t2",
			expected: [][2]string{{"test", "t"}, {"test2", "t2"}},
		},
		{
			sql:      "CREATE VIEW v AS SELECT * FROM test2.t2",
			expected: [][2]string{{"test", "v"}, {"test2", "t2"}},
		},
		{
			sql:      "CREATE SEQUENCE s",
			expected: [][2]string{{"test", "s"}},
		},
		{
			sql:      "CREATE TABLE t(a int default next value for seq)",
			expected: [][2]string{{"test", "t"}, {"test", "seq"}},
		},
		{
			sql:      "SELECT SETVAL(seq2, 10)",
			expected: [][2]string{{"test", "seq2"}},
		},
		{
			sql:      "SELECT *, LASTVAL(seq) FROM t",
			expected: [][2]string{{"test", "seq"}, {"test", "t"}},
		},
	}

	p := parser.New()
	for _, ca := range cases {
		stmt, err := p.ParseOneStmt(ca.sql, "", "")
		require.NoError(t, err)
		require.Equal(t, ca.expected, ExtractTableNames(stmt, currDB), "sql: %s", ca.sql)
	}
}
