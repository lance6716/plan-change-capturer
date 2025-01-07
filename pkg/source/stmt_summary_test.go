package source

import (
	"context"
	"database/sql"
	"flag"
	"testing"

	"github.com/lance6716/plan-change-capturer/pkg/util"
	"github.com/pingcap/tidb/pkg/parser"
	_ "github.com/pingcap/tidb/pkg/parser/test_driver"
	"github.com/stretchr/testify/require"
)

var (
	testEnable   = flag.Bool("enable", false, "enable test that requires a running TiDB")
	testHost     = flag.String("host", "127.0.0.1", "TiDB host")
	testPort     = flag.Int("port", 4000, "TiDB port")
	testUser     = flag.String("user", "root", "TiDB user")
	testPassword = flag.String("password", "", "TiDB password")
)

func mustExec(t *testing.T, conn *sql.Conn, query string) {
	_, err := conn.ExecContext(context.Background(), query)
	require.NoError(t, err)
}

func TestReadStmtSummary(t *testing.T) {
	flag.Parse()
	if !*testEnable {
		t.Skip("test disabled")
	}

	db, err := util.ConnectDB(*testHost, *testPort, *testUser, *testPassword)
	require.NoError(t, err)
	defer db.Close()

	ctx := context.Background()
	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	defer conn.Close()
	mustExec(t, conn, "USE test")
	mustExec(t, conn, "DROP TABLE IF EXISTS test_read_stmt_summary")
	mustExec(t, conn, "CREATE TABLE test_read_stmt_summary (a int key, b int, c int, index idx(b))")
	mustExec(t, conn, "INSERT INTO test_read_stmt_summary VALUES (1, 1, 1), (2, 2, 2)")
	mustExec(t, conn, "SELECT a FROM test_read_stmt_summary WHERE b = 1 AND c = 1")
	mustExec(t, conn, "SELECT a FROM test_read_stmt_summary WHERE b = 2 AND c = 2")

	outCh := make(chan *StmtSummary, 16)
	err = ReadStmtSummary(ctx, db, outCh)
	require.NoError(t, err)
	// at least we have executed above two queries which has same pattern
	require.Greater(t, len(outCh), 0)
}

func TestInterpolateSQLMayHasBrackets(t *testing.T) {
	cases := []struct {
		sql      string
		expected string
	}{
		{
			sql:      "SELECT * FROM t WHERE a = 1",
			expected: "SELECT * FROM t WHERE a = 1",
		},
		{
			sql:      "SELECT * FROM t WHERE a = ' [arguments: (1, 2)]'",
			expected: "SELECT * FROM t WHERE a = ' [arguments: (1, 2)]'",
		},
		{
			sql:      "SELECT DISTINCT c FROM sbtest2 WHERE id BETWEEN ? AND ? ORDER BY c [arguments: (6249305, 6249404)]",
			expected: "SELECT DISTINCT c FROM sbtest2 WHERE id BETWEEN 6249305 AND 6249404 ORDER BY c",
		},
		{
			sql:      "SELECT w_street_1, w_street_2, w_city, w_state, w_zip, w_name FROM warehouse WHERE w_id = ? [arguments: 270]",
			expected: "SELECT w_street_1, w_street_2, w_city, w_state, w_zip, w_name FROM warehouse WHERE w_id = 270",
		},
	}

	p := parser.New()
	for _, c := range cases {
		require.Equal(t, c.expected, interpolateSQLMayHasBrackets(c.sql, p))
	}
}

func TestFillFromSQLRecordedSkip(t *testing.T) {
	p := parser.New()
	shouldSkipCases := []string{
		"aaaaa(len:20)",
		"INSERT INTO test.t VALUES (1, 1, 1), (2, 2, 2)",
		"UPDATE test.t SET a = 1",
		"DELETE FROM test.t",
		"INSERT INTO test.t (ts) SELECT NOW()",
		"insert into `small` ( `create_date` , `desc` ) values ( ... )",
	}

	for _, c := range shouldSkipCases {
		s := &StmtSummary{}
		require.True(t, fillFromSQLRecorded(c, s, p))
	}

	shouldNotSkipCases := []string{
		"SELECT * FROM t WHERE a = 1",
		"INSERT INTO test.t SELECT * FROM t",
		"UPDATE t SET a = 1 WHERE b = 1",
		"DELETE FROM t WHERE b = 1",
	}

	for _, c := range shouldNotSkipCases {
		s := &StmtSummary{}
		require.False(t, fillFromSQLRecorded(c, s, p))
	}
}

func TestBindingDigest(t *testing.T) {
	p := parser.New()
	s := &StmtSummary{
		Schema:        "test",
		PlanInBinding: true,
	}
	require.False(t, fillFromSQLRecorded("select * from t1", s, p))
	expected := "4ea0618129ffc6a7effbc0eff4bbcb41a7f5d4c53a6fa0b2e9be81c7010915b0"
	require.Equal(t, expected, s.BindingDigest)

	require.False(t, fillFromSQLRecorded("SELect * FRom t1", s, p))
	require.Equal(t, expected, s.BindingDigest)

	require.False(t, fillFromSQLRecorded("SELect * FRom T1", s, p))
	require.Equal(t, expected, s.BindingDigest)
}
