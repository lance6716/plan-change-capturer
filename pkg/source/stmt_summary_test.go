package source

import (
	"context"
	"database/sql"
	"flag"
	"strconv"
	"testing"

	"github.com/go-sql-driver/mysql"
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

	c, err := mysql.NewConnector(&mysql.Config{
		User:                 *testUser,
		Passwd:               *testPassword,
		Addr:                 *testHost + ":" + strconv.Itoa(*testPort),
		AllowNativePasswords: true,
		Collation:            "utf8mb4_general_ci",
	})
	require.NoError(t, err)
	db := sql.OpenDB(c)
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

	summaries, err := ReadStmtSummary(ctx, db)
	require.NoError(t, err)
	// at least we have executed above two queries which has same pattern
	require.Greater(t, len(summaries), 0)
}
