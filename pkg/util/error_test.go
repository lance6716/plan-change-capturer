package util

import (
	"flag"
	"testing"

	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/stretchr/testify/require"
)

func TestWrapUnretryableError(t *testing.T) {
	require.False(t, IsUnretryableError(nil))
	require.False(t, IsUnretryableError(errors.New("123")))
	require.True(t, IsUnretryableError(WrapUnretryableError(errors.New("123"))))
	require.True(t, IsUnretryableError(WrapUnretryableError(WrapUnretryableError(errors.New("123")))))
	require.True(t, IsUnretryableError(WrapUnretryableError(errors.Annotate(errors.New("123"), "456"))))
	require.True(t, IsUnretryableError(errors.Annotate(WrapUnretryableError(errors.New("123")), "annotated")))
	require.True(t, IsUnretryableError(errors.Trace(WrapUnretryableError(errors.New("123")))))
	require.True(t, IsUnretryableError(errors.Trace(WrapUnretryableError(errors.Annotate(WrapUnretryableError(errors.New("123")), "annotated")))))
}

var (
	testEnable   = flag.Bool("enable", false, "enable test that requires a running TiDB")
	testHost     = flag.String("host", "127.0.0.1", "TiDB host")
	testPort     = flag.Int("port", 4000, "TiDB port")
	testUser     = flag.String("user", "root", "TiDB user")
	testPassword = flag.String("password", "", "TiDB password")
)

func TestCheckOldDBSQLErrorUnretryable(t *testing.T) {
	if !*testEnable {
		t.Skip("test disabled")
	}

	db, err := ConnectDB(*testHost, *testPort, *testUser, *testPassword)
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("qweqwe dsasdasd")
	require.Error(t, err)
	require.True(t, IsSQLErrorUnretryable(err.(*mysql.MySQLError)), "err: %v", err)

	_, err = db.Exec("DROP DATABASE IF EXISTS unit_test")
	require.NoError(t, err)

	_, err = db.Exec("USE unit_test")
	require.Error(t, err)
	require.True(t, IsSQLErrorUnretryable(err.(*mysql.MySQLError)), "err: %v", err)

	_, err = db.Exec("SHOW CREATE DATABASE unit_test")
	require.Error(t, err)
	require.True(t, IsSQLErrorUnretryable(err.(*mysql.MySQLError)), "err: %v", err)

	_, err = db.Exec("CREATE TABLE unit_test.t (a int)")
	require.Error(t, err)
	require.True(t, IsSQLErrorUnretryable(err.(*mysql.MySQLError)), "err: %v", err)

	_, err = db.Exec("CREATE DATABASE unit_test")
	require.NoError(t, err)

	_, err = db.Exec("CREATE TABLE unit_test.t LIKE unit_test.t2")
	require.Error(t, err)
	require.True(t, IsSQLErrorUnretryable(err.(*mysql.MySQLError)), "err: %v", err)

	_, err = db.Exec("CREATE VIEW unit_test.v FROM SELECT * FROM unit_test.t")
	require.Error(t, err)
	require.True(t, IsSQLErrorUnretryable(err.(*mysql.MySQLError)), "err: %v", err)

	_, err = db.Exec("SHOW CREATE TABLE unit_test.t")
	require.Error(t, err)
	require.True(t, IsSQLErrorUnretryable(err.(*mysql.MySQLError)), "err: %v", err)

	_, err = db.Exec("DROP DATABASE mysql")
	require.Error(t, err)
	require.False(t, IsSQLErrorUnretryable(err.(*mysql.MySQLError)), "err: %v", err)
}
