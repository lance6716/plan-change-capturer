package util

import (
	"database/sql"
	"net"
	"strconv"
	"strings"

	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
)

// EscapeIdentifier escapes an MySQL identifier.
func EscapeIdentifier(s string) string {
	return "`" + strings.ReplaceAll(s, "`", "``") + "`"
}

// ConnectDB connects to a MySQL database.
func ConnectDB(
	host string,
	port int,
	user string,
	password string,
) (*sql.DB, error) {
	// TODO(lance6716): TLS and pool idle connections
	// TODO(lance6716): sql mode?
	c, err := mysql.NewConnector(&mysql.Config{
		User:                 user,
		Passwd:               password,
		Addr:                 net.JoinHostPort(host, strconv.Itoa(port)),
		AllowNativePasswords: true,
		Collation:            "utf8mb4_general_ci",
	})
	if err != nil {
		return nil, errors.Trace(err)
	}
	return sql.OpenDB(c), nil
}

// TODO(lance6716): retry
