package util

import (
	"context"
	"database/sql"
	"net"
	"slices"
	"strconv"
	"strings"
	"sync"

	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser"
)

// EscapeIdentifier escapes an MySQL identifier.
// TODO(lance6716): not all callers use it. And we can't process special characters?
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
	addr := net.JoinHostPort(host, strconv.Itoa(port))
	cfg := mysql.NewConfig()
	cfg.User = user
	cfg.Passwd = password
	cfg.Addr = addr
	cfg.AllowNativePasswords = true
	cfg.ParseTime = true
	cfg.MaxAllowedPacket = -1
	cfg.Params = map[string]string{
		// relax SQL mode
		"sql_mode": "'IGNORE_SPACE,NO_AUTO_VALUE_ON_ZERO,ALLOW_INVALID_DATES,NO_ENGINE_SUBSTITUTION'",
	}

	c, err := mysql.NewConnector(cfg)
	if err != nil {
		return nil, errors.Annotatef(err, "connect to %s as %s", addr, user)
	}
	return sql.OpenDB(c), nil
}

// TODO(lance6716): retry

var ParserPool = sync.Pool{
	New: func() any {
		return parser.New()
	},
}

// ReadStrRowsByColumnName reads given columns from sql.Rows. If not all columns
// are found, allFound will be false, given sql.Rows will not be read. Caller
// need to close rows after it returns.
func ReadStrRowsByColumnName(
	rows *sql.Rows,
	columnNames []string,
) (fields [][]string, allFound bool, err error) {
	columnNameToIndex := make(map[string]int, len(columnNames))
	for i, name := range columnNames {
		columnNameToIndex[name] = i
	}

	columns, err := rows.Columns()
	if err != nil {
		return nil, false, errors.Annotatef(err, "failed to get columns (%v)", columnNames)
	}
	found := 0
	dest := make([]any, len(columns))
	oneRow := make([]string, len(columnNames))
	for i := range dest {
		if idx, ok := columnNameToIndex[columns[i]]; ok {
			dest[i] = &oneRow[idx]
			found++
		} else {
			dest[i] = new(any)
		}
	}

	if found != len(columnNames) {
		return nil, false, nil
	}

	fields = make([][]string, 0, 8)
	for rows.Next() {
		err = rows.Scan(dest...)
		if err != nil {
			return nil, false, errors.Annotatef(err, "failed to scan row to get columns (%v)", columnNames)
		}
		fields = append(fields, slices.Clone(oneRow))
	}
	if err = rows.Err(); err != nil {
		return nil, false, errors.Annotatef(err, "failed to get rows (%v)", columnNames)
	}
	return fields, true, nil
}

func IsMemOrSysTable(dbTable [2]string) bool {
	upper := strings.ToUpper(dbTable[0])
	return upper == "INFORMATION_SCHEMA" ||
		upper == "PERFORMANCE_SCHEMA" ||
		upper == "METRICS_SCHEMA" ||
		upper == "MYSQL" ||
		upper == "SYS"
}
func ReadCreateDatabase(
	ctx context.Context,
	db *sql.DB,
	dbName string,
) (string, error) {
	escapedDBName := EscapeIdentifier(dbName)
	query := "SHOW CREATE DATABASE " + escapedDBName
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return "", errors.Annotatef(err, "failed to execute query: %s", query)
	}
	defer rows.Close()

	create, allFound, err := ReadStrRowsByColumnName(rows, []string{"Create Database"})
	if err != nil {
		return "", errors.Trace(err)
	}
	if allFound {
		return create[0][0], nil
	}
	return "", errors.Errorf("failed to find create database statement for %s", escapedDBName)
}

// ReadCreateTableOrView reads the CREATE TABLE / VIEW statement from the database.
func ReadCreateTableOrView(
	ctx context.Context,
	db *sql.DB,
	dbName, tableOrViewName string,
) (string, error) {
	escapedDBName := EscapeIdentifier(dbName)
	escapedTable := EscapeIdentifier(tableOrViewName)
	query := "SHOW CREATE TABLE " + escapedDBName + "." + escapedTable
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return "", errors.Annotatef(err, "failed to execute query: %s", query)
	}
	defer rows.Close()

	create, allFound, err := ReadStrRowsByColumnName(rows, []string{"Create Table"})
	if err != nil {
		return "", errors.Trace(err)
	}
	if allFound {
		return create[0][0], nil
	}
	// TODO(lance6716): remove DEFINER?
	create, allFound, err = ReadStrRowsByColumnName(rows, []string{"Create View"})
	if err != nil {
		return "", errors.Trace(err)
	}
	if allFound {
		return create[0][0], nil
	}
	columnNames, err := rows.Columns()
	if err != nil {
		return "", errors.Annotatef(err, "failed to get columns for query: %s", query)
	}
	return "", errors.Errorf("failed to find create table or view statement for %s.%s, got columns %v", escapedDBName, escapedTable, columnNames)
}
