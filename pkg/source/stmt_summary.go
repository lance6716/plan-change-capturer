package source

import (
	"context"
	"database/sql"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	"github.com/lance6716/plan-change-capturer/pkg/util"
	"github.com/pingcap/errors"
)

type stmtSummary struct {
	schema     sql.NullString
	sql        string
	tableNames []string
	planStr    string
}

func readStmtSummary(ctx context.Context, db *sql.DB) ([]stmtSummary, error) {
	// TODO(lance6716): filter on table/schema names, sql, sample user...
	// TODO(lance6716): pagination
	query := `SELECT SCHEMA_NAME, QUERY_SAMPLE_TEXT, TABLE_NAMES, PLAN
		FROM INFORMATION_SCHEMA.CLUSTER_STATEMENTS_SUMMARY_HISTORY
		WHERE EXEC_COUNT > 1`
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer rows.Close()

	var ret []stmtSummary
	for rows.Next() {
		var (
			s          stmtSummary
			tableNames sql.NullString
		)
		err = rows.Scan(&s.schema, &s.sql, &tableNames, &s.planStr)
		if err != nil {
			return nil, errors.Trace(err)
		}
		s.tableNames = strings.Split(tableNames.String, ",")
		ret = append(ret, s)
	}
	return ret, errors.Trace(rows.Err())
}

func exportTableStructure(
	ctx context.Context,
	db *sql.DB,
	schema, table string,
) (string, error) {
	query := "SHOW CREATE TABLE " + util.EscapeIdentifier(schema) + "." + util.EscapeIdentifier(table)
	row := db.QueryRowContext(ctx, query)

	var createTable string
	err := row.Scan(&table, &createTable)
	if err != nil {
		return "", errors.Trace(err)
	}
	return createTable, errors.Trace(row.Err())
}
