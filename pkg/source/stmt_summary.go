package source

import (
	"context"
	"database/sql"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	"github.com/lance6716/plan-change-capturer/pkg/util"
	"github.com/pingcap/errors"
)

// StmtSummary is a golang representation of the record in
// INFORMATION_SCHEMA.CLUSTER_STATEMENTS_SUMMARY_HISTORY.
type StmtSummary struct {
	Schema     sql.NullString
	SQL        string
	TableNames [][2]string
	PlanStr    string
	SQLDigest  string
	PlanDigest string
}

func ReadStmtSummary(ctx context.Context, db *sql.DB) ([]StmtSummary, error) {
	// TODO(lance6716): filter on table/schema names, sql, sample user...
	// TODO(lance6716): pagination
	query := `
		SELECT 
    		SCHEMA_NAME, QUERY_SAMPLE_TEXT, TABLE_NAMES, PLAN, DIGEST, PLAN_DIGEST
		FROM INFORMATION_SCHEMA.CLUSTER_STATEMENTS_SUMMARY_HISTORY
		WHERE EXEC_COUNT > 1`
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer rows.Close()

	var ret []StmtSummary
	for rows.Next() {
		var (
			s          StmtSummary
			tableNames sql.NullString
		)
		err = rows.Scan(&s.Schema, &s.SQL, &tableNames, &s.PlanStr, &s.SQLDigest, &s.PlanDigest)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if len(tableNames.String) > 0 {
			tables := strings.Split(tableNames.String, ",")
			s.TableNames = make([][2]string, 0, len(tables))
			for _, table := range tables {
				dbAndTable := strings.Split(table, ".")
				if len(dbAndTable) != 2 {
					return nil, errors.Errorf("invalid table name, expected 2 fields after split on `.` : %s", table)
				}
				s.TableNames = append(s.TableNames, [2]string{dbAndTable[0], dbAndTable[1]})
			}
		}

		ret = append(ret, s)
	}
	return ret, errors.Trace(rows.Err())
}

func ReadTableStructure(
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
