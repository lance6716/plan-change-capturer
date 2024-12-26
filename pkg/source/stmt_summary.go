package source

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	"github.com/lance6716/plan-change-capturer/pkg/util"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser"
)

// StmtSummary represents one record in
// INFORMATION_SCHEMA.CLUSTER_STATEMENTS_SUMMARY_HISTORY.
type StmtSummary struct {
	// fields from the table
	Schema               string
	SQL                  string
	TableNamesNeedToSync [][2]string
	PlanStr              string
	SQLDigest            string
	PlanDigest           string

	// computed fields
	HasParseError bool
}

// TODO(lance6716): can use statements_summary_evicted to calculate confidence
// TODO(lance6716): query CLUSTER_STATEMENTS_SUMMARY to get real time data

func ReadStmtSummary(ctx context.Context, db *sql.DB) ([]StmtSummary, error) {
	// TODO(lance6716): filter on table/schema names, sql, sample user...
	// TODO(lance6716): pagination
	// rely on the ast.GetStmtLabel function to filter out non-select statements
	query := `
		SELECT 
    		SCHEMA_NAME, QUERY_SAMPLE_TEXT, TABLE_NAMES, PLAN, DIGEST, PLAN_DIGEST
		FROM INFORMATION_SCHEMA.CLUSTER_STATEMENTS_SUMMARY_HISTORY
		WHERE EXEC_COUNT > 1 AND STMT_TYPE = 'Select'`
	rows, err := db.QueryContext(ctx, query)
	// TODO(lance6716): use errors.Annotate
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer rows.Close()

	p := util.ParserPool.Get().(*parser.Parser)
	defer util.ParserPool.Put(p)

	var ret []StmtSummary
	for rows.Next() {
		var (
			s                 StmtSummary
			tableNames        sql.NullString
			schema            sql.NullString
			sqlMayHasBrackets string
		)

		err = rows.Scan(&schema, &sqlMayHasBrackets, &tableNames, &s.PlanStr, &s.SQLDigest, &s.PlanDigest)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if schema.Valid {
			s.Schema = schema.String
		}
		s.SQL = interpolateSQLMayHasBrackets(sqlMayHasBrackets, p)

		stmt, err2 := p.ParseOneStmt(s.SQL, "", "")
		if err2 != nil {
			s.HasParseError = true
		} else {
			s.TableNamesNeedToSync = util.ExtractTableNames(stmt, s.Schema)
			// skip simple SELECT without accessing any table
			if len(s.TableNamesNeedToSync) == 0 {
				continue
			}
		}

		if s.HasParseError && len(tableNames.String) > 0 {
			tables := strings.Split(tableNames.String, ",")
			s.TableNamesNeedToSync = make([][2]string, 0, len(tables))
			for _, table := range tables {
				dbAndTable := strings.Split(table, ".")
				if len(dbAndTable) != 2 {
					return nil, errors.Errorf("invalid table name, expected 2 fields after split on `.` : %s", table)
				}
				s.TableNamesNeedToSync = append(s.TableNamesNeedToSync, [2]string{dbAndTable[0], dbAndTable[1]})
			}
		}

		// filter synchronize system tables
		s.TableNamesNeedToSync = slices.DeleteFunc(s.TableNamesNeedToSync, util.IsMemOrSysTable)
		ret = append(ret, s)
	}
	return ret, errors.Trace(rows.Err())
}

// interpolateSQLMayHasBrackets processed the SQL returned by TiDB like `SELECT
// ... [arguments: (6249305, 6249404)]` to a valid SQL statement.
func interpolateSQLMayHasBrackets(sqlMayHasBrackets string, p *parser.Parser) string {
	_, err := p.ParseOneStmt(sqlMayHasBrackets, "", "")
	if err == nil {
		return sqlMayHasBrackets
	}

	// see (*PlanCacheParamList).String()
	const argumentsPrefix = " [arguments: "
	index := strings.Index(sqlMayHasBrackets, argumentsPrefix)
	if index == -1 {
		return sqlMayHasBrackets
	}
	lastIndex := strings.LastIndex(sqlMayHasBrackets, "]")
	sql := sqlMayHasBrackets[:index]
	argsStr := sqlMayHasBrackets[index+len(argumentsPrefix) : lastIndex]
	if argsStr[0] == '(' {
		// remove the first and last bracket
		argsStr = argsStr[1 : len(argsStr)-1]
	}
	// TODO(lance6716): check different argument types can be handled correctly.
	args := strings.Split(argsStr, ", ")
	for _, arg := range args {
		sql = strings.Replace(sql, "?", arg, 1)
	}
	return sql
}
func ReadCreateDatabase(
	ctx context.Context,
	db *sql.DB,
	dbName string,
) (string, error) {
	escapedDBName := util.EscapeIdentifier(dbName)
	query := "SHOW CREATE DATABASE " + escapedDBName
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return "", errors.Annotatef(err, "failed to execute query: %s", query)
	}
	defer rows.Close()

	create, allFound, err := util.ReadStrRowsByColumnName(rows, []string{"Create Database"})
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
	escapedDBName := util.EscapeIdentifier(dbName)
	escapedTable := util.EscapeIdentifier(tableOrViewName)
	query := "SHOW CREATE TABLE " + escapedDBName + "." + escapedTable
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return "", errors.Annotatef(err, "failed to execute query: %s", query)
	}
	defer rows.Close()

	create, allFound, err := util.ReadStrRowsByColumnName(rows, []string{"Create Table"})
	if err != nil {
		return "", errors.Trace(err)
	}
	if allFound {
		return create[0][0], nil
	}
	// TODO(lance6716): remove DEFINER?
	create, allFound, err = util.ReadStrRowsByColumnName(rows, []string{"Create View"})
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

func ReadTableStats(
	ctx context.Context,
	client *http.Client,
	addr string,
	schema, table string,
) (string, error) {
	url := fmt.Sprintf("http://%s/stats/dump/%v/%v", addr, schema, table)
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return "", fmt.Errorf("error when build HTTP request to URL (%s): %s", url, err)
	}
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("error when request URL (%s): %s", url, err)
	}
	if resp.StatusCode != 200 {
		return "", fmt.Errorf("error when request URL (%s): HTTP status not 200, got %d", url, resp.StatusCode)
	}
	content, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("error when read response body from URL (%s): %s", url, err)
	}
	return string(content), nil
}
