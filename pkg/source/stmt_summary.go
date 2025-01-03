package source

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/lance6716/plan-change-capturer/pkg/util"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser"
	"go.uber.org/zap"
)

// StmtSummary represents one record in
// INFORMATION_SCHEMA.CLUSTER_STATEMENTS_SUMMARY_HISTORY. The Instance +
// SummaryBeginTime + SQLDigest + PlanDigest fields are used as the ID of the
// StmtSummary.
type StmtSummary struct {
	// fields from the table
	Schema               string
	SQL                  string
	TableNamesNeedToSync [][2]string
	PlanStr              string
	SQLDigest            string
	PlanDigest           string
	ExecCount            int
	SumLatency           time.Duration
	Instance             string
	SummaryBeginTime     time.Time
	// computed fields
	HasParseError bool
}

// ReadStmtSummary reads the statement summary from the TiDB cluster. It emits
// the StmtSummary one by one into `outCh`, or return error. When work is
// completed, it will return nil. In any cases it will not close the channel.
//
// TODO(lance6716): can use statements_summary_evicted to calculate confidence
// TODO(lance6716): query CLUSTER_STATEMENTS_SUMMARY to get real time data
func ReadStmtSummary(
	ctx context.Context,
	db *sql.DB,
	outCh chan<- *StmtSummary,
) error {
	// TODO(lance6716): filter on table/schema names, sql, sample user...
	// TODO(lance6716): pagination
	// rely on the ast.GetStmtLabel function to filter out non-select statements
	query := `
		SELECT 
    		SCHEMA_NAME, 
    		QUERY_SAMPLE_TEXT, 
    		TABLE_NAMES, 
    		PLAN, 
    		DIGEST, 
    		PLAN_DIGEST,
    		EXEC_COUNT,
    		SUM_LATENCY,
    		INSTANCE,
    		SUMMARY_BEGIN_TIME
		FROM INFORMATION_SCHEMA.CLUSTER_STATEMENTS_SUMMARY_HISTORY
		WHERE EXEC_COUNT > 1 AND STMT_TYPE = 'Select'`
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return errors.Annotatef(err, "failed to execute query: %s", query)
	}
	defer rows.Close()

	p := util.ParserPool.Get().(*parser.Parser)
	defer util.ParserPool.Put(p)

	for rows.Next() {
		var (
			s                 StmtSummary
			tableNames        sql.NullString
			schema            sql.NullString
			sqlMayHasBrackets string
			sumLatencyNanoSec int64
		)

		err = rows.Scan(
			&schema,
			&sqlMayHasBrackets,
			&tableNames,
			&s.PlanStr,
			&s.SQLDigest,
			&s.PlanDigest,
			&s.ExecCount,
			&sumLatencyNanoSec,
			&s.Instance,
			&s.SummaryBeginTime,
		)
		if err != nil {
			return errors.Annotatef(err, "failed to scan row for query: %s", query)
		}
		if schema.Valid {
			s.Schema = schema.String
		}
		s.SQL = interpolateSQLMayHasBrackets(sqlMayHasBrackets, p)
		s.SumLatency = time.Duration(sumLatencyNanoSec)

		stmt, err2 := p.ParseOneStmt(s.SQL, "", "")
		if err2 != nil {
			s.HasParseError = true
		} else {
			s.TableNamesNeedToSync = util.ExtractTableNames(stmt, s.Schema)
		}

		failedToSplitDBTable := false
		if s.HasParseError && len(tableNames.String) > 0 {
			tables := strings.Split(tableNames.String, ",")
			s.TableNamesNeedToSync = make([][2]string, 0, len(tables))
			for _, table := range tables {
				dbAndTable := strings.Split(table, ".")
				if len(dbAndTable) != 2 {
					failedToSplitDBTable = true
					util.Logger.Error(
						"failed to split db and table, error may happen subsequently",
						zap.String("dbAndTable", table),
						zap.String("allTables", tableNames.String),
						zap.String("sqlDigest", s.SQLDigest),
						zap.String("planDigest", s.PlanDigest))
					continue
				}
				s.TableNamesNeedToSync = append(s.TableNamesNeedToSync, [2]string{dbAndTable[0], dbAndTable[1]})
			}
		}

		// filter synchronize system tables
		s.TableNamesNeedToSync = slices.DeleteFunc(s.TableNamesNeedToSync, util.IsMemOrSysTable)
		// skip simple SELECT without accessing any user table
		if len(s.TableNamesNeedToSync) == 0 && !failedToSplitDBTable {
			continue
		}
		outCh <- &s
	}
	return errors.Annotatef(rows.Err(), "failed to get rows for query: %s", query)
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

func ReadTableStats(
	ctx context.Context,
	client *http.Client,
	addr string,
	schema, table string,
) (string, error) {
	url := fmt.Sprintf("http://%s/stats/dump/%v/%v", addr, schema, table)
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return "", errors.Errorf("error when build HTTP request to URL (%s): %s", url, err)
	}
	resp, err := client.Do(req)
	if err != nil {
		return "", errors.Errorf("error when request URL (%s): %s", url, err)
	}
	if resp.StatusCode != 200 {
		return "", errors.Errorf("error when request URL (%s): HTTP status not 200, got %d", url, resp.StatusCode)
	}
	content, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", errors.Errorf("error when read response body from URL (%s): %s", url, err)
	}
	return string(content), nil
}
