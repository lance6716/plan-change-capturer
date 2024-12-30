package pcc

import (
	"container/heap"
	"context"
	"database/sql"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/lance6716/plan-change-capturer/pkg/compare"
	"github.com/lance6716/plan-change-capturer/pkg/filemgr"
	"github.com/lance6716/plan-change-capturer/pkg/plan"
	"github.com/lance6716/plan-change-capturer/pkg/report"
	"github.com/lance6716/plan-change-capturer/pkg/schema"
	"github.com/lance6716/plan-change-capturer/pkg/source"
	"github.com/lance6716/plan-change-capturer/pkg/util"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/parser"
	"go.uber.org/zap"
)

// Run is the main entry function of the pcc logic.
func Run(ctx context.Context, cfg *Config) error {
	cfg.ensureDefaults()
	if err := initLogger(&cfg.Log); err != nil {
		util.Logger.Error("failed to initialize logger", zap.Error(err))
	}
	defer util.Logger.Sync()

	return run(ctx, cfg)
}

// initLogger initializes the logger for the process. The default logger writes
// to stdout. If error happens, the logger will not be changed.
func initLogger(loggerCfg *Log) error {
	if loggerCfg.Filename != "" {
		logger, _, err := log.InitLogger(&log.Config{
			Level: "info",
			File: log.FileLogConfig{
				Filename: loggerCfg.Filename,
			},
		})
		if err != nil {
			return errors.Annotatef(err, "failed to initialize logger with file %s", loggerCfg.Filename)
		}
		util.Logger = logger
	}
	return nil
}

func run(ctx context.Context, cfg *Config) error {
	util.Logger.Info("start to run pcc", zap.Any("config", cfg))
	start := time.Now().Format(time.RFC3339)
	oldDB, newDB, err := prepareDBConnections(ctx, cfg)
	if err != nil {
		return errors.Trace(err)
	}
	defer oldDB.Close()
	defer newDB.Close()

	mgr := filemgr.NewManager(cfg.WorkDir)
	syncer := schema.NewSyncer(newDB)

	oldCfg := &cfg.OldVersion

	summaries, err := source.ReadStmtSummary(ctx, oldDB)
	if err != nil {
		return errors.Trace(err)
	}

	for _, s := range summaries {
		err = mgr.WriteStmtSummary(s)
		if err != nil {
			return errors.Trace(err)
		}
	}

	allResults := make([]*compare.PlanCmpResult, 0, len(summaries))
	for _, s := range summaries {
		result := cmpPlan(ctx, s, oldDB, newDB, syncer, mgr, oldCfg)
		allResults = append(allResults, result)
	}

	waitRetry := make([]*compare.PlanCmpResult, 0, len(summaries))
	waitRetryExecCount := 0
	errResults := make([]*compare.PlanCmpResult, 0, len(summaries))
	errResultsExecCount := 0
	cmpSameResults := make([]*compare.PlanCmpResult, 0, len(summaries))
	cmpSamerResultsExecCount := 0
	cmpDiffResults := make([]*compare.PlanCmpResult, 0, len(summaries))
	cmpDiffResultsExecCount := 0
	for _, result := range allResults {
		s := result.OldVersionInfo
		switch result.Result {
		case compare.Unknown:
			if result.ErrMsg == "" {
				// retryable error
				waitRetry = append(waitRetry, result)
				waitRetryExecCount += s.ExecCount
				continue
			}
			errResults = append(errResults, result)
			errResultsExecCount += s.ExecCount
		case compare.Same:
			cmpSameResults = append(cmpSameResults, result)
			cmpSamerResultsExecCount += s.ExecCount
		case compare.Diff:
			cmpDiffResults = append(cmpDiffResults, result)
			cmpDiffResultsExecCount += s.ExecCount
		}

		err = mgr.WriteResult(result)
		if err != nil {
			return errors.Trace(err)
		}
	}

	host, err := os.Hostname()
	if err != nil {
		return errors.Trace(err)
	}
	lastUpdated := time.Now().Format(time.RFC3339)
	r := &report.Report{
		TaskInfoItems: [][2]string{
			{"Task Name", cfg.TaskName},
			{"Task Owner", os.Getenv("USER")},
			{"Task Host", host},
			{"Description", cfg.Description},
		},
		WorkloadInfoItems: [][2]string{
			{"Old Version TiDB Address", net.JoinHostPort(oldCfg.Host, strconv.Itoa(oldCfg.Port))},
			{"Old Version TiDB User", oldCfg.User},
			{"Capture Method", "Statement Summary"},
			{"Total SQL Statement Count", strconv.Itoa(len(summaries))},
		},
		ExecutionInfoItems: [][2]string{
			{"Started", start},
			{"Last Updated", lastUpdated},
			{"Global Time Limit", "UNLIMITED"},
			{"Per-SQL Time Limit", "UNUSED"},
			{"Status", "Completed"},
			{"Number of Unsupported SQLs", "0"},
			{"Number of Error", strconv.Itoa(len(errResults) + len(waitRetry))},
			{"Number of Successful", strconv.Itoa(len(cmpSameResults) + len(cmpDiffResults))},
		},
		Summary: report.Summary{
			Overall: report.ChangeCount{
				SQL:  waitRetryExecCount + errResultsExecCount + cmpSamerResultsExecCount + cmpDiffResultsExecCount,
				Plan: len(waitRetry) + len(errResults) + len(cmpSameResults) + len(cmpDiffResults),
			},
			Unchanged: report.ChangeCount{
				SQL:  cmpSamerResultsExecCount,
				Plan: len(cmpSameResults),
			},
			MayDegraded: report.ChangeCount{
				SQL:  cmpDiffResultsExecCount,
				Plan: len(cmpDiffResults),
			},
			Errors: report.ChangeCount{
				SQL:  errResultsExecCount + waitRetryExecCount,
				Plan: len(errResults) + len(waitRetry),
			},
		},
	}
	topSQLs := topNSumLatencyPlans(allResults, 500)
	r.TopSQLs = report.Table{
		Header: []string{"DIGEST", "DIGEST_TEXT", "Source AVG_LATENCY", "Source EXEC_COUNT", "Target AVG_LATENCY", "Target EXEC_COUNT", "Plan change"},
		Data:   make([][]string, 0, len(topSQLs)),
	}
	for _, result := range topSQLs {
		s := result.OldVersionInfo
		r.TopSQLs.Data = append(r.TopSQLs.Data, []string{
			s.SQLDigest,
			s.SQL,
			(s.SumLatency / time.Duration(s.ExecCount)).String(),
			strconv.Itoa(s.ExecCount),
			"",
			"",
			string(result.Result),
		})
	}
	r.Details = make([]report.Details, len(allResults))
	for i, result := range allResults {
		r.Details[i] = report.Details{
			Header: "SQL Digest: " + result.OldVersionInfo.SQLDigest + " Plan Digest: " + result.OldVersionInfo.PlanDigest,
			Labels: [][2]string{
				{"Schema Name", result.OldVersionInfo.Schema},
				{"SQL Text", result.OldVersionInfo.SQL},
				{"Source AVG_LATENCY", (result.OldVersionInfo.SumLatency / time.Duration(result.OldVersionInfo.ExecCount)).String()},
				{"Source EXEC_COUNT", strconv.Itoa(result.OldVersionInfo.ExecCount)},
				{"Plan Change", string(result.Result)},
			},
			Source: &report.Plan{
				Text: result.OldPlan,
			},
		}

		if result.NewDiffPlan != "" {
			r.Details[i].Target = &report.Plan{
				Text: result.NewDiffPlan,
			}
		}
	}

	return report.Render(r, filepath.Join(cfg.WorkDir, "report.html"))
}

// prepareDBConnections creates sql.DB to the old and new version databases. For
// the new version database, it also adjusts SQL variables for later usage.
// Caller should close the returned DBs if it returns nil error.
func prepareDBConnections(ctx context.Context, cfg *Config) (*sql.DB, *sql.DB, error) {
	oldCfg := &cfg.OldVersion
	oldDB, err := util.ConnectDB(oldCfg.Host, oldCfg.Port, oldCfg.User, oldCfg.Password)
	if err != nil {
		return nil, nil, errors.Annotatef(err,
			"connect to old version DB at %s",
			net.JoinHostPort(oldCfg.Host, strconv.Itoa(oldCfg.Port)),
		)
	}
	oldDB.SetMaxOpenConns(oldCfg.MaxConn)

	newCfg := &cfg.NewVersion
	newDB, err := util.ConnectDB(newCfg.Host, newCfg.Port, newCfg.User, newCfg.Password)
	if err != nil {
		oldDB.Close()
		return nil, nil, errors.Annotatef(err,
			"connect to new version DB at %s",
			net.JoinHostPort(newCfg.Host, strconv.Itoa(newCfg.Port)),
		)
	}
	// disable auto analyze for new version DB, to avoid stats change during the process
	_, err = newDB.ExecContext(ctx, "SET @@global.tidb_enable_auto_analyze='OFF'")
	if err != nil {
		return nil, nil, errors.Annotate(err, "disable auto analyze for new version DB")
	}
	newDB.SetMaxOpenConns(newCfg.MaxConn)

	return oldDB, newDB, nil
}

// cmpPlan returns the compare result of the plan. When it meets an error, it
// will check if the error is transient or not. If it is transient, it will
// return PlanCmpResult with compare.Unknown result and empty ErrMsg to expect
// the caller to retry at the next interval Otherwise, it will write the error to
// PlanCmpResult.ErrMsg and return it.
func cmpPlan(
	ctx context.Context,
	s *source.StmtSummary,
	oldDB *sql.DB,
	newDB *sql.DB,
	syncer *schema.Syncer,
	mgr *filemgr.Manager,
	oldCfg *TiDB,
) *compare.PlanCmpResult {
	ret := &compare.PlanCmpResult{
		Result:         compare.Unknown,
		OldVersionInfo: s,
	}

	err := syncForDB(ctx, oldDB, s.Schema, syncer, mgr)
	if err != nil {
		// TODO(lance6716): check everywhere, log error even if it is retryable
		util.Logger.Error("sync database failed", zap.Error(err))
		if util.IsUnretryableError(err) {
			ret.ErrMsg = err.Error()
		}
		return ret
	}

	for _, table := range s.TableNamesNeedToSync {
		err2 := syncForTable(ctx, oldDB, table, s.Schema, syncer, mgr, oldCfg)
		if err2 != nil {
			util.Logger.Error("sync table failed", zap.Error(err2))
			if util.IsUnretryableError(err) {
				ret.ErrMsg = err.Error()
			}
			return ret
		}
	}

	oldPlan, oldPlanStr, err2 := plan.NewPlanFromStmtSummaryPlan(s.PlanStr)
	if err2 != nil {
		ret.ErrMsg = err2.Error()
		return ret
	}
	ret.OldPlan = oldPlanStr
	newPlan, newPlanStr, err2 := plan.NewPlanFromQuery(ctx, newDB, s.Schema, s.SQL)
	if err2 != nil {
		util.Logger.Error("get new plan failed", zap.Error(err2))
		// TODO(lance6716): check retryable
		return ret
	}
	ret.NewDiffPlan = newPlanStr

	sql := s.SQL
	if s.HasParseError {
		sql = ""
	}
	reason, err2 := compare.CmpPlan(sql, oldPlan, newPlan)
	if err2 != nil {
		ret.ErrMsg = err2.Error()
		return ret
	}
	ret.Result = reason
	if reason == compare.Same {
		ret.NewDiffPlan = ""
	}

	util.Logger.Info("compare result",
		zap.String("reason", string(reason)),
		zap.String("sql", s.SQL),
	)

	return ret
}

func syncForDB(
	ctx context.Context,
	oldDB *sql.DB,
	dbName string,
	syncer *schema.Syncer,
	mgr *filemgr.Manager,
) error {
	if dbName == "" {
		return nil
	}
	if util.IsMemOrSysTable([2]string{dbName, ""}) {
		return nil
	}

	// TODO(lance6716): skip read structure if we already have it?
	createDatabase, err2 := util.ReadCreateDatabase(ctx, oldDB, dbName)
	if err2 != nil {
		if merr, ok := err2.(*mysql.MySQLError); ok && util.CheckOldDBSQLErrorUnretryable(merr) {
			err2 = util.WrapUnretryableError(err2)
		}
		return errors.Trace(err2)
	}
	err2 = mgr.WriteDatabaseStructure(dbName, createDatabase)
	if err2 != nil {
		return errors.Trace(err2)
	}

	err2 = syncer.CreateDatabase(ctx, dbName, createDatabase)
	if err2 != nil {
		return errors.Trace(err2)
	}

	return nil
}

// TODO(lance6716): test sync user memory table and sequence
func syncForTable(
	ctx context.Context,
	oldDB *sql.DB,
	table [2]string,
	currDB string,
	syncer *schema.Syncer,
	mgr *filemgr.Manager,
	oldCfg *TiDB,
) error {
	createDatabase, err2 := util.ReadCreateDatabase(ctx, oldDB, table[0])
	if err2 != nil {
		if merr, ok := err2.(*mysql.MySQLError); ok && util.CheckOldDBSQLErrorUnretryable(merr) {
			err2 = util.WrapUnretryableError(err2)
		}
		return errors.Trace(err2)
	}
	err2 = mgr.WriteDatabaseStructure(table[0], createDatabase)
	if err2 != nil {
		return errors.Trace(err2)
	}

	createTable, err2 := util.ReadCreateTableOrView(ctx, oldDB, table[0], table[1])
	if err2 != nil {
		return errors.Trace(err2)
	}
	err2 = mgr.WriteTableStructure(table[0], table[1], createTable)
	if err2 != nil {
		return errors.Trace(err2)
	}

	p := util.ParserPool.Get().(*parser.Parser)
	stmt, err := p.ParseOneStmt(createTable, "", "")
	util.ParserPool.Put(p)
	if err != nil {
		return errors.Annotatef(err, "parse create table statement for %s.%s", table[0], table[1])
	}
	tableNames := util.ExtractTableNames(stmt, table[0])
	for _, t := range tableNames {
		if t == table {
			continue
		}
		err = syncForTable(ctx, oldDB, t, currDB, syncer, mgr, oldCfg)
		if err != nil {
			return errors.Trace(err)
		}
	}

	tableStats, err2 := source.ReadTableStats(
		ctx,
		http.DefaultClient,
		net.JoinHostPort(oldCfg.Host, strconv.Itoa(oldCfg.StatusPort)),
		table[0],
		table[1],
	)
	if err2 != nil {
		return errors.Trace(err2)
	}
	err2 = mgr.WriteTableStats(table[0], table[1], tableStats)
	if err2 != nil {
		return errors.Trace(err2)
	}

	err2 = syncer.CreateDatabase(ctx, table[0], createDatabase)
	if err2 != nil {
		return errors.Trace(err2)
	}
	err2 = syncer.CreateTable(ctx, table[0], table[1], createTable)
	if err2 != nil {
		return errors.Trace(err2)
	}
	err2 = syncer.LoadStats(ctx, mgr.GetTableStatsPath(table[0], table[1]))
	if err2 != nil {
		return errors.Trace(err2)
	}

	return nil
}

type ResultHeap []*compare.PlanCmpResult

func (r ResultHeap) Len() int {
	return len(r)
}

func (r ResultHeap) Less(i, j int) bool {
	return r[i].OldVersionInfo.SumLatency < r[j].OldVersionInfo.SumLatency
}

func (r ResultHeap) Swap(i, j int) {
	r[i], r[j] = r[j], r[i]
}

func (r *ResultHeap) Push(x any) {
	*r = append(*r, x.(*compare.PlanCmpResult))
}

func (r *ResultHeap) Pop() any {
	old := *r
	n := len(old)
	x := old[n-1]
	*r = old[0 : n-1]
	return x
}

// topNSumLatencyPlans will not modify the input results, and return the sorted
// results with the top N sum latency.
func topNSumLatencyPlans(results []*compare.PlanCmpResult, n int) []*compare.PlanCmpResult {
	if len(results) <= n {
		ret := slices.Clone(results)
		slices.SortFunc(ret, func(i, j *compare.PlanCmpResult) int {
			return int(j.OldVersionInfo.SumLatency - i.OldVersionInfo.SumLatency)
		})
		return ret
	}

	// maintain a min heap to get N plan with largest SumLatency
	h := make([]*compare.PlanCmpResult, n)
	copy(h, results[:n])
	heap.Init((*ResultHeap)(&h))
	for _, r := range results[n:] {
		if r.OldVersionInfo.SumLatency > h[0].OldVersionInfo.SumLatency {
			h[0] = r
			heap.Fix((*ResultHeap)(&h), 0)
		}
	}

	slices.SortFunc(h, func(i, j *compare.PlanCmpResult) int {
		return int(j.OldVersionInfo.SumLatency - i.OldVersionInfo.SumLatency)
	})
	return h
}
