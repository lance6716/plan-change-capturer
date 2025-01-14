package pcc

import (
	"container/heap"
	"context"
	"database/sql"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"time"

	"github.com/lance6716/plan-change-capturer/pkg/compare"
	"github.com/lance6716/plan-change-capturer/pkg/filemgr"
	"github.com/lance6716/plan-change-capturer/pkg/plan"
	"github.com/lance6716/plan-change-capturer/pkg/report"
	"github.com/lance6716/plan-change-capturer/pkg/schema"
	"github.com/lance6716/plan-change-capturer/pkg/source"
	"github.com/lance6716/plan-change-capturer/pkg/util"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

// Run is the main entry function of the pcc logic.
func Run(ctx context.Context, cfg *Config) error {
	cfg.ensureDefaults()
	if err := initLogger(&cfg.Log); err != nil {
		util.Logger.Error("failed to initialize logger", zap.Error(err))
	}
	defer util.Logger.Sync()

	return errors.Trace(run(ctx, cfg))
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
	start := time.Now()
	oldDB, newDB, err := prepareDBConnections(ctx, cfg)
	if err != nil {
		return errors.Trace(err)
	}
	defer oldDB.Close()
	defer newDB.Close()

	mgr := filemgr.NewManager(cfg.WorkDir)
	syncer := schema.NewSyncer(newDB)

	oldCfg := &cfg.OldVersion
	maxConn := max(cfg.OldVersion.MaxConn, cfg.NewVersion.MaxConn)
	eg, egCtx := errgroup.WithContext(ctx)

	var allBindings map[string]source.Binding
	readBindingDone := make(chan struct{})
	eg.Go(func() error {
		var err2 error
		allBindings, err2 = source.ReadBinding(egCtx, oldDB)
		if err2 != nil {
			return errors.Trace(err2)
		}
		close(readBindingDone)
		return nil
	})

	summFromSourceCh := make(chan *source.StmtSummary, maxConn)
	eg.Go(func() error {
		err2 := source.ReadStmtSummary(egCtx, oldDB, summFromSourceCh)
		if err2 != nil {
			return errors.Trace(err2)
		}
		close(summFromSourceCh)
		return nil
	})

	summAfterPersistCh := make(chan *source.StmtSummary, maxConn)
	eg.Go(func() error {
		// wait binding is loaded
		select {
		case <-readBindingDone:
		case <-egCtx.Done():
			return nil
		}

		for {
			select {
			case s, ok := <-summFromSourceCh:
				if !ok {
					close(summAfterPersistCh)
					return nil
				}
				if s.PlanInBinding {
					b, ok2 := allBindings[s.BindingDigest]
					if !ok2 {
						util.Logger.Warn("binding not found", zap.String("sql_digest", s.SQLDigest))
					} else {
						s.Binding = b
					}
				}
				err2 := mgr.WriteStmtSummary(s)
				if err2 != nil {
					return errors.Trace(err2)
				}
				summAfterPersistCh <- s
			case <-egCtx.Done():
				return nil
			}
		}
	})

	// TODO(lance6716): aggregate the summaries with the same digest but different
	// instance or capture window

	// TODO(lance6716): consumer should be fast enough to avoid blocking the
	// connection and causes connection timeout
	resultCh := make(chan *compare.PlanCmpResult, maxConn)
	cmpWorkerConn := max(maxConn, runtime.NumCPU())
	cmpWorkerCnt := atomic.NewInt64(int64(cmpWorkerConn))
	for range cmpWorkerConn {
		eg.Go(func() error {
			// wait until binding is synced
			select {
			case <-readBindingDone:
			case <-egCtx.Done():
				return nil
			}

			for {
				select {
				case s, ok := <-summAfterPersistCh:
					if !ok {
						if cmpWorkerCnt.Dec() == 0 {
							close(resultCh)
						}
						return nil
					}
					resultCh <- cmpPlan(ctx, s, oldDB, newDB, syncer, mgr, oldCfg)
				case <-egCtx.Done():
					return nil
				}
			}
		})
	}

	allResults := make([]*compare.PlanCmpResult, 0, 128)
	eg.Go(func() error {
		for {
			select {
			case result, ok := <-resultCh:
				if !ok {
					return nil
				}
				allResults = append(allResults, result)
			case <-egCtx.Done():
				return nil
			}
		}
	})

	metaResult := &metadataResult{
		startTime: start,
	}
	sourceInfo, err := util.ReadClusterInfo(egCtx, oldDB)
	if err != nil {
		util.Logger.Error("read source cluster info failed", zap.Error(err))
	} else {
		metaResult.sourceInfo = sourceInfo
	}
	targetInfo, err := util.ReadClusterInfo(egCtx, newDB)
	if err != nil {
		util.Logger.Error("read target cluster info failed", zap.Error(err))
	} else {
		metaResult.targetInfo = targetInfo
	}

	if err = eg.Wait(); err != nil {
		return errors.Trace(err)
	}

	r, err := processResults(allResults, cfg, mgr, metaResult)
	if err != nil {
		return errors.Trace(err)
	}
	return errors.Trace(report.Render(r, filepath.Join(cfg.WorkDir, "report.html")))
}

// prepareDBConnections creates sql.DB to the old and new version databases. For
// the new version database, it also adjusts SQL variables for later usage.
// Caller should close the returned DBs if it returns nil error.
func prepareDBConnections(ctx context.Context, cfg *Config) (*sql.DB, *sql.DB, error) {
	oldCfg := &cfg.OldVersion
	oldDB, err := util.ConnectDB(oldCfg.Host, oldCfg.Port, oldCfg.User, oldCfg.Password)
	if err != nil {
		return nil, nil, err
	}
	oldDB.SetMaxOpenConns(oldCfg.MaxConn)

	newCfg := &cfg.NewVersion
	newDB, err := util.ConnectDB(newCfg.Host, newCfg.Port, newCfg.User, newCfg.Password)
	if err != nil {
		oldDB.Close()
		return nil, nil, err
	}
	// disable auto analyze for new version DB, to avoid stats change during the process
	_, err = newDB.ExecContext(ctx, "SET @@global.tidb_enable_auto_analyze='OFF'")
	if err != nil {
		return nil, nil, errors.Annotate(err, "when disable auto analyze for new version DB")
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

	oldPlan, oldPlanStr, err2 := plan.NewPlanFromStmtSummaryPlan(s.PlanStr)
	if err2 != nil {
		// this error is not related to network, so it must be non-retryable
		ret.ErrMsg = err2.Error()
		return ret
	}
	ret.OldPlan = oldPlanStr

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
			if util.IsUnretryableError(err2) {
				ret.ErrMsg = err2.Error()
			}
			return ret
		}
	}

	if s.Binding.BindSQL != "" {
		err = syncer.CreateBinding(ctx, s.BindingDigest, s.Binding)
		if err != nil {
			util.Logger.Error("sync binding failed", zap.Error(err))
			if util.IsUnretryableError(err) {
				ret.ErrMsg = err.Error()
			}
			return ret
		}
	}

	newPlan, newPlanStr, err2 := plan.NewPlanFromQuery(ctx, newDB, s.Schema, s.SQL)
	if err2 != nil {
		util.Logger.Error("get new plan failed", zap.Error(err2))
		if util.IsUnretryableError(err2) {
			ret.ErrMsg = err2.Error()
		}
		return ret
	}
	ret.NewDiffPlan = newPlanStr

	sql := s.SQL
	if s.HasParseError {
		sql = ""
	}
	reason, err2 := compare.CmpPlan(sql, oldPlan, newPlan)
	if err2 != nil {
		// this error is not related to network, so it must be non-retryable
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

type metadataResult struct {
	startTime  time.Time
	sourceInfo *util.ClusterInfo
	targetInfo *util.ClusterInfo
}

func processResults(
	allResults []*compare.PlanCmpResult,
	cfg *Config,
	mgr *filemgr.Manager,
	m *metadataResult,
) (*report.Report, error) {
	waitRetry := make([]*compare.PlanCmpResult, 0, len(allResults))
	waitRetryExecCount := 0
	errResults := make([]*compare.PlanCmpResult, 0, len(allResults))
	errResultsExecCount := 0
	cmpSameResults := make([]*compare.PlanCmpResult, 0, len(allResults))
	cmpSamerResultsExecCount := 0
	cmpDiffResults := make([]*compare.PlanCmpResult, 0, len(allResults))
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

		err := mgr.WriteResult(result)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	host, err := os.Hostname()
	if err != nil {
		return nil, errors.Trace(err)
	}
	lastUpdated := time.Now()
	oldCfg := &cfg.OldVersion
	r := &report.Report{
		Deployments: report.TableWithColRowHeader{
			ColHeader: []string{"", "Source", "Target"},
			RowHeader: []string{"# of TiDB", "TiDB version", "PD version", "TiKV version"},
			Data: [][]string{
				{strconv.Itoa(m.sourceInfo.TiDBCnt), strconv.Itoa(m.targetInfo.TiDBCnt)},
				{m.sourceInfo.TiDBVersion, m.targetInfo.TiDBVersion},
				{m.sourceInfo.PDVersion, m.targetInfo.PDVersion},
				{m.sourceInfo.TiKVVersion, m.targetInfo.TiKVVersion},
			},
		},
		TaskInfoItems: [][2]string{
			{"Task Name", cfg.TaskName},
			{"Task Owner", os.Getenv("USER")},
			{"Task Host", host},
			{"Description", cfg.Description},
		},
		CaptureInfoItems: [][2]string{
			{"Capture task started", m.startTime.Format(time.RFC3339)},
			{"Capture task completed", lastUpdated.Format(time.RFC3339)},
			{"Total seconds captured", strconv.FormatFloat(lastUpdated.Sub(m.startTime).Seconds(), 'f', 2, 64)},
			{"Interval", "N/A"},
			{"Endpoint", net.JoinHostPort(oldCfg.Host, strconv.Itoa(oldCfg.Port))},
			{"User", oldCfg.User},
			{"Data Source", "system table"},
			{"Filtering Rules", "EXEC_COUNT > 1"},
			{"Total SQL Statement Count", strconv.Itoa(len(allResults))},
		},
		ExecutionInfoItems: [][2]string{

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

	return r, nil
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
