package pcc

import (
	"context"
	"database/sql"
	"net"
	"net/http"
	"strconv"

	"github.com/lance6716/plan-change-capturer/pkg/compare"
	"github.com/lance6716/plan-change-capturer/pkg/filemgr"
	"github.com/lance6716/plan-change-capturer/pkg/plan"
	"github.com/lance6716/plan-change-capturer/pkg/source"
	"github.com/lance6716/plan-change-capturer/pkg/sync"
	"github.com/lance6716/plan-change-capturer/pkg/util"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/parser"
	"go.uber.org/zap"
)

// Run is the main entry function of the pcc logic.
func Run(cfg *Config) {
	cfg.ensureDefaults()
	if err := initLogger(&cfg.Log); err != nil {
		util.Logger.Error("failed to initialize logger", zap.Error(err))
	}
	defer util.Logger.Sync()

	err := run(cfg)
	if err != nil {
		util.Logger.Error("failed to run pcc", zap.Error(err))
	}
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

func run(cfg *Config) error {
	ctx := context.Background()

	oldDB, newDB, err := prepareDBConnections(ctx, cfg)
	if err != nil {
		return errors.Trace(err)
	}
	defer oldDB.Close()
	defer newDB.Close()

	mgr := filemgr.NewManager(cfg.WorkDir)
	syncer := sync.NewSyncer(newDB)

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

	// TODO(lance6716): error should not fail the whole process
	for _, s := range summaries {
		for _, table := range s.TableNames {
			err2 := syncForTable(ctx, oldDB, table, s.Schema, syncer, mgr, oldCfg)
			if err2 != nil {
				return errors.Trace(err2)
			}
		}

		oldPlan, err2 := plan.NewPlanFromStmtSummaryPlan(s.PlanStr)
		if err2 != nil {
			return errors.Trace(err2)
		}
		newPlan, err2 := plan.NewPlanFromQuery(ctx, newDB, s.Schema, s.SQL)
		if err2 != nil {
			return errors.Trace(err2)
		}

		// TODO(lance6716): s.HasParseError?
		reason, err2 := compare.CmpPlan(s.SQL, oldPlan, newPlan)
		if err2 != nil {
			return errors.Trace(err2)
		}

		util.Logger.Info("compare result",
			zap.String("reason", string(reason)),
			zap.String("sql", s.SQL),
		)
	}
	return nil
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

	return oldDB, newDB, nil
}

func syncForTable(
	ctx context.Context,
	oldDB *sql.DB,
	table [2]string,
	currDB string,
	syncer *sync.Syncer,
	mgr *filemgr.Manager,
	oldCfg *TiDB,
) error {
	createDatabase, err2 := source.ReadCreateDatabase(ctx, oldDB, table[0])
	if err2 != nil {
		return errors.Trace(err2)
	}
	err2 = mgr.WriteDatabaseStructure(table[0], createDatabase)
	if err2 != nil {
		return errors.Trace(err2)
	}

	createTable, err2 := source.ReadCreateTableOrView(ctx, oldDB, table[0], table[1])
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
