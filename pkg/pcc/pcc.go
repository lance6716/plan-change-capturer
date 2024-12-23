package pcc

import (
	"context"
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
		cfg := zap.NewProductionConfig()
		cfg.OutputPaths = []string{loggerCfg.Filename}
		logger, err := cfg.Build()
		if err != nil {
			return errors.Trace(err)
		}
		util.Logger = logger
	}
	return nil
}

func run(cfg *Config) error {
	oldCfg := &cfg.OldVersion
	oldDB, err := util.ConnectDB(oldCfg.Host, oldCfg.Port, oldCfg.User, oldCfg.Password)
	if err != nil {
		return errors.Trace(err)
	}
	defer oldDB.Close()
	newCfg := &cfg.NewVersion
	newDB, err := util.ConnectDB(newCfg.Host, newCfg.Port, newCfg.User, newCfg.Password)
	if err != nil {
		return errors.Trace(err)
	}
	defer newDB.Close()

	mgr := filemgr.NewManager(cfg.WorkDir)

	ctx := context.Background()

	summaries, err := source.ReadStmtSummary(ctx, oldDB)
	if err != nil {
		return errors.Trace(err)
	}
	// TODO(lance6716): tolerate some error
	for _, s := range summaries {
		err = mgr.WriteStmtSummary(s)
		if err != nil {
			return errors.Trace(err)
		}

		for _, table := range s.TableNames {
			createTable, err2 := source.ReadTableStructure(ctx, oldDB, table[0], table[1])
			if err2 != nil {
				return errors.Trace(err2)
			}
			err2 = mgr.WriteSchema(table[0], table[1], createTable)
			if err2 != nil {
				return errors.Trace(err2)
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

			err2 = sync.CreateTable(ctx, newDB, table[0], table[1], createTable)
			if err2 != nil {
				return errors.Trace(err2)
			}
			err2 = sync.LoadStats(ctx, newDB, mgr.GetTableStatsPath(table[0], table[1]))
			if err2 != nil {
				return errors.Trace(err2)
			}

			oldPlan, err2 := plan.NewPlanFromStmtSummaryPlan(s.PlanStr)
			if err2 != nil {
				return errors.Trace(err2)
			}
			newPlan, err2 := plan.NewPlanFromQuery(ctx, newDB, table[0], s.SQL)
			if err2 != nil {
				return errors.Trace(err2)
			}

			reason, err2 := compare.CmpPlan(s.SQL, oldPlan, newPlan)
			if err2 != nil {
				return errors.Trace(err2)
			}

			util.Logger.Info("compare result", zap.String("reason", string(reason)))
		}
	}
	return nil
}
