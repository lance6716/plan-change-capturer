package pcc

import (
	"context"

	"github.com/lance6716/plan-change-capturer/pkg/filemgr"
	"github.com/lance6716/plan-change-capturer/pkg/source"
	"github.com/lance6716/plan-change-capturer/pkg/util"
	"github.com/pingcap/errors"
)

// Run is the main entry function of the pcc logic.
func Run(cfg *Config) error {
	cfg.ensureDefaults()

	oldCfg := &cfg.OldVersion
	oldDB, err := util.ConnectDB(oldCfg.Host, oldCfg.Port, oldCfg.User, oldCfg.Password)
	if err != nil {
		return errors.Trace(err)
	}
	defer oldDB.Close()

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
		}
	}
	return nil
}
