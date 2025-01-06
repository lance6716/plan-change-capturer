package pcc

import (
	"context"
	"database/sql"
	"net"
	"net/http"
	"strconv"

	"github.com/go-sql-driver/mysql"
	"github.com/lance6716/plan-change-capturer/pkg/filemgr"
	"github.com/lance6716/plan-change-capturer/pkg/schema"
	"github.com/lance6716/plan-change-capturer/pkg/source"
	"github.com/lance6716/plan-change-capturer/pkg/util"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser"
)

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

// TODO(lance6716): test sync user TEMPORARY, CACHE (plan will be different if
// not ALTER CACHE) table
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

	createTable, err2 := util.ReadCreateTableViewSeq(ctx, oldDB, table[0], table[1])
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
