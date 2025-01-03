package schema

import (
	"bytes"
	"context"
	"database/sql"
	"os"
	"sync"

	"github.com/go-sql-driver/mysql"
	"github.com/lance6716/plan-change-capturer/pkg/util"
	"github.com/pingcap/errors"
	"go.uber.org/zap"
)

// Syncer is used to synchronize the database / table structure and stats to the
// target database. It's concurrent safe and the same object will only be
// synchronized once.
type Syncer struct {
	db *sql.DB

	databaseOnce sync.Map // dbName -> sync.Once
	databaseErr  sync.Map // dbName -> execution error
	tableOnce    sync.Map // {dbName}.{tableName} -> sync.Once
	tableErr     sync.Map // {dbName}.{tableName} -> execution error
	statsOnce    sync.Map // statsPath -> sync.Once
	statsErr     sync.Map // statsPath -> execution error
}

func NewSyncer(db *sql.DB) *Syncer {
	return &Syncer{
		db: db,
	}
}

func (s *Syncer) CreateDatabase(
	ctx context.Context,
	dbName string,
	sql string,
) (err error) {
	o := new(sync.Once)
	once, _ := s.databaseOnce.LoadOrStore(dbName, o)
	once.(*sync.Once).Do(func() {
		s.databaseErr.Store(dbName, s.createDatabase(ctx, dbName, sql))
	})
	errLoaded, _ := s.databaseErr.Load(dbName)
	if errLoaded == nil {
		return nil
	}
	return errLoaded.(error)
}

func (s *Syncer) createDatabase(
	ctx context.Context,
	dbName string,
	sql string,
) (err error) {
	_, err = s.db.ExecContext(ctx, sql)
	if err == nil {
		return nil
	}

	// when error happens, we check if the database is created before
	util.Logger.Warn(
		"create database failed, will check if the same database is created before",
		zap.String("sql", sql),
		zap.Error(err))
	database, err2 := util.ReadCreateDatabase(ctx, s.db, dbName)
	if err2 != nil {
		return errors.Trace(err2)
	}
	if sql == database {
		return nil
	}
	return errors.Annotatef(err,
		"create database failed and the same database is not created before. sql: %s",
		sql,
	)
}

func (s *Syncer) CreateTable(
	ctx context.Context,
	dbName, tableName string,
	sql string,
) (err error) {
	dbDotTable := util.EscapeIdentifier(dbName) + "." + util.EscapeIdentifier(tableName)
	o := new(sync.Once)
	once, _ := s.tableOnce.LoadOrStore(dbDotTable, o)
	once.(*sync.Once).Do(func() {
		s.tableErr.Store(dbDotTable, s.createTable(ctx, dbName, tableName, sql))
	})
	errLoaded, _ := s.tableErr.Load(dbName)
	if errLoaded == nil {
		return nil
	}
	return errLoaded.(error)
}

func (s *Syncer) createTable(
	ctx context.Context,
	dbName, tableName string,
	sql string,
) (err error) {
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return errors.Annotatef(err, "create table for %s.%s", dbName, tableName)
	}
	defer conn.Close()

	_, err = conn.ExecContext(ctx, "USE "+dbName)
	if err != nil {
		return errors.Annotatef(err, "create table for %s.%s", dbName, tableName)
	}

	_, err = conn.ExecContext(ctx, sql)
	if err == nil {
		return nil
	}

	// when error happens, we check if the table is created before
	util.Logger.Warn(
		"create table failed, will check if the same table is created before",
		zap.String("database", dbName),
		zap.String("table", tableName),
		zap.String("sql", sql),
		zap.Error(err))
	sql2, err2 := util.ReadCreateTableViewSeq(ctx, s.db, dbName, tableName)
	if err2 != nil {
		return errors.Trace(err2)
	}
	if sql == sql2 {
		return nil
	}
	return errors.Annotatef(err,
		"create table failed and the same table is not created before. database: %s, table: %s, sql: %s",
		dbName, tableName, sql,
	)
}

func (s *Syncer) LoadStats(
	ctx context.Context,
	statsPath string,
) (err error) {
	o := new(sync.Once)
	once, _ := s.statsOnce.LoadOrStore(statsPath, o)
	once.(*sync.Once).Do(func() {
		s.statsErr.Store(statsPath, s.loadStats(ctx, statsPath))
	})
	errLoaded, _ := s.statsErr.Load(statsPath)
	if errLoaded == nil {
		return nil
	}
	return errLoaded.(error)
}

func (s *Syncer) loadStats(
	ctx context.Context,
	statsPath string,
) (err error) {
	content, err := os.ReadFile(statsPath)
	if err != nil {
		return errors.Annotatef(err, "read stats file %s", statsPath)
	}
	if bytes.Equal(content, []byte("null")) {
		return nil
	}
	mysql.RegisterLocalFile(statsPath)
	defer mysql.DeregisterLocalFile(statsPath)
	_, err = s.db.ExecContext(ctx, "LOAD STATS '"+statsPath+"'")
	return errors.Annotatef(err, "load stats from %s", statsPath)
}
