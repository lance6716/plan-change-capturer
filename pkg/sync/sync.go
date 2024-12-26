package sync

import (
	"bytes"
	"context"
	"database/sql"
	"os"

	"github.com/go-sql-driver/mysql"
	"github.com/lance6716/plan-change-capturer/pkg/source"
	"github.com/lance6716/plan-change-capturer/pkg/util"
	"github.com/pingcap/errors"
	"go.uber.org/zap"
)

// Syncer is used to sync the database / table structure and stats to the target
// database.
// TODO(lance6716): make it concurrent-safe.
type Syncer struct {
	db *sql.DB

	createdDatabase map[string]struct{}
	createdTable    map[string]map[string]struct{}
	createdStats    map[string]struct{}
}

func NewSyncer(db *sql.DB) *Syncer {
	return &Syncer{
		db:              db,
		createdDatabase: make(map[string]struct{}),
		createdTable:    make(map[string]map[string]struct{}),
		createdStats:    make(map[string]struct{}),
	}
}

func (s *Syncer) CreateDatabase(
	ctx context.Context,
	dbName string,
	sql string,
) (err error) {
	if _, ok := s.createdDatabase[dbName]; ok {
		return nil
	}
	defer func() {
		if err == nil {
			s.createdDatabase[dbName] = struct{}{}
		}
	}()
	_, err = s.db.ExecContext(ctx, sql)
	if err == nil {
		return nil
	}

	// when error happens, we check if the database is created before
	util.Logger.Warn(
		"create database failed, will check if the same database is created before",
		zap.String("sql", sql),
		zap.Error(err))
	database, err2 := source.ReadCreateDatabase(ctx, s.db, dbName)
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
	if _, ok := s.createdTable[dbName]; ok {
		if _, ok2 := s.createdTable[dbName][tableName]; ok2 {
			return nil
		}
	} else {
		s.createdTable[dbName] = make(map[string]struct{})
	}
	defer func() {
		if err == nil {
			s.createdTable[dbName][tableName] = struct{}{}
		}
	}()

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
	sql2, err2 := source.ReadCreateTableOrView(ctx, s.db, dbName, tableName)
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
	if _, ok := s.createdStats[statsPath]; ok {
		return nil
	}
	defer func() {
		if err == nil {
			s.createdStats[statsPath] = struct{}{}
		}
	}()
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
