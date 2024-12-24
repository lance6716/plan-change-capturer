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

func CreateDatabase(
	ctx context.Context,
	db *sql.DB,
	dbName string,
	sql string,
) error {
	_, err := db.ExecContext(ctx, sql)
	if err == nil {
		return nil
	}

	// when error happens, we check if the database is created before
	util.Logger.Warn(
		"create database failed, will check if the same database is created before",
		zap.String("sql", sql),
		zap.Error(err))
	database, err2 := source.ReadCreateDatabase(ctx, db, dbName)
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

func CreateTable(
	ctx context.Context,
	db *sql.DB,
	dbName, tableName string,
	sql string,
) error {
	conn, err := db.Conn(ctx)
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
	sql2, err2 := source.ReadCreateTableOrView(ctx, db, dbName, tableName)
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

func LoadStats(
	ctx context.Context,
	db *sql.DB,
	statsPath string,
) error {
	content, err := os.ReadFile(statsPath)
	if err != nil {
		return errors.Annotatef(err, "read stats file %s", statsPath)
	}
	if bytes.Equal(content, []byte("null")) {
		return nil
	}
	mysql.RegisterLocalFile(statsPath)
	_, err = db.ExecContext(ctx, "LOAD STATS '"+statsPath+"'")
	return errors.Annotatef(err, "load stats from %s", statsPath)
}
