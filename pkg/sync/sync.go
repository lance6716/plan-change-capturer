package sync

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"os"

	"github.com/go-sql-driver/mysql"
	"github.com/lance6716/plan-change-capturer/pkg/source"
	"github.com/lance6716/plan-change-capturer/pkg/util"
	"github.com/pingcap/errors"
	"go.uber.org/zap"
)

func CreateTable(
	ctx context.Context,
	db *sql.DB,
	dbName, tableName string,
	sql string,
) error {
	conn, err := db.Conn(ctx)
	if err != nil {
		// TODO(lance6716): use errors.Annotate
		return fmt.Errorf("error when create table for %s.%s: %s", dbName, tableName, err)
	}
	defer conn.Close()

	_, err = conn.ExecContext(ctx, "USE "+dbName)
	if err != nil {
		return fmt.Errorf("error when create table for %s.%s: %s", dbName, tableName, err)
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
	sql2, err := source.ReadTableStructure(ctx, db, dbName, tableName)
	if err != nil {
		return errors.Trace(err)
	}
	if sql == sql2 {
		return nil
	}
	return fmt.Errorf(
		"create table failed and the same table is not created before. database: %s, table: %s, sql: %s, error: %s",
		dbName, tableName, sql, err,
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
