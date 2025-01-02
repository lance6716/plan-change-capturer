package filemgr

import (
	"encoding/json"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"

	"github.com/lance6716/plan-change-capturer/pkg/compare"
	"github.com/lance6716/plan-change-capturer/pkg/source"
	"github.com/pingcap/errors"
)

const (
	stmtSummaryDir     = "stmt-summary"
	stmtSummaryExt     = ".json"
	schemaSubDir       = "schema"
	schemaFilename     = "create.sql"
	tableStatsDir      = "table-stats"
	tableStatsFilename = "table-stats.json"
	resultSubDir       = "result"
	resultExt          = ".json"
)

// Manager owns a folder and organizes the files needed by the plan change capturer.
//
// Currently, Manager organizes files into subfolders:
//
// - stmtSummaryDir: stores the statement summary records captured from
// INFORMATION_SCHEMA.CLUSTER_STATEMENTS_SUMMARY tables.
//
// - schemaSubDir: stores the statements to be restored. So the captured SQL can
// run.
//
// - tableStatsDir: stores the table stats to be restored. So the captured SQL
// can run and generate the same plan.
//
// - resultSubDir: stores the comparison results.
type Manager struct {
	workDir string
}

// NewManager creates a new Manager instance on the given work directory.
func NewManager(workDir string) *Manager {
	return &Manager{workDir: workDir}
}

// WriteStmtSummary writes the statement summary to the file.
func (m *Manager) WriteStmtSummary(s *source.StmtSummary) error {
	dir := filepath.Join(m.workDir, stmtSummaryDir)
	if err := os.MkdirAll(dir, 0776); err != nil {
		return errors.Trace(err)
	}
	content, err := json.Marshal(s)
	if err != nil {
		return errors.Trace(err)
	}
	return errors.Trace(m.atomicWrite(filepath.Join(
		dir, s.SQLDigest+s.PlanDigest+stmtSummaryExt,
	), content))
}

// WriteDatabaseStructure writes the CREATE DATABASE statement to the file.
func (m *Manager) WriteDatabaseStructure(db, createDatabase string) error {
	// TODO(lance6716): skip write file if we have already written.
	dir := filepath.Join(m.workDir, schemaSubDir, db)
	if err := os.MkdirAll(dir, 0776); err != nil {
		return errors.Trace(err)
	}
	return errors.Trace(m.atomicWrite(filepath.Join(dir, schemaFilename), []byte(createDatabase)))
}

// WriteTableStructure writes the CREATE TABLE / VIEW statement to the file.
func (m *Manager) WriteTableStructure(db, table, createTable string) error {
	dir := filepath.Join(m.workDir, schemaSubDir, db, table)
	if err := os.MkdirAll(dir, 0776); err != nil {
		return errors.Trace(err)
	}
	return errors.Trace(m.atomicWrite(filepath.Join(dir, schemaFilename), []byte(createTable)))
}

// WriteTableStats writes the table stats to the file.
func (m *Manager) WriteTableStats(db, table string, json string) error {
	dir := filepath.Join(m.workDir, tableStatsDir, db, table)
	if err := os.MkdirAll(dir, 0776); err != nil {
		return errors.Trace(err)
	}
	return errors.Trace(m.atomicWrite(filepath.Join(dir, tableStatsFilename), []byte(json)))
}

// WriteResult writes the comparison result to the file.
func (m *Manager) WriteResult(r *compare.PlanCmpResult) error {
	dir := filepath.Join(m.workDir, resultSubDir)
	if err := os.MkdirAll(dir, 0776); err != nil {
		return errors.Trace(err)
	}
	content, err := json.Marshal(r)
	if err != nil {
		return errors.Trace(err)
	}
	return errors.Trace(m.atomicWrite(filepath.Join(dir,
		r.OldVersionInfo.SQLDigest+r.OldVersionInfo.PlanDigest+resultExt),
		content,
	))
}

func (m *Manager) atomicWrite(path string, content []byte) error {
	tmpFile := path + ".tmp" + strconv.Itoa(rand.Int())
	if err := os.WriteFile(tmpFile, content, 0666); err != nil {
		return errors.Trace(err)
	}
	return errors.Trace(os.Rename(tmpFile, path))
}

// GetTableStatsPath returns the path of the table stats file.
func (m *Manager) GetTableStatsPath(db, table string) string {
	return filepath.Join(m.workDir, tableStatsDir, db, table, tableStatsFilename)
}

// TODO(lance6716): recover from previous run
