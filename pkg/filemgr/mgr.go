package filemgr

import (
	"encoding/json"
	"math/rand"
	"os"
	"path"
	"strconv"

	"github.com/lance6716/plan-change-capturer/pkg/source"
	"github.com/pingcap/errors"
)

const (
	schemaSubDir       = "schema"
	schemaFilename     = "create-table.sql"
	stmtSummaryDir     = "stmt-summary"
	stmtSummaryExt     = ".json"
	tableStatsDir      = "table-stats"
	tableStatsFilename = "table-stats.json"
)

// Manager owns a folder and organizes the files needed by the plan change capturer.
// TODO(lance6716): explain hierarchy
type Manager struct {
	workDir string
}

// NewManager creates a new Manager instance on the given work directory.
func NewManager(workDir string) *Manager {
	return &Manager{workDir: workDir}
}

// WriteStmtSummary writes the statement summary to the file.
func (m *Manager) WriteStmtSummary(s source.StmtSummary) error {
	dir := path.Join(m.workDir, stmtSummaryDir)
	if err := os.MkdirAll(dir, 0776); err != nil {
		return errors.Trace(err)
	}
	content, err := json.Marshal(s)
	if err != nil {
		return errors.Trace(err)
	}
	return m.atomicWrite(path.Join(
		dir, s.SQLDigest+s.PlanDigest+stmtSummaryExt,
	), content)
}

// WriteSchema writes the create table statement to the schema file.
func (m *Manager) WriteSchema(db, table, createTable string) error {
	dir := path.Join(m.workDir, schemaSubDir, db, table)
	if err := os.MkdirAll(dir, 0776); err != nil {
		return errors.Trace(err)
	}
	return m.atomicWrite(path.Join(dir, schemaFilename), []byte(createTable))
}

// WriteTableStats writes the table stats to the file.
func (m *Manager) WriteTableStats(db, table string, json string) error {
	dir := path.Join(m.workDir, tableStatsDir, db, table)
	if err := os.MkdirAll(dir, 0776); err != nil {
		return errors.Trace(err)
	}
	return m.atomicWrite(path.Join(dir, tableStatsFilename), []byte(json))
}

func (m *Manager) atomicWrite(path string, content []byte) error {
	tmpFile := path + ".tmp" + strconv.Itoa(rand.Int())
	if err := os.WriteFile(tmpFile, content, 0666); err != nil {
		return errors.Trace(err)
	}
	return os.Rename(tmpFile, path)
}

// GetTableStatsPath returns the path of the table stats file.
func (m *Manager) GetTableStatsPath(db, table string) string {
	return path.Join(m.workDir, tableStatsDir, db, table, tableStatsFilename)
}

// TODO(lance6716): recover from previous run
