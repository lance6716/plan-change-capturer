package pcc

import (
	"os"
	"path/filepath"
	"time"
)

// Config is a static struct for pcc's configuration.
type Config struct {
	TaskName    string
	Description string

	OldVersion TiDB
	NewVersion TiDB
	WorkDir    string
	Log        Log
}

type TiDB struct {
	Host       string
	Port       int
	User       string
	Password   string
	StatusPort int
	MaxConn    int
}

type Log struct {
	Filename string
}

const defaultWorkSubDir = "plan-change-capturer"

func (c *Config) ensureDefaults() {
	if c.TaskName == "" {
		c.TaskName = time.Now().Format(time.RFC3339)
	}
	if c.WorkDir == "" {
		c.WorkDir = filepath.Join(os.TempDir(), defaultWorkSubDir)
	}
}
