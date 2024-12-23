package pcc

import (
	"os"
	"path"
)

// Config is a static struct for pcc's configuration.
type Config struct {
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
}

type Log struct {
	Filename string
}

const defaultWorkSubDir = "plan-change-capturer"

func (c *Config) ensureDefaults() {
	if c.WorkDir == "" {
		c.WorkDir = path.Join(os.TempDir(), defaultWorkSubDir)
	}
}

// TODO(lance6716): logger
