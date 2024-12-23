package pcc

import (
	"os"
	"path"
)

type Config struct {
	OldVersion DB
	NewVersion DB

	WorkDir string
}

type DB struct {
	Host     string
	Port     int
	User     string
	Password string
}

const defaultWorkSubDir = "plan-change-capturer"

func (c *Config) ensureDefaults() {
	if c.WorkDir == "" {
		c.WorkDir = path.Join(os.TempDir(), defaultWorkSubDir)
	}
}
