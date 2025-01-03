package util

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func FuzzEscapePath(f *testing.F) {
	workDir := f.TempDir()
	f.Add("a")
	f.Add("a/b")
	f.Add("a\\b")
	f.Add("a:b")
	f.Add("a*b")
	f.Add("a?b")
	f.Add("a\"b")
	f.Add("a<b")
	f.Add("a>b")
	f.Add("a|b")

	f.Fuzz(func(t *testing.T, input string) {
		if len(input) > 20 {
			return
		}
		got := EscapePath(input)
		dir := filepath.Join(workDir, got)
		err := os.Mkdir(dir, 0776)
		require.NoError(t, err, "input: %s, got: %s", input, got)
		err = os.Remove(dir)
		require.NoError(t, err, "input: %s, got: %s", input, got)
	})
}
