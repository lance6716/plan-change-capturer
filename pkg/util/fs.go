package util

import (
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"unicode"

	"github.com/pingcap/errors"
)

// AtomicWrite writes content to path atomically by using mv.
func AtomicWrite(path string, content []byte) error {
	// there's a little chance that rand.Int conflicts
	tmpFile := path + ".tmp" + strconv.Itoa(rand.Int())
	if err := os.WriteFile(tmpFile, content, 0666); err != nil {
		return errors.Trace(err)
	}
	return errors.Trace(os.Rename(tmpFile, path))
}

// EscapePath encodes special characters in a string to make it safe for use as a file system path.
func EscapePath(input string) string {
	if input == "" {
		return "empty-string"
	}
	var builder strings.Builder
	for _, r := range input {
		if unicode.IsPrint(r) && r != '/' && r != '\\' && r != ':' &&
			r != '*' && r != '?' && r != '"' && r != '<' && r != '>' &&
			r != '|' && r != '.' {
			// Keep printable and safe characters as-is
			builder.WriteRune(r)
		} else {
			// Encode all other characters as %XX (hexadecimal representation)
			builder.WriteString(fmt.Sprintf("%%%02X", r))
		}
	}
	return builder.String()
}
