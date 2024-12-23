package util

import "strings"

func EscapeIdentifier(s string) string {
	return "`" + strings.ReplaceAll(s, "`", "``") + "`"
}
