package common

import "regexp"

var (
	sensitiveKeyValuePattern = regexp.MustCompile(`(?i)\b(access[_-]?key|secret[_-]?key|api[_-]?key|token|password|authorization|credentials?)\b(\s*[:=]\s*)("[^"]*"|'[^']*'|[^\s,]+)`)
	bearerTokenPattern       = regexp.MustCompile(`(?i)\bbearer\s+[A-Za-z0-9._~+/=-]+`)
)

func RedactSecrets(value string) string {
	value = bearerTokenPattern.ReplaceAllString(value, "Bearer [redacted]")
	return sensitiveKeyValuePattern.ReplaceAllString(value, `$1$2[redacted]`)
}
