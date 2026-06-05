package agent

import (
	"os"
	"strings"
	"time"
)

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func envBool(name string) bool {
	value := strings.ToLower(strings.TrimSpace(os.Getenv(name)))
	return value == "1" || value == "true" || value == "yes" || value == "on"
}

func nextBackoff(current, max time.Duration) time.Duration {
	if current <= 0 {
		current = time.Second
	}
	if max <= 0 {
		return current
	}
	current *= 2
	if current > max {
		return max
	}
	return current
}
