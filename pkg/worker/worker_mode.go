package worker

import (
	"strings"
)

func (s *Worker) agentWorker() bool {
	return s != nil && s.persistent && s.machineID != "" && s.routeTransport != ""
}

func firstNonEmptyWorkerValue(values ...string) string {
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			return value
		}
	}
	return ""
}
