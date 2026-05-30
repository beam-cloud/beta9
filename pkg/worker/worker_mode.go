package worker

import (
	"strings"

	"github.com/beam-cloud/beta9/pkg/types"
)

func (s *Worker) agentWorker() bool {
	return s != nil && s.persistent && s.machineID != "" && s.routeTransport != ""
}

func (s *Worker) useMemoryOverlay(request *types.ContainerRequest) bool {
	return request.IsBuildRequest() || s.agentWorker()
}

func firstNonEmptyWorkerValue(values ...string) string {
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			return value
		}
	}
	return ""
}
