package worker

import (
	"context"
	"log/slog"
	"strconv"
	"time"

	"github.com/beam-cloud/beta9/pkg/runtime"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/rs/zerolog/log"
)

// Application OOMs report a guest process kill without terminating the sandbox.
// The container's lifetime context owns this watcher, like its metrics watcher.
func (s *Worker) watchApplicationOOM(ctx context.Context, request *types.ContainerRequest, outputLogger *slog.Logger, rt runtime.Runtime) {
	events, err := rt.Events(ctx, request.ContainerId)
	if err != nil {
		log.Warn().Err(err).Str("container_id", request.ContainerId).Msg("application OOM watcher failed to start")
		return
	}
	for {
		select {
		case <-ctx.Done():
			return
		case event, ok := <-events:
			if !ok {
				return
			}
			if event.ApplicationOOM == nil {
				continue
			}
			oom := event.ApplicationOOM
			outputLogger.Info(types.EventMessageApplicationOOMKilled.String(),
				"oom_kills", oom.Kills, "memory_limit", oom.MemoryLimit,
				"memory_usage", oom.MemoryUsage, "memory_peak", oom.MemoryPeak)
			s.recordContainerEvent(ctx, request, types.EventContainerEventSchema{
				ID:        types.ContainerEventApplicationOOMKilled,
				Domain:    types.EventDomainRuntime,
				Timestamp: time.Now().UTC(),
				Reason:    "OOM",
				Source:    types.EventSourceWorkerRuntime.String(),
				Message:   types.EventMessageApplicationOOMKilled.String(),
				Attrs: map[string]string{
					types.EventAttrExitCode: "137",
					"oom_kills":             strconv.FormatUint(oom.Kills, 10),
					"memory_limit":          strconv.FormatInt(oom.MemoryLimit, 10),
					"memory_usage":          strconv.FormatUint(oom.MemoryUsage, 10),
					"memory_peak":           strconv.FormatUint(oom.MemoryPeak, 10),
				},
			})
		}
	}
}
