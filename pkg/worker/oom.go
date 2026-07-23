package worker

import (
	"context"
	"log/slog"
	"strings"
	"sync/atomic"
	"time"

	runtime "github.com/beam-cloud/beta9/pkg/runtime"
	types "github.com/beam-cloud/beta9/pkg/types"
	"github.com/opencontainers/runtime-spec/specs-go"
	"github.com/rs/zerolog/log"
)

// setupOOMWatcher configures and starts the appropriate OOM watcher for the container
// Returns true if OOM is detected (via the atomic bool)
func (s *Worker) setupOOMWatcher(
	ctx context.Context,
	containerId string,
	pid int,
	spec *specs.Spec,
	request *types.ContainerRequest,
	outputLogger *slog.Logger,
	isOOMKilled *atomic.Bool,
) {
	var oomWatcher runtime.OOMWatcher

	if s.runtime.Name() == types.ContainerRuntimeGvisor.String() {
		if !s.config.Worker.ContainerResourceLimits.MemoryEnforced {
			return
		}
		// runsc places the sandbox and gofer in a cgroup named after the
		// container. memory.events accounts shared mappings once and only
		// reports an OOM after the kernel actually enforces the hard limit.
		cgroupPath := containerId
		if spec != nil && spec.Linux != nil && spec.Linux.CgroupsPath != "" {
			cgroupPath = strings.TrimPrefix(spec.Linux.CgroupsPath, "/")
		}
		oomWatcher = runtime.NewCgroupOOMWatcher(ctx, cgroupPath)
	} else {
		cgroupPath, err := runtime.GetCgroupPathFromPID(pid)
		if err != nil {
			log.Warn().Str("container_id", containerId).Err(err).Msg("failed to get cgroup path, OOM detection disabled")
			return
		}
		oomWatcher = runtime.NewCgroupOOMWatcher(ctx, cgroupPath)
	}

	containerInstance, exists := s.containerInstances.Get(containerId)
	if !exists {
		return
	}

	containerInstance.OOMWatcher = oomWatcher
	s.containerInstances.Set(containerId, containerInstance)

	err := oomWatcher.Watch(func() {
		s.handleOOMKill(ctx, containerId, request, outputLogger, isOOMKilled)
	})

	if err != nil {
		log.Warn().Str("container_id", containerId).Err(err).Msg("OOM watcher failed to start")
	}
}

// handleOOMKill handles the OOM kill event
func (s *Worker) handleOOMKill(
	ctx context.Context,
	containerId string,
	request *types.ContainerRequest,
	outputLogger *slog.Logger,
	isOOMKilled *atomic.Bool,
) {
	log.Warn().Str("container_id", containerId).Msg("OOM kill detected")
	isOOMKilled.Store(true)
	outputLogger.Info(types.WorkerContainerExitCodeOomKillMessage)

	go s.recordContainerEvent(ctx, request, types.EventContainerEventSchema{
		ID:        types.ContainerEventRuntimeOOMKilled,
		Domain:    types.EventDomainRuntime,
		Timestamp: time.Now().UTC(),
		Reason:    "OOM",
		Source:    types.EventSourceWorkerRuntime.String(),
		Message:   types.EventMessageRuntimeOOMKilled.String(),
		Attrs: map[string]string{
			types.EventAttrOOMKilled: "true",
		},
	})

	// For gVisor, manually stop the container (kernel won't do it automatically)
	if s.runtime.Name() == types.ContainerRuntimeGvisor.String() {
		log.Info().Str("container_id", containerId).Msg("stopping container due to OOM (gVisor)")
		go func() {
			if err := s.stopContainer(containerId, true); err != nil {
				log.Error().Str("container_id", containerId).Err(err).Msg("failed to stop OOM container")
			}
		}()
	}
}
