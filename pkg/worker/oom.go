package worker

import (
	"context"
	"log/slog"
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

	// Select appropriate OOM watcher based on runtime
	if s.runtime.Name() == types.ContainerRuntimeGvisor.String() {
		// The gVisor watcher observes host-side runsc RSS, which includes runtime
		// overhead and is not the same as guest workload memory. Do not turn that
		// approximation into a kill signal when memory enforcement is disabled.
		if !s.config.Worker.ContainerResourceLimits.MemoryEnforced {
			return
		}

		if spec == nil ||
			spec.Linux == nil ||
			spec.Linux.Resources == nil ||
			spec.Linux.Resources.Memory == nil ||
			spec.Linux.Resources.Memory.Limit == nil {
			return
		}

		memoryLimit := s.getMemoryLimit(spec, request)
		oomWatcher = runtime.NewGvisorOOMWatcher(ctx, pid, memoryLimit)
	} else {
		// For runc: use cgroup-based OOM detection
		cgroupPath, err := runtime.GetCgroupPathFromPID(pid)
		if err != nil {
			log.Warn().Str("container_id", containerId).Err(err).Msg("failed to get cgroup path, OOM detection disabled")
			return
		}
		oomWatcher = runtime.NewCgroupOOMWatcher(ctx, cgroupPath)
	}

	// Store watcher and start monitoring
	containerInstance, exists := s.containerInstances.Get(containerId)
	if !exists {
		return
	}

	containerInstance.OOMWatcher = oomWatcher
	s.containerInstances.Set(containerId, containerInstance)

	// Start watching with callback
	err := oomWatcher.Watch(func() {
		s.handleOOMKill(ctx, containerId, request, outputLogger, isOOMKilled)
	})

	if err != nil {
		log.Warn().Str("container_id", containerId).Err(err).Msg("OOM watcher failed to start")
	}
}

// getMemoryLimit extracts memory limit from spec or request
func (s *Worker) getMemoryLimit(spec *specs.Spec, request *types.ContainerRequest) uint64 {
	if spec != nil &&
		spec.Linux != nil &&
		spec.Linux.Resources != nil &&
		spec.Linux.Resources.Memory != nil &&
		spec.Linux.Resources.Memory.Limit != nil {
		return uint64(*spec.Linux.Resources.Memory.Limit)
	}
	// Fallback to request memory (convert MB to bytes)
	return uint64(request.Memory * 1024 * 1024)
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
