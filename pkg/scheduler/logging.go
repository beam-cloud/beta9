package scheduler

import (
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/rs/zerolog"
)

func requestLog(event *zerolog.Event, request *types.ContainerRequest) *zerolog.Event {
	if request == nil {
		return event
	}
	return event.
		Str("container_id", request.ContainerId).
		Str("stub_id", request.StubId).
		Str("workspace_id", request.WorkspaceId).
		Str("pool_selector", request.PoolSelector).
		Int64("cpu", request.Cpu).
		Int64("memory", request.Memory).
		Str("gpu", request.Gpu).
		Uint32("gpu_count", request.GpuCount).
		Int("retry_count", request.RetryCount)
}

func workerLog(event *zerolog.Event, worker *types.Worker) *zerolog.Event {
	if worker == nil {
		return event
	}
	return event.
		Str("worker_id", worker.Id).
		Str("pool_name", worker.PoolName).
		Str("machine_id", worker.MachineId).
		Str("worker_status", string(worker.Status)).
		Int64("free_cpu", worker.FreeCpu).
		Int64("free_memory", worker.FreeMemory).
		Uint32("free_gpu_count", worker.FreeGpuCount)
}
