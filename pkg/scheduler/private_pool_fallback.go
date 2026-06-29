package scheduler

import (
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/rs/zerolog/log"
)

type privatePoolCapacityChecker interface {
	HasWorkerCapacity(cpu int64, memory int64, gpuCount uint32) (bool, error)
}

func (a *schedulingAttempt) tryPrivatePoolFallback() bool {
	fallbackRequest, poolName, ok := a.privatePoolFallbackRequest()
	if !ok {
		return false
	}

	requestLog(log.Debug(), a.request).
		Str("fallback_from_pool", poolName).
		Msg("falling back from private pool to regular pools")

	fallback := newSchedulingAttempt(a.scheduler, fallbackRequest, a.workers)
	if fallback.scheduleOnAvailableWorker() {
		return true
	}
	if fallback.reservePendingWorkerCapacity() {
		return true
	}
	if !fallback.canProvisionWorker() {
		return false
	}

	fallback.provisionWorker()
	return true
}

func (a *schedulingAttempt) privatePoolFallbackRequest() (*types.ContainerRequest, string, bool) {
	if a == nil || a.request == nil {
		return nil, "", false
	}
	if a.request.HasDurableDiskMount() {
		// Durable disk fallback is handled before scheduling so snapshot
		// availability is checked before the pool selector is cleared.
		return nil, "", false
	}

	pool, ok := a.selectedPrivatePool()
	if !ok || pool.Controller == nil {
		return nil, "", false
	}

	checker, ok := pool.Controller.(privatePoolCapacityChecker)
	if !ok {
		return nil, "", false
	}

	cpu := a.scheduler.workerCPUForControllerRequest(pool.Controller, a.request)
	memory := a.scheduler.workerMemoryForControllerRequest(pool.Controller, a.request)
	gpuCount := a.scheduler.workerGPUCountForControllerRequest(pool.Controller, a.request)
	hasCapacity, err := checker.HasWorkerCapacity(cpu, memory, gpuCount)
	if err != nil {
		requestLog(log.Debug(), a.request).
			Str("pool_name", pool.Name).
			Err(err).
			Msg("unable to check private pool capacity for fallback")
		return nil, "", false
	}
	if hasCapacity {
		return nil, "", false
	}

	fallback := a.request.Clone()
	if fallback == nil {
		return nil, "", false
	}
	fallback.PoolSelector = ""
	return fallback, pool.Name, true
}

func (a *schedulingAttempt) selectedPrivatePool() (*WorkerPool, bool) {
	if a == nil || a.scheduler == nil || a.request == nil || a.request.PoolSelector == "" || a.scheduler.workerPoolManager == nil {
		return nil, false
	}

	pool, ok := a.scheduler.workerPoolManager.GetPool(a.request.PoolSelector)
	if !ok || pool.Config.Mode != types.PoolModePrivate {
		return nil, false
	}
	return pool, true
}

func (a *schedulingAttempt) canProvisionWorker() bool {
	controllers, err := a.scheduler.getControllers(a.request)
	if err != nil {
		return false
	}
	controller, _ := a.scheduler.workerProvisioningController(controllers)
	return controller != nil
}
