package scheduler

import (
	"errors"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/rs/zerolog/log"
)

// tryPrivatePoolFallback moves a request that names its workspace's own pool
// onto the regular pools when that pool cannot take it and the stub allows
// it. The request goes back to the backlog without its pool, and the next
// pass plans it like any other. It reports whether it dealt with the
// request.
func (a *schedulingAttempt) tryPrivatePoolFallback() bool {
	fallbackRequest, poolName, ok := a.privatePoolFallbackRequest()
	if !ok {
		return false
	}

	requestLog(log.Debug(), a.request).
		Str("fallback_from_pool", poolName).
		Msg("falling back from private pool to regular pools")

	fallback := newSchedulingAttempt(a.scheduler, fallbackRequest, a.workers)
	if !fallback.hasManagedFallbackPath() {
		return false
	}

	refusal, err := fallback.admitToRegularPools()
	if err == nil {
		// The request keeps its place in line.
		fallback.requeueForWorkerWaitDelay(0, "managed_fallback")
		return true
	}
	// A container that was stopped in the meantime has nothing left to
	// schedule.
	if !a.runnable() {
		return true
	}
	if refusal == "" {
		// No answer is not a refusal: the request asks again.
		requestLog(log.Warn(), fallbackRequest).
			Err(err).
			Msg("unable to ask whether private pool fallback is allowed")
		a.retry("managed_fallback_check_failed")
		return true
	}
	requestLog(log.Info(), fallbackRequest).
		Err(err).
		Msg("private pool fallback refused")
	a.fail(refusal)
	return true
}

// admitToRegularPools asks what Run asks of a request for the regular pools,
// and what this request was exempt from on its workspace's own pool: whether
// the workspace has credit, and whether the request fits within the
// workspace's concurrency limit, which it then counts against.
//
// It returns nil when the request is admitted. Otherwise the error is the
// answer: refusal is why the request fails when the answer is no, and empty
// when there was no answer.
func (a *schedulingAttempt) admitToRegularPools() (refusal types.ContainerSchedulingFailureReason, err error) {
	err = a.scheduler.checkWorkspaceCredit(a.request, false)
	var noCredit *types.InsufficientCreditsError
	if errors.As(err, &noCredit) && noCredit.Code != creditGateErrorBillingUnavailable {
		return types.ContainerSchedulingFailureManagedFallbackNoCredit, err
	}
	if err != nil {
		return "", err
	}

	err = a.reserveManagedFallbackQuota()
	var overLimit *types.ThrottledByConcurrencyLimitError
	if errors.As(err, &overLimit) {
		return types.ContainerSchedulingFailureManagedFallbackConcurrencyLimit, err
	}
	return "", err
}

func (a *schedulingAttempt) privatePoolFallbackRequest() (*types.ContainerRequest, string, bool) {
	if a.request.HasDurableDiskMount() {
		// Durable disk fallback is handled before scheduling so snapshot
		// availability is checked before the pool selector is cleared.
		return nil, "", false
	}
	if !a.privatePoolAllowsManagedFallback() {
		return nil, "", false
	}

	pool, ok := a.selectedPrivatePool()
	if !ok || pool.Controller == nil {
		return nil, "", false
	}

	// Without workspace storage the request cannot run on the private pool
	// at all. With it, the request stays unless the pool has no machine with
	// room for it.
	if a.request.StorageAvailable() {
		size := a.scheduler.workerSizeFor(pool, a.request)
		canAdd, err := pool.Controller.CanAddWorker(size.cpu, size.memory, size.gpuCount)
		if err != nil {
			requestLog(log.Debug(), a.request).
				Str("pool_name", pool.Name).
				Err(err).
				Msg("unable to check private pool capacity for fallback")
			return nil, "", false
		}
		if canAdd {
			return nil, "", false
		}
	}

	fallback := a.request.Clone()
	fallback.PoolSelector = ""
	return fallback, pool.Name, true
}

func (a *schedulingAttempt) hasManagedFallbackPath() bool {
	if worker, err := a.scheduler.selectWorkerFromWorkers(a.workers, a.request); err == nil && worker != nil {
		return true
	}
	if worker, err := a.scheduler.selectWorkerFromWorkersByStatus(a.workers, a.request, types.WorkerStatusPending); err == nil && worker != nil {
		return true
	}
	return a.canGrow()
}

func (a *schedulingAttempt) privatePoolAllowsManagedFallback() bool {
	stubConfig, err := a.request.Stub.UnmarshalConfig()
	if err != nil || stubConfig == nil || stubConfig.Pool == nil {
		return true
	}
	fallback := stubConfig.Pool.Fallback
	return fallback == "" || fallback == types.PrivatePoolFallbackInternal
}

func (a *schedulingAttempt) reserveManagedFallbackQuota() error {
	quota, err := a.scheduler.managedConcurrencyLimit(a.request)
	if err != nil {
		return err
	}
	return a.scheduler.containerRepo.ReserveContainerConcurrencyForPending(quota, a.request)
}

func (a *schedulingAttempt) selectedPrivatePool() (*WorkerPool, bool) {
	if a.request.PoolSelector == "" || a.scheduler.workerPoolManager == nil {
		return nil, false
	}

	pool, ok := a.scheduler.privateAgentPool(a.request.WorkspaceId, a.request.PoolSelector)
	if !ok {
		return nil, false
	}
	return pool, true
}
