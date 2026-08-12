package scheduler

import (
	"sync"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
)

const (
	workerProvisioningBackoffDuration = 5 * time.Second
	workerProvisioningBackoffMaxWait  = 5 * time.Second
)

type workerProvisioningBackoff struct {
	mu        sync.Mutex
	expiresAt map[string]time.Time
}

func newWorkerProvisioningBackoff() *workerProvisioningBackoff {
	return &workerProvisioningBackoff{
		expiresAt: map[string]time.Time{},
	}
}

func (s *Scheduler) workerProvisioningControllerForRequest(controllers []WorkerPoolController, request *types.ContainerRequest) (WorkerPoolController, time.Duration, error) {
	if s == nil {
		return firstController(controllers), provisioningWorkerRequeueDelay, nil
	}

	var poolNames []string
	var capacityErr error
	for _, controller := range controllers {
		if controller == nil {
			continue
		}
		if pool, ok := s.poolForController(controller); ok && !workerPoolSupportsProvisioning(pool.Config) {
			continue
		}
		if request != nil {
			if checker, ok := controller.(workerPoolCapacityChecker); ok {
				hasCapacity, err := checker.HasWorkerCapacity(
					s.workerCPUForControllerRequest(controller, request),
					s.workerMemoryForControllerRequest(controller, request),
					s.workerGPUCountForControllerRequest(controller, request),
				)
				if err != nil {
					if capacityErr == nil {
						capacityErr = err
					}
					continue
				}
				if !hasCapacity {
					continue
				}
			}
		}
		poolNames = append(poolNames, controller.Name())
		if s.workerProvisioningBackoff == nil || s.workerProvisioningBackoff.canAttempt(controller.Name()) {
			return controller, provisioningWorkerRequeueDelay, nil
		}
	}

	if capacityErr != nil {
		return nil, provisioningWorkerRequeueDelay, capacityErr
	}
	if s.workerProvisioningBackoff == nil {
		return nil, provisioningWorkerRequeueDelay, nil
	}
	return nil, s.workerProvisioningBackoff.nextDelay(poolNames), nil
}

func firstController(controllers []WorkerPoolController) WorkerPoolController {
	for _, controller := range controllers {
		if controller != nil {
			return controller
		}
	}
	return nil
}

func (s *Scheduler) recordWorkerProvisioningFailure(controller WorkerPoolController, err error) {
	if s == nil || controller == nil || s.workerProvisioningBackoff == nil {
		return
	}
	s.workerProvisioningBackoff.record(controller.Name(), err)
}

func (b *workerProvisioningBackoff) record(poolName string, err error) {
	if b == nil || poolName == "" || err == nil {
		return
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	b.ensureMap()
	b.expiresAt[poolName] = time.Now().Add(workerProvisioningBackoffDuration)
}

func (b *workerProvisioningBackoff) canAttempt(poolName string) bool {
	if b == nil || poolName == "" {
		return true
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	b.ensureMap()

	expiresAt, ok := b.expiresAt[poolName]
	if !ok {
		return true
	}
	if time.Now().After(expiresAt) {
		delete(b.expiresAt, poolName)
		return true
	}
	return false
}

func (b *workerProvisioningBackoff) nextDelay(poolNames []string) time.Duration {
	if b == nil || len(poolNames) == 0 {
		return provisioningWorkerRequeueDelay
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	b.ensureMap()

	now := time.Now()
	var minDelay time.Duration
	for _, poolName := range poolNames {
		expiresAt, ok := b.expiresAt[poolName]
		if !ok {
			return provisioningWorkerRequeueDelay
		}
		if now.After(expiresAt) {
			delete(b.expiresAt, poolName)
			return provisioningWorkerRequeueDelay
		}

		delay := expiresAt.Sub(now)
		if minDelay == 0 || delay < minDelay {
			minDelay = delay
		}
	}

	if minDelay == 0 {
		return provisioningWorkerRequeueDelay
	}
	if minDelay > workerProvisioningBackoffMaxWait {
		return workerProvisioningBackoffMaxWait
	}
	return minDelay
}

func (b *workerProvisioningBackoff) ensureMap() {
	if b.expiresAt == nil {
		b.expiresAt = map[string]time.Time{}
	}
}
