package scheduler

import (
	"sync"
	"time"
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

func (s *Scheduler) workerProvisioningController(controllers []WorkerPoolController) (WorkerPoolController, time.Duration) {
	if s == nil || s.workerProvisioningBackoff == nil {
		return firstController(controllers), provisioningWorkerRequeueDelay
	}

	var poolNames []string
	for _, controller := range controllers {
		if controller == nil {
			continue
		}
		poolNames = append(poolNames, controller.Name())
		if s.workerProvisioningBackoff.canAttempt(controller.Name()) {
			return controller, provisioningWorkerRequeueDelay
		}
	}

	return nil, s.workerProvisioningBackoff.nextDelay(poolNames)
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
