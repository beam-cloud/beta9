package repository

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/redis/go-redis/v9"
)

const (
	// The most containers the shared loop reads per workerAddressPollInterval.
	// With more than this waiting, the interval grows in proportion, so what
	// the waiting costs Redis stays flat however large a burst is.
	workerAddressesPerPoll   = 1000
	workerAddressReadTimeout = 5 * time.Second
)

// workerAddressWaits serves every caller in this process that is waiting for
// a container to be given a worker.
//
// Each caller used to poll for its own container: two reads every 25 ms.
// Many callers waiting together cost more round trips than placing their
// containers does, and the slower Redis answered the longer they all waited.
// One loop now reads what all of them are waiting for in a single round trip.
type workerAddressWaits struct {
	mu      sync.Mutex
	waiters map[string][]chan workerAddressResult
	running bool
}

type workerAddressResult struct {
	address string
	err     error
}

// wait registers a caller that is waiting for a container's worker address.
// The channel receives one result: the address, or why there will not be one.
// The caller calls the returned function when it stops waiting.
func (w *workerAddressWaits) wait(rdb *common.RedisClient, containerId string) (<-chan workerAddressResult, func()) {
	result := make(chan workerAddressResult, 1)

	w.mu.Lock()
	if w.waiters == nil {
		w.waiters = map[string][]chan workerAddressResult{}
	}
	w.waiters[containerId] = append(w.waiters[containerId], result)
	if !w.running {
		w.running = true
		go w.run(rdb)
	}
	w.mu.Unlock()

	return result, func() { w.forget(containerId, result) }
}

func (w *workerAddressWaits) forget(containerId string, result chan workerAddressResult) {
	w.mu.Lock()
	defer w.mu.Unlock()

	waiting := w.waiters[containerId]
	for i, candidate := range waiting {
		if candidate == result {
			waiting = append(waiting[:i], waiting[i+1:]...)
			break
		}
	}
	if len(waiting) == 0 {
		delete(w.waiters, containerId)
		return
	}
	w.waiters[containerId] = waiting
}

// settle gives a result to everyone waiting for a container. A channel is
// sent to once, because settling removes it, so the send never blocks.
func (w *workerAddressWaits) settle(containerId string, result workerAddressResult) {
	w.mu.Lock()
	waiting := w.waiters[containerId]
	delete(w.waiters, containerId)
	w.mu.Unlock()

	for _, caller := range waiting {
		caller <- result
	}
}

// waiting is how many containers have a caller waiting for them.
func (w *workerAddressWaits) waiting() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.waiters)
}

// run reads the addresses callers are waiting for until nobody is waiting.
func (w *workerAddressWaits) run(rdb *common.RedisClient) {
	for {
		time.Sleep(workerAddressReadInterval(w.waiting()))

		w.mu.Lock()
		if len(w.waiters) == 0 {
			w.running = false
			w.mu.Unlock()
			return
		}
		containerIds := make([]string, 0, len(w.waiters))
		for containerId := range w.waiters {
			containerIds = append(containerIds, containerId)
		}
		w.mu.Unlock()

		w.read(rdb, containerIds)
	}
}

// read looks up, in one round trip, the worker address of each container and
// whether the scheduler has given up on it.
func (w *workerAddressWaits) read(rdb *common.RedisClient, containerIds []string) {
	ctx, cancel := context.WithTimeout(context.Background(), workerAddressReadTimeout)
	defer cancel()

	pipe := rdb.Pipeline()
	addresses := make([]*redis.StringCmd, len(containerIds))
	statuses := make([]*redis.StringCmd, len(containerIds))
	for i, containerId := range containerIds {
		addresses[i] = pipe.Get(ctx, common.RedisKeys.SchedulerWorkerAddress(containerId))
		statuses[i] = pipe.Get(ctx, common.RedisKeys.SchedulerContainerRequestStatus(containerId))
	}
	// Exec reports the first command that failed, and a container without an
	// address yet is one of those, so each result is checked on its own.
	_, _ = pipe.Exec(ctx)

	for i, containerId := range containerIds {
		address, err := addresses[i].Result()
		switch {
		case err == nil:
			w.settle(containerId, workerAddressResult{address: address})
		case err != redis.Nil:
			w.settle(containerId, workerAddressResult{err: fmt.Errorf("failed to get worker addr for container %s: %w", containerId, err)})
		case statuses[i].Val() == string(types.ContainerRequestStatusFailed):
			w.settle(containerId, workerAddressResult{err: fmt.Errorf("failed to schedule container, container id: %s", containerId)})
		}
	}
}

// workerAddressReadInterval is how long the loop waits between reads: the
// interval a caller used to poll at on its own, stretched once more than
// workerAddressesPerPoll containers are being waited for.
func workerAddressReadInterval(waiting int) time.Duration {
	return workerAddressPollInterval * time.Duration(max(1, (waiting+workerAddressesPerPoll-1)/workerAddressesPerPoll))
}
