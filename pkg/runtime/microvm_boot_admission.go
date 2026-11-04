package runtime

import (
	"context"
	"os"
	"strconv"
	"sync"

	"golang.org/x/sync/semaphore"
)

// Bound overlapping cold boots through guest-control connection. Preparation
// can finish ahead of admission, and authenticated exec readiness is unchanged.
type microVMBootAdmission struct {
	capacity int64
	slots    *semaphore.Weighted
}

func newMicroVMBootAdmission() *microVMBootAdmission {
	limit, err := strconv.Atoi(os.Getenv("BETA9_MICROVM_BOOT_CONCURRENCY"))
	if err != nil || limit < 1 || limit > 256 {
		return nil
	}
	return &microVMBootAdmission{int64(limit), semaphore.NewWeighted(int64(limit))}
}

func (a *microVMBootAdmission) acquire(ctx context.Context, vcpus int, ready <-chan struct{}) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if a == nil {
		return func() {}, nil
	}
	cost := min(max(int64(vcpus), 1), a.capacity)
	if err := a.slots.Acquire(ctx, cost); err != nil {
		return nil, err
	}
	done := make(chan struct{})
	release := sync.OnceFunc(func() { a.slots.Release(cost); close(done) })
	if err := ctx.Err(); err != nil {
		release()
		return nil, err
	}
	go func() {
		select {
		case <-ready:
			release()
		case <-done:
		}
	}()
	return release, nil
}
