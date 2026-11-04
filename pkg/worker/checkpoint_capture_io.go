package worker

import (
	"context"
	"os"
	"strconv"
	"sync"

	"github.com/beam-cloud/beta9/pkg/types"
)

var microVMSnapshotIOSlots = newMicroVMSnapshotIOSlots(os.Getenv("BETA9_MICROVM_SNAPSHOT_IO_CONCURRENCY"))

func newMicroVMSnapshotIOSlots(value string) chan struct{} {
	n, err := strconv.Atoi(value)
	if err != nil || n < 1 || n > 128 {
		return nil
	}
	return make(chan struct{}, n)
}

// The existing capture lease bounds all unarchived checkpoints. This separate
// gate limits pivots and memory writes after the guest has paused. Queued
// guests must stop using CPU before compression of earlier captures begins.
// Release after capture joins so compression can overlap the next capture.
func withCheckpointCaptureIO(ctx context.Context, runtimeName string, slots chan struct{}, capture func(admit func(context.Context) error) (string, error)) (string, error) {
	if runtimeName != types.ContainerRuntimeMicroVM.String() || slots == nil {
		return capture(func(context.Context) error { return nil })
	}
	var once sync.Once
	release := func() {}
	var admissionErr error
	admit := func(pausedCtx context.Context) error {
		once.Do(func() {

			var slotRelease func()
			slotRelease, admissionErr = acquireCheckpointSlot(pausedCtx, slots)
			if admissionErr == nil {
				release = slotRelease
			}
		})
		return admissionErr
	}
	defer func() { release() }()
	return capture(admit)
}
