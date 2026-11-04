package worker

import (
	"context"
	"errors"
	"os"
	"sync"

	"github.com/beam-cloud/beta9/pkg/types"
)

// Memory can be captured after the disk pivot is durable while its immutable
// layers upload. Keep the first rollout narrow: terminal native snapshots with
// exactly one writable qcow disk. Other checkpoint kinds retain the serial path.
func checkpointDiskPipelineEnabled(request *types.ContainerRequest, runtimeName string, terminal bool) bool {
	if os.Getenv("BETA9_CHECKPOINT_DISK_PIPELINE") != "1" || !terminal || runtimeName != types.ContainerRuntimeMicroVM.String() || request == nil {
		return false
	}
	writable := 0
	for i := range request.Mounts {
		mount := &request.Mounts[i]
		if mount.DurableDisk == nil || mount.ReadOnly {
			continue
		}
		if !isQcowDurableDiskMount(mount) {
			return false
		}
		writable++
	}
	return writable == 1
}

type checkpointDiskPublication struct {
	sealed, done chan struct{}
	cancel       context.CancelFunc
	snapshots    []*types.DiskSnapshot
	err          error
}

func startCheckpointDiskPublication(ctx context.Context, publish func(context.Context, func()) ([]*types.DiskSnapshot, error)) *checkpointDiskPublication {
	ctx, cancel := context.WithCancel(ctx)
	p := &checkpointDiskPublication{sealed: make(chan struct{}), done: make(chan struct{}), cancel: cancel}
	onSealed := sync.OnceFunc(func() { close(p.sealed) })
	go func() {
		defer close(p.done)
		p.snapshots, p.err = publish(ctx, onSealed)
	}()
	return p
}

func (p *checkpointDiskPublication) waitSealed(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-p.sealed:
		return ctx.Err()
	case <-p.done:
		if p.err != nil {
			return p.err
		}
		select {
		case <-p.sealed:
			return ctx.Err()
		default:
			return errors.New("disk publication completed without a seal fence")
		}
	}
}

func (p *checkpointDiskPublication) wait(ctx context.Context) ([]*types.DiskSnapshot, error) {
	select {
	case <-ctx.Done():
		p.cancel()
		<-p.done // Never detach or release a disk while publication still uses it.
		return p.snapshots, errors.Join(ctx.Err(), p.err)
	case <-p.done:
		return p.snapshots, p.err
	}
}

func (p *checkpointDiskPublication) cancelAndWait() {
	p.cancel()
	<-p.done
}
