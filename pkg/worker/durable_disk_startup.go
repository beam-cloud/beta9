package worker

import (
	"context"
	"path/filepath"
	"sync"

	"github.com/beam-cloud/beta9/pkg/types"
)

type qcowRootPreparationKey struct{}

// A root's authoritative head read overlaps the delivery claim. Its host lock
// remains held until attachment, and neither credentials nor any disk writes
// are used before the claim and runtime validation succeed.
type qcowRootPreparation struct {
	ctx         context.Context
	cancel      context.CancelFunc
	done        chan struct{}
	claimed     chan struct{}
	attach      chan struct{}
	attachOnce  sync.Once
	request     *types.ContainerRequest
	mount       *types.Mount
	credentials *types.ContainerRequest
	claimErr    error
	err         error
}

func (s *Worker) startQcowRootPreparation(ctx context.Context, request *types.ContainerRequest) *qcowRootPreparation {
	root := qcowRootDiskMount(request)
	if !request.IsPersistentVM() || s.diskManager == nil || root == nil || root.LocalPath != filepath.Join(types.DefaultDurableDisksPath, request.Workspace.Name, types.SafeDurableDiskName(root.DurableDisk.Name)) {
		return nil
	}
	copy := request.Clone()
	mount := qcowRootDiskMount(copy)
	// The head read owns its disk metadata while the claim hydrates the request.
	disk := *mount.DurableDisk
	mount.DurableDisk = &disk
	if mount.DurableDisk.SourceSnapshotId == "" {
		mount.DurableDisk.SourceSnapshotId = durableDiskSourceSnapshotFromStub(copy, mount.DurableDisk.Name)
	}
	return newQcowRootPreparation(ctx, copy, mount, s.prepareQcowDurableDiskMountAfterHead)
}

func newQcowRootPreparation(ctx context.Context, request *types.ContainerRequest, mount *types.Mount,
	prepare func(context.Context, *types.ContainerRequest, *types.Mount, func() error) error,
) *qcowRootPreparation {
	ctx, cancel := context.WithCancel(ctx)
	p := &qcowRootPreparation{
		ctx:     ctx,
		cancel:  cancel,
		request: request,
		mount:   mount,
		done:    make(chan struct{}),
		claimed: make(chan struct{}),
		attach:  make(chan struct{}),
	}
	go func() {
		defer close(p.done)
		p.err = withDurableDiskLock(ctx, mount, func() error {
			return prepare(ctx, request, mount, p.waitForClaimAndValidation)
		})
	}()
	return p
}

func (p *qcowRootPreparation) withContext(ctx context.Context) context.Context {
	if p == nil {
		return ctx
	}

	return context.WithValue(ctx, qcowRootPreparationKey{}, p)
}

func (p *qcowRootPreparation) finishClaim(request *types.ContainerRequest, err error) {
	if p == nil {
		return
	}

	p.claimErr = err
	if err == nil {
		p.credentials = request.Clone()
	}
	close(p.claimed)
}

func (p *qcowRootPreparation) allowAttach() { p.attachOnce.Do(func() { close(p.attach) }) }

func (p *qcowRootPreparation) waitForClaimAndValidation() error {
	select {
	case <-p.ctx.Done():
		return p.ctx.Err()
	case <-p.claimed:
	}
	if p.claimErr != nil {
		return p.claimErr
	}
	select {
	case <-p.ctx.Done():
		return p.ctx.Err()
	case <-p.attach:
	}
	if err := p.ctx.Err(); err != nil {
		return err
	}
	// The clone owns its environment and disk metadata. Channel publication
	// synchronizes credential hydration without racing other startup tasks.
	p.request.Env = append([]string(nil), p.credentials.Env...)
	p.request.Workspace = p.credentials.Workspace
	return nil
}

func (p *qcowRootPreparation) matches(mount *types.Mount) bool {
	return isQcowRootDiskMount(mount) && mount.LocalPath == p.mount.LocalPath &&
		mount.DurableDisk.Name == p.mount.DurableDisk.Name
}

func (p *qcowRootPreparation) wait(ctx context.Context, request *types.ContainerRequest) error {
	select {
	case <-ctx.Done():
		p.cancel()
		<-p.done
		return ctx.Err()
	case <-p.done:
		if p.err == nil {
			qcowRootDiskMount(request).DurableDisk.SourceSnapshotId = p.mount.DurableDisk.SourceSnapshotId
		}
		return p.err
	}
}

func (p *qcowRootPreparation) close() {
	if p == nil {
		return
	}

	p.cancel()
	<-p.done
}
