package worker

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

func rootPreparationRequest(t *testing.T) *types.ContainerRequest {
	t.Helper()
	return &types.ContainerRequest{UseVM: true, Env: []string{"BEAM_VM_SYSTEMD=1"}, Mounts: []types.Mount{{
		MountType: types.StorageModeDurableDisk, MountPath: "/", LocalPath: filepath.Join(t.TempDir(), "root"),
		DurableDisk: &types.DurableDiskMountConfig{Name: "root", Driver: "qcow"},
	}}}
}

func TestQcowRootPreparationReadsBeforeClaimAndRetainsDiskFence(t *testing.T) {
	request := rootPreparationRequest(t)
	mount := &request.Mounts[0]
	headRead, attached := make(chan struct{}), make(chan []string, 1)
	p := newQcowRootPreparation(context.Background(), request, mount,
		func(_ context.Context, request *types.ContainerRequest, _ *types.Mount, wait func() error) error {
			close(headRead)
			if err := wait(); err != nil {
				return err
			}
			attached <- request.Env
			return nil
		})
	t.Cleanup(p.close)
	select {
	case <-headRead:
	case <-time.After(time.Second):
		t.Fatal("head read waited for claim")
	}
	lock := NewFileLock(filepath.Join(filepath.Dir(mount.LocalPath), durableDiskLockDir, filepath.Base(mount.LocalPath)+".lock"))
	require.Error(t, lock.Acquire(), "the head read must retain its fence across the claim")
	hydrated := request.Clone()
	hydrated.Env = append(hydrated.Env, "BETA9_TOKEN=hydrated-token")
	p.finishClaim(hydrated, nil)
	select {
	case <-attached:
		t.Fatal("accepted delivery attached before runtime validation")
	default:
	}
	p.allowAttach()
	require.NoError(t, p.wait(context.Background(), request))
	require.Contains(t, <-attached, "BETA9_TOKEN=hydrated-token")
	require.NoError(t, lock.Acquire(), "attachment must release its fence")
	require.NoError(t, lock.Release())
}

func TestQcowRootPreparationNeverAttachesRejectedOrCancelledDelivery(t *testing.T) {
	for _, name := range []string{"claim rejected", "validation cancelled"} {
		t.Run(name, func(t *testing.T) {
			request := rootPreparationRequest(t)
			headRead := make(chan struct{})
			p := newQcowRootPreparation(context.Background(), request, &request.Mounts[0],
				func(_ context.Context, _ *types.ContainerRequest, _ *types.Mount, wait func() error) error {
					close(headRead)
					if err := wait(); err != nil {
						return err
					}
					t.Error("failed delivery attached its disk")
					return nil
				})
			t.Cleanup(p.close)
			<-headRead
			if name == "claim rejected" {
				p.finishClaim(request, errors.New("owned elsewhere"))
				p.allowAttach()
				require.ErrorContains(t, p.wait(context.Background(), request), "owned elsewhere")
			} else {
				p.finishClaim(request, nil)
				p.close()
				require.ErrorIs(t, p.wait(context.Background(), request), context.Canceled)
			}
		})
	}
}
