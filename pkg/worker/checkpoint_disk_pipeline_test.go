package worker

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/disk"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestCheckpointDiskPipelineSealPrecedesCaptureButPublicationStillFencesAvailability(t *testing.T) {
	sealed, originAck := make(chan struct{}), make(chan struct{})
	volume := &backloggedQcowVolume{writes: 1, depth: 1}
	want := &types.DiskSnapshot{ExternalId: "durable-generation"}
	p := startCheckpointDiskPublication(context.Background(), func(ctx context.Context, onSealed func()) ([]*types.DiskSnapshot, error) {
		err := snapshotQcowVolumeWithSeal(ctx, volume, durableDiskSyncMemoryCheckpoint, false, func([]disk.SealedLayer) error {
			close(sealed)
			select {
			case <-originAck:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}, onSealed)
		return []*types.DiskSnapshot{want}, err
	})
	defer p.cancelAndWait()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, p.waitSealed(ctx), "capture should proceed before origin acknowledges")
	<-sealed
	require.Equal(t, []string{"seal"}, volume.events)
	finished := make(chan error, 1)
	go func() {
		rows, err := p.wait(ctx)
		if len(rows) != 1 || rows[0] != want {
			err = errors.Join(err, errors.New("lost paired disk generation"))
		}
		finished <- err
	}()
	select {
	case <-finished:
		t.Fatal("availability outran disk origin acknowledgement")
	case <-time.After(20 * time.Millisecond):
	}
	close(originAck)
	require.NoError(t, <-finished)
}

func TestCheckpointDiskPipelineFailuresAndCancellationJoinPublisher(t *testing.T) {
	sealFailure := errors.New("disk pivot failed")
	p := startCheckpointDiskPublication(context.Background(), func(context.Context, func()) ([]*types.DiskSnapshot, error) {
		return nil, sealFailure
	})
	require.ErrorIs(t, p.waitSealed(context.Background()), sealFailure)
	p.cancelAndWait()
	joined := make(chan struct{})
	p = startCheckpointDiskPublication(context.Background(), func(ctx context.Context, onSealed func()) ([]*types.DiskSnapshot, error) {
		onSealed()
		<-ctx.Done()
		defer close(joined)
		return nil, ctx.Err()
	})
	require.NoError(t, p.waitSealed(context.Background()))
	p.cancelAndWait()
	select {
	case <-joined:
	default:
		t.Fatal("cancellation returned before the publisher released its disk")
	}
	_, err := p.wait(context.Background())
	require.ErrorIs(t, err, context.Canceled)
}

func TestCheckpointDiskPipelineRejectsMissingSealAndLateOriginFailure(t *testing.T) {
	for _, onSealed := range []bool{false, true} {
		originFailure := errors.New("disk upload failed")
		p := startCheckpointDiskPublication(context.Background(), func(_ context.Context, seal func()) ([]*types.DiskSnapshot, error) {
			if onSealed {
				seal()
				return nil, originFailure
			}
			return nil, nil
		})
		if onSealed {
			_, err := p.wait(context.Background())
			require.ErrorIs(t, err, originFailure)
		} else {
			require.ErrorContains(t, p.waitSealed(context.Background()), "without a seal fence")
		}
		p.cancelAndWait()
	}
}

func TestCheckpointDiskPipelineEligibility(t *testing.T) {
	t.Setenv("BETA9_CHECKPOINT_DISK_PIPELINE", "1")
	request := &types.ContainerRequest{Mounts: []types.Mount{{DurableDisk: &types.DurableDiskMountConfig{Driver: types.DurableDiskDriverQcow}}}}
	native := types.ContainerRuntimeMicroVM.String()
	require.True(t, checkpointDiskPipelineEnabled(request, native, true))
	require.False(t, checkpointDiskPipelineEnabled(request, native, false))
	require.False(t, checkpointDiskPipelineEnabled(request, "runc", true))
	request.Mounts = append(request.Mounts, request.Mounts[0])
	require.False(t, checkpointDiskPipelineEnabled(request, native, true), "multiple disk fences require a separate implementation")
	t.Setenv("BETA9_CHECKPOINT_DISK_PIPELINE", "0")
	request.Mounts = request.Mounts[:1]
	require.False(t, checkpointDiskPipelineEnabled(request, native, true))
}
