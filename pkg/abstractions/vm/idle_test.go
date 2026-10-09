package vm

import (
	"context"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/stretchr/testify/require"
)

func TestIdleExpirySnapshotsShutsDownAndColdRestarts(t *testing.T) {
	s, v, info, runtime, gateway := fixture()
	v.TokenID, v.Spec.IdleTimeout = info.Token.ExternalId, 30
	v.LastActiveAt = time.Now().Add(-time.Minute)
	s.urls(v)
	url, oldRuntime := v.DesktopURL, v.ContainerID
	ctx := context.Background()
	require.NoError(t, s.reconcileVM(ctx, v))
	require.Equal(t, "stopped", v.DesiredState)
	require.Equal(t, "stopped", v.Status)
	require.Empty(t, v.ContainerID)
	require.Equal(t, 1, runtime.snapshots)
	require.Equal(t, 1, gateway.stops)
	require.Equal(t, "root-after-shutdown", v.RootSnapshotID)
	require.NoError(t, s.reconcileVM(ctx, v))
	require.Empty(t, runtime.requests, "expired VMs must stay asleep")
	require.NoError(t, s.activate(auth.ContextWithAuthInfo(ctx, info), info, v))
	require.NotEqual(t, oldRuntime, v.ContainerID)
	require.Equal(t, "running", v.DesiredState)
	require.Empty(t, runtime.checkpoint, "default TTL resumes from disk")
	s.urls(v)
	require.Equal(t, url, v.DesktopURL)
}

func TestIdleStopIsOptionalAndActivityFencesExpiry(t *testing.T) {
	for _, active := range []bool{false, true} {
		t.Run(map[bool]string{false: "no TTL", true: "new activity"}[active], func(t *testing.T) {
			s, v, info, runtime, gateway := fixture()
			v.TokenID = info.Token.ExternalId
			v.UpdatedAt = time.Now()
			v.LastActiveAt = time.Now().Add(-time.Hour)
			if active {
				v.Spec.IdleTimeout = 30
				stored := *v
				stored.LastActiveAt = time.Now()
				s.repo.(*vmStore).rows[v.ID] = &stored
			}
			require.NoError(t, s.reconcileVM(context.Background(), v))
			require.Equal(t, "running", v.DesiredState)
			require.Zero(t, gateway.stops)
			require.Zero(t, runtime.snapshots)
		})
	}
}

func TestIdleSnapshotFailureRetainsComputeAndRetriesShutdown(t *testing.T) {
	s, v, info, runtime, gateway := fixture()
	v.TokenID, v.Spec.IdleTimeout = info.Token.ExternalId, 30
	v.LastActiveAt = time.Now().Add(-time.Minute)
	runtime.snapshotError = true
	ctx := context.Background()
	err := s.reconcileVM(ctx, v)
	require.ErrorContains(t, err, "upload failed")
	s.failed(ctx, v, err)
	require.Zero(t, gateway.stops, "failed durability must not release compute")
	require.NotEmpty(t, v.ContainerID)
	require.Equal(t, "stopped", v.DesiredState)
	runtime.snapshotError = false
	require.NoError(t, s.reconcileVM(ctx, v))
	require.Empty(t, v.ContainerID)
	require.Equal(t, "stopped", v.Status)
	require.Len(t, runtime.requests, 0)
	stored, err := s.repo.GetVM(ctx, info.Workspace.Id, v.ID)
	require.NoError(t, err)
	require.Equal(t, "root-after-shutdown", stored.RootSnapshotID)
}
