package managedendpoint

import (
	"context"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEvictOrderPrefersPrefillThenServeThenDecode(t *testing.T) {
	assert.Less(t, evictOrder(types.ReplicaRolePrefill), evictOrder(types.ReplicaRoleServe))
	assert.Less(t, evictOrder(types.ReplicaRoleServe), evictOrder(types.ReplicaRoleDecode))
	assert.Equal(t, evictOrder(types.ReplicaRoleServe), evictOrder(""))
}

func TestSyncReplicaFollowsSchedulerEviction(t *testing.T) {
	s := newServiceForTest(t)
	s.containers = repository.NewContainerRedisRepositoryForTest(s.rdb)
	endpoint := seedEndpoint(t, s)
	replica := seedReplica(t, s, endpoint)
	replica.Status = types.ReplicaStatusReady
	replica.StartedAt = time.Now().Add(-time.Hour)
	ctx := context.Background()

	require.NoError(t, s.containers.SetContainerState(replica.ContainerID, &types.ContainerState{
		ContainerId: replica.ContainerID, Status: types.ContainerStatusRunning, WorkerId: "w1",
		Evictable: true, DrainSeconds: 30,
	}))
	// The scheduler picked this replica as a victim.
	require.NoError(t, s.rdb.HSet(ctx, common.RedisKeys.SchedulerContainerState(replica.ContainerID),
		"status", string(types.ContainerStatusStopping), "evicting", "true").Err())

	require.NoError(t, s.controller.syncReplica(ctx, replica))
	assert.Equal(t, types.ReplicaStatusEvicting, replica.Status)
	assert.WithinDuration(t, time.Now().Add(30*time.Second), replica.DrainDeadline, 5*time.Second)
	assert.Equal(t, "w1", replica.WorkerID)

	// Once the worker finishes the stop, the replica is recorded as evicted.
	require.NoError(t, s.containers.DeleteContainerState(replica.ContainerID))
	require.NoError(t, s.containers.SetContainerExitCode(replica.ContainerID, int(types.ContainerExitCodeEvicted)))
	require.NoError(t, s.controller.syncReplica(ctx, replica))
	assert.Equal(t, types.ReplicaStatusEvicted, replica.Status)
	backoff, err := s.repo.InScheduleBackoff(ctx, endpoint.Spec.ID, targetKey(replica.Role, replica.GPU))
	require.NoError(t, err)
	assert.False(t, backoff, "eviction is not a failure; the fill loop may retry immediately")
}

func TestExitStatusUsesEvictedExitCode(t *testing.T) {
	s := newServiceForTest(t)
	s.containers = repository.NewContainerRedisRepositoryForTest(s.rdb)
	endpoint := seedEndpoint(t, s)
	replica := seedReplica(t, s, endpoint)
	// Even a replica that was still loading (never Ready) counts as evicted
	// when the worker reports the eviction exit code.
	replica.Status = types.ReplicaStatusLoading
	replica.Protected = true
	require.NoError(t, s.containers.SetContainerExitCode(replica.ContainerID, int(types.ContainerExitCodeEvicted)))
	assert.Equal(t, types.ReplicaStatusEvicted, s.controller.exitStatus(replica))

	require.NoError(t, s.containers.SetContainerExitCode(replica.ContainerID, 1))
	assert.Equal(t, types.ReplicaStatusFailed, s.controller.exitStatus(replica))
}

func TestSyncReplicaBacksOffWhenOpportunisticPlacementFails(t *testing.T) {
	s := newServiceForTest(t)
	s.containers = repository.NewContainerRedisRepositoryForTest(s.rdb)
	endpoint := seedEndpoint(t, s)
	replica := seedReplica(t, s, endpoint)
	replica.StartedAt = time.Now().Add(-time.Minute)
	ctx := context.Background()

	// The scheduler failed the request fast: no state, failed request status.
	require.NoError(t, s.containers.SetContainerRequestStatus(replica.ContainerID, types.ContainerRequestStatusFailed))
	require.NoError(t, s.controller.syncReplica(ctx, replica))
	assert.Equal(t, types.ReplicaStatusFailed, replica.Status)
	assert.Equal(t, "not scheduled: no idle capacity", replica.StatusReason)
	backoff, err := s.repo.InScheduleBackoff(ctx, endpoint.Spec.ID, targetKey(replica.Role, replica.GPU))
	require.NoError(t, err)
	assert.True(t, backoff)
}
