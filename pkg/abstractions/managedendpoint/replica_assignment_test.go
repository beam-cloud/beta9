package managedendpoint

import (
	"context"
	"testing"

	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestReplicaProtectionFollowsReassignedPendingContainer(t *testing.T) {
	s := newServiceForTest(t)
	s.containers = repository.NewContainerRedisRepositoryForTest(s.rdb)
	replica := seedReplica(t, s, seedEndpoint(t, s))
	replica.WorkerID, replica.MachineID, replica.ProviderWorkspaceID = "old-worker", "old-machine", "old-provider"
	replica.Address, replica.EngineReady = "stale-address", true
	replica.Status = types.ReplicaStatusReady
	for _, worker := range []string{"", "new-worker"} {
		require.NoError(t, s.containers.SetContainerState(replica.ContainerID, &types.ContainerState{
			ContainerId: replica.ContainerID, Status: types.ContainerStatusPending,
			WorkerId: worker, MachineId: "new-machine", Evictable: false,
		}))
		require.NoError(t, s.controller.syncReplica(context.Background(), replica))
		require.Equal(t, worker, replica.WorkerID)
		require.Equal(t, "new-machine", replica.MachineID)
		require.Empty(t, replica.ProviderWorkspaceID)
		require.Empty(t, replica.Address)
		require.True(t, replica.Protected)
		require.False(t, replica.EngineReady)
		require.Equal(t, types.ReplicaStatusScheduling, replica.Status)
	}
}
