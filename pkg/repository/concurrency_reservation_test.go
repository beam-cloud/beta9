package repository

import (
	"context"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestReservationStoresOnlyWhatIsReadBack(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	require.NoError(t, err)
	repo := NewContainerRedisRepositoryForTest(rdb).(*ContainerRedisRepository)
	ctx := context.Background()

	request := testContainerRequest("sandbox-test-stub-reserved", "test-workspace", 100)
	require.NoError(t, repo.CreateContainerStateWithConcurrencyLimit(&types.ConcurrencyLimit{CPUMillicoreLimit: 1_000}, request))

	// The workspace and container ids are in the key and are not repeated.
	reservationKey := common.RedisKeys.WorkspaceConcurrencyLimitReservation(request.WorkspaceId, request.ContainerId)
	fields, err := rdb.HKeys(ctx, reservationKey).Result()
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"gpu_count", "cpu", "created_at"}, fields)

	// A rebuild of the workspace's counter writes the reservation again.
	require.NoError(t, repo.rebuildWorkspaceConcurrencyCounter(ctx, request.WorkspaceId))
	fields, err = rdb.HKeys(ctx, reservationKey).Result()
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"gpu_count", "cpu", "created_at"}, fields)
}

func TestRebuildKeepsARunningContainersReservationWithOrWithoutStoredIds(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	require.NoError(t, err)
	repo := NewContainerRedisRepositoryForTest(rdb).(*ContainerRedisRepository)
	ctx := context.Background()
	const workspaceID = "test-workspace"

	// Every reservation here was made an hour ago, half of them by an older
	// gateway that also stored the ids in the reservation.
	reserve := func(containerID string, storeIds bool) {
		key := common.RedisKeys.WorkspaceConcurrencyLimitReservation(workspaceID, containerID)
		require.NoError(t, rdb.HSet(ctx, key, "gpu_count", 0, "cpu", 100, "created_at", time.Now().Add(-time.Hour).Unix()).Err())
		if storeIds {
			require.NoError(t, rdb.HSet(ctx, key, "workspace_id", workspaceID, "container_id", containerID).Err())
		}
		require.NoError(t, rdb.SAdd(ctx, common.RedisKeys.WorkspaceConcurrencyLimitReservationIndex(workspaceID), containerID).Err())
	}
	running := []string{"sandbox-running", "sandbox-running-older-gateway"}
	for i, containerID := range running {
		require.NoError(t, repo.SetContainerState(containerID, &types.ContainerState{
			ContainerId: containerID,
			StubId:      "test-stub",
			WorkspaceId: workspaceID,
			Status:      types.ContainerStatusRunning,
			Cpu:         100,
		}))
		reserve(containerID, i == 1)
	}
	// These two containers' state expired.
	gone := []string{"sandbox-gone", "sandbox-gone-older-gateway"}
	for i, containerID := range gone {
		reserve(containerID, i == 1)
	}

	require.NoError(t, repo.rebuildWorkspaceConcurrencyCounter(ctx, workspaceID))

	used, err := rdb.HGet(ctx, common.RedisKeys.WorkspaceConcurrencyLimitUsage(workspaceID), "cpu").Int64()
	require.NoError(t, err)
	require.EqualValues(t, 200, used)
	reserved, err := rdb.SMembers(ctx, common.RedisKeys.WorkspaceConcurrencyLimitReservationIndex(workspaceID)).Result()
	require.NoError(t, err)
	require.ElementsMatch(t, running, reserved)
	for _, containerID := range running {
		exists, err := rdb.Exists(ctx, common.RedisKeys.WorkspaceConcurrencyLimitReservation(workspaceID, containerID)).Result()
		require.NoError(t, err)
		require.EqualValues(t, 1, exists, "a running container lost its reservation: %s", containerID)
	}
	for _, containerID := range gone {
		exists, err := rdb.Exists(ctx, common.RedisKeys.WorkspaceConcurrencyLimitReservation(workspaceID, containerID)).Result()
		require.NoError(t, err)
		require.Zero(t, exists, "a container that is gone kept its reservation: %s", containerID)
	}
}
