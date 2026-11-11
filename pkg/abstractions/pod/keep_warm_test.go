package pod

import (
	"context"
	"testing"

	abstractions "github.com/beam-cloud/beta9/pkg/abstractions/common"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestStoppedNeverExpiringSandboxLeavesNoKeepWarmLock(t *testing.T) {
	rdb, err := repository.NewRedisClientForTest()
	require.NoError(t, err)
	repo := repository.NewContainerRedisRepositoryForTest(rdb)
	ctx := context.Background()

	instance := &podInstance{AutoscaledInstance: &abstractions.AutoscaledInstance{
		Ctx:                ctx,
		ContainerEventChan: make(chan types.ContainerEvent, 4),
		Workspace:          &types.Workspace{Name: "workspace", ExternalId: "workspace-id"},
		Stub:               &types.StubWithRelated{Stub: types.Stub{ExternalId: "stub", Type: types.StubType(types.StubTypeSandbox)}},
		StubConfig:         &types.StubConfigV1{KeepWarmSeconds: -1},
		ContainerRepo:      repo,
	}}

	// Two sandboxes that never expire: their locks are stored without a TTL.
	for _, containerID := range []string{"sandbox-stub-stopped", "sandbox-stub-running"} {
		require.NoError(t, repo.SetPodKeepWarmLock(ctx, "workspace", "stub", containerID, -1))
		require.NoError(t, repo.SetContainerState(containerID, &types.ContainerState{
			ContainerId: containerID,
			StubId:      "stub",
			WorkspaceId: "workspace-id",
			Status:      types.ContainerStatusRunning,
		}))
	}

	// One is stopped. The gateway sees its state go and tells the instance.
	require.NoError(t, repo.DeleteContainerState("sandbox-stub-stopped"))
	instance.ConsumeContainerEvent(types.ContainerEvent{ContainerId: "sandbox-stub-stopped", Change: -1})
	// The other only had its state written again.
	instance.ConsumeContainerEvent(types.ContainerEvent{ContainerId: "sandbox-stub-running", Change: 1})

	stopped, err := repo.PodKeepWarmLockExists(ctx, "workspace", "stub", "sandbox-stub-stopped")
	require.NoError(t, err)
	require.False(t, stopped, "the stopped sandbox's keep-warm lock was left behind")

	running, err := repo.PodKeepWarmLockExists(ctx, "workspace", "stub", "sandbox-stub-running")
	require.NoError(t, err)
	require.True(t, running, "the running sandbox lost its keep-warm lock")
}
