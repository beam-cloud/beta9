package pod

import (
	"context"
	"sync"
	"testing"
	"time"

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

	require.Eventually(t, func() bool {
		stopped, err := repo.PodKeepWarmLockExists(ctx, "workspace", "stub", "sandbox-stub-stopped")
		return err == nil && !stopped
	}, time.Second, 5*time.Millisecond, "the stopped sandbox's keep-warm lock was left behind")

	running, err := repo.PodKeepWarmLockExists(ctx, "workspace", "stub", "sandbox-stub-running")
	require.NoError(t, err)
	require.True(t, running, "the running sandbox lost its keep-warm lock")
}

// slowLockRepo holds every keep-warm lock delete until it is released.
type slowLockRepo struct {
	repository.ContainerRepository
	release chan struct{}
	mu      sync.Mutex
	deleted []string
}

func (r *slowLockRepo) SetPodKeepWarmLock(ctx context.Context, workspaceName, stubId, containerId string, keepWarmSeconds int) error {
	<-r.release
	r.mu.Lock()
	r.deleted = append(r.deleted, containerId)
	r.mu.Unlock()
	return nil
}

// The gateway hands an instance every container event of its kind from one
// goroutine, so an event for a container that is gone must not wait for Redis.
func TestAContainerThatIsGoneDoesNotHoldUpTheEventsBehindIt(t *testing.T) {
	repo := &slowLockRepo{release: make(chan struct{})}
	instance := &podInstance{AutoscaledInstance: &abstractions.AutoscaledInstance{
		Ctx:                context.Background(),
		ContainerEventChan: make(chan types.ContainerEvent, 8),
		Workspace:          &types.Workspace{Name: "workspace", ExternalId: "workspace-id"},
		Stub:               &types.StubWithRelated{Stub: types.Stub{ExternalId: "stub", Type: types.StubType(types.StubTypeSandbox)}},
		StubConfig:         &types.StubConfigV1{KeepWarmSeconds: -1},
		ContainerRepo:      repo,
	}}

	gone := []string{"sandbox-stub-a", "sandbox-stub-b", "sandbox-stub-c"}
	delivered := make(chan struct{})
	go func() {
		for _, containerID := range gone {
			instance.ConsumeContainerEvent(types.ContainerEvent{ContainerId: containerID, Change: -1})
		}
		close(delivered)
	}()

	// Redis has answered nothing yet, and every event was taken.
	select {
	case <-delivered:
	case <-time.After(time.Second):
		t.Fatal("an event waited for a keep-warm lock to be deleted")
	}

	// Every lock is still deleted, in the order the containers went.
	close(repo.release)
	require.Eventually(t, func() bool {
		repo.mu.Lock()
		defer repo.mu.Unlock()
		return len(repo.deleted) == len(gone)
	}, time.Second, 5*time.Millisecond)
	require.Equal(t, gone, repo.deleted)
}
