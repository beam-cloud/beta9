package auth

import (
	"context"
	"testing"

	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

type workerContainerRepo struct {
	repository.ContainerRepository
	states []types.ContainerState
}

func (r workerContainerRepo) GetContainerState(containerID string) (*types.ContainerState, error) {
	for _, state := range r.states {
		if state.ContainerId == containerID {
			return &state, nil
		}
	}
	return nil, &types.ErrContainerStateNotFound{ContainerId: containerID}
}

func (r workerContainerRepo) GetActiveContainersByWorkerId(workerID string) ([]types.ContainerState, error) {
	var states []types.ContainerState
	for _, state := range r.states {
		if state.WorkerId == workerID {
			states = append(states, state)
		}
	}
	return states, nil
}

func TestWorkerFromAuthInfoRequiresABoundWorkerToken(t *testing.T) {
	workspace := &types.Workspace{Id: 1, ExternalId: "own"}
	for _, tc := range []struct {
		name      string
		authInfo  *AuthInfo
		trusted   bool
		permitted bool
	}{
		{name: "no auth"},
		{name: "workspace token", authInfo: &AuthInfo{Token: &types.Token{TokenType: types.TokenTypeWorkspace, WorkerID: "worker"}, Workspace: workspace}},
		{name: "unbound worker token", authInfo: &AuthInfo{Token: &types.Token{TokenType: types.TokenTypeWorker}, Workspace: workspace}},
		{name: "private worker without a workspace", authInfo: &AuthInfo{Token: &types.Token{TokenType: types.TokenTypeWorkerPrivate, WorkerID: "worker"}}},
		{name: "private worker", authInfo: &AuthInfo{Token: &types.Token{TokenType: types.TokenTypeWorkerPrivate, WorkerID: "worker"}, Workspace: workspace}, permitted: true},
		{name: "cluster worker", authInfo: &AuthInfo{Token: &types.Token{TokenType: types.TokenTypeWorker, WorkerID: "worker"}}, permitted: true, trusted: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			worker, err := WorkerFromContext(ContextWithAuthInfo(context.Background(), tc.authInfo))
			if !tc.permitted {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, "worker", worker.ID)
			require.Equal(t, tc.trusted, worker.Trusted)
		})
	}
}

func TestUntrustedWorkersActOnlyInTheirWorkspace(t *testing.T) {
	repo := workerContainerRepo{states: []types.ContainerState{
		{ContainerId: "own", StubId: "stub", WorkerId: "worker", WorkspaceId: "own"},
		{ContainerId: "other-workspace", StubId: "other-stub", WorkerId: "worker", WorkspaceId: "other"},
		{ContainerId: "other-worker", StubId: "stub", WorkerId: "other-worker", WorkspaceId: "own"},
	}}
	workspace := &types.Workspace{Id: 1, ExternalId: "own"}
	private := &Worker{ID: "worker", Token: &types.Token{TokenType: types.TokenTypeWorkerPrivate}, Workspace: workspace}
	cluster := &Worker{ID: "worker", Trusted: true, Token: &types.Token{TokenType: types.TokenTypeWorker}, Workspace: workspace}

	require.True(t, private.InWorkspace("own"))
	require.False(t, private.InWorkspace("other"))
	require.False(t, private.InWorkspace(""))
	require.True(t, private.InWorkspaceID(1))
	require.False(t, private.InWorkspaceID(2))
	require.True(t, cluster.InWorkspace("other"))
	require.False(t, cluster.InWorkspace(""))
	require.True(t, cluster.InWorkspaceID(2))

	for worker, owned := range map[*Worker][]string{private: {"own"}, cluster: {"own", "other-workspace"}} {
		for _, containerID := range []string{"own", "other-workspace", "other-worker", "missing", ""} {
			_, err := worker.Container(repo, containerID)
			require.Equal(t, contains(owned, containerID), err == nil, "trusted=%t %s", worker.Trusted, containerID)
		}
		var ids []string
		for _, state := range worker.Containers(repo, "") {
			ids = append(ids, state.ContainerId)
		}
		require.Equal(t, owned, ids)
	}

	_, err := private.Container(repo, "missing")
	require.True(t, (&types.ErrContainerStateNotFound{}).From(err))
	require.True(t, private.RunsStub(repo, "stub"))
	require.False(t, private.RunsStub(repo, "other-stub"))
	require.False(t, private.RunsStub(repo, ""))
	require.True(t, cluster.RunsStub(repo, "other-stub"))
}

func contains(values []string, value string) bool {
	for _, v := range values {
		if v == value {
			return true
		}
	}
	return false
}
