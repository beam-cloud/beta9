package managedendpoint

import (
	"context"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func repoEndpoint(t *testing.T, path string, spec types.ManagedEndpointSpec) *pb.RepoEndpoint {
	t.Helper()
	return &pb.RepoEndpoint{Path: path, Id: spec.ID, SpecJson: mustJSON(spec), StubId: "stub-2", Version: 2}
}

func TestApplyRepoDryRunValidatesWithoutApplying(t *testing.T) {
	s := newServiceForTest(t)
	existing := seedEndpoint(t, s)
	other := existing.Spec
	other.ID = "acme/other"
	out, err := s.ApplyRepo(adminCtx(), &pb.ApplyRepoRequest{
		Sha: "abc", DryRun: true,
		ConfigYaml: "acme/model:\n  enabled: true\n  gpus:\n    A100:\n      priority: 1\nacme/ghost:\n  enabled: true\n  gpus: {}\n",
		Endpoints: []*pb.RepoEndpoint{
			repoEndpoint(t, "acme/model", existing.Spec),
			repoEndpoint(t, "acme/renamed", other),
			{Path: "acme/broken", Error: "ImportError: no module named vllm"},
		},
	})
	require.NoError(t, err)
	assert.False(t, out.Ok)
	assert.Equal(t, []string{
		`acme/renamed: id "acme/other" must equal its directory "acme/renamed"`,
		"acme/broken: ImportError: no module named vllm",
		"config.yaml: acme/ghost is not an app in the repo",
		`config.yaml: acme/model does not declare gpu "A100" in its app`,
	}, out.Errors)
	assert.Nil(t, out.State)

	state, err := s.repo.GetGitOpsState(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "abc", state.PendingSHA, "a dry run naming a commit announces a deploy")
	assert.Empty(t, state.LastSHA, "and applies nothing")
	fleet, err := s.repo.GetFleet(context.Background())
	require.NoError(t, err)
	assert.Contains(t, fleet.Endpoints, "acme/model")
	assert.Equal(t, "fleet-sha", fleet.GitSHA)
}

func TestApplyRepoAppliesFleetStampsShaAndRetires(t *testing.T) {
	s := newServiceForTest(t)
	existing := seedEndpoint(t, s)
	gone := existing.Spec
	gone.ID = "acme/gone"
	require.NoError(t, s.repo.SaveEndpoint(context.Background(), &types.ManagedEndpoint{Spec: gone, StubID: "stub-9", Version: 3, Status: types.EndpointStatusActive}))

	_, err := s.ApplyRepo(adminCtx(), &pb.ApplyRepoRequest{Sha: "abc123", DryRun: true, ConfigYaml: "{}\n"})
	require.NoError(t, err)
	out, err := s.ApplyRepo(adminCtx(), &pb.ApplyRepoRequest{
		RepoUrl: "github.com/acme/endpoints", Ref: "main", Sha: "abc123",
		ConfigYaml: "acme/model:\n  enabled: true\n  gpus:\n    H100:\n      priority: 1\n      minReplicas: 1\n",
		Endpoints:  []*pb.RepoEndpoint{repoEndpoint(t, "acme/model", existing.Spec)},
	})
	require.NoError(t, err)
	require.True(t, out.Ok, out.ErrMsg)
	assert.Equal(t, "abc123", out.State.LastSha)
	assert.Empty(t, out.State.LastError)
	assert.Empty(t, out.State.PendingSha, "applying settles the announced deploy")

	fleet, err := s.repo.GetFleet(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "abc123", fleet.GitSHA)
	assert.Equal(t, uint32(1), fleet.Endpoints["acme/model"].GPUs["H100"].MinReplicas)

	updated, err := s.repo.GetEndpoint(context.Background(), "acme/model")
	require.NoError(t, err)
	assert.Equal(t, "abc123", updated.GitSHA)
	assert.Equal(t, "stub-1", updated.StubID, "the deploy RPC owns the stub; apply only stamps the commit")

	retired, err := s.repo.GetEndpoint(context.Background(), "acme/gone")
	require.NoError(t, err)
	assert.Equal(t, types.EndpointStatusRetired, retired.Status)

	list, err := s.ListEndpoints(adminCtx(), &pb.ListEndpointsRequest{})
	require.NoError(t, err)
	states := map[string]string{}
	for _, e := range list.Endpoints {
		states[e.Id] = e.State
	}
	assert.Equal(t, map[string]string{"acme/model": StateWaitingForCapacity, "acme/gone": StateRetired}, states)
}

func TestApplyRepoKeepsEndpointOfBrokenImport(t *testing.T) {
	s := newServiceForTest(t)
	existing := seedEndpoint(t, s)
	require.NoError(t, s.repo.SaveGitOpsState(context.Background(), &types.GitOpsState{
		PerEndpoint: map[string]types.GitOpsEndpointState{"acme/model": {Path: "acme/model", ID: "acme/model", Status: types.GitOpsStatusApplied}},
	}))
	out, err := s.ApplyRepo(adminCtx(), &pb.ApplyRepoRequest{
		Sha: "bad", ConfigYaml: "acme/model:\n  enabled: true\n  gpus:\n    H100:\n      priority: 1\n",
		Endpoints: []*pb.RepoEndpoint{{Path: "acme/model", Error: "SyntaxError"}},
	})
	require.NoError(t, err)
	assert.False(t, out.Ok)
	assert.Equal(t, "acme/model: SyntaxError", out.State.LastError)

	kept, err := s.repo.GetEndpoint(context.Background(), existing.Spec.ID)
	require.NoError(t, err)
	assert.Equal(t, types.EndpointStatusActive, kept.Status, "a bad commit never tears down the previous deploy")
	fleet, err := s.repo.GetFleet(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "bad", fleet.GitSHA, "config.yaml still applies to the previous deploy")

	get, err := s.GetEndpoint(adminCtx(), &pb.GetEndpointRequest{EndpointId: existing.Spec.ID})
	require.NoError(t, err)
	assert.Equal(t, StateDeployFailed, get.Endpoint.State)
	assert.Equal(t, "SyntaxError", get.Endpoint.StateReason)
}

func TestEndpointState(t *testing.T) {
	endpoint := &types.ManagedEndpoint{Spec: types.ManagedEndpointSpec{ID: "acme/model"}, Status: types.EndpointStatusActive}
	placed := &types.Fleet{Endpoints: map[string]types.FleetEndpoint{"acme/model": {Enabled: true, GPUs: map[string]types.FleetPlacement{"H100": {Priority: 1, MinReplicas: 1}}}}}
	now := time.Now()
	replica := func(status types.ReplicaStatus, reason string) *types.EndpointReplica {
		return &types.EndpointReplica{EndpointID: "acme/model", Status: status, StatusReason: reason, EndedAt: now}
	}
	for _, tc := range []struct {
		name, want string
		fleet      *types.Fleet
		replicas   []*types.EndpointReplica
	}{
		{name: "disabled", want: StateDisabled, fleet: &types.Fleet{}},
		{name: "waiting", want: StateWaitingForCapacity, fleet: placed},
		{name: "idle", want: StateIdle, fleet: &types.Fleet{Endpoints: map[string]types.FleetEndpoint{"acme/model": {Enabled: true, GPUs: map[string]types.FleetPlacement{"H100": {Priority: 1, MaxReplicas: 1, Serverless: true}}}}}},
		{name: "loading", want: StateLoading, fleet: placed, replicas: []*types.EndpointReplica{replica(types.ReplicaStatusLoading, "")}},
		{name: "ready", want: StateReady, fleet: placed, replicas: []*types.EndpointReplica{replica(types.ReplicaStatusReady, ""), replica(types.ReplicaStatusFailed, "oom")}},
		{name: "failed", want: StateFailed, fleet: placed, replicas: []*types.EndpointReplica{replica(types.ReplicaStatusFailed, "oom")}},
	} {
		state, reason := endpointState(endpoint, tc.fleet, nil, tc.replicas)
		assert.Equal(t, tc.want, state, tc.name)
		assert.NotEmpty(t, reason, tc.name)
	}
	state, reason := endpointState(endpoint, placed, nil, []*types.EndpointReplica{replica(types.ReplicaStatusFailed, "engine exited: CUDA out of memory")})
	assert.Equal(t, StateFailed, state)
	assert.Contains(t, reason, "CUDA out of memory")
}
