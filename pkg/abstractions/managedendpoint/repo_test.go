package managedendpoint

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func repoEndpoint(path string, app *types.ManagedEndpoint) *pb.RepoEndpoint {
	return &pb.RepoEndpoint{Path: path, Id: app.Spec.ID, SpecJson: mustJSON(app.Spec), StubId: app.StubID, Version: uint32(app.Version)}
}

const publishedModel = `acme/model:
  enabled: true
  catalog: {name: Model, description: A model, context_length: 32768}
  public: true
  pricing: {prompt_tokens: "0.000001", completion_tokens: "0.000002"}
  gpus:
    H100:
      priority: 1
      minReplicas: 1
`

func TestApplyRepoDryRunValidatesWithoutApplying(t *testing.T) {
	s := newServiceForTest(t)
	existing := seedEndpoint(t, s)
	other := *existing
	other.Spec.ID = "acme/other"
	out, err := s.ApplyRepo(adminCtx(), &pb.ApplyRepoRequest{
		Sha: "abc", DryRun: true,
		ConfigYaml: "acme/model:\n  enabled: true\n  pricing: {request: \"0\"}\n  gpus:\n    A100:\n      priority: 1\nacme/ghost:\n  enabled: true\n  pricing: {request: \"0\"}\n  gpus: {}\n",
		Endpoints: []*pb.RepoEndpoint{
			repoEndpoint("acme/model", existing),
			repoEndpoint("acme/renamed", &other),
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
	assert.Equal(t, "abc", state.PendingSHA, "a dry run with a sha announces the deploy")
	assert.Empty(t, state.LastSHA)
	fleet, err := s.repo.GetFleet(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "fleet-sha", fleet.GitSHA)
}

func TestApplyRepoPublishesPlacesStampsAndRetires(t *testing.T) {
	s := newServiceForTest(t)
	existing := seedEndpoint(t, s)
	existing.Published, existing.Publication = false, types.Publication{}
	require.NoError(t, s.repo.SaveEndpoint(context.Background(), existing))
	gone := *existing
	gone.Spec.ID, gone.StubID = "acme/gone", "stub-9"
	require.NoError(t, s.repo.SaveEndpoint(context.Background(), &gone))

	_, err := s.ApplyRepo(adminCtx(), &pb.ApplyRepoRequest{Sha: "abc123", DryRun: true, ConfigYaml: "{}\n"})
	require.NoError(t, err)
	out, err := s.ApplyRepo(adminCtx(), &pb.ApplyRepoRequest{
		RepoUrl: "github.com/acme/endpoints", Ref: "main", Sha: "abc123",
		ConfigYaml: publishedModel,
		Endpoints:  []*pb.RepoEndpoint{repoEndpoint("acme/model", existing)},
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
	assert.Equal(t, "stub-1", updated.StubID, "DeployStub owns the stub; apply only stamps the commit")
	assert.True(t, updated.Published)
	assert.Equal(t, types.Publication{Catalog: types.Catalog{Name: "Model", Description: "A model", ContextLength: 32768}, Public: true, Pricing: types.Pricing{PromptTokens: "0.000001", CompletionTokens: "0.000002"}}, updated.Publication,
		"config.yaml is the one place publication lives")

	retired, err := s.repo.GetEndpoint(context.Background(), "acme/gone")
	require.NoError(t, err)
	assert.Equal(t, types.EndpointStatusRetired, retired.Status)

	list, err := s.ListEndpoints(adminCtx(), &pb.ListEndpointsRequest{})
	require.NoError(t, err)
	states := map[string]EndpointState{}
	for _, e := range list.Endpoints {
		states[e.Id] = EndpointState(e.State)
	}
	assert.Equal(t, map[string]EndpointState{"acme/model": StateWaitingForCapacity, "acme/gone": StateRetired}, states)

	// Disabling the app in config.yaml unpublishes it without retiring the deploy.
	out, err = s.ApplyRepo(adminCtx(), &pb.ApplyRepoRequest{
		Sha: "def456", ConfigYaml: "acme/model:\n  enabled: false\n  gpus: {}\n", Endpoints: []*pb.RepoEndpoint{repoEndpoint("acme/model", existing)},
	})
	require.NoError(t, err)
	require.True(t, out.Ok, out.ErrMsg)
	updated, err = s.repo.GetEndpoint(context.Background(), "acme/model")
	require.NoError(t, err)
	assert.False(t, updated.Published)
	assert.Equal(t, types.EndpointStatusActive, updated.Status)
	assert.Equal(t, http.StatusNotFound, call(t, s, userInfo, http.MethodPost, "/v1/chat/completions", `{"model":"acme/model","messages":[]}`).Code, "an unpublished app is not callable")
}

func TestApplyRepoRejectsUnpublishablePricing(t *testing.T) {
	s := newServiceForTest(t)
	seedEndpoint(t, s)
	queue := seedRunner(t, s, "acme/video", types.StubTypeTaskQueue, types.Pricing{Request: "0.10"})
	for name, tc := range map[string]struct{ yaml, want string }{
		"mixed forms":            {"acme/model:\n  enabled: true\n  pricing: {request: \"0.1\", prompt_tokens: \"0\", completion_tokens: \"0\"}\n  gpus: {H100: {priority: 1}}\n", "mutually exclusive"},
		"no price":               {"acme/model:\n  enabled: true\n  gpus: {H100: {priority: 1}}\n", "declare request or prompt_tokens/completion_tokens"},
		"tokens on a task queue": {"acme/video:\n  enabled: true\n  pricing: {prompt_tokens: \"0\", completion_tokens: \"0\"}\n  gpus: {A10G: {priority: 1, maxReplicas: 1, serverless: true}}\n", "priced per request"},
		"legacy image price":     {"acme/model:\n  enabled: true\n  pricing: {image: \"0.01\"}\n  gpus: {H100: {priority: 1}}\n", "field image not found"},
		"access in catalog":      {"acme/model:\n  enabled: true\n  catalog: {public: true}\n  pricing: {request: \"0\"}\n  gpus: {H100: {priority: 1}}\n", "field public not found"},
	} {
		t.Run(name, func(t *testing.T) {
			out, err := s.ApplyRepo(adminCtx(), &pb.ApplyRepoRequest{
				Sha: "x", DryRun: true, ConfigYaml: tc.yaml, Endpoints: []*pb.RepoEndpoint{repoEndpoint("acme/model", seedEndpoint(t, s)), repoEndpoint("acme/video", queue)},
			})
			require.NoError(t, err)
			assert.False(t, out.Ok)
			require.Len(t, out.Errors, 1, out.Errors)
			assert.Contains(t, out.Errors[0], tc.want)
		})
	}
}

func TestApplyRepoKeepsAppOfBrokenImport(t *testing.T) {
	s := newServiceForTest(t)
	existing := seedEndpoint(t, s)
	require.NoError(t, s.repo.SaveGitOpsState(context.Background(), &types.GitOpsState{
		PerEndpoint: map[string]types.GitOpsEndpointState{"acme/model": {Path: "acme/model", ID: "acme/model", Status: types.GitOpsStatusApplied}},
	}))
	out, err := s.ApplyRepo(adminCtx(), &pb.ApplyRepoRequest{
		Sha: "bad", ConfigYaml: publishedModel, Endpoints: []*pb.RepoEndpoint{{Path: "acme/model", Error: "SyntaxError"}},
	})
	require.NoError(t, err)
	assert.False(t, out.Ok)
	assert.Equal(t, "acme/model: SyntaxError", out.State.LastError)

	kept, err := s.repo.GetEndpoint(context.Background(), existing.Spec.ID)
	require.NoError(t, err)
	assert.Equal(t, types.EndpointStatusActive, kept.Status, "a broken import keeps the last good deploy")
	fleet, err := s.repo.GetFleet(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "bad", fleet.GitSHA, "config.yaml still applies to the previous deploy")

	get, err := s.GetEndpoint(adminCtx(), &pb.GetEndpointRequest{EndpointId: existing.Spec.ID})
	require.NoError(t, err)
	assert.Equal(t, StateDeployFailed, EndpointState(get.Endpoint.State))
	assert.Equal(t, "SyntaxError", get.Endpoint.StateReason)
}

func TestApplyRepoRequiresRegisteredStub(t *testing.T) {
	s := newServiceForTest(t)
	e := repoEndpoint("acme/model", seedEndpoint(t, s))
	e.StubId = "stub-from-another-cluster"
	out, err := s.ApplyRepo(adminCtx(), &pb.ApplyRepoRequest{Sha: "x", ConfigYaml: publishedModel, Endpoints: []*pb.RepoEndpoint{e}})
	require.NoError(t, err)
	assert.False(t, out.Ok)
	assert.Contains(t, out.Errors[0], "was not registered as acme/model")
}

func TestEndpointState(t *testing.T) {
	endpoint := &types.ManagedEndpoint{Spec: types.ManagedEndpointSpec{ID: "acme/model"}, Status: types.EndpointStatusActive}
	placed := &types.Fleet{Endpoints: map[string]types.FleetEndpoint{"acme/model": {Enabled: true, GPUs: map[string]types.FleetPlacement{"H100": {Priority: 1, MinReplicas: 1}}}}}
	now := time.Now()
	replica := func(status types.ReplicaStatus, reason string) *types.EndpointReplica {
		return &types.EndpointReplica{EndpointID: "acme/model", Status: status, StatusReason: reason, EndedAt: now}
	}
	for _, tc := range []struct {
		name     string
		want     EndpointState
		fleet    *types.Fleet
		replicas []*types.EndpointReplica
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
