package repository

import (
	"context"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

func newManagedEndpointRepoForTest(t *testing.T) ManagedEndpointRepository {
	t.Helper()
	rdb, err := NewRedisClientForTest()
	require.NoError(t, err)
	return NewManagedEndpointRedisRepository(rdb)
}

func TestManagedEndpointRegistryRoundTrip(t *testing.T) {
	repo := newManagedEndpointRepoForTest(t)
	ctx := context.Background()

	spec := types.ManagedEndpointSpec{ID: "acme/model", Kind: types.EndpointKindLLM, Entrypoint: []string{"x"}}
	spec.Normalize()
	require.NoError(t, repo.SaveEndpoint(ctx, &types.ManagedEndpoint{Spec: spec, StubID: "stub-1", Version: 1, Enabled: true, Status: types.EndpointStatusActive}))
	require.NoError(t, repo.SaveEndpoint(ctx, &types.ManagedEndpoint{Spec: types.ManagedEndpointSpec{ID: "zeta/other"}, StubID: "stub-2", Version: 1}))

	got, err := repo.GetEndpoint(ctx, "acme/model")
	require.NoError(t, err)
	require.Equal(t, "stub-1", got.StubID)
	require.Equal(t, spec.Gpu, got.Spec.Gpu)
	require.False(t, got.CreatedAt.IsZero())

	missing, err := repo.GetEndpoint(ctx, "nope")
	require.NoError(t, err)
	require.Nil(t, missing)

	list, err := repo.ListEndpoints(ctx)
	require.NoError(t, err)
	require.Len(t, list, 2)
	require.Equal(t, "acme/model", list[0].Spec.ID, "sorted by id")

	require.NoError(t, repo.DeleteEndpoint(ctx, "zeta/other"))
	list, err = repo.ListEndpoints(ctx)
	require.NoError(t, err)
	require.Len(t, list, 1)

	require.NoError(t, repo.SaveService(ctx, &types.ManagedService{Spec: types.ManagedServiceSpec{Name: "mooncake-master"}, StubID: "svc-1"}))
	services, err := repo.ListServices(ctx)
	require.NoError(t, err)
	require.Len(t, services, 1)
	svc, err := repo.GetService(ctx, "mooncake-master")
	require.NoError(t, err)
	require.Equal(t, "svc-1", svc.StubID)
	require.NoError(t, repo.DeleteService(ctx, "mooncake-master"))
	svc, err = repo.GetService(ctx, "mooncake-master")
	require.NoError(t, err)
	require.Nil(t, svc)
}

func TestManagedEndpointVersionsAndRollout(t *testing.T) {
	repo := newManagedEndpointRepoForTest(t)
	ctx := context.Background()

	require.NoError(t, repo.SaveVersion(ctx, &types.EndpointVersion{EndpointID: "acme/model", Version: 2, StubID: "s2", State: types.VersionStateCanary}))
	require.NoError(t, repo.SaveVersion(ctx, &types.EndpointVersion{EndpointID: "acme/model", Version: 1, StubID: "s1", State: types.VersionStateActive}))
	versions, err := repo.ListVersions(ctx, "acme/model")
	require.NoError(t, err)
	require.Len(t, versions, 2)
	require.Equal(t, uint(1), versions[0].Version, "ordered by version")
	require.Equal(t, uint(2), versions[1].Version)

	rollout, err := repo.GetRollout(ctx, "acme/model")
	require.NoError(t, err)
	require.Nil(t, rollout)
	require.NoError(t, repo.SaveRollout(ctx, &types.RolloutState{EndpointID: "acme/model", ActiveVersion: 1, CanaryVersion: 2, Phase: "baking"}))
	rollout, err = repo.GetRollout(ctx, "acme/model")
	require.NoError(t, err)
	require.Equal(t, uint(2), rollout.CanaryVersion)
}

func TestManagedEndpointReplicas(t *testing.T) {
	repo := newManagedEndpointRepoForTest(t)
	ctx := context.Background()

	replica := &types.EndpointReplica{ID: "rep-1", EndpointID: "acme/model", ContainerID: "container-1", Role: types.ReplicaRoleServe, GPU: "H100x2", Status: types.ReplicaStatusLoading}
	require.NoError(t, repo.SaveReplica(ctx, replica))
	require.NoError(t, repo.SaveReplica(ctx, &types.EndpointReplica{ID: "rep-2", EndpointID: "other/model", ContainerID: "container-2", Status: types.ReplicaStatusReady}))

	byContainer, err := repo.GetReplicaByContainer(ctx, "container-1")
	require.NoError(t, err)
	require.Equal(t, "rep-1", byContainer.ID)
	perEndpoint, err := repo.ListReplicas(ctx, "acme/model")
	require.NoError(t, err)
	require.Len(t, perEndpoint, 1)
	all, err := repo.ListAllReplicas(ctx)
	require.NoError(t, err)
	require.Len(t, all, 2)

	var calls int
	require.NoError(t, repo.WithReplicaLock(ctx, "rep-1", func(ctx context.Context) error {
		calls++
		current, err := repo.GetReplica(ctx, "rep-1")
		require.NoError(t, err)
		current.Status = types.ReplicaStatusReady
		return repo.SaveReplica(ctx, current)
	}))
	require.Equal(t, 1, calls)
	updated, err := repo.GetReplica(ctx, "rep-1")
	require.NoError(t, err)
	require.Equal(t, types.ReplicaStatusReady, updated.Status)

	drain, _, err := repo.DrainRequested(ctx, "rep-1")
	require.NoError(t, err)
	require.False(t, drain)
	require.NoError(t, repo.RequestDrain(ctx, "rep-1", 7))
	drain, seconds, err := repo.DrainRequested(ctx, "rep-1")
	require.NoError(t, err)
	require.True(t, drain)
	require.Equal(t, uint32(7), seconds)

	require.NoError(t, repo.DeleteReplica(ctx, "rep-1"))
	gone, err := repo.GetReplica(ctx, "rep-1")
	require.NoError(t, err)
	require.Nil(t, gone)
	byContainer, err = repo.GetReplicaByContainer(ctx, "container-1")
	require.NoError(t, err)
	require.Nil(t, byContainer)
	all, err = repo.ListAllReplicas(ctx)
	require.NoError(t, err)
	require.Len(t, all, 1)

	inBackoff, err := repo.InScheduleBackoff(ctx, "acme/model", "serve:H100x2")
	require.NoError(t, err)
	require.False(t, inBackoff)
	require.NoError(t, repo.SetScheduleBackoff(ctx, "acme/model", "serve:H100x2", time.Minute))
	inBackoff, err = repo.InScheduleBackoff(ctx, "acme/model", "serve:H100x2")
	require.NoError(t, err)
	require.True(t, inBackoff)
}

func TestManagedEndpointConfigRevisions(t *testing.T) {
	repo := newManagedEndpointRepoForTest(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	updates, err := repo.SubscribeConfigRevisions(ctx, "acme/model")
	require.NoError(t, err)

	target := &types.EndpointConfigRevision{EndpointID: "acme/model", Scope: types.ConfigScopeTarget, ScopeKey: "serve:H100x2", Config: map[string]any{"max_num_seqs": 256}, Source: types.ConfigSourceGit}
	require.NoError(t, repo.CreateConfigRevision(ctx, target))
	require.Equal(t, uint64(1), target.Revision)
	replicaScoped := &types.EndpointConfigRevision{EndpointID: "acme/model", Scope: types.ConfigScopeReplica, ScopeKey: "rep-1", Config: map[string]any{"max_num_seqs": 128}, Source: types.ConfigSourceLive, Author: "agent"}
	require.NoError(t, repo.CreateConfigRevision(ctx, replicaScoped))
	require.Equal(t, uint64(2), replicaScoped.Revision)

	select {
	case got := <-updates:
		require.Equal(t, uint64(1), got.Revision)
	case <-time.After(2 * time.Second):
		t.Fatal("expected config revision event")
	}

	latest, err := repo.LatestConfigRevision(ctx, "acme/model", types.ConfigScopeTarget, "serve:H100x2")
	require.NoError(t, err)
	require.Equal(t, uint64(1), latest.Revision)
	require.EqualValues(t, 256, latest.Config["max_num_seqs"])
	none, err := repo.LatestConfigRevision(ctx, "acme/model", types.ConfigScopeTarget, "serve:A100x1")
	require.NoError(t, err)
	require.Nil(t, none)

	revisions, err := repo.ListConfigRevisions(ctx, "acme/model", types.ConfigScopeReplica, "rep-1", 10)
	require.NoError(t, err)
	require.Len(t, revisions, 1)
	require.Equal(t, "agent", revisions[0].Author)

	require.NoError(t, repo.SaveConfigAck(ctx, &types.ConfigAck{ReplicaID: "rep-1", Revision: 2, Applied: true}))
	gotAck, err := repo.GetConfigAck(ctx, "rep-1", 2)
	require.NoError(t, err)
	require.True(t, gotAck.Applied)
	require.False(t, gotAck.At.IsZero())

	require.NoError(t, repo.DeleteConfigRevisions(ctx, "acme/model", types.ConfigScopeReplica, "rep-1"))
	revisions, err = repo.ListConfigRevisions(ctx, "acme/model", types.ConfigScopeReplica, "rep-1", 10)
	require.NoError(t, err)
	require.Empty(t, revisions)
	byRev, err := repo.GetConfigRevision(ctx, "acme/model", 2)
	require.NoError(t, err)
	require.Nil(t, byRev)
}

func TestManagedEndpointGitOpsAndMetrics(t *testing.T) {
	repo := newManagedEndpointRepoForTest(t)
	ctx := context.Background()

	state, err := repo.GetGitOpsState(ctx)
	require.NoError(t, err)
	require.Nil(t, state)
	require.NoError(t, repo.SaveGitOpsState(ctx, &types.GitOpsState{RepoURL: "git@github.com:beam-cloud/endpoints.git", Ref: "main", LastSHA: "abc"}))
	state, err = repo.GetGitOpsState(ctx)
	require.NoError(t, err)
	require.Equal(t, "abc", state.LastSHA)
	require.NotNil(t, state.PerEndpoint)

	now := time.Now()
	samples := []types.RouteSample{
		{EndpointID: "acme/model", GPU: "H100x2", Version: 1, StatusCode: 200, PromptTokens: 100, CompletionTokens: 50, Duration: 2 * time.Second, TTFT: 200 * time.Millisecond, CostMicroUSD: 12, At: now},
		{EndpointID: "acme/model", GPU: "H100x2", Version: 2, StatusCode: 500, PromptTokens: 10, Duration: time.Second, At: now},
		{EndpointID: "acme/model", GPU: "A100x1", Version: 1, StatusCode: 200, PromptTokens: 20, CompletionTokens: 20, Duration: time.Second, TTFT: 100 * time.Millisecond, At: now.Add(-2 * time.Minute)},
	}
	for _, sample := range samples {
		require.NoError(t, repo.RecordRouteSample(ctx, sample))
	}

	all, err := repo.GetRouteMetrics(ctx, "acme/model", "", 0, 10*time.Minute)
	require.NoError(t, err)
	require.EqualValues(t, 3, all.Requests)
	require.EqualValues(t, 1, all.Errors)
	require.EqualValues(t, 130, all.PromptTokens)
	require.EqualValues(t, 70, all.CompletionTokens)
	require.EqualValues(t, 12, all.CostMicroUSD)
	require.EqualValues(t, 4000, all.DurationSumMs)
	require.EqualValues(t, 150, all.MeanTTFTMs())
	require.InDelta(t, 1.0/3.0, all.ErrorRate(), 1e-9)

	h100v2, err := repo.GetRouteMetrics(ctx, "acme/model", "H100x2", 2, 10*time.Minute)
	require.NoError(t, err)
	require.EqualValues(t, 1, h100v2.Requests)
	require.EqualValues(t, 1, h100v2.Errors)
	require.Equal(t, "H100x2", h100v2.GPU)

	v1, err := repo.GetRouteMetrics(ctx, "acme/model", "", 1, 10*time.Minute)
	require.NoError(t, err)
	require.EqualValues(t, 2, v1.Requests)
	require.Zero(t, v1.Errors)

	recent, err := repo.GetRouteMetrics(ctx, "acme/model", "", 0, time.Minute)
	require.NoError(t, err)
	require.EqualValues(t, 2, recent.Requests, "older bucket falls outside the window")
}

func TestManagedEndpointGenerations(t *testing.T) {
	repo := newManagedEndpointRepoForTest(t)
	ctx := context.Background()

	missing, err := repo.GetGeneration(ctx, "gen-1")
	require.NoError(t, err)
	require.Nil(t, missing)
	require.NoError(t, repo.SaveGeneration(ctx, &types.EventEndpointRouteSchema{RequestID: "gen-1", EndpointID: "acme/model"}, time.Hour))
	record, err := repo.GetGeneration(ctx, "gen-1")
	require.NoError(t, err)
	require.Equal(t, "acme/model", record.EndpointID)
}

func TestManagedEndpointProviderEarnings(t *testing.T) {
	repo := newManagedEndpointRepoForTest(t)
	ctx := context.Background()
	now := time.Now().UTC()

	empty, err := repo.GetProviderEarnings(ctx, "ws-provider", 7)
	require.NoError(t, err)
	require.Equal(t, types.ProviderEarnings{}, empty.Total)

	require.NoError(t, repo.AddProviderEarnings(ctx, "ws-provider", "machine-a", now, types.ProviderEarnings{Requests: 1, PromptTokens: 100, CompletionTokens: 50, EarningsMicroUSD: 700}))
	require.NoError(t, repo.AddProviderEarnings(ctx, "ws-provider", "machine-b", now, types.ProviderEarnings{Requests: 1, Images: 2, EarningsMicroUSD: 300}))
	require.NoError(t, repo.AddProviderEarnings(ctx, "ws-provider", "machine-a", now.AddDate(0, 0, -1), types.ProviderEarnings{Requests: 1, EarningsMicroUSD: 1000}))

	report, err := repo.GetProviderEarnings(ctx, "ws-provider", 7)
	require.NoError(t, err)
	require.Equal(t, types.ProviderEarnings{Requests: 3, PromptTokens: 100, CompletionTokens: 50, Images: 2, EarningsMicroUSD: 2000}, report.Total)
	require.Equal(t, types.ProviderEarnings{Requests: 2, PromptTokens: 100, CompletionTokens: 50, EarningsMicroUSD: 1700}, report.PerMachine["machine-a"])
	require.Equal(t, types.ProviderEarnings{Requests: 1, Images: 2, EarningsMicroUSD: 300}, report.PerMachine["machine-b"])
	require.Len(t, report.PerDay, 2)
	require.Equal(t, int64(1000), report.PerDay[now.AddDate(0, 0, -1).Format(time.DateOnly)].EarningsMicroUSD)

	// Only today is in a 1-day window.
	today, err := repo.GetProviderEarnings(ctx, "ws-provider", 1)
	require.NoError(t, err)
	require.Equal(t, int64(1000), today.Total.EarningsMicroUSD)
}
