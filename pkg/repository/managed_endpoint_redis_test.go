package repository

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/redis/go-redis/v9"
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
	spec.Normalize(true)
	require.NoError(t, repo.SaveEndpoint(ctx, &types.ManagedEndpoint{Spec: spec, StubID: "stub-1", Version: 1, Status: types.EndpointStatusActive}))
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
}

func TestManagedEndpointFleet(t *testing.T) {
	repo := newManagedEndpointRepoForTest(t)
	ctx := context.Background()

	empty, err := repo.GetFleet(ctx)
	require.NoError(t, err)
	require.NotNil(t, empty)
	require.NotNil(t, empty.Endpoints, "an unset fleet reads as empty, never nil")
	require.Empty(t, empty.Placements("acme/model"))

	require.Error(t, repo.SaveFleet(ctx, nil))
	fleet := &types.Fleet{GitSHA: "abc", Endpoints: map[string]types.FleetEndpoint{"acme/model": {Enabled: true, GPUs: map[string]types.FleetPlacement{"H100": {Priority: 1, MaxReplicas: 2}}}}}
	require.NoError(t, repo.SaveFleet(ctx, fleet))
	require.False(t, fleet.UpdatedAt.IsZero())

	got, err := repo.GetFleet(ctx)
	require.NoError(t, err)
	require.Equal(t, "abc", got.GitSHA)
	require.Equal(t, map[string]types.FleetPlacement{"H100": {Priority: 1, MaxReplicas: 2}}, got.Placements("acme/model"))
}

func TestManagedEndpointReplicas(t *testing.T) {
	repo := newManagedEndpointRepoForTest(t)
	ctx := context.Background()

	replica := &types.EndpointReplica{ID: "rep-1", EndpointID: "acme/model", ContainerID: "container-1", GPU: "H100", GPUCount: 2, Status: types.ReplicaStatusLoading}
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

	inBackoff, err := repo.InScheduleBackoff(ctx, "acme/model", "H100")
	require.NoError(t, err)
	require.False(t, inBackoff)
	require.NoError(t, repo.SetScheduleBackoff(ctx, "acme/model", "H100", time.Minute))
	inBackoff, err = repo.InScheduleBackoff(ctx, "acme/model", "H100")
	require.NoError(t, err)
	require.True(t, inBackoff)
}

func TestManagedEndpointReplicaConfigNotify(t *testing.T) {
	repo := newManagedEndpointRepoForTest(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	updates, err := repo.SubscribeReplicaConfig(ctx, "rep-1")
	require.NoError(t, err)
	other, err := repo.SubscribeReplicaConfig(ctx, "rep-2")
	require.NoError(t, err)

	// The config itself lives on the replica record; the notification only
	// carries the revision that woke the watcher.
	replica := &types.EndpointReplica{ID: "rep-1", EndpointID: "acme/model", Config: types.ReplicaConfig{Revision: 3, Config: json.RawMessage(`{"max_num_seqs":8}`), Author: "agent"}}
	require.NoError(t, repo.SaveReplica(ctx, replica))
	require.NoError(t, repo.NotifyReplicaConfig(ctx, "rep-1", 3))

	select {
	case got := <-updates:
		require.Equal(t, uint64(3), got)
	case <-time.After(2 * time.Second):
		t.Fatal("expected replica config notification")
	}
	select {
	case got := <-other:
		t.Fatalf("notification for another replica leaked: %d", got)
	case <-time.After(100 * time.Millisecond):
	}

	stored, err := repo.GetReplica(ctx, "rep-1")
	require.NoError(t, err)
	require.Equal(t, uint64(3), stored.Config.Revision)
	require.JSONEq(t, `{"max_num_seqs":8}`, string(stored.Config.Config))
	require.False(t, stored.Config.Acked())
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
	samples := []*types.Charge{
		{ID: "c1", Status: types.ChargeSettled, AppID: "acme/model", GPU: "H100", ReplicaID: "rep-1", ConfigRevision: 2, StatusCode: 200, Work: types.Work{PromptTokens: 100, CompletionTokens: 50}, DurationMs: 2000, TTFTMs: 200, Cost: types.Cost{MicroUSD: 12}, SettledAt: now},
		{ID: "c2", Status: types.ChargeVoid, AppID: "acme/model", GPU: "H100", ReplicaID: "rep-2", StatusCode: 500, Work: types.Work{PromptTokens: 10}, DurationMs: 1000, SettledAt: now},
		{ID: "c3", Status: types.ChargeSettled, AppID: "acme/model", GPU: "A100", StatusCode: 200, Work: types.Work{PromptTokens: 20, CompletionTokens: 20}, DurationMs: 1000, TTFTMs: 100, SettledAt: now.Add(-2 * time.Minute)},
	}
	for _, sample := range samples {
		require.NoError(t, repo.RecordRouteSample(ctx, sample))
	}

	all, err := repo.GetRouteMetrics(ctx, "acme/model", "", "", 0, 10*time.Minute)
	require.NoError(t, err)
	require.EqualValues(t, 3, all.Requests)
	require.EqualValues(t, 1, all.Errors)
	require.EqualValues(t, 130, all.PromptTokens)
	require.EqualValues(t, 70, all.CompletionTokens)
	require.EqualValues(t, 12, all.CostMicroUSD)
	require.EqualValues(t, 4000, all.DurationSumMs)
	require.EqualValues(t, 150, all.MeanTTFTMs())

	h100, err := repo.GetRouteMetrics(ctx, "acme/model", "H100", "", 0, 10*time.Minute)
	require.NoError(t, err)
	require.EqualValues(t, 2, h100.Requests, "the GPU aggregate spans both replicas")
	require.EqualValues(t, 1, h100.Errors)
	require.Equal(t, "H100", h100.GPU)
	require.Empty(t, h100.ReplicaID)

	rep2, err := repo.GetRouteMetrics(ctx, "acme/model", "H100", "rep-2", 0, 10*time.Minute)
	require.NoError(t, err)
	require.EqualValues(t, 1, rep2.Requests, "a replica bucket holds only its own samples")
	require.EqualValues(t, 1, rep2.Errors)
	require.Equal(t, "rep-2", rep2.ReplicaID)

	rep1, err := repo.GetRouteMetrics(ctx, "acme/model", "H100", "rep-1", 0, 10*time.Minute)
	require.NoError(t, err)
	require.EqualValues(t, 1, rep1.Requests)
	require.Zero(t, rep1.Errors)
	require.EqualValues(t, 12, rep1.CostMicroUSD)

	// A replica's traffic can be split by the live config it had acknowledged.
	rev2, err := repo.GetRouteMetrics(ctx, "acme/model", "H100", "rep-1", 2, 10*time.Minute)
	require.NoError(t, err)
	require.EqualValues(t, 1, rev2.Requests)
	require.EqualValues(t, 2, rev2.ConfigRevision)
	require.Equal(t, "rep-1", rev2.ReplicaID)
	rev1, err := repo.GetRouteMetrics(ctx, "acme/model", "H100", "rep-1", 1, 10*time.Minute)
	require.NoError(t, err)
	require.Zero(t, rev1.Requests)

	wrongGPU, err := repo.GetRouteMetrics(ctx, "acme/model", "A100", "rep-1", 0, 10*time.Minute)
	require.NoError(t, err)
	require.Zero(t, wrongGPU.Requests, "replica samples live under the replica's own gpu")

	recent, err := repo.GetRouteMetrics(ctx, "acme/model", "", "", 0, time.Minute)
	require.NoError(t, err)
	require.EqualValues(t, 2, recent.Requests, "older bucket falls outside the window")
}

func TestManagedEndpointCharges(t *testing.T) {
	repo := newManagedEndpointRepoForTest(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)

	missing, err := repo.GetCharge(ctx, "gen-1")
	require.NoError(t, err)
	require.Nil(t, missing)
	_, err = repo.SaveCharge(ctx, &types.Charge{ID: "gen-1"})
	require.Error(t, err, "a charge without its caller and app cannot be journaled")

	// A task charge is opened when the work is accepted, updated once when it
	// settles, and final after that: a duplicate completion changes nothing.
	open := &types.Charge{ID: "task-1", Status: types.ChargeOpen, WorkspaceID: "ws-tenant", AppID: "acme/model", Pricing: types.Pricing{Request: "0.05"}, AcceptedAt: now}
	written, err := repo.SaveCharge(ctx, open)
	require.NoError(t, err)
	require.True(t, written)
	due, err := repo.ListPendingCharges(ctx, now, 10)
	require.NoError(t, err)
	require.Empty(t, due, "an open task is not due until its poll interval passes")
	due, err = repo.ListPendingCharges(ctx, now.Add(TaskPollInterval), 10)
	require.NoError(t, err)
	require.Len(t, due, 1)

	settled := *open
	require.NoError(t, settled.Settle(types.Work{}, now.Add(time.Minute)))
	written, err = repo.SaveCharge(ctx, &settled)
	require.NoError(t, err)
	require.True(t, written)
	duplicate := *open
	duplicate.Void("late duplicate completion", now.Add(2*time.Minute))
	written, err = repo.SaveCharge(ctx, &duplicate)
	require.NoError(t, err)
	require.False(t, written)
	stored, err := repo.GetCharge(ctx, "task-1")
	require.NoError(t, err)
	require.Equal(t, types.ChargeSettled, stored.Status)
	require.EqualValues(t, 50_000, stored.Cost.MicroUSD)
	require.EqualValues(t, 1, stored.Work.Requests)

	due, err = repo.ListPendingCharges(ctx, now.Add(time.Minute), 10)
	require.NoError(t, err)
	require.Len(t, due, 1, "a settled charge is due immediately")
	require.NoError(t, repo.CompleteAccounting(ctx, "task-1", time.Hour))
	due, err = repo.ListPendingCharges(ctx, now.Add(time.Hour), 10)
	require.NoError(t, err)
	require.Empty(t, due)
	kept, err := repo.GetCharge(ctx, "task-1")
	require.NoError(t, err)
	require.NotNil(t, kept, "the journal entry stays readable for the generation API")

	require.NoError(t, repo.DeferAccounting(ctx, "task-2", now.Add(time.Hour)))
	due, err = repo.ListPendingCharges(ctx, now.Add(2*time.Hour), 10)
	require.Error(t, err, "a pending id without a journal entry is a corrupt index, not silently skipped")
	require.Nil(t, due)
}

// Pre-consolidation route records are read as charges with their historical
// amounts intact; an image charge stays in the total and is never
// reinterpreted as a per-request price.
func TestManagedEndpointLegacyRouteRecordReadsAsCharge(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	require.NoError(t, err)
	repo := NewManagedEndpointRedisRepository(rdb)
	ctx := context.Background()
	legacy := `{"request_id":"gen-old","endpoint_id":"acme/image","workspace_id":"ws-tenant","route":"images/generations","status_code":200,` +
		`"images":2,"cost_micro_usd":900,"image_micro_usd":900,"prompt_tokens":0,"timestamp":"2026-09-01T00:00:00Z"}`
	require.NoError(t, rdb.Set(ctx, meKey("generation", "gen-old"), legacy, 0).Err())
	require.NoError(t, rdb.ZAdd(ctx, meKey("accounting", "pending"), redis.Z{Score: 1, Member: "gen-old"}).Err())

	c, err := repo.GetCharge(ctx, "gen-old")
	require.NoError(t, err)
	require.Equal(t, types.ChargeSettled, c.Status)
	require.Equal(t, "acme/image", c.AppID)
	require.Equal(t, "ws-tenant", c.WorkspaceID)
	require.EqualValues(t, 900, c.Cost.MicroUSD)
	require.Zero(t, c.Cost.RequestMicroUSD)
	require.EqualValues(t, 1, c.Work.Requests)
	require.Equal(t, types.Cost{MicroUSD: 900}, c.Cost)
	pending, err := repo.ListPendingCharges(ctx, time.Now(), 10)
	require.NoError(t, err)
	require.Len(t, pending, 1)

	written, err := repo.SaveCharge(ctx, c)
	require.NoError(t, err)
	require.True(t, written, "migration may rewrite a legacy record as a charge")
	written, err = repo.SaveCharge(ctx, c)
	require.NoError(t, err)
	require.False(t, written, "after which it is final")

	failed := `{"request_id":"gen-failed","endpoint_id":"acme/model","workspace_id":"ws-tenant","status_code":502,"error":"boom","timestamp":"2026-09-01T00:00:00Z"}`
	require.NoError(t, rdb.Set(ctx, meKey("generation", "gen-failed"), failed, 0).Err())
	c, err = repo.GetCharge(ctx, "gen-failed")
	require.NoError(t, err)
	require.Equal(t, types.ChargeVoid, c.Status)

	schema, err := repo.GetChargeSchema(ctx)
	require.NoError(t, err)
	require.Empty(t, schema)
	require.NoError(t, repo.SetChargeSchema(ctx, ChargeSchema))
	schema, err = repo.GetChargeSchema(ctx)
	require.NoError(t, err)
	require.Equal(t, ChargeSchema, schema)
}

func usage(w types.Work, c types.Cost) types.Usage { return types.Usage{Work: w, Cost: c} }

func TestManagedEndpointUsage(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	require.NoError(t, err)
	repo := NewManagedEndpointRedisRepository(rdb)
	ctx := context.Background()
	now := time.Now().UTC()
	yesterday := now.AddDate(0, 0, -1)

	empty, err := repo.GetUsage(ctx, types.UsageSpend, "ws-tenant", now.AddDate(0, 0, -6), now)
	require.NoError(t, err)
	require.Equal(t, types.Usage{}, empty.Total)
	require.Empty(t, empty.PerModel)
	require.Empty(t, empty.PerDay)

	require.Error(t, repo.AddUsage(ctx, types.UsageSpend, "", "acme/model", "req-1", now, usage(types.Work{Requests: 1}, types.Cost{})))
	require.Error(t, repo.AddUsage(ctx, types.UsageSpend, "ws-tenant", "", "req-2", now, usage(types.Work{Requests: 1}, types.Cost{})))

	// A tenant spends on two models across two days ...
	require.NoError(t, repo.AddUsage(ctx, types.UsageSpend, "ws-tenant", "acme/model", "req-3", now, usage(types.Work{Requests: 1, PromptTokens: 100, CompletionTokens: 50}, types.Cost{MicroUSD: 700})))
	require.NoError(t, repo.AddUsage(ctx, types.UsageSpend, "ws-tenant", "acme/image", "req-4", now, usage(types.Work{Requests: 1}, types.Cost{MicroUSD: 300, RequestMicroUSD: 300})))
	require.NoError(t, repo.AddUsage(ctx, types.UsageSpend, "ws-tenant", "acme/model", "req-5", yesterday, usage(types.Work{Requests: 1}, types.Cost{MicroUSD: 1000})))
	// ... and the provider whose machine served one request earns its share.
	require.NoError(t, repo.AddUsage(ctx, types.UsageEarned, "ws-provider", "acme/model", "req-6", now, usage(types.Work{Requests: 1, PromptTokens: 100, CompletionTokens: 50}, types.Cost{MicroUSD: 490})))
	// Historical image counters written by the previous schema are not part of the usage model; readers ignore them.
	require.NoError(t, rdb.HIncrBy(ctx, meKey("usage", "spend", "ws-tenant", now.Format(time.DateOnly)), "acme/image|images", 2).Err())
	require.NoError(t, rdb.HIncrBy(ctx, meKey("usage", "spend", "ws-tenant", now.Format(time.DateOnly)), "acme/image|image_micro_usd", 300).Err())

	spend, err := repo.GetUsage(ctx, types.UsageSpend, "ws-tenant", now.AddDate(0, 0, -6), now)
	require.NoError(t, err)
	require.Equal(t, usage(types.Work{Requests: 3, PromptTokens: 100, CompletionTokens: 50}, types.Cost{MicroUSD: 2000, RequestMicroUSD: 300}), spend.Total)
	require.Equal(t, usage(types.Work{Requests: 2, PromptTokens: 100, CompletionTokens: 50}, types.Cost{MicroUSD: 1700}), spend.PerModel["acme/model"])
	require.Equal(t, usage(types.Work{Requests: 1}, types.Cost{MicroUSD: 300, RequestMicroUSD: 300}), spend.PerModel["acme/image"])
	require.Len(t, spend.PerDay, 2)
	require.Equal(t, int64(1000), spend.PerDay[yesterday.Format(time.DateOnly)].MicroUSD)
	require.Equal(t, int64(1000), spend.PerDay[now.Format(time.DateOnly)].MicroUSD)

	// Only today is in a 1-day window.
	today, err := repo.GetUsage(ctx, types.UsageSpend, "ws-tenant", now, now)
	require.NoError(t, err)
	require.Equal(t, int64(1000), today.Total.MicroUSD)

	// Replaying a request id is a no-op, so a retried accounting leg never double-counts.
	require.NoError(t, repo.AddUsage(ctx, types.UsageSpend, "ws-tenant", "acme/model", "req-3", now, usage(types.Work{Requests: 1}, types.Cost{MicroUSD: 700})))
	replayed, err := repo.GetUsage(ctx, types.UsageSpend, "ws-tenant", now.AddDate(0, 0, -6), now)
	require.NoError(t, err)
	require.Equal(t, spend.Total, replayed.Total)

	// Spend and earnings are separate ledgers.
	earned, err := repo.GetUsage(ctx, types.UsageEarned, "ws-provider", now.AddDate(0, 0, -6), now)
	require.NoError(t, err)
	require.Equal(t, usage(types.Work{Requests: 1, PromptTokens: 100, CompletionTokens: 50}, types.Cost{MicroUSD: 490}), earned.Total)
	require.Equal(t, earned.Total, earned.PerModel["acme/model"])
	tenantEarned, err := repo.GetUsage(ctx, types.UsageEarned, "ws-tenant", now.AddDate(0, 0, -6), now)
	require.NoError(t, err)
	require.Equal(t, types.Usage{}, tenantEarned.Total)
}

type failOnceHook struct {
	command string
	failed  atomic.Bool
}

func (h *failOnceHook) DialHook(next redis.DialHook) redis.DialHook { return next }
func (h *failOnceHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if cmd.Name() == h.command && h.failed.CompareAndSwap(false, true) {
			return errors.New("synthetic transport failure")
		}
		return next(ctx, cmd)
	}
}
func (h *failOnceHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

// A request whose write failed must be repairable by replaying the same
// request id: the dedupe marker and the counters commit together or not at all.
func TestManagedEndpointUsageReplayRepairsFailedWrite(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	require.NoError(t, err)
	rdb.AddHook(&failOnceHook{command: "evalsha"})
	repo := NewManagedEndpointRedisRepository(rdb)
	ctx := context.Background()
	now := time.Now()
	u := usage(types.Work{Requests: 1}, types.Cost{MicroUSD: 7})

	require.Error(t, repo.AddUsage(ctx, types.UsageSpend, "tenant", "acme/model", "request-1", now, u))
	require.NoError(t, repo.AddUsage(ctx, types.UsageSpend, "tenant", "acme/model", "request-1", now, u))
	require.NoError(t, repo.AddUsage(ctx, types.UsageSpend, "tenant", "acme/model", "request-1", now, u), "a second replay is a no-op")
	result, err := repo.GetUsage(ctx, types.UsageSpend, "tenant", now, now)
	require.NoError(t, err)
	require.Equal(t, u, result.Total)
	require.Equal(t, u, result.PerModel["acme/model"])
}

func TestUsageLuaFailureDoesNotCommitDedupeOrPartialCounters(t *testing.T) {
	for _, corrupt := range []string{"not-an-integer", "9007199254740991"} {
		t.Run(corrupt, func(t *testing.T) {
			rdb, err := NewRedisClientForTest()
			require.NoError(t, err)
			repo := NewManagedEndpointRedisRepository(rdb)
			ctx, now := context.Background(), time.Now()
			key := meKey("usage", "spend", "buyer", now.UTC().Format(time.DateOnly))
			require.NoError(t, rdb.HSet(ctx, key, "micro_usd", corrupt).Err())
			delta := usage(types.Work{Requests: 1, PromptTokens: 20, CachedTokens: 10}, types.Cost{MicroUSD: 7, PromptMicroUSD: 4, CachedMicroUSD: 3})
			require.Error(t, repo.AddUsage(ctx, types.UsageSpend, "buyer", "acme/model", "req-1", now, delta))
			require.EqualValues(t, 0, rdb.Exists(ctx, meKey("usage", "seen", "spend", "req-1")).Val())
			require.EqualValues(t, 1, rdb.HLen(ctx, key).Val(), "preflight rejects before any increment")
			require.NoError(t, rdb.HDel(ctx, key, "micro_usd").Err())
			require.NoError(t, repo.AddUsage(ctx, types.UsageSpend, "buyer", "acme/model", "req-1", now, delta))
			report, err := repo.GetUsage(ctx, types.UsageSpend, "buyer", now, now)
			require.NoError(t, err)
			require.Equal(t, delta, report.Total)
			require.Equal(t, delta, report.PerDay[now.UTC().Format(time.DateOnly)])
		})
	}
}
