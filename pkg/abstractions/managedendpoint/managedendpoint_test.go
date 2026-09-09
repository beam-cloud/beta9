package managedendpoint

import (
	"context"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// newServiceForTest wires a Service onto miniredis with no backend or
// scheduler; RPCs that only touch the registry are exercised directly.
func newServiceForTest(t *testing.T) *Service {
	t.Helper()
	rdb, err := repository.NewRedisClientForTest()
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	s := &Service{
		ctx:            ctx,
		config:         types.ManagedEndpointsConfig{Enabled: true},
		repo:           repository.NewManagedEndpointRedisRepository(rdb),
		rdb:            rdb,
		drainCtx:       ctx,
		adminWorkspace: &types.Workspace{Id: 1, ExternalId: "admin-ws", Name: "admin"},
	}
	s.controller = newController(s)
	return s
}

func adminCtx() context.Context {
	return auth.ContextWithAuthInfo(context.Background(), &auth.AuthInfo{
		Workspace: &types.Workspace{Id: 1, ExternalId: "admin-ws"},
		Token:     &types.Token{TokenType: types.TokenTypeClusterAdmin, ExternalId: "tok"},
	})
}

func harnessCtx() context.Context {
	return auth.ContextWithAuthInfo(context.Background(), &auth.AuthInfo{
		Workspace: &types.Workspace{Id: 1, ExternalId: "admin-ws"},
		Token:     &types.Token{TokenType: types.TokenTypeWorkspaceRestricted, ExternalId: "runtime"},
	})
}

func seedEndpoint(t *testing.T, s *Service) *types.ManagedEndpoint {
	t.Helper()
	spec := types.ManagedEndpointSpec{
		ID: "acme/model", Kind: types.EndpointKindLLM, Engine: "vllm", Port: 8000,
		Gpu:     []types.GpuTarget{{Type: "H100", Count: 1, MinReplicas: 1, MaxReplicas: 4, Share: 0.5, Harness: map[string]any{"max_num_seqs": 64}}},
		Harness: types.HarnessSpec{Enabled: true},
		Catalog: types.Catalog{Public: true},
	}
	spec.Normalize()
	endpoint := &types.ManagedEndpoint{Spec: spec, StubID: "stub-1", Version: 1, Enabled: true, Status: types.EndpointStatusActive}
	require.NoError(t, s.repo.SaveEndpoint(context.Background(), endpoint))
	require.NoError(t, s.repo.SaveVersion(context.Background(), &types.EndpointVersion{EndpointID: spec.ID, Version: 1, StubID: "stub-1", State: types.VersionStateActive}))
	require.NoError(t, s.repo.SaveRollout(context.Background(), &types.RolloutState{EndpointID: spec.ID, ActiveVersion: 1, Phase: types.RolloutPhaseIdle}))
	return endpoint
}

func seedReplica(t *testing.T, s *Service, endpoint *types.ManagedEndpoint) *types.EndpointReplica {
	t.Helper()
	replica := &types.EndpointReplica{
		ID: "rep-1", EndpointID: endpoint.Spec.ID, Version: 1, Role: types.ReplicaRoleServe, GPU: "H100x1", GPUCount: 1,
		ContainerID: "managed-stub-1-abc", Status: types.ReplicaStatusScheduling, HarnessEnabled: true, StartedAt: time.Now(),
	}
	require.NoError(t, s.repo.SaveReplica(context.Background(), replica))
	return replica
}

func TestAuthorization(t *testing.T) {
	s := newServiceForTest(t)

	_, err := s.ListEndpoints(context.Background(), &pb.ListEndpointsRequest{})
	assert.Equal(t, codes.PermissionDenied, status.Code(err))

	_, err = s.ListEndpoints(harnessCtx(), &pb.ListEndpointsRequest{})
	assert.Equal(t, codes.PermissionDenied, status.Code(err), "workspace tokens cannot use admin RPCs")

	_, err = s.Heartbeat(context.Background(), &pb.HarnessHeartbeatRequest{ReplicaId: "x"})
	assert.Equal(t, codes.Unauthenticated, status.Code(err))

	foreign := auth.ContextWithAuthInfo(context.Background(), &auth.AuthInfo{
		Workspace: &types.Workspace{Id: 99, ExternalId: "other"},
		Token:     &types.Token{TokenType: types.TokenTypeWorkspace},
	})
	_, err = s.Heartbeat(foreign, &pb.HarnessHeartbeatRequest{ReplicaId: "x"})
	assert.Equal(t, codes.PermissionDenied, status.Code(err), "only admin-workspace tokens may call harness RPCs")

	disabled := newServiceForTest(t)
	disabled.config.Enabled = false
	_, err = disabled.ListEndpoints(adminCtx(), &pb.ListEndpointsRequest{})
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
}

func TestHarnessLifecycle(t *testing.T) {
	s := newServiceForTest(t)
	endpoint := seedEndpoint(t, s)
	replica := seedReplica(t, s, endpoint)
	ctx := harnessCtx()

	// Fleet config from git lands before the harness registers.
	require.NoError(t, s.controller.ensureFleetRevisions(context.Background(), endpoint))

	reg, err := s.Register(ctx, &pb.HarnessRegisterRequest{ContainerId: "nope"})
	require.NoError(t, err)
	assert.False(t, reg.Ok)

	reg, err = s.Register(ctx, &pb.HarnessRegisterRequest{ContainerId: replica.ContainerID, Engine: "vllm", CapabilitiesJson: `{"knobs":["max_num_seqs"]}`})
	require.NoError(t, err)
	require.True(t, reg.Ok, reg.ErrMsg)
	assert.Equal(t, replica.ID, reg.ReplicaId)
	assert.Equal(t, endpoint.Spec.ID, reg.EndpointId)
	require.NotNil(t, reg.Current)
	assert.Equal(t, uint64(1), reg.Current.Revision)
	assert.JSONEq(t, `{"max_num_seqs":64}`, reg.Current.ConfigJson)
	assert.Equal(t, uint32(5), reg.HeartbeatIntervalSeconds)

	stored, err := s.repo.GetReplica(context.Background(), replica.ID)
	require.NoError(t, err)
	assert.Equal(t, types.ReplicaStatusLoading, stored.Status)
	assert.JSONEq(t, `{"knobs":["max_num_seqs"]}`, string(stored.Capabilities))

	// Heartbeats drive status and capacity; drain is not requested yet.
	hb, err := s.Heartbeat(ctx, &pb.HarnessHeartbeatRequest{
		ReplicaId: replica.ID, Status: "ready", AppliedRevision: 1,
		Capacity: &pb.ReplicaCapacity{InFlight: 2, MaxConcurrency: 64, KvCacheFreeMilli: 800},
	})
	require.NoError(t, err)
	require.True(t, hb.Ok)
	assert.False(t, hb.Drain)

	stored, _ = s.repo.GetReplica(context.Background(), replica.ID)
	assert.Equal(t, types.ReplicaStatusReady, stored.Status)
	assert.False(t, stored.ReadyAt.IsZero())
	assert.Equal(t, int64(64), stored.Capacity.MaxConcurrency)
	assert.Equal(t, uint64(1), stored.ConfigRevision)

	// Ack a config revision.
	ack, err := s.AckConfig(ctx, &pb.HarnessAckConfigRequest{ReplicaId: replica.ID, Revision: 2, Applied: true, EffectiveJson: `{"max_num_seqs":128}`})
	require.NoError(t, err)
	assert.True(t, ack.Ok)
	got, err := s.repo.GetConfigAck(context.Background(), replica.ID, 2)
	require.NoError(t, err)
	assert.True(t, got.Applied)
	stored, _ = s.repo.GetReplica(context.Background(), replica.ID)
	assert.Equal(t, uint64(2), stored.ConfigRevision)

	// A drain request surfaces on the next heartbeat; control-plane states are sticky.
	require.NoError(t, s.controller.drainReplica(context.Background(), stored, 30, false, "test"))
	hb, err = s.Heartbeat(ctx, &pb.HarnessHeartbeatRequest{ReplicaId: replica.ID, Status: "ready"})
	require.NoError(t, err)
	assert.True(t, hb.Drain)
	assert.Equal(t, uint32(30), hb.DrainSeconds)
	stored, _ = s.repo.GetReplica(context.Background(), replica.ID)
	assert.Equal(t, types.ReplicaStatusDraining, stored.Status)

	events, err := s.PublishEvents(ctx, &pb.HarnessPublishEventsRequest{ReplicaId: replica.ID, Events: []*pb.Event{
		{Name: "engine.started", PayloadJson: `{"took_ms": 1200}`},
		{Name: ""},
	}})
	require.NoError(t, err)
	assert.Equal(t, uint32(1), events.Accepted)
}

// fakeWatchStream captures streamed revisions.
type fakeWatchStream struct {
	grpc.ServerStream
	ctx  context.Context
	sent chan *pb.ConfigRevision
}

func (f *fakeWatchStream) Context() context.Context        { return f.ctx }
func (f *fakeWatchStream) Send(r *pb.ConfigRevision) error { f.sent <- r; return nil }
func (f *fakeWatchStream) SetHeader(metadata.MD) error     { return nil }
func (f *fakeWatchStream) SendHeader(metadata.MD) error    { return nil }
func (f *fakeWatchStream) SetTrailer(metadata.MD)          {}

func TestWatchConfigStreamsFleetAndReplicaRevisions(t *testing.T) {
	s := newServiceForTest(t)
	endpoint := seedEndpoint(t, s)
	replica := seedReplica(t, s, endpoint)
	require.NoError(t, s.controller.ensureFleetRevisions(context.Background(), endpoint))

	ctx, cancel := context.WithCancel(harnessCtx())
	defer cancel()
	stream := &fakeWatchStream{ctx: ctx, sent: make(chan *pb.ConfigRevision, 8)}
	done := make(chan error, 1)
	go func() { done <- s.WatchConfig(&pb.HarnessWatchConfigRequest{ReplicaId: replica.ID}, stream) }()

	first := <-stream.sent
	assert.Equal(t, uint64(1), first.Revision)
	assert.Equal(t, "target", first.Scope)

	// A new fleet revision for this target is pushed.
	require.NoError(t, s.repo.CreateConfigRevision(context.Background(), &types.EndpointConfigRevision{
		EndpointID: endpoint.Spec.ID, Scope: types.ConfigScopeTarget, ScopeKey: "serve:H100x1@v1",
		Config: map[string]any{"max_num_seqs": 96}, Author: "live@v1:agent", Source: types.ConfigSourceLive,
	}))
	select {
	case second := <-stream.sent:
		assert.Equal(t, uint64(2), second.Revision)
		assert.JSONEq(t, `{"max_num_seqs":96}`, second.ConfigJson)
	case <-time.After(3 * time.Second):
		t.Fatal("fleet revision not streamed")
	}

	// Revisions for other targets are filtered out.
	require.NoError(t, s.repo.CreateConfigRevision(context.Background(), &types.EndpointConfigRevision{
		EndpointID: endpoint.Spec.ID, Scope: types.ConfigScopeTarget, ScopeKey: "serve:A100x1@v1", Config: map[string]any{}, Source: types.ConfigSourceGit,
	}))
	// Replica-scoped revisions reach only that replica.
	require.NoError(t, s.repo.CreateConfigRevision(context.Background(), &types.EndpointConfigRevision{
		EndpointID: endpoint.Spec.ID, Scope: types.ConfigScopeReplica, ScopeKey: replica.ID, Config: map[string]any{"max_num_seqs": 8}, Source: types.ConfigSourceLive,
	}))
	select {
	case third := <-stream.sent:
		assert.Equal(t, uint64(4), third.Revision)
		assert.Equal(t, "replica", third.Scope)
	case <-time.After(3 * time.Second):
		t.Fatal("replica revision not streamed")
	}

	cancel()
	select {
	case err := <-done:
		assert.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("watch did not exit on cancel")
	}
}

func TestAdminReadAndRolloutRPCs(t *testing.T) {
	s := newServiceForTest(t)
	endpoint := seedEndpoint(t, s)
	replica := seedReplica(t, s, endpoint)
	replica.Status = types.ReplicaStatusReady
	require.NoError(t, s.repo.SaveReplica(context.Background(), replica))
	require.NoError(t, s.controller.ensureFleetRevisions(context.Background(), endpoint))
	ctx := adminCtx()

	list, err := s.ListEndpoints(ctx, &pb.ListEndpointsRequest{})
	require.NoError(t, err)
	require.True(t, list.Ok)
	require.Len(t, list.Endpoints, 1)
	assert.Equal(t, uint32(1), list.Endpoints[0].ReadyReplicas)

	get, err := s.GetEndpoint(ctx, &pb.GetEndpointRequest{EndpointId: endpoint.Spec.ID})
	require.NoError(t, err)
	require.True(t, get.Ok)
	assert.Len(t, get.Replicas, 1)
	assert.Equal(t, uint32(1), get.Rollout.ActiveVersion)
	assert.Len(t, get.Rollout.Versions, 1)

	cfg, err := s.GetConfig(ctx, &pb.GetConfigRequest{EndpointId: endpoint.Spec.ID, ScopeKey: "H100x1"})
	require.NoError(t, err)
	require.True(t, cfg.Ok, cfg.ErrMsg)
	assert.JSONEq(t, `{"max_num_seqs":64}`, cfg.Revision.ConfigJson)

	revs, err := s.ListConfigRevisions(ctx, &pb.ListConfigRevisionsRequest{EndpointId: endpoint.Spec.ID, ScopeKey: "serve:H100x1"})
	require.NoError(t, err)
	assert.Len(t, revs.Revisions, 1)

	pin, err := s.PinVersion(ctx, &pb.PinVersionRequest{EndpointId: endpoint.Spec.ID, Version: 1})
	require.NoError(t, err)
	require.True(t, pin.Ok)
	assert.Equal(t, uint32(1), pin.Rollout.PinnedVersion)
	pin, err = s.PinVersion(ctx, &pb.PinVersionRequest{EndpointId: endpoint.Spec.ID, Version: 9})
	require.NoError(t, err)
	assert.False(t, pin.Ok)

	action, err := s.PromoteRollout(ctx, &pb.PromoteRolloutRequest{EndpointId: endpoint.Spec.ID})
	require.NoError(t, err)
	assert.False(t, action.Ok, "nothing baking")
	action, err = s.RollbackRollout(ctx, &pb.RollbackRolloutRequest{EndpointId: endpoint.Spec.ID})
	require.NoError(t, err)
	assert.False(t, action.Ok, "no previous version")

	toggled, err := s.SetEndpointEnabled(ctx, &pb.SetEndpointEnabledRequest{EndpointId: endpoint.Spec.ID, Enabled: false})
	require.NoError(t, err)
	require.True(t, toggled.Ok)
	assert.False(t, toggled.Endpoint.Enabled)
	list, _ = s.ListEndpoints(ctx, &pb.ListEndpointsRequest{})
	assert.Empty(t, list.Endpoints)
	list, _ = s.ListEndpoints(ctx, &pb.ListEndpointsRequest{IncludeDisabled: true})
	assert.Len(t, list.Endpoints, 1)

	metrics, err := s.GetMetrics(ctx, &pb.GetMetricsRequest{EndpointId: endpoint.Spec.ID, Gpu: "H100x1"})
	require.NoError(t, err)
	require.True(t, metrics.Ok)
	assert.Len(t, metrics.Replicas, 1)

	valid, err := s.ValidateEndpointSpec(ctx, &pb.ValidateEndpointSpecRequest{SpecJson: `{"id":"a/b","kind":"llm","engine":"vllm","port":8000,"gpu":[{"type":"H100","count":1,"share":0.2}],"entrypoint":["vllm","serve"]}`})
	require.NoError(t, err)
	assert.True(t, valid.Ok, valid.ErrMsg)
	assert.Contains(t, valid.SpecJson, `"chat/completions"`)

	invalid, err := s.ValidateEndpointSpec(ctx, &pb.ValidateEndpointSpecRequest{SpecJson: `{"id":"BAD ID","kind":"llm"}`})
	require.NoError(t, err)
	assert.False(t, invalid.Ok)
	assert.NotEmpty(t, invalid.Errors)

	gitops, err := s.TriggerGitOpsSync(ctx, &pb.TriggerGitOpsSyncRequest{})
	require.NoError(t, err)
	assert.False(t, gitops.Ok)
}

func TestExperimentRequiresHarnessAndKnownTarget(t *testing.T) {
	s := newServiceForTest(t)
	endpoint := seedEndpoint(t, s)
	ctx := adminCtx()

	resp, err := s.StartExperiment(ctx, &pb.StartExperimentRequest{EndpointId: endpoint.Spec.ID, Gpu: "A100"})
	require.NoError(t, err)
	assert.False(t, resp.Ok)
	assert.Contains(t, resp.ErrMsg, "no target")

	endpoint.Spec.Harness.Enabled = false
	require.NoError(t, s.repo.SaveEndpoint(context.Background(), endpoint))
	resp, err = s.StartExperiment(ctx, &pb.StartExperimentRequest{EndpointId: endpoint.Spec.ID, Gpu: "H100"})
	require.NoError(t, err)
	assert.False(t, resp.Ok)
	assert.Contains(t, resp.ErrMsg, "harness")

	resp, err = s.GetExperiment(ctx, &pb.GetExperimentRequest{ExperimentId: "missing"})
	require.NoError(t, err)
	assert.False(t, resp.Ok)
}

func TestFleetRevisionsFollowVersions(t *testing.T) {
	s := newServiceForTest(t)
	endpoint := seedEndpoint(t, s)
	ctx := context.Background()

	require.NoError(t, s.controller.ensureFleetRevisions(ctx, endpoint))
	require.NoError(t, s.controller.ensureFleetRevisions(ctx, endpoint))
	revs, _ := s.repo.ListConfigRevisions(ctx, endpoint.Spec.ID, types.ConfigScopeTarget, "serve:H100x1@v1", 10)
	assert.Len(t, revs, 1, "idempotent for the same version")

	// A live fleet edit on this version is preserved.
	require.NoError(t, s.repo.CreateConfigRevision(ctx, &types.EndpointConfigRevision{
		EndpointID: endpoint.Spec.ID, Scope: types.ConfigScopeTarget, ScopeKey: "serve:H100x1@v1",
		Config: map[string]any{"max_num_seqs": 200}, Author: "live@v1:agent", Source: types.ConfigSourceLive,
	}))
	require.NoError(t, s.controller.ensureFleetRevisions(ctx, endpoint))
	latest, _ := s.repo.LatestConfigRevision(ctx, endpoint.Spec.ID, types.ConfigScopeTarget, "serve:H100x1@v1")
	require.NotNil(t, latest)
	assert.Equal(t, float64(200), latest.Config["max_num_seqs"])

	// A new version gets its own stream seeded from git; the old version's
	// stream (still serving until promotion) is untouched, so an active
	// fleet and a baking canary never flip-flop over one "latest".
	canary := *endpoint
	canary.Version = 2
	canary.Spec.Gpu[0].Harness = map[string]any{"max_num_seqs": 32}
	require.NoError(t, s.controller.ensureFleetRevisions(ctx, &canary))
	require.NoError(t, s.controller.ensureFleetRevisions(ctx, endpoint))
	require.NoError(t, s.controller.ensureFleetRevisions(ctx, &canary))

	v2, _ := s.repo.LatestConfigRevision(ctx, endpoint.Spec.ID, types.ConfigScopeTarget, "serve:H100x1@v2")
	require.NotNil(t, v2)
	assert.Equal(t, float64(32), v2.Config["max_num_seqs"])
	assert.Equal(t, types.ConfigSourceGit, v2.Source)
	v1, _ := s.repo.LatestConfigRevision(ctx, endpoint.Spec.ID, types.ConfigScopeTarget, "serve:H100x1@v1")
	assert.Equal(t, latest.Revision, v1.Revision, "live edit on v1 survives the canary")
	v2revs, _ := s.repo.ListConfigRevisions(ctx, endpoint.Spec.ID, types.ConfigScopeTarget, "serve:H100x1@v2", 10)
	assert.Len(t, v2revs, 1)

	// Admin reads default to the active version and accept an explicit one.
	ctx = adminCtx()
	cfg, err := s.GetConfig(ctx, &pb.GetConfigRequest{EndpointId: endpoint.Spec.ID, ScopeKey: "H100x1"})
	require.NoError(t, err)
	require.True(t, cfg.Ok, cfg.ErrMsg)
	assert.Equal(t, "serve:H100x1@v1", cfg.Revision.ScopeKey)
	cfg, err = s.GetConfig(ctx, &pb.GetConfigRequest{EndpointId: endpoint.Spec.ID, ScopeKey: "serve:H100x1@v2"})
	require.NoError(t, err)
	require.True(t, cfg.Ok, cfg.ErrMsg)
	assert.JSONEq(t, `{"max_num_seqs":32}`, cfg.Revision.ConfigJson)
}
