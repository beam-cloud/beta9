package managedendpoint

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/abstractions/common/llmroute"
	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/labstack/echo/v4"
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
	config := types.ManagedEndpointsConfig{Enabled: true}
	config.ApplyDefaults()
	s := &Service{
		ctx:            ctx,
		config:         config,
		repo:           repository.NewManagedEndpointRedisRepository(rdb),
		rdb:            rdb,
		drainCtx:       ctx,
		adminWorkspace: &types.Workspace{Id: 1, ExternalId: "admin-ws", Name: "admin"},
	}
	s.controller = newController(s)
	s.meter = newMeter(s)
	return s
}

func adminCtx() context.Context {
	return auth.ContextWithAuthInfo(context.Background(), &auth.AuthInfo{
		Workspace: &types.Workspace{Id: 1, ExternalId: "admin-ws"},
		Token:     &types.Token{TokenType: types.TokenTypeClusterAdmin, ExternalId: "tok"},
	})
}

const testReplicaSecret = "replica-secret-1"

// harnessCtx is what a replica container presents: no workspace token, only
// its own replica secret in gRPC metadata.
func harnessCtx() context.Context {
	return metadata.NewIncomingContext(context.Background(), metadata.Pairs(replicaSecretHeader, testReplicaSecret))
}

// seedEndpoint registers acme/model (an H100 vLLM endpoint with the harness)
// as version 1 and places it on H100 through the fleet.
func seedEndpoint(t *testing.T, s *Service) *types.ManagedEndpoint {
	t.Helper()
	spec := types.ManagedEndpointSpec{
		ID: "acme/model", Kind: types.EndpointKindLLM, Engine: "vllm", Port: 8000, Entrypoint: []string{"vllm", "serve"},
		Gpu:          map[string]types.GpuSpec{"H100": {Config: map[string]any{"max_num_seqs": 64}}},
		DrainSeconds: 5,
		Public:       true,
	}
	spec.Normalize()
	endpoint := &types.ManagedEndpoint{Spec: spec, StubID: "stub-1", Version: 1, Status: types.EndpointStatusActive}
	require.NoError(t, s.repo.SaveEndpoint(context.Background(), endpoint))
	seedFleet(t, s, map[string]types.FleetEndpoint{spec.ID: {Enabled: true, GPUs: map[string]types.FleetPlacement{"H100": {Priority: 1, MaxReplicas: 2}}}})
	return endpoint
}

func seedFleet(t *testing.T, s *Service, endpoints map[string]types.FleetEndpoint) *types.Fleet {
	t.Helper()
	fleet := &types.Fleet{GitSHA: "fleet-sha", Endpoints: endpoints}
	fleet.Normalize()
	require.NoError(t, s.repo.SaveFleet(context.Background(), fleet))
	return fleet
}

func seedReplica(t *testing.T, s *Service, endpoint *types.ManagedEndpoint) *types.EndpointReplica {
	t.Helper()
	replica := &types.EndpointReplica{
		ID: "rep-1", EndpointID: endpoint.Spec.ID, Version: 1, GPU: "H100", GPUCount: 1,
		ContainerID: "managed-stub-1-abc", Status: types.ReplicaStatusScheduling, HarnessEnabled: true, StartedAt: time.Now(),
		SecretHash: hashReplicaSecret(testReplicaSecret), Probe: probeFor(&endpoint.Spec),
	}
	require.NoError(t, s.repo.SaveReplica(context.Background(), replica))
	return replica
}

func TestAuthorization(t *testing.T) {
	s := newServiceForTest(t)

	_, err := s.ListEndpoints(context.Background(), &pb.ListEndpointsRequest{})
	assert.Equal(t, codes.PermissionDenied, status.Code(err))

	workspaceToken := auth.ContextWithAuthInfo(context.Background(), &auth.AuthInfo{
		Workspace: &types.Workspace{Id: 1, ExternalId: "admin-ws"},
		Token:     &types.Token{TokenType: types.TokenTypeWorkspace, ExternalId: "tok"},
	})
	_, err = s.ListEndpoints(workspaceToken, &pb.ListEndpointsRequest{})
	assert.Equal(t, codes.PermissionDenied, status.Code(err), "workspace tokens cannot use admin RPCs")

	// Harness RPCs are authorized by the replica secret alone; an unknown
	// replica has none to compare against.
	_, err = s.Heartbeat(context.Background(), &pb.HarnessHeartbeatRequest{ReplicaId: "x"})
	assert.Equal(t, codes.NotFound, status.Code(err))

	// Neither a workspace token nor a cluster-admin token stands in for the
	// replica's own secret.
	endpoint := seedEndpoint(t, s)
	replica := seedReplica(t, s, endpoint)
	_, err = s.Heartbeat(context.Background(), &pb.HarnessHeartbeatRequest{ReplicaId: replica.ID})
	assert.Equal(t, codes.PermissionDenied, status.Code(err))
	_, err = s.Heartbeat(workspaceToken, &pb.HarnessHeartbeatRequest{ReplicaId: replica.ID})
	assert.Equal(t, codes.PermissionDenied, status.Code(err))
	_, err = s.Heartbeat(adminCtx(), &pb.HarnessHeartbeatRequest{ReplicaId: replica.ID})
	assert.Equal(t, codes.PermissionDenied, status.Code(err), "no cluster-admin bypass for harness RPCs")
	wrong := metadata.NewIncomingContext(context.Background(), metadata.Pairs(replicaSecretHeader, "someone-else"))
	_, err = s.Register(wrong, &pb.HarnessRegisterRequest{ContainerId: replica.ContainerID})
	assert.Equal(t, codes.PermissionDenied, status.Code(err))
	_, err = s.PublishEvents(wrong, &pb.HarnessPublishEventsRequest{ReplicaId: replica.ID})
	assert.Equal(t, codes.PermissionDenied, status.Code(err))
	_, err = s.AckConfig(wrong, &pb.HarnessAckConfigRequest{ReplicaId: replica.ID, Revision: 1})
	assert.Equal(t, codes.PermissionDenied, status.Code(err))
	hb, err := s.Heartbeat(harnessCtx(), &pb.HarnessHeartbeatRequest{ReplicaId: replica.ID, Status: "ready"})
	require.NoError(t, err)
	assert.True(t, hb.Ok)

	disabled := newServiceForTest(t)
	disabled.config.Enabled = false
	_, err = disabled.ListEndpoints(adminCtx(), &pb.ListEndpointsRequest{})
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
}

func TestHarnessLifecycle(t *testing.T) {
	s := newServiceForTest(t)
	endpoint := seedEndpoint(t, s)
	replica := seedReplica(t, s, endpoint)
	replica.HarnessEnabled = false // Instrumented engines opt in by authenticated registration.
	require.NoError(t, s.repo.SaveReplica(context.Background(), replica))
	ctx := harnessCtx()
	health := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer health.Close()
	probe := func() {
		_, err := s.updateReplica(context.Background(), replica.ID, func(r *types.EndpointReplica) {
			r.Address = strings.TrimPrefix(health.URL, "http://")
			s.controller.probeReplica(context.Background(), r)
		})
		require.NoError(t, err)
	}

	reg, err := s.Register(ctx, &pb.HarnessRegisterRequest{ContainerId: "nope"})
	require.NoError(t, err)
	assert.False(t, reg.Ok)

	reg, err = s.Register(ctx, &pb.HarnessRegisterRequest{ContainerId: replica.ContainerID, Engine: "vllm", CapabilitiesJson: `{"knobs":["max_num_seqs"]}`})
	require.NoError(t, err)
	require.True(t, reg.Ok, reg.ErrMsg)
	assert.Equal(t, replica.ID, reg.ReplicaId)
	assert.Equal(t, endpoint.Spec.ID, reg.EndpointId)
	assert.Equal(t, "H100", reg.Gpu)
	assert.Nil(t, reg.Current, "no live config has been pushed yet; the harness seed comes from the environment")
	assert.Equal(t, uint32(5), reg.HeartbeatIntervalSeconds)

	stored, err := s.repo.GetReplica(context.Background(), replica.ID)
	require.NoError(t, err)
	assert.True(t, stored.HarnessEnabled)
	assert.Equal(t, types.ReplicaStatusLoading, stored.Status)
	assert.JSONEq(t, `{"knobs":["max_num_seqs"]}`, string(stored.Capabilities))

	// Heartbeats report engine readiness and capacity; HTTP health gates routing.
	hb, err := s.Heartbeat(ctx, &pb.HarnessHeartbeatRequest{
		ReplicaId: replica.ID, Status: "ready",
		Capacity:    &pb.ReplicaCapacity{InFlight: 2, MaxConcurrency: 64, KvCacheFreeMilli: 800},
		MetricsJson: `{"running":2}`,
	})
	require.NoError(t, err)
	require.True(t, hb.Ok)
	assert.False(t, hb.Drain)

	stored, _ = s.repo.GetReplica(context.Background(), replica.ID)
	assert.Equal(t, types.ReplicaStatusLoading, stored.Status)
	assert.True(t, stored.EngineReady)
	probe()
	stored, _ = s.repo.GetReplica(context.Background(), replica.ID)
	assert.Equal(t, types.ReplicaStatusReady, stored.Status)
	assert.False(t, stored.ReadyAt.IsZero())
	assert.Equal(t, int64(64), stored.Capacity.MaxConcurrency)
	assert.JSONEq(t, `{"running":2}`, string(stored.EngineMetrics))
	assert.Equal(t, uint64(0), stored.Config.Revision)

	// An engine that reloads reports loading and leaves the serving set until
	// it is ready again.
	_, err = s.Heartbeat(ctx, &pb.HarnessHeartbeatRequest{ReplicaId: replica.ID, Status: "loading"})
	require.NoError(t, err)
	stored, _ = s.repo.GetReplica(context.Background(), replica.ID)
	assert.Equal(t, types.ReplicaStatusLoading, stored.Status)
	assert.False(t, stored.Serving())
	_, err = s.Heartbeat(ctx, &pb.HarnessHeartbeatRequest{ReplicaId: replica.ID, Status: "ready"})
	require.NoError(t, err)
	stored, _ = s.repo.GetReplica(context.Background(), replica.ID)
	assert.Equal(t, types.ReplicaStatusLoading, stored.Status)
	assert.True(t, stored.EngineReady)
	probe()
	stored, _ = s.repo.GetReplica(context.Background(), replica.ID)
	assert.Equal(t, types.ReplicaStatusReady, stored.Status)

	// A config revision is pushed by an admin and acked by the harness.
	_, err = s.updateReplica(context.Background(), replica.ID, func(r *types.EndpointReplica) {
		r.Config.Revision, r.Config.Config = 2, []byte(`{"max_num_seqs":128}`)
	})
	require.NoError(t, err)
	ack, err := s.AckConfig(ctx, &pb.HarnessAckConfigRequest{ReplicaId: replica.ID, Revision: 2, Applied: true, EffectiveJson: `{"max_num_seqs":128}`})
	require.NoError(t, err)
	assert.True(t, ack.Ok)
	stored, _ = s.repo.GetReplica(context.Background(), replica.ID)
	assert.Equal(t, uint64(2), stored.Config.AckedRevision)
	assert.True(t, stored.Config.Applied)
	assert.True(t, stored.Config.Acked())
	assert.JSONEq(t, `{"max_num_seqs":128}`, string(stored.Config.Effective))
	assert.False(t, stored.Config.AckedAt.IsZero())

	// A stale ack never moves the acked revision backwards.
	_, err = s.AckConfig(ctx, &pb.HarnessAckConfigRequest{ReplicaId: replica.ID, Revision: 1, Applied: false, Error: "old"})
	require.NoError(t, err)
	stored, _ = s.repo.GetReplica(context.Background(), replica.ID)
	assert.Equal(t, uint64(2), stored.Config.AckedRevision)
	assert.True(t, stored.Config.Applied)

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

// fakeWatchStream captures streamed configs.
type fakeWatchStream struct {
	grpc.ServerStream
	ctx  context.Context
	sent chan *pb.ReplicaConfig
}

func (f *fakeWatchStream) Context() context.Context       { return f.ctx }
func (f *fakeWatchStream) Send(r *pb.ReplicaConfig) error { f.sent <- r; return nil }
func (f *fakeWatchStream) SetHeader(metadata.MD) error    { return nil }
func (f *fakeWatchStream) SendHeader(metadata.MD) error   { return nil }
func (f *fakeWatchStream) SetTrailer(metadata.MD)         {}

func TestWatchConfigStreamsReplicaConfig(t *testing.T) {
	s := newServiceForTest(t)
	endpoint := seedEndpoint(t, s)
	replica := seedReplica(t, s, endpoint)

	ctx, cancel := context.WithCancel(harnessCtx())
	defer cancel()
	stream := &fakeWatchStream{ctx: ctx, sent: make(chan *pb.ReplicaConfig, 8)}
	done := make(chan error, 1)
	go func() { done <- s.WatchConfig(&pb.HarnessWatchConfigRequest{ReplicaId: replica.ID}, stream) }()

	// Nothing is sent until an admin pushes a revision.
	select {
	case first := <-stream.sent:
		t.Fatalf("unexpected config before any revision: %v", first)
	case <-time.After(200 * time.Millisecond):
	}

	set := make(chan *pb.SetReplicaConfigResponse, 1)
	go func() {
		resp, err := s.SetReplicaConfig(adminCtx(), &pb.SetReplicaConfigRequest{ReplicaId: replica.ID, ConfigJson: `{"max_num_seqs":96}`, Author: "agent", WaitSeconds: 10})
		if err != nil {
			resp = &pb.SetReplicaConfigResponse{ErrMsg: err.Error()}
		}
		set <- resp
	}()

	var pushed *pb.ReplicaConfig
	select {
	case pushed = <-stream.sent:
	case <-time.After(3 * time.Second):
		t.Fatal("config revision not streamed")
	}
	assert.Equal(t, uint64(1), pushed.Revision)
	assert.Equal(t, "agent", pushed.Author)
	assert.JSONEq(t, `{"max_num_seqs":96}`, pushed.ConfigJson)
	assert.Equal(t, uint64(0), pushed.AckedRevision)

	// The admin call is still waiting for the ack ...
	select {
	case resp := <-set:
		t.Fatalf("SetReplicaConfig returned before the harness acked: %v", resp)
	case <-time.After(100 * time.Millisecond):
	}

	// ... and returns the acked replica once the harness answers.
	_, err := s.AckConfig(harnessCtx(), &pb.HarnessAckConfigRequest{ReplicaId: replica.ID, Revision: pushed.Revision, Applied: true, EffectiveJson: `{"max_num_seqs":96}`})
	require.NoError(t, err)
	select {
	case resp := <-set:
		require.True(t, resp.Ok, resp.ErrMsg)
		require.NotNil(t, resp.Replica.Config)
		assert.Equal(t, uint64(1), resp.Replica.Config.Revision)
		assert.Equal(t, uint64(1), resp.Replica.Config.AckedRevision)
		assert.True(t, resp.Replica.Config.Applied)
		assert.JSONEq(t, `{"max_num_seqs":96}`, resp.Replica.Config.EffectiveJson)
	case <-time.After(3 * time.Second):
		t.Fatal("SetReplicaConfig did not return after the ack")
	}

	// Another replica's revision is not delivered here.
	other := &types.EndpointReplica{ID: "rep-2", EndpointID: endpoint.Spec.ID, Version: 1, GPU: "H100", ContainerID: "managed-stub-1-def", Status: types.ReplicaStatusReady, HarnessEnabled: true, StartedAt: time.Now()}
	require.NoError(t, s.repo.SaveReplica(context.Background(), other))
	resp, err := s.SetReplicaConfig(adminCtx(), &pb.SetReplicaConfigRequest{ReplicaId: other.ID, ConfigJson: `{"max_num_seqs":8}`, WaitSeconds: 1})
	require.NoError(t, err)
	require.True(t, resp.Ok, resp.ErrMsg)
	assert.Equal(t, "admin", resp.Replica.Config.Author)
	assert.Equal(t, uint64(0), resp.Replica.Config.AckedRevision, "returned unacked after the wait")
	select {
	case leaked := <-stream.sent:
		t.Fatalf("revision for another replica leaked: %v", leaked)
	case <-time.After(200 * time.Millisecond):
	}

	cancel()
	select {
	case err := <-done:
		assert.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("watch did not exit on cancel")
	}
}

// capturingEvents records endpoint.* events; every other push is dropped.
type capturingEvents struct {
	repository.EventRepository
	events []types.EventEndpointSchema
}

func (c *capturingEvents) PushEndpointEvent(_ string, event types.EventEndpointSchema) {
	c.events = append(c.events, event)
}
func (c *capturingEvents) PushEndpointRouteEvent(types.EventEndpointRouteSchema) {}

func (c *capturingEvents) find(action string) *types.EventEndpointSchema {
	for i := range c.events {
		if c.events[i].Action == action {
			return &c.events[i]
		}
	}
	return nil
}

type notifyFailingRepo struct {
	repository.ManagedEndpointRepository
}

func (notifyFailingRepo) NotifyReplicaConfig(context.Context, string, uint64) error {
	return errors.New("synthetic publish outage")
}

// A live config change is audited the moment it is saved (the harness applies
// it on its next keepalive read even if the wakeup fails), with the
// authenticated actor; the ack event carries requested and effective config so
// the history is complete without the replica record.
func TestConfigHistoryIsCompleteAndBounded(t *testing.T) {
	s := newServiceForTest(t)
	endpoint := seedEndpoint(t, s)
	replica := seedReplica(t, s, endpoint)
	stubID := "5e3e31ff-aef4-40b6-a98d-439268a9832e"
	replica.ContainerID = "managed-" + stubID + "-1717f4fc"
	require.NoError(t, s.repo.SaveReplica(context.Background(), replica))
	events := &capturingEvents{}
	s.events = events
	s.repo = notifyFailingRepo{s.repo}

	resp, err := s.SetReplicaConfig(adminCtx(), &pb.SetReplicaConfigRequest{ReplicaId: replica.ID, ConfigJson: `{"max_num_seqs":96}`, Author: "agent", WaitSeconds: 1})
	require.NoError(t, err)
	assert.True(t, resp.Ok, resp.ErrMsg)
	set := events.find("config.set")
	require.NotNil(t, set, "the revision is history even though the wakeup failed")
	assert.Equal(t, "admin-ws", set.WorkspaceID)
	assert.Equal(t, stubID, set.StubID)
	assert.EqualValues(t, 1, set.Revision)
	assert.Equal(t, "tok", set.Data["actor"])
	assert.Equal(t, "agent", set.Data["author"])
	assert.Equal(t, "tok", resp.Replica.Config.Actor)

	// An ack for a revision that was never issued is refused and leaves the
	// cursor alone; so does a heartbeat claiming one.
	ack, err := s.AckConfig(harnessCtx(), &pb.HarnessAckConfigRequest{ReplicaId: replica.ID, Revision: 100, Applied: true})
	require.NoError(t, err)
	assert.False(t, ack.Ok)
	_, err = s.Heartbeat(harnessCtx(), &pb.HarnessHeartbeatRequest{ReplicaId: replica.ID, Status: "ready", AppliedRevision: 100})
	require.NoError(t, err)
	stored, _ := s.repo.GetReplica(context.Background(), replica.ID)
	assert.EqualValues(t, 0, stored.Config.AckedRevision)
	assert.False(t, stored.Config.Acked())

	ack, err = s.AckConfig(harnessCtx(), &pb.HarnessAckConfigRequest{ReplicaId: replica.ID, Revision: 1, Applied: true, EffectiveJson: `{"max_num_seqs":64}`})
	require.NoError(t, err)
	assert.True(t, ack.Ok)
	applied := events.find("config.applied")
	require.NotNil(t, applied)
	assert.Equal(t, "admin-ws", applied.WorkspaceID)
	assert.Equal(t, stubID, applied.StubID)
	assert.EqualValues(t, 1, applied.Revision)
	assert.JSONEq(t, `{"max_num_seqs":96}`, string(applied.Data["requested"].(json.RawMessage)))
	assert.JSONEq(t, `{"max_num_seqs":64}`, string(applied.Data["effective"].(json.RawMessage)), "the engine's effective value, not the request")
	assert.Equal(t, "tok", applied.Data["actor"])

	// Requests served under the acknowledged revision are attributed to it.
	stored, _ = s.repo.GetReplica(context.Background(), replica.ID)
	assert.EqualValues(t, 1, stored.Config.AckedRevision)
}

func TestAdminReadRPCs(t *testing.T) {
	s := newServiceForTest(t)
	endpoint := seedEndpoint(t, s)
	replica := seedReplica(t, s, endpoint)
	replica.Status = types.ReplicaStatusReady
	require.NoError(t, s.repo.SaveReplica(context.Background(), replica))
	ctx := adminCtx()

	list, err := s.ListEndpoints(ctx, &pb.ListEndpointsRequest{})
	require.NoError(t, err)
	require.True(t, list.Ok)
	require.Len(t, list.Endpoints, 1)
	assert.Equal(t, uint32(1), list.Endpoints[0].ReadyReplicas)
	assert.Equal(t, uint32(1), list.Endpoints[0].TotalReplicas)
	assert.Equal(t, string(types.EndpointStatusActive), list.Endpoints[0].Status)
	assert.JSONEq(t, `{"H100":{"priority":1,"max_replicas":2}}`, list.Endpoints[0].PlacementsJson)

	get, err := s.GetEndpoint(ctx, &pb.GetEndpointRequest{EndpointId: endpoint.Spec.ID})
	require.NoError(t, err)
	require.True(t, get.Ok)
	assert.Len(t, get.Replicas, 1)
	assert.Equal(t, endpoint.Spec.ID, get.Endpoint.Id)
	assert.Contains(t, get.Endpoint.SpecJson, `"H100"`)
	get, err = s.GetEndpoint(ctx, &pb.GetEndpointRequest{EndpointId: "acme/missing"})
	require.NoError(t, err)
	assert.False(t, get.Ok)
	assert.Contains(t, get.ErrMsg, "not found")

	replicas, err := s.ListReplicas(ctx, &pb.ListReplicasRequest{Gpu: "h100", Status: "ready"})
	require.NoError(t, err)
	require.True(t, replicas.Ok)
	assert.Len(t, replicas.Replicas, 1)
	replicas, err = s.ListReplicas(ctx, &pb.ListReplicasRequest{EndpointId: endpoint.Spec.ID, Gpu: "A100"})
	require.NoError(t, err)
	assert.Empty(t, replicas.Replicas)

	metrics, err := s.GetMetrics(ctx, &pb.GetMetricsRequest{EndpointId: endpoint.Spec.ID, Gpu: "H100"})
	require.NoError(t, err)
	require.True(t, metrics.Ok)
	assert.Len(t, metrics.Replicas, 1)
	assert.Equal(t, uint32(1), metrics.Metrics.ReadyReplicas)

	stop, err := s.StopReplica(ctx, &pb.StopReplicaRequest{ReplicaId: "missing"})
	require.NoError(t, err)
	assert.False(t, stop.Ok)
	stop, err = s.StopReplica(ctx, &pb.StopReplicaRequest{ReplicaId: replica.ID, DrainSeconds: 30})
	require.NoError(t, err)
	require.True(t, stop.Ok, stop.ErrMsg)
	assert.Equal(t, string(types.ReplicaStatusDraining), stop.Replica.Status)
	list, _ = s.ListEndpoints(ctx, &pb.ListEndpointsRequest{})
	assert.Equal(t, uint32(0), list.Endpoints[0].ReadyReplicas)

	gitops, err := s.GetGitOpsStatus(ctx, &pb.GetGitOpsStatusRequest{})
	require.NoError(t, err)
	require.True(t, gitops.Ok)
	assert.JSONEq(t, `{"acme/model":{"enabled":true,"gpus":{"H100":{"priority":1,"max_replicas":2}}}}`, gitops.FleetJson)
	sync, err := s.TriggerGitOpsSync(ctx, &pb.TriggerGitOpsSyncRequest{})
	require.NoError(t, err)
	assert.False(t, sync.Ok, "no repo configured")
}

func TestRouteRecordCreditsProviderWorkspace(t *testing.T) {
	s := newServiceForTest(t)
	r := &router{s: s}
	now := time.Now()
	endpoint := &types.ManagedEndpoint{Spec: types.ManagedEndpointSpec{ID: "acme/model", Pricing: types.Pricing{CompletionTokens: "0.000001"}}}
	replica := &types.EndpointReplica{ID: "rep-1", GPU: "H100", MachineID: "machine-a", ProviderWorkspaceID: "ws-provider"}
	rq := &routeRequest{
		auth:      &auth.AuthInfo{Workspace: &types.Workspace{ExternalId: "ws-tenant"}, Token: &types.Token{ExternalId: "tok"}},
		requestID: "req-1", route: types.EndpointRouteChatCompletions, models: []string{"acme/model"}, startedAt: time.Now(),
	}
	ctx := context.Background()
	// record persists synchronously; the generation record is the event as written.
	generation := func(requestID string) *types.EventEndpointRouteSchema {
		t.Helper()
		record, err := s.repo.GetGeneration(ctx, requestID)
		require.NoError(t, err)
		require.NotNil(t, record)
		return record
	}

	r.record(rq, endpoint, replica, 200, Usage{CompletionTokens: 1000, Found: true}, 0, "")
	event := generation("req-1")
	require.Equal(t, int64(1000), event.CostMicroUSD)
	require.Equal(t, "ws-provider", event.ProviderWorkspaceID)
	require.Equal(t, "machine-a", event.MachineID)
	require.Equal(t, int64(700), event.ProviderShareMicroUSD) // default 70% share
	require.Equal(t, "acme/model", event.Model)
	require.Equal(t, "ws-tenant", event.WorkspaceID)

	spend, err := s.repo.GetUsage(ctx, types.UsageSpend, "ws-tenant", now, now)
	require.NoError(t, err)
	require.Equal(t, types.Usage{Requests: 1, CompletionTokens: 1000, MicroUSD: 1000, CompletionMicroUSD: 1000}, spend.PerModel["acme/model"])
	earned, err := s.repo.GetUsage(ctx, types.UsageEarned, "ws-provider", now, now)
	require.NoError(t, err)
	require.Equal(t, types.Usage{Requests: 1, CompletionTokens: 1000, MicroUSD: 700}, earned.PerModel["acme/model"])

	// Recording the same request twice (a retried accounting leg) never double-counts.
	r.record(rq, endpoint, replica, 200, Usage{CompletionTokens: 1000, Found: true}, 0, "")
	replayed, err := s.repo.GetUsage(ctx, types.UsageSpend, "ws-tenant", now, now)
	require.NoError(t, err)
	require.Equal(t, spend.Total, replayed.Total)

	// Free requests earn nothing and carry no provider attribution.
	rq.requestID = "req-2"
	r.record(rq, endpoint, replica, 200, Usage{Found: true}, 0, "")
	require.Empty(t, generation("req-2").ProviderWorkspaceID)

	// Failed requests are recorded but never billed.
	rq.requestID = "req-3"
	r.record(rq, endpoint, replica, 502, Usage{CompletionTokens: 5, Found: true}, 0, "boom")
	failed := generation("req-3")
	require.Zero(t, failed.CostMicroUSD)
	require.Equal(t, "boom", failed.Error)
	spend, err = s.repo.GetUsage(ctx, types.UsageSpend, "ws-tenant", now, now)
	require.NoError(t, err)
	require.Equal(t, int64(2), spend.Total.Requests, "the free request counts, the failed one does not")

	// Every sample lands in the serving replica's own metrics bucket.
	metrics, err := s.repo.GetRouteMetrics(ctx, "acme/model", "H100", "rep-1", 0, time.Minute)
	require.NoError(t, err)
	require.EqualValues(t, 4, metrics.Requests)
	require.EqualValues(t, 1, metrics.Errors)
}

// TestProxyStreamWithoutUsageIsNotBilled: a billable SSE stream that completes
// without a usage chunk reaches the client intact but is recorded as a 502
// with no cost, like the buffered path, so it is not billed.
func TestProxyStreamWithoutUsageIsNotBilled(t *testing.T) {
	s := newServiceForTest(t)
	r := &router{s: s, states: map[string]*llmroute.State{}}

	var withUsage atomic.Bool
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "data: {\"id\":\"chatcmpl-upstream\",\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n")
		if withUsage.Load() {
			fmt.Fprint(w, "data: {\"id\":\"chatcmpl-upstream\",\"choices\":[],\"usage\":{\"prompt_tokens\":3,\"completion_tokens\":7}}\n\n")
		}
		fmt.Fprint(w, "data: [DONE]\n\n")
	}))
	defer upstream.Close()
	r.s.transports.Store(upstream.Listener.Addr().String(), &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return net.Dial("tcp", upstream.Listener.Addr().String())
		},
	})

	endpoint := &types.ManagedEndpoint{Spec: types.ManagedEndpointSpec{ID: "acme/model", Pricing: types.Pricing{CompletionTokens: "0.000001"}}}
	replica := &types.EndpointReplica{ID: "rep-1", Address: upstream.Listener.Addr().String(), GPU: "H100"}
	proxyOnce := func() (*httptest.ResponseRecorder, types.EventEndpointRouteSchema) {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"acme/model","stream":true}`))
		rq := &routeRequest{
			ctx: echo.New().NewContext(req, rec), adapter: adapters[types.EndpointRouteChatCompletions], route: types.EndpointRouteChatCompletions,
			auth:      &auth.AuthInfo{Workspace: &types.Workspace{ExternalId: "ws-tenant"}, Token: &types.Token{ExternalId: "tok"}},
			requestID: "req-1", models: []string{"acme/model"}, body: []byte(`{"model":"acme/model","stream":true}`), stream: true, startedAt: time.Now(),
		}
		if withUsage.Load() {
			rq.requestID = "req-with-usage"
		}
		retry, err := r.proxy(context.Background(), rq, endpoint, replica)
		if withUsage.Load() {
			require.NoError(t, err)
		} else {
			require.Error(t, err)
		}
		require.False(t, retry)
		event, err := s.repo.GetGeneration(context.Background(), rq.requestID)
		require.NoError(t, err)
		require.NotNil(t, event, "record persists the generation synchronously")
		return rec, *event
	}

	rec, event := proxyOnce()
	assert.Equal(t, http.StatusOK, rec.Code, "the stream already reached the client")
	assert.NotContains(t, rec.Body.String(), "data: [DONE]", "missing usage cannot complete a successful billed stream")
	assert.Contains(t, rec.Body.String(), "upstream_stream_interrupted")
	assert.Contains(t, rec.Body.String(), `"id":"req-1"`, "chunks carry the gateway generation id")
	assert.NotContains(t, rec.Body.String(), "chatcmpl-upstream")
	assert.Equal(t, http.StatusBadGateway, event.StatusCode)
	assert.Equal(t, errMissingUsage.Message, event.Error)
	assert.Zero(t, event.CostMicroUSD)

	withUsage.Store(true)
	_, event = proxyOnce()
	assert.Equal(t, http.StatusOK, event.StatusCode)
	assert.Equal(t, int64(7), event.CompletionTokens)
	assert.Equal(t, int64(7), event.CostMicroUSD)
}
