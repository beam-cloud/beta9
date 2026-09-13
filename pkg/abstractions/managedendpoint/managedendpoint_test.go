package managedendpoint

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

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

func TestAuthorization(t *testing.T) {
	s := newServiceForTest(t)

	workspaceToken := &auth.AuthInfo{
		Workspace: &types.Workspace{Id: 1, ExternalId: "admin-ws"},
		Token:     &types.Token{TokenType: types.TokenTypeWorkspace, ExternalId: "tok"},
	}
	assert.Equal(t, http.StatusUnauthorized, call(t, s, nil, http.MethodGet, "/api/v1/endpoints", nil).Code)
	assert.Equal(t, http.StatusUnauthorized, call(t, s, workspaceToken, http.MethodGet, "/api/v1/endpoints", nil).Code, "workspace tokens cannot use the admin API")
	assert.Equal(t, http.StatusOK, call(t, s, adminInfo, http.MethodGet, "/api/v1/endpoints", nil).Code)
	_, err := s.ApplyRepo(auth.ContextWithAuthInfo(context.Background(), workspaceToken), &pb.ApplyRepoRequest{})
	assert.Equal(t, codes.PermissionDenied, status.Code(err), "workspace tokens cannot apply the repo")

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
	_, err = s.Heartbeat(auth.ContextWithAuthInfo(context.Background(), workspaceToken), &pb.HarnessHeartbeatRequest{ReplicaId: replica.ID})
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

	// The hosted route needs a workspace token; the admin API is not enough
	// to be a caller, but a cluster admin may call anything.
	assert.Equal(t, http.StatusUnauthorized, call(t, s, nil, http.MethodGet, "/v1/models", nil).Code)
	assert.Equal(t, http.StatusOK, call(t, s, userInfo, http.MethodGet, "/v1/models", nil).Code)
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

	code, resp := callJSON(t, s, adminInfo, http.MethodPost, "/api/v1/endpoints/replicas/"+replica.ID+"/config", map[string]any{"config_json": `{"max_num_seqs":96}`, "author": "agent", "wait_seconds": 1})
	require.Equal(t, http.StatusOK, code, resp)
	set := events.find("config.set")
	require.NotNil(t, set, "the revision is history even though the wakeup failed")
	assert.Equal(t, "admin-ws", set.WorkspaceID)
	assert.Equal(t, stubID, set.StubID)
	assert.EqualValues(t, 1, set.Revision)
	assert.Equal(t, "tok", set.Data["actor"])
	assert.Equal(t, "agent", set.Data["author"])
	assert.Equal(t, "tok", resp["replica"].(map[string]any)["config"].(map[string]any)["actor"])

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
	var record types.ManagedEndpoint
	require.NoError(t, json.Unmarshal([]byte(get.Endpoint.SpecJson), &record))
	assert.Equal(t, endpoint.Publication, record.Publication, "spec_json carries the published record: spec, catalog, access and pricing")
	assert.True(t, record.Published)
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
	assert.JSONEq(t, `{"acme/model":{"enabled":true,"gpus":{"H100":{"priority":1,"max_replicas":2}},"catalog":{"name":"Model","context_length":32768},"public":true,"pricing":{"prompt_tokens":"0.000001","completion_tokens":"0.000002"}}}`, gitops.FleetJson,
		"config.yaml as applied: placement and publication together")
}

// Every finished request is one Charge: the caller, the price and the
// reported work are journaled together, the counters are updated once, and
// the provider whose machine served it earns its share.
func TestChargeFlowCreditsProviderWorkspace(t *testing.T) {
	s := newServiceForTest(t)
	r := s.router
	now := time.Now()
	app := &types.ManagedEndpoint{Spec: types.ManagedEndpointSpec{ID: "acme/model"}, Publication: types.Publication{Pricing: types.Pricing{PromptTokens: "0", CompletionTokens: "0.000001"}}}
	replica := &types.EndpointReplica{ID: "rep-1", GPU: "H100", MachineID: "machine-a", ProviderWorkspaceID: "ws-provider"}
	rq := func(id string) *routeRequest {
		rq := &routeRequest{auth: userInfo, requestID: id, route: types.EndpointRouteChatCompletions, startedAt: time.Now()}
		rq.charge = newCharge(id, app, userInfo.Workspace, userInfo.Token.ExternalId, rq.route, rq.startedAt)
		return rq
	}
	ctx := context.Background()

	require.NoError(t, r.finish(rq("req-1"), app, replica, 200, types.Work{CompletionTokens: 1000}, true, 0, ""))
	c := charge(t, s, "req-1")
	require.Equal(t, types.ChargeSettled, c.Status)
	require.EqualValues(t, 1000, c.Cost.MicroUSD)
	require.Equal(t, "ws-provider", c.ProviderWorkspaceID)
	require.Equal(t, "machine-a", c.MachineID)
	require.EqualValues(t, 700, c.ProviderShareMicroUSD) // default 70% share
	require.Equal(t, "acme/model", c.AppID)
	require.Equal(t, "user-ws", c.WorkspaceID)
	require.Equal(t, app.Pricing, c.Pricing, "the price is snapshotted on the charge")

	spendReport, err := s.repo.GetUsage(ctx, types.UsageSpend, "user-ws", now, now)
	require.NoError(t, err)
	require.Equal(t, types.Usage{Work: types.Work{Requests: 1, CompletionTokens: 1000}, Cost: types.Cost{MicroUSD: 1000, CompletionMicroUSD: 1000}}, spendReport.PerModel["acme/model"])
	earned, err := s.repo.GetUsage(ctx, types.UsageEarned, "ws-provider", now, now)
	require.NoError(t, err)
	require.Equal(t, types.Usage{Work: types.Work{Requests: 1, CompletionTokens: 1000}, Cost: types.Cost{MicroUSD: 700}}, earned.PerModel["acme/model"])

	// Finishing the same request twice (a duplicate completion) never double-counts.
	require.NoError(t, r.finish(rq("req-1"), app, replica, 200, types.Work{CompletionTokens: 1000}, true, 0, ""))
	replayed, err := s.repo.GetUsage(ctx, types.UsageSpend, "user-ws", now, now)
	require.NoError(t, err)
	require.Equal(t, spendReport.Total, replayed.Total)

	// Free requests earn nothing and carry no provider attribution.
	require.NoError(t, r.finish(rq("req-2"), app, replica, 200, types.Work{}, true, 0, ""))
	require.Empty(t, charge(t, s, "req-2").ProviderWorkspaceID)

	// Failed requests are journaled but void: never billed, never counted as a request.
	require.NoError(t, r.finish(rq("req-3"), app, replica, 502, types.Work{CompletionTokens: 5}, true, 0, "boom"))
	failed := charge(t, s, "req-3")
	require.Equal(t, types.ChargeVoid, failed.Status)
	require.Zero(t, failed.Cost.MicroUSD)
	require.Equal(t, "boom", failed.Error)
	spendReport, err = s.repo.GetUsage(ctx, types.UsageSpend, "user-ws", now, now)
	require.NoError(t, err)
	require.Equal(t, int64(2), spendReport.Total.Requests, "the free request counts, the failed one does not")

	// Every sample lands in the serving replica's own metrics bucket.
	metrics, err := s.repo.GetRouteMetrics(ctx, "acme/model", "H100", "rep-1", 0, time.Minute)
	require.NoError(t, err)
	require.EqualValues(t, 3, metrics.Requests)
	require.EqualValues(t, 1, metrics.Errors)

	// Nothing is left waiting for accounting.
	pending, err := s.repo.ListPendingCharges(ctx, time.Now().Add(time.Hour), 10)
	require.NoError(t, err)
	require.Empty(t, pending)
}

// A crash between journaling a charge and updating the counters leaves the
// charge pending; the next flush applies it exactly once.
func TestPendingChargeIsAccountedOnceByFlush(t *testing.T) {
	s := newServiceForTest(t)
	ctx := context.Background()
	c := &types.Charge{ID: "req-crash", Status: types.ChargeOpen, WorkspaceID: "user-ws", AppID: "acme/model", Pricing: types.Pricing{Request: "0.01"}, AcceptedAt: time.Now()}
	require.NoError(t, c.Settle(types.Work{}, time.Now()))
	_, err := s.repo.SaveCharge(ctx, c)
	require.NoError(t, err)
	require.NoError(t, s.repo.SetChargeSchema(ctx, repository.ChargeSchema))
	require.Zero(t, spend(t, s, "user-ws").Requests)

	s.usage = &recordingUsageMetrics{}
	require.NoError(t, s.billing.flush(ctx))
	require.NoError(t, s.billing.flush(ctx))
	total := spend(t, s, "user-ws")
	require.EqualValues(t, 1, total.Requests)
	require.EqualValues(t, 10_000, total.MicroUSD)
	pending, err := s.repo.ListPendingCharges(ctx, time.Now().Add(time.Hour), 10)
	require.NoError(t, err)
	require.Empty(t, pending)
}

// Pre-consolidation route records still awaiting accounting are migrated
// into charges with their amounts preserved before the schema marker is set.
func TestFlushMigratesLegacyPendingRecords(t *testing.T) {
	s := newServiceForTest(t)
	ctx := context.Background()
	s.usage = &recordingUsageMetrics{}
	legacy := `{"request_id":"gen-old","endpoint_id":"acme/image","workspace_id":"user-ws","status_code":200,"images":2,"cost_micro_usd":900,"image_micro_usd":900,"timestamp":"` + time.Now().UTC().Format(time.RFC3339Nano) + `"}`
	require.NoError(t, s.rdb.Set(ctx, "managed_endpoint:generation:gen-old", legacy, 0).Err())
	require.NoError(t, s.repo.DeferAccounting(ctx, "gen-old", time.Now().Add(-time.Minute)))

	require.NoError(t, s.billing.flush(ctx))
	schema, err := s.repo.GetChargeSchema(ctx)
	require.NoError(t, err)
	require.Equal(t, repository.ChargeSchema, schema)
	total := spend(t, s, "user-ws")
	require.EqualValues(t, 1, total.Requests)
	require.EqualValues(t, 900, total.MicroUSD, "the historical image amount is preserved in the total")
	require.Zero(t, total.RequestMicroUSD, "and never reinterpreted as a per-request price")
	c := charge(t, s, "gen-old")
	require.Equal(t, types.ChargeSettled, c.Status)
	require.NoError(t, s.billing.flush(ctx), "a second flush finds nothing to migrate")
	require.Equal(t, total, spend(t, s, "user-ws"))
}

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
		adminToken:     "admin-token",
	}
	s.controller = newController(s)
	s.router = newRouter(s)
	s.billing = newBilling(s)
	return s
}

var (
	adminInfo = &auth.AuthInfo{Workspace: &types.Workspace{Id: 1, ExternalId: "admin-ws", Name: "admin"}, Token: &types.Token{TokenType: types.TokenTypeClusterAdmin, ExternalId: "tok"}}
	userInfo  = &auth.AuthInfo{Workspace: &types.Workspace{Id: 2, ExternalId: "user-ws", Name: "user"}, Token: &types.Token{TokenType: types.TokenTypeWorkspace, ExternalId: "user-tok"}}
	otherInfo = &auth.AuthInfo{Workspace: &types.Workspace{Id: 3, ExternalId: "other-ws", Name: "other"}, Token: &types.Token{TokenType: types.TokenTypeWorkspace, ExternalId: "other-tok"}}
)

func adminCtx() context.Context { return auth.ContextWithAuthInfo(context.Background(), adminInfo) }

const testReplicaSecret = "replica-secret-1"

// harnessCtx is what a replica container presents: no workspace token, only
// its own replica secret in gRPC metadata.
func harnessCtx() context.Context {
	return metadata.NewIncomingContext(context.Background(), metadata.Pairs(replicaSecretHeader, testReplicaSecret))
}

// asCaller is the auth middleware the gateway would run: it stamps the
// caller's identity onto the request context.
func asCaller(info *auth.AuthInfo) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			if info == nil {
				return echo.NewHTTPError(http.StatusUnauthorized)
			}
			return next(&auth.HttpAuthContext{Context: c, AuthInfo: info})
		}
	}
}

// testServer serves the hosted /v1 route and the admin API in-process, as
// the caller identified by info.
func testServer(s *Service, info *auth.AuthInfo) *echo.Echo {
	e := echo.New()
	s.router.mount(e.Group(""), asCaller(info))
	s.mountAdminRoutes(e.Group("/api/v1/endpoints", asCaller(info)))
	return e
}

// call runs one request through the hosted routes as the given caller and
// returns the recorder; body may be a string, []byte or a JSON-able value.
func call(t *testing.T, s *Service, info *auth.AuthInfo, method, path string, body any, headers ...string) *httptest.ResponseRecorder {
	t.Helper()
	var reader io.Reader
	contentType := "application/json"
	switch b := body.(type) {
	case nil:
	case string:
		reader = strings.NewReader(b)
	case []byte:
		reader = bytes.NewReader(b)
	default:
		data, err := json.Marshal(b)
		require.NoError(t, err)
		reader = bytes.NewReader(data)
	}
	req := httptest.NewRequest(method, path, reader)
	req.Header.Set("Content-Type", contentType)
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	rec := httptest.NewRecorder()
	testServer(s, info).ServeHTTP(rec, req)
	return rec
}

// callJSON is call with the response decoded as a JSON object.
func callJSON(t *testing.T, s *Service, info *auth.AuthInfo, method, path string, body any, headers ...string) (int, map[string]any) {
	t.Helper()
	rec := call(t, s, info, method, path, body, headers...)
	var out map[string]any
	if len(bytes.TrimSpace(rec.Body.Bytes())) > 0 {
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out), rec.Body.String())
	}
	return rec.Code, out
}

// seedEndpoint registers acme/model (an H100 vLLM model server with the harness)
// as version 1, publishes it publicly at a per-token price and places it on H100.
func seedEndpoint(t *testing.T, s *Service) *types.ManagedEndpoint {
	t.Helper()
	spec := types.ManagedEndpointSpec{
		ID: "acme/model", Kind: types.EndpointKindLLM, Engine: "vllm", Port: 8000, Entrypoint: []string{"vllm", "serve"},
		Gpu:          map[string]types.GpuSpec{"H100": {Config: map[string]any{"max_num_seqs": 64}}},
		DrainSeconds: 5,
	}
	spec.Normalize(true)
	app := &types.ManagedEndpoint{
		Spec: spec, StubID: "stub-1", StubType: types.StubType(types.StubTypeManagedEndpointDeployment), Version: 1, Status: types.EndpointStatusActive, Published: true,
		Publication: types.Publication{Catalog: types.Catalog{Name: "Model", ContextLength: 32768}, Public: true, Pricing: types.Pricing{PromptTokens: "0.000001", CompletionTokens: "0.000002"}},
	}
	require.NoError(t, s.repo.SaveEndpoint(context.Background(), app))
	seedFleet(t, s, map[string]types.FleetEndpoint{spec.ID: {Enabled: true, Publication: app.Publication, GPUs: map[string]types.FleetPlacement{"H100": {Priority: 1, MaxReplicas: 2}}}})
	return app
}

// seedRunner registers a hosted task queue (an ordinary deployment) priced per request.
func seedRunner(t *testing.T, s *Service, id, kind string, pricing types.Pricing) *types.ManagedEndpoint {
	t.Helper()
	spec := types.ManagedEndpointSpec{ID: id, Gpu: map[string]types.GpuSpec{"A10G": {Count: 1}}}
	spec.Normalize(false)
	app := &types.ManagedEndpoint{
		Spec: spec, StubID: "stub-" + strings.ReplaceAll(id, "/", "-"), StubType: types.StubType(kind + "/deployment"), Version: 1, Status: types.EndpointStatusActive, Published: true,
		Publication: types.Publication{Catalog: types.Catalog{Name: id}, Public: true, Pricing: pricing},
	}
	require.NoError(t, s.repo.SaveEndpoint(context.Background(), app))
	fleet, err := s.repo.GetFleet(context.Background())
	require.NoError(t, err)
	fleet.Endpoints[id] = types.FleetEndpoint{Enabled: true, Publication: app.Publication, GPUs: map[string]types.FleetPlacement{"A10G": {Priority: 1, MaxReplicas: 1, Serverless: true}}}
	require.NoError(t, s.repo.SaveFleet(context.Background(), fleet))
	return app
}

func seedFleet(t *testing.T, s *Service, endpoints map[string]types.FleetEndpoint) *types.Fleet {
	t.Helper()
	fleet := &types.Fleet{GitSHA: "fleet-sha", Endpoints: endpoints}
	fleet.Normalize()
	require.NoError(t, s.repo.SaveFleet(context.Background(), fleet))
	return fleet
}

func seedReplica(t *testing.T, s *Service, app *types.ManagedEndpoint) *types.EndpointReplica {
	t.Helper()
	replica := &types.EndpointReplica{
		ID: "rep-1", EndpointID: app.Spec.ID, Version: 1, GPU: "H100", GPUCount: 1,
		ContainerID: "managed-stub-1-abc", Status: types.ReplicaStatusScheduling, HarnessEnabled: true, StartedAt: time.Now(),
		SecretHash: hashReplicaSecret(testReplicaSecret), Probe: probeFor(&app.Spec),
	}
	require.NoError(t, s.repo.SaveReplica(context.Background(), replica))
	return replica
}

// readyReplica makes a replica serve at address with the given concurrency.
func readyReplica(t *testing.T, s *Service, replica *types.EndpointReplica, address string, maxConcurrency int64) *types.EndpointReplica {
	t.Helper()
	replica.Status, replica.Address, replica.ReadyAt = types.ReplicaStatusReady, address, time.Now().Add(-time.Hour)
	replica.Capacity.MaxConcurrency = maxConcurrency
	require.NoError(t, s.repo.SaveReplica(context.Background(), replica))
	return replica
}

// charge reads the journaled charge for one request or task id.
func charge(t *testing.T, s *Service, id string) *types.Charge {
	t.Helper()
	c, err := s.repo.GetCharge(context.Background(), id)
	require.NoError(t, err)
	require.NotNil(t, c, "charge %s was not journaled", id)
	return c
}

func spend(t *testing.T, s *Service, workspaceID string) types.Usage {
	t.Helper()
	now := time.Now()
	report, err := s.repo.GetUsage(context.Background(), types.UsageSpend, workspaceID, now.Add(-24*time.Hour), now)
	require.NoError(t, err)
	return report.Total
}
