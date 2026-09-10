package managedendpoint

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type routeReplicaRepository struct {
	repository.ManagedEndpointRepository
	err        error
	reads      atomic.Int64
	read       chan struct{}
	fleetErr   error
	fleetReads atomic.Int64
}

func (r *routeReplicaRepository) GetFleet(ctx context.Context) (*types.Fleet, error) {
	r.fleetReads.Add(1)
	if r.fleetErr != nil {
		return nil, r.fleetErr
	}
	return r.ManagedEndpointRepository.GetFleet(ctx)
}

func (r *routeReplicaRepository) ListReplicas(ctx context.Context, endpointID string) ([]*types.EndpointReplica, error) {
	r.reads.Add(1)
	if r.read != nil {
		select {
		case r.read <- struct{}{}:
		default:
		}
	}
	if r.err != nil {
		return nil, r.err
	}
	return r.ManagedEndpointRepository.ListReplicas(ctx, endpointID)
}

func coldRouteContext() (*auth.HttpAuthContext, *httptest.ResponseRecorder) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"acme/model","messages":[{"role":"user","content":"hello"}]}`))
	req.Header.Set("Content-Type", "application/json")
	return &auth.HttpAuthContext{
		Context: echo.New().NewContext(req, rec),
		AuthInfo: &auth.AuthInfo{
			Workspace: &types.Workspace{Id: 2, ExternalId: "user-ws", Name: "user"},
			Token:     &types.Token{TokenType: types.TokenTypeWorkspace, ExternalId: "tok"},
		},
	}, rec
}

func TestOnDemandRouteWaitsForFirstReplica(t *testing.T) {
	s := newServiceForTest(t)
	endpoint := seedEndpoint(t, s)
	seedFleet(t, s, map[string]types.FleetEndpoint{endpoint.Spec.ID: {
		Enabled: true, GPUs: map[string]types.FleetPlacement{"H100": {Priority: 1, MaxReplicas: 1, Serverless: true}},
	}})
	s.config.Routing.MaxQueueWait = 20 * time.Millisecond
	repo := &routeReplicaRepository{ManagedEndpointRepository: s.repo}
	s.repo = repo
	r := newRouter(s)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"completion-1","choices":[{"message":{"role":"assistant","content":"hello"}}],"usage":{"prompt_tokens":2,"completion_tokens":1,"total_tokens":3}}`))
	}))
	t.Cleanup(upstream.Close)
	transport := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "tcp", upstream.Listener.Addr().String())
	}}
	t.Cleanup(transport.CloseIdleConnections)
	s.transports.Store("test-replica", transport)
	ctx, rec := coldRouteContext()
	done := make(chan error, 1)
	go func() { done <- r.handleRoute(ctx) }()
	require.Eventually(t, func() bool {
		demand, err := s.demand(context.Background(), endpoint.Spec.ID, "read", "", 0)
		return err == nil && demand.active == 1
	}, time.Second, 5*time.Millisecond, "an admitted cold request registers demand before a replica exists")
	// The warm queue timeout has elapsed, but a cold request is still waiting.
	time.Sleep(30 * time.Millisecond)
	select {
	case err := <-done:
		t.Fatalf("cold request returned before readiness: %v (%s)", err, rec.Body.String())
	default:
	}
	replica := seedReplica(t, s, endpoint)
	replica.Address = "test-replica"
	replica.Status = types.ReplicaStatusReady
	replica.Capacity.MaxConcurrency = 1
	require.NoError(t, s.repo.SaveReplica(context.Background(), replica))
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("cold request did not resume after its replica became ready")
	}
	assert.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	assert.Contains(t, rec.Body.String(), `"content":"hello"`)
	assert.Contains(t, rec.Body.String(), `"id":"gen-`)
	assert.Equal(t, replica.ID, rec.Header().Get(headerReplicaServed))
	demand, err := s.demand(context.Background(), endpoint.Spec.ID, "read", "", 0)
	require.NoError(t, err)
	assert.Zero(t, demand.active, "completion releases the request lease")
	assert.LessOrEqual(t, repo.reads.Load(), int64(4), "cold wait does not poll Redis at the warm ten-per-second rate")
}

func TestRejectedRouteDoesNotCreateDemand(t *testing.T) {
	for _, reason := range []string{"unauthenticated", "private", "admission"} {
		t.Run(reason, func(t *testing.T) {
			s := newServiceForTest(t)
			endpoint := seedEndpoint(t, s)
			seedFleet(t, s, map[string]types.FleetEndpoint{endpoint.Spec.ID: {
				Enabled: true, GPUs: map[string]types.FleetPlacement{"H100": {Priority: 1, MaxReplicas: 1, Serverless: true}},
			}})
			r := newRouter(s)
			ctx, rec := coldRouteContext()
			want := http.StatusUnauthorized
			switch reason {
			case "unauthenticated":
				ctx.AuthInfo.Token = nil
			case "private":
				endpoint.Spec.Public = false
				require.NoError(t, s.repo.SaveEndpoint(context.Background(), endpoint))
				want = http.StatusForbidden
			case "admission":
				s.config.Routing.PerEndpointConcurrency = 1
				counter(&r.admission, endpoint.Spec.ID).Store(1)
				want = http.StatusTooManyRequests
			}
			require.NoError(t, r.handleRoute(ctx))
			assert.Equal(t, want, rec.Code, rec.Body.String())
			keys, err := s.rdb.Exists(context.Background(), "managed_endpoint:demand:"+endpoint.Spec.ID).Result()
			require.NoError(t, err)
			assert.Zero(t, keys, "rejection must create neither an active lease nor an idle marker")
		})
	}
}

func TestOnDemandRouteSharedLimitReturns429(t *testing.T) {
	s := newServiceForTest(t)
	endpoint := seedEndpoint(t, s)
	seedFleet(t, s, map[string]types.FleetEndpoint{endpoint.Spec.ID: {
		Enabled: true, GPUs: map[string]types.FleetPlacement{"H100": {Priority: 1, MaxReplicas: 1, Serverless: true}},
	}})
	for i := 0; i < serverlessMaxQueuedRequests; i++ {
		_, err := s.demand(context.Background(), endpoint.Spec.ID, "acquire", "existing-"+strconv.Itoa(i), 0)
		require.NoError(t, err)
	}
	r := newRouter(s)
	s.config.Routing.PerEndpointConcurrency = 2
	ctx, rec := coldRouteContext()
	require.NoError(t, r.handleRoute(ctx))
	assert.Equal(t, http.StatusTooManyRequests, rec.Code, rec.Body.String())
	assert.Contains(t, rec.Body.String(), "endpoint_saturated")
	demand, err := s.demand(context.Background(), endpoint.Spec.ID, "read", "", 0)
	require.NoError(t, err)
	assert.EqualValues(t, serverlessMaxQueuedRequests, demand.active)
	assert.Zero(t, counter(&r.admission, endpoint.Spec.ID).Load(), "shared rejection releases local admission")
}

func TestReplicaLookupFailureIsNotColdCapacity(t *testing.T) {
	s := newServiceForTest(t)
	endpoint := seedEndpoint(t, s)
	s.repo = &routeReplicaRepository{ManagedEndpointRepository: s.repo, err: errors.New("registry read failed")}
	r := newRouter(s)
	ctx, _ := coldRouteContext()
	rq := &routeRequest{ctx: ctx, auth: ctx.AuthInfo, models: []string{endpoint.Spec.ID}, route: types.EndpointRouteChatCompletions, startedAt: time.Now(), serverless: true}
	resolved, rerr := r.resolveEndpoint(context.Background(), rq)
	assert.Nil(t, resolved)
	assert.Equal(t, errRegistry, rerr)
	started := time.Now()
	replica, rerr := r.pick(context.Background(), rq, endpoint, nil)
	assert.Nil(t, replica)
	assert.Equal(t, errRegistry, rerr)
	assert.Less(t, time.Since(started), 200*time.Millisecond)
}

func TestColdReplicaWaitCancelsPromptly(t *testing.T) {
	for _, reason := range []string{"client", "gateway"} {
		t.Run(reason, func(t *testing.T) {
			s := newServiceForTest(t)
			endpoint := seedEndpoint(t, s)
			repo := &routeReplicaRepository{ManagedEndpointRepository: s.repo, read: make(chan struct{}, 1)}
			s.repo = repo
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			if reason == "gateway" {
				s.drainCtx = ctx
				ctx = context.Background()
			}
			rq := &routeRequest{startedAt: time.Now(), serverless: true}
			done := make(chan *routeError, 1)
			go func() {
				_, rerr := newRouter(s).pick(ctx, rq, endpoint, nil)
				done <- rerr
			}()
			select {
			case <-repo.read:
			case <-time.After(time.Second):
				t.Fatal("pick did not enter its initial replica lookup")
			}
			cancel()
			select {
			case rerr := <-done:
				require.NotNil(t, rerr)
				if reason == "client" {
					assert.Equal(t, "client_closed", rerr.Code)
				} else {
					assert.Equal(t, "gateway_draining", rerr.Code)
				}
			case <-time.After(200 * time.Millisecond):
				t.Fatal("cancellation waited for the one-second cold poll")
			}
		})
	}
}

func TestWarmOnlyReplicaWaitKeepsExistingTimeout(t *testing.T) {
	s := newServiceForTest(t)
	endpoint := seedEndpoint(t, s)
	s.config.Routing.MaxQueueWait = 20 * time.Millisecond
	started := time.Now()
	replica, rerr := newRouter(s).pick(context.Background(), &routeRequest{startedAt: started}, endpoint, nil)
	assert.Nil(t, replica)
	require.NotNil(t, rerr)
	assert.Equal(t, "no_capacity", rerr.Code)
	assert.Less(t, time.Since(started), 200*time.Millisecond, "warm-only models keep their short queue timeout")
}

func TestModelCatalogReportsColdServerlessAvailability(t *testing.T) {
	for _, serverless := range []bool{false, true} {
		t.Run(strconv.FormatBool(serverless), func(t *testing.T) {
			s := newServiceForTest(t)
			endpoint := seedEndpoint(t, s)
			seedFleet(t, s, map[string]types.FleetEndpoint{endpoint.Spec.ID: {
				Enabled: true, GPUs: map[string]types.FleetPlacement{"H100": {Priority: 1, MaxReplicas: 1, Serverless: serverless}},
			}})
			repo := &routeReplicaRepository{ManagedEndpointRepository: s.repo}
			s.repo = repo
			ctx, rec := coldRouteContext()
			require.NoError(t, newRouter(s).handleListModels(ctx))
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			var body struct {
				Data []map[string]any `json:"data"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
			require.Len(t, body.Data, 1)
			assert.Equal(t, false, body.Data[0]["is_ready"])
			assert.Equal(t, serverless, body.Data[0]["serverless"])
			assert.EqualValues(t, 1, repo.fleetReads.Load(), "one fleet lookup serves the whole catalog")
		})
	}
}

func TestModelCatalogRejectsUnknownServerlessAvailability(t *testing.T) {
	s := newServiceForTest(t)
	seedEndpoint(t, s)
	s.repo = &routeReplicaRepository{ManagedEndpointRepository: s.repo, fleetErr: errors.New("fleet unavailable")}
	ctx, rec := coldRouteContext()
	require.NoError(t, newRouter(s).handleListModels(ctx))
	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	assert.Contains(t, rec.Body.String(), "registry_unavailable")
}
