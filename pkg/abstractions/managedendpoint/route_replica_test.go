package managedendpoint

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
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

func TestReplicaLookupFailureIsNotColdCapacity(t *testing.T) {
	s := newServiceForTest(t)
	endpoint := seedEndpoint(t, s)
	s.repo = &routeReplicaRepository{ManagedEndpointRepository: s.repo, err: errors.New("registry read failed")}
	r := newRouter(s)
	ctx, _ := coldRouteContext()
	rq := &routeRequest{ctx: ctx, auth: ctx.AuthInfo, models: []string{endpoint.Spec.ID}, route: types.EndpointRouteChatCompletions, startedAt: time.Now()}
	resolved, rerr := r.resolveEndpoint(context.Background(), rq)
	assert.Nil(t, resolved)
	assert.Equal(t, errRegistry, rerr)
	started := time.Now()
	replica, rerr := r.pick(context.Background(), rq, endpoint, nil)
	assert.Nil(t, replica)
	assert.Equal(t, errRegistry, rerr)
	assert.Less(t, time.Since(started), 200*time.Millisecond)
}

func TestReplicaPickHonorsCancellationAndDrain(t *testing.T) {
	for _, reason := range []string{"client", "gateway"} {
		t.Run(reason, func(t *testing.T) {
			s := newServiceForTest(t)
			endpoint := seedEndpoint(t, s)
			ctx, cancel := context.WithCancel(context.Background())
			if reason == "gateway" {
				s.drainCtx = ctx
				ctx = context.Background()
			}
			cancel()
			_, rerr := newRouter(s).pick(ctx, &routeRequest{}, endpoint, nil)
			require.NotNil(t, rerr)
			if reason == "client" {
				assert.Equal(t, "client_closed", rerr.Code)
			} else {
				assert.Equal(t, "gateway_draining", rerr.Code)
			}
		})
	}
}

func TestHotModelCapacityRejectionDoesNotWait(t *testing.T) {
	s := newServiceForTest(t)
	endpoint := seedEndpoint(t, s)
	started := time.Now()
	replica, rerr := newRouter(s).pick(context.Background(), &routeRequest{startedAt: started}, endpoint, nil)
	assert.Nil(t, replica)
	require.NotNil(t, rerr)
	assert.Equal(t, http.StatusTooManyRequests, rerr.Status)
	assert.Equal(t, "rate_limit_exceeded", rerr.Code)
	assert.Less(t, time.Since(started), 200*time.Millisecond, "capacity rejections do not queue")
}

func TestModelCatalogKeepsPlacementInternal(t *testing.T) {
	s := newServiceForTest(t)
	endpoint := seedEndpoint(t, s)
	seedFleet(t, s, map[string]types.FleetEndpoint{endpoint.Spec.ID: {
		Enabled: true, GPUs: map[string]types.FleetPlacement{"H100": {Priority: 1, MaxReplicas: 1}},
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
	assert.Zero(t, repo.fleetReads.Load(), "public catalog does not need placement policy")
}

func TestModelCatalogDoesNotDependOnPlacementConfig(t *testing.T) {
	s := newServiceForTest(t)
	seedEndpoint(t, s)
	s.repo = &routeReplicaRepository{ManagedEndpointRepository: s.repo, fleetErr: errors.New("fleet unavailable")}
	ctx, rec := coldRouteContext()
	require.NoError(t, newRouter(s).handleListModels(ctx))
	assert.Equal(t, http.StatusOK, rec.Code)
}
