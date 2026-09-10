package managedendpoint

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCapacityRejectionUsesOpenAI429BeforeStreaming(t *testing.T) {
	for _, scenario := range []string{"missing", "loading", "draining", "evicting", "busy", "endpoint limit", "workspace limit"} {
		for _, stream := range []bool{false, true} {
			t.Run(scenario+"/stream="+map[bool]string{true: "true", false: "false"}[stream], func(t *testing.T) {
				s := newServiceForTest(t)
				endpoint := seedEndpoint(t, s)
				router := newRouter(s)
				switch scenario {
				case "loading", "draining", "evicting", "busy":
					replica := seedReplica(t, s, endpoint)
					replica.Address = "unused:8000"
					replica.Status = types.ReplicaStatus(scenario)
					if scenario == "busy" {
						replica.Status = types.ReplicaStatusReady
						replica.Capacity.MaxConcurrency = 1
						counter(&router.inflight, replica.ID).Store(1)
					}
					require.NoError(t, s.repo.SaveReplica(context.Background(), replica))
				case "endpoint limit":
					s.config.Routing.PerEndpointConcurrency = 1
					counter(&router.admission, endpoint.Spec.ID).Store(1)
				case "workspace limit":
					s.config.Routing.PerWorkspaceConcurrency = 1
					counter(&router.admission, endpoint.Spec.ID+"|user-ws").Store(1)
				}
				ctx, rec := coldRouteContext()
				body, _ := json.Marshal(map[string]any{"model": endpoint.Spec.ID, "messages": []map[string]string{{"role": "user", "content": "hello"}}, "stream": stream})
				req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(body))
				req.Header.Set("Content-Type", "application/json")
				ctx.SetRequest(req)
				require.NoError(t, router.handleRoute(ctx))
				assert.Equal(t, http.StatusTooManyRequests, rec.Code, rec.Body.String())
				assert.Contains(t, rec.Header().Get("Content-Type"), "application/json")
				assert.Equal(t, "1", rec.Header().Get("Retry-After"))
				assert.NotEmpty(t, rec.Header().Get(headerRequestID))
				var envelope struct {
					Error struct {
						Type    string `json:"type"`
						Code    string `json:"code"`
						Message string `json:"message"`
						Param   any    `json:"param"`
					} `json:"error"`
				}
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &envelope))
				assert.Equal(t, "rate_limit_error", envelope.Error.Type)
				assert.Equal(t, "rate_limit_exceeded", envelope.Error.Code)
				assert.NotEmpty(t, envelope.Error.Message)
				assert.Nil(t, envelope.Error.Param)
				assert.NotContains(t, rec.Body.String(), "data:")
				now := time.Now().UTC()
				usage, err := s.repo.GetUsage(context.Background(), types.UsageSpend, "user-ws", now, now)
				require.NoError(t, err)
				assert.Equal(t, types.Usage{}, usage.Total, "capacity rejections must never charge credits")
			})
		}
	}
}

func TestBackendFailureIsNotReportedAsCapacity(t *testing.T) {
	s := newServiceForTest(t)
	seedEndpoint(t, s)
	s.repo = &routeReplicaRepository{ManagedEndpointRepository: s.repo, err: errors.New("registry unavailable")}
	ctx, rec := coldRouteContext()
	require.NoError(t, newRouter(s).handleRoute(ctx))
	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	assert.Empty(t, rec.Header().Get("Retry-After"))
	assert.Contains(t, rec.Body.String(), `"code":"registry_unavailable"`)
	assert.Contains(t, rec.Body.String(), `"type":"server_error"`)
}

func TestRequestRewritesPreserveToolSchemaNumbers(t *testing.T) {
	const body = `{"model":"acme/model","models":["acme/model"],"messages":[{"role":"user","content":"tool test"}],"stream":true,"reasoning":{"enabled":false},"tools":[{"type":"function","function":{"name":"lookup","parameters":{"type":"object","properties":{"id":{"type":"integer","enum":[9007199254740993,9223372036854775807]},"ratio":{"const":0.1234567890123456789}}}}}]}`
	s := newServiceForTest(t)
	ctx, _ := coldRouteContext()
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	ctx.SetRequest(req)
	rq := &routeRequest{ctx: ctx, adapter: adapters[types.EndpointRouteChatCompletions]}
	require.Nil(t, newRouter(s).readRequest(rq, ""))
	rq.setModel("acme/selected", true)
	var before, after map[string]json.RawMessage
	require.NoError(t, json.Unmarshal([]byte(body), &before))
	require.NoError(t, json.Unmarshal(rq.body, &after))
	var original, rewritten bytes.Buffer
	require.NoError(t, json.Compact(&original, before["tools"]))
	require.NoError(t, json.Compact(&rewritten, after["tools"]))
	// Object key order may change; decode with UseNumber for semantic equality.
	var want, got any
	a, b := json.NewDecoder(&original), json.NewDecoder(&rewritten)
	a.UseNumber()
	b.UseNumber()
	require.NoError(t, a.Decode(&want))
	require.NoError(t, b.Decode(&got))
	assert.Equal(t, want, got)
	assert.Equal(t, `"acme/selected"`, string(after["model"]))
	assert.NotContains(t, after, "models")
	assert.NotContains(t, after, "reasoning")
}

func TestRequestJSONRejectsTrailingValues(t *testing.T) {
	for _, body := range []string{`{"model":"acme/model"} {}`, `{"model":"acme/model"} true`, `{"model":"acme/model"} garbage`, `[]`} {
		rec := httptest.NewRecorder()
		ctx := echo.New().NewContext(httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body)), rec)
		rq := &routeRequest{ctx: ctx, adapter: adapters[types.EndpointRouteChatCompletions]}
		err := newRouter(newServiceForTest(t)).readRequest(rq, "")
		require.NotNil(t, err, body)
		assert.Equal(t, http.StatusBadRequest, err.Status)
	}
}

func TestMountedOpenAIAuthenticationErrors(t *testing.T) {
	for _, path := range []string{"/v1/chat/completions", "/v1/models", "/v1/models/openrouter"} {
		for _, invalid := range []bool{false, true} {
			s := newServiceForTest(t)
			s.config.RoutePrefix = "/v1"
			e := echo.New()
			middleware := func(next echo.HandlerFunc) echo.HandlerFunc {
				return func(ctx echo.Context) error {
					if invalid {
						return echo.NewHTTPError(http.StatusUnauthorized)
					}
					return next(ctx)
				}
			}
			newRouter(s).mount(e.Group(""), middleware)
			method := http.MethodGet
			if strings.HasSuffix(path, "/completions") {
				method = http.MethodPost
			}
			rec := httptest.NewRecorder()
			e.ServeHTTP(rec, httptest.NewRequest(method, path, nil))
			assert.Equal(t, http.StatusUnauthorized, rec.Code)
			assert.Contains(t, rec.Body.String(), `"type":"authentication_error"`)
			assert.Contains(t, rec.Body.String(), `"code":"invalid_api_key"`)
		}
	}
}

func TestEngineCapacityErrorRemainsJSON429(t *testing.T) {
	s := newServiceForTest(t)
	endpoint := seedEndpoint(t, s)
	replica := seedReplica(t, s, endpoint)
	replica.Address = "engine:8000"
	replica.Status = types.ReplicaStatusReady
	require.NoError(t, s.repo.SaveReplica(context.Background(), replica))
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Retry-After", "3")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte("internal engine overload detail"))
	}))
	defer upstream.Close()
	transport := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "tcp", upstream.Listener.Addr().String())
	}}
	defer transport.CloseIdleConnections()
	s.transports.Store(replica.Address, transport)
	ctx, rec := coldRouteContext()
	require.NoError(t, newRouter(s).handleRoute(ctx))
	assert.Equal(t, http.StatusTooManyRequests, rec.Code)
	assert.Equal(t, "3", rec.Header().Get("Retry-After"))
	assert.Contains(t, rec.Header().Get("Content-Type"), "application/json")
	assert.Contains(t, rec.Body.String(), `"type":"rate_limit_error"`)
	assert.NotContains(t, rec.Body.String(), "internal engine")
}

func TestSharedReservationFailureDoesNotFallBackToLocalAdmission(t *testing.T) {
	s := newServiceForTest(t)
	r := newRouter(s)
	endpoint := seedEndpoint(t, s)
	replica := seedReplica(t, s, endpoint)
	replica.Capacity.MaxConcurrency = 1
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	ok, err := r.reserve(ctx, &routeRequest{requestID: "canceled-request"}, r.state(endpoint.Spec.ID), replica)
	require.Error(t, err)
	assert.False(t, ok)
	assert.Zero(t, counter(&r.inflight, replica.ID).Load(), "failed shared admission releases the local slot")
}

func TestSharedPressureFailureReturns503WithoutWakingCapacity(t *testing.T) {
	s := newServiceForTest(t)
	endpoint := seedEndpoint(t, s)
	seedFleet(t, s, map[string]types.FleetEndpoint{endpoint.Spec.ID: {
		Enabled: true, GPUs: map[string]types.FleetPlacement{"H100": {Priority: 1, MaxReplicas: 1, Serverless: true}},
	}})
	replica := seedReplica(t, s, endpoint)
	replica.Status, replica.Address = types.ReplicaStatusReady, "unused:8000"
	replica.Capacity.MaxConcurrency = 1
	require.NoError(t, s.repo.SaveReplica(context.Background(), replica))
	// Wrong-type Redis data simulates a failed shared capacity read after the
	// replica registry succeeds. It must not be presented as an empty pool.
	require.NoError(t, s.rdb.Set(context.Background(), "managed_endpoint:route:"+endpoint.Spec.ID+":llm_pressure:"+replica.ID, "invalid", 0).Err())
	ctx, rec := coldRouteContext()
	require.NoError(t, newRouter(s).handleRoute(ctx))
	assert.Equal(t, http.StatusServiceUnavailable, rec.Code, rec.Body.String())
	assert.Contains(t, rec.Body.String(), `"code":"registry_unavailable"`)
	d, err := s.demand(context.Background(), endpoint.Spec.ID, "read", "", 0)
	require.NoError(t, err)
	assert.False(t, d.pending)
	assert.Zero(t, d.active)
}
