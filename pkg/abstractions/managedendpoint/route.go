package managedendpoint

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	abstractions "github.com/beam-cloud/beta9/pkg/abstractions/common"
	"github.com/beam-cloud/beta9/pkg/abstractions/common/llmroute"
	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/network"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
	"github.com/rs/zerolog/log"
)

// The /v1 route: OpenRouter/OpenAI-compatible inference surface over managed
// endpoint replicas. One pipeline for every kind; adapters carry the
// per-route differences.

const (
	maxRequestBody      = 64 << 20
	maxResponseBody     = 64 << 20
	queuePollInterval   = 100 * time.Millisecond
	replicaDialTimeout  = 5 * time.Second
	generationTTL       = time.Hour
	usageQueueSize      = 4096
	headerReplicaPin    = "X-Beam-Endpoint-Replica"
	headerRequestID     = "X-Request-ID"
	headerEndpointID    = "X-Beam-Endpoint-ID"
	headerReplicaServed = "X-Beam-Replica"
	providerName        = "beam"
)

type router struct {
	s        *Service
	prefix   string
	selector llmroute.Selector

	stateMu sync.Mutex
	states  map[string]*llmroute.State

	transports sync.Map // address -> *http.Transport
	inflight   sync.Map // replica id -> *atomic.Int64
	admission  sync.Map // endpoint id / endpoint|workspace -> *atomic.Int64

	usageQueue chan usageRecord
	usageWG    sync.WaitGroup
}

func newRouter(s *Service) *router {
	r := &router{
		s:          s,
		prefix:     s.config.RoutePrefixOrDefault(),
		states:     map[string]*llmroute.State{},
		usageQueue: make(chan usageRecord, usageQueueSize),
	}
	r.usageWG.Add(1)
	go r.drainUsage()
	return r
}

func (r *router) mount(group *echo.Group, authMiddleware echo.MiddlewareFunc) {
	g := group.Group(r.prefix, authMiddleware)

	g.GET("/models", auth.WithAuth(r.handleListModels))
	g.GET("/models/:author/:slug/endpoints", auth.WithAuth(r.handleModelEndpoints))
	g.GET("/models/:slug/endpoints", auth.WithAuth(r.handleModelEndpoints))
	g.GET("/generation", auth.WithAuth(r.handleGeneration))
	g.GET("/key", auth.WithAuth(r.handleKey))

	for _, path := range []string{"/chat/completions", "/completions", "/embeddings", "/images/generations", "/images/edits", "/models/:author/:slug/invoke", "/models/:slug/invoke"} {
		g.POST(path, auth.WithAuth(r.handleRoute))
	}
}

func (r *router) state(endpointID string) *llmroute.State {
	r.stateMu.Lock()
	defer r.stateMu.Unlock()
	if st, ok := r.states[endpointID]; ok {
		return st
	}
	st := llmroute.NewState(r.s.rdb, "managed_endpoint:route:"+endpointID)
	r.states[endpointID] = st
	return st
}

func (r *router) counter(m *sync.Map, key string) *atomic.Int64 {
	if v, ok := m.Load(key); ok {
		return v.(*atomic.Int64)
	}
	v, _ := m.LoadOrStore(key, &atomic.Int64{})
	return v.(*atomic.Int64)
}

// --- errors ------------------------------------------------------------------

type routeError struct {
	Status  int
	Code    string
	Message string
}

func (e *routeError) Error() string { return e.Message }

func writeRouteError(ctx echo.Context, e *routeError) error {
	return ctx.JSON(e.Status, map[string]any{
		"error": map[string]any{
			"message": e.Message,
			"type":    errorType(e.Status),
			"code":    e.Code,
		},
	})
}

func errorType(status int) string {
	switch {
	case status == http.StatusUnauthorized || status == http.StatusForbidden:
		return "authentication_error"
	case status == http.StatusPaymentRequired:
		return "insufficient_quota"
	case status == http.StatusTooManyRequests:
		return "rate_limit_error"
	case status == http.StatusNotFound:
		return "not_found_error"
	case status >= 500:
		return "server_error"
	}
	return "invalid_request_error"
}

// --- request context ---------------------------------------------------------

type routeRequest struct {
	ctx        echo.Context
	auth       *auth.AuthInfo
	adapter    adapter
	route      types.EndpointRoute
	requestID  string
	models     []string
	body       []byte
	payload    map[string]any
	stream     bool
	info       *llmroute.RequestInfo
	pinReplica string
	startedAt  time.Time
	queueWait  time.Duration
	retried    bool
}

func (r *router) handleRoute(ctx echo.Context) error {
	cc, ok := ctx.(*auth.HttpAuthContext)
	if !ok || cc.AuthInfo == nil || cc.AuthInfo.Workspace == nil || cc.AuthInfo.Token == nil {
		return writeRouteError(ctx, &routeError{http.StatusUnauthorized, "unauthorized", "a workspace token is required"})
	}
	if !r.s.Enabled() {
		return writeRouteError(ctx, &routeError{http.StatusNotFound, "not_found", "managed endpoints are not enabled"})
	}

	route, pathModel, ok := routeFromPath(r.prefix, ctx.Request().URL.Path)
	if !ok {
		return writeRouteError(ctx, &routeError{http.StatusNotFound, "not_found", "unknown route"})
	}
	if pathModel == "" && ctx.Param("slug") != "" {
		pathModel = ctx.Param("slug")
		if author := ctx.Param("author"); author != "" {
			pathModel = author + "/" + pathModel
		}
	}
	rq := &routeRequest{
		ctx:        ctx,
		auth:       cc.AuthInfo,
		adapter:    adapters[route],
		route:      route,
		requestID:  "gen-" + strings.ReplaceAll(uuid.New().String(), "-", "")[:20],
		pinReplica: strings.TrimSpace(ctx.Request().Header.Get(headerReplicaPin)),
		startedAt:  time.Now(),
	}
	if rq.pinReplica != "" && cc.AuthInfo.Token.TokenType != types.TokenTypeClusterAdmin {
		rq.pinReplica = ""
	}

	if rerr := r.readRequest(rq, pathModel); rerr != nil {
		return writeRouteError(ctx, rerr)
	}

	endpoint, rerr := r.resolveEndpoint(ctx.Request().Context(), rq)
	if rerr != nil {
		return writeRouteError(ctx, rerr)
	}
	if rerr := r.admit(ctx.Request().Context(), rq, endpoint); rerr != nil {
		return writeRouteError(ctx, rerr)
	}
	defer r.release(rq, endpoint)

	return r.serve(rq, endpoint)
}

// readRequest buffers the body, extracts the model list and prepares the
// payload (forcing usage in streams).
func (r *router) readRequest(rq *routeRequest, pathModel string) *routeError {
	req := rq.ctx.Request()
	body, err := io.ReadAll(io.LimitReader(req.Body, maxRequestBody+1))
	if err != nil {
		return &routeError{http.StatusBadRequest, "invalid_body", "failed to read request body"}
	}
	if int64(len(body)) > maxRequestBody {
		return &routeError{http.StatusRequestEntityTooLarge, "body_too_large", "request body exceeds 64MB"}
	}
	rq.body = body

	contentType, _, _ := mime.ParseMediaType(req.Header.Get("Content-Type"))
	switch {
	case strings.HasPrefix(contentType, "multipart/"):
		rq.models = multipartModel(req.Header.Get("Content-Type"), body)
	default:
		if len(bytes.TrimSpace(body)) > 0 {
			if err := json.Unmarshal(body, &rq.payload); err != nil {
				return &routeError{http.StatusBadRequest, "invalid_json", "request body must be a JSON object"}
			}
		}
		if rq.payload != nil {
			if model, _ := rq.payload["model"].(string); model != "" {
				rq.models = append(rq.models, model)
			}
			if list, ok := rq.payload["models"].([]any); ok {
				for _, m := range list {
					if s, ok := m.(string); ok && s != "" {
						rq.models = append(rq.models, s)
					}
				}
			}
			if rq.adapter.Streamable && forceIncludeUsage(rq.payload) {
				rq.stream = true
				if body, err := json.Marshal(rq.payload); err == nil {
					rq.body = body
				}
			}
		}
	}
	if pathModel != "" {
		rq.models = append([]string{pathModel}, rq.models...)
	}
	if len(rq.models) == 0 {
		return &routeError{http.StatusBadRequest, "missing_model", "the model field is required"}
	}
	return nil
}

func multipartModel(contentType string, body []byte) []string {
	_, params, err := mime.ParseMediaType(contentType)
	if err != nil || params["boundary"] == "" {
		return nil
	}
	reader := multipart.NewReader(bytes.NewReader(body), params["boundary"])
	for {
		part, err := reader.NextPart()
		if err != nil {
			return nil
		}
		if part.FormName() == "model" {
			value, _ := io.ReadAll(io.LimitReader(part, 1024))
			if model := strings.TrimSpace(string(value)); model != "" {
				return []string{model}
			}
		}
	}
}

// resolveEndpoint picks the first requested model the caller may use that
// serves the route, preferring one with ready replicas ("models" fallback).
func (r *router) resolveEndpoint(ctx context.Context, rq *routeRequest) (*types.ManagedEndpoint, *routeError) {
	var first *types.ManagedEndpoint
	var denied *routeError
	for _, model := range rq.models {
		endpoint, err := r.s.repo.GetEndpoint(ctx, model)
		if err != nil {
			return nil, &routeError{http.StatusServiceUnavailable, "registry_unavailable", "endpoint registry unavailable"}
		}
		if endpoint == nil || !endpoint.Enabled {
			continue
		}
		if !endpoint.Spec.ServesRoute(rq.route) {
			denied = &routeError{http.StatusNotFound, "route_not_supported", fmt.Sprintf("model %s does not serve %s", model, rq.route)}
			continue
		}
		if !r.allowed(ctx, endpoint, rq.auth) {
			denied = &routeError{http.StatusForbidden, "model_not_allowed", fmt.Sprintf("model %s is not available to this workspace", model)}
			continue
		}
		if first == nil {
			first = endpoint
		}
		if len(r.servingReplicas(ctx, endpoint, rq.pinReplica, nil)) > 0 {
			return endpoint, nil
		}
	}
	if first != nil {
		return first, nil
	}
	if denied != nil {
		return nil, denied
	}
	return nil, &routeError{http.StatusNotFound, "model_not_found", fmt.Sprintf("model %s not found", rq.models[0])}
}

func (r *router) allowed(ctx context.Context, endpoint *types.ManagedEndpoint, authInfo *auth.AuthInfo) bool {
	if authInfo.Token.TokenType == types.TokenTypeClusterAdmin {
		return true
	}
	if admin, err := r.s.AdminWorkspace(ctx); err == nil && admin.Id == authInfo.Workspace.Id {
		return true
	}
	if endpoint.Spec.Catalog.Public {
		return true
	}
	for _, ws := range endpoint.Spec.Catalog.AllowedWorkspaces {
		if ws == authInfo.Workspace.ExternalId || ws == authInfo.Workspace.Name {
			return true
		}
	}
	return false
}

// admit applies the credit gate and concurrency caps.
func (r *router) admit(ctx context.Context, rq *routeRequest, endpoint *types.ManagedEndpoint) *routeError {
	billable := !endpoint.Spec.Catalog.Free && !endpoint.Spec.Pricing.IsZero()
	if billable && r.s.scheduler != nil && rq.auth.Token.TokenType != types.TokenTypeClusterAdmin {
		if gate := r.s.scheduler.CreditGate(); gate != nil {
			if err := gate.Check(ctx, rq.auth.Workspace.ExternalId); err != nil {
				var insufficient *types.InsufficientCreditsError
				if errors.As(err, &insufficient) {
					return &routeError{http.StatusPaymentRequired, "insufficient_credits", "insufficient credits: add credits to continue using inference endpoints"}
				}
				return &routeError{http.StatusServiceUnavailable, "billing_unavailable", "billing check unavailable"}
			}
		}
	}

	if limit := r.s.config.Routing.PerEndpointConcurrency; limit > 0 {
		c := r.counter(&r.admission, endpoint.Spec.ID)
		if c.Add(1) > int64(limit) {
			c.Add(-1)
			return &routeError{http.StatusTooManyRequests, "endpoint_saturated", "endpoint is at capacity, retry shortly"}
		}
	}
	if limit := r.s.config.Routing.PerWorkspaceConcurrency; limit > 0 {
		c := r.counter(&r.admission, endpoint.Spec.ID+"|"+rq.auth.Workspace.ExternalId)
		if c.Add(1) > int64(limit) {
			c.Add(-1)
			if lim := r.s.config.Routing.PerEndpointConcurrency; lim > 0 {
				r.counter(&r.admission, endpoint.Spec.ID).Add(-1)
			}
			return &routeError{http.StatusTooManyRequests, "rate_limited", "too many concurrent requests for this workspace"}
		}
	}
	return nil
}

func (r *router) release(rq *routeRequest, endpoint *types.ManagedEndpoint) {
	if r.s.config.Routing.PerEndpointConcurrency > 0 {
		r.counter(&r.admission, endpoint.Spec.ID).Add(-1)
	}
	if r.s.config.Routing.PerWorkspaceConcurrency > 0 {
		r.counter(&r.admission, endpoint.Spec.ID+"|"+rq.auth.Workspace.ExternalId).Add(-1)
	}
}

// servingReplicas lists replicas that may take this request right now.
func (r *router) servingReplicas(ctx context.Context, endpoint *types.ManagedEndpoint, pin string, exclude map[string]bool) []*types.EndpointReplica {
	replicas, err := r.s.repo.ListReplicas(ctx, endpoint.Spec.ID)
	if err != nil {
		return nil
	}
	var out []*types.EndpointReplica
	for _, replica := range replicas {
		if exclude[replica.ID] || replica.Address == "" {
			continue
		}
		if pin != "" {
			if replica.ID == pin && replica.Status == types.ReplicaStatusReady {
				return []*types.EndpointReplica{replica}
			}
			continue
		}
		if !replica.Serving() || replica.Version != endpoint.Version && !r.isCanary(ctx, endpoint, replica) {
			continue
		}
		if replica.Role != types.ReplicaRoleServe && replica.Role != types.ReplicaRoleDecode {
			continue
		}
		out = append(out, replica)
	}
	return out
}

// isCanary lets baking canary replicas take a share of traffic.
func (r *router) isCanary(ctx context.Context, endpoint *types.ManagedEndpoint, replica *types.EndpointReplica) bool {
	rollout, err := r.s.repo.GetRollout(ctx, endpoint.Spec.ID)
	return err == nil && rollout != nil && rollout.CanaryVersion != 0 && replica.Version == rollout.CanaryVersion
}

// pick waits (bounded) for a serving replica and selects one.
func (r *router) pick(ctx context.Context, rq *routeRequest, endpoint *types.ManagedEndpoint, exclude map[string]bool) (*types.EndpointReplica, *routeError) {
	deadline := rq.startedAt.Add(r.s.config.Routing.MaxQueueWaitOrDefault())
	for {
		candidates := r.servingReplicas(ctx, endpoint, rq.pinReplica, exclude)
		if len(candidates) > 0 {
			if replica := r.choose(ctx, rq, endpoint, candidates); replica != nil {
				rq.queueWait = time.Since(rq.startedAt)
				return replica, nil
			}
		}
		if time.Now().After(deadline) {
			if len(candidates) == 0 && len(exclude) == 0 {
				return nil, &routeError{http.StatusServiceUnavailable, "no_capacity", fmt.Sprintf("model %s has no ready replicas", endpoint.Spec.ID)}
			}
			return nil, &routeError{http.StatusTooManyRequests, "endpoint_saturated", "all replicas are busy, retry shortly"}
		}
		select {
		case <-ctx.Done():
			return nil, &routeError{499, "client_closed", "client closed request"}
		case <-time.After(queuePollInterval):
		}
	}
}

// select scores candidates. LLM routes use llmroute (capacity, pressure,
// affinity, power-of-two); other kinds pick the least loaded replica. Nil
// means every candidate is saturated.
func (r *router) choose(ctx context.Context, rq *routeRequest, endpoint *types.ManagedEndpoint, candidates []*types.EndpointReplica) *types.EndpointReplica {
	now := time.Now()
	slowStart := r.s.config.Routing.SlowStartOrDefault()
	state := r.state(endpoint.Spec.ID)

	var eligible []llmroute.Candidate
	for _, replica := range candidates {
		local := r.counter(&r.inflight, replica.ID).Load()
		pressure, _ := state.Pressure(ctx, replica.ID)
		inFlight := local
		if pressure.ActiveStreams > inFlight {
			inFlight = pressure.ActiveStreams
		}
		if replica.Capacity.MaxConcurrency > 0 && inFlight >= replica.Capacity.MaxConcurrency {
			continue
		}
		penalty := int64(0)
		if !replica.ReadyAt.IsZero() && now.Sub(replica.ReadyAt) < slowStart {
			remaining := 1 - float64(now.Sub(replica.ReadyAt))/float64(slowStart)
			penalty = int64(remaining * 8)
		}
		eligible = append(eligible, llmroute.Candidate{
			ID:          replica.ID,
			Connections: local + penalty,
			Pressure:    pressure,
			Engine:      engineFromCapacity(replica.Capacity),
			ContextLen:  int64(endpoint.Spec.Catalog.ContextLength),
			Payload:     replica,
		})
	}
	if len(eligible) == 0 {
		return nil
	}

	var affinity llmroute.Affinity
	if rq.adapter.LLM && rq.info != nil {
		affinity = state.Affinity(ctx, rq.info)
	}
	selection, ok := r.selector.Select(eligible, affinity, rq.info)
	if !ok {
		return nil
	}
	return selection.Candidate.Payload.(*types.EndpointReplica)
}

func engineFromCapacity(c types.ReplicaCapacity) llmroute.EngineMetrics {
	return llmroute.EngineMetrics{
		RunningRequests:       c.Running,
		WaitingRequests:       c.Waiting,
		TTFTMs:                c.TTFTMs,
		TPOTMs:                c.TPOTMs,
		DecodeTokensPerSecond: c.DecodeTokensPerSec,
		PromptTokensPerSecond: c.PromptTokensPerSec,
		GPUCacheUsageMilli:    1000 - c.KVCacheFreeMilli,
		PrefixCacheHitMilli:   c.PrefixCacheHitMilli,
		UpdatedAtUnixMs:       time.Now().UnixMilli(),
	}
}

// --- proxy -------------------------------------------------------------------

func (r *router) transport(address string) *http.Transport {
	if t, ok := r.transports.Load(address); ok {
		return t.(*http.Transport)
	}
	transport := &http.Transport{
		MaxIdleConns:        512,
		MaxIdleConnsPerHost: 64,
		IdleConnTimeout:     90 * time.Second,
		DisableCompression:  true,
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			conn, err := network.ConnectToBackend(ctx, address, replicaDialTimeout, r.s.tailscale, r.s.appConfig.Tailscale, r.s.containers)
			if err != nil {
				return nil, err
			}
			// -1: no read deadline; streams are bounded by the request context.
			abstractions.SetConnOptions(conn, true, 30*time.Second, -1)
			return conn, nil
		},
	}
	actual, loaded := r.transports.LoadOrStore(address, transport)
	if loaded {
		transport.CloseIdleConnections()
	}
	return actual.(*http.Transport)
}

func (r *router) upstreamPath(rq *routeRequest, endpoint *types.ManagedEndpoint) string {
	if rq.adapter.UpstreamPath != "" {
		return rq.adapter.UpstreamPath
	}
	// custom: /v1/models/<id>/invoke -> /invoke on the engine
	return "/invoke"
}

var hopHeaders = map[string]bool{
	"Connection": true, "Keep-Alive": true, "Proxy-Authenticate": true, "Proxy-Authorization": true,
	"Te": true, "Trailer": true, "Transfer-Encoding": true, "Upgrade": true, "Authorization": true,
	"Content-Length": true, "Host": true,
}

func (r *router) upstreamRequest(ctx context.Context, rq *routeRequest, endpoint *types.ManagedEndpoint, replica *types.EndpointReplica) (*http.Request, error) {
	url := "http://replica" + r.upstreamPath(rq, endpoint)
	if q := rq.ctx.Request().URL.RawQuery; q != "" {
		url += "?" + q
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(rq.body))
	if err != nil {
		return nil, err
	}
	for name, values := range rq.ctx.Request().Header {
		if hopHeaders[http.CanonicalHeaderKey(name)] || strings.HasPrefix(name, "X-Beam-") {
			continue
		}
		for _, v := range values {
			req.Header.Add(name, v)
		}
	}
	req.Header.Set(headerRequestID, rq.requestID)
	req.Header.Set(headerEndpointID, endpoint.Spec.ID)
	req.ContentLength = int64(len(rq.body))
	return req, nil
}

// serve runs the selection -> proxy -> meter pipeline, retrying once on a
// different replica when the first attempt fails before any byte reached
// the client.
func (r *router) serve(rq *routeRequest, endpoint *types.ManagedEndpoint) error {
	ctx := rq.ctx.Request().Context()

	if rq.adapter.LLM {
		req := rq.ctx.Request()
		req.Body = io.NopCloser(bytes.NewReader(rq.body))
		info, err := llmroute.Inspect(req, r.prefix+"/"+string(rq.route), llmroute.InspectOptions{DefaultModel: endpoint.Spec.ID})
		if err == nil {
			info.RequestID = rq.requestID
			rq.info = info
			rq.stream = info.Stream
		}
	}

	exclude := map[string]bool{}
	for attempt := 0; attempt < 2; attempt++ {
		replica, rerr := r.pick(ctx, rq, endpoint, exclude)
		if rerr != nil {
			r.record(rq, endpoint, nil, rerr.Status, Usage{}, 0, 0, rerr.Message)
			return writeRouteError(rq.ctx, rerr)
		}

		retry, err := r.proxy(ctx, rq, endpoint, replica)
		if err == nil {
			return nil
		}
		if !retry {
			log.Warn().Err(err).Str("endpoint_id", endpoint.Spec.ID).Str("replica_id", replica.ID).Msg("managed endpoints: proxy failed after response started")
			return nil
		}
		exclude[replica.ID] = true
		rq.retried = true
		log.Warn().Err(err).Str("endpoint_id", endpoint.Spec.ID).Str("replica_id", replica.ID).Msg("managed endpoints: upstream failed before response; retrying")
	}
	rerr := &routeError{http.StatusBadGateway, "upstream_unavailable", "upstream replicas failed"}
	r.record(rq, endpoint, nil, rerr.Status, Usage{}, 0, 0, rerr.Message)
	return writeRouteError(rq.ctx, rerr)
}

// proxy sends the request to one replica and relays the response. The bool
// reports whether a retry on another replica is safe (nothing was written).
func (r *router) proxy(ctx context.Context, rq *routeRequest, endpoint *types.ManagedEndpoint, replica *types.EndpointReplica) (bool, error) {
	inflight := r.counter(&r.inflight, replica.ID)
	inflight.Add(1)
	defer inflight.Add(-1)

	state := r.state(endpoint.Spec.ID)
	var tokenPressure int64
	if rq.info != nil {
		tokenPressure = rq.info.TokenPressure
	}
	_ = state.AddPressure(ctx, replica.ID, 1, tokenPressure)
	defer func() { _ = state.AddPressure(context.Background(), replica.ID, -1, -tokenPressure) }()

	upstreamCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		// Keep draining upstream until the client is gone or the drain
		// context (gateway shutdown) ends.
		select {
		case <-r.s.drainCtx.Done():
			cancel()
		case <-upstreamCtx.Done():
		}
	}()

	req, err := r.upstreamRequest(upstreamCtx, rq, endpoint, replica)
	if err != nil {
		return true, err
	}
	sentAt := time.Now()
	resp, err := r.transport(replica.Address).RoundTrip(req)
	if err != nil {
		return true, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusBadGateway || resp.StatusCode == http.StatusServiceUnavailable || resp.StatusCode == http.StatusGatewayTimeout {
		io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
		return true, fmt.Errorf("upstream returned %d", resp.StatusCode)
	}

	if rq.info != nil {
		state.RecordAffinity(ctx, rq.info, replica.ID)
	}

	w := rq.ctx.Response()
	copyResponseHeaders(w.Header(), resp.Header)
	w.Header().Set(headerRequestID, rq.requestID)
	w.Header().Set(headerReplicaServed, replica.ID)

	contentType, _, _ := mime.ParseMediaType(resp.Header.Get("Content-Type"))
	if contentType == "text/event-stream" {
		// Headers arrive with the first token on SSE, so this is a real TTFT.
		// Buffered JSON responses carry the whole generation and record none.
		ttft := time.Since(sentAt)
		w.WriteHeader(resp.StatusCode)
		usage, err := r.relayStream(w, resp.Body)
		cost := r.cost(endpoint, usage)
		r.record(rq, endpoint, replica, resp.StatusCode, usage, cost, ttft, errString(err))
		r.finishRoute(rq, endpoint, replica, resp.StatusCode, usage, cost, ttft, err)
		return false, err
	}
	ttft := time.Duration(0)

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBody))
	if err != nil {
		return false, err
	}
	usage := Usage{}
	if resp.StatusCode < 300 {
		usage = rq.adapter.Usage(body)
		billable := !endpoint.Spec.Catalog.Free && !endpoint.Spec.Pricing.IsZero()
		if billable && !usage.Found {
			rerr := &routeError{http.StatusBadGateway, "missing_usage", "upstream response carried no usage; request not billed"}
			r.record(rq, endpoint, replica, rerr.Status, usage, 0, 0, rerr.Message)
			r.s.emit(types.EventEndpointHarness, types.EventEndpointSchema{
				EndpointID: endpoint.Spec.ID, Action: "route.missing_usage", ReplicaID: replica.ID, GPU: replica.GPU, Version: replica.Version,
			})
			return false, writeRouteError(rq.ctx, rerr)
		}
	}
	cost := r.cost(endpoint, usage)
	if resp.StatusCode < 300 && strings.Contains(contentType, "json") {
		body = decorateJSON(body, rq.requestID, usage, cost)
	}
	w.Header().Set("Content-Length", fmt.Sprint(len(body)))
	w.WriteHeader(resp.StatusCode)
	_, werr := w.Write(body)
	r.record(rq, endpoint, replica, resp.StatusCode, usage, cost, ttft, "")
	r.finishRoute(rq, endpoint, replica, resp.StatusCode, usage, cost, ttft, werr)
	return false, nil
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func copyResponseHeaders(dst, src http.Header) {
	for name, values := range src {
		if hopHeaders[http.CanonicalHeaderKey(name)] && name != "Content-Length" {
			continue
		}
		if name == "Content-Length" {
			continue
		}
		for _, v := range values {
			dst.Add(name, v)
		}
	}
}

// relayStream forwards SSE events as they arrive, flushing per event, and
// pulls usage from the final chunk (decorating it with cost).
func (r *router) relayStream(w *echo.Response, body io.Reader) (Usage, error) {
	flusher, _ := w.Writer.(http.Flusher)
	reader := bufio.NewReaderSize(body, 64<<10)
	var usage Usage
	for {
		line, err := reader.ReadBytes('\n')
		if len(line) > 0 {
			if u, ok := sseUsage(line); ok {
				usage = u
			}
			if _, werr := w.Write(line); werr != nil {
				return usage, werr
			}
			if flusher != nil && (len(bytes.TrimSpace(line)) == 0 || bytes.HasPrefix(line, []byte("data:"))) {
				flusher.Flush()
			}
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				return usage, nil
			}
			return usage, err
		}
	}
}

// decorateJSON adds OpenRouter-style fields to a successful JSON object body.
func decorateJSON(body []byte, requestID string, usage Usage, costMicro int64) []byte {
	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil || payload == nil {
		return body
	}
	if _, ok := payload["id"]; !ok {
		payload["id"] = requestID
	}
	payload["provider"] = providerName
	if u, ok := payload["usage"].(map[string]any); ok && usage.Found {
		u["cost"] = costUSD(costMicro)
		payload["usage"] = u
	}
	out, err := json.Marshal(payload)
	if err != nil {
		return body
	}
	return out
}

func (r *router) cost(endpoint *types.ManagedEndpoint, usage Usage) int64 {
	if endpoint.Spec.Catalog.Free || !usage.Found {
		return 0
	}
	cost, err := computeCostMicroUSD(endpoint.Spec.Pricing, usage)
	if err != nil {
		log.Warn().Err(err).Str("endpoint_id", endpoint.Spec.ID).Msg("managed endpoints: pricing error")
		return 0
	}
	return cost
}

// --- metering ------------------------------------------------------------------

type usageRecord struct {
	event types.EventEndpointRouteSchema
	usage types.EndpointUsage
}

// record writes the route sample used by the controller (rollouts, demand).
func (r *router) record(rq *routeRequest, endpoint *types.ManagedEndpoint, replica *types.EndpointReplica, status int, usage Usage, cost int64, ttft time.Duration, errMsg string) {
	sample := types.RouteSample{
		EndpointID:       endpoint.Spec.ID,
		StatusCode:       status,
		PromptTokens:     usage.PromptTokens,
		CompletionTokens: usage.CompletionTokens,
		Images:           usage.Images,
		CostMicroUSD:     cost,
		Duration:         time.Since(rq.startedAt),
		TTFT:             ttft,
		QueueWait:        rq.queueWait,
		At:               time.Now(),
	}
	if replica != nil {
		sample.GPU = replica.GPU
		sample.ReplicaID = replica.ID
		sample.Version = replica.Version
	}
	if err := r.s.repo.RecordRouteSample(context.Background(), sample); err != nil {
		log.Debug().Err(err).Msg("managed endpoints: record route sample")
	}
	if replica == nil {
		r.enqueue(rq, endpoint, nil, status, usage, cost, 0, errMsg)
	}
}

// finishRoute emits the route/usage events and counters for a served request.
func (r *router) finishRoute(rq *routeRequest, endpoint *types.ManagedEndpoint, replica *types.EndpointReplica, status int, usage Usage, cost int64, ttft time.Duration, err error) {
	r.enqueue(rq, endpoint, replica, status, usage, cost, ttft, errString(err))
}

func (r *router) enqueue(rq *routeRequest, endpoint *types.ManagedEndpoint, replica *types.EndpointReplica, status int, usage Usage, cost int64, ttft time.Duration, errMsg string) {
	event := types.EventEndpointRouteSchema{
		EndpointID:       endpoint.Spec.ID,
		WorkspaceID:      rq.auth.Workspace.ExternalId,
		TokenID:          rq.auth.Token.ExternalId,
		RequestID:        rq.requestID,
		Route:            string(rq.route),
		Model:            rq.models[0],
		Version:          endpoint.Version,
		StatusCode:       status,
		Stream:           rq.stream,
		Retried:          rq.retried,
		PromptTokens:     usage.PromptTokens,
		CompletionTokens: usage.CompletionTokens,
		CachedTokens:     usage.CachedTokens,
		Images:           usage.Images,
		CostMicroUSD:     cost,
		DurationMs:       time.Since(rq.startedAt).Milliseconds(),
		TTFTMs:           ttft.Milliseconds(),
		QueueWaitMs:      rq.queueWait.Milliseconds(),
		Error:            errMsg,
		Timestamp:        time.Now().UTC(),
	}
	if replica != nil {
		event.Version = replica.Version
		event.ReplicaID = replica.ID
		event.ContainerID = replica.ContainerID
		event.GPU = replica.GPU
		event.Role = replica.Role
		event.Locality = replica.Locality
	}
	if rq.info != nil {
		event.RouteReason = rq.info.RouteReason
		if rq.info.PrefixCacheMatches > 0 || strings.Contains(rq.info.RouteReason, "affinity") {
			event.KVHitSource = "engine"
		} else {
			event.KVHitSource = "none"
		}
	}
	record := usageRecord{event: event}
	if usage.Found && status < 300 {
		record.usage = types.EndpointUsage{
			EndpointID:       endpoint.Spec.ID,
			WorkspaceID:      rq.auth.Workspace.ExternalId,
			TokenID:          rq.auth.Token.ExternalId,
			PromptTokens:     usage.PromptTokens,
			CompletionTokens: usage.CompletionTokens,
			CachedTokens:     usage.CachedTokens,
			Requests:         1,
			Images:           usage.Images,
			CostMicroUSD:     cost,
		}
	}
	select {
	case r.usageQueue <- record:
	default:
		log.Warn().Str("endpoint_id", endpoint.Spec.ID).Msg("managed endpoints: usage queue full; dropping route record")
	}
}

// drainUsage persists route records off the request path. It runs until the
// service context ends and then flushes what is queued.
func (r *router) drainUsage() {
	defer r.usageWG.Done()
	for {
		select {
		case rec := <-r.usageQueue:
			r.persist(rec)
		case <-r.s.ctx.Done():
			for {
				select {
				case rec := <-r.usageQueue:
					r.persist(rec)
				default:
					return
				}
			}
		}
	}
}

func (r *router) persist(rec usageRecord) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	r.s.emitRoute(rec.event)
	_ = r.s.repo.SaveGeneration(ctx, &rec.event, generationTTL)
	if rec.usage.WorkspaceID == "" {
		return
	}
	if err := r.s.repo.AddWorkspaceUsage(ctx, rec.usage, rec.event.Timestamp); err != nil {
		log.Debug().Err(err).Msg("managed endpoints: workspace usage")
	}
	r.s.emit(types.EventEndpointUsage, types.EventEndpointSchema{
		EndpointID:  rec.event.EndpointID,
		Action:      "usage",
		WorkspaceID: rec.event.WorkspaceID,
		ReplicaID:   rec.event.ReplicaID,
		GPU:         rec.event.GPU,
		Role:        rec.event.Role,
		Locality:    rec.event.Locality,
		Version:     rec.event.Version,
		Data: map[string]any{
			"token_id":          rec.usage.TokenID,
			"request_id":        rec.event.RequestID,
			"prompt_tokens":     rec.usage.PromptTokens,
			"completion_tokens": rec.usage.CompletionTokens,
			"cached_tokens":     rec.usage.CachedTokens,
			"images":            rec.usage.Images,
			"cost_micro_usd":    rec.usage.CostMicroUSD,
			"duration_ms":       rec.event.DurationMs,
			"ttft_ms":           rec.event.TTFTMs,
			"status_code":       rec.event.StatusCode,
		},
		Timestamp: rec.event.Timestamp,
	})
	if r.s.usage != nil {
		labels := map[string]any{"workspace_id": rec.usage.WorkspaceID, "endpoint_id": rec.usage.EndpointID}
		if rec.usage.PromptTokens > 0 {
			_ = r.s.usage.IncrementCounter(types.UsageMetricsEndpointPromptTokens, labels, float64(rec.usage.PromptTokens))
		}
		if rec.usage.CompletionTokens > 0 {
			_ = r.s.usage.IncrementCounter(types.UsageMetricsEndpointCompletionTokens, labels, float64(rec.usage.CompletionTokens))
		}
		if rec.usage.Images > 0 {
			_ = r.s.usage.IncrementCounter(types.UsageMetricsEndpointImages, labels, float64(rec.usage.Images))
		}
		_ = r.s.usage.IncrementCounter(types.UsageMetricsEndpointRequests, labels, 1)
		if rec.usage.CostMicroUSD > 0 {
			// Billing consumes cents, like container_cost_cents.
			_ = r.s.usage.IncrementCounter(types.UsageMetricsEndpointCost, labels, float64(rec.usage.CostMicroUSD)/10_000)
			if r.s.scheduler != nil {
				if gate := r.s.scheduler.CreditGate(); gate != nil {
					gate.Invalidate(ctx, rec.usage.WorkspaceID)
				}
			}
		}
	}
}
