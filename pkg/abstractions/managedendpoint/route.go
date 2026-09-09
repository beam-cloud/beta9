package managedendpoint

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"mime"
	"mime/multipart"
	"net"
	"net/http"
	"slices"
	"sort"
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
// per-route differences. Listings mirror OpenRouter's paths and field names
// so its SDKs and provider tooling work unchanged.

const (
	maxRequestBody        = 64 << 20
	maxResponseBody       = 64 << 20
	queuePollInterval     = 100 * time.Millisecond
	replicaDialTimeout    = 5 * time.Second
	generationTTL         = time.Hour
	usageQueueSize        = 4096
	headerReplicaPin      = "X-Beam-Endpoint-Replica"
	headerRequestID       = "X-Request-ID"
	headerEndpointID      = "X-Beam-Endpoint-ID"
	headerReplicaServed   = "X-Beam-Replica"
	providerName          = "beam"
	providerSchemaVersion = "2.4"
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

func counter(m *sync.Map, key string) *atomic.Int64 {
	v, _ := m.LoadOrStore(key, &atomic.Int64{})
	return v.(*atomic.Int64)
}

// --- adapters ------------------------------------------------------------------

// An adapter describes how one OpenAI-style route is proxied and metered.
// Adding a modality (audio, ...) is a new adapter, nothing else.
type adapter struct {
	// UpstreamPath is the path on the engine; empty means /invoke.
	UpstreamPath string
	// LLM routes use prompt/session affinity and token-aware selection.
	LLM bool
	// Streamable routes accept "stream": true and must carry usage in the
	// final SSE chunk.
	Streamable bool
	// Usage extracts billable usage from a complete (non-stream) JSON body.
	Usage func(body []byte) Usage
}

var adapters = map[types.EndpointRoute]adapter{
	types.EndpointRouteChatCompletions:  {UpstreamPath: "/v1/chat/completions", LLM: true, Streamable: true, Usage: tokenUsage},
	types.EndpointRouteCompletions:      {UpstreamPath: "/v1/completions", LLM: true, Streamable: true, Usage: tokenUsage},
	types.EndpointRouteEmbeddings:       {UpstreamPath: "/v1/embeddings", Usage: tokenUsage},
	types.EndpointRouteImageGenerations: {UpstreamPath: "/v1/images/generations", Usage: imageUsage},
	types.EndpointRouteImageEdits:       {UpstreamPath: "/v1/images/edits", Usage: imageUsage},
	types.EndpointRouteInvoke:           {UpstreamPath: "/invoke", Usage: func([]byte) Usage { return Usage{Requests: 1, Found: true} }},
}

// Usage is the authoritative billable usage extracted from an upstream
// response. Never estimated: when the engine reports nothing, the request is
// not billed and is flagged as missing usage.
type Usage struct {
	PromptTokens     int64
	CompletionTokens int64
	CachedTokens     int64
	Images           int64
	Requests         int64
	// Found reports whether the response carried a usage object at all.
	Found bool
}

// tokenUsage reads the OpenAI usage object from a response body.
func tokenUsage(body []byte) Usage {
	var env struct {
		Usage *struct {
			PromptTokens        int64 `json:"prompt_tokens"`
			CompletionTokens    int64 `json:"completion_tokens"`
			PromptTokensDetails *struct {
				CachedTokens int64 `json:"cached_tokens"`
			} `json:"prompt_tokens_details"`
		} `json:"usage"`
	}
	if err := json.Unmarshal(body, &env); err != nil || env.Usage == nil {
		return Usage{}
	}
	u := Usage{PromptTokens: env.Usage.PromptTokens, CompletionTokens: env.Usage.CompletionTokens, Requests: 1, Found: true}
	if env.Usage.PromptTokensDetails != nil {
		u.CachedTokens = env.Usage.PromptTokensDetails.CachedTokens
	}
	return u
}

// imageUsage counts generated images; token usage is added when present
// (gpt-image style engines report it).
func imageUsage(body []byte) Usage {
	var payload struct {
		Data []json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return Usage{}
	}
	u := tokenUsage(body)
	u.Images = int64(len(payload.Data))
	u.Requests = 1
	u.Found = u.Images > 0 || u.Found
	return u
}

// sseUsage scans one SSE data line for a usage object.
func sseUsage(line []byte) (Usage, bool) {
	payload := bytes.TrimSpace(bytes.TrimPrefix(line, []byte("data:")))
	if len(payload) == 0 || bytes.Equal(payload, []byte("[DONE]")) || !bytes.Contains(payload, []byte(`"usage"`)) {
		return Usage{}, false
	}
	u := tokenUsage(payload)
	return u, u.Found
}

// forceIncludeUsage rewrites a streaming request so the engine emits a final
// usage chunk (stream_options.include_usage). Reports whether it is a stream.
func forceIncludeUsage(payload map[string]any) bool {
	if stream, _ := payload["stream"].(bool); !stream {
		return false
	}
	opts, _ := payload["stream_options"].(map[string]any)
	if opts == nil {
		opts = map[string]any{}
	}
	opts["include_usage"] = true
	payload["stream_options"] = opts
	return true
}

// routeFromPath maps "/v1/chat/completions" -> chat/completions and
// "/v1/models/<id>/invoke" -> invoke with the model id.
func routeFromPath(prefix, path string) (types.EndpointRoute, string, bool) {
	rest := strings.TrimPrefix(strings.TrimPrefix(strings.TrimSuffix(path, "/"), prefix), "/")
	if strings.HasPrefix(rest, "models/") && strings.HasSuffix(rest, "/invoke") {
		id := strings.TrimSuffix(strings.TrimPrefix(rest, "models/"), "/invoke")
		return types.EndpointRouteInvoke, id, id != ""
	}
	route := types.EndpointRoute(rest)
	_, ok := adapters[route]
	return route, "", ok && route != types.EndpointRouteInvoke
}

// --- pricing -------------------------------------------------------------------

var microUSD = big.NewRat(1_000_000, 1)

// computeCostMicroUSD prices usage with exact rational arithmetic and rounds
// half-up to micro-dollars. Cached prompt tokens are billed at the cached
// rate when one is set and at the prompt rate otherwise; they are a subset of
// PromptTokens.
func computeCostMicroUSD(p types.Pricing, u Usage) (int64, error) {
	if p.IsZero() {
		return 0, nil
	}
	cached := min(u.CachedTokens, u.PromptTokens)
	if p.CachedPromptTokens == "" {
		cached = 0
	}
	total := new(big.Rat)
	for _, line := range []struct {
		price    string
		quantity int64
	}{
		{p.PromptTokens, u.PromptTokens - cached},
		{p.CachedPromptTokens, cached},
		{p.CompletionTokens, u.CompletionTokens},
		{p.Image, u.Images},
		{p.Request, u.Requests},
	} {
		if line.quantity <= 0 || line.price == "" {
			continue
		}
		rate, err := types.PricingRat(line.price)
		if err != nil {
			return 0, err
		}
		total.Add(total, new(big.Rat).Mul(rate, big.NewRat(line.quantity, 1)))
	}
	total.Mul(total, microUSD)
	total.Add(total, big.NewRat(1, 2)) // round half-up
	return new(big.Int).Quo(total.Num(), total.Denom()).Int64(), nil
}

// costUSD renders micro-dollars as the float OpenRouter puts in usage.cost.
func costUSD(microUSD int64) float64 { return float64(microUSD) / 1_000_000 }

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

func billable(endpoint *types.ManagedEndpoint) bool {
	return !endpoint.Spec.Catalog.Free && !endpoint.Spec.Pricing.IsZero()
}

// --- errors --------------------------------------------------------------------

type routeError struct {
	Status  int
	Code    string
	Message string
}

func (e *routeError) Error() string { return e.Message }

func (e *routeError) write(ctx echo.Context) error {
	kind := "invalid_request_error"
	switch {
	case e.Status == http.StatusUnauthorized || e.Status == http.StatusForbidden:
		kind = "authentication_error"
	case e.Status == http.StatusPaymentRequired:
		kind = "insufficient_quota"
	case e.Status == http.StatusTooManyRequests:
		kind = "rate_limit_error"
	case e.Status == http.StatusNotFound:
		kind = "not_found_error"
	case e.Status >= 500:
		kind = "server_error"
	}
	return ctx.JSON(e.Status, map[string]any{"error": map[string]any{"message": e.Message, "type": kind, "code": e.Code}})
}

// --- request pipeline ----------------------------------------------------------

// routeRequest is the state of one inference request through the pipeline.
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
		return (&routeError{http.StatusUnauthorized, "unauthorized", "a workspace token is required"}).write(ctx)
	}
	if !r.s.Enabled() {
		return (&routeError{http.StatusNotFound, "not_found", "managed endpoints are not enabled"}).write(ctx)
	}
	route, pathModel, ok := routeFromPath(r.prefix, ctx.Request().URL.Path)
	if !ok {
		return (&routeError{http.StatusNotFound, "not_found", "unknown route"}).write(ctx)
	}
	if pathModel == "" && ctx.Param("slug") != "" {
		pathModel = modelParam(ctx)
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
	if cc.AuthInfo.Token.TokenType != types.TokenTypeClusterAdmin {
		rq.pinReplica = ""
	}
	if rerr := r.readRequest(rq, pathModel); rerr != nil {
		return rerr.write(ctx)
	}
	endpoint, rerr := r.resolveEndpoint(ctx.Request().Context(), rq)
	if rerr != nil {
		return rerr.write(ctx)
	}
	if rerr := r.admit(ctx.Request().Context(), rq, endpoint); rerr != nil {
		return rerr.write(ctx)
	}
	defer r.release(rq, endpoint)
	return r.serve(rq, endpoint)
}

// modelParam joins the optional :author and :slug path params into a model id.
func modelParam(ctx echo.Context) string {
	if author := ctx.Param("author"); author != "" {
		return author + "/" + ctx.Param("slug")
	}
	return ctx.Param("slug")
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

	if contentType, _, _ := mime.ParseMediaType(req.Header.Get("Content-Type")); strings.HasPrefix(contentType, "multipart/") {
		rq.models = multipartModel(req.Header.Get("Content-Type"), body)
	} else {
		if len(bytes.TrimSpace(body)) > 0 {
			if err := json.Unmarshal(body, &rq.payload); err != nil {
				return &routeError{http.StatusBadRequest, "invalid_json", "request body must be a JSON object"}
			}
		}
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
		if rq.payload != nil && rq.adapter.Streamable && forceIncludeUsage(rq.payload) {
			rq.stream = true
			if body, err := json.Marshal(rq.payload); err == nil {
				rq.body = body
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
	switch {
	case first != nil:
		return first, nil
	case denied != nil:
		return nil, denied
	}
	return nil, &routeError{http.StatusNotFound, "model_not_found", fmt.Sprintf("model %s not found", rq.models[0])}
}

func (r *router) allowed(ctx context.Context, endpoint *types.ManagedEndpoint, authInfo *auth.AuthInfo) bool {
	if authInfo.Token.TokenType == types.TokenTypeClusterAdmin || endpoint.Spec.Catalog.Public {
		return true
	}
	if admin, err := r.s.AdminWorkspace(ctx); err == nil && admin.Id == authInfo.Workspace.Id {
		return true
	}
	allowed := endpoint.Spec.Catalog.AllowedWorkspaces
	return slices.Contains(allowed, authInfo.Workspace.ExternalId) || slices.Contains(allowed, authInfo.Workspace.Name)
}

// admit applies the credit gate and concurrency caps.
func (r *router) admit(ctx context.Context, rq *routeRequest, endpoint *types.ManagedEndpoint) *routeError {
	if billable(endpoint) && r.s.scheduler != nil && rq.auth.Token.TokenType != types.TokenTypeClusterAdmin {
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
	endpointCap, workspaceCap := r.s.config.Routing.PerEndpointConcurrency, r.s.config.Routing.PerWorkspaceConcurrency
	if endpointCap > 0 {
		if c := counter(&r.admission, endpoint.Spec.ID); c.Add(1) > int64(endpointCap) {
			c.Add(-1)
			return &routeError{http.StatusTooManyRequests, "endpoint_saturated", "endpoint is at capacity, retry shortly"}
		}
	}
	if workspaceCap > 0 {
		if c := counter(&r.admission, endpoint.Spec.ID+"|"+rq.auth.Workspace.ExternalId); c.Add(1) > int64(workspaceCap) {
			c.Add(-1)
			if endpointCap > 0 {
				counter(&r.admission, endpoint.Spec.ID).Add(-1)
			}
			return &routeError{http.StatusTooManyRequests, "rate_limited", "too many concurrent requests for this workspace"}
		}
	}
	return nil
}

func (r *router) release(rq *routeRequest, endpoint *types.ManagedEndpoint) {
	if r.s.config.Routing.PerEndpointConcurrency > 0 {
		counter(&r.admission, endpoint.Spec.ID).Add(-1)
	}
	if r.s.config.Routing.PerWorkspaceConcurrency > 0 {
		counter(&r.admission, endpoint.Spec.ID+"|"+rq.auth.Workspace.ExternalId).Add(-1)
	}
}

// servingReplicas lists replicas that may take this request right now:
// ready serve/decode replicas on the active version or a baking canary.
func (r *router) servingReplicas(ctx context.Context, endpoint *types.ManagedEndpoint, pin string, exclude map[string]bool) []*types.EndpointReplica {
	replicas, err := r.s.repo.ListReplicas(ctx, endpoint.Spec.ID)
	if err != nil {
		return nil
	}
	canaryVersion := uint(0)
	if rollout, err := r.s.repo.GetRollout(ctx, endpoint.Spec.ID); err == nil && rollout != nil {
		canaryVersion = rollout.CanaryVersion
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
		onCanary := canaryVersion != 0 && replica.Version == canaryVersion
		if !replica.Serving() || (replica.Version != endpoint.Version && !onCanary) {
			continue
		}
		if replica.Role == types.ReplicaRoleServe || replica.Role == types.ReplicaRoleDecode {
			out = append(out, replica)
		}
	}
	return out
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

// choose scores candidates. LLM routes use llmroute (capacity, pressure,
// affinity, power-of-two); other kinds pick the least loaded replica. Nil
// means every candidate is saturated.
func (r *router) choose(ctx context.Context, rq *routeRequest, endpoint *types.ManagedEndpoint, candidates []*types.EndpointReplica) *types.EndpointReplica {
	now := time.Now()
	slowStart := r.s.config.Routing.SlowStartOrDefault()
	state := r.state(endpoint.Spec.ID)

	var eligible []llmroute.Candidate
	for _, replica := range candidates {
		local := counter(&r.inflight, replica.ID).Load()
		pressure, _ := state.Pressure(ctx, replica.ID)
		if replica.Capacity.MaxConcurrency > 0 && max(local, pressure.ActiveStreams) >= replica.Capacity.MaxConcurrency {
			continue
		}
		penalty := int64(0)
		if !replica.ReadyAt.IsZero() && now.Sub(replica.ReadyAt) < slowStart {
			penalty = int64((1 - float64(now.Sub(replica.ReadyAt))/float64(slowStart)) * 8)
		}
		eligible = append(eligible, llmroute.Candidate{
			ID:          replica.ID,
			Connections: local + penalty,
			Pressure:    pressure,
			Engine:      engineMetrics(replica.Capacity),
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

func engineMetrics(c types.ReplicaCapacity) llmroute.EngineMetrics {
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

// --- proxy ---------------------------------------------------------------------

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

var hopHeaders = map[string]bool{
	"Connection": true, "Keep-Alive": true, "Proxy-Authenticate": true, "Proxy-Authorization": true,
	"Te": true, "Trailer": true, "Transfer-Encoding": true, "Upgrade": true, "Authorization": true,
	"Content-Length": true, "Host": true,
}

func (r *router) upstreamRequest(ctx context.Context, rq *routeRequest, endpoint *types.ManagedEndpoint) (*http.Request, error) {
	url := "http://replica" + rq.adapter.UpstreamPath
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
		req.Header[name] = values
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
		if info, err := llmroute.Inspect(req, r.prefix+"/"+string(rq.route), llmroute.InspectOptions{DefaultModel: endpoint.Spec.ID}); err == nil {
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
			return rerr.write(rq.ctx)
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
	return rerr.write(rq.ctx)
}

// proxy sends the request to one replica and relays the response. The bool
// reports whether a retry on another replica is safe (nothing was written).
func (r *router) proxy(ctx context.Context, rq *routeRequest, endpoint *types.ManagedEndpoint, replica *types.EndpointReplica) (bool, error) {
	inflight := counter(&r.inflight, replica.ID)
	inflight.Add(1)
	defer inflight.Add(-1)

	state := r.state(endpoint.Spec.ID)
	var tokenPressure int64
	if rq.info != nil {
		tokenPressure = rq.info.TokenPressure
	}
	_ = state.AddPressure(ctx, replica.ID, 1, tokenPressure)
	defer func() { _ = state.AddPressure(context.Background(), replica.ID, -1, -tokenPressure) }()

	// Upstream lives until the client is gone or the gateway drains.
	upstreamCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		select {
		case <-r.s.drainCtx.Done():
			cancel()
		case <-upstreamCtx.Done():
		}
	}()

	req, err := r.upstreamRequest(upstreamCtx, rq, endpoint)
	if err != nil {
		return true, err
	}
	sentAt := time.Now()
	resp, err := r.transport(replica.Address).RoundTrip(req)
	if err != nil {
		return true, err
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
		return true, fmt.Errorf("upstream returned %d", resp.StatusCode)
	}
	if rq.info != nil {
		state.RecordAffinity(ctx, rq.info, replica.ID)
	}

	w := rq.ctx.Response()
	for name, values := range resp.Header {
		if !hopHeaders[http.CanonicalHeaderKey(name)] {
			w.Header()[name] = values
		}
	}
	w.Header().Set(headerRequestID, rq.requestID)
	w.Header().Set(headerReplicaServed, replica.ID)

	contentType, _, _ := mime.ParseMediaType(resp.Header.Get("Content-Type"))
	if contentType == "text/event-stream" {
		// Headers arrive with the first token on SSE, so this is a real TTFT.
		// Buffered JSON responses carry the whole generation and record none.
		ttft := time.Since(sentAt)
		w.WriteHeader(resp.StatusCode)
		usage, err := relayStream(w, resp.Body)
		r.record(rq, endpoint, replica, resp.StatusCode, usage, r.cost(endpoint, usage), ttft, errString(err))
		return false, err
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBody))
	if err != nil {
		return false, err
	}
	usage := Usage{}
	if resp.StatusCode < 300 {
		usage = rq.adapter.Usage(body)
		if billable(endpoint) && !usage.Found {
			rerr := &routeError{http.StatusBadGateway, "missing_usage", "upstream response carried no usage; request not billed"}
			r.record(rq, endpoint, replica, rerr.Status, usage, 0, 0, rerr.Message)
			r.s.emit(types.EventEndpointHarness, types.EventEndpointSchema{
				EndpointID: endpoint.Spec.ID, Action: "route.missing_usage", ReplicaID: replica.ID, GPU: replica.GPU, Version: replica.Version,
			})
			return false, rerr.write(rq.ctx)
		}
	}
	cost := r.cost(endpoint, usage)
	if resp.StatusCode < 300 && strings.Contains(contentType, "json") {
		body = decorateJSON(body, rq.requestID, usage, cost)
	}
	w.Header().Set("Content-Length", fmt.Sprint(len(body)))
	w.WriteHeader(resp.StatusCode)
	_, werr := w.Write(body)
	r.record(rq, endpoint, replica, resp.StatusCode, usage, cost, 0, errString(werr))
	return false, nil
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// relayStream forwards SSE events as they arrive, flushing per event, and
// pulls usage from the final chunk.
func relayStream(w *echo.Response, body io.Reader) (Usage, error) {
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
		if errors.Is(err, io.EOF) {
			return usage, nil
		}
		if err != nil {
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
	}
	out, err := json.Marshal(payload)
	if err != nil {
		return body
	}
	return out
}

// --- metering ------------------------------------------------------------------

type usageRecord struct {
	event types.EventEndpointRouteSchema
	usage types.EndpointUsage
}

// record writes the route sample the controller reads (rollouts, demand) and
// queues the route/usage events and counters for the request.
func (r *router) record(rq *routeRequest, endpoint *types.ManagedEndpoint, replica *types.EndpointReplica, status int, usage Usage, cost int64, ttft time.Duration, errMsg string) {
	now := time.Now()
	sample := types.RouteSample{
		EndpointID:       endpoint.Spec.ID,
		StatusCode:       status,
		PromptTokens:     usage.PromptTokens,
		CompletionTokens: usage.CompletionTokens,
		Images:           usage.Images,
		CostMicroUSD:     cost,
		Duration:         now.Sub(rq.startedAt),
		TTFT:             ttft,
		QueueWait:        rq.queueWait,
		At:               now,
	}
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
		DurationMs:       sample.Duration.Milliseconds(),
		TTFTMs:           ttft.Milliseconds(),
		QueueWaitMs:      rq.queueWait.Milliseconds(),
		Error:            errMsg,
		Timestamp:        now.UTC(),
	}
	if replica != nil {
		sample.GPU, sample.ReplicaID, sample.Version = replica.GPU, replica.ID, replica.Version
		event.Version, event.ReplicaID, event.ContainerID = replica.Version, replica.ID, replica.ContainerID
		event.GPU, event.Role, event.Locality = replica.GPU, replica.Role, replica.Locality
	}
	if rq.info != nil {
		event.RouteReason = rq.info.RouteReason
		event.KVHitSource = "none"
		if rq.info.PrefixCacheMatches > 0 || strings.Contains(rq.info.RouteReason, "affinity") {
			event.KVHitSource = "engine"
		}
	}
	if err := r.s.repo.RecordRouteSample(context.Background(), sample); err != nil {
		log.Debug().Err(err).Msg("managed endpoints: record route sample")
	}

	rec := usageRecord{event: event}
	if usage.Found && status < 300 {
		rec.usage = types.EndpointUsage{
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
	case r.usageQueue <- rec:
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

	if r.s.events != nil {
		r.s.events.PushEndpointRouteEvent(rec.event)
	}
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
	if r.s.usage == nil {
		return
	}
	labels := map[string]any{"workspace_id": rec.usage.WorkspaceID, "endpoint_id": rec.usage.EndpointID}
	for metric, value := range map[string]float64{
		types.UsageMetricsEndpointPromptTokens:     float64(rec.usage.PromptTokens),
		types.UsageMetricsEndpointCompletionTokens: float64(rec.usage.CompletionTokens),
		types.UsageMetricsEndpointImages:           float64(rec.usage.Images),
		types.UsageMetricsEndpointRequests:         1,
		types.UsageMetricsEndpointCost:             float64(rec.usage.CostMicroUSD) / 10_000, // billing consumes cents
	} {
		if value > 0 {
			_ = r.s.usage.IncrementCounter(metric, labels, value)
		}
	}
	if rec.usage.CostMicroUSD > 0 && r.s.scheduler != nil {
		if gate := r.s.scheduler.CreditGate(); gate != nil {
			gate.Invalidate(ctx, rec.usage.WorkspaceID)
		}
	}
}

// --- listings ------------------------------------------------------------------

func (r *router) visibleEndpoints(ctx context.Context, authInfo *auth.AuthInfo) ([]*types.ManagedEndpoint, error) {
	endpoints, err := r.s.repo.ListEndpoints(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]*types.ManagedEndpoint, 0, len(endpoints))
	for _, endpoint := range endpoints {
		if endpoint.Enabled && r.allowed(ctx, endpoint, authInfo) {
			out = append(out, endpoint)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Spec.ID < out[j].Spec.ID })
	return out, nil
}

func modalities(spec *types.ManagedEndpointSpec) (input []string, output []string) {
	input, output = []string{"text"}, []string{"text"}
	switch spec.Kind {
	case types.EndpointKindLLM:
		for _, m := range spec.Catalog.Modalities {
			switch m {
			case "image", "vision":
				input = append(input, "image")
			case "audio":
				input = append(input, "audio")
			}
		}
	case types.EndpointKindEmbedding:
		output = []string{"embeddings"}
	case types.EndpointKindImage:
		output = []string{"image"}
		if spec.ServesRoute(types.EndpointRouteImageEdits) {
			input = append(input, "image")
		}
	}
	return input, output
}

// pricingEntry renders per-unit prices as OpenRouter does ("0" when unset).
func pricingEntry(p types.Pricing, includeCacheRead bool) map[string]any {
	str := func(v string) string {
		if v == "" {
			return "0"
		}
		return v
	}
	entry := map[string]any{
		"prompt":     str(p.PromptTokens),
		"completion": str(p.CompletionTokens),
		"request":    str(p.Request),
		"image":      str(p.Image),
	}
	if includeCacheRead || p.CachedPromptTokens != "" {
		entry["input_cache_read"] = str(p.CachedPromptTokens)
	}
	return entry
}

func orNil[T comparable](v T) any {
	var zero T
	if v == zero {
		return nil
	}
	return v
}

func orEmpty(list []string) []string {
	if list == nil {
		return []string{}
	}
	return list
}

func (r *router) modelEntry(endpoint *types.ManagedEndpoint) map[string]any {
	spec := &endpoint.Spec
	input, output := modalities(spec)
	name := spec.Catalog.Name
	if name == "" {
		name = spec.ID
	}
	return map[string]any{
		"id":             spec.ID,
		"canonical_slug": spec.ID,
		"name":           name,
		"created":        endpoint.CreatedAt.Unix(),
		"description":    spec.Catalog.Description,
		"context_length": spec.Catalog.ContextLength,
		"architecture": map[string]any{
			"modality":          strings.Join(input, "+") + "->" + strings.Join(output, "+"),
			"input_modalities":  input,
			"output_modalities": output,
			"tokenizer":         spec.Catalog.Tokenizer,
			"instruct_type":     orNil(spec.Catalog.InstructType),
		},
		"pricing": pricingEntry(spec.Pricing, false),
		"top_provider": map[string]any{
			"context_length":        spec.Catalog.ContextLength,
			"max_completion_tokens": orNil(spec.Catalog.MaxCompletionTokens),
			"is_moderated":          false,
		},
		"per_request_limits":   nil,
		"supported_parameters": orEmpty(spec.Catalog.SupportedParameters),
		"hugging_face_id":      spec.Catalog.HFID,
		"owned_by":             providerName,
		"object":               "model",
	}
}

func (r *router) handleListModels(ctx echo.Context) error {
	cc := ctx.(*auth.HttpAuthContext)
	endpoints, err := r.visibleEndpoints(ctx.Request().Context(), cc.AuthInfo)
	if err != nil {
		return (&routeError{http.StatusServiceUnavailable, "registry_unavailable", "endpoint registry unavailable"}).write(ctx)
	}
	if ctx.QueryParam("format") == "openrouter-provider" {
		return r.providerDocument(ctx, endpoints)
	}
	data := make([]map[string]any, 0, len(endpoints))
	for _, endpoint := range endpoints {
		data = append(data, r.modelEntry(endpoint))
	}
	return ctx.JSON(http.StatusOK, map[string]any{"object": "list", "data": data})
}

// providerDocument renders the OpenRouter provider listing: one model per
// endpoint with readiness, capacity and datacenters derived from localities.
func (r *router) providerDocument(ctx echo.Context, endpoints []*types.ManagedEndpoint) error {
	replicas, _ := r.s.repo.ListAllReplicas(ctx.Request().Context())
	routePath := func(spec *types.ManagedEndpointSpec, route types.EndpointRoute) any {
		if !spec.ServesRoute(route) {
			return nil
		}
		return r.prefix + "/" + string(route)
	}
	models := make([]map[string]any, 0, len(endpoints))
	for _, endpoint := range endpoints {
		spec := &endpoint.Spec
		input, output := modalities(spec)
		ready, maxConcurrency, datacenters := 0, int64(0), []string{}
		for _, replica := range replicas {
			if replica.EndpointID != spec.ID || !replica.Serving() {
				continue
			}
			ready++
			maxConcurrency += replica.Capacity.MaxConcurrency
			if replica.Locality != "" && !slices.Contains(datacenters, replica.Locality) {
				datacenters = append(datacenters, replica.Locality)
			}
		}
		sort.Strings(datacenters)
		name := spec.Catalog.Name
		if name == "" {
			name = spec.ID
		}
		models = append(models, map[string]any{
			"id":                    spec.ID,
			"name":                  name,
			"hugging_face_id":       spec.Catalog.HFID,
			"is_ready":              ready > 0,
			"description":           spec.Catalog.Description,
			"context_length":        spec.Catalog.ContextLength,
			"max_completion_tokens": spec.Catalog.MaxCompletionTokens,
			"quantization":          "",
			"modalities":            map[string]any{"input": input, "output": output},
			"pricing":               pricingEntry(spec.Pricing, true),
			"capacity":              map[string]any{"ready_replicas": ready, "max_concurrency": maxConcurrency},
			"supported_parameters":  orEmpty(spec.Catalog.SupportedParameters),
			"datacenters":           datacenters,
			"endpoints": map[string]any{
				"chat_completions":  routePath(spec, types.EndpointRouteChatCompletions),
				"completions":       routePath(spec, types.EndpointRouteCompletions),
				"embeddings":        routePath(spec, types.EndpointRouteEmbeddings),
				"image_generations": routePath(spec, types.EndpointRouteImageGenerations),
			},
		})
	}
	return ctx.JSON(http.StatusOK, map[string]any{"schema_version": providerSchemaVersion, "provider": providerName, "models": models})
}

// handleModelEndpoints lists one entry per (gpu target, locality) with status
// and recent latency, mirroring OpenRouter's /models/:author/:slug/endpoints.
func (r *router) handleModelEndpoints(ctx echo.Context) error {
	cc := ctx.(*auth.HttpAuthContext)
	rctx := ctx.Request().Context()
	endpoint, err := r.s.repo.GetEndpoint(rctx, modelParam(ctx))
	if err != nil {
		return (&routeError{http.StatusServiceUnavailable, "registry_unavailable", "endpoint registry unavailable"}).write(ctx)
	}
	if endpoint == nil || !endpoint.Enabled || !r.allowed(rctx, endpoint, cc.AuthInfo) {
		return (&routeError{http.StatusNotFound, "model_not_found", "model not found"}).write(ctx)
	}
	replicas, _ := r.s.repo.ListReplicas(rctx, endpoint.Spec.ID)

	type groupKey struct{ gpu, locality string }
	groups := map[groupKey][]*types.EndpointReplica{}
	for _, replica := range replicas {
		if replica.Alive() && !replica.Tuning {
			key := groupKey{replica.GPU, replica.Locality}
			groups[key] = append(groups[key], replica)
		}
	}
	// Configured targets with no replicas anywhere still get a (down) entry.
	for _, rt := range endpoint.Spec.Targets() {
		key := groupKey{gpu: rt.Target.Key()}
		present := false
		for k := range groups {
			present = present || k.gpu == key.gpu
		}
		if !present {
			groups[key] = nil
		}
	}

	entries := make([]map[string]any, 0, len(groups))
	for key, list := range groups {
		ready := 0
		for _, replica := range list {
			if replica.Status == types.ReplicaStatusReady {
				ready++
			}
		}
		status := 0
		if ready == 0 {
			status = -1
		}
		entry := map[string]any{
			"name":                  endpoint.Spec.ID + " | " + key.gpu,
			"provider_name":         providerName,
			"tag":                   key.gpu,
			"gpu":                   key.gpu,
			"locality":              key.locality,
			"status":                status,
			"ready_replicas":        ready,
			"total_replicas":        len(list),
			"context_length":        endpoint.Spec.Catalog.ContextLength,
			"max_completion_tokens": orNil(endpoint.Spec.Catalog.MaxCompletionTokens),
			"quantization":          nil,
			"supported_parameters":  orEmpty(endpoint.Spec.Catalog.SupportedParameters),
			"pricing":               pricingEntry(endpoint.Spec.Pricing, false),
			"uptime_last_30m":       nil,
		}
		if metrics, _ := r.s.repo.GetRouteMetrics(rctx, endpoint.Spec.ID, key.gpu, 0, 15*time.Minute); metrics != nil && metrics.Requests > 0 {
			entry["latency_ms"] = metrics.MeanTTFTMs()
			entry["requests_15m"] = metrics.Requests
			entry["error_rate_15m"] = metrics.ErrorRate()
		}
		entries = append(entries, entry)
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i]["name"].(string) < entries[j]["name"].(string) })

	model := r.modelEntry(endpoint)
	model["endpoints"] = entries
	return ctx.JSON(http.StatusOK, map[string]any{"data": model})
}

// handleGeneration returns the metered record of one request by id.
func (r *router) handleGeneration(ctx echo.Context) error {
	cc := ctx.(*auth.HttpAuthContext)
	id := strings.TrimSpace(ctx.QueryParam("id"))
	if id == "" {
		return (&routeError{http.StatusBadRequest, "missing_id", "id query parameter is required"}).write(ctx)
	}
	record, err := r.s.repo.GetGeneration(ctx.Request().Context(), id)
	if err != nil || record == nil || (record.WorkspaceID != cc.AuthInfo.Workspace.ExternalId && cc.AuthInfo.Token.TokenType != types.TokenTypeClusterAdmin) {
		return (&routeError{http.StatusNotFound, "generation_not_found", "generation not found"}).write(ctx)
	}
	return ctx.JSON(http.StatusOK, map[string]any{"data": map[string]any{
		"id":                       record.RequestID,
		"model":                    record.Model,
		"provider_name":            providerName,
		"created_at":               record.Timestamp.UTC().Format(time.RFC3339Nano),
		"streamed":                 record.Stream,
		"generation_time":          record.DurationMs,
		"latency":                  record.TTFTMs,
		"tokens_prompt":            record.PromptTokens,
		"tokens_completion":        record.CompletionTokens,
		"native_tokens_prompt":     record.PromptTokens,
		"native_tokens_completion": record.CompletionTokens,
		"native_tokens_cached":     record.CachedTokens,
		"num_media_generation":     record.Images,
		"total_cost":               costUSD(record.CostMicroUSD),
		"usage":                    costUSD(record.CostMicroUSD),
		"cache_discount":           nil,
		"finish_reason":            nil,
		"is_byok":                  false,
		"gpu":                      record.GPU,
		"locality":                 record.Locality,
		"status_code":              record.StatusCode,
	}})
}

// handleKey reports the calling key's label and recent spend.
func (r *router) handleKey(ctx echo.Context) error {
	cc := ctx.(*auth.HttpAuthContext)
	rctx := ctx.Request().Context()
	workspaceID := cc.AuthInfo.Workspace.ExternalId
	today, _, _ := r.s.repo.GetWorkspaceUsage(rctx, workspaceID, 1)
	week, _, _ := r.s.repo.GetWorkspaceUsage(rctx, workspaceID, 7)
	month, perEndpoint, _ := r.s.repo.GetWorkspaceUsage(rctx, workspaceID, 30)

	label := cc.AuthInfo.Token.ExternalId
	if cc.AuthInfo.Workspace.Name != "" {
		label = cc.AuthInfo.Workspace.Name + "/" + label
	}
	endpoints := map[string]any{}
	for id, usage := range perEndpoint {
		endpoints[id] = map[string]any{
			"requests":          usage.Requests,
			"prompt_tokens":     usage.PromptTokens,
			"completion_tokens": usage.CompletionTokens,
			"images":            usage.Images,
			"cost":              costUSD(usage.CostMicroUSD),
		}
	}
	return ctx.JSON(http.StatusOK, map[string]any{"data": map[string]any{
		"label":               label,
		"limit":               nil,
		"usage":               costUSD(month.CostMicroUSD),
		"usage_daily":         costUSD(today.CostMicroUSD),
		"usage_weekly":        costUSD(week.CostMicroUSD),
		"usage_monthly":       costUSD(month.CostMicroUSD),
		"is_free_tier":        false,
		"is_provisioning_key": cc.AuthInfo.Token.TokenType == types.TokenTypeClusterAdmin,
		"rate_limit":          map[string]any{"requests": r.s.config.Routing.PerWorkspaceConcurrency, "interval": "concurrent"},
		"endpoints":           endpoints,
	}})
}
