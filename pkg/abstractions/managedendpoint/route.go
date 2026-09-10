package managedendpoint

import (
	"bufio"
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"mime"
	"mime/multipart"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/beam-cloud/beta9/pkg/abstractions/common/llmroute"
	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
	"github.com/rs/zerolog/log"
)

// The /v1 route: an OpenAI/OpenRouter-compatible surface over replicas. One
// pipeline for every kind; adapters carry the per-route differences.

const (
	maxBody             = 64 << 20
	queuePollInterval   = 100 * time.Millisecond
	replicaDialTimeout  = 5 * time.Second
	generationTTL       = time.Hour
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

	inflight  sync.Map // replica id -> *atomic.Int64
	admission sync.Map // endpoint id / endpoint|workspace -> *atomic.Int64 (per gateway; see admit)
}

func newRouter(s *Service) *router {
	return &router{s: s, prefix: s.config.RoutePrefix, states: map[string]*llmroute.State{}}
}

func (r *router) mount(group *echo.Group, authMiddleware echo.MiddlewareFunc) {
	g := group.Group(r.prefix, authMiddleware)
	g.GET("/models", auth.WithAuth(r.handleListModels))
	g.GET("/generation", auth.WithAuth(r.handleGeneration))
	for _, path := range []string{"/chat/completions", "/completions", "/embeddings", "/images/generations", "/images/edits", "/models/:author/:slug/invoke", "/models/:slug/invoke"} {
		g.POST(path, auth.WithAuth(r.handleRoute))
	}
}

func (r *router) state(endpointID string) *llmroute.State {
	r.stateMu.Lock()
	defer r.stateMu.Unlock()
	st, ok := r.states[endpointID]
	if !ok {
		st = llmroute.NewState(r.s.rdb, "managed_endpoint:route:"+endpointID)
		r.states[endpointID] = st
	}
	return st
}

func counter(m *sync.Map, key string) *atomic.Int64 {
	v, _ := m.LoadOrStore(key, &atomic.Int64{})
	return v.(*atomic.Int64)
}

// adapter is how one OpenAI-style route is proxied and metered.
type adapter struct {
	UpstreamPath string
	LLM          bool // affinity and token-aware selection; streams carry usage in the final chunk
	// Usage extracts billable usage from a complete (non-stream) JSON body.
	Usage func(body []byte) Usage
}

var adapters = map[types.EndpointRoute]adapter{
	types.EndpointRouteChatCompletions:  {"/v1/chat/completions", true, tokenUsage},
	types.EndpointRouteCompletions:      {"/v1/completions", true, tokenUsage},
	types.EndpointRouteEmbeddings:       {"/v1/embeddings", false, tokenUsage},
	types.EndpointRouteImageGenerations: {"/v1/images/generations", false, imageUsage},
	types.EndpointRouteImageEdits:       {"/v1/images/edits", false, imageUsage},
	types.EndpointRouteInvoke:           {"/invoke", false, func([]byte) Usage { return Usage{Requests: 1, Found: true} }},
}

// Usage is the billable usage the engine reported. It is never estimated: a
// response without usage is not billed.
type Usage struct {
	PromptTokens     int64
	CompletionTokens int64
	CachedTokens     int64
	Images           int64
	Requests         int64
	Found            bool // the response carried a usage object
}

// tokenUsage reads the OpenAI usage object from a response body.
func tokenUsage(body []byte) Usage {
	var env struct {
		Usage *struct {
			PromptTokens     int64 `json:"prompt_tokens"`
			CompletionTokens int64 `json:"completion_tokens"`
			Details          *struct {
				CachedTokens int64 `json:"cached_tokens"`
			} `json:"prompt_tokens_details"`
		} `json:"usage"`
	}
	if err := json.Unmarshal(body, &env); err != nil || env.Usage == nil {
		return Usage{}
	}
	u := Usage{PromptTokens: env.Usage.PromptTokens, CompletionTokens: env.Usage.CompletionTokens, Requests: 1, Found: true}
	if env.Usage.Details != nil {
		u.CachedTokens = env.Usage.Details.CachedTokens
	}
	return u
}

// imageUsage counts generated images plus any token usage the engine reports.
func imageUsage(body []byte) Usage {
	var payload struct {
		Data []json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return Usage{}
	}
	u := tokenUsage(body)
	u.Images, u.Requests = int64(len(payload.Data)), 1
	u.Found = u.Found || u.Images > 0
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

// forceIncludeUsage asks a streaming request for a final usage chunk and reports whether it is a stream.
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

func routeFromPath(prefix, path string) (types.EndpointRoute, string, bool) {
	rest := strings.TrimPrefix(strings.TrimPrefix(strings.TrimSuffix(path, "/"), prefix), "/")
	if id, ok := strings.CutPrefix(rest, "models/"); ok {
		id, ok = strings.CutSuffix(id, "/invoke")
		return types.EndpointRouteInvoke, id, ok && id != ""
	}
	route := types.EndpointRoute(rest)
	_, ok := adapters[route]
	return route, "", ok && route != types.EndpointRouteInvoke
}

// computeCostMicroUSD prices usage exactly and rounds half-up to micro-dollars.
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
		total.Add(total, rate.Mul(rate, big.NewRat(line.quantity, 1)))
	}
	total.Mul(total, big.NewRat(1_000_000, 1))
	total.Add(total, big.NewRat(1, 2)) // round half-up
	return new(big.Int).Quo(total.Num(), total.Denom()).Int64(), nil
}

// costUSD renders micro-dollars as the float OpenRouter puts in usage.cost.
func costUSD(microUSD int64) float64 { return float64(microUSD) / 1_000_000 }

func (r *router) cost(endpoint *types.ManagedEndpoint, usage Usage) int64 {
	if !billable(endpoint) || !usage.Found {
		return 0
	}
	cost, err := computeCostMicroUSD(endpoint.Spec.Pricing, usage)
	if err != nil {
		log.Warn().Err(err).Str("endpoint_id", endpoint.Spec.ID).Msg("managed endpoints: pricing error")
	}
	return cost
}

func billable(endpoint *types.ManagedEndpoint) bool {
	return !endpoint.Spec.Catalog.Free && !endpoint.Spec.Pricing.IsZero()
}

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

var errRegistry = &routeError{http.StatusServiceUnavailable, "registry_unavailable", "endpoint registry unavailable"}

// routeRequest is the state of one inference request through the pipeline.
type routeRequest struct {
	ctx        echo.Context
	auth       *auth.AuthInfo
	adapter    adapter
	route      types.EndpointRoute
	requestID  string
	models     []string // requested, in preference order
	model      string   // the endpoint selected (what the engine sees and what is billed)
	body       []byte
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
		pathModel = strings.TrimPrefix(ctx.Param("author")+"/"+ctx.Param("slug"), "/")
	}
	rq := &routeRequest{
		ctx: ctx, auth: cc.AuthInfo, adapter: adapters[route], route: route,
		requestID: "gen-" + strings.ReplaceAll(uuid.New().String(), "-", "")[:20],
		startedAt: time.Now(),
	}
	if cc.AuthInfo.Token.TokenType == types.TokenTypeClusterAdmin {
		rq.pinReplica = strings.TrimSpace(ctx.Request().Header.Get(headerReplicaPin))
	}
	if rerr := r.readRequest(rq, pathModel); rerr != nil {
		return rerr.write(ctx)
	}
	endpoint, rerr := r.resolveEndpoint(ctx.Request().Context(), rq)
	if rerr != nil {
		return rerr.write(ctx)
	}
	rq.model = endpoint.Spec.ID
	if rq.route != types.EndpointRouteInvoke {
		// /invoke payloads are the app's own schema and are left untouched.
		rq.setModel(endpoint.Spec.ID, endpoint.Spec.Engine == "vllm")
	}
	if rerr := r.admit(ctx.Request().Context(), rq, endpoint); rerr != nil {
		return rerr.write(ctx)
	}
	defer r.release(rq, endpoint)
	return r.serve(rq, endpoint)
}

func (r *router) readRequest(rq *routeRequest, pathModel string) *routeError {
	req := rq.ctx.Request()
	body, err := io.ReadAll(io.LimitReader(req.Body, maxBody+1))
	if err != nil {
		return &routeError{http.StatusBadRequest, "invalid_body", "failed to read request body"}
	}
	if len(body) > maxBody {
		return &routeError{http.StatusRequestEntityTooLarge, "body_too_large", "request body exceeds 64MB"}
	}
	rq.body = body
	if pathModel != "" {
		rq.models = []string{pathModel}
	}

	contentType, params, _ := mime.ParseMediaType(req.Header.Get("Content-Type"))
	if strings.HasPrefix(contentType, "multipart/") {
		if model := multipartModel(params["boundary"], body); model != "" {
			rq.models = append(rq.models, model)
		}
	} else {
		var payload map[string]any
		if len(bytes.TrimSpace(body)) > 0 && json.Unmarshal(body, &payload) != nil {
			return &routeError{http.StatusBadRequest, "invalid_json", "request body must be a JSON object"}
		}
		if model, _ := payload["model"].(string); model != "" {
			rq.models = append(rq.models, model)
		}
		if list, ok := payload["models"].([]any); ok {
			for _, m := range list {
				if s, ok := m.(string); ok && s != "" {
					rq.models = append(rq.models, s)
				}
			}
		}
		if payload != nil && rq.adapter.LLM {
			changed, err := normalizeReasoning(payload)
			if err != nil {
				return &routeError{http.StatusBadRequest, "invalid_reasoning", err.Error()}
			}
			rq.stream = forceIncludeUsage(payload)
			if changed || rq.stream {
				if body, err := json.Marshal(payload); err == nil {
					rq.body = body
				}
			}
		}
	}
	if len(rq.models) == 0 {
		return &routeError{http.StatusBadRequest, "missing_model", "the model field is required"}
	}
	return nil
}

// setModel makes the selected endpoint the model the engine sees.
func (rq *routeRequest) setModel(model string, continuousUsage bool) {
	var payload map[string]any
	if json.Unmarshal(rq.body, &payload) != nil || payload == nil {
		return
	}
	continuousUsage = continuousUsage && rq.stream
	if current, _ := payload["model"].(string); current == model && payload["models"] == nil && !continuousUsage {
		return
	}
	payload["model"] = model
	delete(payload, "models")
	if continuousUsage {
		// vLLM reports cumulative counters on every chunk. A preemption can
		// then retain observed token usage even when the final chunk is lost.
		// Failed streams remain unbilled under the existing error policy.
		options, _ := payload["stream_options"].(map[string]any)
		if options == nil {
			options = map[string]any{}
		}
		options["include_usage"] = true
		options["continuous_usage_stats"] = true
		payload["stream_options"] = options
	}
	if body, err := json.Marshal(payload); err == nil {
		rq.body = body
	}
}

func multipartModel(boundary string, body []byte) string {
	if boundary == "" {
		return ""
	}
	reader := multipart.NewReader(bytes.NewReader(body), boundary)
	for {
		part, err := reader.NextPart()
		if err != nil {
			return ""
		}
		if part.FormName() == "model" {
			value, _ := io.ReadAll(io.LimitReader(part, 1024))
			return strings.TrimSpace(string(value))
		}
	}
}

// resolveEndpoint picks the first requested model the caller may use that
// serves the route, preferring one with ready replicas.
func (r *router) resolveEndpoint(ctx context.Context, rq *routeRequest) (*types.ManagedEndpoint, *routeError) {
	var first *types.ManagedEndpoint
	var denied *routeError
	for _, model := range rq.models {
		endpoint, err := r.s.repo.GetEndpoint(ctx, model)
		if err != nil {
			return nil, errRegistry
		}
		switch {
		case endpoint == nil || !endpoint.Enabled():
			continue
		case !endpoint.Spec.ServesRoute(rq.route):
			denied = &routeError{http.StatusNotFound, "route_not_supported", fmt.Sprintf("model %s does not serve %s", model, rq.route)}
			continue
		case !r.allowed(ctx, endpoint, rq.auth):
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

// admissionKeys are the configured concurrency counters a request holds.
func (r *router) admissionKeys(rq *routeRequest, endpoint *types.ManagedEndpoint) (keys []string, caps []uint32) {
	if c := r.s.config.Routing.PerEndpointConcurrency; c > 0 {
		keys, caps = append(keys, endpoint.Spec.ID), append(caps, c)
	}
	if c := r.s.config.Routing.PerWorkspaceConcurrency; c > 0 {
		keys, caps = append(keys, endpoint.Spec.ID+"|"+rq.auth.Workspace.ExternalId), append(caps, c)
	}
	return keys, caps
}

// admit applies the credit gate and this gateway's concurrency caps; the
// cluster-wide bound is the replicas' MaxConcurrency, enforced in reserve.
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
	keys, caps := r.admissionKeys(rq, endpoint)
	for i, key := range keys {
		if counter(&r.admission, key).Add(1) > int64(caps[i]) {
			for _, held := range keys[:i+1] {
				counter(&r.admission, held).Add(-1)
			}
			if i == 0 && key == endpoint.Spec.ID {
				return &routeError{http.StatusTooManyRequests, "endpoint_saturated", "endpoint is at capacity, retry shortly"}
			}
			return &routeError{http.StatusTooManyRequests, "rate_limited", "too many concurrent requests for this workspace"}
		}
	}
	return nil
}

func (r *router) release(rq *routeRequest, endpoint *types.ManagedEndpoint) {
	keys, _ := r.admissionKeys(rq, endpoint)
	for _, key := range keys {
		counter(&r.admission, key).Add(-1)
	}
}

// servingReplicas lists replicas that may take this request; a pinned replica
// (X-Beam-Endpoint-Replica) bypasses the pool.
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
		if replica.Serving() {
			out = append(out, replica)
		}
	}
	return out
}

// pick waits (bounded) for a serving replica and selects one.
func (r *router) pick(ctx context.Context, rq *routeRequest, endpoint *types.ManagedEndpoint, exclude map[string]bool) (*types.EndpointReplica, *routeError) {
	deadline := rq.startedAt.Add(r.s.config.Routing.MaxQueueWait)
	for {
		candidates := r.servingReplicas(ctx, endpoint, rq.pinReplica, exclude)
		if len(candidates) == 0 && len(exclude) > 0 {
			return nil, &routeError{http.StatusBadGateway, "upstream_unavailable", "upstream replicas failed"}
		}
		if replica := r.choose(ctx, rq, endpoint, candidates); replica != nil {
			rq.queueWait = time.Since(rq.startedAt)
			return replica, nil
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

// choose picks a replica (llmroute for LLMs, least loaded otherwise) with one
// inflight slot reserved; the caller must releaseReplica it exactly once. Nil
// means every candidate is saturated.
func (r *router) choose(ctx context.Context, rq *routeRequest, endpoint *types.ManagedEndpoint, candidates []*types.EndpointReplica) *types.EndpointReplica {
	now := time.Now()
	slowStart := time.Duration(r.s.config.Routing.SlowStartSeconds) * time.Second
	state := r.state(endpoint.Spec.ID)

	var eligible []llmroute.Candidate
	for _, replica := range candidates {
		local := counter(&r.inflight, replica.ID).Load()
		pressure, _ := state.Pressure(ctx, replica.ID)
		if replica.Capacity.MaxConcurrency > 0 && max(local, pressure.ActiveStreams) >= replica.Capacity.MaxConcurrency {
			continue
		}
		penalty := int64(0)
		if age := now.Sub(replica.ReadyAt); !replica.ReadyAt.IsZero() && age < slowStart {
			penalty = int64((1 - float64(age)/float64(slowStart)) * 8)
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
	// Losers of the reservation race move on to the next candidate.
	for len(eligible) > 0 {
		selection, ok := r.selector.Select(eligible, affinity, rq.info)
		if !ok {
			return nil
		}
		replica := selection.Candidate.Payload.(*types.EndpointReplica)
		if r.reserve(ctx, rq, state, replica) {
			return replica
		}
		eligible = slices.DeleteFunc(eligible, func(c llmroute.Candidate) bool { return c.ID == replica.ID })
	}
	return nil
}

// reserve takes one inflight slot: the local counter and the shared Redis
// reservation that bounds MaxConcurrency across gateways. If Redis is
// unreachable the local bound alone applies.
func (r *router) reserve(ctx context.Context, rq *routeRequest, state *llmroute.State, replica *types.EndpointReplica) bool {
	inflight := counter(&r.inflight, replica.ID)
	if n := inflight.Add(1); replica.Capacity.MaxConcurrency > 0 && n > replica.Capacity.MaxConcurrency {
		inflight.Add(-1)
		return false
	}
	ok, err := state.Reserve(ctx, replica.ID, rq.tokenPressure(), replica.Capacity.MaxConcurrency)
	if err != nil {
		log.Debug().Err(err).Str("replica_id", replica.ID).Msg("managed endpoints: shared reservation unavailable; local bound only")
		return true
	}
	if !ok {
		inflight.Add(-1)
	}
	return ok
}

// releaseReplica returns the slot taken by reserve.
func (r *router) releaseReplica(rq *routeRequest, state *llmroute.State, replica *types.EndpointReplica) {
	counter(&r.inflight, replica.ID).Add(-1)
	_ = state.AddPressure(context.Background(), replica.ID, -1, -rq.tokenPressure())
}

func (rq *routeRequest) tokenPressure() int64 {
	if rq.info == nil {
		return 0
	}
	return rq.info.TokenPressure
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

var hopHeaders = map[string]bool{
	"Connection": true, "Keep-Alive": true, "Proxy-Authenticate": true, "Proxy-Authorization": true,
	"Te": true, "Trailer": true, "Transfer-Encoding": true, "Upgrade": true, "Authorization": true,
	"Content-Length": true, "Host": true,
}

// serve runs select -> proxy -> meter, retrying once on another replica when
// the first attempt fails before any byte reached the client.
func (r *router) serve(rq *routeRequest, endpoint *types.ManagedEndpoint) error {
	ctx := rq.ctx.Request().Context()
	if rq.adapter.LLM {
		req := rq.ctx.Request()
		req.Body = io.NopCloser(bytes.NewReader(rq.body))
		if info, err := llmroute.Inspect(req, r.prefix+"/"+string(rq.route), llmroute.InspectOptions{DefaultModel: endpoint.Spec.ID}); err == nil {
			info.RequestID = rq.requestID
			rq.info, rq.stream = info, info.Stream
		}
	}

	exclude := map[string]bool{}
	for attempt := 0; attempt < 2; attempt++ {
		replica, rerr := r.pick(ctx, rq, endpoint, exclude)
		if rerr != nil {
			r.record(rq, endpoint, nil, rerr.Status, Usage{}, 0, rerr.Message)
			return rerr.write(rq.ctx)
		}
		retry, err := r.proxy(ctx, rq, endpoint, replica)
		r.releaseReplica(rq, r.state(endpoint.Spec.ID), replica) // the slot reserved by pick/choose; the single owner
		if err == nil {
			return nil
		}
		logger := log.Warn().Err(err).Str("endpoint_id", endpoint.Spec.ID).Str("replica_id", replica.ID)
		if !retry {
			logger.Msg("managed endpoints: proxy failed after response started")
			return nil
		}
		exclude[replica.ID], rq.retried = true, true
		logger.Msg("managed endpoints: upstream failed before response; retrying")
	}
	rerr := &routeError{http.StatusBadGateway, "upstream_unavailable", "upstream replicas failed"}
	r.record(rq, endpoint, nil, rerr.Status, Usage{}, 0, rerr.Message)
	return rerr.write(rq.ctx)
}

// proxy relays one attempt; the bool reports whether a retry is safe (nothing was written).
func (r *router) proxy(ctx context.Context, rq *routeRequest, endpoint *types.ManagedEndpoint, replica *types.EndpointReplica) (bool, error) {
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

	url := "http://replica" + rq.adapter.UpstreamPath
	if q := upstreamQuery(rq.ctx.Request().URL.Query()); q != "" {
		url += "?" + q
	}
	req, err := http.NewRequestWithContext(upstreamCtx, http.MethodPost, url, bytes.NewReader(rq.body))
	if err != nil {
		return true, err
	}
	for name, values := range rq.ctx.Request().Header {
		if !hopHeaders[http.CanonicalHeaderKey(name)] && !strings.HasPrefix(name, "X-Beam-") {
			req.Header[name] = values
		}
	}
	req.Header.Set(headerRequestID, rq.requestID)
	req.Header.Set(headerEndpointID, endpoint.Spec.ID)
	req.ContentLength = int64(len(rq.body))

	sentAt := time.Now()
	resp, err := r.s.transport(replica.Address).RoundTrip(req)
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
		r.state(endpoint.Spec.ID).RecordAffinity(ctx, rq.info, replica.ID)
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
		w.WriteHeader(resp.StatusCode)
		usage, ttft, err := relayStream(w, resp.Body, rq.requestID, sentAt)
		status := resp.StatusCode
		if err != nil && status < 300 {
			status = http.StatusBadGateway // the stream broke: not a success, not billed
		}
		if err == nil && status < 300 && billable(endpoint) && !usage.Found {
			// The stream is already with the client; record a 502 so it is not billed.
			r.recordMissingUsage(rq, endpoint, replica, usage, ttft)
			return false, nil
		}
		r.record(rq, endpoint, replica, status, usage, ttft, errString(err))
		return false, err
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxBody+1))
	switch {
	case err != nil:
		rerr := &routeError{http.StatusBadGateway, "upstream_failed", "upstream response ended early"}
		r.record(rq, endpoint, replica, rerr.Status, Usage{}, 0, err.Error())
		return false, rerr.write(rq.ctx)
	case len(body) > maxBody:
		rerr := &routeError{http.StatusBadGateway, "upstream_too_large", "upstream response exceeds 64MB"}
		r.record(rq, endpoint, replica, rerr.Status, Usage{}, 0, rerr.Message)
		return false, rerr.write(rq.ctx)
	}
	usage := Usage{}
	if resp.StatusCode < 300 {
		usage = rq.adapter.Usage(body)
		if billable(endpoint) && !usage.Found {
			r.recordMissingUsage(rq, endpoint, replica, usage, 0)
			return false, errMissingUsage.write(rq.ctx)
		}
		if strings.Contains(contentType, "json") {
			body = decorateJSON(body, rq.requestID, usage, r.cost(endpoint, usage))
		}
	}
	w.Header().Set("Content-Length", fmt.Sprint(len(body)))
	w.WriteHeader(resp.StatusCode)
	_, werr := w.Write(body)
	r.record(rq, endpoint, replica, resp.StatusCode, usage, 0, errString(werr))
	return false, nil
}

// upstreamQuery drops the gateway's own query parameters (auth_token).
func upstreamQuery(q url.Values) string {
	q.Del("auth_token")
	return q.Encode()
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

var errMissingUsage = &routeError{http.StatusBadGateway, "missing_usage", "upstream response carried no usage; request not billed"}

// recordMissingUsage files a billable response without usage as a 502: not
// billed, counted as an error, and raised as a route.missing_usage event.
func (r *router) recordMissingUsage(rq *routeRequest, endpoint *types.ManagedEndpoint, replica *types.EndpointReplica, usage Usage, ttft time.Duration) {
	r.record(rq, endpoint, replica, errMissingUsage.Status, usage, ttft, errMissingUsage.Message)
	r.s.emit(types.EventEndpointHarness, types.EventEndpointSchema{EndpointID: endpoint.Spec.ID, Action: "route.missing_usage", ReplicaID: replica.ID, GPU: replica.GPU, Version: replica.Version})
	log.Warn().Str("endpoint_id", endpoint.Spec.ID).Str("replica_id", replica.ID).Str("request_id", rq.requestID).Bool("stream", rq.stream).Msg("managed endpoints: upstream response carried no usage; request not billed")
}

// relayStream forwards SSE events as they arrive, stamping the generation id,
// pulling usage from the final chunk and measuring TTFT at the first output.
func relayStream(w *echo.Response, body io.Reader, requestID string, sentAt time.Time) (Usage, time.Duration, error) {
	flusher, _ := w.Writer.(http.Flusher)
	reader := bufio.NewReaderSize(body, 64<<10)
	var usage Usage
	var ttft time.Duration
	for {
		line, err := reader.ReadBytes('\n')
		if len(line) > 0 {
			if bytes.HasPrefix(line, []byte("data:")) {
				if ttft == 0 && generatesOutput(line) {
					ttft = time.Since(sentAt)
				}
				line = stampSSE(line, requestID)
			}
			if u, ok := sseUsage(line); ok {
				usage = u
			}
			if _, werr := w.Write(line); werr != nil {
				return usage, ttft, werr
			}
			if flusher != nil && (len(bytes.TrimSpace(line)) == 0 || bytes.HasPrefix(line, []byte("data:"))) {
				flusher.Flush()
			}
		}
		if errors.Is(err, io.EOF) {
			return usage, ttft, nil
		}
		if err != nil {
			return usage, ttft, err
		}
	}
}

// generatesOutput reports whether an SSE line carries content, a tool call or
// completion text, so TTFT is the first generated token and not the engine's
// opening frame.
func generatesOutput(line []byte) bool {
	payload := bytes.TrimSpace(line[len("data:"):])
	if len(payload) == 0 || payload[0] != '{' {
		return false
	}
	var chunk struct {
		Choices *[]struct {
			Text  *string `json:"text"`
			Delta *struct {
				Content   *string         `json:"content"`
				ToolCalls json.RawMessage `json:"tool_calls"`
			} `json:"delta"`
		} `json:"choices"`
		Usage json.RawMessage `json:"usage"`
	}
	if json.Unmarshal(payload, &chunk) != nil {
		return true // not JSON we understand: any frame is output
	}
	if chunk.Choices == nil {
		return len(chunk.Usage) == 0 // not an OpenAI chunk unless it is usage-only
	}
	for _, c := range *chunk.Choices {
		if c.Text != nil && *c.Text != "" {
			return true
		}
		if c.Delta != nil && ((c.Delta.Content != nil && *c.Delta.Content != "") || (len(c.Delta.ToolCalls) > 0 && string(c.Delta.ToolCalls) != "null")) {
			return true
		}
	}
	return false
}

// stampSSE rewrites the id of a JSON SSE chunk to the gateway generation id.
func stampSSE(line []byte, requestID string) []byte {
	payload := bytes.TrimSpace(line[len("data:"):])
	if len(payload) == 0 || payload[0] != '{' || !bytes.Contains(payload, []byte(`"id"`)) {
		return line
	}
	var chunk map[string]any
	if json.Unmarshal(payload, &chunk) != nil || chunk["id"] == requestID {
		return line
	}
	chunk["id"] = requestID
	out, err := json.Marshal(chunk)
	if err != nil {
		return line
	}
	return append(append([]byte("data: "), out...), '\n')
}

// decorateJSON adds OpenRouter-style fields (generation id, usage, cost) to a JSON body.
func decorateJSON(body []byte, requestID string, usage Usage, costMicro int64) []byte {
	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil || payload == nil {
		return body
	}
	payload["id"] = requestID
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

func (r *router) record(rq *routeRequest, endpoint *types.ManagedEndpoint, replica *types.EndpointReplica, status int, usage Usage, ttft time.Duration, errMsg string) {
	now := time.Now()
	cost := int64(0)
	if status < 300 {
		cost = r.cost(endpoint, usage)
	}
	sample := types.RouteSample{
		EndpointID: endpoint.Spec.ID, StatusCode: status,
		PromptTokens: usage.PromptTokens, CompletionTokens: usage.CompletionTokens, Images: usage.Images, CostMicroUSD: cost,
		Duration: now.Sub(rq.startedAt), TTFT: ttft, QueueWait: rq.queueWait, At: now,
	}
	event := types.EventEndpointRouteSchema{
		EndpointID: endpoint.Spec.ID, WorkspaceID: rq.auth.Workspace.ExternalId, TokenID: rq.auth.Token.ExternalId,
		RequestID: rq.requestID, Route: string(rq.route), Model: endpoint.Spec.ID, Version: endpoint.Version,
		StatusCode: status, Stream: rq.stream, Retried: rq.retried,
		PromptTokens: usage.PromptTokens, CompletionTokens: usage.CompletionTokens, CachedTokens: usage.CachedTokens,
		Images: usage.Images, CostMicroUSD: cost,
		DurationMs: sample.Duration.Milliseconds(), TTFTMs: ttft.Milliseconds(), QueueWaitMs: rq.queueWait.Milliseconds(),
		Error: errMsg, Timestamp: now.UTC(),
	}
	if replica != nil {
		sample.GPU, sample.ReplicaID, sample.ConfigRevision = replica.GPU, replica.ID, replica.Config.AckedRevision
		event.ConfigRevision = replica.Config.AckedRevision
		event.Version, event.ReplicaID, event.ContainerID = replica.Version, replica.ID, replica.ContainerID
		event.GPU, event.Locality, event.MachineID = replica.GPU, replica.Locality, replica.MachineID
		if replica.ProviderWorkspaceID != "" && cost > 0 {
			event.ProviderWorkspaceID = replica.ProviderWorkspaceID
			event.ProviderShareMicroUSD = int64(float64(cost) * r.s.config.ProviderRevenueShare)
		}
	}
	if rq.info != nil {
		event.RouteReason, event.KVHitSource = rq.info.RouteReason, "none"
		if rq.info.PrefixCacheMatches > 0 || strings.Contains(rq.info.RouteReason, "affinity") {
			event.KVHitSource = "engine"
		}
	}
	if err := r.s.repo.RecordRouteSample(context.Background(), sample); err != nil {
		log.Debug().Err(err).Msg("managed endpoints: record route sample")
	}
	r.persist(event)
}

// persist is the accounting for one request: route event, generation and
// AddUsage, each idempotent on the request id.
func (r *router) persist(event types.EventEndpointRouteSchema) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	warn := func(leg string, err error) {
		if err != nil {
			log.Warn().Err(err).Str("request_id", event.RequestID).Str("leg", leg).Msg("managed endpoints: accounting leg failed")
		}
	}
	if r.s.events != nil {
		r.s.events.PushEndpointRouteEvent(event)
	}
	warn("generation", r.s.repo.SaveGeneration(ctx, &event, generationTTL))
	if event.StatusCode >= 300 {
		return
	}
	usage := types.Usage{Requests: 1, PromptTokens: event.PromptTokens, CompletionTokens: event.CompletionTokens, Images: event.Images, MicroUSD: event.CostMicroUSD}
	warn("spend", r.s.repo.AddUsage(ctx, types.UsageSpend, event.WorkspaceID, event.Model, event.RequestID, event.Timestamp, usage))
	if event.ProviderWorkspaceID != "" {
		usage.MicroUSD = event.ProviderShareMicroUSD
		warn("earned", r.s.repo.AddUsage(ctx, types.UsageEarned, event.ProviderWorkspaceID, event.Model, event.RequestID, event.Timestamp, usage))
	}
	if event.CostMicroUSD > 0 && r.s.scheduler != nil {
		if gate := r.s.scheduler.CreditGate(); gate != nil {
			gate.Invalidate(ctx, event.WorkspaceID)
		}
	}
}

func modalities(spec *types.ManagedEndpointSpec) (input []string, output []string) {
	input, output = []string{"text"}, []string{"text"}
	switch spec.Kind {
	case types.EndpointKindLLM:
		for _, m := range spec.Catalog.Modalities {
			if m == "image" || m == "vision" || m == "audio" {
				input = append(input, strings.Replace(m, "vision", "image", 1))
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
func pricingEntry(p types.Pricing) map[string]any {
	entry := map[string]any{
		"prompt":     cmp.Or(p.PromptTokens, "0"),
		"completion": cmp.Or(p.CompletionTokens, "0"),
		"request":    cmp.Or(p.Request, "0"),
		"image":      cmp.Or(p.Image, "0"),
	}
	if p.CachedPromptTokens != "" {
		entry["input_cache_read"] = p.CachedPromptTokens
	}
	return entry
}

func orEmpty(list []string) []string {
	if list == nil {
		return []string{}
	}
	return list
}

func orNil[T comparable](v T) any {
	var zero T
	if v == zero {
		return nil
	}
	return v
}

func (r *router) handleListModels(ctx echo.Context) error {
	cc := ctx.(*auth.HttpAuthContext)
	rctx := ctx.Request().Context()
	all, err := r.s.repo.ListEndpoints(rctx)
	if err != nil {
		return errRegistry.write(ctx)
	}
	endpoints := slices.DeleteFunc(all, func(e *types.ManagedEndpoint) bool { return !e.Enabled() || !r.allowed(rctx, e, cc.AuthInfo) })
	replicas, _ := r.s.repo.ListAllReplicas(rctx)
	ready := map[string]bool{}
	regions := map[string][]string{} // endpoint id -> localities with a serving replica
	for _, replica := range replicas {
		if !replica.Serving() {
			continue
		}
		ready[replica.EndpointID] = true
		if replica.Locality != "" && !slices.Contains(regions[replica.EndpointID], replica.Locality) {
			regions[replica.EndpointID] = append(regions[replica.EndpointID], replica.Locality)
		}
	}
	data := make([]map[string]any, 0, len(endpoints))
	for _, endpoint := range endpoints {
		spec := &endpoint.Spec
		input, output := modalities(spec)
		routes := map[string]any{}
		for _, route := range spec.Routes {
			routes[strings.ReplaceAll(string(route), "/", "_")] = r.prefix + "/" + string(route)
		}
		slices.Sort(regions[spec.ID])
		data = append(data, map[string]any{
			"id":             spec.ID,
			"canonical_slug": spec.ID,
			"name":           cmp.Or(spec.Catalog.Name, spec.ID),
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
			"pricing": pricingEntry(spec.Pricing),
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
			// Beam extensions: live state for the dashboard and OpenRouter-style route paths.
			"is_ready":    ready[spec.ID],
			"datacenters": orEmpty(regions[spec.ID]),
			"endpoints":   routes,
		})
	}
	return ctx.JSON(http.StatusOK, map[string]any{"object": "list", "data": data})
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
