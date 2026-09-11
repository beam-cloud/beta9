package managedendpoint

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
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
	g := group.Group(r.prefix, openAIErrors, authMiddleware)
	g.GET("/models", auth.WithAuth(r.handleListModels))
	g.GET("/models/openrouter", auth.WithAuth(r.handleListOpenRouterModels))
	g.GET("/generation", auth.WithAuth(r.handleGeneration))
	for _, path := range []string{"/chat/completions", "/completions", "/embeddings", "/images/generations", "/images/edits", "/models/:author/:slug/invoke", "/models/:slug/invoke"} {
		g.POST(path, auth.WithAuth(r.handleRoute))
	}
}

// Keep shared authentication/method errors in the same envelope as inference
// errors without changing the rest of the platform's API middleware.
func openAIErrors(next echo.HandlerFunc) echo.HandlerFunc {
	return func(ctx echo.Context) error {
		err := next(ctx)
		if err == nil || ctx.Response().Committed {
			return err
		}
		var httpErr *echo.HTTPError
		if !errors.As(err, &httpErr) {
			return err
		}
		code := "invalid_request"
		if httpErr.Code == http.StatusUnauthorized || httpErr.Code == http.StatusForbidden {
			code = "invalid_api_key"
		} else if httpErr.Code >= 500 {
			code = "server_error"
		}
		return (&routeError{httpErr.Code, code, http.StatusText(httpErr.Code)}).write(ctx)
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
	if u.PromptTokens < 0 || u.CompletionTokens < 0 || u.CachedTokens < 0 || u.CachedTokens > u.PromptTokens || u.PromptTokens > types.MaxUsageCounter || u.CompletionTokens > types.MaxUsageCounter {
		return Usage{}
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

// costUSD renders micro-dollars as the float OpenRouter puts in usage.cost.
func costUSD(microUSD int64) float64 { return float64(microUSD) / 1_000_000 }

func (r *router) cost(endpoint *types.ManagedEndpoint, usage Usage) int64 {
	if !billable(endpoint) || !usage.Found {
		return 0
	}
	priced, err := priceUsage(endpoint.Spec.Pricing, usage)
	if err != nil {
		log.Warn().Err(err).Str("endpoint_id", endpoint.Spec.ID).Msg("managed endpoints: pricing error")
	}
	return priced.MicroUSD
}

func billable(endpoint *types.ManagedEndpoint) bool {
	return !endpoint.Spec.Pricing.IsZero()
}

type routeError struct {
	Status  int
	Code    string
	Message string
}

func (e *routeError) Error() string { return e.Message }

func (e *routeError) write(ctx echo.Context) error {
	ctx.Response().Header().Set("Content-Type", "application/json")
	ctx.Response().Header().Del("Content-Encoding")
	kind := "invalid_request_error"
	switch {
	case e.Status == http.StatusUnauthorized || e.Status == http.StatusForbidden:
		kind = "authentication_error"
	case e.Status == http.StatusPaymentRequired:
		kind = "insufficient_quota"
	case e.Status == http.StatusTooManyRequests:
		kind = "rate_limit_error"
		if ctx.Response().Header().Get("Retry-After") == "" {
			ctx.Response().Header().Set("Retry-After", "1")
		}
	case e.Status == http.StatusNotFound:
		kind = "not_found_error"
	case e.Status >= 500:
		kind = "server_error"
	}
	return ctx.JSON(e.Status, map[string]any{"error": map[string]any{"message": e.Message, "type": kind, "code": e.Code, "param": nil}})
}

var errRegistry = &routeError{http.StatusServiceUnavailable, "registry_unavailable", "endpoint registry unavailable"}

func capacityError(message string) *routeError {
	return &routeError{http.StatusTooManyRequests, "rate_limit_exceeded", message}
}

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
	ctx.Response().Header().Set(headerRequestID, rq.requestID)
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
		if len(bytes.TrimSpace(body)) > 0 {
			payload, err = decodeRequestJSON(body)
			if err != nil {
				return &routeError{http.StatusBadRequest, "invalid_json", "request body must be a JSON object"}
			}
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

// Preserve tool schemas and provider parameters exactly when adding routing
// fields. float64 would silently round JSON integers larger than 2^53.
func decodeRequestJSON(body []byte) (map[string]any, error) {
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	var payload map[string]any
	if err := decoder.Decode(&payload); err != nil {
		return nil, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return nil, errors.New("request body must contain one JSON object")
	}
	return payload, nil
}

// setModel makes the selected endpoint the model the engine sees.
func (rq *routeRequest) setModel(model string, continuousUsage bool) {
	payload, err := decodeRequestJSON(rq.body)
	if err != nil || payload == nil {
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
		replicas, err := r.servingReplicas(ctx, endpoint, rq.pinReplica, nil)
		if err != nil {
			return nil, errRegistry
		}
		if len(replicas) > 0 {
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
	if authInfo.Token.TokenType == types.TokenTypeClusterAdmin || endpoint.Spec.Public {
		return true
	}
	if admin, err := r.s.AdminWorkspace(ctx); err == nil && admin.Id == authInfo.Workspace.Id {
		return true
	}
	allowed := endpoint.Spec.AllowedWorkspaces
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
				return capacityError("endpoint is at capacity, retry shortly")
			}
			return capacityError("too many concurrent requests for this workspace")
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
func (r *router) servingReplicas(ctx context.Context, endpoint *types.ManagedEndpoint, pin string, exclude map[string]bool) ([]*types.EndpointReplica, error) {
	replicas, err := r.s.repo.ListReplicas(ctx, endpoint.Spec.ID)
	if err != nil {
		return nil, err
	}
	var out []*types.EndpointReplica
	for _, replica := range replicas {
		if exclude[replica.ID] || replica.Address == "" {
			continue
		}
		if pin != "" {
			if replica.ID == pin && replica.Status == types.ReplicaStatusReady {
				return []*types.EndpointReplica{replica}, nil
			}
			continue
		}
		if replica.Serving() {
			out = append(out, replica)
		}
	}
	return out, nil
}

// pick reserves available capacity immediately. Providers must reject overload
// before opening a stream.
func (r *router) pick(ctx context.Context, rq *routeRequest, endpoint *types.ManagedEndpoint, exclude map[string]bool) (*types.EndpointReplica, *routeError) {
	var drainDone <-chan struct{}
	if r.s.drainCtx != nil {
		drainDone = r.s.drainCtx.Done()
	}
	stopped := func() *routeError {
		select {
		case <-ctx.Done():
			return &routeError{499, "client_closed", "client closed request"}
		case <-drainDone:
			return &routeError{http.StatusServiceUnavailable, "gateway_draining", "gateway is restarting, retry shortly"}
		default:
			return nil
		}
	}
	if rerr := stopped(); rerr != nil {
		return nil, rerr
	}
	candidates, err := r.servingReplicas(ctx, endpoint, rq.pinReplica, exclude)
	if rerr := stopped(); rerr != nil {
		return nil, rerr
	}
	if err != nil {
		return nil, errRegistry
	}
	if len(candidates) == 0 && len(exclude) > 0 {
		return nil, &routeError{http.StatusBadGateway, "upstream_unavailable", "upstream replicas failed"}
	}
	replica, chooseErr := r.choose(ctx, rq, endpoint, candidates)
	if chooseErr != nil {
		if rerr := stopped(); rerr != nil {
			return nil, rerr
		}
		return nil, errRegistry
	}
	if replica != nil {
		rq.queueWait = time.Since(rq.startedAt)
		return replica, nil
	}
	if rerr := stopped(); rerr != nil {
		return nil, rerr
	}
	return nil, capacityError(fmt.Sprintf("model %s is temporarily at capacity; retry shortly", endpoint.Spec.ID))
}

// choose picks a replica (llmroute for LLMs, least loaded otherwise) with one
// inflight slot reserved; the caller must releaseReplica it exactly once. Nil
// means every candidate is saturated.
func (r *router) choose(ctx context.Context, rq *routeRequest, endpoint *types.ManagedEndpoint, candidates []*types.EndpointReplica) (*types.EndpointReplica, error) {
	now := time.Now()
	slowStart := time.Duration(r.s.config.Routing.SlowStartSeconds) * time.Second
	state := r.state(endpoint.Spec.ID)

	var eligible []llmroute.Candidate
	for _, replica := range candidates {
		local := counter(&r.inflight, replica.ID).Load()
		pressure, err := state.Pressure(ctx, replica.ID)
		if err != nil {
			return nil, err
		}
		if replica.Capacity.MaxConcurrency > 0 && local >= replica.Capacity.MaxConcurrency {
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
		return nil, nil
	}
	var affinity llmroute.Affinity
	if rq.adapter.LLM && rq.info != nil {
		affinity = state.Affinity(ctx, rq.info)
	}
	// Losers of the reservation race move on to the next candidate.
	for len(eligible) > 0 {
		selection, ok := r.selector.Select(eligible, affinity, rq.info)
		if !ok {
			return nil, nil
		}
		replica := selection.Candidate.Payload.(*types.EndpointReplica)
		reserved, err := r.reserve(ctx, rq, state, replica)
		if err != nil {
			return nil, err
		}
		if reserved {
			return replica, nil
		}
		eligible = slices.DeleteFunc(eligible, func(c llmroute.Candidate) bool { return c.ID == replica.ID })
	}
	return nil, nil
}

// reserve takes one inflight slot: the local counter and the shared Redis
// reservation that bounds MaxConcurrency across gateways. Uncertain shared
// capacity fails closed rather than overloading the engine.
func (r *router) reserve(ctx context.Context, rq *routeRequest, state *llmroute.State, replica *types.EndpointReplica) (bool, error) {
	inflight := counter(&r.inflight, replica.ID)
	if n := inflight.Add(1); replica.Capacity.MaxConcurrency > 0 && n > replica.Capacity.MaxConcurrency {
		inflight.Add(-1)
		return false, nil
	}
	ok, err := r.s.slot(ctx, replica.ID, "acquire", rq.requestID, replica.Capacity.MaxConcurrency)
	if err != nil {
		inflight.Add(-1)
		// The server may have acquired the slot before its reply was lost.
		// Releasing this request ID is safe even when acquire never succeeded.
		_, _ = r.s.slot(context.Background(), replica.ID, "release", rq.requestID, 0)
		return false, err
	}
	if !ok {
		inflight.Add(-1)
	} else {
		hintCtx, cancel := context.WithTimeout(ctx, slotOpTimeout)
		_ = state.AddPressure(hintCtx, replica.ID, 1, rq.tokenPressure())
		cancel()
	}
	return ok, nil
}

// releaseReplica returns the slot taken by reserve.
func (r *router) releaseReplica(rq *routeRequest, state *llmroute.State, replica *types.EndpointReplica) {
	counter(&r.inflight, replica.ID).Add(-1)
	released, _ := r.s.slot(context.Background(), replica.ID, "release", rq.requestID, 0)
	if released {
		hintCtx, cancel := context.WithTimeout(context.Background(), slotOpTimeout)
		defer cancel()
		_ = state.AddPressure(hintCtx, replica.ID, -1, -rq.tokenPressure())
	}
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
		var leaseLost bool
		retry, err := func() (bool, error) {
			attemptCtx, stopRenewal := r.renewSlot(ctx, replica.ID, rq.requestID)
			defer func() {
				stopRenewal()
				r.releaseReplica(rq, r.state(endpoint.Spec.ID), replica)
			}()
			retry, err := r.proxy(attemptCtx, rq, endpoint, replica)
			leaseLost = errors.Is(context.Cause(attemptCtx), errSlotLeaseLost)
			return retry, err
		}()
		if leaseLost && err != nil {
			if !rq.ctx.Response().Committed {
				r.record(rq, endpoint, replica, http.StatusServiceUnavailable, Usage{}, 0, errSlotLeaseLost.Error())
				return errRegistry.write(rq.ctx)
			}
			if rq.stream && ctx.Err() == nil {
				writeStreamError(rq.ctx.Response(), rq.requestID, endpoint.Spec.ID, &streamFailure{http.StatusServiceUnavailable, "Endpoint capacity lease lost", "registry_unavailable"})
			}
			return nil
		}
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
	// Already-admitted inference survives readiness draining. The gateway service
	// context ends only after the HTTP graceful-shutdown window has elapsed.
	upstreamCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		select {
		case <-r.s.ctx.Done():
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
	// Metering and response rewriting require plain JSON/SSE. The transport
	// disables automatic decompression, so do not forward browser encodings.
	req.Header.Set("Accept-Encoding", "identity")
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
	if resp.StatusCode == http.StatusTooManyRequests {
		// Engine overload must stay a JSON 429 even for streaming requests.
		// Never relay a backend-specific error body or open an SSE response.
		rerr := capacityError("model is temporarily at capacity; retry shortly")
		r.record(rq, endpoint, replica, rerr.Status, Usage{}, 0, rerr.Message)
		return false, rerr.write(rq.ctx)
	}

	contentType, _, _ := mime.ParseMediaType(resp.Header.Get("Content-Type"))
	if contentType == "text/event-stream" {
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("X-Accel-Buffering", "no")
		w.WriteHeader(resp.StatusCode)
		recorded := false
		usage, ttft, err := relayStream(w, resp.Body, rq.requestID, sentAt, func(usage Usage, ttft time.Duration) error {
			recorded = true
			if resp.StatusCode < 300 && billable(endpoint) && !usage.Found {
				r.recordMissingUsage(rq, endpoint, replica, usage, ttft)
				return errors.New("upstream response carried no usage")
			}
			return r.record(rq, endpoint, replica, resp.StatusCode, usage, ttft, "")
		})
		status := resp.StatusCode
		if err != nil && status < 300 {
			failure := streamFailureFor(err)
			status = failure.status // the stream broke: not a success, not billed
			if errors.Is(context.Cause(ctx), errSlotLeaseLost) {
				status = http.StatusServiceUnavailable
			}
			if ctx.Err() == nil {
				// HTTP headers are already committed. An SSE error lets SDKs
				// distinguish preemption from a completed generation.
				writeStreamError(w, rq.requestID, endpoint.Spec.ID, failure)
			}
		}
		if err == nil && status < 300 && billable(endpoint) && !usage.Found {
			// The stream is already with the client; record a 502 so it is not billed.
			r.recordMissingUsage(rq, endpoint, replica, usage, ttft)
			return false, nil
		}
		if !recorded {
			r.record(rq, endpoint, replica, status, usage, ttft, errString(err))
		}
		return false, err
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxBody+1))
	switch {
	case err != nil:
		rerr := &routeError{http.StatusBadGateway, "upstream_failed", "upstream response ended early"}
		if errors.Is(context.Cause(ctx), errSlotLeaseLost) {
			rerr = errRegistry
		}
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
	if err := r.record(rq, endpoint, replica, resp.StatusCode, usage, 0, ""); err != nil {
		return false, errAccountingUnavailable.write(rq.ctx)
	}
	w.Header().Set("Content-Length", fmt.Sprint(len(body)))
	w.WriteHeader(resp.StatusCode)
	_, _ = w.Write(body)
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

var errAccountingUnavailable = &routeError{http.StatusServiceUnavailable, "accounting_unavailable", "Unable to record request usage"}

var errMissingUsage = &routeError{http.StatusBadGateway, "missing_usage", "upstream response carried no usage; request not billed"}

// recordMissingUsage files a billable response without usage as a 502: not
// billed, counted as an error, and raised as a route.missing_usage event.
func (r *router) recordMissingUsage(rq *routeRequest, endpoint *types.ManagedEndpoint, replica *types.EndpointReplica, usage Usage, ttft time.Duration) {
	r.record(rq, endpoint, replica, errMissingUsage.Status, usage, ttft, errMissingUsage.Message)
	r.s.emit(types.EventEndpointHarness, types.EventEndpointSchema{EndpointID: endpoint.Spec.ID, Action: "route.missing_usage", ReplicaID: replica.ID, GPU: replica.GPU, Version: replica.Version})
	log.Warn().Str("endpoint_id", endpoint.Spec.ID).Str("replica_id", replica.ID).Str("request_id", rq.requestID).Bool("stream", rq.stream).Msg("managed endpoints: upstream response carried no usage; request not billed")
}

func (r *router) record(rq *routeRequest, endpoint *types.ManagedEndpoint, replica *types.EndpointReplica, status int, usage Usage, ttft time.Duration, errMsg string) error {
	now := time.Now()
	var priced types.Usage
	if status < 300 && usage.Found {
		var err error
		priced, err = priceUsage(endpoint.Spec.Pricing, usage)
		if err != nil {
			return err
		}
	}
	cost := priced.MicroUSD
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
		PromptMicroUSD: priced.PromptMicroUSD, CompletionMicroUSD: priced.CompletionMicroUSD,
		CachedMicroUSD: priced.CachedMicroUSD, RequestMicroUSD: priced.RequestMicroUSD, ImageMicroUSD: priced.ImageMicroUSD,
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
	if err := r.persist(event); err != nil {
		log.Error().Err(err).Str("request_id", event.RequestID).Msg("managed endpoints: accounting pending or unavailable")
		return err
	}
	metricsCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := r.s.repo.RecordRouteSample(metricsCtx, sample); err != nil {
		log.Debug().Err(err).Msg("managed endpoints: record route sample")
	}
	return nil
}

// persist journals the immutable request before applying its counters. The
// meter retries unfinished journal entries after errors or gateway restarts.
func (r *router) persist(event types.EventEndpointRouteSchema) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := r.s.repo.SaveGeneration(ctx, &event, generationTTL); err != nil {
		return fmt.Errorf("journal request usage: %w", err)
	}
	if r.s.events != nil {
		r.s.events.PushEndpointRouteEvent(event)
	}
	if event.StatusCode >= 300 {
		return nil
	}
	if err := r.account(ctx, event); err != nil {
		// The complete response is durably journaled and will be charged once.
		// Returning an error here would invite a client retry even though this
		// successful engine request is already accepted for billing.
		log.Warn().Err(err).Str("request_id", event.RequestID).Msg("managed endpoints: usage journaled; accounting will retry")
	}
	return nil
}

func (r *router) account(ctx context.Context, event types.EventEndpointRouteSchema) error {
	usage := types.Usage{
		Requests: 1, PromptTokens: event.PromptTokens, CompletionTokens: event.CompletionTokens,
		CachedTokens: event.CachedTokens, Images: event.Images, MicroUSD: event.CostMicroUSD,
		PromptMicroUSD: event.PromptMicroUSD, CompletionMicroUSD: event.CompletionMicroUSD,
		CachedMicroUSD: event.CachedMicroUSD, RequestMicroUSD: event.RequestMicroUSD, ImageMicroUSD: event.ImageMicroUSD,
	}
	if err := r.s.repo.AddUsage(ctx, types.UsageSpend, event.WorkspaceID, event.Model, event.RequestID, event.Timestamp, usage); err != nil {
		return fmt.Errorf("record request spend: %w", err)
	}
	if event.CostMicroUSD > 0 && r.s.scheduler != nil {
		if gate := r.s.scheduler.CreditGate(); gate != nil {
			gate.Invalidate(ctx, event.WorkspaceID)
		}
	}
	if event.ProviderWorkspaceID != "" {
		earned := types.Usage{Requests: 1, PromptTokens: event.PromptTokens, CompletionTokens: event.CompletionTokens, CachedTokens: event.CachedTokens, Images: event.Images, MicroUSD: event.ProviderShareMicroUSD}
		if err := r.s.repo.AddUsage(ctx, types.UsageEarned, event.ProviderWorkspaceID, event.Model, event.RequestID, event.Timestamp, earned); err != nil {
			return fmt.Errorf("record provider earnings: %w", err)
		}
	}
	return r.s.repo.CompleteAccounting(ctx, event.RequestID, generationTTL)
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
	for _, replica := range replicas {
		if !replica.Serving() {
			continue
		}
		ready[replica.EndpointID] = true
	}
	data := make([]map[string]any, 0, len(endpoints))
	for _, endpoint := range endpoints {
		spec := &endpoint.Spec
		routes := map[string]any{}
		for _, route := range spec.Routes {
			routes[strings.ReplaceAll(string(route), "/", "_")] = r.prefix + "/" + string(route)
		}
		data = append(data, map[string]any{
			"id":             spec.ID,
			"name":           cmp.Or(spec.Catalog.Name, spec.ID),
			"created":        endpoint.CreatedAt.Unix(),
			"description":    spec.Catalog.Description,
			"context_length": spec.Catalog.ContextLength,
			"kind":           spec.Kind,
			"pricing":        pricingEntry(spec.Pricing),
			"owned_by":       providerName,
			"object":         "model",
			// Beam extensions: live state for the dashboard and OpenRouter-style route paths.
			"is_ready":  ready[spec.ID],
			"endpoints": routes,
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
		"status_code":              record.StatusCode,
	}})
}
