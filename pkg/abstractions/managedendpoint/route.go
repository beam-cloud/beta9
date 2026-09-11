package managedendpoint

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"mime"
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

// routeRequest is the state of one inference request through the pipeline.
type routeRequest struct {
	ctx           echo.Context
	auth          *auth.AuthInfo
	adapter       adapter
	route         types.EndpointRoute
	requestID     string
	models        []string // requested, in preference order
	model         string   // the endpoint selected (what the engine sees and what is billed)
	body          []byte
	payload       map[string]any // decoded JSON body; nil for multipart or empty bodies
	stream        bool
	info          *llmroute.RequestInfo
	pinReplica    string
	startedAt     time.Time
	queueWait     time.Duration
	retried       bool
	serverless    bool
	readyCapacity int64 // finite serving slots from the selected endpoint's routing snapshot
}

func (r *router) handleRoute(ctx echo.Context) error {
	cc, ok := ctx.(*auth.HttpAuthContext)
	if !ok || !workspaceCaller(cc.AuthInfo) {
		return errUnauthorized.write(ctx)
	}
	if !r.s.Enabled() {
		return errEndpointsDisabled.write(ctx)
	}
	route, pathModel, ok := routeFromPath(r.prefix, ctx.Request().URL.Path)
	if !ok {
		return errUnknownRoute.write(ctx)
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
	if clusterAdmin(cc.AuthInfo) {
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
	rq.prepareBody(endpoint)
	if rerr := r.admit(ctx.Request().Context(), rq, endpoint); rerr != nil {
		return rerr.write(ctx)
	}
	defer r.release(rq, endpoint)
	fleet, err := r.s.repo.GetFleet(ctx.Request().Context())
	if err != nil {
		return errRegistry.write(ctx)
	}
	rq.serverless = fleet.Serverless(endpoint.Spec.ID) && rq.pinReplica == ""
	if rq.serverless {
		release, err := r.holdDemand(rq)
		if err != nil {
			if errors.Is(err, errDemandLimit) {
				return capacityError("endpoint is at capacity, retry shortly").write(ctx)
			}
			return errRegistry.write(ctx)
		}
		defer release()
	}
	return r.serve(rq, endpoint)
}

// resolveEndpoint picks the first requested model the caller may use that
// serves the route, preferring one with ready replicas.
func (r *router) resolveEndpoint(ctx context.Context, rq *routeRequest) (*types.ManagedEndpoint, *routeError) {
	rq.readyCapacity = 0
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
			denied = notFound("route_not_supported", fmt.Sprintf("model %s does not serve %s", model, rq.route))
			continue
		case !r.allowed(ctx, endpoint, rq.auth):
			denied = forbidden("model_not_allowed", fmt.Sprintf("model %s is not available to this workspace", model))
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
			for _, replica := range replicas {
				// Zero means unbounded to routing, but cannot provide a
				// finite admission budget. Keep its queue allowance bounded.
				capacity := max(replica.Capacity.MaxConcurrency, 0)
				rq.readyCapacity += min(capacity, math.MaxInt64-rq.readyCapacity)
			}
			return endpoint, nil
		}
	}
	switch {
	case first != nil:
		return first, nil
	case denied != nil:
		return nil, denied
	}
	return nil, modelNotFound(rq.models[0])
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
					return errInsufficientCredits
				}
				return errBillingUnavailable
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
// before opening a stream; a rejected on-demand request still triggers startup.
func (r *router) pick(ctx context.Context, rq *routeRequest, endpoint *types.ManagedEndpoint, exclude map[string]bool) (*types.EndpointReplica, *routeError) {
	candidates, err := r.servingReplicas(ctx, endpoint, rq.pinReplica, exclude)
	var replica *types.EndpointReplica
	if err == nil && (len(candidates) > 0 || len(exclude) == 0) {
		replica, err = r.choose(ctx, rq, endpoint, candidates)
	}
	if replica != nil {
		rq.queueWait = time.Since(rq.startedAt)
		return replica, nil
	}
	// A cancelled request or a draining gateway explains most failures here
	// and must not be reported as capacity.
	if rerr := r.stopped(ctx); rerr != nil {
		return nil, rerr
	}
	switch {
	case err != nil:
		return nil, errRegistry
	case len(candidates) == 0 && len(exclude) > 0:
		return nil, errUpstreamUnavailable
	}
	if rerr := r.wake(ctx, rq); rerr != nil {
		return nil, rerr
	}
	return nil, capacityError(fmt.Sprintf("model %s is temporarily at capacity; retry shortly", endpoint.Spec.ID))
}

func (r *router) stopped(ctx context.Context) *routeError {
	var drainDone <-chan struct{}
	if r.s.drainCtx != nil {
		drainDone = r.s.drainCtx.Done()
	}
	select {
	case <-ctx.Done():
		return errClientClosed
	case <-drainDone:
		return errGatewayDraining
	default:
		return nil
	}
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
	rerr := errUpstreamUnavailable
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
		return false, r.proxyStream(ctx, rq, endpoint, replica, resp, sentAt)
	}
	return false, r.proxyJSON(ctx, rq, endpoint, replica, resp, contentType)
}

// proxyStream relays SSE as it arrives. Headers are committed, so a failure
// becomes an SSE error event; the request is metered exactly once, by the
// meter when the stream completes and here otherwise.
func (r *router) proxyStream(ctx context.Context, rq *routeRequest, endpoint *types.ManagedEndpoint, replica *types.EndpointReplica, resp *http.Response, sentAt time.Time) error {
	w := rq.ctx.Response()
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(resp.StatusCode)

	m := &streamMeter{r: r, rq: rq, endpoint: endpoint, replica: replica, status: resp.StatusCode}
	usage, ttft, err := relayStream(w, resp.Body, rq.requestID, sentAt, m.complete)
	status := resp.StatusCode
	if err != nil && status < 300 {
		status = r.streamBroke(ctx, rq, endpoint, err)
	}
	switch {
	case err == nil && unbilled(endpoint, status, usage):
		r.recordMissingUsage(rq, endpoint, replica, usage, ttft)
	case !m.recorded:
		r.record(rq, endpoint, replica, status, usage, ttft, errString(err))
	}
	return err
}

// streamMeter bills a stream once its final usage arrives, before the
// terminal marker is relayed, so an unbilled response fails visibly.
type streamMeter struct {
	r        *router
	rq       *routeRequest
	endpoint *types.ManagedEndpoint
	replica  *types.EndpointReplica
	status   int
	recorded bool
}

func (m *streamMeter) complete(usage Usage, ttft time.Duration) error {
	m.recorded = true
	if unbilled(m.endpoint, m.status, usage) {
		m.r.recordMissingUsage(m.rq, m.endpoint, m.replica, usage, ttft)
		return errors.New("upstream response carried no usage")
	}
	return m.r.record(m.rq, m.endpoint, m.replica, m.status, usage, ttft, "")
}

// streamBroke tells a still-connected client why its stream ended and returns
// the status to meter: a broken stream is not a success and is not billed.
func (r *router) streamBroke(ctx context.Context, rq *routeRequest, endpoint *types.ManagedEndpoint, err error) int {
	failure := streamFailureFor(err)
	if ctx.Err() == nil {
		writeStreamError(rq.ctx.Response(), rq.requestID, endpoint.Spec.ID, failure)
	}
	if errors.Is(context.Cause(ctx), errSlotLeaseLost) {
		return http.StatusServiceUnavailable
	}
	return failure.status
}

// proxyJSON buffers the response, meters it and writes it decorated with usage and cost.
func (r *router) proxyJSON(ctx context.Context, rq *routeRequest, endpoint *types.ManagedEndpoint, replica *types.EndpointReplica, resp *http.Response, contentType string) error {
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxBody+1))
	switch {
	case err != nil:
		rerr := errUpstreamEnded
		if errors.Is(context.Cause(ctx), errSlotLeaseLost) {
			rerr = errRegistry
		}
		r.record(rq, endpoint, replica, rerr.Status, Usage{}, 0, err.Error())
		return rerr.write(rq.ctx)
	case len(body) > maxBody:
		rerr := errUpstreamTooLarge
		r.record(rq, endpoint, replica, rerr.Status, Usage{}, 0, rerr.Message)
		return rerr.write(rq.ctx)
	}
	usage := Usage{}
	if resp.StatusCode < 300 {
		usage = rq.adapter.Usage(body)
		if unbilled(endpoint, resp.StatusCode, usage) {
			r.recordMissingUsage(rq, endpoint, replica, usage, 0)
			return errMissingUsage.write(rq.ctx)
		}
		if strings.Contains(contentType, "json") {
			body = decorateJSON(body, rq.requestID, usage, r.cost(endpoint, usage))
		}
	}
	if err := r.record(rq, endpoint, replica, resp.StatusCode, usage, 0, ""); err != nil {
		return errAccountingUnavailable.write(rq.ctx)
	}
	w := rq.ctx.Response()
	w.Header().Set("Content-Length", fmt.Sprint(len(body)))
	w.WriteHeader(resp.StatusCode)
	_, _ = w.Write(body)
	return nil
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
// mayRead: a generation is visible to its workspace and to cluster admins.
func mayRead(a *auth.AuthInfo, record *types.EventEndpointRouteSchema) bool {
	return record.WorkspaceID == a.Workspace.ExternalId || clusterAdmin(a)
}

func (r *router) handleGeneration(ctx echo.Context) error {
	cc := ctx.(*auth.HttpAuthContext)
	id := strings.TrimSpace(ctx.QueryParam("id"))
	if id == "" {
		return errMissingID.write(ctx)
	}
	record, err := r.s.repo.GetGeneration(ctx.Request().Context(), id)
	if err != nil || record == nil || !mayRead(cc.AuthInfo, record) {
		return errGenerationNotFound.write(ctx)
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
