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

// The /v1 route is the one hosted admission layer: publication lookup,
// caller authorization and credit check, then dispatch to whatever executes
// the app. OpenAI-compatible paths are thin protocol adapters over it.

const (
	headerReplicaPin    = "X-Beam-Endpoint-Replica"
	headerRequestID     = "X-Request-ID"
	headerEndpointID    = "X-Beam-Endpoint-ID"
	headerReplicaServed = "X-Beam-Replica"
)

// protocol is one OpenAI-style path: where a model server serves it and
// whether it carries LLM usage (affinity routing; usage in the final chunk).
type protocol struct {
	upstream string
	llm      bool
}

var protocols = map[types.EndpointRoute]protocol{
	types.EndpointRouteChatCompletions:  {"/v1/chat/completions", true},
	types.EndpointRouteCompletions:      {"/v1/completions", true},
	types.EndpointRouteEmbeddings:       {"/v1/embeddings", false},
	types.EndpointRouteImageGenerations: {"/v1/images/generations", false},
	types.EndpointRouteImageEdits:       {"/v1/images/edits", false},
	types.EndpointRouteAudioSpeech:      {"/v1/audio/speech", false},
	types.EndpointRouteInvoke:           {"/invoke", false},
	types.EndpointRouteTasks:            {"", false},
}

// modelRoutes are the protocols a model server of each kind serves besides invoke.
var modelRoutes = map[types.EndpointKind][]types.EndpointRoute{
	types.EndpointKindLLM:       {types.EndpointRouteChatCompletions, types.EndpointRouteCompletions},
	types.EndpointKindEmbedding: {types.EndpointRouteEmbeddings},
	types.EndpointKindImage:     {types.EndpointRouteImageGenerations, types.EndpointRouteImageEdits},
}

// serves reports whether an app executes a route: model servers by kind,
// task queues only queue work, every other deployment takes any synchronous route.
func serves(app *types.ManagedEndpoint, route types.EndpointRoute) bool {
	switch {
	case app.ModelServer():
		return route == types.EndpointRouteInvoke || slices.Contains(modelRoutes[app.Spec.Kind], route)
	case app.StubType.Kind() == types.StubTypeTaskQueue:
		return route.Async()
	}
	return !route.Async()
}

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
	g.GET("/tasks/:id", auth.WithAuth(r.handleTask))
	g.DELETE("/tasks/:id", auth.WithAuth(r.handleTask))
	for name := range protocols {
		if name.ModelScoped() {
			g.POST("/models/:author/:slug/"+string(name), auth.WithAuth(r.handleRoute))
			g.POST("/models/:slug/"+string(name), auth.WithAuth(r.handleRoute))
		} else {
			g.POST("/"+string(name), auth.WithAuth(r.handleRoute))
		}
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
	rq := &routeRequest{
		ctx: ctx, auth: cc.AuthInfo, route: route, proto: protocols[route],
		requestID: "gen-" + strings.ReplaceAll(uuid.New().String(), "-", "")[:20], startedAt: time.Now(),
	}
	ctx.Response().Header().Set(headerRequestID, rq.requestID)
	if clusterAdmin(cc.AuthInfo) {
		rq.pinReplica = strings.TrimSpace(ctx.Request().Header.Get(headerReplicaPin))
	}
	if rerr := r.readRequest(rq, pathModel); rerr != nil {
		return rerr.write(ctx)
	}
	app, rerr := r.resolveEndpoint(ctx.Request().Context(), rq)
	if rerr != nil {
		return rerr.write(ctx)
	}
	rq.app = app
	rq.prepareBody(app)
	if rerr := r.admit(ctx.Request().Context(), rq, app); rerr != nil {
		return rerr.write(ctx)
	}
	defer r.release(rq, app)
	rq.charge = newCharge(rq.requestID, app, rq.auth.Workspace, rq.auth.Token.ExternalId, route, rq.startedAt)
	rq.charge.Stream = rq.stream
	if route.Async() {
		return r.enqueue(rq, app)
	}
	if app.ModelServer() {
		fleet, err := r.s.repo.GetFleet(ctx.Request().Context())
		if err != nil {
			return errRegistry.write(ctx)
		}
		if rq.serverless = fleet.Serverless(app.Spec.ID) && rq.pinReplica == ""; rq.serverless {
			release, err := r.holdDemand(rq)
			if err != nil {
				if errors.Is(err, errDemandLimit) {
					return capacityError("endpoint is at capacity, retry shortly").write(ctx)
				}
				return errRegistry.write(ctx)
			}
			defer release()
		}
		return r.serveModel(rq, app)
	}
	return r.serveDeployment(rq, app)
}

// resolve picks the first requested model the caller may use that serves
// the route, preferring a model server with ready replicas.
func (r *router) resolveEndpoint(ctx context.Context, rq *routeRequest) (*types.ManagedEndpoint, *routeError) {
	var first *types.ManagedEndpoint
	var denied *routeError
	for _, model := range rq.models {
		app, err := r.s.repo.GetEndpoint(ctx, model)
		if err != nil {
			return nil, errRegistry
		}
		switch {
		case app == nil || !app.Callable():
			continue
		case !serves(app, rq.route):
			denied = notFound("route_not_supported", fmt.Sprintf("model %s does not serve %s", model, rq.route))
			continue
		case !r.allowed(ctx, app, rq.auth):
			denied = forbidden("model_not_allowed", fmt.Sprintf("model %s is not available to this workspace", model))
			continue
		}
		if first == nil {
			first = app
		}
		if !app.ModelServer() {
			return app, nil
		}
		replicas, err := r.servingReplicas(ctx, app, rq.pinReplica, nil)
		if err != nil {
			return nil, errRegistry
		}
		if len(replicas) > 0 {
			for _, replica := range replicas {
				// Zero means unbounded to routing but cannot budget admission.
				capacity := max(replica.Capacity.MaxConcurrency, 0)
				rq.readyCapacity += min(capacity, math.MaxInt64-rq.readyCapacity)
			}
			return app, nil
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

func (r *router) allowed(ctx context.Context, app *types.ManagedEndpoint, authInfo *auth.AuthInfo) bool {
	if clusterAdmin(authInfo) {
		return true
	}
	if admin, err := r.s.AdminWorkspace(ctx); err == nil && admin.Id == authInfo.Workspace.Id {
		return true
	}
	return app.Allows(authInfo.Workspace.ExternalId, authInfo.Workspace.Name)
}

// admissionKeys are the configured concurrency counters a request holds.
func (r *router) admissionKeys(rq *routeRequest, app *types.ManagedEndpoint) (keys []string, caps []uint32) {
	if c := r.s.config.Routing.PerEndpointConcurrency; c > 0 {
		keys, caps = append(keys, app.Spec.ID), append(caps, c)
	}
	if c := r.s.config.Routing.PerWorkspaceConcurrency; c > 0 {
		keys, caps = append(keys, app.Spec.ID+"|"+rq.auth.Workspace.ExternalId), append(caps, c)
	}
	return keys, caps
}

// admit applies the credit gate and this gateway's concurrency caps; the
// cluster-wide bound is the replicas' MaxConcurrency, enforced in reserve.
func (r *router) admit(ctx context.Context, rq *routeRequest, app *types.ManagedEndpoint) *routeError {
	if !app.Pricing.Free() && r.s.scheduler != nil && !clusterAdmin(rq.auth) {
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
	keys, caps := r.admissionKeys(rq, app)
	for i, key := range keys {
		if counter(&r.admission, key).Add(1) > int64(caps[i]) {
			for _, held := range keys[:i+1] {
				counter(&r.admission, held).Add(-1)
			}
			if i == 0 && key == app.Spec.ID {
				return capacityError("endpoint is at capacity, retry shortly")
			}
			return capacityError("too many concurrent requests for this workspace")
		}
	}
	return nil
}

func (r *router) release(rq *routeRequest, app *types.ManagedEndpoint) {
	keys, _ := r.admissionKeys(rq, app)
	for _, key := range keys {
		counter(&r.admission, key).Add(-1)
	}
}

// servingReplicas lists replicas that may take this request; a pinned replica
// (X-Beam-Endpoint-Replica) bypasses the pool.
func (r *router) servingReplicas(ctx context.Context, app *types.ManagedEndpoint, pin string, exclude map[string]bool) ([]*types.EndpointReplica, error) {
	replicas, err := r.s.repo.ListReplicas(ctx, app.Spec.ID)
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
func (r *router) pick(ctx context.Context, rq *routeRequest, app *types.ManagedEndpoint, exclude map[string]bool) (*types.EndpointReplica, *routeError) {
	candidates, err := r.servingReplicas(ctx, app, rq.pinReplica, exclude)
	var replica *types.EndpointReplica
	if err == nil && (len(candidates) > 0 || len(exclude) == 0) {
		replica, err = r.choose(ctx, rq, app, candidates)
	}
	if replica != nil {
		rq.queueWait = time.Since(rq.startedAt)
		return replica, nil
	}
	if rerr := r.s.stopped(ctx); rerr != nil {
		return nil, rerr // a cancelled request or draining gateway is not "at capacity"
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
	return nil, capacityError(fmt.Sprintf("model %s is temporarily at capacity; retry shortly", app.Spec.ID))
}

// choose picks a replica (llmroute for LLMs, least loaded otherwise) with one
// inflight slot reserved; the caller must releaseReplica it exactly once. Nil
// means every candidate is saturated.
func (r *router) choose(ctx context.Context, rq *routeRequest, app *types.ManagedEndpoint, candidates []*types.EndpointReplica) (*types.EndpointReplica, error) {
	now := time.Now()
	slowStart := time.Duration(r.s.config.Routing.SlowStartSeconds) * time.Second
	state := r.state(app.Spec.ID)
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
			ID: replica.ID, Connections: local + penalty, Pressure: pressure, Engine: engineMetrics(replica.Capacity),
			ContextLen: int64(app.Catalog.ContextLength), Payload: replica,
		})
	}
	if len(eligible) == 0 {
		return nil, nil
	}
	var affinity llmroute.Affinity
	if rq.proto.llm && rq.info != nil {
		affinity = state.Affinity(ctx, rq.info)
	}
	for len(eligible) > 0 { // losers of the reservation race move on to the next candidate
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
		RunningRequests: c.Running, WaitingRequests: c.Waiting, TTFTMs: c.TTFTMs, TPOTMs: c.TPOTMs,
		DecodeTokensPerSecond: c.DecodeTokensPerSec, PromptTokensPerSecond: c.PromptTokensPerSec,
		GPUCacheUsageMilli: 1000 - c.KVCacheFreeMilli, PrefixCacheHitMilli: c.PrefixCacheHitMilli, UpdatedAtUnixMs: time.Now().UnixMilli(),
	}
}

// serveModel runs select -> proxy -> settle, retrying once on another
// replica when the first attempt fails before any byte reached the client.
func (r *router) serveModel(rq *routeRequest, app *types.ManagedEndpoint) error {
	ctx := rq.ctx.Request().Context()
	if rq.proto.llm {
		req := rq.ctx.Request()
		req.Body = io.NopCloser(bytes.NewReader(rq.body))
		if info, err := llmroute.Inspect(req, r.prefix+"/"+string(rq.route), llmroute.InspectOptions{DefaultModel: app.Spec.ID}); err == nil {
			info.RequestID = rq.requestID
			rq.info, rq.stream = info, info.Stream
		}
	}
	exclude := map[string]bool{}
	for attempt := 0; attempt < 2; attempt++ {
		replica, rerr := r.pick(ctx, rq, app, exclude)
		if rerr != nil {
			r.finish(rq, app, nil, rerr.Status, types.Work{}, false, 0, rerr.Message)
			return rerr.write(rq.ctx)
		}
		var leaseLost bool
		retry, err := func() (bool, error) {
			attemptCtx, stopRenewal := r.renewSlot(ctx, replica.ID, rq.requestID)
			defer func() {
				stopRenewal()
				r.releaseReplica(rq, r.state(app.Spec.ID), replica)
			}()
			retry, err := r.proxy(attemptCtx, rq, app, replica, r.s.transport(replica.Address), rq.proto.upstream, "")
			leaseLost = errors.Is(context.Cause(attemptCtx), errSlotLeaseLost)
			return retry, err
		}()
		if leaseLost && err != nil {
			if !rq.ctx.Response().Committed {
				r.finish(rq, app, replica, http.StatusServiceUnavailable, types.Work{}, false, 0, errSlotLeaseLost.Error())
				return errRegistry.write(rq.ctx)
			}
			if rq.stream && ctx.Err() == nil {
				writeStreamError(rq.ctx.Response(), rq.requestID, app.Spec.ID, &streamFailure{http.StatusServiceUnavailable, "Endpoint capacity lease lost", "registry_unavailable"})
			}
			return nil
		}
		if err == nil {
			return nil
		}
		logger := log.Warn().Err(err).Str("endpoint_id", app.Spec.ID).Str("replica_id", replica.ID)
		if !retry {
			logger.Msg("managed endpoints: proxy failed after response started")
			return nil
		}
		exclude[replica.ID] = true
		logger.Msg("managed endpoints: upstream failed before response; retrying")
	}
	rerr := errUpstreamUnavailable
	r.finish(rq, app, nil, rerr.Status, types.Work{}, false, 0, rerr.Message)
	return rerr.write(rq.ctx)
}

var hopHeaders = map[string]bool{
	"Connection": true, "Keep-Alive": true, "Proxy-Authenticate": true, "Proxy-Authorization": true,
	"Te": true, "Trailer": true, "Transfer-Encoding": true, "Upgrade": true, "Authorization": true,
	"Content-Length": true, "Host": true,
}

// proxy relays one attempt to an executor and settles the charge from its
// response. The bool reports whether a retry is safe (nothing was written).
func (r *router) proxy(ctx context.Context, rq *routeRequest, app *types.ManagedEndpoint, replica *types.EndpointReplica, transport http.RoundTripper, path, authorization string) (bool, error) {
	// Already-admitted work survives readiness draining; the service context
	// ends only after the HTTP graceful-shutdown window.
	upstreamCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		select {
		case <-r.s.ctx.Done():
			cancel()
		case <-upstreamCtx.Done():
		}
	}()
	target := "http://upstream" + path
	if q := rq.ctx.Request().URL.Query(); len(q) > 0 {
		q.Del("auth_token")
		target += "?" + q.Encode()
	}
	req, err := http.NewRequestWithContext(upstreamCtx, http.MethodPost, target, bytes.NewReader(rq.body))
	if err != nil {
		return true, err
	}
	for name, values := range rq.ctx.Request().Header {
		if !hopHeaders[http.CanonicalHeaderKey(name)] && !strings.HasPrefix(name, "X-Beam-") {
			req.Header[name] = values
		}
	}
	// Metering and response rewriting need plain JSON/SSE; the transport does
	// not decompress, so browser encodings are not forwarded.
	req.Header.Set("Accept-Encoding", "identity")
	req.Header.Set(headerRequestID, rq.requestID)
	req.Header.Set(headerEndpointID, app.Spec.ID)
	if authorization != "" {
		req.Header.Set("Authorization", authorization)
	}
	req.ContentLength = int64(len(rq.body))

	sentAt := time.Now()
	resp, err := transport.RoundTrip(req)
	if err != nil {
		return true, err
	}
	defer resp.Body.Close()
	if replica != nil {
		switch resp.StatusCode {
		case http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
			io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
			return true, fmt.Errorf("upstream returned %d", resp.StatusCode)
		}
		if rq.info != nil {
			r.state(app.Spec.ID).RecordAffinity(ctx, rq.info, replica.ID)
		}
	}
	w := rq.ctx.Response()
	for name, values := range resp.Header {
		if !hopHeaders[http.CanonicalHeaderKey(name)] {
			w.Header()[name] = values
		}
	}
	w.Header().Set(headerRequestID, rq.requestID)
	if replica != nil {
		w.Header().Set(headerReplicaServed, replica.ID)
	}
	if resp.StatusCode == http.StatusTooManyRequests {
		// Overload stays a JSON 429 even for streaming requests: never relay a
		// backend-specific error body or open an SSE response.
		rerr := capacityError("model is temporarily at capacity; retry shortly")
		r.finish(rq, app, replica, rerr.Status, types.Work{}, false, 0, rerr.Message)
		return false, rerr.write(rq.ctx)
	}
	contentType, _, _ := mime.ParseMediaType(resp.Header.Get("Content-Type"))
	switch {
	case contentType == "text/event-stream":
		return false, r.proxyStream(ctx, rq, app, replica, resp, sentAt)
	case strings.Contains(contentType, "json") || resp.StatusCode >= 300:
		return false, r.proxyJSON(ctx, rq, app, replica, resp, contentType)
	}
	return false, r.proxyBinary(rq, app, replica, resp)
}

// proxyBinary streams a non-JSON success (audio, images) straight through.
// The work is one request; it is billed only once the whole body was sent.
func (r *router) proxyBinary(rq *routeRequest, app *types.ManagedEndpoint, replica *types.EndpointReplica, resp *http.Response) error {
	if app.Pricing.PerToken() {
		rerr := errMissingUsage
		r.finish(rq, app, replica, rerr.Status, types.Work{}, false, 0, "token-priced app returned "+resp.Header.Get("Content-Type"))
		return rerr.write(rq.ctx)
	}
	w := rq.ctx.Response()
	w.WriteHeader(resp.StatusCode)
	_, err := io.Copy(w, resp.Body)
	status := resp.StatusCode
	if err != nil {
		status = http.StatusBadGateway
	}
	r.finish(rq, app, replica, status, types.Work{}, true, 0, errString(err))
	return err
}

// proxyStream relays SSE as it arrives. Headers are committed, so a failure
// becomes an SSE error event; the request settles exactly once: on the
// terminal marker when the stream completes, here otherwise.
func (r *router) proxyStream(ctx context.Context, rq *routeRequest, app *types.ManagedEndpoint, replica *types.EndpointReplica, resp *http.Response, sentAt time.Time) error {
	w := rq.ctx.Response()
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(resp.StatusCode)
	settled := false
	finalize := func(work types.Work, found bool, ttft time.Duration) error {
		settled = true
		return r.finish(rq, app, replica, resp.StatusCode, work, found, ttft, "")
	}
	work, found, ttft, err := relayStream(w, resp.Body, rq.requestID, sentAt, finalize)
	status := resp.StatusCode
	if err != nil && status < 300 && !settled {
		failure := streamFailureFor(err)
		if ctx.Err() == nil {
			writeStreamError(w, rq.requestID, app.Spec.ID, failure)
		}
		status = failure.status
		if errors.Is(context.Cause(ctx), errSlotLeaseLost) {
			status = http.StatusServiceUnavailable
		}
	}
	if !settled {
		r.finish(rq, app, replica, status, work, found, ttft, errString(err))
	}
	return err
}

// proxyJSON buffers the response, settles it and writes it decorated with usage and cost.
func (r *router) proxyJSON(ctx context.Context, rq *routeRequest, app *types.ManagedEndpoint, replica *types.EndpointReplica, resp *http.Response, contentType string) error {
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxBody+1))
	switch {
	case err != nil:
		rerr := errUpstreamEnded
		if errors.Is(context.Cause(ctx), errSlotLeaseLost) {
			rerr = errRegistry
		}
		r.finish(rq, app, replica, rerr.Status, types.Work{}, false, 0, err.Error())
		return rerr.write(rq.ctx)
	case len(body) > maxBody:
		rerr := errUpstreamTooLarge
		r.finish(rq, app, replica, rerr.Status, types.Work{}, false, 0, rerr.Message)
		return rerr.write(rq.ctx)
	}
	work, found := tokenUsage(body)
	if resp.StatusCode < 300 && !found && app.Pricing.PerToken() {
		r.finish(rq, app, replica, errMissingUsage.Status, work, false, 0, errMissingUsage.Message)
		return errMissingUsage.write(rq.ctx)
	}
	if err := r.finish(rq, app, replica, resp.StatusCode, work, found, 0, ""); err != nil {
		return errAccountingUnavailable.write(rq.ctx)
	}
	if resp.StatusCode < 300 && strings.Contains(contentType, "json") {
		body = decorateJSON(body, rq.requestID, found, rq.charge.Cost.MicroUSD)
	}
	w := rq.ctx.Response()
	w.Header().Set("Content-Length", fmt.Sprint(len(body)))
	w.WriteHeader(resp.StatusCode)
	_, _ = w.Write(body)
	return nil
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// finish settles or voids the request's charge from its outcome and hands
// it to billing. A success without the usage its price needs is filed as a
// 502: unbilled, counted as an error, and raised as a route.missing_usage event.
func (r *router) finish(rq *routeRequest, app *types.ManagedEndpoint, replica *types.EndpointReplica, status int, work types.Work, found bool, ttft time.Duration, errMsg string) error {
	c := rq.charge
	now := time.Now()
	c.StatusCode, c.Error = status, errMsg
	c.DurationMs, c.TTFTMs, c.QueueWaitMs = now.Sub(rq.startedAt).Milliseconds(), ttft.Milliseconds(), rq.queueWait.Milliseconds()
	switch {
	case status < 300 && (found || !app.Pricing.PerToken()):
		if err := c.Settle(work, now); err != nil {
			c.Void(err.Error(), now)
		}
	default:
		c.Void(errMsg, now)
		if status == errMissingUsage.Status && errMsg == errMissingUsage.Message && replica != nil {
			r.s.emit(types.EventEndpointHarness, types.EventEndpointSchema{EndpointID: app.Spec.ID, Action: "route.missing_usage", ReplicaID: replica.ID, GPU: replica.GPU, Version: replica.Version})
		}
	}
	r.s.billing.attribute(c, replica)
	if err := r.s.billing.finish(context.Background(), c); err != nil {
		log.Error().Err(err).Str("charge_id", c.ID).Msg("managed endpoints: accounting unavailable")
		return err
	}
	return nil
}

// pricingEntry renders per-unit prices as OpenRouter does ("0" when unset).
func pricingEntry(p types.Pricing) map[string]any {
	entry := map[string]any{"prompt": p.PromptTokens, "completion": p.CompletionTokens, "request": p.Request}
	for key, value := range entry {
		if value == "" {
			entry[key] = "0"
		}
	}
	if p.CachedPromptTokens != "" {
		entry["input_cache_read"] = p.CachedPromptTokens
	}
	return entry
}

// visible lists the enabled apps the caller may use.
func (r *router) visible(ctx context.Context, a *auth.AuthInfo) ([]*types.ManagedEndpoint, error) {
	all, err := r.s.repo.ListEndpoints(ctx)
	if err != nil {
		return nil, err
	}
	return slices.DeleteFunc(all, func(app *types.ManagedEndpoint) bool { return !app.Callable() || !r.allowed(ctx, app, a) }), nil
}

func (r *router) handleListModels(ctx echo.Context) error {
	cc := ctx.(*auth.HttpAuthContext)
	rctx := ctx.Request().Context()
	apps, err := r.visible(rctx, cc.AuthInfo)
	if err != nil {
		return errRegistry.write(ctx)
	}
	replicas, _ := r.s.repo.ListAllReplicas(rctx)
	ready := map[string]bool{}
	for _, replica := range replicas {
		ready[replica.EndpointID] = ready[replica.EndpointID] || replica.Serving()
	}
	data := make([]map[string]any, 0, len(apps))
	for _, app := range apps {
		routes := map[string]any{}
		for name := range protocols {
			if serves(app, name) {
				if name.ModelScoped() {
					routes[string(name)] = r.prefix + "/models/" + app.Spec.ID + "/" + string(name)
				} else {
					routes[strings.ReplaceAll(string(name), "/", "_")] = r.prefix + "/" + string(name)
				}
			}
		}
		data = append(data, map[string]any{
			"id": app.Spec.ID, "name": app.Catalog.Name, "created": app.CreatedAt.Unix(), "description": app.Catalog.Description,
			"context_length": app.Catalog.ContextLength, "kind": cmp.Or(string(app.Spec.Kind), app.StubType.Kind()), "pricing": pricingEntry(app.Pricing),
			"owned_by": providerName, "object": "model",
			// Beam extensions: live state for the dashboard and OpenRouter-style route paths.
			"is_ready": ready[app.Spec.ID] || !app.ModelServer(), "endpoints": routes,
		})
	}
	return ctx.JSON(http.StatusOK, map[string]any{"object": "list", "data": data})
}

// mayRead: a charge is visible to its caller and to cluster admins.
func mayRead(a *auth.AuthInfo, c *types.Charge) bool {
	return c.WorkspaceID == a.Workspace.ExternalId || clusterAdmin(a)
}

// chargeView is the caller-facing settlement of one charge.
func chargeView(c *types.Charge) map[string]any {
	return map[string]any{
		"status": c.Status, "total_cost": costUSD(c.Cost.MicroUSD), "pricing": c.Pricing, "usage": c.Usage(),
		"settled_at": nullableTime(c.SettledAt),
	}
}

func nullableTime(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return t.UTC().Format(time.RFC3339Nano)
}

// handleGeneration returns the metered record of one request by id in
// OpenRouter's generation shape.
func (r *router) handleGeneration(ctx echo.Context) error {
	cc := ctx.(*auth.HttpAuthContext)
	id := strings.TrimSpace(ctx.QueryParam("id"))
	if id == "" {
		return errMissingID.write(ctx)
	}
	c, err := r.s.repo.GetCharge(ctx.Request().Context(), id)
	if err != nil || c == nil || !mayRead(cc.AuthInfo, c) {
		return errGenerationNotFound.write(ctx)
	}
	return ctx.JSON(http.StatusOK, map[string]any{"data": map[string]any{
		"id": c.ID, "model": c.AppID, "provider_name": providerName, "created_at": c.AcceptedAt.UTC().Format(time.RFC3339Nano),
		"streamed": c.Stream, "generation_time": c.DurationMs, "latency": c.TTFTMs,
		"tokens_prompt": c.Work.PromptTokens, "tokens_completion": c.Work.CompletionTokens,
		"native_tokens_prompt": c.Work.PromptTokens, "native_tokens_completion": c.Work.CompletionTokens, "native_tokens_cached": c.Work.CachedTokens,
		"total_cost": costUSD(c.Cost.MicroUSD), "usage": costUSD(c.Cost.MicroUSD), "cache_discount": nil, "finish_reason": nil, "is_byok": false,
		"gpu": c.GPU, "status_code": c.StatusCode, "charge": chargeView(c),
	}})
}

// stopped reports whether the caller went away or the gateway is draining.
func (s *Service) stopped(ctx context.Context) *routeError {
	var drainDone <-chan struct{}
	if s.drainCtx != nil {
		drainDone = s.drainCtx.Done()
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
