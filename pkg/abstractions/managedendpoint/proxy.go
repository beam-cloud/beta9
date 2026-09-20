package managedendpoint

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/abstractions/common/llmroute"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/rs/zerolog/log"
)

// One admitted request against one replica: pick a replica with a free
// inflight slot, relay the request, settle its charge from the response.

// serve runs select -> proxy -> settle, retrying once on another replica
// when the first attempt fails before any byte reached the client.
func (r *router) serve(rq *routeRequest, app *types.ManagedEndpoint) error {
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
			return r.reject(rq, app, nil, rerr)
		}
		var leaseLost bool
		retry, err := func() (bool, error) {
			ticker := time.NewTicker(leaseRenewInterval)
			defer ticker.Stop()
			attemptCtx, stopRenewal := r.renewSlot(ctx, replica.ID, rq.requestID, ticker.C)
			defer func() {
				stopRenewal()
				r.releaseReplica(rq, r.state(app.Spec.ID), replica)
			}()
			retry, err := r.proxy(attemptCtx, rq, app, replica)
			leaseLost = errors.Is(context.Cause(attemptCtx), errLeaseLost)
			return retry, err
		}()
		if leaseLost && err != nil {
			if !rq.ctx.Response().Committed {
				r.finish(rq, app, replica, http.StatusServiceUnavailable, nil, 0, errLeaseLost.Error())
				return errRegistry.write(rq.ctx)
			}
			if rq.stream && ctx.Err() == nil {
				writeStreamError(rq.ctx.Response(), rq.requestID, app.Spec.ID, &streamFailure{http.StatusServiceUnavailable, "Request lease lost", "registry_unavailable"})
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
	return r.reject(rq, app, nil, errUpstreamUnavailable)
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
	ok, err := r.s.lease(ctx, slotKey(replica.ID), leaseAcquire, rq.requestID, replica.Capacity.MaxConcurrency)
	if err != nil {
		inflight.Add(-1)
		// The server may have acquired the slot before its reply was lost.
		_, _ = r.s.lease(context.Background(), slotKey(replica.ID), leaseRelease, rq.requestID, 0)
		return false, err
	}
	if !ok {
		inflight.Add(-1)
	} else {
		hintCtx, cancel := context.WithTimeout(ctx, leaseOpTimeout)
		_ = state.AddPressure(hintCtx, replica.ID, 1, rq.tokenPressure())
		cancel()
	}
	return ok, nil
}

// releaseReplica returns the slot taken by reserve.
func (r *router) releaseReplica(rq *routeRequest, state *llmroute.State, replica *types.EndpointReplica) {
	counter(&r.inflight, replica.ID).Add(-1)
	released, _ := r.s.lease(context.Background(), slotKey(replica.ID), leaseRelease, rq.requestID, 0)
	if released {
		hintCtx, cancel := context.WithTimeout(context.Background(), leaseOpTimeout)
		defer cancel()
		_ = state.AddPressure(hintCtx, replica.ID, -1, -rq.tokenPressure())
	}
}

// renewSlot keeps one attempt's slot alive; losing it cancels the attempt
// with errLeaseLost. Admission drain is not a reason to stop: running
// generation survives that phase.
func (r *router) renewSlot(parent context.Context, replicaID, requestID string, ticks <-chan time.Time) (context.Context, func()) {
	return r.s.keepAlive(parent, ticks, func(ctx context.Context) error {
		_, err := r.s.lease(ctx, slotKey(replicaID), leaseRenew, requestID, 0)
		return err
	}, errLeaseLost)
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

var hopHeaders = map[string]bool{
	"Connection": true, "Keep-Alive": true, "Proxy-Authenticate": true, "Proxy-Authorization": true,
	"Te": true, "Trailer": true, "Transfer-Encoding": true, "Upgrade": true, "Authorization": true,
	"Content-Length": true, "Host": true,
}

// proxy relays one attempt to a replica and settles the charge from its
// response. The bool reports whether a retry is safe (nothing was written).
func (r *router) proxy(ctx context.Context, rq *routeRequest, app *types.ManagedEndpoint, replica *types.EndpointReplica) (bool, error) {
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
	target := "http://upstream" + rq.proto.upstream
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
		r.state(app.Spec.ID).RecordAffinity(ctx, rq.info, replica.ID)
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
		// Overload stays a JSON 429 even for streaming requests: never relay a
		// backend-specific error body or open an SSE response.
		return false, r.reject(rq, app, replica, capacityError("model is temporarily at capacity; retry shortly"))
	}
	contentType, _, _ := mime.ParseMediaType(resp.Header.Get("Content-Type"))
	switch {
	case contentType == "text/event-stream":
		return false, r.proxyStream(ctx, rq, app, replica, resp, sentAt)
	case strings.Contains(contentType, "json") || resp.StatusCode >= 300:
		return false, r.proxyJSON(ctx, rq, app, replica, resp, contentType, sentAt)
	}
	return false, r.proxyBinary(rq, app, replica, resp)
}

// proxyBinary streams a non-JSON success (audio, images) straight through.
// The work is one request; it is billed only once the whole body was sent.
func (r *router) proxyBinary(rq *routeRequest, app *types.ManagedEndpoint, replica *types.EndpointReplica, resp *http.Response) error {
	if app.Pricing.PerToken() {
		return r.reject(rq, app, replica, errMissingUsage)
	}
	w := rq.ctx.Response()
	w.WriteHeader(resp.StatusCode)
	if _, err := io.Copy(w, resp.Body); err != nil {
		r.finish(rq, app, replica, http.StatusBadGateway, nil, 0, err.Error())
		return err
	}
	r.finish(rq, app, replica, resp.StatusCode, nil, 0, "")
	return nil
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
	finalize := func(work *types.Work, ttft time.Duration) error {
		settled = true
		if rerr := r.finish(rq, app, replica, resp.StatusCode, work, ttft, ""); rerr != nil {
			return rerr
		}
		return nil
	}
	work, ttft, err := relayStream(w, resp.Body, rq.requestID, sentAt, finalize)
	if settled {
		return err
	}
	status, errMsg := resp.StatusCode, ""
	if err != nil {
		errMsg = err.Error()
		if status < 300 {
			status = r.streamBroke(ctx, rq, app, err)
		}
	}
	r.finish(rq, app, replica, status, work, ttft, errMsg)
	return err
}

// streamBroke tells a still-connected client why its stream ended and returns
// the status to settle: a broken stream is not a success and is not billed.
func (r *router) streamBroke(ctx context.Context, rq *routeRequest, app *types.ManagedEndpoint, err error) int {
	failure := streamFailureFor(err)
	if ctx.Err() == nil {
		writeStreamError(rq.ctx.Response(), rq.requestID, app.Spec.ID, failure)
	}
	if errors.Is(context.Cause(ctx), errLeaseLost) {
		return http.StatusServiceUnavailable
	}
	return failure.status
}

// proxyJSON buffers the response, settles it and writes it decorated with usage and cost.
func (r *router) proxyJSON(ctx context.Context, rq *routeRequest, app *types.ManagedEndpoint, replica *types.EndpointReplica, resp *http.Response, contentType string, sentAt time.Time) error {
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxBody+1))
	addResponseTiming(rq, sentAt, time.Now())
	switch {
	case err != nil:
		rerr := errUpstreamEnded
		if errors.Is(context.Cause(ctx), errLeaseLost) {
			rerr = errRegistry
		}
		r.finish(rq, app, replica, rerr.Status, nil, 0, err.Error())
		return rerr.write(rq.ctx)
	case len(body) > maxBody:
		return r.reject(rq, app, replica, errUpstreamTooLarge)
	}
	work := tokenUsage(body)
	if rerr := r.finish(rq, app, replica, resp.StatusCode, work, 0, ""); rerr != nil {
		return rerr.write(rq.ctx)
	}
	if resp.StatusCode < 300 && strings.Contains(contentType, "json") && !rq.proto.verbatim {
		body = decorateJSON(body, rq.requestID, work != nil, rq.charge.Cost.MicroUSD)
	}
	w := rq.ctx.Response()
	w.Header().Set("Content-Length", fmt.Sprint(len(body)))
	w.WriteHeader(resp.StatusCode)
	_, _ = w.Write(body)
	return nil
}

// addResponseTiming measures buffered responses without changing their body.
// Prepare runs from this route handler's start to the final upstream dispatch:
// admission, routing and any earlier failed attempt. Upstream includes the
// transport, model server and complete response read, not just model compute.
// Settle includes usage parsing, the durable journal, accounting scheduling and
// response formatting. Total ends just before headers are written; middleware
// authentication, client networking, response transfer and async metering are
// outside this clock. Preserve any model-provided Server-Timing values.
func addResponseTiming(rq *routeRequest, sentAt, receivedAt time.Time) {
	w := rq.ctx.Response()
	w.Before(func() {
		now := time.Now()
		milliseconds := func(d time.Duration) float64 { return float64(max(d, 0)) / float64(time.Millisecond) }
		w.Header().Add("Server-Timing", fmt.Sprintf("beam_prepare;dur=%.3f, beam_upstream;dur=%.3f, beam_settle;dur=%.3f, beam_total;dur=%.3f",
			milliseconds(sentAt.Sub(rq.startedAt)), milliseconds(receivedAt.Sub(sentAt)),
			milliseconds(now.Sub(receivedAt)), milliseconds(now.Sub(rq.startedAt))))
	})
}

// reject voids the charge of a request that got no response and answers with the error.
func (r *router) reject(rq *routeRequest, app *types.ManagedEndpoint, replica *types.EndpointReplica, rerr *routeError) error {
	r.finish(rq, app, replica, rerr.Status, nil, 0, rerr.Message)
	return rerr.write(rq.ctx)
}

// finish settles or voids the request's charge from its outcome and hands it
// to billing. work is what the app reported, nil when it reported nothing.
// The result is the error to answer with when a success cannot stand: a
// token-priced response without usage is filed as a 502 (unbilled, counted
// as an error, raised as a route.missing_usage event), and a response whose
// charge could not be journaled is not confirmed to the caller either.
func (r *router) finish(rq *routeRequest, app *types.ManagedEndpoint, replica *types.EndpointReplica, status int, work *types.Work, ttft time.Duration, errMsg string) *routeError {
	c := rq.charge
	now := time.Now()
	var rerr *routeError
	switch {
	case status >= 300:
		c.Void(errMsg, now)
	case work == nil && app.Pricing.PerToken():
		rerr = errMissingUsage
		status, errMsg = rerr.Status, rerr.Message
		c.Void(errMsg, now)
		if replica != nil {
			r.s.emit(types.EventEndpointHarness, types.EventEndpointSchema{EndpointID: app.Spec.ID, Action: "route.missing_usage", ReplicaID: replica.ID, GPU: replica.GPU, Version: replica.Version})
		}
	default:
		if work == nil {
			work = &types.Work{}
		}
		if err := c.Settle(*work, now); err != nil {
			c.Void(err.Error(), now)
		}
	}
	c.StatusCode, c.Error = status, errMsg
	c.DurationMs, c.TTFTMs, c.QueueWaitMs = now.Sub(rq.startedAt).Milliseconds(), ttft.Milliseconds(), rq.queueWait.Milliseconds()
	if err := r.s.billing.finish(context.Background(), c, replica); err != nil {
		log.Error().Err(err).Str("charge_id", c.ID).Msg("managed endpoints: accounting unavailable")
		return cmp.Or(rerr, errAccountingUnavailable)
	}
	return rerr
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
