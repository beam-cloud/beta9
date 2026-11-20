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
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/abstractions/common/llmroute"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/rs/zerolog/log"
)

// One admitted request against one replica: reserve a replica with a free
// inflight slot, relay the request, settle its charge from the response.

const (
	// maxAttempts is how many replicas one request may try.
	maxAttempts = 3
	// ejectFor keeps a replica that just failed a request out of this
	// gateway's rotation while the controller's probes judge it.
	ejectFor = 15 * time.Second
	// lostCheckInterval is how often running requests are checked against
	// the replicas that can still answer them.
	lostCheckInterval = 2 * time.Second
)

// errReplicaLost ends a request whose replica can no longer answer it.
var errReplicaLost = errors.New("replica lost")

// serve runs pick -> proxy -> settle. While the caller has received nothing
// (no response yet, or a stream that has not produced its first event) a
// failed replica is replaced by another.
func (r *router) serve(rq *routeRequest, app *types.ManagedEndpoint) error {
	ctx := rq.ctx.Request().Context()
	if rq.proto.llm {
		rq.prompt = llmroute.Read(rq.payload, rq.cacheKey())
	}
	tried := map[string]bool{}
	for range maxAttempts {
		replica, rerr := r.pick(ctx, rq, app, tried)
		if rerr != nil {
			return r.reject(rq, app, nil, rerr)
		}
		tried[replica.ID] = true
		retry, err := r.attempt(ctx, rq, app, replica)
		logger := log.Warn().Err(err).Str("endpoint_id", app.Spec.ID).Str("replica_id", replica.ID)
		if !retry {
			if err != nil {
				logger.Msg("managed endpoints: request failed after its response started")
			}
			return nil
		}
		if errors.Is(err, errLeaseLost) {
			return r.reject(rq, app, replica, errRegistry)
		}
		if rerr := r.s.stopped(ctx); rerr != nil {
			return r.reject(rq, app, replica, rerr)
		}
		r.ejected.Store(replica.ID, time.Now().Add(ejectFor))
		logger.Msg("managed endpoints: replica failed before any output; trying another")
	}
	return r.reject(rq, app, nil, errUpstreamUnavailable)
}

// attempt runs the request on a reserved replica and releases it. It reports
// whether another replica may take over: the caller received nothing here.
func (r *router) attempt(ctx context.Context, rq *routeRequest, app *types.ManagedEndpoint, replica *types.EndpointReplica) (retry bool, err error) {
	ticker := time.NewTicker(leaseRenewInterval)
	defer ticker.Stop()
	leased, stopRenewal := r.renewSlot(ctx, replica.ID, rq.requestID, ticker.C)
	defer func() {
		stopRenewal()
		r.releaseReplica(rq, replica)
	}()
	running, lose := context.WithCancelCause(leased)
	defer lose(nil)
	r.running.Store(rq, flight{replica.ID, lose})
	defer r.running.Delete(rq)
	retry, err = r.proxy(running, rq, app, replica)
	if cause := context.Cause(running); retry && cause != nil {
		err = cause // why the gateway gave up on the replica, not how the transport reported it
	}
	return retry, err
}

// pick reserves available capacity immediately. Providers must reject overload
// before opening a stream; a rejected on-demand request still triggers startup.
func (r *router) pick(ctx context.Context, rq *routeRequest, app *types.ManagedEndpoint, tried map[string]bool) (*types.EndpointReplica, *routeError) {
	candidates, err := r.servingReplicas(ctx, app, rq.pinReplica, tried)
	var replica *types.EndpointReplica
	if err == nil && (len(candidates) > 0 || len(tried) == 0) {
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
	case len(candidates) == 0 && len(tried) > 0:
		return nil, errUpstreamUnavailable
	}
	if rerr := r.wake(ctx, rq); rerr != nil {
		return nil, rerr
	}
	return nil, capacityError(fmt.Sprintf("model %s is temporarily at capacity; retry shortly", app.Spec.ID))
}

// servingReplicas lists replicas that may take this request; a pinned replica
// (X-Beam-Endpoint-Replica) bypasses the pool. A replica that just failed a
// request here sits out, unless it is the only one left.
func (r *router) servingReplicas(ctx context.Context, app *types.ManagedEndpoint, pin string, tried map[string]bool) ([]*types.EndpointReplica, error) {
	replicas, err := r.s.repo.ListReplicas(ctx, app.Spec.ID)
	if err != nil {
		return nil, err
	}
	now := time.Now()
	var serving, ejected []*types.EndpointReplica
	for _, replica := range replicas {
		switch {
		case tried[replica.ID] || replica.Address == "" || !replica.Serving() || r.s.silent(replica, now):
		case pin != "":
			if replica.ID == pin {
				return []*types.EndpointReplica{replica}, nil
			}
		case r.isEjected(replica.ID, now):
			ejected = append(ejected, replica)
		default:
			serving = append(serving, replica)
		}
	}
	if len(serving) == 0 {
		return ejected, nil
	}
	return serving, nil
}

func (r *router) isEjected(replicaID string, now time.Time) bool {
	until, ok := r.ejected.Load(replicaID)
	if ok && now.After(until.(time.Time)) {
		r.ejected.Delete(replicaID)
		return false
	}
	return ok
}

// choose reserves one inflight slot on the best replica for the request; the
// caller must releaseReplica it exactly once. An LLM prompt goes where its
// prefix is cached, anything else to the least loaded replica. Nil means
// every candidate is full.
func (r *router) choose(ctx context.Context, rq *routeRequest, app *types.ManagedEndpoint, replicas []*types.EndpointReplica) (*types.EndpointReplica, error) {
	candidates := make([]llmroute.Replica, len(replicas))
	for i, replica := range replicas {
		c := replica.Capacity
		candidates[i] = llmroute.Replica{
			ID: replica.ID, Max: c.MaxConcurrency, Queued: c.Waiting,
			// This gateway's requests are exact; the engine's last report covers the other gateways'.
			Load: max(counter(&r.inflight, replica.ID).Load(), c.InFlight, c.Running+c.Waiting),
		}
	}
	table := r.table(app.Spec.ID)
	for _, i := range table.Rank(rq.prompt, candidates) { // losers of the reservation race move on to the next replica
		reserved, err := r.reserve(ctx, rq, replicas[i])
		if err != nil {
			return nil, err
		}
		if reserved {
			table.Served(rq.prompt, replicas[i].ID)
			return replicas[i], nil
		}
	}
	return nil, nil
}

// reserve takes one inflight slot: the local counter and the shared Redis
// reservation that bounds MaxConcurrency across gateways. Uncertain shared
// capacity fails closed rather than overloading the engine.
func (r *router) reserve(ctx context.Context, rq *routeRequest, replica *types.EndpointReplica) (bool, error) {
	inflight := counter(&r.inflight, replica.ID)
	if n := inflight.Add(1); replica.Capacity.MaxConcurrency > 0 && n > replica.Capacity.MaxConcurrency {
		inflight.Add(-1)
		return false, nil
	}
	ok, err := r.s.lease(ctx, slotKey(replica.ID), leaseAcquire, rq.requestID, replica.Capacity.MaxConcurrency)
	if err != nil {
		// The server may have acquired the slot before its reply was lost.
		_, _ = r.s.lease(context.Background(), slotKey(replica.ID), leaseRelease, rq.requestID, 0)
	}
	if err != nil || !ok {
		inflight.Add(-1)
	}
	return ok, err
}

// releaseReplica returns the slot taken by reserve.
func (r *router) releaseReplica(rq *routeRequest, replica *types.EndpointReplica) {
	counter(&r.inflight, replica.ID).Add(-1)
	_, _ = r.s.lease(context.Background(), slotKey(replica.ID), leaseRelease, rq.requestID, 0)
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

// cacheKey is the name the caller gave the conversation, OpenAI's
// prompt_cache_key or a session header, so its requests share a replica.
func (rq *routeRequest) cacheKey() string {
	if key, _ := rq.payload["prompt_cache_key"].(string); key != "" {
		return key
	}
	return strings.TrimSpace(rq.ctx.Request().Header.Get(headerSession))
}

// flight is a request in flight on a replica, and how to end it.
type flight struct {
	replicaID string
	lose      context.CancelCauseFunc
}

// watch ends the requests of replicas that were lost: finished by the
// controller, or silent past the heartbeat bound. A dead machine sends
// nothing, not even a reset: its requests would hang until the caller gave up.
func (r *router) watch(ctx context.Context) {
	ticker := time.NewTicker(lostCheckInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.dropLost(ctx)
		}
	}
}

func (r *router) dropLost(ctx context.Context) {
	var flights []flight
	r.running.Range(func(_, f any) bool {
		flights = append(flights, f.(flight))
		return true
	})
	if len(flights) == 0 {
		return
	}
	replicas, err := r.s.repo.ListAllReplicas(ctx)
	if err != nil {
		return // unknown is not lost
	}
	now := time.Now()
	live := map[string]bool{}
	for _, replica := range replicas {
		live[replica.ID] = !replica.Status.Terminal() && !r.s.silent(replica, now)
	}
	for _, f := range flights {
		if !live[f.replicaID] {
			f.lose(errReplicaLost)
		}
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
	// An engine closes connections that sit idle (uvicorn after 5s), and a
	// request written to one as it closes fails without reaching the engine.
	// An idempotency key lets the transport send it again on a new connection
	// instead of reporting a failed replica; with no value it is not sent.
	if _, ok := req.Header["Idempotency-Key"]; !ok {
		req.Header["Idempotency-Key"] = nil
	}
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
	contentType, _, _ := mime.ParseMediaType(resp.Header.Get("Content-Type"))
	stream := contentType == "text/event-stream"
	w := rq.ctx.Response()
	if !w.Committed {
		for name, values := range resp.Header {
			if !hopHeaders[http.CanonicalHeaderKey(name)] {
				w.Header()[name] = values
			}
		}
		w.Header().Set(headerRequestID, rq.requestID)
		w.Header().Set(headerReplicaServed, replica.ID)
	}
	switch {
	case resp.StatusCode == http.StatusTooManyRequests:
		// Overload stays a JSON 429 even for streaming requests: never relay a
		// backend-specific error body or open an SSE response.
		return false, r.reject(rq, app, replica, capacityError("model is temporarily at capacity; retry shortly"))
	case w.Committed && !stream:
		// A stream that an earlier replica opened can only continue as one.
		return false, r.reject(rq, app, replica, errUpstreamUnavailable)
	case stream:
		return r.proxyStream(ctx, rq, app, replica, resp, sentAt)
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
// terminal marker when the stream completes, here otherwise. An upstream
// that breaks before its first event has given the caller nothing, and the
// stream may continue from another replica.
func (r *router) proxyStream(ctx context.Context, rq *routeRequest, app *types.ManagedEndpoint, replica *types.EndpointReplica, resp *http.Response, sentAt time.Time) (retry bool, err error) {
	w := rq.ctx.Response()
	if !w.Committed {
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("X-Accel-Buffering", "no")
		w.WriteHeader(resp.StatusCode)
	}
	settled := false
	finalize := func(work *types.Work, ttft time.Duration) error {
		settled = true
		if rerr := r.finish(rq, app, replica, resp.StatusCode, work, ttft, ""); rerr != nil {
			return rerr
		}
		return nil
	}
	work, ttft, err := relayStream(w, resp.Body, rq.requestID, sentAt, finalize)
	switch {
	case settled:
		return false, err
	case errors.Is(err, errStreamUnopened) && resp.StatusCode < 300:
		return true, err
	}
	status, errMsg := resp.StatusCode, ""
	if err != nil {
		errMsg = err.Error()
		if status < 300 {
			status = r.streamBroke(ctx, rq, app, err)
		}
	}
	r.finish(rq, app, replica, status, work, ttft, errMsg)
	return false, err
}

// streamBroke tells a still-connected client why its stream ended and returns
// the status to settle: a broken stream is not a success and is not billed.
func (r *router) streamBroke(ctx context.Context, rq *routeRequest, app *types.ManagedEndpoint, err error) int {
	failure := streamFailureFor(err)
	if errors.Is(context.Cause(ctx), errLeaseLost) {
		failure = streamFailureFor(errRegistry)
	}
	if rq.ctx.Request().Context().Err() == nil {
		writeStreamError(rq.ctx.Response(), rq.requestID, app.Spec.ID, failure)
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

// reject voids the charge of a request that got no response and answers with
// the error: as the response, or as the last event of a stream already open.
func (r *router) reject(rq *routeRequest, app *types.ManagedEndpoint, replica *types.EndpointReplica, rerr *routeError) error {
	r.finish(rq, app, replica, rerr.Status, nil, 0, rerr.Message)
	if !rq.ctx.Response().Committed {
		return rerr.write(rq.ctx)
	}
	if rq.ctx.Request().Context().Err() == nil {
		writeStreamError(rq.ctx.Response(), rq.requestID, app.Spec.ID, streamFailureFor(rerr))
	}
	return nil
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
