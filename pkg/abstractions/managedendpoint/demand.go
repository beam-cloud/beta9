package managedendpoint

import (
	"context"
	"errors"
	"math"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
)

const (
	demandLeaseTTL              = time.Minute
	demandRenewInterval         = 20 * time.Second
	demandIdleTimeout           = 5 * time.Minute
	demandOpTimeout             = time.Second
	serverlessStartupTimeout    = 10 * time.Minute
	serverlessAdmissionHeadroom = 128
)

var errDemandLimit = errors.New("on-demand endpoint request limit reached")

// One expiring member per admitted request, plus idle and startup deadlines. Redis time
// keeps gateways' leases comparable. A process crash expires its own requests;
// traffic on another gateway cannot keep those abandoned leases alive.
const endpointDemandScript = `
local now = redis.call('TIME')
now = now[1] * 1000 + math.floor(now[2] / 1000)
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', now)
local held = redis.call('ZSCORE', KEYS[1], ARGV[2])
if ARGV[1] == 'renew' and not held then return {-1, 0} end
if ARGV[1] == 'acquire' and not held then
  local count = redis.call('ZCARD', KEYS[1])
  if redis.call('ZSCORE', KEYS[1], '~idle') then count = count - 1 end
  if redis.call('ZSCORE', KEYS[1], '~wake') then count = count - 1 end
  if count >= tonumber(ARGV[5]) then return {-2, 0} end
end
if ARGV[1] == 'acquire' or ARGV[1] == 'renew' or (ARGV[1] == 'release' and held) then
  if ARGV[1] ~= 'release' then
    redis.call('ZADD', KEYS[1], now + tonumber(ARGV[3]), ARGV[2])
  else
    redis.call('ZREM', KEYS[1], ARGV[2])
  end
  redis.call('ZADD', KEYS[1], now + tonumber(ARGV[4]), '~idle')
  redis.call('PEXPIRE', KEYS[1], tonumber(ARGV[4]) + tonumber(ARGV[3]))
end
if ARGV[1] == 'wake' then
  redis.call('ZADD', KEYS[1], now + tonumber(ARGV[3]), '~wake')
  redis.call('ZADD', KEYS[1], now + tonumber(ARGV[4]), '~idle')
  redis.call('PEXPIRE', KEYS[1], tonumber(ARGV[4]) + tonumber(ARGV[3]))
end
local warm = redis.call('ZSCORE', KEYS[1], '~idle') and 1 or 0
local pending = redis.call('ZSCORE', KEYS[1], '~wake') and 1 or 0
return {redis.call('ZCARD', KEYS[1]) - warm - pending, warm, pending}
`

type endpointDemand struct {
	active   int64
	warm     bool
	pending  bool                            // a recent authorized capacity rejection requested startup
	capacity int64                           // serving capacity across every GPU type, including hot copies
	starting bool                            // wait for capacity to become known before adding another copy
	gpus     map[string]types.FleetPlacement // configured on-demand GPU alternatives
}

// readyCapacity is the finite serving capacity observed at admission. Transient
// admissions get headroom so a full engine can record a scale-out signal.
// Capacity changes never prevent an existing request from renewing its lease.
func (s *Service) demand(ctx context.Context, endpointID, operation, requestID string, readyCapacity int64) (*endpointDemand, error) {
	if s.rdb == nil {
		return nil, errors.New("on-demand endpoints require redis")
	}
	ctx, cancel := context.WithTimeout(ctx, demandOpTimeout)
	defer cancel()
	limit := serverlessAdmissionHeadroom + min(max(readyCapacity, 0), math.MaxInt64-serverlessAdmissionHeadroom)
	values, err := s.rdb.Eval(ctx, endpointDemandScript, []string{"managed_endpoint:demand:" + endpointID},
		operation, requestID, demandLeaseTTL.Milliseconds(), demandIdleTimeout.Milliseconds(), limit).Int64Slice()
	if err != nil {
		return nil, err
	}
	if values[0] == -2 {
		return nil, errDemandLimit
	}
	if values[0] < 0 {
		return nil, errors.New("on-demand request lease expired")
	}
	return &endpointDemand{active: values[0], warm: values[1] == 1, pending: values[2] == 1}, nil
}

// A rejected request may be gone before the controller ticks. Coalesce these
// requests into one short-lived signal, separate from active generation leases.
func (r *router) wake(ctx context.Context, rq *routeRequest) *routeError {
	if rq.serverless {
		if _, err := r.s.demand(ctx, rq.model, "wake", "", 0); err != nil {
			return errRegistry
		}
	}
	return nil
}

func initialDemandGrace(replica *types.EndpointReplica, now time.Time) bool {
	if !replica.ReadyAt.IsZero() {
		return now.Sub(replica.ReadyAt) < demandIdleTimeout
	}
	return !replica.StartedAt.IsZero() && now.Sub(replica.StartedAt) < serverlessStartupTimeout
}

// holdDemand runs only for hosted endpoints with an on-demand placement, after
// authorization, credit admission and concurrency admission. Losing renewal
// cancels the request rather than serving work the controller cannot observe.
func (r *router) holdDemand(rq *routeRequest) (func(), error) {
	ctx, cancel := context.WithCancel(rq.ctx.Request().Context())
	if _, err := r.s.demand(ctx, rq.model, "acquire", rq.requestID, rq.readyCapacity); err != nil {
		cancel()
		return nil, err
	}
	rq.ctx.SetRequest(rq.ctx.Request().WithContext(ctx))
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(demandRenewInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-r.s.ctx.Done():
				cancel()
				return
			case <-ticker.C:
				if _, err := r.s.demand(ctx, rq.model, "renew", rq.requestID, 0); err != nil {
					cancel()
					return
				}
			}
		}
	}()
	return func() {
		cancel()
		<-done // a late renewal must not resurrect a completed request
		_, _ = r.s.demand(context.Background(), rq.model, "release", rq.requestID, 0)
	}, nil
}

func (c *controller) readDemand(ctx context.Context, fleet *types.Fleet, live []*types.EndpointReplica) map[string]*endpointDemand {
	out := make(map[string]*endpointDemand)
	for id := range fleet.Endpoints {
		if fleet.Serverless(id) {
			// A failed read leaves nil: neither scale up nor scale down based
			// on unknown demand. Hot placements continue independently.
			out[id], _ = c.s.demand(ctx, id, "read", "", 0)
			if demand := out[id]; demand != nil {
				demand.gpus = make(map[string]types.FleetPlacement)
				for gpu, placement := range fleet.Placements(id) {
					if placement.Serverless {
						demand.gpus[gpu] = placement
					}
				}
			}
		}
	}
	for _, replica := range live {
		if replica.Status == types.ReplicaStatusDraining || replica.Status == types.ReplicaStatusEvicting {
			if id, reclaimed := strings.CutPrefix(replica.StatusReason, "gpu reclaimed for "); reclaimed {
				if demand := out[id]; demand != nil {
					demand.starting = true // wait for a donor on any GPU type to release capacity
				}
			}
		}
		demand := out[replica.EndpointID]
		if demand == nil || !replica.Alive() {
			continue
		}
		if !replica.Serving() {
			demand.starting = true
		} else {
			capacity := replica.Capacity.MaxConcurrency
			if capacity <= 0 {
				capacity = math.MaxInt64 // routing treats zero as unbounded
			}
			demand.capacity += min(capacity, math.MaxInt64-demand.capacity)
		}
	}
	return out
}
