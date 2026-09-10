package managedendpoint

import (
	"context"
	"errors"
	"time"
)

const (
	slotLeaseTTL      = time.Minute
	slotRenewInterval = 20 * time.Second
	slotOpTimeout     = time.Second
)

var errSlotLeaseLost = errors.New("hosted endpoint capacity lease lost")

// One member per request makes expiry and release independent. A crashed
// gateway cannot keep occupying a slot while other traffic renews the key;
// an expired request cannot release a newer request's reservation.
const endpointSlotScript = `
local now = redis.call('TIME')
now = now[1] * 1000 + math.floor(now[2] / 1000)
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', now)
if ARGV[1] == 'release' then
  local removed = redis.call('ZREM', KEYS[1], ARGV[2])
  if redis.call('ZCARD', KEYS[1]) == 0 then redis.call('DEL', KEYS[1]) end
  return removed
end
local held = redis.call('ZSCORE', KEYS[1], ARGV[2])
if ARGV[1] == 'renew' and not held then return -1 end
if ARGV[1] == 'acquire' and not held and tonumber(ARGV[4]) > 0 then
  if redis.call('ZCARD', KEYS[1]) >= tonumber(ARGV[4]) then return 0 end
end
redis.call('ZADD', KEYS[1], now + tonumber(ARGV[3]), ARGV[2])
redis.call('PEXPIRE', KEYS[1], ARGV[3])
return 1
`

func slotKey(replicaID string) string { return "managed_endpoint:slots:" + replicaID }

func (s *Service) slot(ctx context.Context, replicaID, operation, requestID string, capacity int64) (bool, error) {
	if s.rdb == nil || replicaID == "" || requestID == "" {
		return false, errors.New("hosted capacity requires redis and replica/request identities")
	}
	if operation != "acquire" && operation != "renew" && operation != "release" {
		return false, errors.New("invalid hosted capacity operation")
	}
	ctx, cancel := context.WithTimeout(ctx, slotOpTimeout)
	defer cancel()
	result, err := s.rdb.Eval(ctx, endpointSlotScript, []string{slotKey(replicaID)}, operation, requestID, slotLeaseTTL.Milliseconds(), capacity).Int64()
	if err != nil {
		return false, err
	}
	if result < 0 {
		return false, errSlotLeaseLost
	}
	return result == 1, nil
}

// renewSlot owns one attempt's lease lifecycle. Stop and join renewal before
// release so a late heartbeat can never revive completed work. Admission drain
// is intentionally excluded; already-running generation survives that phase.
func (r *router) renewSlot(parent context.Context, replicaID, requestID string) (context.Context, func()) {
	ticks := time.NewTicker(slotRenewInterval)
	ctx, stop := r.renewSlotWithTicks(parent, replicaID, requestID, ticks.C)
	return ctx, func() { ticks.Stop(); stop() }
}

func (r *router) renewSlotWithTicks(parent context.Context, replicaID, requestID string, ticks <-chan time.Time) (context.Context, func()) {
	ctx, cancel := context.WithCancelCause(parent)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-ctx.Done():
				return
			case <-r.s.ctx.Done():
				cancel(context.Canceled)
				return
			case <-ticks:
				if _, err := r.s.slot(ctx, replicaID, "renew", requestID, 0); err != nil {
					cancel(errSlotLeaseLost)
					return
				}
			}
		}
	}()
	return ctx, func() { cancel(context.Canceled); <-done }
}
