package repository

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

// redisRoundTrips counts Redis round trips while enabled (a pipeline is one
// round trip) and can delay each of them.
type redisRoundTrips struct {
	enabled atomic.Bool
	calls   atomic.Int64
	delay   time.Duration
}

func (h *redisRoundTrips) before() {
	if !h.enabled.Load() {
		return
	}
	h.calls.Add(1)
	if h.delay > 0 {
		time.Sleep(h.delay)
	}
}

func (h *redisRoundTrips) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h *redisRoundTrips) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		h.before()
		return next(ctx, cmd)
	}
}

func (h *redisRoundTrips) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		h.before()
		return next(ctx, cmds)
	}
}
