package vm

import (
	"context"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

const activityReconnectGrace = 150 * time.Second

func activityKey(id string) string { return "vm:activity:" + id }

func (s *Service) renewActivity(ctx context.Context, id, lease string) error {
	if s.rdb == nil {
		return nil
	}
	key := activityKey(id)
	_, err := s.rdb.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.ZRemRangeByScore(ctx, key, "-inf", strconv.FormatInt(time.Now().UnixMilli(), 10))
		pipe.ZAdd(ctx, key, redis.Z{Member: lease, Score: float64(time.Now().Add(activityReconnectGrace).UnixMilli())})
		pipe.Expire(ctx, key, activityReconnectGrace)
		return nil
	})
	return err
}

func (s *Service) hasActivity(ctx context.Context, id string) (bool, error) {
	if s.rdb == nil {
		return false, nil
	}
	count, err := s.rdb.ZCount(ctx, activityKey(id), strconv.FormatInt(time.Now().UnixMilli(), 10), "+inf").Result()
	return count > 0, err
}

func (s *Service) keepActive(ctx context.Context, id string, idleTimeout int64) (func(), error) {
	if err := s.repo.TouchVM(ctx, id); err != nil {
		return nil, err
	}
	lease := uuid.NewString()
	if err := s.renewActivity(ctx, id, lease); err != nil {
		return nil, err
	}

	done := make(chan struct{})
	go func() {
		interval := 15 * time.Second
		if idleTimeout > 0 {
			interval = min(interval, time.Duration(idleTimeout)*time.Second/3)
		}

		tick := time.NewTicker(interval)
		defer tick.Stop()
		for {
			select {
			case <-done:
				return
			case <-ctx.Done():
				return
			case <-tick.C:
				_ = s.repo.TouchVM(ctx, id)
				_ = s.renewActivity(ctx, id, lease)
			}
		}
	}()
	return func() {
		close(done)
		// Keep the lease across gateway shutdown; a replacement can reattach.
		if s.rdb != nil && (s.ctx == nil || s.ctx.Err() == nil) {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			s.rdb.ZRem(cleanupCtx, activityKey(id), lease)
		}
	}, nil
}
