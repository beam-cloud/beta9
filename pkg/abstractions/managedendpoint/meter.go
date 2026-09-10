package managedendpoint

import (
	"context"
	"fmt"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/rs/zerolog/log"
)

// meter flushes closed minute buckets (see AddUsage) to the billing meter as
// idempotent events; a bucket is deleted only after every event landed.

const (
	meterLockKey  = "managed_endpoint:meter"
	meterLockTTL  = 30 * time.Second
	meterInterval = 5 * time.Second
)

type meter struct {
	s    *Service
	lock *common.RedisLock
}

func newMeter(s *Service) *meter { return &meter{s: s, lock: common.NewRedisLock(s.rdb)} }

func (m *meter) run(ctx context.Context) {
	ticker := time.NewTicker(meterInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		err := m.lock.WithLease(ctx, meterLockKey, common.RedisLockOptions{TtlS: int(meterLockTTL.Seconds()), Retries: 0}, m.flush)
		if err != nil && !common.IsRedisLockNotObtained(err) {
			log.Warn().Err(err).Msg("managed endpoints: meter flush failed; will retry")
		}
	}
}

// flush delivers closed buckets oldest first, stopping at the first failure.
func (m *meter) flush(ctx context.Context) error {
	// Billing verifies this marker before reading the shared counter schema;
	// pointing it at an unrelated empty Redis must never look like free usage.
	if err := m.s.rdb.Set(ctx, "managed_endpoint:accounting:schema", "2", 0).Err(); err != nil {
		return err
	}
	if err := m.recoverAccounting(ctx); err != nil {
		return err
	}
	// AddUsage chooses the bucket using Redis TIME. Use the same clock to
	// close it, so gateway clock skew cannot flush a still-writable minute.
	now, err := m.s.rdb.Time(ctx).Result()
	if err != nil {
		return err
	}
	buckets, err := m.s.repo.ListMeterBuckets(ctx, now.Truncate(time.Minute).Add(-time.Second))
	if err != nil {
		return err
	}
	for _, bucket := range buckets {
		if err := m.send(bucket); err != nil {
			return fmt.Errorf("bucket %s: %w", bucket.Key, err)
		}
		if err := m.s.repo.DeleteMeterBucket(ctx, bucket.Key); err != nil {
			return err
		}
	}
	return nil
}

func (m *meter) recoverAccounting(ctx context.Context) error {
	entries, err := m.s.repo.ListPendingAccounting(ctx, 100)
	if err != nil {
		return err
	}
	for _, event := range entries {
		if err := m.s.router.account(ctx, event); err != nil {
			return err
		}
	}
	return nil
}

func (m *meter) send(bucket types.MeterBucket) error {
	if m.s.usage == nil {
		return fmt.Errorf("usage meter is not configured")
	}
	for _, row := range bucket.Rows {
		labels := map[string]any{
			"workspace_id": row.WorkspaceID, "endpoint_id": row.Model,
			"interval_start": bucket.Start.Format(time.RFC3339Nano), "interval_end": bucket.Start.Add(time.Minute).Format(time.RFC3339Nano),
		}
		// Preserve the complete price snapshot on the billing event. OpenMeter
		// can aggregate any component without repricing historical tokens.
		for name, value := range map[string]int64{
			"prompt_tokens": row.Usage.PromptTokens, "completion_tokens": row.Usage.CompletionTokens,
			"cached_tokens": row.Usage.CachedTokens, "prompt_micro_usd": row.Usage.PromptMicroUSD,
			"completion_micro_usd": row.Usage.CompletionMicroUSD, "cached_micro_usd": row.Usage.CachedMicroUSD,
			"request_micro_usd": row.Usage.RequestMicroUSD, "image_micro_usd": row.Usage.ImageMicroUSD,
		} {
			labels[name] = value
		}
		var counters map[string]float64
		switch bucket.Kind {
		case types.UsageEarned:
			counters = map[string]float64{types.UsageMetricsEndpointProviderEarnings: float64(row.Usage.MicroUSD) / 10_000}
		default:
			counters = map[string]float64{
				types.UsageMetricsEndpointRequests:         float64(row.Usage.Requests),
				types.UsageMetricsEndpointPromptTokens:     float64(row.Usage.PromptTokens),
				types.UsageMetricsEndpointCompletionTokens: float64(row.Usage.CompletionTokens),
				types.UsageMetricsEndpointImages:           float64(row.Usage.Images),
				types.UsageMetricsEndpointCost:             float64(row.Usage.MicroUSD) / 10_000, // billing consumes cents
			}
		}
		for metric, value := range counters {
			if value <= 0 {
				continue
			}
			if err := m.s.usage.IncrementCounter(metric, labels, value); err != nil {
				return fmt.Errorf("%s %s/%s: %w", metric, row.WorkspaceID, row.Model, err)
			}
		}
	}
	return nil
}
