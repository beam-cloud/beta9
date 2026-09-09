package managedendpoint

import (
	"context"
	"fmt"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/rs/zerolog/log"
)

// Metering follows the managed compute pattern: the request path only
// increments Redis counters (AddUsage, one atomic write per request), and a
// periodic flush sends each closed minute bucket to the billing meter as one
// event per workspace, model and metric. Bucket ids make the events idempotent
// at the meter, and a bucket is deleted only after every event landed, so a
// meter outage delays billing and never loses or duplicates it. One gateway
// flushes at a time under a lease.

const (
	meterLockKey  = "managed_endpoint:meter"
	meterLockTTL  = 30 * time.Second
	meterInterval = time.Minute
	// meterGrace keeps the current minute open; buckets are keyed by the time of
	// recording, so anything older than this is final.
	meterGrace = 2 * time.Minute
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

// flush delivers closed buckets oldest first and stops at the first failure so
// billing stays ordered and the failed bucket is retried next tick.
func (m *meter) flush(ctx context.Context) error {
	buckets, err := m.s.repo.ListMeterBuckets(ctx, time.Now().Add(-meterGrace))
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

func (m *meter) send(bucket types.MeterBucket) error {
	if m.s.usage == nil {
		return nil
	}
	for _, row := range bucket.Rows {
		labels := map[string]any{
			"workspace_id": row.WorkspaceID, "endpoint_id": row.Model,
			"interval_start": bucket.Start.Format(time.RFC3339Nano), "interval_end": bucket.Start.Add(time.Minute).Format(time.RFC3339Nano),
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
