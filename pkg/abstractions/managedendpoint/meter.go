package managedendpoint

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/rs/zerolog/log"
)

func (b *billing) run(ctx context.Context) {
	ticker := time.NewTicker(billingInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		err := b.lock.WithLease(ctx, billingLockKey, common.RedisLockOptions{TtlS: int(billingLockTTL.Seconds()), Retries: 0}, b.flush)
		if err != nil && !common.IsRedisLockNotObtained(err) {
			log.Warn().Err(err).Msg("managed endpoints: billing flush failed; will retry")
		}
	}
}

// flush finishes pending accounting oldest first, then delivers closed
// minute buckets to the billing meter, stopping at the first failure.
func (b *billing) flush(ctx context.Context) error {
	if err := b.migrate(ctx); err != nil {
		return err
	}
	pending, err := b.s.repo.ListPendingCharges(ctx, time.Now(), pendingBatch)
	if err != nil {
		return err
	}
	for _, c := range pending {
		if c.Status == types.ChargeOpen {
			if _, err := b.settleTask(ctx, c); err != nil {
				return fmt.Errorf("settle task %s: %w", c.ID, err)
			}
			continue
		}
		if err := b.account(ctx, c); err != nil {
			return err
		}
	}
	// AddUsage chooses the bucket using Redis TIME. Use the same clock to
	// close it, so gateway clock skew cannot flush a still-writable minute.
	now, err := b.s.rdb.Time(ctx).Result()
	if err != nil {
		return err
	}
	buckets, err := b.s.repo.ListMeterBuckets(ctx, now.Truncate(time.Minute).Add(-time.Second))
	if err != nil {
		return err
	}
	for _, bucket := range buckets {
		if err := b.send(bucket); err != nil {
			return fmt.Errorf("bucket %s: %w", bucket.Key, err)
		}
		if err := b.s.repo.DeleteMeterBucket(ctx, bucket.Key); err != nil {
			return err
		}
	}
	return nil
}

// migrate rewrites pre-consolidation route records still awaiting
// accounting as charges, preserving their amounts, then marks the schema.
// Billing readers verify the marker before trusting the counters.
func (b *billing) migrate(ctx context.Context) error {
	schema, err := b.s.repo.GetChargeSchema(ctx)
	if err != nil || schema == repository.ChargeSchema {
		return err
	}
	for {
		pending, err := b.s.repo.ListPendingCharges(ctx, time.Now(), pendingBatch)
		if err != nil {
			return err
		}
		for _, c := range pending {
			// GetCharge decoded legacy records into charges; write them back
			// as charges so every later reader sees one schema.
			if _, err := b.s.repo.SaveCharge(ctx, c); err != nil {
				return err
			}
			if c.Status == types.ChargeOpen {
				if err := b.s.repo.DeferAccounting(ctx, c.ID, time.Now().Add(repository.TaskPollInterval)); err != nil {
					return err
				}
				continue
			}
			if err := b.account(ctx, c); err != nil {
				return err
			}
		}
		if len(pending) < pendingBatch {
			break
		}
	}
	log.Info().Msg("managed endpoints: accounting journal migrated to charges")
	return b.s.repo.SetChargeSchema(ctx, repository.ChargeSchema)
}

// send delivers one closed minute of usage as idempotent billing events; the
// meter payload is derived from the same counters the usage API reports.
func (b *billing) send(bucket types.MeterBucket) error {
	if b.s.usage == nil {
		return errors.New("usage meter is not configured")
	}
	for _, row := range bucket.Rows {
		labels := map[string]any{
			"workspace_id": row.WorkspaceID, "endpoint_id": row.Model,
			"interval_start": bucket.Start.Format(time.RFC3339Nano), "interval_end": bucket.Start.Add(time.Minute).Format(time.RFC3339Nano),
		}
		// The complete price snapshot rides on the event so billing can
		// aggregate any component without repricing historical tokens.
		for i, value := range row.Usage.Fields() {
			labels[types.UsageFieldNames[i]] = *value
		}
		counters := map[string]float64{types.UsageMetricsEndpointProviderEarnings: float64(row.Usage.MicroUSD) / 10_000}
		if bucket.Kind == types.UsageSpend {
			counters = map[string]float64{
				types.UsageMetricsEndpointRequests:         float64(row.Usage.Requests),
				types.UsageMetricsEndpointPromptTokens:     float64(row.Usage.PromptTokens),
				types.UsageMetricsEndpointCompletionTokens: float64(row.Usage.CompletionTokens),
				types.UsageMetricsEndpointCost:             float64(row.Usage.MicroUSD) / 10_000, // billing consumes cents
			}
		}
		for metric, value := range counters {
			if value <= 0 {
				continue
			}
			if err := b.s.usage.IncrementCounter(metric, labels, value); err != nil {
				return fmt.Errorf("%s %s/%s: %w", metric, row.WorkspaceID, row.Model, err)
			}
		}
	}
	return nil
}
