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

// billing is the one accounting path. A Charge snapshots the caller, app
// version and price when a request is accepted; finishing it prices what the
// app reported and journals it; accounting applies the journal to the usage
// counters, the credit gate and the billing meter, then closes the journal
// entry. Anything left pending after a crash is finished by the next flush.

const (
	chargeTTL       = time.Hour // journal retention after accounting
	billingLockKey  = "managed_endpoint:meter"
	billingLockTTL  = 30 * time.Second
	billingInterval = 5 * time.Second
	pendingBatch    = 100
)

type billing struct {
	s    *Service
	lock *common.RedisLock
}

func newBilling(s *Service) *billing {
	return &billing{s: s, lock: common.NewRedisLock(s.rdb)}
}

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

// finish journals a finished charge and applies it. The journal write is the
// commit point: once it succeeds the work is accepted for billing and a
// failure applying counters is retried by flush, never surfaced as a client
// error that would invite a retry of already-billed work. A charge that was
// already final (a duplicate completion) is left exactly as it was.
func (b *billing) finish(ctx context.Context, c *types.Charge, replica *types.EndpointReplica) error {
	if replica != nil {
		c.ReplicaID, c.ContainerID, c.MachineID, c.GPU, c.ConfigRevision = replica.ID, replica.ContainerID, replica.MachineID, replica.GPU, replica.Config.AckedRevision
		if replica.ProviderWorkspaceID != "" && c.Cost.MicroUSD > 0 {
			c.ProviderWorkspaceID = replica.ProviderWorkspaceID
			c.ProviderShareMicroUSD = int64(float64(c.Cost.MicroUSD) * b.s.config.ProviderRevenueShare)
		}
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	written, err := b.s.repo.SaveCharge(ctx, c)
	if err != nil {
		return fmt.Errorf("journal charge: %w", err)
	}
	if !written {
		return nil
	}
	b.s.emit(types.EventEndpointRoute, types.EventEndpointSchema{
		EndpointID: c.AppID, Action: "route." + string(c.Status), WorkspaceID: c.WorkspaceID, Version: c.Version, ReplicaID: c.ReplicaID, ContainerID: c.ContainerID, GPU: c.GPU,
		Revision: c.ConfigRevision, Message: c.Error, Data: map[string]any{"charge": c},
	})
	if err := b.s.repo.RecordRouteSample(ctx, c); err != nil {
		log.Debug().Err(err).Msg("managed endpoints: record route sample")
	}
	if err := b.account(ctx, c); err != nil {
		log.Warn().Err(err).Str("charge_id", c.ID).Msg("managed endpoints: charge journaled; accounting will retry")
	}
	return nil
}

// account applies a settled charge to the caller's spend, the provider's
// earnings and the credit gate, then closes the journal entry. Every step is
// idempotent on the charge id, so a retry after a partial failure charges once.
func (b *billing) account(ctx context.Context, c *types.Charge) error {
	if c.Status == types.ChargeSettled {
		if err := b.s.repo.AddUsage(ctx, types.UsageSpend, c.WorkspaceID, c.AppID, c.ID, c.SettledAt, c.Usage()); err != nil {
			return fmt.Errorf("record spend: %w", err)
		}
		if c.Cost.MicroUSD > 0 && b.s.scheduler != nil {
			if gate := b.s.scheduler.CreditGate(); gate != nil {
				gate.Invalidate(ctx, c.WorkspaceID)
			}
		}
		if c.ProviderWorkspaceID != "" {
			earned := types.Usage{Work: c.Work, Cost: types.Cost{MicroUSD: c.ProviderShareMicroUSD}}
			if err := b.s.repo.AddUsage(ctx, types.UsageEarned, c.ProviderWorkspaceID, c.AppID, c.ID, c.SettledAt, earned); err != nil {
				return fmt.Errorf("record provider earnings: %w", err)
			}
		}
	}
	return b.s.repo.CompleteAccounting(ctx, c.ID, chargeTTL)
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
