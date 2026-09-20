package managedendpoint

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"
)

// billing is the one accounting path. A Charge snapshots the caller, app
// version and price when a request is accepted; finishing it prices what the
// app reported and journals it before responding. Bounded workers meter
// journaled charges; the durable pending index recovers failed, overflowed or
// interrupted work in a flush, which one gateway runs at a time.

const (
	chargeTTL       = time.Hour // journal retention after accounting
	billingLockKey  = "managed_endpoint:meter"
	billingLockTTL  = 30 * time.Second
	billingInterval = 5 * time.Second
	pendingRetry    = 30 * time.Second // grace for the gateway's prompt accounting workers
	pendingBatch    = 100
	billingWorkers  = 4
	billingQueue    = 128
)

type billing struct {
	s     *Service
	queue chan types.Charge // an optimization; Redis owns every queued or overflowed charge
}

func newBilling(s *Service) *billing {
	return &billing{s: s, queue: make(chan types.Charge, billingQueue)}
}

func (b *billing) run(ctx context.Context) {
	var workers sync.WaitGroup
	for range billingWorkers {
		workers.Add(1)
		go func() { defer workers.Done(); b.work(ctx) }()
	}
	defer workers.Wait()
	ticker := time.NewTicker(billingInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		err := b.s.lock.WithLease(ctx, billingLockKey, common.RedisLockOptions{TtlS: int(billingLockTTL.Seconds()), Retries: 0}, b.flush)
		if err != nil && !common.IsRedisLockNotObtained(err) {
			log.Warn().Err(err).Msg("managed endpoints: billing flush failed; will retry")
		}
	}
}

func (b *billing) work(ctx context.Context) {
	for ctx.Err() == nil {
		select {
		case <-ctx.Done():
			return
		case c := <-b.queue:
			if err := b.account(ctx, &c); err != nil {
				log.Warn().Err(err).Str("charge_id", c.ID).Msg("managed endpoints: charge journaled; accounting will retry")
			}
		}
	}
}

// finish journals a finished charge and schedules accounting. The journal
// write is the commit point: once it succeeds the work is accepted for billing and a
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
	// Preserve credit checks while the external meter catches up: invalidate
	// the cached decision now, then again after accounting updates the balance.
	// Queue overflow and shutdown leave the journal pending for replay.
	b.invalidateCredit(ctx, c)
	select {
	case b.queue <- *c:
	default:
	}
	return nil
}

func (b *billing) invalidateCredit(ctx context.Context, c *types.Charge) {
	if c.Status == types.ChargeSettled && c.Cost.MicroUSD > 0 && b.s.scheduler != nil {
		if gate := b.s.scheduler.CreditGate(); gate != nil {
			gate.Invalidate(ctx, c.WorkspaceID)
		}
	}
}

// account meters a settled charge for the caller (spend) and, when a
// contributed machine served it, for the provider (earned), refreshes the
// credit gate and closes the journal entry. The meter deduplicates on the
// charge id, so a retry after a partial failure bills once.
func (b *billing) account(ctx context.Context, c *types.Charge) error {
	if c.Status == types.ChargeSettled {
		if err := b.meter(types.UsageSpend, c.WorkspaceID, c, c.Usage()); err != nil {
			return err
		}
		if c.ProviderWorkspaceID != "" {
			earned := types.Usage{Work: c.Work, Cost: types.Cost{MicroUSD: c.ProviderShareMicroUSD}}
			if err := b.meter(types.UsageEarned, c.ProviderWorkspaceID, c, earned); err != nil {
				return err
			}
		}
		b.invalidateCredit(ctx, c)
	}
	return b.s.repo.CompleteAccounting(ctx, c.ID, chargeTTL)
}

// meter sends one endpoint_usage event carrying every Usage field; each
// field has its own meter, so billing aggregates any component without
// repricing historical tokens.
func (b *billing) meter(kind types.UsageKind, workspaceID string, c *types.Charge, u types.Usage) error {
	if b.s.usage == nil {
		return errors.New("usage meter is not configured")
	}
	data := map[string]any{
		"charge_id": c.ID, "kind": string(kind), "workspace_id": workspaceID, "endpoint_id": c.AppID,
		"settled_at": c.SettledAt.UTC().Format(time.RFC3339Nano),
	}
	for i, value := range u.Fields() {
		data[types.UsageFieldNames[i]] = *value
	}
	if err := b.s.usage.IncrementCounter(types.UsageMetricsEndpointUsage, data, 1); err != nil {
		return fmt.Errorf("meter %s %s: %w", kind, c.ID, err)
	}
	return nil
}

// flush finishes pending accounting oldest first, stopping at the first
// failure. Charges younger than pendingRetry are left to the prompt workers.
func (b *billing) flush(ctx context.Context) error {
	pending, err := b.s.repo.ListPendingCharges(ctx, time.Now().Add(-pendingRetry), pendingBatch)
	if err != nil {
		return err
	}
	for _, c := range pending {
		if err := b.account(ctx, c); err != nil {
			return err
		}
	}
	return nil
}

// Usage reads one workspace's spend or earnings back from the billing meter
// as the dashboard shows it: totals, per UTC day and per model.
func Usage(ctx context.Context, meter repository.UsageMetricsRepository, kind types.UsageKind, workspaceID string, from, to time.Time) (*types.UsageReport, error) {
	reader, ok := meter.(repository.UsageMeterReader)
	if !ok {
		return nil, errors.New("usage reporting requires a collector that keeps meters")
	}
	report := &types.UsageReport{PerModel: map[string]types.Usage{}, PerDay: map[string]types.Usage{}}
	var mu sync.Mutex
	g, ctx := errgroup.WithContext(ctx)
	for i, field := range types.UsageFieldNames {
		g.Go(func() error {
			rows, err := reader.QueryMeter(ctx, types.EndpointUsageMeter(field), workspaceID, from, to, []string{"endpoint_id", "kind"})
			if err != nil {
				return err
			}
			mu.Lock()
			defer mu.Unlock()
			for _, row := range rows {
				if row.GroupBy["kind"] != string(kind) {
					continue
				}
				add := func(usage map[string]types.Usage, key string) {
					u := usage[key]
					*u.Fields()[i] += int64(row.Value)
					usage[key] = u
				}
				*report.Total.Fields()[i] += int64(row.Value)
				add(report.PerDay, row.WindowStart.UTC().Format(time.DateOnly))
				add(report.PerModel, row.GroupBy["endpoint_id"])
			}
			return nil
		})
	}
	return report, g.Wait()
}
