package managedendpoint

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

type accountingFailure struct {
	repository.ManagedEndpointRepository
	kind types.UsageKind
	fail bool
}

func (r *accountingFailure) AddUsage(ctx context.Context, kind types.UsageKind, workspace, model, id string, at time.Time, usage types.Usage) error {
	if r.fail && kind == r.kind {
		r.fail = false
		return errors.New("interrupted accounting")
	}
	return r.ManagedEndpointRepository.AddUsage(ctx, kind, workspace, model, id, at, usage)
}

func TestAccountingRecoversJournalWithoutDuplicateSpend(t *testing.T) {
	for _, kind := range []types.UsageKind{types.UsageSpend, types.UsageEarned} {
		t.Run(string(kind), func(t *testing.T) {
			s := newServiceForTest(t)
			s.router = newRouter(s)
			ctx := context.Background()
			s.repo = &accountingFailure{ManagedEndpointRepository: s.repo, kind: kind, fail: true}
			now := time.Now()
			event := types.EventEndpointRouteSchema{
				RequestID: "request-1", EndpointID: "acme/model", Model: "acme/model", WorkspaceID: "buyer",
				StatusCode: 200, Timestamp: now, ProviderWorkspaceID: "provider", ProviderShareMicroUSD: 49,
				PromptTokens: 1000, CachedTokens: 800, CompletionTokens: 100, CostMicroUSD: 70,
				PromptMicroUSD: 20, CachedMicroUSD: 20, CompletionMicroUSD: 30,
			}
			require.NoError(t, s.router.persist(event), "durable acceptance stays successful even when counters need recovery")
			pending, err := s.repo.ListPendingAccounting(ctx, 100)
			require.NoError(t, err)
			require.Len(t, pending, 1)
			ttl, err := s.rdb.TTL(ctx, "managed_endpoint:generation:request-1").Result()
			require.NoError(t, err)
			require.Equal(t, time.Duration(-1), ttl, "pending journal never expires during an outage")
			// A replacement gateway recovers the durable record. Both accounting
			// legs replay safely even when spend committed before earnings failed.
			s.router = newRouter(s)
			require.NoError(t, s.meter.recoverAccounting(ctx))
			require.NoError(t, s.meter.recoverAccounting(ctx))
			spend, err := s.repo.GetUsage(ctx, types.UsageSpend, "buyer", now, now)
			require.NoError(t, err)
			require.EqualValues(t, 1, spend.Total.Requests)
			require.EqualValues(t, 70, spend.Total.MicroUSD)
			require.EqualValues(t, 800, spend.Total.CachedTokens)
			require.EqualValues(t, 20, spend.Total.CachedMicroUSD)
			earned, err := s.repo.GetUsage(ctx, types.UsageEarned, "provider", now, now)
			require.NoError(t, err)
			require.EqualValues(t, 49, earned.Total.MicroUSD)
			pending, err = s.repo.ListPendingAccounting(ctx, 100)
			require.NoError(t, err)
			require.Empty(t, pending)
		})
	}
}

func TestUnconfiguredMeterRetainsUnbilledUsage(t *testing.T) {
	s := newServiceForTest(t)
	s.usage = nil
	require.Error(t, s.meter.send(types.MeterBucket{}))
}
