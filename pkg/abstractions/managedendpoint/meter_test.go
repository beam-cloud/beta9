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

type recordingUsageMetrics struct {
	failures int
	events   []map[string]any
}

func (r *recordingUsageMetrics) Init(string) error                              { return nil }
func (r *recordingUsageMetrics) SetGauge(string, map[string]any, float64) error { return nil }
func (r *recordingUsageMetrics) IncrementCounter(name string, labels map[string]any, value float64) error {
	if r.failures > 0 {
		r.failures--
		return errors.New("synthetic meter outage")
	}
	event := map[string]any{"name": name, "value": value}
	for k, v := range labels {
		event[k] = v
	}
	r.events = append(r.events, event)
	return nil
}

// Requests only increment counters; the meter flush delivers each closed minute
// bucket once, keeps it through an outage and never sends it twice.
func TestMeterFlushDeliversClosedBucketsOnce(t *testing.T) {
	s := newServiceForTest(t)
	sink := &recordingUsageMetrics{failures: 1}
	s.usage = sink
	ctx := context.Background()
	now := time.Now()
	require.NoError(t, s.repo.AddUsage(ctx, types.UsageSpend, "ws-tenant", "acme/model", "req-1", now, types.Usage{Work: types.Work{Requests: 1, CompletionTokens: 10}, Cost: types.Cost{MicroUSD: 20_000}}))
	require.NoError(t, s.repo.AddUsage(ctx, types.UsageSpend, "ws-tenant", "acme/model", "req-2", now, types.Usage{Work: types.Work{Requests: 1, CompletionTokens: 5}, Cost: types.Cost{MicroUSD: 10_000}}))
	require.NoError(t, s.repo.AddUsage(ctx, types.UsageEarned, "ws-provider", "acme/model", "req-1", now, types.Usage{Work: types.Work{Requests: 1, CompletionTokens: 10}, Cost: types.Cost{MicroUSD: 14_000}}))

	// The current minute is still open.
	require.NoError(t, s.repo.SetChargeSchema(ctx, repository.ChargeSchema))
	require.NoError(t, s.billing.flush(ctx))
	require.Empty(t, sink.events)
	open, err := s.repo.ListMeterBuckets(ctx, now.Add(time.Hour))
	require.NoError(t, err)
	require.Len(t, open, 2)

	// Once closed, a meter outage keeps the bucket for the next tick.
	flushClosed := func() error {
		buckets, err := s.repo.ListMeterBuckets(ctx, now.Add(time.Hour))
		require.NoError(t, err)
		for _, b := range buckets {
			if err := s.billing.send(b); err != nil {
				return err
			}
			require.NoError(t, s.repo.DeleteMeterBucket(ctx, b.Key))
		}
		return nil
	}
	require.Error(t, flushClosed())
	require.Empty(t, sink.events)
	require.NoError(t, flushClosed())

	byName := map[string]map[string]any{}
	for _, e := range sink.events {
		byName[e["name"].(string)] = e
	}
	require.Len(t, sink.events, 4, "requests, completion tokens, cost and provider earnings; zero-valued metrics are skipped")
	require.EqualValues(t, 2, byName[types.UsageMetricsEndpointRequests]["value"])
	require.EqualValues(t, 15, byName[types.UsageMetricsEndpointCompletionTokens]["value"])
	require.EqualValues(t, 3, byName[types.UsageMetricsEndpointCost]["value"], "cents")
	require.Equal(t, "ws-tenant", byName[types.UsageMetricsEndpointCost]["workspace_id"])
	require.Equal(t, "acme/model", byName[types.UsageMetricsEndpointCost]["endpoint_id"])
	require.EqualValues(t, 1.4, byName[types.UsageMetricsEndpointProviderEarnings]["value"])
	require.Equal(t, "ws-provider", byName[types.UsageMetricsEndpointProviderEarnings]["workspace_id"])
	require.NotEmpty(t, byName[types.UsageMetricsEndpointCost]["interval_start"])

	remaining, err := s.repo.ListMeterBuckets(ctx, now.Add(time.Hour))
	require.NoError(t, err)
	require.Empty(t, remaining)
	require.NoError(t, flushClosed())
	require.Len(t, sink.events, 4)
}
