package managedendpoint

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/scheduler"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

// Most route unit tests do not run service workers. Explicitly finish queued
// accounting when those tests assert the resulting meter totals.
func drainAccounting(t *testing.T, s *Service) {
	t.Helper()
	for {
		select {
		case c := <-s.billing.queue:
			require.NoError(t, s.billing.account(context.Background(), &c))
		default:
			return
		}
	}
}

type blockedBillingMeter struct {
	recordingMeter
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (m *blockedBillingMeter) IncrementCounter(name string, data map[string]any, value float64) error {
	m.once.Do(func() { close(m.entered) })
	<-m.release
	return m.recordingMeter.IncrementCounter(name, data, value)
}

func TestResponseAndReplicaReleaseDoNotWaitForMeter(t *testing.T) {
	s := newServiceForTest(t)
	meter := &blockedBillingMeter{entered: make(chan struct{}), release: make(chan struct{})}
	s.usage = meter
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(meter.release) }) }
	defer unblock()
	workerCtx, stop := context.WithCancel(s.ctx)
	workerDone := make(chan struct{})
	go func() { defer close(workerDone); s.billing.run(workerCtx) }()
	defer func() { unblock(); stop(); <-workerDone }()

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"model":"acme/decision","answers":{"decision":{"type":"noul","noul":0.8}},"usage":{"input_tokens":100,"output_tokens":0}}`))
	}))
	defer upstream.Close()
	app := seedApp(t, s, "acme/decision", types.EndpointKindDecision, types.Pricing{PromptTokens: "0.000001", CompletionTokens: "0"})
	replica := seedReplica(t, s, app)
	replica.Status, replica.Address = types.ReplicaStatusReady, strings.TrimPrefix(upstream.URL, "http://")
	replica.Capacity.MaxConcurrency = 1
	require.NoError(t, s.repo.SaveReplica(context.Background(), replica))
	server := testServer(s, userInfo)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/systemone", strings.NewReader(`{"model":"acme/decision","state":"Refund requested","questions":{"decision":{"type":"noul","instructions":"Does the customer request a refund?"}}}`))
	req.Header.Set("Content-Type", "application/json")
	responseDone := make(chan struct{})
	started := time.Now()
	go func() { defer close(responseDone); server.ServeHTTP(rec, req) }()
	select {
	case <-meter.entered:
	case <-time.After(time.Second):
		t.Fatal("request never reached the meter")
	}
	select {
	case <-responseDone:
		t.Logf("response completed in %s while the meter remained blocked", time.Since(started))
	case <-time.After(time.Second):
		unblock()
		<-responseDone
		t.Fatal("a completed inference waited for external metering")
	}
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	c := charge(t, s, rec.Header().Get(headerRequestID))
	require.Equal(t, types.ChargeSettled, c.Status, "success requires durable settlement")
	require.EqualValues(t, 100, c.Cost.MicroUSD)
	require.Zero(t, counter(&s.router.inflight, replica.ID).Load())
	held, err := s.rdb.ZCard(context.Background(), slotKey(replica.ID)).Result()
	require.NoError(t, err)
	require.Zero(t, held, "a slow meter cannot occupy inference capacity")
}

func TestBillingQueueOverflowAndRestartReplayEveryCharge(t *testing.T) {
	s := newServiceForTest(t)
	ctx := context.Background()
	count := billingQueue + 2
	for i := range count {
		c := &types.Charge{ID: fmt.Sprintf("overflow-%d", i), WorkspaceID: "user-ws", AppID: "acme/model", Pricing: types.Pricing{Request: "0.01"}, AcceptedAt: time.Now().Add(-time.Minute)}
		require.NoError(t, c.Settle(types.Work{}, time.Now().Add(-time.Minute)))
		require.NoError(t, s.billing.finish(ctx, c, &types.EndpointReplica{ProviderWorkspaceID: "provider-ws"}))
	}
	require.Len(t, s.billing.queue, billingQueue, "prompt accounting is bounded")
	pending, err := s.repo.ListPendingCharges(ctx, time.Now(), int64(count))
	require.NoError(t, err)
	require.Len(t, pending, count, "overflow cannot discard durable charges")

	// A replacement gateway loses the entire optimization queue, but recovers
	// both queued and overflowed charges using the same pending index.
	s.billing = newBilling(s)
	require.NoError(t, s.billing.flush(ctx))
	require.NoError(t, s.billing.flush(ctx))
	require.EqualValues(t, count, spend(t, s, "user-ws").Requests)
	require.EqualValues(t, count*10_000, spend(t, s, "user-ws").Cost.MicroUSD)
	require.EqualValues(t, count*7_000, metered(s, types.UsageEarned, "provider-ws").Cost.MicroUSD)
	require.NoError(t, s.billing.flush(ctx))
	require.EqualValues(t, count, spend(t, s, "user-ws").Requests, "repeated recovery cannot double bill")
	pending, err = s.repo.ListPendingCharges(ctx, time.Now(), int64(count))
	require.NoError(t, err)
	require.Empty(t, pending)
}

type failProviderOnceMeter struct {
	recordingMeter
	failed bool
}

func (m *failProviderOnceMeter) IncrementCounter(name string, data map[string]any, value float64) error {
	if data["kind"] == string(types.UsageEarned) && !m.failed {
		m.failed = true
		return errors.New("provider meter unavailable")
	}
	return m.recordingMeter.IncrementCounter(name, data, value)
}

func TestBillingReplayAfterPartialProviderAccounting(t *testing.T) {
	s := newServiceForTest(t)
	meter := &failProviderOnceMeter{}
	s.usage = meter
	ctx := context.Background()
	c := &types.Charge{ID: "partial-provider", WorkspaceID: "user-ws", AppID: "acme/model", Pricing: types.Pricing{Request: "0.01"}, AcceptedAt: time.Now().Add(-time.Minute)}
	require.NoError(t, c.Settle(types.Work{}, time.Now().Add(-time.Minute)))
	require.NoError(t, s.billing.finish(ctx, c, &types.EndpointReplica{ProviderWorkspaceID: "provider-ws"}))
	queued := <-s.billing.queue
	require.Error(t, s.billing.account(ctx, &queued))
	require.Len(t, meter.events, 1, "spend succeeded before the provider meter failed")
	pending, err := s.repo.ListPendingCharges(ctx, time.Now(), 10)
	require.NoError(t, err)
	require.Len(t, pending, 1)
	require.NoError(t, s.billing.flush(ctx))
	require.NoError(t, s.billing.flush(ctx))
	require.Len(t, meter.events, 2, "replay deduplicates spend and completes provider earnings")
	for _, event := range meter.events {
		require.EqualValues(t, 1, event["requests"])
	}
	pending, err = s.repo.ListPendingCharges(ctx, time.Now(), 10)
	require.NoError(t, err)
	require.Empty(t, pending)
}

func TestBillingInvalidatesCreditBeforeAndAfterAsyncAccounting(t *testing.T) {
	s := newServiceForTest(t)
	creditServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true,"available_cents":100,"required_cents":1}`))
	}))
	defer creditServer.Close()
	config := types.AppConfig{}
	config.GatewayService.CreditGate.Mode = types.CreditGateModeHTTP
	config.GatewayService.CreditGate.Endpoint = creditServer.URL
	var err error
	s.scheduler, err = scheduler.NewScheduler(s.ctx, config, s.rdb, s.usage, nil, nil, nil)
	require.NoError(t, err)
	ctx := context.Background()
	cacheKey := common.RedisKeys.WorkspaceCreditGate("user-ws")
	gate := s.scheduler.CreditGate()
	require.NotNil(t, gate)
	checkedAt := func() time.Time {
		var cached struct {
			CheckedAt time.Time `json:"checked_at"`
		}
		body, err := s.rdb.Get(ctx, cacheKey).Bytes()
		require.NoError(t, err)
		require.NoError(t, json.Unmarshal(body, &cached))
		return cached.CheckedAt
	}
	require.NoError(t, gate.Check(ctx, "user-ws"))
	require.Less(t, time.Since(checkedAt()), config.GatewayService.CreditGate.CacheTTLOrDefault())
	c := &types.Charge{ID: "credit-async", WorkspaceID: "user-ws", AppID: "acme/model", Pricing: types.Pricing{Request: "0.01"}, AcceptedAt: time.Now()}
	require.NoError(t, c.Settle(types.Work{}, time.Now()))
	require.NoError(t, s.billing.finish(ctx, c, nil))
	require.GreaterOrEqual(t, time.Since(checkedAt()), config.GatewayService.CreditGate.CacheTTLOrDefault(), "settlement makes the existing approval stale before responding")
	require.NoError(t, gate.Check(ctx, "user-ws"))
	require.Eventually(t, func() bool { return time.Since(checkedAt()) < config.GatewayService.CreditGate.CacheTTLOrDefault() }, time.Second, time.Millisecond, "an intervening request can refresh before metering finishes")
	drainAccounting(t, s)
	require.GreaterOrEqual(t, time.Since(checkedAt()), config.GatewayService.CreditGate.CacheTTLOrDefault(), "meter completion makes any pre-meter balance decision stale again")
}
