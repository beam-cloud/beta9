package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeCreditBackend struct {
	calls    atomic.Int32
	decision creditDecision
	err      error
}

func (f *fakeCreditBackend) Check(ctx context.Context, workspaceId string) (creditDecision, error) {
	f.calls.Add(1)
	if f.err != nil {
		return creditDecision{}, f.err
	}
	return f.decision, nil
}

func newTestRedis(t *testing.T) *common.RedisClient {
	t.Helper()

	server, err := miniredis.Run()
	require.NoError(t, err)
	t.Cleanup(server.Close)

	rdb, err := common.NewRedisClient(types.RedisConfig{Addrs: []string{server.Addr()}, Mode: types.RedisModeSingle})
	require.NoError(t, err)
	t.Cleanup(func() { _ = rdb.Close() })
	return rdb
}

func newTestCreditGate(t *testing.T, backend creditGateBackend, config types.CreditGateConfig) (*CreditGate, *time.Time) {
	t.Helper()

	now := time.Now()
	gate := newCreditGate(config, backend, newTestRedis(t))
	gate.now = func() time.Time { return now }
	return gate, &now
}

func TestCreditGateDeniesWorkspaceWithoutCredit(t *testing.T) {
	backend := &fakeCreditBackend{decision: creditDecision{
		OK: false, ErrorCode: "insufficient_credits", Message: "no credits remaining",
	}}
	gate, _ := newTestCreditGate(t, backend, types.CreditGateConfig{})

	err := gate.Check(context.Background(), "ws-1")
	require.Error(t, err)

	var denied *types.InsufficientCreditsError
	require.True(t, errors.As(err, &denied))
	assert.Equal(t, "ws-1", denied.WorkspaceId)
	assert.Equal(t, "insufficient_credits", denied.Code)
	assert.Equal(t, "insufficient_credits: no credits remaining", err.Error())
}

func TestCreditGateAllowsWorkspaceWithCredit(t *testing.T) {
	backend := &fakeCreditBackend{decision: creditDecision{OK: true, AvailableCents: 500}}
	gate, _ := newTestCreditGate(t, backend, types.CreditGateConfig{})

	assert.NoError(t, gate.Check(context.Background(), "ws-1"))
}

func TestCreditGateCachesDecisionsUntilCacheTTL(t *testing.T) {
	backend := &fakeCreditBackend{decision: creditDecision{OK: true}}
	gate, now := newTestCreditGate(t, backend, types.CreditGateConfig{CacheTTL: 30 * time.Second})

	for i := 0; i < 3; i++ {
		assert.NoError(t, gate.Check(context.Background(), "ws-1"))
	}
	assert.Equal(t, int32(1), backend.calls.Load(), "fresh decisions should be served from the cache")

	// Another workspace is a separate decision.
	assert.NoError(t, gate.Check(context.Background(), "ws-2"))
	assert.Equal(t, int32(2), backend.calls.Load())

	*now = now.Add(31 * time.Second)
	assert.NoError(t, gate.Check(context.Background(), "ws-1"))
	assert.Equal(t, int32(3), backend.calls.Load(), "expired decisions should be refreshed")
}

func TestCreditGateRechecksDenialsAlmostImmediately(t *testing.T) {
	// A customer who has just bought credits must not be refused for the
	// rest of the approval cache window.
	backend := &fakeCreditBackend{decision: creditDecision{OK: false, ErrorCode: "insufficient_credits"}}
	gate, now := newTestCreditGate(t, backend, types.CreditGateConfig{CacheTTL: 30 * time.Second})

	assert.Error(t, gate.Check(context.Background(), "ws-1"))
	assert.Error(t, gate.Check(context.Background(), "ws-1"))
	assert.Equal(t, int32(1), backend.calls.Load(), "a denial is still deduplicated within its short window")

	*now = now.Add(deniedDecisionTTL + time.Millisecond)
	backend.decision = creditDecision{OK: true}
	assert.NoError(t, gate.Check(context.Background(), "ws-1"), "the purchase should take effect on the next check")
	assert.Equal(t, int32(2), backend.calls.Load())

	// Approvals keep the long window.
	*now = now.Add(10 * time.Second)
	assert.NoError(t, gate.Check(context.Background(), "ws-1"))
	assert.Equal(t, int32(2), backend.calls.Load())
}

func TestCreditGateReusesStaleDecisionWhenBillingIsDown(t *testing.T) {
	backend := &fakeCreditBackend{decision: creditDecision{OK: false, ErrorCode: "insufficient_credits"}}
	gate, now := newTestCreditGate(t, backend, types.CreditGateConfig{CacheTTL: 30 * time.Second, StaleTTL: 10 * time.Minute})

	require.Error(t, gate.Check(context.Background(), "ws-1"))

	backend.err = errors.New("billing is down")
	*now = now.Add(time.Minute)

	err := gate.Check(context.Background(), "ws-1")
	require.Error(t, err, "a stale denial must still deny while billing is unreachable")
	assert.Equal(t, int32(2), backend.calls.Load())

	// And a stale allow keeps allowing, even when configured to fail closed.
	failClosed := false
	allowBackend := &fakeCreditBackend{decision: creditDecision{OK: true}}
	allowGate, allowNow := newTestCreditGate(t, allowBackend, types.CreditGateConfig{CacheTTL: 30 * time.Second, FailOpen: &failClosed})
	require.NoError(t, allowGate.Check(context.Background(), "ws-1"))
	allowBackend.err = errors.New("billing is down")
	*allowNow = allowNow.Add(time.Minute)
	assert.NoError(t, allowGate.Check(context.Background(), "ws-1"))
}

func TestCreditGateFailurePolicyWithoutCachedDecision(t *testing.T) {
	t.Run("fail open by default", func(t *testing.T) {
		backend := &fakeCreditBackend{err: errors.New("billing is down")}
		gate, _ := newTestCreditGate(t, backend, types.CreditGateConfig{})

		assert.NoError(t, gate.Check(context.Background(), "ws-1"))

		// Fail-open decisions are not cached: billing is asked again next time.
		assert.NoError(t, gate.Check(context.Background(), "ws-1"))
		assert.Equal(t, int32(2), backend.calls.Load())
	})

	t.Run("fail closed when configured", func(t *testing.T) {
		failOpen := false
		backend := &fakeCreditBackend{err: errors.New("billing is down")}
		gate, _ := newTestCreditGate(t, backend, types.CreditGateConfig{FailOpen: &failOpen})

		err := gate.Check(context.Background(), "ws-1")
		require.Error(t, err)

		var denied *types.InsufficientCreditsError
		require.True(t, errors.As(err, &denied))
		assert.Equal(t, creditGateErrorBillingUnavailable, denied.Code)
	})
}

func TestCreditGateInvalidateForcesRefresh(t *testing.T) {
	backend := &fakeCreditBackend{decision: creditDecision{OK: false, ErrorCode: "insufficient_credits"}}
	gate, _ := newTestCreditGate(t, backend, types.CreditGateConfig{})

	require.Error(t, gate.Check(context.Background(), "ws-1"))

	backend.decision = creditDecision{OK: true}
	require.Error(t, gate.Check(context.Background(), "ws-1"), "still cached")

	gate.Invalidate(context.Background(), "ws-1")
	assert.NoError(t, gate.Check(context.Background(), "ws-1"))
	assert.Equal(t, int32(2), backend.calls.Load())
}

func TestNilCreditGateAllowsEverything(t *testing.T) {
	var gate *CreditGate
	assert.False(t, gate.Enabled())
	assert.NoError(t, gate.Check(context.Background(), "ws-1"))

	decision, err := gate.Decision(context.Background(), "ws-1")
	assert.NoError(t, err)
	assert.True(t, decision.OK)
}

func TestNewCreditGateRespectsConfig(t *testing.T) {
	rdb := newTestRedis(t)

	assert.Nil(t, NewCreditGate(types.CreditGateConfig{}, "", rdb))
	assert.Nil(t, NewCreditGate(types.CreditGateConfig{Mode: "noop", Endpoint: "http://billing"}, "", rdb))
	assert.Nil(t, NewCreditGate(types.CreditGateConfig{Mode: "http"}, "", rdb), "http mode needs an endpoint")

	gate := NewCreditGate(types.CreditGateConfig{Mode: "http", Endpoint: "http://billing/access/"}, "fallback-token", rdb)
	require.NotNil(t, gate)
	backend, ok := gate.backend.(*httpCreditGateBackend)
	require.True(t, ok)
	assert.Equal(t, "fallback-token", backend.token, "falls back to the managed compute billing token")

	gate = NewCreditGate(types.CreditGateConfig{Mode: "http", Endpoint: "http://billing/access/", AuthToken: "own-token"}, "fallback-token", rdb)
	require.NotNil(t, gate)
	assert.Equal(t, "own-token", gate.backend.(*httpCreditGateBackend).token)
}

func TestHTTPCreditGateBackend(t *testing.T) {
	var gotAuth, gotBody string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		body, _ := io.ReadAll(r.Body)
		gotBody = string(body)

		assert.Equal(t, http.MethodPost, r.Method)
		assert.Equal(t, "application/json", r.Header.Get("Content-Type"))

		_ = json.NewEncoder(w).Encode(map[string]any{
			"ok":              false,
			"error_code":      "insufficient_credits",
			"message":         "no credits remaining",
			"available_cents": 0,
			"required_cents":  1,
		})
	}))
	defer server.Close()

	backend := &httpCreditGateBackend{client: server.Client(), endpoint: server.URL, token: "secret"}
	decision, err := backend.Check(context.Background(), "ws-1")
	require.NoError(t, err)

	assert.Equal(t, "Bearer secret", gotAuth)
	assert.JSONEq(t, `{"workspace_id":"ws-1"}`, gotBody)
	assert.False(t, decision.OK)
	assert.Equal(t, "insufficient_credits", decision.ErrorCode)
	assert.Equal(t, int64(1), decision.RequiredCents)
}

func TestHTTPCreditGateBackendReportsHTTPErrors(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "nope", http.StatusForbidden)
	}))
	defer server.Close()

	backend := &httpCreditGateBackend{client: server.Client(), endpoint: server.URL}
	_, err := backend.Check(context.Background(), "ws-1")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "403")
}

func TestRunRejectsWorkspaceWithoutCredit(t *testing.T) {
	wb, err := NewSchedulerForTest()
	require.NoError(t, err)

	backend := &fakeCreditBackend{decision: creditDecision{OK: false, ErrorCode: "insufficient_credits", Message: "no credits remaining"}}
	wb.creditGate = newCreditGate(types.CreditGateConfig{}, backend, newTestRedis(t))

	request := &types.ContainerRequest{ContainerId: "no-credit-container", WorkspaceId: "ws-broke"}
	err = wb.Run(request)
	require.Error(t, err)

	var denied *types.InsufficientCreditsError
	assert.True(t, errors.As(err, &denied))

	_, err = wb.containerRepo.GetContainerState(request.ContainerId)
	notFound := &types.ErrContainerStateNotFound{}
	assert.True(t, notFound.From(err), "no container state should be created for a rejected request")
	assert.Equal(t, int64(0), wb.requestBacklog.Len())

	// The pre-flight check used by autoscalers and task submission agrees.
	err = wb.CheckConcurrencyLimit(&types.ContainerRequest{WorkspaceId: "ws-broke"})
	assert.True(t, errors.As(err, &denied))

	// A funded workspace is unaffected.
	backend.decision = creditDecision{OK: true}
	assert.NoError(t, wb.Run(&types.ContainerRequest{ContainerId: "funded-container", WorkspaceId: "ws-funded"}))
}

func TestEnforceCreditsStopsContainersOfWorkspacesWithoutCredit(t *testing.T) {
	wb, err := NewSchedulerForTest()
	require.NoError(t, err)

	rdb := wb.requestBacklog.rdb
	ctx := context.Background()

	backend := &perWorkspaceCreditBackend{decisions: map[string]creditDecision{
		"ws-broke":  {OK: false, ErrorCode: "insufficient_credits"},
		"ws-funded": {OK: true},
	}}
	wb.creditGate = newCreditGate(types.CreditGateConfig{}, backend, rdb)

	require.NoError(t, wb.workerRepo.AddWorker(&types.Worker{Id: "worker-1", PoolName: "default", Status: types.WorkerStatusAvailable}))

	addRunning := func(containerId, workspaceId string) {
		require.NoError(t, wb.containerRepo.SetContainerState(containerId, &types.ContainerState{
			ContainerId: containerId,
			Status:      types.ContainerStatusRunning,
			WorkspaceId: workspaceId,
			WorkerId:    "worker-1",
			ScheduledAt: time.Now().Unix(),
		}))
		require.NoError(t, rdb.SAdd(ctx,
			common.RedisKeys.SchedulerContainerWorkerIndex("worker-1"),
			common.RedisKeys.SchedulerContainerState(containerId),
		).Err())
	}
	addRunning("broke-1", "ws-broke")
	addRunning("broke-2", "ws-broke")
	addRunning("funded-1", "ws-funded")

	require.NoError(t, wb.enforceCredits(ctx))

	for _, containerId := range []string{"broke-1", "broke-2"} {
		state, err := wb.containerRepo.GetContainerState(containerId)
		require.NoError(t, err)
		assert.Equal(t, types.ContainerStatusStopping, state.Status, "%s should be stopping", containerId)
	}

	state, err := wb.containerRepo.GetContainerState("funded-1")
	require.NoError(t, err)
	assert.Equal(t, types.ContainerStatusRunning, state.Status, "funded workspace must be left alone")

	// One billing call per workspace, not per container.
	assert.Equal(t, int32(2), backend.calls.Load())
}

func TestEnforceCreditsSkipsPrivatePools(t *testing.T) {
	wb, err := NewSchedulerForTest()
	require.NoError(t, err)

	rdb := wb.requestBacklog.rdb
	ctx := context.Background()

	backend := &fakeCreditBackend{decision: creditDecision{OK: false, ErrorCode: "insufficient_credits"}}
	wb.creditGate = newCreditGate(types.CreditGateConfig{}, backend, rdb)

	wb.workerPoolManager.SetPool("byo-pool", types.WorkerPoolConfig{Mode: types.PoolModePrivate}, &LocalWorkerPoolControllerForTest{
		ctx: ctx, name: "byo-pool", config: wb.config, workerRepo: wb.workerRepo,
	})
	require.NoError(t, wb.workerRepo.AddWorker(&types.Worker{Id: "private-worker", PoolName: "byo-pool", Status: types.WorkerStatusAvailable}))

	require.NoError(t, wb.containerRepo.SetContainerState("private-1", &types.ContainerState{
		ContainerId: "private-1",
		Status:      types.ContainerStatusRunning,
		WorkspaceId: "ws-broke",
		WorkerId:    "private-worker",
		ScheduledAt: time.Now().Unix(),
	}))
	require.NoError(t, rdb.SAdd(ctx,
		common.RedisKeys.SchedulerContainerWorkerIndex("private-worker"),
		common.RedisKeys.SchedulerContainerState("private-1"),
	).Err())

	require.NoError(t, wb.enforceCredits(ctx))

	state, err := wb.containerRepo.GetContainerState("private-1")
	require.NoError(t, err)
	assert.Equal(t, types.ContainerStatusRunning, state.Status)
	assert.Equal(t, int32(0), backend.calls.Load(), "private pool containers never consult billing")
}

type perWorkspaceCreditBackend struct {
	calls     atomic.Int32
	decisions map[string]creditDecision
}

func (b *perWorkspaceCreditBackend) Check(ctx context.Context, workspaceId string) (creditDecision, error) {
	b.calls.Add(1)
	decision, ok := b.decisions[workspaceId]
	if !ok {
		return creditDecision{OK: true}, nil
	}
	return decision, nil
}
