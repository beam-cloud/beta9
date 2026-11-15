package scheduler

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/beam-cloud/beta9/pkg/clock"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"
	"github.com/tj/assert"
)

func NewRequestBacklogForTest(rdb *common.RedisClient) *RequestBacklog {
	return NewRequestBacklog(rdb)
}

// Push puts a request in its lane, ready at once.
func (rb *RequestBacklog) Push(request *types.ContainerRequest) error {
	return rb.PushAfter(request, 0)
}

// Pop takes the oldest request that is ready. It is an error if none is.
func (rb *RequestBacklog) Pop() (*types.ContainerRequest, error) {
	requests, err := rb.PopN(1)
	if err != nil {
		return nil, err
	}
	if len(requests) == 0 {
		return nil, errors.New("no request is ready")
	}
	return requests[0], nil
}

// Len is the number of requests waiting in both lanes.
func (rb *RequestBacklog) Len() int64 {
	ctx := context.TODO()
	pipe := rb.rdb.Pipeline()
	foreground := pipe.ZCard(ctx, common.RedisKeys.SchedulerContainerRequests())
	background := pipe.ZCard(ctx, common.RedisKeys.SchedulerBackgroundRequests())
	if _, err := pipe.Exec(ctx); err != nil {
		return 0
	}
	return foreground.Val() + background.Val()
}

func TestRequestBacklogOrdering(t *testing.T) {
	s, err := miniredis.Run()
	assert.NotNil(t, s)
	assert.NoError(t, err)

	redisClient, err := common.NewRedisClient(types.RedisConfig{Addrs: []string{s.Addr()}, Mode: types.RedisModeSingle})
	assert.NotNil(t, redisClient)
	assert.NoError(t, err)

	rb := NewRequestBacklogForTest(redisClient)

	// Create some container requests with increasing timestamps
	req1 := &types.ContainerRequest{Timestamp: time.Unix(1, 0)}
	req2 := &types.ContainerRequest{Timestamp: time.Unix(2, 0)}
	req3 := &types.ContainerRequest{Timestamp: time.Unix(3, 0)}

	// Push them in out of order
	if err := rb.Push(req2); err != nil {
		t.Fatalf("Could not push request: %v", err)
	}
	select {
	case <-rb.ready:
	default:
		t.Fatal("ready push did not wake scheduler")
	}
	if err := rb.Push(req1); err != nil {
		t.Fatalf("Could not push request: %v", err)
	}
	if err := rb.Push(req3); err != nil {
		t.Fatalf("Could not push request: %v", err)
	}

	// Pop them and verify the order based on timestamp
	var poppedReq *types.ContainerRequest

	poppedReq, err = rb.Pop()
	if err != nil {
		t.Fatalf("Could not pop request: %v", err)
	}
	if poppedReq.Timestamp.Unix() != req1.Timestamp.Unix() {
		t.Errorf("Expected timestamp %v, got %v", req1.Timestamp.Unix(), poppedReq.Timestamp.Unix())
	}

	poppedReq, err = rb.Pop()
	if err != nil {
		t.Fatalf("Could not pop request: %v", err)
	}
	if poppedReq.Timestamp.Unix() != req2.Timestamp.Unix() {
		t.Errorf("Expected timestamp %v, got %v", req2.Timestamp.Unix(), poppedReq.Timestamp.Unix())
	}

	poppedReq, err = rb.Pop()
	if err != nil {
		t.Fatalf("Could not pop request: %v", err)
	}
	if poppedReq.Timestamp.Unix() != req3.Timestamp.Unix() {
		t.Errorf("Expected timestamp %v, got %v", req3.Timestamp.Unix(), poppedReq.Timestamp.Unix())
	}
}

func TestRequestBacklogDelayedRequestsOnlyPopWhenReady(t *testing.T) {
	s, err := miniredis.Run()
	assert.NotNil(t, s)
	assert.NoError(t, err)

	redisClient, err := common.NewRedisClient(types.RedisConfig{Addrs: []string{s.Addr()}, Mode: types.RedisModeSingle})
	assert.NotNil(t, redisClient)
	assert.NoError(t, err)

	rb := NewRequestBacklogForTest(redisClient)
	request := &types.ContainerRequest{
		ContainerId: "delayed",
		Timestamp:   time.Now(),
	}

	assert.NoError(t, rb.PushAfter(request, 50*time.Millisecond))

	early, err := rb.PopN(1)
	assert.NoError(t, err)
	assert.Empty(t, early)

	time.Sleep(75 * time.Millisecond)

	poppedReq, err := rb.Pop()
	assert.NoError(t, err)
	assert.Equal(t, request.ContainerId, poppedReq.ContainerId)
}

func TestSchedulerPushBacklogSanitizesPrivatePoolRequests(t *testing.T) {
	s, err := miniredis.Run()
	assert.NotNil(t, s)
	assert.NoError(t, err)

	redisClient, err := common.NewRedisClient(types.RedisConfig{Addrs: []string{s.Addr()}, Mode: types.RedisModeSingle})
	assert.NotNil(t, redisClient)
	assert.NoError(t, err)

	manager := NewWorkerPoolManager()
	manager.SetPool("private-pool", types.WorkerPoolConfig{Mode: types.PoolModePrivate}, nil)

	storageID := uint(1)
	storageSecret := "storage-secret"
	signingKey := "workspace-signing-key"
	scheduler := &Scheduler{
		requestBacklog:    NewRequestBacklogForTest(redisClient),
		workerPoolManager: manager,
	}
	stubConfig, err := json.Marshal(types.StubConfigV1{
		Secrets: []types.Secret{{Name: "SECRET"}},
	})
	assert.NoError(t, err)

	request := &types.ContainerRequest{
		ContainerId:  "container-1",
		PoolSelector: "private-pool",
		Env:          []string{"BETA9_TOKEN=user-token", "SECRET=value", "SAFE=value"},
		Stub: types.StubWithRelated{
			Stub: types.Stub{Config: string(stubConfig)},
		},
		Workspace: types.Workspace{
			SigningKey: &signingKey,
			Storage: &types.WorkspaceStorage{
				Id:        &storageID,
				SecretKey: &storageSecret,
			},
		},
		Mounts: []types.Mount{{
			MountPointConfig: &types.MountPointConfig{
				AccessKey: "mount-access",
				SecretKey: "mount-secret",
			},
		}},
	}

	assert.NoError(t, scheduler.pushBacklog(request, 0))

	poppedReq, err := scheduler.requestBacklog.Pop()
	assert.NoError(t, err)
	assert.Equal(t, []string{"SAFE=value"}, poppedReq.Env)
	assert.Nil(t, poppedReq.Workspace.SigningKey)
	assert.NotNil(t, poppedReq.Workspace.Storage)
	assert.Nil(t, poppedReq.Workspace.Storage.SecretKey)
	assert.Empty(t, poppedReq.Mounts[0].MountPointConfig.AccessKey)
	assert.Empty(t, poppedReq.Mounts[0].MountPointConfig.SecretKey)
}

// A request waits in the backlog free of the machine it was last placed on.
// Otherwise it could only ever run there again, and would wait for ever once
// that machine is gone.
func TestBacklogHoldsARequestFreeOfTheMachineItWasPlacedOn(t *testing.T) {
	s := miniredis.RunT(t)
	redisClient, err := common.NewRedisClient(types.RedisConfig{Addrs: []string{s.Addr()}, Mode: types.RedisModeSingle})
	assert.NoError(t, err)
	rb := NewRequestBacklogForTest(redisClient)

	placed := &types.ContainerRequest{ContainerId: "placed", Timestamp: time.Unix(1, 0)}
	placed.AssignMachine("machine-1")
	pinned := &types.ContainerRequest{ContainerId: "pinned", Timestamp: time.Unix(2, 0), MachineId: "machine-1"}
	pinned.AssignMachine("machine-1")
	assert.NoError(t, rb.Push(placed))
	assert.NoError(t, rb.Push(pinned))

	waiting, err := rb.Pop()
	assert.NoError(t, err)
	assert.Equal(t, "placed", waiting.ContainerId)
	assert.Equal(t, "", waiting.MachineId)
	assert.False(t, waiting.MachineAssigned)
	// The caller's copy is as it was: it may still be reporting on the
	// placement that failed.
	assert.Equal(t, "machine-1", placed.MachineId)

	waiting, err = rb.Pop()
	assert.NoError(t, err)
	assert.Equal(t, "pinned", waiting.ContainerId)
	assert.Equal(t, "machine-1", waiting.MachineId, "the caller chose the machine")
}

// A pass takes up to requestProcessingBatchSize requests out of the backlog
// before it reads any of them. One that cannot be read is gone by then, and
// must not take the rest of the batch with it.

func newBacklogForTest(t *testing.T) (*RequestBacklog, *common.RedisClient) {
	t.Helper()
	server := miniredis.RunT(t)
	rdb, err := common.NewRedisClient(types.RedisConfig{Addrs: []string{server.Addr()}, Mode: types.RedisModeSingle})
	require.NoError(t, err)
	t.Cleanup(func() { _ = rdb.Close() })
	return NewRequestBacklog(rdb), rdb
}

// addUnreadableEntry puts an entry that is not a request in the lane, to be
// taken up at readyAt.
func addUnreadableEntry(t *testing.T, rdb *common.RedisClient, lane string, readyAt time.Time) {
	t.Helper()
	require.NoError(t, rdb.ZAdd(context.Background(), lane, redis.Z{
		Score:  float64(readyAt.UnixNano()),
		Member: `{"container_id":"unreadable","cpu":"a lot"}`,
	}).Err())
}

// captureLog returns what is logged for the rest of the test.
func captureLog(t *testing.T) *bytes.Buffer {
	t.Helper()
	logged := &bytes.Buffer{}
	original := log.Logger
	log.Logger = zerolog.New(logged)
	t.Cleanup(func() { log.Logger = original })
	return logged
}

func containerIDs(requests []*types.ContainerRequest) []string {
	ids := make([]string, 0, len(requests))
	for _, request := range requests {
		ids = append(ids, request.ContainerId)
	}
	return ids
}

func TestABacklogEntryThatCannotBeReadCostsOnlyItself(t *testing.T) {
	backlog, rdb := newBacklogForTest(t)
	require.NoError(t, backlog.Push(&types.ContainerRequest{ContainerId: "before", Timestamp: time.Unix(1, 0)}))
	addUnreadableEntry(t, rdb, common.RedisKeys.SchedulerContainerRequests(), time.Unix(2, 0))
	require.NoError(t, backlog.Push(&types.ContainerRequest{ContainerId: "after", Timestamp: time.Unix(3, 0)}))
	require.NoError(t, backlog.Push(&types.ContainerRequest{ContainerId: "background", Timestamp: time.Unix(1, 0), Evictable: true}))
	addUnreadableEntry(t, rdb, common.RedisKeys.SchedulerBackgroundRequests(), time.Unix(2, 0))
	logged := captureLog(t)

	requests, err := backlog.PopN(requestProcessingBatchSize)

	require.NoError(t, err)
	require.Equal(t, []string{"before", "after", "background"}, containerIDs(requests))
	require.Zero(t, backlog.Len(), "an entry that cannot be read is not kept")
	require.Equal(t, 2, bytes.Count(logged.Bytes(), []byte(`"level":"error"`)), "each one is reported: %s", logged)
	require.NotContains(t, logged.String(), "a lot", "what a request holds is not for the log")
}

func TestTakingFromABacklogWithNothingReadyIsNotAnError(t *testing.T) {
	backlog, _ := newBacklogForTest(t)

	requests, err := backlog.PopN(requestProcessingBatchSize)
	require.NoError(t, err)
	require.Empty(t, requests)

	// A request that is waiting out a delay is not ready either.
	require.NoError(t, backlog.PushAfter(&types.ContainerRequest{ContainerId: "delayed"}, time.Minute))
	requests, err = backlog.PopN(requestProcessingBatchSize)
	require.NoError(t, err)
	require.Empty(t, requests)
	require.EqualValues(t, 1, backlog.Len())
}

func TestAPassPlacesTheRequestsItCanReadWhenOneCannotBeRead(t *testing.T) {
	s, err := NewSchedulerForTest()
	require.NoError(t, err)
	worker := &types.Worker{Id: "worker", Status: types.WorkerStatusAvailable, PoolName: "beta9-cpu",
		TotalCpu: 4000, FreeCpu: 4000, TotalMemory: 8000, FreeMemory: 8000}
	require.NoError(t, s.workerRepo.AddWorker(worker))
	requests := []*types.ContainerRequest{
		{ContainerId: "first", Cpu: 1000, Memory: 1000, Workspace: testWorkspaceWithStorage()},
		{ContainerId: "second", Cpu: 1000, Memory: 1000, Workspace: testWorkspaceWithStorage()},
	}
	for _, request := range requests {
		require.NoError(t, s.Run(request))
	}
	addUnreadableEntry(t, s.requestBacklog.rdb, common.RedisKeys.SchedulerContainerRequests(), time.Unix(1, 0))

	require.Equal(t, 2, s.ProcessBacklog())

	placed, err := s.workerRepo.GetNextContainerRequests(worker.Id, 10)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"first", "second"}, containerIDs(placed))
	require.Zero(t, s.requestBacklog.Len())
}

func TestAPassThatCannotTakeFromTheBacklogReportsIt(t *testing.T) {
	s, err := NewSchedulerForTest()
	require.NoError(t, err)
	// The backlog cannot be read at all.
	rdb := s.requestBacklog.rdb
	require.NoError(t, rdb.Set(context.Background(), common.RedisKeys.SchedulerContainerRequests(), "not a sorted set", 0).Err())
	logged := captureLog(t)
	reported := func() int { return strings.Count(logged.String(), "unable to take requests from the backlog") }

	require.Zero(t, s.ProcessBacklog())

	require.Contains(t, logged.String(), `"level":"error"`)
	require.Equal(t, 1, reported())

	// The loop comes back twenty times a second. It says so once in a while.
	fake := clock.NewFake(time.Now())
	s.clock = fake
	for range 20 {
		require.Zero(t, s.ProcessBacklog())
		fake.Advance(requestProcessingInterval)
	}
	require.Equal(t, 1, reported())
	fake.Advance(backlogErrorInterval)
	require.Zero(t, s.ProcessBacklog())
	require.Equal(t, 2, reported())
}

// The scheduler looks at the backlog every requestProcessingInterval. Most of
// those looks find nothing, and they stay as cheap and as quiet as they were
// when an empty backlog was reported as an error.
func TestAPassThatFindsNothingReadyAsksRedisOnceAndReportsNothing(t *testing.T) {
	s, counts := placementCostScheduler(t, 1)
	require.Zero(t, s.ProcessBacklog()) // the first pass loads the script
	logged := captureLog(t)

	counts.Start()
	require.Zero(t, s.ProcessBacklog())
	commands, roundTrips := counts.Stop()

	require.Equal(t, 1, roundTrips, "a pass with nothing to plan does not go on to read the workers: %v", commands)
	require.Empty(t, logged.String())
}
