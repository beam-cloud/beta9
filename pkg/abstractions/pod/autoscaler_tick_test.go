package pod

import (
	"context"
	"fmt"
	"sync"
	"testing"

	abstractions "github.com/beam-cloud/beta9/pkg/abstractions/common"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

// redisCommands counts the commands a client sends to Redis, by name, and
// the round trips they take. A pipeline is one round trip and each command
// in it is counted. A script counts as the one EVALSHA the client sends; the
// commands the script runs inside Redis are not seen here.
type redisCommands struct {
	mu     sync.Mutex
	byName map[string]int
	trips  int
}

func (h *redisCommands) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h *redisCommands) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		h.count(cmd)
		return next(ctx, cmd)
	}
}

func (h *redisCommands) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		h.count(cmds...)
		return next(ctx, cmds)
	}
}

func (h *redisCommands) count(cmds ...redis.Cmder) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.byName == nil {
		h.byName = map[string]int{}
	}
	h.trips++
	for _, cmd := range cmds {
		h.byName[cmd.Name()]++
	}
}

// reset forgets what was counted so far.
func (h *redisCommands) reset() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.byName, h.trips = nil, 0
}

// counts is the number of commands of each name since the last reset.
func (h *redisCommands) counts() map[string]int {
	h.mu.Lock()
	defer h.mu.Unlock()
	counts := make(map[string]int, len(h.byName))
	for name, n := range h.byName {
		counts[name] = n
	}
	return counts
}

// total is the number of commands counted since the last reset.
func (h *redisCommands) total() int {
	total := 0
	for _, n := range h.counts() {
		total += n
	}
	return total
}

// roundTrips is the number of round trips counted since the last reset.
func (h *redisCommands) roundTrips() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.trips
}

// sandboxStub is the instance of a sandbox stub, "stub", on its own Redis,
// with the commands that Redis receives counted.
type sandboxStub struct {
	instance *podInstance
	repo     repository.ContainerRepository
	rdb      *common.RedisClient
	commands *redisCommands
}

// newSandboxStub builds the instance of a sandbox stub whose sandboxes are
// kept for keepWarmSeconds, as createPodInstance does, without its proxy and
// without a scheduler: a test that expects a sandbox to be stopped gives
// the instance its own StopContainersFunc. The instance fails the test if
// it starts a container.
func newSandboxStub(t *testing.T, keepWarmSeconds int) *sandboxStub {
	t.Helper()
	rdb, err := repository.NewRedisClientForTest()
	require.NoError(t, err)
	repo := repository.NewContainerRedisRepositoryForTest(rdb)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	instance := &podInstance{}
	instance.AutoscaledInstance = &abstractions.AutoscaledInstance{
		Ctx:             ctx,
		CancelFunc:      cancel,
		Rdb:             rdb,
		Lock:            common.NewRedisLock(rdb),
		InstanceLockKey: Keys.podInstanceLock("workspace", "stub"),
		IsActive:        true,
		Workspace:       &types.Workspace{Name: "workspace", ExternalId: "workspace-id"},
		Stub:            &types.StubWithRelated{Stub: types.Stub{ExternalId: "stub", Type: types.StubType(types.StubTypeSandbox)}},
		StubConfig:      &types.StubConfigV1{KeepWarmSeconds: keepWarmSeconds},
		ContainerRepo:   repo,
		ScaleEventChan:  make(chan int, 1),
		StartContainersFunc: func(int) error {
			t.Error("the autoscaler started a sandbox")
			return nil
		},
		StopContainersFunc:      instance.stopContainers,
		StoppableContainersFunc: instance.stoppableNow,
	}

	commands := &redisCommands{}
	rdb.AddHook(commands)
	return &sandboxStub{instance: instance, repo: repo, rdb: rdb, commands: commands}
}

// run records count running sandboxes of the stub, each with the keep-warm
// lock it is given when it is created, and returns their ids.
func (s *sandboxStub) run(t *testing.T, count int) []string {
	t.Helper()
	containerIDs := make([]string, count)
	for n := range count {
		containerID := fmt.Sprintf("sandbox-stub-%04d", n)
		containerIDs[n] = containerID
		require.NoError(t, s.repo.SetContainerState(containerID, &types.ContainerState{
			ContainerId: containerID,
			StubId:      "stub",
			WorkspaceId: "workspace-id",
			Status:      types.ContainerStatusRunning,
		}))
		require.NoError(t, s.repo.SetPodKeepWarmLock(context.Background(), "workspace", "stub", containerID, s.instance.StubConfig.KeepWarmSeconds))
	}
	return containerIDs
}

// sample is the first half of a tick of the stub's autoscaler, as
// Autoscaler.Start runs it: the sample and the scale result, which queues a
// scaling event.
func (s *sandboxStub) sample(t *testing.T) {
	t.Helper()
	sample, err := podAutoscalerSampleFunc(s.instance)
	require.NoError(t, err)
	result := podScaleFunc(s.instance, sample)
	require.True(t, result.ResultValid)
	s.instance.ConsumeScaleResult(result)
}

// scale is the second half, as Monitor runs it: the scaling event.
func (s *sandboxStub) scale(t *testing.T) {
	t.Helper()
	require.NoError(t, s.instance.HandleScalingEvent(<-s.instance.ScaleEventChan))
}

// tick runs one tick of the stub's autoscaler end to end.
func (s *sandboxStub) tick(t *testing.T) {
	t.Helper()
	s.sample(t)
	s.scale(t)
}

// stops has the stub record what a scaling event that stops sandboxes
// would choose from, instead of stopping them: the containers
// stopContainers picks among, and whether the instance lock is held then.
type stops struct {
	asked     int
	locked    bool
	stoppable []string
}

func (s *sandboxStub) recordStops(t *testing.T) *stops {
	t.Helper()
	recorded := &stops{}
	s.instance.StopContainersFunc = func(containers int) error {
		recorded.asked += containers
		held, err := s.rdb.Exists(context.Background(), s.instance.InstanceLockKey).Result()
		require.NoError(t, err)
		recorded.locked = held == 1
		recorded.stoppable, err = s.instance.stoppableContainers()
		return err
	}
	return recorded
}

// A tick that changes nothing is what a stub costs while its sandboxes are
// held: every one of them running and within its keep-warm time.
//
// Before, it sent 7 commands and 5 for each sandbox, in 12 round trips: 12
// for a stub of one sandbox and 507 for a stub of a hundred. The sample read
// the stub (its index twice, each sandbox's state and exit code) and its
// proxies; then the scaling event took the instance lock, read the stub all
// over again, read each sandbox's keep-warm lock, and released the lock.
// The two lock scripts ran two commands each inside Redis on top of that.
//
// Now the event goes by the sample's read, and the sample reads the stub's
// index once, with every state and exit code in one round trip: 2 commands
// and 3 for each sandbox, in 4 round trips, and no lock.
func TestATickThatChangesNothingReadsEachSandboxOnceAndTakesNoLock(t *testing.T) {
	for _, test := range []struct{ sandboxes, before, after int }{
		{sandboxes: 1, before: 12, after: 5},
		{sandboxes: 100, before: 507, after: 302},
	} {
		t.Run(fmt.Sprintf("%d sandboxes", test.sandboxes), func(t *testing.T) {
			stub := newSandboxStub(t, 600)
			stub.run(t, test.sandboxes)
			stub.tick(t)
			stub.commands.reset()
			stub.tick(t)

			require.Equal(t, map[string]int{
				"smembers": 2,                  // the stub's index and its proxies
				"hgetall":  test.sandboxes,     // each sandbox's state
				"get":      2 * test.sandboxes, // each sandbox's exit code and keep-warm lock
			}, stub.commands.counts())
			require.Equal(t, test.after, stub.commands.total())
			require.Equal(t, 4, stub.commands.roundTrips(), "round trips")
			require.Less(t, 100*stub.commands.total(), 60*test.before, "at most 60% of the commands it took before")
		})
	}
}

func TestASandboxPastItsKeepWarmTimeIsStoppedByTheNextTickUnderTheLock(t *testing.T) {
	stub := newSandboxStub(t, 600)
	sandboxes := stub.run(t, 3)
	stopped := stub.recordStops(t)

	// While all three are within their keep-warm time, nothing is stopped.
	stub.tick(t)
	require.Zero(t, stopped.asked)

	// The second one's runs out. The stub is always asked for none of its
	// sandboxes, and the next tick finds the one that may now be stopped.
	require.NoError(t, stub.repo.SetPodKeepWarmLock(context.Background(), "workspace", "stub", sandboxes[1], 0))
	stub.commands.reset()
	stub.tick(t)
	require.Equal(t, 3, stopped.asked)
	require.True(t, stopped.locked, "sandboxes were chosen to be stopped without the instance lock")
	require.Equal(t, []string{sandboxes[1]}, stopped.stoppable)
	require.Equal(t, 2, stub.commands.counts()["evalsha"], "the lock, taken and released")
}

// Every gateway samples the stub, so two of them find the same sandbox past
// its keep-warm time. The one that gets to it second must see that.
func TestASandboxStoppedSinceTheSampleIsNotChosenAgain(t *testing.T) {
	stub := newSandboxStub(t, 600)
	sandboxes := stub.run(t, 3)
	stopped := stub.recordStops(t)
	require.NoError(t, stub.repo.SetPodKeepWarmLock(context.Background(), "workspace", "stub", sandboxes[1], 0))

	// This gateway's sample finds the sandbox running. Another gateway
	// stops it before this one's scaling event runs.
	stub.sample(t)
	require.NoError(t, stub.repo.UpdateContainerStatus(sandboxes[1], types.ContainerStatusStopping, 60))
	stub.scale(t)

	require.Equal(t, 2, stopped.asked, "the two sandboxes still running, as read under the lock")
	require.True(t, stopped.locked)
	require.Empty(t, stopped.stoppable, "chose from the sample, not from the read under the lock")
}

// A lock that cannot be read stops nothing, on this path as on the old one.
// It is not a stub with nothing to stop, though: its scaling event takes the
// lock as before, so the stub is not left to wait its turn among the
// gateway's stubs with a sandbox that may be past its time.
func TestAStubWhoseKeepWarmLocksCannotBeReadIsNotTakenToBeHoldingStill(t *testing.T) {
	stub := newSandboxStub(t, 600)
	sandboxes := stub.run(t, 2)
	stopped := stub.recordStops(t)

	// A lock that is not a number: reading it fails.
	require.NoError(t, stub.rdb.Set(context.Background(), Keys.podKeepWarmLock("workspace", "stub", sandboxes[0]), "unreadable", 0).Err())
	stub.tick(t)
	require.Equal(t, 2, stopped.asked, "the scaling event did not reach the lock")
	require.Empty(t, stopped.stoppable)
}
