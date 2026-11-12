package abstractions

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

// newTestSandboxInstance builds the instance of a sandbox stub, "test-stub",
// on its own Redis. A sandbox's failures are reported but never stop its
// stub, and its autoscaler starts no sandbox: it only stops them.
func newTestSandboxInstance(t *testing.T) (*AutoscaledInstance, repository.ContainerRepository, *miniredis.Miniredis) {
	t.Helper()
	instance, repo, server := newTestAutoscaledInstance(t, true, nil)
	instance.Stub.Type = types.StubType(types.StubTypeSandbox)
	instance.StubConfig = &types.StubConfigV1{}
	return instance, repo, server
}

// hold gives the instance a read that found these containers, as State does.
func hold(instance *AutoscaledInstance, state AutoscaledInstanceState) {
	containers := state.RunningContainers + state.PendingContainers + state.StoppingContainers
	instance.containersMu.Lock()
	defer instance.containersMu.Unlock()
	instance.containers = stubContainers{active: make([]types.ContainerState, containers), state: state, readAt: time.Now()}
	instance.countContainers(containers)
}

// gatewayHolds has other instances of this process hold this many containers
// until the test ends. The containers of the test's own instances add to them.
func gatewayHolds(t *testing.T, containers int) {
	t.Helper()
	// Instances of earlier tests give theirs back when they are cancelled.
	require.Eventually(t, func() bool { return gatewayContainers.Load() == 0 }, 5*time.Second, time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	hold(&AutoscaledInstance{Ctx: ctx}, AutoscaledInstanceState{RunningContainers: containers})
}

// settle is the scaling event of a sampled tick finding nothing to do, with
// the next sample as late as the pace allows.
func settle(instance *AutoscaledInstance) {
	instance.markSettled(true)
	instance.containersMu.Lock()
	instance.autoscalerEarly = 0
	instance.containersMu.Unlock()
}

// Before, this event took the instance lock and read the stub a second
// time: two scripts, the stub's index twice, and every container's state and
// exit code, on every tick of every stub.
func TestAScalingEventThatChangesNothingTakesNoLockAndReadsNothing(t *testing.T) {
	instance, repo, server := newTestAutoscaledInstance(t, true, nil)
	seedTestContainer(t, repo, types.ContainerStatusRunning)
	started := countStarts(t, instance)

	// The autoscaler samples the stub, and the event its sample leads to
	// finds the one container that is wanted running.
	_, err := instance.State()
	require.NoError(t, err)
	before := server.CommandCount()
	require.NoError(t, instance.HandleScalingEvent(1))
	require.Zero(t, server.CommandCount()-before, "commands for a scaling event that changes nothing")
	require.Zero(t, *started)

	// An event that does not follow a sample has nothing to go by. It takes
	// the lock and reads the stub under it, as before.
	instance.containers.readAt = time.Now().Add(-2 * containersJustReadFor)
	before = server.CommandCount()
	require.NoError(t, instance.HandleScalingEvent(1))
	require.Positive(t, server.CommandCount()-before)
	require.Zero(t, *started)
}

func TestAScalingEventThatActsDecidesFromAReadTakenUnderTheLock(t *testing.T) {
	another := &types.ContainerState{ContainerId: "pod-test-stub-11111111", StubId: "test-stub", WorkspaceId: "test-workspace", Status: types.ContainerStatusRunning}

	t.Run("it starts the container a stub is short of", func(t *testing.T) {
		instance, repo, server := newTestAutoscaledInstance(t, true, nil)
		seedTestContainer(t, repo, types.ContainerStatusRunning)
		started, locked := 0, false
		instance.StartContainersFunc = func(containers int) error {
			started, locked = started+containers, server.Exists(instance.InstanceLockKey)
			return nil
		}

		_, err := instance.State()
		require.NoError(t, err)
		require.NoError(t, instance.HandleScalingEvent(2))
		require.Equal(t, 1, started)
		require.True(t, locked, "the container was started without the instance lock")
	})

	t.Run("it does not start one that another gateway started since the sample", func(t *testing.T) {
		instance, repo, _ := newTestAutoscaledInstance(t, true, nil)
		seedTestContainer(t, repo, types.ContainerStatusRunning)
		started := countStarts(t, instance)

		_, err := instance.State()
		require.NoError(t, err)
		require.NoError(t, repo.SetContainerState(another.ContainerId, another))
		require.NoError(t, instance.HandleScalingEvent(2))
		require.Zero(t, *started, "decided from the sample, not from the read under the lock")
	})

	t.Run("it stops a stub whose containers fail", func(t *testing.T) {
		instance, repo, server := newTestAutoscaledInstance(t, true, nil)
		instance.FailedContainerThreshold = 1
		instance.EventRepo = &stubStateEvents{states: make(chan string, 1)}
		seedTestContainer(t, repo, types.ContainerStatusRunning)
		seedFailedContainers(t, repo, 1)
		stopped, locked := 0, false
		instance.StopContainersFunc = func(containers int) error {
			stopped, locked = stopped+containers, server.Exists(instance.InstanceLockKey)
			return nil
		}

		// One container is wanted and one is running, but one has failed.
		_, err := instance.State()
		require.NoError(t, err)
		require.NoError(t, instance.HandleScalingEvent(1))
		require.Equal(t, 1, stopped)
		require.True(t, locked, "the container was stopped without the instance lock")
	})

	t.Run("it stops the containers of an inactive stub", func(t *testing.T) {
		instance, repo, _ := newTestAutoscaledInstance(t, false, nil)
		seedTestContainer(t, repo, types.ContainerStatusRunning)
		stopped := 0
		instance.StopContainersFunc = func(containers int) error {
			stopped += containers
			return nil
		}

		_, err := instance.State()
		require.NoError(t, err)
		require.NoError(t, instance.HandleScalingEvent(1))
		require.Equal(t, 1, stopped)
	})

	t.Run("it retires an instance without containers", func(t *testing.T) {
		instance, _, _ := newTestAutoscaledInstance(t, true, nil)

		_, err := instance.State()
		require.NoError(t, err)
		require.NoError(t, instance.HandleScalingEvent(0))
		require.ErrorIs(t, instance.Ctx.Err(), context.Canceled)
	})

	t.Run("it does not retire one that was given a container since the sample", func(t *testing.T) {
		instance, repo, _ := newTestAutoscaledInstance(t, true, nil)
		stopped := 0
		instance.StopContainersFunc = func(containers int) error {
			stopped += containers
			return nil
		}

		_, err := instance.State()
		require.NoError(t, err)
		require.NoError(t, repo.SetContainerState(another.ContainerId, another))
		require.NoError(t, instance.HandleScalingEvent(0))
		require.NoError(t, instance.Ctx.Err(), "retired an instance that has a container")
		require.Equal(t, 1, stopped)
	})
}

// A sandbox that fails stays among its stub's failed containers for an hour,
// and a sandbox stub is never stopped for its failures. Were they to send it
// to the lock, one crash would cost the stub an hour of what this saves.
func TestAFailedSandboxIsReportedByAnEventThatTakesNoLock(t *testing.T) {
	instance, repo, server := newTestSandboxInstance(t)
	events := &stubStateEvents{states: make(chan string, 1)}
	instance.EventRepo = events
	seedTestContainer(t, repo, types.ContainerStatusRunning)
	seedFailedContainers(t, repo, 1)
	countStarts(t, instance)

	_, err := instance.State()
	require.NoError(t, err)
	before := server.CommandCount()
	require.NoError(t, instance.HandleScalingEvent(1))
	select {
	case state := <-events.states:
		require.Equal(t, types.StubStateWarning, state)
	case <-time.After(5 * time.Second):
		t.Fatal("the failed sandbox was not reported")
	}
	// The stub's state is read, and written because it changed.
	require.Equal(t, 2, server.CommandCount()-before, "commands for the event of a stub with a failed sandbox")
}

// Where failures do stop a stub, the read under the lock counts them. One
// failed container of a stub that tolerates three is not nothing to do, and
// the stub is sampled on every tick for as long as it has one.
func TestAFailedContainerThatCountsTowardsStoppingItsStubSendsTheEventToTheLock(t *testing.T) {
	gatewayHolds(t, 1_000_000)
	instance, repo, server := newTestAutoscaledInstance(t, true, nil)
	instance.Stub.Type = types.StubType(types.StubTypePodRun)
	instance.FailedContainerThreshold = types.FailedDeploymentContainerThreshold
	instance.EventRepo = &stubStateEvents{states: make(chan string, 1)}
	seedTestContainer(t, repo, types.ContainerStatusRunning)
	seedFailedContainers(t, repo, 1)
	countStarts(t, instance)

	start := time.Now()
	for tick := range 3 {
		require.True(t, instance.autoscalerTickDue(start.Add(time.Duration(tick)*sampleRate)), "tick %d", tick)

		before := server.CommandCount()
		_, err := instance.State()
		require.NoError(t, err)
		sample := server.CommandCount() - before
		require.NoError(t, instance.HandleScalingEvent(1))
		// The lock, taken and released, around a read of the stub.
		require.GreaterOrEqual(t, server.CommandCount()-before, sample+2+sample, "commands for the tick")
	}
}

func TestASandboxStubHoldingStillIsSampledAtTheGatewaysPace(t *testing.T) {
	start := time.Now()
	tick := func(n int) time.Time { return start.Add(time.Duration(n) * sampleRate) }

	t.Run("as often as one stub of all the gateway's containers", func(t *testing.T) {
		// With this stub's sandbox the gateway holds 10,000 containers: five
		// times what an autoscaler samples a second.
		gatewayHolds(t, 5*autoscalerContainersPerSecond-1)
		sandbox, repo, _ := newTestSandboxInstance(t)
		seedTestContainer(t, repo, types.ContainerStatusRunning)
		countStarts(t, sandbox)
		// What follows a tick that is due: the sample reads the stub, and
		// its scaling event finds the one sandbox that is wanted running.
		sampleAndScale := func() {
			_, err := sandbox.State()
			require.NoError(t, err)
			require.NoError(t, sandbox.HandleScalingEvent(1))
			sandbox.autoscalerEarly = 0
		}

		// Every tick is due until the scaling event of one finds nothing to do.
		require.True(t, sandbox.autoscalerTickDue(tick(0)))
		require.True(t, sandbox.autoscalerTickDue(tick(1)))
		sampleAndScale()

		// Then the stub waits its turn: every fifth tick.
		for n := 2; n <= 5; n++ {
			require.False(t, sandbox.autoscalerTickDue(tick(n)), "tick %d", n)
		}
		require.True(t, sandbox.autoscalerTickDue(tick(6)))

		// Each sample has to find nothing to do again. Until its event has,
		// as when the sample fails, the next tick is due.
		require.True(t, sandbox.autoscalerTickDue(tick(7)))
		sampleAndScale()
		require.False(t, sandbox.autoscalerTickDue(tick(8)))
	})

	t.Run("at least every thirty seconds, however much the gateway holds", func(t *testing.T) {
		gatewayHolds(t, 1_000_000)
		sandbox, _, _ := newTestSandboxInstance(t)
		hold(sandbox, AutoscaledInstanceState{RunningContainers: 1})

		require.True(t, sandbox.autoscalerTickDue(tick(0)))
		settle(sandbox)
		require.False(t, sandbox.autoscalerTickDue(tick(29)))
		require.True(t, sandbox.autoscalerTickDue(tick(30)))

		// A turn is taken up to a fifth early, so that stubs which came due
		// together drift apart, and never late.
		settle(sandbox)
		sandbox.autoscalerEarly = 0.2
		require.False(t, sandbox.autoscalerTickDue(tick(30+23)))
		require.True(t, sandbox.autoscalerTickDue(tick(30+24)))
	})

	t.Run("and a pod run too", func(t *testing.T) {
		gatewayHolds(t, 1_000_000)
		run, _, _ := newTestSandboxInstance(t)
		run.Stub.Type = types.StubType(types.StubTypePodRun)
		hold(run, AutoscaledInstanceState{RunningContainers: 1})

		require.True(t, run.autoscalerTickDue(tick(0)))
		settle(run)
		require.False(t, run.autoscalerTickDue(tick(1)))
	})

	// These are sampled on every tick as before, whatever the gateway holds
	// and though their last scaling event found nothing to do.
	for name, test := range map[string]struct {
		stubType string
		state    AutoscaledInstanceState
	}{
		"but not an endpoint, which starts containers when a sample finds requests": {
			types.StubTypeEndpointDeployment, AutoscaledInstanceState{RunningContainers: 1},
		},
		"nor a task queue, which starts them when a sample finds tasks": {
			types.StubTypeTaskQueueDeployment, AutoscaledInstanceState{RunningContainers: 1},
		},
		"nor a pod deployment, which starts them when a sample finds connections": {
			types.StubTypePodDeployment, AutoscaledInstanceState{RunningContainers: 1},
		},
		"nor a sandbox stub with a sandbox starting": {
			types.StubTypeSandbox, AutoscaledInstanceState{RunningContainers: 1, PendingContainers: 1},
		},
		"nor a sandbox stub with a sandbox stopping": {
			types.StubTypeSandbox, AutoscaledInstanceState{RunningContainers: 1, StoppingContainers: 1},
		},
	} {
		t.Run(name, func(t *testing.T) {
			gatewayHolds(t, 1_000_000)
			instance, _, _ := newTestSandboxInstance(t)
			instance.Stub.Type = types.StubType(test.stubType)
			hold(instance, test.state)

			for n := range 3 {
				require.True(t, instance.autoscalerTickDue(tick(n)), "tick %d", n)
				settle(instance)
			}
		})
	}
}

// Before, a stub's pace went by its own size alone: stubs of one sandbox each
// were each sampled once a second, however many of them there were.
func TestManySmallStubsAreSampledBetweenThemAtTheRateOfOneLargeStub(t *testing.T) {
	const (
		stubs    = 5 * autoscalerContainersPerSecond
		interval = 5 // ticks: what a stub of that many containers waits
		ticks    = 60
	)
	gatewayHolds(t, 0)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	sandboxes := make([]*AutoscaledInstance, stubs)
	for n := range sandboxes {
		sandboxes[n] = &AutoscaledInstance{
			Ctx:  ctx,
			Stub: &types.StubWithRelated{Stub: types.Stub{Type: types.StubType(types.StubTypeSandbox)}},
		}
		hold(sandboxes[n], AutoscaledInstanceState{RunningContainers: 1})
	}
	require.EqualValues(t, stubs, gatewayContainers.Load())

	// Every stub's autoscaler ticks once a second, and the scaling event of
	// each sample finds every sandbox within its keep-warm time.
	start := time.Now()
	samples := make([]int, ticks+1)
	sampledAt := make([]int, stubs)
	longestWait := 0
	for tick := 1; tick <= ticks; tick++ {
		now := start.Add(time.Duration(tick) * sampleRate)
		for n, sandbox := range sandboxes {
			if !sandbox.autoscalerTickDue(now) {
				continue
			}
			samples[tick]++
			if sampledAt[n] > 0 {
				longestWait = max(longestWait, tick-sampledAt[n])
			}
			sampledAt[n] = tick
			sandbox.markSettled(true)
		}
	}

	// A new stub is sampled on its first tick, as before.
	require.Equal(t, stubs, samples[1])

	// From then on the stubs take turns. Turns are taken up to a fifth
	// early, so a tick samples a little over a fifth of the stubs: about
	// 2,200 where there were 10,000.
	settled := 0
	for tick := ticks/2 + 1; tick <= ticks; tick++ {
		settled += samples[tick]
	}
	perTick := float64(settled) / float64(ticks/2)
	require.GreaterOrEqual(t, perTick, float64(stubs)/interval, "samples a tick")
	require.LessOrEqual(t, perTick, 1.25*float64(stubs)/interval, "samples a tick")

	// No tick takes most of them at once, though they all began together.
	for tick := 2; tick <= ticks; tick++ {
		require.LessOrEqual(t, samples[tick], 2*stubs/interval, "samples on tick %d", tick)
	}

	// And no stub waits longer than one stub of 10,000 containers does.
	require.LessOrEqual(t, longestWait, interval, "ticks between two samples of a stub")
}

func TestACancelledInstanceGivesItsContainersBackToTheGateway(t *testing.T) {
	gatewayHolds(t, 0)
	instance, repo, _ := newTestAutoscaledInstance(t, true, nil)
	seedTestContainer(t, repo, types.ContainerStatusRunning)

	// The gateway holds what each instance last read: counted once, however
	// often it reads, and counted again when it changes.
	for range 3 {
		_, err := instance.State()
		require.NoError(t, err)
	}
	require.EqualValues(t, 1, gatewayContainers.Load())

	require.NoError(t, repo.SetContainerState("pod-test-stub-11111111", &types.ContainerState{
		ContainerId: "pod-test-stub-11111111",
		StubId:      "test-stub",
		WorkspaceId: "test-workspace",
		Status:      types.ContainerStatusPending,
	}))
	_, err := instance.State()
	require.NoError(t, err)
	require.EqualValues(t, 2, gatewayContainers.Load())

	// An instance that is retired no longer slows the others down.
	instance.CancelFunc()
	require.Eventually(t, func() bool { return gatewayContainers.Load() == 0 }, 5*time.Second, time.Millisecond)

	// Nor does a read it was in the middle of.
	_, err = instance.State()
	require.NoError(t, err)
	require.Zero(t, gatewayContainers.Load())
}

func TestAContainerThatComesOrGoesReturnsItsStubToEveryTick(t *testing.T) {
	gatewayHolds(t, 1_000_000)
	sandbox, repo, _ := newTestSandboxInstance(t)
	seedTestContainer(t, repo, types.ContainerStatusRunning)
	sandbox.Containers = map[string]bool{"pod-test-stub-00000000": true}
	sandbox.ContainerEventChan = make(chan types.ContainerEvent)
	sandbox.Autoscaler = &monitorTestAutoscaler{started: make(chan struct{})}
	done := make(chan struct{})
	go func() { defer close(done); _ = sandbox.Monitor() }()
	t.Cleanup(func() { sandbox.CancelFunc(); <-done })
	send := func(event types.ContainerEvent) {
		t.Helper()
		select {
		case sandbox.ContainerEventChan <- event:
		case <-time.After(5 * time.Second):
			t.Fatal("the monitor stopped consuming container events")
		}
	}

	// The stub holds still, and its next turn is half a minute away.
	start := time.Now()
	_, err := sandbox.State()
	require.NoError(t, err)
	require.True(t, sandbox.autoscalerTickDue(start))
	settle(sandbox)
	require.False(t, sandbox.autoscalerTickDue(start.Add(sampleRate)))

	// News of a container it already tracks changes nothing.
	send(types.ContainerEvent{ContainerId: "pod-test-stub-00000000", Change: 1})
	send(types.ContainerEvent{ContainerId: "pod-test-stub-00000000", Change: 1})
	require.False(t, sandbox.autoscalerTickDue(start.Add(sampleRate)))

	// A sandbox that exits, as one that fails does, is looked at on the
	// next tick. Reaching the second receive proves the first was handled.
	send(types.ContainerEvent{ContainerId: "pod-test-stub-00000000", Change: -1})
	send(types.ContainerEvent{ContainerId: "pod-test-stub-00000000", Change: -1})
	require.True(t, sandbox.autoscalerTickDue(start.Add(sampleRate)))

	// So is one that is created.
	settle(sandbox)
	require.False(t, sandbox.autoscalerTickDue(start.Add(2*sampleRate)))
	send(types.ContainerEvent{ContainerId: "pod-test-stub-11111111", Change: 1})
	send(types.ContainerEvent{ContainerId: "pod-test-stub-11111111", Change: 1})
	require.True(t, sandbox.autoscalerTickDue(start.Add(2*sampleRate)))
}

func TestATriggerSamplesAStubThatIsWaitingItsTurn(t *testing.T) {
	gatewayHolds(t, 1_000_000)
	sandbox, _, _ := newTestSandboxInstance(t)
	hold(sandbox, AutoscaledInstanceState{RunningContainers: 1})
	require.True(t, sandbox.AutoscalerTickDue())
	settle(sandbox)

	samples := make(chan struct{}, 8)
	autoscaler := NewAutoscaler(sandbox, func(*AutoscaledInstance) (int, error) {
		samples <- struct{}{}
		return 0, nil
	}, func(*AutoscaledInstance, int) *AutoscalerResult { return nil })
	go autoscaler.Start(sandbox.Ctx)

	// Its ticks pass without a sample: its turn is half a minute away.
	select {
	case <-samples:
		t.Fatal("a stub that is holding still was sampled before its turn")
	case <-time.After(2*sampleRate + sampleRate/2):
	}

	// A request cannot wait that long, and does not.
	autoscaler.Trigger()
	select {
	case <-samples:
	case <-time.After(sampleRate / 2):
		t.Fatal("a trigger was not sampled")
	}
}
