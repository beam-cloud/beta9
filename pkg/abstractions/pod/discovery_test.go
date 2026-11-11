package pod

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync/atomic"
	"testing"
	"time"

	abstractions "github.com/beam-cloud/beta9/pkg/abstractions/common"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

// discoveredStub is the proxy of a stub, "stub", on its own Redis, with the
// commands that Redis receives counted. Its containers accept connections.
type discoveredStub struct {
	proxy     *PodProxyBuffer
	repo      repository.ContainerRepository
	rdb       *common.RedisClient
	commands  *redisCommands
	addresses []string
	// How many times a container was dialled to see whether it is ready.
	dials atomic.Int64
}

func newDiscoveredStub(t *testing.T) *discoveredStub {
	t.Helper()
	stub := &discoveredStub{}
	// The containers share a few addresses. Few enough of them share one
	// that a pass which dials them all never fills its accept queue.
	stub.addresses = make([]string, 16)
	for n := range stub.addresses {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		t.Cleanup(func() { listener.Close() })
		stub.addresses[n] = listener.Addr().String()
		go func() {
			for {
				conn, err := listener.Accept()
				if err != nil {
					return
				}
				stub.dials.Add(1)
				conn.Close()
			}
		}()
	}

	stub.rdb = newPodProxyTestRedis(t)
	stub.repo = repository.NewContainerRedisRepositoryForTest(stub.rdb)
	stub.commands = &redisCommands{}
	stub.rdb.AddHook(stub.commands)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	stub.proxy = &PodProxyBuffer{
		ctx:           ctx,
		rdb:           stub.rdb,
		workspace:     &types.Workspace{Name: "workspace"},
		stubId:        "stub",
		proxyId:       "this-gateway",
		stubConfig:    &types.StubConfigV1{},
		containerRepo: stub.repo,
		buffer:        abstractions.NewRingBuffer[*connection](2),
		workReady:     make(chan struct{}, 1),
		discoverReady: make(chan struct{}, 1),
	}
	return stub
}

// unreadableAddresses is a container repository that cannot read address
// maps while it is failing, as when Redis is away for a moment.
type unreadableAddresses struct {
	repository.ContainerRepository
	failing atomic.Bool
}

func (r *unreadableAddresses) GetContainerAddressMaps(containerIDs []string) (map[string]map[int32]string, error) {
	if r.failing.Load() {
		return map[string]map[int32]string{}, errors.New("redis is away")
	}
	return r.ContainerRepository.GetContainerAddressMaps(containerIDs)
}

// run records count running containers of the stub and returns their ids.
// With addresses, each is given an address that accepts connections.
func (s *discoveredStub) run(t *testing.T, count int, addresses bool) []string {
	t.Helper()
	containerIDs := make([]string, count)
	for n := range count {
		containerID := fmt.Sprintf("sandbox-stub-%04d", n)
		containerIDs[n] = containerID
		require.NoError(t, s.repo.SetContainerState(containerID, &types.ContainerState{
			ContainerId: containerID,
			StubId:      "stub",
			WorkspaceId: "workspace",
			Status:      types.ContainerStatusRunning,
		}))
		if addresses {
			require.NoError(t, s.repo.SetContainerAddressMap(containerID, map[int32]string{8080: s.addresses[n%len(s.addresses)]}))
		}
	}
	return containerIDs
}

// pass waits for discovery to finish a pass. A stub that routes nothing is
// discovered every thirty seconds whatever happens; a pass sooner than
// that is one that was asked for.
func (s *discoveredStub) pass(t *testing.T) {
	t.Helper()
	select {
	case <-s.proxy.workReady:
	case <-time.After(idleContainerDiscoveryInterval / 2):
		t.Fatal("discovery did not finish a pass")
	}
}

// available is the ids of the containers discovery last found.
func (s *discoveredStub) available() []string {
	containerIDs := []string{}
	for _, c := range s.proxy.availableContainerSnapshot() {
		containerIDs = append(containerIDs, c.id)
	}
	return containerIDs
}

// Before, a pass read the stub's containers and then, from a goroutine for
// each running one, its addresses and the stub's proxies: 1 command and 3
// for each container, in 2 round trips and 2 for each container. That is
// 301 commands in 202 round trips for a stub of 100 containers, and 2,001
// round trips, started together, for one of 1,000.
//
// Now a pass is 2 commands and 2 for each container, in 4 round trips.
func TestADiscoveryPassReadsAStubOfAnySizeInTheSameRoundTrips(t *testing.T) {
	for _, containers := range []int{1, 100, 1000} {
		t.Run(fmt.Sprintf("%d containers", containers), func(t *testing.T) {
			stub := newDiscoveredStub(t)
			containerIDs := stub.run(t, containers, true)
			stub.commands.reset()

			go stub.proxy.discoverContainers()
			stub.pass(t)

			require.Equal(t, containerIDs, stub.available())
			require.Equal(t, map[string]int{
				"smembers": 2,          // the stub's index and its proxies
				"hgetall":  containers, // each container's state
				"get":      containers, // each container's addresses
			}, stub.commands.counts())
			require.Equal(t, 4, stub.commands.roundTrips(), "round trips")
		})
	}
}

func TestADiscoveryPassCountsTheConnectionsOtherGatewaysHold(t *testing.T) {
	stub := newDiscoveredStub(t)
	containerIDs := stub.run(t, 3, true)
	ctx := context.Background()

	// The first container has three connections: one through this gateway
	// and two through another. The last has one, through this gateway.
	require.NoError(t, stub.rdb.SAdd(ctx, Keys.podProxyConnectionIndex("workspace", "stub"), "this-gateway", "another-gateway").Err())
	require.NoError(t, stub.rdb.Set(ctx, Keys.podProxyConnections("workspace", "stub", "this-gateway", containerIDs[0]), 1, 0).Err())
	require.NoError(t, stub.rdb.Set(ctx, Keys.podProxyConnections("workspace", "stub", "another-gateway", containerIDs[0]), 2, 0).Err())
	require.NoError(t, stub.rdb.Set(ctx, Keys.podProxyConnections("workspace", "stub", "this-gateway", containerIDs[2]), 1, 0).Err())
	for _, containerID := range []string{containerIDs[0], containerIDs[2]} {
		connections, _ := stub.proxy.containerConnectionCounter(containerID)
		connections.Store(1)
	}
	stub.commands.reset()

	go stub.proxy.discoverContainers()
	stub.pass(t)

	remote := map[string]int{}
	for _, c := range stub.proxy.availableContainerSnapshot() {
		remote[c.id] = c.remoteConnections
	}
	require.Equal(t, map[string]int{containerIDs[0]: 2, containerIDs[1]: 0, containerIDs[2]: 0}, remote)
	// One more than for a stub without connections: the count each of the
	// two gateways holds on each container, read together.
	require.Equal(t, 5, stub.commands.roundTrips(), "round trips")
}

func TestADiscoveryPassLeavesOutOnlyTheContainersWhoseAddressesItCannotUse(t *testing.T) {
	stub := newDiscoveredStub(t)
	containerIDs := stub.run(t, 4, true)
	ctx := context.Background()

	// One container's addresses cannot be read, and another has none yet.
	require.NoError(t, stub.rdb.Set(ctx, common.RedisKeys.SchedulerContainerAddressMap(containerIDs[1]), "not an address map", 0).Err())
	require.NoError(t, stub.rdb.Del(ctx, common.RedisKeys.SchedulerContainerAddressMap(containerIDs[2])).Err())

	go stub.proxy.discoverContainers()
	stub.pass(t)
	require.Equal(t, []string{containerIDs[0], containerIDs[3]}, stub.available())
}

// A container is dialled the first time a pass sees it, and what the pass
// learns is kept. A pass that forgot it whenever Redis was away for a
// moment would have the next one dial every container of the stub again.
func TestAPassThatCannotReadAddressesKeepsWhatIsKnownOfEachContainersReadiness(t *testing.T) {
	stub := newDiscoveredStub(t)
	containerIDs := stub.run(t, 5, true)
	repo := &unreadableAddresses{ContainerRepository: stub.repo}
	stub.proxy.containerRepo = repo

	require.Len(t, stub.proxy.readyContainers(containerIDs), 5)
	require.Eventually(t, func() bool { return stub.dials.Load() == 5 }, 5*time.Second, time.Millisecond)

	// No container is offered whose addresses could not be read, as before.
	repo.failing.Store(true)
	require.Empty(t, stub.proxy.readyContainers(containerIDs))

	// Once they can be read again, the containers are offered again, and
	// none has to be dialled to learn what the first pass learned.
	repo.failing.Store(false)
	require.Len(t, stub.proxy.readyContainers(containerIDs), 5)
	require.Never(t, func() bool { return stub.dials.Load() > 5 }, 200*time.Millisecond, 10*time.Millisecond)
}

// Each container of a stub that goes signals discovery, and each signal
// started a pass over all that were left. Deleting a stub's N sandboxes
// together read up to N containers N times.
func TestSignalsThatFollowAPassWaitForItsRestAndLeadToOnePass(t *testing.T) {
	stub := newDiscoveredStub(t)
	stub.run(t, 2000, false)
	require.Equal(t, time.Second, discoveryRest(2000))

	go stub.proxy.discoverContainers()
	stub.pass(t)
	stub.commands.reset()

	// Fifty signals in the tenth of a second after the pass start nothing.
	for range 50 {
		stub.proxy.signalDiscovery()
		time.Sleep(2 * time.Millisecond)
	}
	time.Sleep(100 * time.Millisecond)
	require.Zero(t, stub.commands.total(), "commands while the stub rests")

	// When the rest is over they have led to one pass, and to no more. The
	// pass reads the stub's index and each container's state and addresses;
	// none has an address yet, so there are no connections to read.
	stub.pass(t)
	onePass := stub.commands.total()
	require.Equal(t, 1+2*2000, onePass)
	time.Sleep(200 * time.Millisecond)
	require.Equal(t, onePass, stub.commands.total(), "commands after the one pass")
}

func TestASignalStartsAPassAtOnceWhenARequestIsWaitingOrTheRestIsOver(t *testing.T) {
	await := func(stub *discoveredStub, rest time.Duration) chan bool {
		next := make(chan bool, 1)
		go func() { next <- stub.proxy.awaitDiscovery(rest) }()
		return next
	}
	atOnce := func(t *testing.T, next chan bool) {
		t.Helper()
		select {
		case pass := <-next:
			require.True(t, pass)
		case <-time.After(5 * time.Second):
			t.Fatal("the signal did not start a pass")
		}
	}

	t.Run("a request is waiting for a container", func(t *testing.T) {
		stub := newDiscoveredStub(t)
		next := await(stub, time.Hour)

		// Without a request, a signal waits for the rest to end.
		stub.proxy.signalDiscovery()
		select {
		case <-next:
			t.Fatal("a signal during the rest started a pass with no request waiting")
		case <-time.After(100 * time.Millisecond):
		}

		// A request that finds no container is queued, and signals.
		stub.proxy.buffer.PushWithOverwrite(&connection{}, false)
		stub.proxy.signalDiscovery()
		atOnce(t, next)
	})

	t.Run("the rest is over, as a small stub's is in no time", func(t *testing.T) {
		require.Equal(t, 500*time.Microsecond, discoveryRest(1))
		require.Equal(t, 5*time.Millisecond, discoveryRest(10))
		require.Equal(t, idleContainerDiscoveryInterval, discoveryRest(1_000_000))

		stub := newDiscoveredStub(t)
		next := await(stub, discoveryRest(10))
		time.Sleep(50 * time.Millisecond)
		stub.proxy.signalDiscovery()
		atOnce(t, next)
	})

	t.Run("and the proxy going away ends discovery", func(t *testing.T) {
		stub := newDiscoveredStub(t)
		ctx, cancel := context.WithCancel(context.Background())
		stub.proxy.ctx = ctx
		next := await(stub, time.Hour)
		cancel()
		select {
		case pass := <-next:
			require.False(t, pass)
		case <-time.After(5 * time.Second):
			t.Fatal("discovery outlived its proxy")
		}
	})
}
