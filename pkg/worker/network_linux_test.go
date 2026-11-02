//go:build linux

package worker

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/opencontainers/runtime-spec/specs-go"
	"github.com/stretchr/testify/require"
	"github.com/vishvananda/netlink"
	"github.com/vishvananda/netns"
	"google.golang.org/grpc"
)

func TestCloseReleasesPooledSlotsTogether(t *testing.T) {
	repoClient := &slotPoolWorkerRepoClient{removeDelay: 10 * time.Millisecond}
	manager := &ContainerNetworkManager{
		ctx:                context.Background(),
		workerId:           "worker-a",
		workerRepoClient:   repoClient,
		containerInstances: common.NewSafeMap[*ContainerInstance](),
		allocatedIPs:       map[string]struct{}{},
		containerIPs:       map[string]string{},
		containerSlots:     map[string]*containerNetworkSlot{},
		slotDiscards:       make(chan pendingSlotDiscard, 1),
	}
	for i := 0; i < 2*networkSlotCleanupConcurrency; i++ {
		manager.freeSlots = append(manager.freeSlots, &containerNetworkSlot{id: fmt.Sprintf("slot-%d", i), namespace: "missing-netns", vethHost: "missing-veth", ip: fmt.Sprintf("192.168.0.%d", 10+i)})
	}
	manager.totalSlots = len(manager.freeSlots)
	manager.slotDiscards <- pendingSlotDiscard{slot: &containerNetworkSlot{id: "slot-used", namespace: "missing-netns", vethHost: "missing-veth", ip: "192.168.0.9"}}

	require.NoError(t, manager.Close())

	released := 0
	for _, call := range repoClient.calls {
		if strings.HasPrefix(call, "remove ") {
			released++
		}
	}
	require.Equal(t, 2*networkSlotCleanupConcurrency+1, released, "every free and pending slot is released")
	require.Greater(t, repoClient.peak, 1, "releases run concurrently")
	first, last := slices.MinFunc(repoClient.deadlines, time.Time.Compare), slices.MaxFunc(repoClient.deadlines, time.Time.Compare)
	require.GreaterOrEqual(t, last.Sub(first), repoClient.removeDelay, "later releases do not inherit an earlier deadline")
	require.True(t, manager.slotPoolClosed)
	require.Zero(t, manager.totalSlots)
}

func TestCleanupStaleNetworkSlotsReleasesTheLockBeforeDestroyingSlots(t *testing.T) {
	reservation := containerNetworkSlotReservationIDForWorker("worker-a", "slot-stale")
	repoClient := &slotPoolWorkerRepoClient{assignments: []*pb.ContainerIpAssignment{{ContainerId: reservation, IpAddress: "192.168.0.44"}}}
	manager := &ContainerNetworkManager{
		ctx:              context.Background(),
		workerId:         "worker-a",
		networkPrefix:    "node",
		workerRepoClient: repoClient,
		allocatedIPs:     map[string]struct{}{},
		containerIPs:     map[string]string{},
	}

	require.NoError(t, manager.cleanupStaleNetworkSlots())
	require.Equal(t, []string{"lock", "list", "unlock", "remove " + reservation}, repoClient.calls)
}

// unreachableIPWorkerRepoClient cannot list the container IPs in use, so no
// container IP can be reserved.
type unreachableIPWorkerRepoClient struct {
	pb.WorkerRepositoryServiceClient
}

func (unreachableIPWorkerRepoClient) GetContainerIps(context.Context, *pb.GetContainerIpsRequest, ...grpc.CallOption) (*pb.GetContainerIpsResponse, error) {
	return nil, errors.New("repository unavailable")
}

func TestSetupRemovesItsNetworkWhenItFails(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("creating network namespaces requires root")
	}
	for _, tc := range []struct {
		name string
		fail func(t *testing.T, containerId string)
	}{
		{"namespace", func(t *testing.T, containerId string) {
			// NewNamed fails on an existing path after moving the thread into a
			// new namespace, with the veth already on the bridge.
			require.NoError(t, os.MkdirAll(types.HostNetnsPath, 0o755))
			require.NoError(t, os.WriteFile(filepath.Join(types.HostNetnsPath, containerId), nil, 0o444))
		}},
		{"configuration", func(*testing.T, string) {}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			runtime.LockOSThread()
			defer runtime.UnlockOSThread()
			hostNS, err := netns.Get()
			require.NoError(t, err)
			defer hostNS.Close()
			testNS, err := netns.New()
			require.NoError(t, err)
			defer testNS.Close()
			defer netns.Set(hostNS)

			require.NoError(t, netlink.LinkAdd(&netlink.Bridge{LinkAttrs: netlink.LinkAttrs{Name: containerBridgeLinkName}}))
			bridge, err := netlink.LinkByName(containerBridgeLinkName)
			require.NoError(t, err)
			manager := &ContainerNetworkManager{
				ctx:              context.Background(),
				workerRepoClient: unreachableIPWorkerRepoClient{},
				defaultLink:      bridge,
				bridgeConfigured: true,
				bridgeLink:       bridge,
			}
			containerId := "container-" + randomNetworkSlotID()
			t.Cleanup(func() { _ = deleteNamedNetworkNamespace(containerId) })
			tc.fail(t, containerId)

			err = manager.Setup(containerId, &specs.Spec{Linux: &specs.Linux{}}, &types.ContainerRequest{ContainerId: containerId})
			require.Error(t, err)

			current, err := netns.Get()
			require.NoError(t, err)
			defer current.Close()
			require.True(t, current.Equal(testNS), "the thread is back in the worker's namespace")
			vethHost, _ := containerVethNames(containerId)
			var notFound netlink.LinkNotFoundError
			_, err = netlink.LinkByName(vethHost)
			require.ErrorAs(t, err, &notFound)
			require.NoFileExists(t, filepath.Join(types.HostNetnsPath, containerId))
		})
	}
}
