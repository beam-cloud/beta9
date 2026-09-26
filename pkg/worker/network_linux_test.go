//go:build linux

package worker

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/stretchr/testify/require"
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
