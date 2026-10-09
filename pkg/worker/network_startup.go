package worker

import (
	"context"

	pb "github.com/beam-cloud/beta9/proto"
	"github.com/rs/zerolog/log"
)

// A network claim holds an unused namespace locally. Its IP remains owned by
// the pool reservation until the gateway accepts delivery and moves that fence.
type networkClaim struct {
	manager     *ContainerNetworkManager
	slot        *containerNetworkSlot
	containerID string
	committed   bool
}

type networkClaimPreparer interface {
	prepareNetworkClaim(containerID string) *networkClaim
}

func (m *ContainerNetworkManager) prepareNetworkClaim(containerID string) *networkClaim {
	slot := m.acquireNetworkSlot()
	if slot == nil {
		return nil
	}
	claim := &networkClaim{manager: m, slot: slot, containerID: containerID}
	if err := m.prepareNetworkSlotForAssignment(slot); err != nil {
		// Normal setup can discard a drifted slot after the delivery claim.
		// Do not put namespace teardown on the acknowledgement path.
		claim.returnSlot()
		return nil
	}
	return claim
}

func (c *networkClaim) request() *pb.MoveContainerIpRequest {
	return &pb.MoveContainerIpRequest{
		NetworkPrefix: c.manager.networkPrefix, FromContainerId: c.manager.containerNetworkSlotReservationID(c.slot.id),
		ToContainerId: c.containerID, IpAddress: c.slot.ip,
	}
}

func (c *networkClaim) commit(prepared bool) error {
	m := c.manager
	if !prepared {
		// An older gateway accepted delivery but ignored the optional startup
		// fields. Preserve its existing assignment protocol.
		if err := m.assignPreallocatedNetworkSlot(c.containerID, c.slot); err != nil {
			return err
		}
	} else {
		m.ipMu.Lock()
		delete(m.containerIPs, m.containerNetworkSlotReservationID(c.slot.id))
		m.rememberContainerIPLocked(c.containerID, c.slot.ip)
		m.ipMu.Unlock()
	}
	m.slotMu.Lock()
	m.containerSlots[c.containerID] = c.slot
	m.slotMu.Unlock()
	c.committed = true
	return nil
}

func (c *networkClaim) close() {
	if c == nil || c.committed {
		return
	}
	m := c.manager
	ctx, cancel := context.WithTimeout(context.Background(), containerNetworkCleanupRPCTimeout)
	defer cancel()
	reservationID := m.containerNetworkSlotReservationID(c.slot.id)
	resp, err := m.workerRepoClient.GetContainerIp(ctx, &pb.GetContainerIpRequest{NetworkPrefix: m.networkPrefix, ContainerId: reservationID})
	if err == nil && resp.Ok && resp.IpAddress == c.slot.ip {
		c.returnSlot()
		return
	}
	// A lost claim reply may already have moved the IP. The reverse move
	// checks both its value and owner; it cannot remove another owner's IP.
	_, err = handleGRPCResponse(m.workerRepoClient.MoveContainerIp(ctx, &pb.MoveContainerIpRequest{
		NetworkPrefix: m.networkPrefix, FromContainerId: c.containerID, ToContainerId: reservationID, IpAddress: c.slot.ip,
	}))
	if err == nil {
		c.returnSlot()
		return
	}
	log.Warn().Err(err).Str("container_id", c.containerID).Str("network_slot", c.slot.id).Msg("unable to recover startup network reservation; retiring its namespace")
	// Keep the IP reserved when ownership could not be recovered. Releasing it
	// locally could let a later slot reuse an IP still owned by a container.
	if err := m.discardNetworkSlot("", c.slot, false); err != nil {
		log.Warn().Err(err).Str("network_slot", c.slot.id).Msg("retire startup network reservation")
	}
}

func (c *networkClaim) returnSlot() {
	m := c.manager
	m.slotMu.Lock()
	closed := m.slotPoolClosed
	if !closed {
		m.freeSlots = append(m.freeSlots, c.slot)
	}
	m.slotMu.Unlock()
	if closed {
		if err := m.discardNetworkSlot("", c.slot, true); err != nil {
			log.Warn().Err(err).Str("network_slot", c.slot.id).Msg("retire startup network reservation during shutdown")
		}
	}
}
