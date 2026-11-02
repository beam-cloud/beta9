//go:build linux

package worker

import "github.com/vishvananda/netlink"

// attachToContainerBridge makes a container's veth an isolated bridge port: it
// reaches the worker, which filters and routes its traffic, but no other port.
func attachToContainerBridge(veth, bridge netlink.Link) error {
	if err := netlink.LinkSetMaster(veth, bridge); err != nil {
		return err
	}
	return netlink.LinkSetIsolated(veth, true)
}
