//go:build !linux

package worker

import (
	"errors"

	"github.com/vishvananda/netlink"
)

func attachToContainerBridge(veth, bridge netlink.Link) error {
	return errors.ErrUnsupported
}
