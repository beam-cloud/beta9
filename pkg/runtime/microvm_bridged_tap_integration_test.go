//go:build linux && microvm

package runtime

import (
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"

	"github.com/vishvananda/netlink"
	"github.com/vishvananda/netlink/nl"
	"golang.org/x/sys/unix"
)

// prepareBridgedTap stands in for plumbNetwork on hosts whose kernel has no
// tc mirred action (MICROVM_TEST_BRIDGED_TAP=1): the tap and the veth share a
// bridge inside the namespace instead of redirecting into each other. The
// guest still owns the veth's addresses and MAC, so the veth takes another
// MAC or the bridge would keep the guest's frames for itself. Test-only: it
// has none of the anti-spoof filters.
func prepareBridgedTap(rt *MicroVM, nsPath string) error {
	var prepared preparedNetwork
	network := &prepared.Network
	err := inNetworkNamespace(nsPath, func() error {
		veth, err := findVeth()
		if err != nil {
			return err
		}
		attrs := veth.Attrs()
		network.MAC = attrs.HardwareAddr.String()
		network.MTU = attrs.MTU
		v4, err := netlink.AddrList(veth, netlink.FAMILY_V4)
		if err != nil {
			return err
		}
		v6, err := netlink.AddrList(veth, netlink.FAMILY_V6)
		if err != nil {
			return err
		}
		if len(v4) == 0 {
			return fmt.Errorf("veth %s has no IPv4 address", attrs.Name)
		}
		network.IPv4 = v4[0].IPNet.String()
		for _, addr := range v6 {
			if !addr.IP.IsLinkLocalUnicast() {
				network.IPv6 = addr.IPNet.String()
				break
			}
		}
		network.Gateway4 = defaultGateway(veth, netlink.FAMILY_V4)
		if network.IPv6 != "" {
			network.Gateway6 = defaultGateway(veth, netlink.FAMILY_V6)
		}
		_ = netlink.LinkSetIP6AddrGenMode(veth, nl.IN6_ADDR_GEN_MODE_NONE)
		for _, addr := range append(v4, v6...) {
			if err := netlink.AddrDel(veth, &addr); err != nil && !errors.Is(err, unix.EADDRNOTAVAIL) {
				return err
			}
		}

		tap := &netlink.Tuntap{
			LinkAttrs: netlink.LinkAttrs{Name: microVMTapName, MTU: attrs.MTU},
			Mode:      netlink.TUNTAP_MODE_TAP,
			Flags:     netlink.TUNTAP_NO_PI | netlink.TUNTAP_VNET_HDR,
		}
		if err := netlink.LinkAdd(tap); err != nil && !errors.Is(err, unix.EEXIST) {
			return fmt.Errorf("create tap: %w", err)
		}
		tapLink, err := netlink.LinkByName(microVMTapName)
		if err != nil {
			return err
		}
		if err := netlink.LinkSetMTU(tapLink, network.MTU); err != nil {
			return err
		}
		_ = netlink.LinkSetIP6AddrGenMode(tapLink, nl.IN6_ADDR_GEN_MODE_NONE)
		prepared.TapIndex = tapLink.Attrs().Index

		mac := make(net.HardwareAddr, 6)
		if _, err := rand.Read(mac); err != nil {
			return err
		}
		mac[0] = mac[0]&0xfe | 0x02
		if err := netlink.LinkSetDown(veth); err != nil {
			return err
		}
		if err := netlink.LinkSetHardwareAddr(veth, mac); err != nil {
			return fmt.Errorf("re-address veth: %w", err)
		}
		bridge := &netlink.Bridge{LinkAttrs: netlink.LinkAttrs{Name: "b9sw0", MTU: attrs.MTU}}
		if err := netlink.LinkAdd(bridge); err != nil {
			return fmt.Errorf("create namespace bridge: %w", err)
		}
		_ = netlink.LinkSetIP6AddrGenMode(bridge, nl.IN6_ADDR_GEN_MODE_NONE)
		for _, link := range []netlink.Link{veth, tapLink} {
			if err := netlink.LinkSetMaster(link, bridge); err != nil {
				return err
			}
			if err := netlink.LinkSetUp(link); err != nil {
				return err
			}
		}
		return netlink.LinkSetUp(bridge)
	})
	if err != nil {
		return err
	}
	data, err := json.Marshal(prepared)
	if err != nil {
		return err
	}
	path := rt.preparedNetworkPath(nsPath)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o600)
}
