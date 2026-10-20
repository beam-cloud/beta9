package worker

import (
	"net"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/vishvananda/netlink"
)

func TestConntrackCapacityScalesWithHostAndPreservesLargerSettings(t *testing.T) {
	for _, tc := range []struct {
		name                string
		memoryGiB, existing uint64
		wantLimit, wantHash uint64
	}{
		{"small host", 32, 262144, 262144, 262144},
		{"gladiator", 512, 262144, 2097152, 1048576},
		{"operator override", 512, 4194304, 4194304, 2097152},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			for name, value := range map[string]uint64{"nf_conntrack_max": tc.existing, "nf_conntrack_buckets": 262144} {
				require.NoError(t, os.WriteFile(filepath.Join(root, name), []byte(strconv.FormatUint(value, 10)), 0o644))
			}
			require.NoError(t, configureConntrack(root, tc.memoryGiB<<30))
			for name, want := range map[string]uint64{"nf_conntrack_max": tc.wantLimit, "nf_conntrack_buckets": tc.wantHash} {
				got, err := readKernelUint(filepath.Join(root, name))
				require.NoError(t, err)
				require.Equal(t, want, got)
				// A repeated bootstrap must not need to rewrite correct settings.
				require.NoError(t, os.Chmod(filepath.Join(root, name), 0o444))
			}
			require.NoError(t, configureConntrack(root, tc.memoryGiB<<30))
		})
	}
}

func TestHostIPv6GuardPreservesUsableAndExplicitRoutes(t *testing.T) {
	address := func(ip string, link int) netlink.Addr {
		return netlink.Addr{IPNet: &net.IPNet{IP: net.ParseIP(ip), Mask: net.CIDRMask(128, 128)}, LinkIndex: link}
	}
	ra := netlink.Route{LinkIndex: 2, Protocol: routeProtocolRA, Gw: net.ParseIP("fe80::1")}
	static := ra
	static.Protocol = 4
	for _, tc := range []struct {
		name      string
		addresses []netlink.Addr
		routes    []netlink.Route
		want      bool
	}{
		{"only VM bridge", nil, []netlink.Route{ra}, true},
		{"RA arrives later", nil, nil, true},
		{"link local WAN", []netlink.Addr{address("fe80::2", 2)}, []netlink.Route{ra}, true},
		{"public WAN", []netlink.Addr{address("2001:4860::2", 2)}, []netlink.Route{ra}, false},
		{"public loopback source", []netlink.Addr{address("2001:4860::2", 1)}, []netlink.Route{ra}, false},
		{"private WAN with NAT66", []netlink.Addr{address("fd00:1234::2", 2)}, []netlink.Route{ra}, false},
		{"static configuration", nil, []netlink.Route{static}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			addresses := append([]netlink.Addr{address("fd00:abcd::1", 10)}, tc.addresses...)
			require.Equal(t, tc.want, shouldGuardHostIPv6(2, addresses, tc.routes))
		})
	}
	require.False(t, shouldGuardHostIPv6(2, nil, []netlink.Route{ra}), "IPv6-disabled hosts need no guard")
}
