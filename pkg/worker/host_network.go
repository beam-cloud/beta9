package worker

import (
	"context"
	"errors"
	"fmt"
	"math/bits"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/metrics"
	"github.com/rs/zerolog/log"
	"github.com/shirou/gopsutil/v4/mem"
	"github.com/vishvananda/netlink"
)

const (
	conntrackSysctlRoot = "/proc/sys/net/netfilter"
	// Roughly 0.1% of RAM for 256-byte flow entries, plus extensions. Round
	// up to a power of two and never reduce an operator's larger setting.
	conntrackBytesPerEntry  = 256 << 10
	conntrackMinimum        = 1 << 18
	routeProtocolRA         = 9    // Linux RTPROT_RA.
	workerIPv6GuardProtocol = 253  // Agent-owned routing protocol, distinct from RA/static routes.
	routeTypeUnreachable    = 7    // Linux RTN_UNREACHABLE.
	ipv6AddressDADFailed    = 0x08 // Linux IFA_F_DADFAILED.
	ipv6AddressTentative    = 0x40 // Linux IFA_F_TENTATIVE.
)

func configureHostNetwork(link netlink.Link) error {
	var conntrackErr error
	if memory, err := mem.VirtualMemory(); err != nil {
		conntrackErr = fmt.Errorf("read host memory: %w", err)
	} else if err := configureConntrack(conntrackSysctlRoot, memory.Total); err != nil {
		conntrackErr = fmt.Errorf("configure conntrack: %w", err)
	}
	ipv6Err := configureHostIPv6(link)
	if ipv6Err != nil {
		ipv6Err = fmt.Errorf("configure host IPv6: %w", ipv6Err)
	}
	return errors.Join(conntrackErr, ipv6Err)
}

type hostIPv6Routes interface {
	RouteReplace(*netlink.Route) error
	RouteDel(*netlink.Route) error
}

func configureHostIPv6(link netlink.Link) error {
	addresses, err := netlink.AddrList(nil, syscall.AF_INET6)
	if err != nil {
		return err
	}
	routes, err := netlink.RouteList(nil, syscall.AF_INET6)
	if err != nil {
		return err
	}
	return reconcileHostIPv6(link.Attrs().Index, addresses, routes, &netlink.Handle{})
}

func reconcileHostIPv6(linkIndex int, addresses []netlink.Addr, routes []netlink.Route, table hostIPv6Routes) error {
	needed := shouldGuardHostIPv6(linkIndex, addresses, routes)
	for _, route := range routes {
		if isWorkerIPv6Guard(route) {
			if needed {
				return nil
			}
			if err := table.RouteDel(&route); err != nil && !errors.Is(err, syscall.ESRCH) {
				return err
			}
		}
	}
	if !needed {
		return nil
	}
	// Networkd may reinstall RA routes independently of kernel accept_ra.
	// Reject unusable public IPv6 immediately so dual-stack clients fall back
	// to IPv4, while connected guest IPv6 routes continue to work.
	return table.RouteReplace(&netlink.Route{
		Family: syscall.AF_INET6, Dst: &net.IPNet{IP: net.IPv6zero, Mask: net.CIDRMask(0, 128)},
		Type: routeTypeUnreachable, Protocol: workerIPv6GuardProtocol, Priority: 1,
	})
}

func isDefaultIPv6Route(route netlink.Route) bool {
	if route.Dst == nil {
		return true
	}
	ones, _ := route.Dst.Mask.Size()
	return ones == 0
}

func isWorkerIPv6Guard(route netlink.Route) bool {
	return route.Protocol == workerIPv6GuardProtocol && route.Type == routeTypeUnreachable && route.Priority == 1 && isDefaultIPv6Route(route)
}

func hasHostIPv6Source(linkIndex int, addresses []netlink.Addr) bool {
	for _, address := range addresses {
		if address.IPNet == nil || address.Flags&(ipv6AddressDADFailed|ipv6AddressTentative) != 0 {
			continue
		}
		ip := address.IP
		// A public address on another interface may intentionally source this
		// route. Private WAN addresses also support explicitly configured NAT66.
		if common.IsPublicIP(ip) || (address.LinkIndex == linkIndex && ip.IsGlobalUnicast()) {
			return true
		}
	}
	return false
}

func shouldGuardHostIPv6(linkIndex int, addresses []netlink.Addr, routes []netlink.Route) bool {
	if hasHostIPv6Source(linkIndex, addresses) {
		return false
	}
	bridgeIPv6 := false
	bridgeIP := net.ParseIP(containerBridgeAddressIPv6)
	for _, address := range addresses {
		bridgeIPv6 = bridgeIPv6 || (address.IPNet != nil && address.IP.Equal(bridgeIP))
	}
	if !bridgeIPv6 {
		return false // IPv6-disabled hosts do not need a guard.
	}
	for _, route := range routes {
		if isDefaultIPv6Route(route) && !isWorkerIPv6Guard(route) && (route.Protocol != routeProtocolRA || route.LinkIndex != linkIndex) {
			return false // Preserve explicitly configured or alternate-WAN defaults.
		}
	}
	return true // Also guard against RAs arriving after worker bootstrap.
}

func readKernelUint(path string) (uint64, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	return strconv.ParseUint(strings.TrimSpace(string(data)), 10, 64)
}

func configureConntrack(root string, memoryBytes uint64) error {
	capacity, err := readKernelUint(filepath.Join(root, "nf_conntrack_max"))
	if err != nil {
		return err
	}
	desired := uint64(1) << bits.Len64(max(uint64(conntrackMinimum), memoryBytes/conntrackBytesPerEntry)-1)
	desired = max(desired, capacity)
	var failures []error
	// Resize buckets before admitting more flows; increasing only the limit
	// would lengthen the chains searched on every packet.
	for _, setting := range []struct {
		name  string
		value uint64
	}{{"nf_conntrack_buckets", (desired + 1) / 2}, {"nf_conntrack_max", desired}} {
		path := filepath.Join(root, setting.name)
		current, err := readKernelUint(path)
		if err != nil {
			failures = append(failures, fmt.Errorf("%s: %w", setting.name, err))
			continue
		}
		if current >= setting.value {
			continue
		}
		if err := os.WriteFile(path, []byte(strconv.FormatUint(setting.value, 10)+"\n"), 0o644); err != nil {
			failures = append(failures, fmt.Errorf("%s: %w", setting.name, err))
		}
	}
	return errors.Join(failures...)
}

func monitorHostNetwork(ctx context.Context, workerID string, link netlink.Link) {
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	pressured := false
	lastError := ""
	for {
		configErr := configureHostNetwork(link)
		message := ""
		if configErr != nil {
			message = configErr.Error()
		}
		if message != "" && message != lastError {
			log.Warn().Err(configErr).Msg("could not refresh host network settings")
		}
		lastError = message
		count, countErr := readKernelUint(filepath.Join(conntrackSysctlRoot, "nf_conntrack_count"))
		capacity, capacityErr := readKernelUint(filepath.Join(conntrackSysctlRoot, "nf_conntrack_max"))
		if countErr == nil && capacityErr == nil && capacity > 0 {
			metrics.RecordWorkerConntrack(workerID, count, capacity)
			high := count >= capacity*4/5
			if high && !pressured {
				log.Warn().Uint64("entries", count).Uint64("capacity", capacity).Msg("host conntrack utilization exceeds 80%")
			}
			pressured = high
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
