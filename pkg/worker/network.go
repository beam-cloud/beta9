package worker

import (
	"bufio"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	mathrand "math/rand"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/coreos/go-iptables/iptables"
	"github.com/opencontainers/runtime-spec/specs-go"
	"github.com/rs/zerolog/log"
	"github.com/vishvananda/netlink"
	"github.com/vishvananda/netns"
	"golang.org/x/sys/unix"
)

const (
	containerBridgeLinkName      string = "b9_br0"
	containerVethHostPrefix      string = "b9_veth_h_"
	containerVethContainerPrefix string = "b9_veth_c_"
	containerSubnet              string = "192.168.1.0/24" // TODO: replace with dynamic subnet
	containerGatewayAddress      string = "192.168.1.1"
	containerBridgeAddress       string = "192.168.1.1"
	containerSubnetIPv6          string = "fd00:abcd::/64"
	containerGatewayAddressIPv6  string = "fd00:abcd::1"
	containerBridgeAddressIPv6   string = "fd00:abcd::1"

	containerNetworkCleanupInterval time.Duration = time.Minute * 1
)

type ContainerNetworkManager struct {
	ctx                 context.Context
	defaultLink         netlink.Link
	ipt                 *iptables.IPTables
	ipt6                *iptables.IPTables
	worker              *types.Worker
	workerRepoClient    pb.WorkerRepositoryServiceClient
	containerRepoClient pb.ContainerRepositoryServiceClient
	networkPrefix       string
	mu                  sync.Mutex
	config              types.AppConfig
	containerInstances  *common.SafeMap[*ContainerInstance]
}

func NewContainerNetworkManager(ctx context.Context, workerId string, workerRepoClient pb.WorkerRepositoryServiceClient, containerRepoClient pb.ContainerRepositoryServiceClient, config types.AppConfig, containerInstances *common.SafeMap[*ContainerInstance]) (*ContainerNetworkManager, error) {
	defaultLink, err := getDefaultInterface()
	if err != nil {
		return nil, err
	}

	ipTablesMode := detectIptablesMode()

	ipv4Path := ""
	ipv6Path := ""
	switch ipTablesMode {
	case "nftables":
		ipv4Path = "/usr/sbin/iptables-nft"
		ipv6Path = "/usr/sbin/ip6tables-nft"
	case "legacy":
		fallthrough
	default:
		ipv4Path = "/usr/sbin/iptables"
		ipv6Path = "/usr/sbin/ip6tables"
	}

	ipt, err := iptables.New(iptables.Path(ipv4Path), iptables.IPFamily(iptables.ProtocolIPv4))
	if err != nil {
		return nil, err
	}

	// Initialize ip6tables for IPv6 support
	var ipt6 *iptables.IPTables

	ipt6Supported := true
	ipt6, err = iptables.New(iptables.Path(ipv6Path), iptables.IPFamily(iptables.ProtocolIPv6))
	if err != nil {
		log.Warn().Err(err).Msg("IPv6 iptables initialization failed, falling back to IPv4 only")
		ipt6Supported = false
	} else {
		// Check if the ip6tables NAT table can be accessed
		_, err := ipt6.List("nat", "POSTROUTING")
		if err != nil {
			log.Warn().Err(err).Msg("IPv6 iptables NAT table not available, falling back to IPv4 only")
			ipt6Supported = false
		}
	}

	networkPrefix := os.Getenv("NETWORK_PREFIX")
	if networkPrefix == "" {
		return nil, errors.New("invalid network prefix")
	}

	m := &ContainerNetworkManager{
		ctx:                 ctx,
		ipt:                 ipt,
		ipt6:                ipt6,
		defaultLink:         defaultLink,
		workerRepoClient:    workerRepoClient,
		containerRepoClient: containerRepoClient,
		networkPrefix:       networkPrefix,
		mu:                  sync.Mutex{},
		config:              config,
		containerInstances:  containerInstances,
	}

	// Disable IPv6 if ip6tables is not supported
	if !ipt6Supported {
		m.ipt6 = nil
	}

	go m.cleanupOrphanedNamespaces()

	return m, nil
}

// detectIptablesMode detects which iptables version is use on the host based on where the KUBE-FORWARD chain has been setup
func detectIptablesMode() string {
	iptNft, err := iptables.New(iptables.IPFamily(iptables.ProtocolIPv4), iptables.Path("/usr/sbin/iptables-nft"))
	if err == nil {
		if exists, _ := iptNft.ChainExists("filter", "KUBE-FORWARD"); exists {
			return "nftables"
		}
	}

	iptLegacy, err := iptables.New(iptables.IPFamily(iptables.ProtocolIPv4), iptables.Path("/usr/sbin/iptables-legacy"))
	if err == nil {
		if exists, _ := iptLegacy.ChainExists("filter", "KUBE-FORWARD"); exists {
			return "legacy"
		}
	}

	// Default to legacy if no KUBE-FORWARD chain found
	return "legacy"
}

func (m *ContainerNetworkManager) Setup(containerId string, spec *specs.Spec, request *types.ContainerRequest) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	truncatedContainerId := containerId[len(containerId)-5:]
	namespace := containerId
	vethHost := fmt.Sprintf("%s%s", containerVethHostPrefix, truncatedContainerId)
	vethContainer := fmt.Sprintf("%s%s", containerVethContainerPrefix, truncatedContainerId)

	// Store default network namespace for later
	hostNS, err := netns.Get()
	if err != nil {
		return err
	}
	defer hostNS.Close()

	// Create a veth pair in the host namespace
	if err = m.createVethPair(vethHost, vethContainer); err != nil {
		return err
	}

	// Set up the bridge in the host namespace and add the host side of the veth pair to it
	hostVeth, err := netlink.LinkByName(vethHost)
	if err != nil {
		return err
	}
	bridge, err := m.setupBridge(containerBridgeLinkName)
	if err != nil {
		return err
	}

	// Associate new veth on the host side with the bridge device
	if err := netlink.LinkSetMaster(hostVeth, bridge); err != nil {
		return err
	}

	if err := netlink.LinkSetUp(hostVeth); err != nil {
		return err
	}

	// Create a new namespace for the container
	newNs, err := netns.NewNamed(namespace)
	if err != nil {
		return err
	}
	defer newNs.Close()

	// By default, creating a new namespace automatically sets the current namespace
	// to that newly created namespace. So we have to move back to host namespace.
	err = netns.Set(hostNS)
	if err != nil {
		return err
	}

	// Move the container side of the veth pair into the new namespace
	containerVeth, err := netlink.LinkByName(vethContainer)
	if err != nil {
		return err
	}
	err = netlink.LinkSetNsFd(containerVeth, int(newNs))
	if err != nil {
		return err
	}

	// Update the runc spec to use the new network namespace
	spec.Linux.Namespaces = append(spec.Linux.Namespaces, specs.LinuxNamespace{
		Type: specs.NetworkNamespace,
		Path: filepath.Join("/var/run/netns", namespace),
	})

	// Configure the network inside the container's namespace
	err = netns.Set(newNs)
	if err != nil {
		return err
	}
	defer netns.Set(hostNS) // Reset to the original namespace after setting up the container network

	return m.configureContainerNetwork(&containerNetworkConfigOpts{
		containerId:   containerId,
		containerVeth: containerVeth,
		request:       request,
	})
}

func (m *ContainerNetworkManager) createVethPair(hostVethName, containerVethName string) error {
	link := &netlink.Veth{
		LinkAttrs: netlink.LinkAttrs{Name: hostVethName,
			MTU:          m.defaultLink.Attrs().MTU,
			HardwareAddr: generateUniqueMAC(),
		},
		PeerName:         containerVethName,
		PeerHardwareAddr: generateUniqueMAC(),
	}

	return netlink.LinkAdd(link)
}

func (m *ContainerNetworkManager) setupBridge(bridgeName string) (netlink.Link, error) {
	lockResponse, err := handleGRPCResponse(m.workerRepoClient.SetNetworkLock(m.ctx, &pb.SetNetworkLockRequest{
		NetworkPrefix: m.networkPrefix,
		Ttl:           10,
		Retries:       10,
	}))
	if err != nil {
		return nil, err
	}
	defer m.workerRepoClient.RemoveNetworkLock(m.ctx, &pb.RemoveNetworkLockRequest{
		NetworkPrefix: m.networkPrefix,
		Token:         lockResponse.Token,
	})

	bridge, err := netlink.LinkByName(bridgeName)
	if err == nil {
		// Bridge is already set up, do nothing
		return bridge, nil
	}

	bridge = &netlink.Bridge{
		LinkAttrs: netlink.LinkAttrs{
			Name:         bridgeName,
			MTU:          m.defaultLink.Attrs().MTU,
			HardwareAddr: generateUniqueMAC(),
		},
	}

	if err := netlink.LinkAdd(bridge); err != nil && err != unix.EEXIST {
		return nil, err
	}

	bridge, err = netlink.LinkByName(bridgeName)
	if err != nil {
		return nil, err
	}

	if err := netlink.LinkSetUp(bridge); err != nil {
		return nil, err
	}

	bridgeIPv4 := &netlink.Addr{
		IPNet: &net.IPNet{
			IP:   net.ParseIP(containerBridgeAddress),
			Mask: net.CIDRMask(24, 32),
		},
	}
	if err := netlink.AddrAdd(bridge, bridgeIPv4); err != nil {
		return nil, err
	}

	if m.ipt6 != nil {
		_, ipv6Net, _ := net.ParseCIDR(containerSubnetIPv6)
		bridgeIPv6 := &netlink.Addr{
			IPNet: &net.IPNet{
				IP:   net.ParseIP(containerBridgeAddressIPv6),
				Mask: ipv6Net.Mask,
			},
		}
		if err := netlink.AddrAdd(bridge, bridgeIPv6); err != nil {
			return nil, err
		}
	}

	// Allow containers to communicate with each other and the internet
	// (NAT outgoing traffic from the containers)

	// IPv4
	if err := m.ipt.AppendUnique("nat", "POSTROUTING", "-s", containerSubnet, "-o", m.defaultLink.Attrs().Name, "-j", "MASQUERADE"); err != nil {
		return nil, err
	}

	// IPv6
	if m.ipt6 != nil {
		if err := m.ipt6.AppendUnique("nat", "POSTROUTING", "-s", containerSubnetIPv6, "-o", m.defaultLink.Attrs().Name, "-j", "MASQUERADE"); err != nil {
			return nil, err
		}
	}

	// Allow forwarding of traffic from the bridge to the external network and back
	if err := m.ipt.InsertUnique("filter", "FORWARD", 1, "-i", bridgeName, "-o", m.defaultLink.Attrs().Name, "-j", "ACCEPT"); err != nil {
		return nil, err
	}

	if err := m.ipt.InsertUnique("filter", "FORWARD", 1, "-i", m.defaultLink.Attrs().Name, "-o", bridgeName, "-j", "ACCEPT"); err != nil {
		return nil, err
	}

	return bridge, err
}

type containerNetworkConfigOpts struct {
	containerId   string
	containerVeth netlink.Link
	request       *types.ContainerRequest
}

func (m *ContainerNetworkManager) configureContainerNetwork(opts *containerNetworkConfigOpts) error {
	lockResponse, err := handleGRPCResponse(m.workerRepoClient.SetNetworkLock(m.ctx, &pb.SetNetworkLockRequest{
		NetworkPrefix: m.networkPrefix,
		Ttl:           10,
		Retries:       10,
	}))
	if err != nil {
		return err
	}
	defer m.workerRepoClient.RemoveNetworkLock(m.ctx, &pb.RemoveNetworkLockRequest{
		NetworkPrefix: m.networkPrefix,
		Token:         lockResponse.Token,
	})

	lo, err := netlink.LinkByName("lo")
	if err != nil {
		return err
	}

	if err := netlink.LinkSetUp(lo); err != nil {
		return err
	}

	// Force-enable IPv6 in the namespace (ensures AF_INET6 is usable for CRIU restore binds)
	sysctlDisableIPv6 := "/proc/sys/net/ipv6/conf/all/disable_ipv6"
	if err := os.WriteFile(sysctlDisableIPv6, []byte("0\n"), 0644); err != nil {
		return err
	}

	// As backup, allow binds to non-local addresses
	sysctlNonlocalBind := "/proc/sys/net/ipv6/ip_nonlocal_bind"
	if err := os.WriteFile(sysctlNonlocalBind, []byte("1\n"), 0644); err != nil {
		return err
	}

	// Always add IPv6 loopback address to enable IPv6 wildcard binds
	ipv6Lo := &netlink.Addr{
		IPNet: &net.IPNet{
			IP:   net.ParseIP("::1"),
			Mask: net.CIDRMask(128, 128),
		},
	}
	if err := netlink.AddrAdd(lo, ipv6Lo); err != nil && !errors.Is(err, unix.EEXIST) {
		return err
	}

	ipv4Lo := &netlink.Addr{
		IPNet: &net.IPNet{
			IP:   net.ParseIP("127.0.0.1"),
			Mask: net.CIDRMask(8, 32),
		},
	}
	if err := netlink.AddrAdd(lo, ipv4Lo); err != nil && !errors.Is(err, unix.EEXIST) {
		return err
	}
	// Set up the veth interface
	if err := netlink.LinkSetUp(opts.containerVeth); err != nil {
		return err
	}

	// See what IP addresses are already allocated
	getContainerIpsResponse, err := handleGRPCResponse(m.workerRepoClient.GetContainerIps(m.ctx, &pb.GetContainerIpsRequest{
		NetworkPrefix: m.networkPrefix,
	}))
	if err != nil {
		return err
	}

	allocatedIpAddresses := getContainerIpsResponse.Ips
	allocatedSet := make(map[string]bool, len(allocatedIpAddresses))
	for _, ip := range allocatedIpAddresses {
		allocatedSet[ip] = true
	}

	var ipAddr *netlink.Addr = nil
	var ipv4LastOctet int = -1

	// For checkpointed containers, we need to use the IP address that was used for the checkpoint
	// We try to do this in the upper bound of the subnet range
	// If we have an existing checkpoint, we try to allocate that IP. If it's not available, we can't
	// launch the container on this worker right now
	if opts.request.Checkpoint != nil || opts.request.CheckpointEnabled {
		var ip string

		if opts.request.Checkpoint != nil {
			ip = opts.request.Checkpoint.ContainerIp
		}

		if ip != "" {
			log.Info().Str("container_id", opts.containerId).Msgf("checkpoint enabled, using stored IP address: %s", ip)

			ipAddr = &netlink.Addr{
				IPNet: &net.IPNet{
					IP:   net.ParseIP(ip),
					Mask: net.CIDRMask(24, 32),
				},
			}

			ipv4LastOctet = int(ipAddr.IP.To4()[3])

			if _, allocated := allocatedSet[ipAddr.IP.String()]; allocated {
				log.Info().Str("container_id", opts.containerId).Msgf("checkpoint enabled, but preferred IP address is already allocated, cannot use it: %s", ipAddr.IP.String())

				// If we were unable to use the preferred IP address, we need to disable checkpointing
				// for this container request
				ipAddr = nil
				ipv4LastOctet = -1

				opts.request.CheckpointEnabled = false
				opts.request.Checkpoint = nil
			}
		} else {
			// Assign a random IP address in the range 128-255 to avoid _most_ conflicts with non-checkpointed containers
			ipAddr, err = assignIpInRange(allocatedSet, 128, 255)
			if err != nil {
				return err
			}

			log.Info().Str("container_id", opts.containerId).Msgf("checkpoint enabled, using random IP address in range 128-255: %s", ipAddr.IP.String())
			ipv4LastOctet = int(ipAddr.IP.To4()[3])

		}
	}

	if ipAddr == nil {
		// Choose a new address that lies in containerSubnet
		_, ipNet, _ := net.ParseCIDR(containerSubnet)
		for ip := ipNet.IP.Mask(ipNet.Mask); ipNet.Contains(ip); ip = nextIP(ip, 1) {
			ipStr := ip.String()

			// Skip the gateway address (i.e. 192.168.1.1)
			if ipStr == containerBridgeAddress || ipStr == ipNet.IP.String() {
				continue
			}

			if _, allocated := allocatedSet[ipStr]; allocated {
				continue
			}

			ipAddr = &netlink.Addr{
				IPNet: &net.IPNet{
					IP:   ip,
					Mask: ipNet.Mask,
				},
			}

			// Extract the last octet of the IPv4 address
			ipv4LastOctet = int(ip.To4()[3])
			break
		}
	}

	if ipAddr == nil {
		return errors.New("unable to assign IP address to container")
	}

	if err := netlink.AddrAdd(opts.containerVeth, ipAddr); err != nil {
		return err
	}

	// Add a default route (IPv4)
	defaultRoute := &netlink.Route{
		LinkIndex: opts.containerVeth.Attrs().Index,
		Gw:        net.ParseIP(containerGatewayAddress),
	}
	if err := netlink.RouteAdd(defaultRoute); err != nil {
		return err
	}

	if m.ipt6 != nil {
		// Parse the IPv6 subnet
		_, ipv6Net, _ := net.ParseCIDR(containerSubnetIPv6)
		ipv6Prefix := ipv6Net.IP.String()

		// Allocate an IPv6 address using the last octet of the IPv4 address
		ipv6Address := fmt.Sprintf("%s%x", ipv6Prefix, ipv4LastOctet)
		ipv6Addr := &netlink.Addr{
			IPNet: &net.IPNet{
				IP:   net.ParseIP(ipv6Address),
				Mask: ipv6Net.Mask,
			},
		}

		if err := netlink.AddrAdd(opts.containerVeth, ipv6Addr); err != nil {
			return err
		}

		// Add a default route (IPv6)
		defaultIPv6Route := &netlink.Route{
			LinkIndex: opts.containerVeth.Attrs().Index,
			Gw:        net.ParseIP(containerGatewayAddressIPv6),
		}
		if err := netlink.RouteAdd(defaultIPv6Route); err != nil {
			return err
		}
	}

	_, err = handleGRPCResponse(m.workerRepoClient.SetContainerIp(m.ctx, &pb.SetContainerIpRequest{
		NetworkPrefix: m.networkPrefix,
		ContainerId:   opts.containerId,
		IpAddress:     ipAddr.IP.String(),
	}))
	if err != nil {
		return err
	}

	containerInstance, exists := m.containerInstances.Get(opts.containerId)
	if exists {
		containerInstance.ContainerIp = ipAddr.IP.String()
		m.containerInstances.Set(opts.containerId, containerInstance)
	}

	return nil
}

func (m *ContainerNetworkManager) cleanupOrphanedNamespaces() {
	ticker := time.NewTicker(containerNetworkCleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C:
			containerIds, err := m.listContainerIdsFromIptables()
			if err != nil {
				log.Error().Err(err).Msg("error listing container ids")
				continue
			}

			for _, containerId := range containerIds {
				func() {
					// Only allow one worker on this machine/worker handle the cleanup
					// We have a secondary lock for the IP assignment, but we need this lock for the "container" level consistency
					lockResponse, err := handleGRPCResponse(m.workerRepoClient.SetNetworkLock(m.ctx, &pb.SetNetworkLockRequest{
						NetworkPrefix: m.networkPrefix + "-" + containerId,
						Ttl:           10,
						Retries:       0,
					}))
					if err != nil {
						return
					}
					defer m.workerRepoClient.RemoveNetworkLock(m.ctx, &pb.RemoveNetworkLockRequest{
						NetworkPrefix: m.networkPrefix + "-" + containerId,
						Token:         lockResponse.Token,
					})

					// Check if the container still exists
					notFoundErr := &types.ErrContainerStateNotFound{}
					_, err = handleGRPCResponse(m.containerRepoClient.GetContainerState(context.Background(), &pb.GetContainerStateRequest{ContainerId: containerId}))
					if err != nil && notFoundErr.From(err) {
						// Container state not found, so tear down the namespace and associated resources
						log.Info().Str("container_id", containerId).Msg("orphaned namespace detected, cleaning up")

						if err := m.TearDown(containerId); err != nil {
							log.Error().Str("container_id", containerId).Err(err).Msg("error tearing down orphaned namespace")
						}
					}

				}()

			}
		}
	}
}

// Taken from: https://gist.github.com/udhos/b468fbfd376aa0b655b6b0c539a88c03
func nextIP(ip net.IP, inc uint) net.IP {
	i := ip.To4()
	v := uint(i[0])<<24 + uint(i[1])<<16 + uint(i[2])<<8 + uint(i[3])
	v += inc
	v3 := byte(v & 0xFF)
	v2 := byte((v >> 8) & 0xFF)
	v1 := byte((v >> 16) & 0xFF)
	v0 := byte((v >> 24) & 0xFF)
	return net.IPv4(v0, v1, v2, v3)
}

func (m *ContainerNetworkManager) TearDown(containerId string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	lockResponse, err := handleGRPCResponse(m.workerRepoClient.SetNetworkLock(m.ctx, &pb.SetNetworkLockRequest{
		NetworkPrefix: m.networkPrefix,
		Ttl:           10,
		Retries:       10,
	}))
	if err != nil {
		return err
	}
	defer m.workerRepoClient.RemoveNetworkLock(m.ctx, &pb.RemoveNetworkLockRequest{
		NetworkPrefix: m.networkPrefix,
		Token:         lockResponse.Token,
	})

	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	truncatedContainerId := containerId[len(containerId)-5:]
	vethHost := fmt.Sprintf("%s%s", containerVethHostPrefix, truncatedContainerId)
	namespace := containerId

	hostVeth, err := netlink.LinkByName(vethHost)
	if err == nil {
		// Remove the veth from the bridge
		if err := netlink.LinkSetNoMaster(hostVeth); err != nil {
			return err
		}

		// Immediately delete the veth without setting it down first
		if err := netlink.LinkDel(hostVeth); err != nil {
			return err
		}
	}

	containerIpResponse, err := handleGRPCResponse(m.workerRepoClient.GetContainerIp(m.ctx, &pb.GetContainerIpRequest{
		NetworkPrefix: m.networkPrefix,
		ContainerId:   containerId,
	}))
	if err != nil {
		return err
	}

	containerIp := containerIpResponse.IpAddress

	// Calculate the corresponding IPv6 address
	ip := net.ParseIP(containerIp)
	ipv4LastOctet := int(ip.To4()[3])
	_, ipv6Net, _ := net.ParseCIDR(containerSubnetIPv6)
	ipv6Prefix := ipv6Net.IP.String()

	// Allocate an IPv6 address using the last octet of the IPv4 address
	ipv6Address := fmt.Sprintf("%s%x", ipv6Prefix, ipv4LastOctet)

	// Remove iptables and ip6tables rules
	if err := m.removeIPTablesRules(containerIp, m.ipt); err != nil {
		return err
	}

	if m.ipt6 != nil {
		if err := m.removeIPTablesRules(ipv6Address, m.ipt6); err != nil {
			return err
		}
	}

	// Delete container namespace don't bother handling
	// the error because the namespace is likely to be gone at this point
	netns.DeleteNamed(namespace)

	_, err = handleGRPCResponse(m.workerRepoClient.RemoveContainerIp(m.ctx, &pb.RemoveContainerIpRequest{
		NetworkPrefix: m.networkPrefix,
		ContainerId:   containerId,
	}))
	if err != nil {
		return err
	}

	// Flush ARP cache on the bridge device
	cmd := exec.Command("ip", "neigh", "flush", "dev", containerBridgeLinkName)
	output, err := cmd.CombinedOutput()
	if err != nil {
		log.Debug().Err(err).Str("output", string(output)).Msg("failed to flush ARP entries")
	}

	return nil
}

func (m *ContainerNetworkManager) removeIPTablesRules(ip string, ipt *iptables.IPTables) error {
	tables := []string{"nat", "filter"}
	for _, table := range tables {
		chains := []string{"PREROUTING", "FORWARD"}

		for _, chain := range chains {
			// List rules in the chain
			rules, err := ipt.List(table, chain)
			if err != nil {
				continue
			}

			for _, rule := range rules {
				if strings.Contains(rule, ip) {
					parts := strings.Fields(rule)

					// Remove any double quotes
					for i, part := range parts {
						parts[i] = strings.ReplaceAll(part, `"`, "")
					}

					if err := ipt.Delete(table, chain, parts[2:]...); err != nil {
						return err
					}
				}
			}
		}
	}
	return nil
}

func (m *ContainerNetworkManager) ExposePort(containerId string, hostPort, containerPort int) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	containerIpResponse, err := handleGRPCResponse(m.workerRepoClient.GetContainerIp(m.ctx, &pb.GetContainerIpRequest{
		NetworkPrefix: m.networkPrefix,
		ContainerId:   containerId,
	}))
	if err != nil {
		return err
	}

	containerIp := containerIpResponse.IpAddress

	// Extract the last octet from the IPv4 address (192.168.1.2 -> 2)
	ip := net.ParseIP(containerIp)
	if ip == nil {
		return fmt.Errorf("invalid IPv4 address: %s", containerIp)
	}

	// Recreate the corresponding IPv6 address using the last octet
	ipv4LastOctet := int(ip.To4()[3])
	_, ipv6Net, _ := net.ParseCIDR(containerSubnetIPv6)
	ipv6Prefix := ipv6Net.IP.String()
	containerIp_IPv6 := fmt.Sprintf("%s%x", ipv6Prefix, ipv4LastOctet)

	truncatedContainerId := containerId[len(containerId)-5:]
	vethHost := fmt.Sprintf("%s%s", containerVethHostPrefix, truncatedContainerId)
	comment := fmt.Sprintf("%s:%s", vethHost, containerId)

	// Insert NAT PREROUTING rule at the top of the chain
	// IPv4
	err = m.ipt.InsertUnique("nat", "PREROUTING", 1, "-p", "tcp", "--dport", fmt.Sprintf("%d", hostPort), "-j", "DNAT", "--to-destination", fmt.Sprintf("%s:%d", containerIp, containerPort), "-m", "comment", "--comment", comment)
	if err != nil {
		return err
	}

	// IPv6
	if m.ipt6 != nil {
		err = m.ipt6.InsertUnique("nat", "PREROUTING", 1, "-p", "tcp", "--dport", fmt.Sprintf("%d", hostPort), "-j", "DNAT", "--to-destination", fmt.Sprintf("[%s]:%d", containerIp_IPv6, containerPort), "-m", "comment", "--comment", comment)
		if err != nil {
			return err
		}
	}

	// Add FORWARD rule for the DNAT'd traffic
	// IPv4
	err = m.ipt.AppendUnique("filter", "FORWARD", "-p", "tcp", "-d", containerIp, "--dport", fmt.Sprintf("%d", containerPort), "-j", "ACCEPT", "-m", "comment", "--comment", comment)
	if err != nil {
		return err
	}

	// IPv6
	if m.ipt6 != nil {
		err = m.ipt6.AppendUnique("filter", "FORWARD", "-p", "tcp", "-d", containerIp_IPv6, "--dport", fmt.Sprintf("%d", containerPort), "-j", "ACCEPT", "-m", "comment", "--comment", comment)
		if err != nil {
			return err
		}
	}

	return nil
}

// getRandomFreePort chooses a random free port
func getRandomFreePort() (int, error) {
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		return 0, err
	}

	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return 0, err
	}
	defer l.Close()

	return l.Addr().(*net.TCPAddr).Port, nil
}

// GetPodAddr gets the IP from the POD_IP env var.
// Returns an error if it fails to retrieve an IP.
func GetPodAddr() (string, error) {
	addr, exists := os.LookupEnv("POD_HOSTNAME")
	if exists {
		return addr, nil
	}

	return getIPFromEnv("POD_IP")
}

// getDefaultInterface returns the link that goes to the internet.
func getDefaultInterface() (netlink.Link, error) {
	file, err := os.Open("/proc/net/route")
	if err != nil {
		return nil, err
	}
	defer file.Close()

	linkName := ""
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if fields[1] == "00000000" { // Destination of default route
			linkName = fields[0]
		}
	}

	if linkName == "" {
		return nil, fmt.Errorf("default route not found")
	}

	link, err := netlink.LinkByName(linkName)
	if err != nil {
		return nil, err
	}

	return link, nil
}

// getIPFromEnv gets the IP address from an environment variable.
func getIPFromEnv(varName string) (string, error) {
	addr := os.Getenv(varName)
	if addr == "" {
		return "", fmt.Errorf("no ip found in environment variable")
	}

	ip := net.ParseIP(addr)
	if ip == nil {
		return "", errors.New("failed to parse ip address")
	}

	// If the parsed IP is an IPv6 address, encapsulate in brackets
	if ip.To4() == nil {
		return fmt.Sprintf("[%s]", ip.String()), nil
	}

	return ip.String(), nil
}

func (m *ContainerNetworkManager) listContainerIdsFromIptables() ([]string, error) {
	containerIdsSet := make(map[string]struct{})

	rules, err := m.ipt.List("nat", "PREROUTING")
	if err != nil {
		return nil, err
	}

	for _, rule := range rules {
		if strings.Contains(rule, containerVethHostPrefix) {
			parts := strings.Split(rule, ":")
			if len(parts) > 1 {
				containerId := strings.Fields(parts[1])[0]
				containerId = strings.TrimRight(containerId, `\"`)
				containerIdsSet[containerId] = struct{}{}
			}
		}
	}

	if m.ipt6 != nil {
		rules6, err := m.ipt6.List("nat", "PREROUTING")
		if err != nil {
			return nil, err
		}

		for _, rule := range rules6 {
			if strings.Contains(rule, containerVethHostPrefix) {
				parts := strings.Split(rule, ":")
				if len(parts) > 1 {
					containerId := strings.Fields(parts[1])[0]
					containerId = strings.TrimRight(containerId, `\"`)
					containerIdsSet[containerId] = struct{}{}
				}
			}
		}
	}

	containerIds := make([]string, 0, len(containerIdsSet))
	for id := range containerIdsSet {
		containerIds = append(containerIds, id)
	}

	return containerIds, nil
}

// generateUniqueMAC generates a random MAC address with a specific OUI (Organizationally Unique Identifier).
func generateUniqueMAC() net.HardwareAddr {
	mac := make([]byte, 6)
	_, err := rand.Read(mac)
	if err != nil {
		// Fall back to using a deterministic value in case of failure
		// However, the chance of crypto/rand failing is extremely low
		mac = []byte{0x02, 0x00, 0x00, 0x00, 0x00, 0x00}
	}

	// Set the local bit (second least significant bit of the first byte)
	// and unset the multicast bit (least significant bit of the first byte).
	mac[0] = (mac[0] | 0x02) & 0xfe

	return net.HardwareAddr(mac)
}

// assignIpInRange assigns an IP address in the range [rangeStart, rangeEnd) that is not already allocated.
// (returns an error if no IP address can be assigned)
func assignIpInRange(allocatedSet map[string]bool, rangeStart, rangeEnd uint8) (*netlink.Addr, error) {
	_, ipNet, _ := net.ParseCIDR(containerSubnet)
	baseIP := ipNet.IP.To4()
	if baseIP == nil {
		return nil, errors.New("invalid IPv4 subnet")
	}

	var candidates []net.IP
	for i := rangeStart; i < rangeEnd; i++ {
		ip := net.IPv4(baseIP[0], baseIP[1], baseIP[2], byte(i))
		if !ipNet.Contains(ip) {
			continue
		}

		ipStr := ip.String()
		if ipStr == containerBridgeAddress || ipStr == ipNet.IP.String() {
			continue
		}

		if _, allocated := allocatedSet[ipStr]; allocated {
			continue
		}

		candidates = append(candidates, ip)
	}

	if len(candidates) == 0 {
		return nil, errors.New("unable to assign IP address in range")
	}

	mathrand.Seed(time.Now().UnixNano())
	mathrand.Shuffle(len(candidates), func(i, j int) { candidates[i], candidates[j] = candidates[j], candidates[i] })

	ip := candidates[0]

	return &netlink.Addr{
		IPNet: &net.IPNet{
			IP:   ip,
			Mask: ipNet.Mask,
		},
	}, nil
}
