package worker

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/coreos/go-iptables/iptables"
	"github.com/opencontainers/runtime-spec/specs-go"
	"github.com/vishvananda/netlink"
	"github.com/vishvananda/netns"
	"golang.org/x/sys/unix"
)

const (
	containerBridgeLinkName         string        = "b9_br0"
	containerVethHostPrefix         string        = "b9_veth_h_"
	containerVethContainerPrefix    string        = "b9_veth_c_"
	containerSubnet                 string        = "192.168.1.0/24"
	containerGatewayAddress         string        = "192.168.1.1"
	containerBridgeAddress          string        = "192.168.1.1"
	containerNetworkCleanupInterval time.Duration = time.Minute * 2
)

type ContainerNetworkManager struct {
	ctx           context.Context
	defaultLink   netlink.Link
	ipt           *iptables.IPTables
	mu            sync.Mutex
	worker        *types.Worker
	workerRepo    repository.WorkerRepository
	containerRepo repository.ContainerRepository
	networkPrefix string
}

func NewContainerNetworkManager(ctx context.Context, workerId string, workerRepo repository.WorkerRepository, containerRepo repository.ContainerRepository) (*ContainerNetworkManager, error) {
	defaultLink, err := getDefaultInterface()
	if err != nil {
		return nil, err
	}

	ipt, err := iptables.New()
	if err != nil {
		return nil, err
	}

	worker, err := workerRepo.GetWorkerById(workerId)
	if err != nil {
		return nil, err
	}

	networkPrefix := worker.Id
	if worker.MachineId != "" {
		networkPrefix = worker.MachineId
	}

	m := &ContainerNetworkManager{
		ctx:           ctx,
		ipt:           ipt,
		defaultLink:   defaultLink,
		mu:            sync.Mutex{},
		worker:        worker,
		workerRepo:    workerRepo,
		containerRepo: containerRepo,
		networkPrefix: networkPrefix,
	}

	go m.cleanupOrphanedNamespaces()

	return m, nil
}

func (m *ContainerNetworkManager) Setup(containerId string, spec *specs.Spec) error {
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

	return m.configureContainerNetwork(containerId, containerVeth)
}

func (m *ContainerNetworkManager) createVethPair(hostVethName, containerVethName string) error {
	link := &netlink.Veth{
		LinkAttrs: netlink.LinkAttrs{Name: hostVethName, MTU: m.defaultLink.Attrs().MTU},
		PeerName:  containerVethName,
	}

	return netlink.LinkAdd(link)
}

func (m *ContainerNetworkManager) setupBridge(bridgeName string) (netlink.Link, error) {
	bridge, err := netlink.LinkByName(bridgeName)
	if err == nil {
		// Bridge is already set up, do nothing
		return bridge, nil
	}

	bridge = &netlink.Bridge{
		LinkAttrs: netlink.LinkAttrs{Name: bridgeName, MTU: m.defaultLink.Attrs().MTU},
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

	bridgeIP := &netlink.Addr{
		IPNet: &net.IPNet{
			IP:   net.ParseIP(containerBridgeAddress),
			Mask: net.CIDRMask(24, 32),
		},
	}
	if err := netlink.AddrAdd(bridge, bridgeIP); err != nil {
		return nil, err
	}

	// Allow containers to communicate with each other and the internet
	// NAT outgoing traffic from the containers
	if err := m.ipt.AppendUnique("nat", "POSTROUTING", "-s", containerSubnet, "-o", m.defaultLink.Attrs().Name, "-j", "MASQUERADE"); err != nil {
		return nil, err
	}

	// Allow forwarding of traffic from the bridge to the external network and back
	if err := m.ipt.AppendUnique("filter", "FORWARD", "-i", bridgeName, "-o", m.defaultLink.Attrs().Name, "-j", "ACCEPT"); err != nil {
		return nil, err
	}

	if err := m.ipt.AppendUnique("filter", "FORWARD", "-i", m.defaultLink.Attrs().Name, "-o", bridgeName, "-j", "ACCEPT"); err != nil {
		return nil, err
	}

	// Allow forwarding of traffic between containers on the bridge
	if err := m.ipt.AppendUnique("filter", "FORWARD", "-i", bridgeName, "-o", bridgeName, "-j", "ACCEPT"); err != nil {
		return nil, err
	}

	return bridge, err
}

func (m *ContainerNetworkManager) configureContainerNetwork(containerId string, containerVeth netlink.Link) error {
	err := m.workerRepo.SetNetworkLock(m.networkPrefix)
	if err != nil {
		return err
	}
	defer m.workerRepo.RemoveNetworkLock(m.networkPrefix)

	lo, err := netlink.LinkByName("lo")
	if err != nil {
		return err
	}

	if err := netlink.LinkSetUp(lo); err != nil {
		return err
	}

	// Set up the veth interface
	if err := netlink.LinkSetUp(containerVeth); err != nil {
		return err
	}

	// See what IP addresses are already allocated
	allocatedIpAddresses, err := m.workerRepo.GetContainerIps(m.worker.Id)
	if err != nil {
		return err
	}

	allocatedSet := make(map[string]bool, len(allocatedIpAddresses))
	for _, ip := range allocatedIpAddresses {
		allocatedSet[ip] = true
	}

	// Choose a few address that lies in containerSubnet
	_, ipNet, _ := net.ParseCIDR(containerSubnet)
	var ipAddr *netlink.Addr = nil

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
		break
	}
	if ipAddr == nil {
		return errors.New("unable to assign IP address to container")
	}

	// Store allocated IP address
	if err := m.workerRepo.SetContainerIp(m.worker.Id, containerId, ipAddr.IP.String()); err != nil {
		return err
	}

	if err := netlink.AddrAdd(containerVeth, ipAddr); err != nil {
		return err
	}

	// Add a default route
	defaultRoute := &netlink.Route{
		LinkIndex: containerVeth.Attrs().Index,
		Gw:        net.ParseIP(containerGatewayAddress),
	}

	return netlink.RouteAdd(defaultRoute)
}

func (m *ContainerNetworkManager) cleanupOrphanedNamespaces() {
	ticker := time.NewTicker(containerNetworkCleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C:
			m.mu.Lock()

			namespaces, err := listNamespaces("/var/run/netns")
			if err != nil {
				m.mu.Unlock()
				log.Printf("network manager: error listing namespaces - %v\n", err)
				continue
			}

			for _, namespace := range namespaces {
				containerId := namespace // namespace is the same as containerId

				// Check if the container still exists
				_, err := m.containerRepo.GetContainerState(containerId)
				if err != nil {
					// Container state not found, so tear down the namespace and associated resources
					log.Printf("network manager: orphaned namespace detected<%s>, cleaning up...\n", containerId)

					if err := m.TearDown(containerId); err != nil {
						log.Printf("network manager: error tearing down namespace<%s> - %v\n", containerId, err)
					}
				}
			}

			m.mu.Unlock()
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
	err := m.workerRepo.SetNetworkLock(m.networkPrefix)
	if err != nil {
		return err
	}
	defer m.workerRepo.RemoveNetworkLock(m.networkPrefix)

	m.mu.Lock()
	defer m.mu.Unlock()

	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	truncatedContainerId := containerId[len(containerId)-5:]
	vethContainer := fmt.Sprintf("%s%s", containerVethContainerPrefix, truncatedContainerId)
	namespace := containerId

	// Store host namespace for later
	hostNS, err := netns.Get()
	if err != nil {
		return err
	}
	defer hostNS.Close()

	// Switch to container namespace
	containerNS, err := netns.GetFromName(namespace)
	if err != nil {
		return err
	}
	defer containerNS.Close()
	if err := netns.Set(containerNS); err != nil {
		return err
	}

	containerVeth, err := netlink.LinkByName(vethContainer)
	if err != nil {
		return err
	}

	// Delete container side veth
	if err := netlink.LinkSetDown(containerVeth); err != nil {
		return err
	}
	if err := netlink.LinkDel(containerVeth); err != nil {
		return err
	}

	// Switch back to host namespace
	netns.Set(hostNS)

	// Remove container namespace
	if err := netns.DeleteNamed(namespace); err != nil {
		return err
	}

	containerIp, err := m.workerRepo.GetContainerIp(m.networkPrefix, containerId)
	if err != nil {
		return err
	}

	// Look up and remove iptables rules that reference the container IP
	tables := []string{"nat", "filter"}
	for _, table := range tables {
		chains := []string{"PREROUTING", "FORWARD"}

		for _, chain := range chains {
			// List rules in the chain
			rules, err := m.ipt.List(table, chain)
			if err != nil {
				continue
			}

			for _, rule := range rules {
				if strings.Contains(rule, containerIp) {
					if err := m.ipt.Delete(table, chain, strings.Fields(rule)[2:]...); err != nil {
						return err
					}
				}
			}
		}
	}

	return m.workerRepo.RemoveContainerIp(m.networkPrefix, containerId)
}

func (m *ContainerNetworkManager) ExposePort(containerId string, hostPort, containerPort int) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	containerIp, err := m.workerRepo.GetContainerIp(m.networkPrefix, containerId)
	if err != nil {
		return err
	}

	// Add NAT PREROUTING rule
	err = m.ipt.AppendUnique("nat", "PREROUTING", "-p", "tcp", "--dport", fmt.Sprintf("%d", hostPort), "-j", "DNAT", "--to-destination", fmt.Sprintf("%s:%d", containerIp, containerPort))
	if err != nil {
		return err
	}

	// Add FORWARD rule for the DNAT'd traffic
	err = m.ipt.AppendUnique("filter", "FORWARD", "-p", "tcp", "-d", containerIp, "--dport", fmt.Sprintf("%d", containerPort), "-j", "ACCEPT")
	if err != nil {
		return err
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

// listNamespaces lists all valid network namespaces in the specified directory.
func listNamespaces(netnsPath string) ([]string, error) {
	var namespaces []string

	err := filepath.Walk(netnsPath, func(path string, info os.FileInfo, err error) error {
		if path == netnsPath {
			return nil
		}

		if err != nil {
			return err
		}

		if !info.IsDir() {
			nsHandle, err := netns.GetFromPath(path)
			if err == nil {
				defer nsHandle.Close()
				namespaces = append(namespaces, info.Name())
			}
		}

		return nil
	})

	return namespaces, err
}
