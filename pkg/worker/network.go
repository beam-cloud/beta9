package worker

import (
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"

	"github.com/coreos/go-iptables/iptables"
	"github.com/opencontainers/runtime-spec/specs-go"
	"github.com/vishvananda/netlink"
	"github.com/vishvananda/netns"
	"golang.org/x/sys/unix"
)

const (
	containerBridgeLinkName      string = "br0"
	containerVethHostPrefix      string = "veth_h_"
	containerVethContainerPrefix string = "veth_c_"
)

type ContainerNetworkManager struct {
	containerId   string
	namespace     string
	vethHost      string
	vethContainer string
	exposedPorts  map[int]int // Map of hostPort -> containerPort
	ipt           *iptables.IPTables
}

func NewContainerNetworkManager(containerId string) (*ContainerNetworkManager, error) {
	truncatedContainerId := containerId[len(containerId)-8:]
	namespace := containerId
	vethHost := fmt.Sprintf("%s%s", containerVethHostPrefix, truncatedContainerId)
	vethContainer := fmt.Sprintf("%s%s", containerVethContainerPrefix, truncatedContainerId)

	ipt, err := iptables.New()
	if err != nil {
		return nil, err
	}

	return &ContainerNetworkManager{
		ipt:           ipt,
		containerId:   containerId,
		namespace:     namespace,
		vethHost:      vethHost,
		vethContainer: vethContainer,
		exposedPorts:  make(map[int]int),
	}, nil
}

func (m *ContainerNetworkManager) Setup(spec *specs.Spec) error {
	hostNS, err := netns.Get()
	if err != nil {
		return err
	}
	defer hostNS.Close()

	// Create a veth pair in the host namespace
	err = createVethPair(m.vethHost, m.vethContainer)
	if err != nil {
		return err
	}

	// Set up the bridge in the host namespace and add the host side of the veth pair to it
	hostVeth, err := netlink.LinkByName(m.vethHost)
	if err != nil {
		return err
	}

	if err := setupBridge(containerBridgeLinkName, hostVeth); err != nil {
		return err
	}

	// Create a new namespace for the container
	newNs, err := netns.NewNamed(m.namespace)
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
	containerVeth, err := netlink.LinkByName(m.vethContainer)
	if err != nil {
		return err
	}
	err = netlink.LinkSetNsFd(containerVeth, int(newNs))
	if err != nil {
		return err
	}

	// Configure the network inside the container's namespace
	err = netns.Set(newNs)
	if err != nil {
		return err
	}
	defer netns.Set(hostNS) // Reset to the original namespace after setting up the container network
	if err := configureContainerNetwork(containerVeth); err != nil {
		return err
	}

	// Update the runc spec to use the new network namespace
	spec.Linux.Namespaces = append(spec.Linux.Namespaces, specs.LinuxNamespace{
		Type: specs.NetworkNamespace,
		Path: filepath.Join("/var/run/netns", m.namespace),
	})

	return nil
}

func createVethPair(hostVethName, containerVethName string) error {
	link := &netlink.Veth{
		LinkAttrs: netlink.LinkAttrs{Name: hostVethName},
		PeerName:  containerVethName,
	}

	if err := netlink.LinkAdd(link); err != nil {
		return err
	}

	return nil
}

func setupBridge(bridgeName string, veth netlink.Link) error {
	bridge := &netlink.Bridge{
		LinkAttrs: netlink.LinkAttrs{Name: bridgeName},
	}

	if err := netlink.LinkAdd(bridge); err != nil && err != unix.EEXIST {
		return err
	}

	if err := netlink.LinkSetUp(bridge); err != nil {
		return err
	}

	if err := netlink.LinkSetMaster(veth, bridge); err != nil {
		return err
	}

	return netlink.LinkSetUp(veth)
}

func configureContainerNetwork(containerVeth netlink.Link) error {
	if err := netlink.LinkSetUp(containerVeth); err != nil {
		return err
	}

	// Assign an IP address to the device
	ipAddr := &netlink.Addr{IPNet: &net.IPNet{
		IP:   net.ParseIP("192.168.1.2"),
		Mask: net.CIDRMask(24, 32),
	}}

	if err := netlink.AddrAdd(containerVeth, ipAddr); err != nil {
		return err
	}

	// Add a default route
	defaultRoute := &netlink.Route{
		LinkIndex: containerVeth.Attrs().Index,
		Gw:        net.ParseIP("192.168.1.1"),
	}

	return netlink.RouteAdd(defaultRoute)
}

func (m *ContainerNetworkManager) TearDown() error {
	return nil
}

func (m *ContainerNetworkManager) ExposePort(hostPort, containerPort int) error {
	// Add NAT POSTROUTING rule
	err := m.ipt.AppendUnique("nat", "POSTROUTING", "-s", "192.168.1.0/24", "-o", "br0", "-j", "MASQUERADE")
	if err != nil {
		return fmt.Errorf("failed to add POSTROUTING rule: %w", err)
	}

	// Add FORWARD rule for bridge to vethHost
	err = m.ipt.AppendUnique("filter", "FORWARD", "-i", "br0", "-o", m.vethHost, "-j", "ACCEPT")
	if err != nil {
		return fmt.Errorf("failed to add FORWARD rule (bridge to vethHost): %w", err)
	}

	// Add FORWARD rule for vethHost to bridge
	err = m.ipt.AppendUnique("filter", "FORWARD", "-i", m.vethHost, "-o", "br0", "-j", "ACCEPT")
	if err != nil {
		return fmt.Errorf("failed to add FORWARD rule (vethHost to bridge): %w", err)
	}

	// Add NAT PREROUTING rule
	err = m.ipt.AppendUnique("nat", "PREROUTING", "-p", "tcp", "--dport", fmt.Sprintf("%d", hostPort), "-j", "DNAT", "--to-destination", fmt.Sprintf("192.168.1.2:%d", containerPort))
	if err != nil {
		return fmt.Errorf("failed to add PREROUTING rule: %w", err)
	}

	// Add FORWARD rule for the DNAT'd traffic
	err = m.ipt.AppendUnique("filter", "FORWARD", "-p", "tcp", "-d", "192.168.1.2", "--dport", fmt.Sprintf("%d", containerPort), "-j", "ACCEPT")
	if err != nil {
		return fmt.Errorf("failed to add FORWARD rule for DNAT'd traffic: %w", err)
	}

	// Store the mapping of exposed ports
	m.exposedPorts[hostPort] = containerPort
	return nil
}

// GetRandomFreePort chooses a random free port
func GetRandomFreePort() (int, error) {
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
