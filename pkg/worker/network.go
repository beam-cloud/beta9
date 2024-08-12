package worker

import (
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"

	"github.com/opencontainers/runtime-spec/specs-go"
	"github.com/vishvananda/netlink"
	"github.com/vishvananda/netns"
)

type ContainerNetworkManager struct {
	containerId   string
	namespace     string
	vethHost      string
	vethContainer string
	ipContainer   *net.IPNet
	randomPort    int
	containerPort int
	bridgeName    string
	hostNS        netns.NsHandle
	containerNS   netns.NsHandle
	exposedPorts  map[int]int // Map of hostPort -> containerPort
}

func NewContainerNetworkManager(containerId string, containerPort int) *ContainerNetworkManager {
	namespace := fmt.Sprintf("ns_%s", containerId)
	vethHost := fmt.Sprintf("vth_h_%s", containerId[len(containerId)-8:])
	vethContainer := fmt.Sprintf("vth_c_%s", containerId[len(containerId)-8:])
	bridgeName := fmt.Sprintf("br_%s", containerId[len(containerId)-8:])

	return &ContainerNetworkManager{
		containerId:   containerId,
		namespace:     namespace,
		vethHost:      vethHost,
		vethContainer: vethContainer,
		containerPort: containerPort,
		exposedPorts:  make(map[int]int),
		bridgeName:    bridgeName,
	}
}

func (m *ContainerNetworkManager) Setup(spec *specs.Spec) error {
	var err error

	// Save the current network namespace
	m.hostNS, err = netns.Get()
	if err != nil {
		return fmt.Errorf("failed to get current namespace: %v", err)
	}

	// Create or get the bridge on the host (in the default namespace)
	if err := m.setupBridge(); err != nil {
		return fmt.Errorf("failed to set up bridge: %v", err)
	}

	br, err := netlink.LinkByName(m.bridgeName)
	if err != nil {
		return fmt.Errorf("failed to get bridge: %v", err)
	}

	// Create a new network namespace for the container
	m.containerNS, err = netns.NewNamed(m.namespace)
	if err != nil {
		return fmt.Errorf("failed to create namespace: %v", err)
	}

	// Create the veth pair in the host namespace
	veth := &netlink.Veth{
		LinkAttrs: netlink.LinkAttrs{Name: m.vethHost},
		PeerName:  m.vethContainer,
	}
	if err := netlink.LinkAdd(veth); err != nil {
		return fmt.Errorf("failed to create veth pair: %v", err)
	}

	// Attach vethHost to the bridge
	hostVeth, err := netlink.LinkByName(m.vethHost)
	if err != nil {
		return fmt.Errorf("failed to get host veth: %v", err)
	}

	if err := netlink.LinkSetMaster(hostVeth, br); err != nil {
		return fmt.Errorf("failed to attach vethHost to bridge: %v", err)
	}

	if err := netlink.LinkSetUp(hostVeth); err != nil {
		return fmt.Errorf("failed to set host veth up: %v", err)
	}

	// Move the container side of the veth into the container's namespace
	containerVeth, err := netlink.LinkByName(m.vethContainer)
	if err != nil {
		return fmt.Errorf("failed to get container veth before moving: %v", err)
	}
	if err := netlink.LinkSetNsFd(containerVeth, int(m.containerNS)); err != nil {
		return fmt.Errorf("failed to move veth to container namespace: %v", err)
	}

	// Now switch to the container's network namespace to configure the container side
	if err := netns.Set(m.containerNS); err != nil {
		return fmt.Errorf("failed to switch to container namespace: %v", err)
	}
	defer netns.Set(m.hostNS) // Ensure we switch back to the host namespace

	// After moving, configure the container-side veth interface
	containerVeth, err = netlink.LinkByName(m.vethContainer)
	if err != nil {
		return fmt.Errorf("failed to get container veth after moving: %v", err)
	}
	if err := netlink.LinkSetUp(containerVeth); err != nil {
		return fmt.Errorf("failed to set container veth up: %v", err)
	}

	// Dynamically assign an IP to the container-side veth interface
	m.ipContainer, err = generateUniqueIPForContainer("192.168.0.0/16")
	if err != nil {
		return fmt.Errorf("failed to generate unique IP: %v", err)
	}

	// Assign IP address to the container-side veth
	addr := &netlink.Addr{IPNet: m.ipContainer}
	if err := netlink.AddrAdd(containerVeth, addr); err != nil {
		return fmt.Errorf("failed to assign IP to container veth: %v", err)
	}

	// Enable loopback in the container's namespace
	lo, err := netlink.LinkByName("lo")
	if err != nil {
		return fmt.Errorf("failed to get loopback interface: %v", err)
	}
	if err := netlink.LinkSetUp(lo); err != nil {
		return fmt.Errorf("failed to set loopback up: %v", err)
	}

	// Update the spec to use the new network namespace
	spec.Linux.Namespaces = append(spec.Linux.Namespaces, specs.LinuxNamespace{
		Type: specs.NetworkNamespace,
		Path: fmt.Sprintf("/var/run/netns/%s", m.namespace),
	})

	// Switch back to the host namespace before setting up iptables
	if err := netns.Set(m.hostNS); err != nil {
		return fmt.Errorf("failed to switch back to host namespace: %v", err)
	}

	return nil
}

func (m *ContainerNetworkManager) setupBridge() error {
	// Check if the bridge already exists
	_, err := netlink.LinkByName(m.bridgeName)
	if err == nil {
		// Bridge already exists, nothing to do
		return nil
	}

	// Bridge does not exist, create it
	bridge := &netlink.Bridge{
		LinkAttrs: netlink.LinkAttrs{Name: m.bridgeName},
	}
	if err := netlink.LinkAdd(bridge); err != nil {
		return fmt.Errorf("failed to create bridge: %v", err)
	}

	// Set the bridge up
	if err := netlink.LinkSetUp(bridge); err != nil {
		return fmt.Errorf("failed to set bridge up: %v", err)
	}

	// Optionally, assign an IP to the bridge
	bridgeIP, bridgeNet, _ := net.ParseCIDR("192.168.0.1/16")
	addr := &netlink.Addr{IPNet: &net.IPNet{IP: bridgeIP, Mask: bridgeNet.Mask}}
	if err := netlink.AddrAdd(bridge, addr); err != nil {
		return fmt.Errorf("failed to assign IP to bridge: %v", err)
	}

	return nil
}

func (m *ContainerNetworkManager) TearDown() error {
	if err := netns.DeleteNamed(m.namespace); err != nil { // Delete the container network namespace
		return fmt.Errorf("failed to delete namespace: %v", err)
	}

	// Clean up iptables rules
	if err := m.teardownIptables(); err != nil {
		return err
	}

	return nil
}

func (m *ContainerNetworkManager) ExposePort(hostPort int) error {
	m.exposedPorts[hostPort] = m.containerPort

	// Set up iptables rules to forward traffic from hostPort to containerPort
	if err := m.setupIptables(hostPort, m.containerPort); err != nil {
		return err
	}

	return nil
}

func (m *ContainerNetworkManager) setupIptables(hostPort, containerPort int) error {
	containerIP := m.ipContainer.IP.String()

	// Set up the DNAT rule to forward traffic from hostPort to containerIP:containerPort
	dnatCmd := exec.Command("iptables", "-t", "nat", "-A", "PREROUTING", "-p", "tcp", "--dport", fmt.Sprintf("%d", hostPort), "-j", "DNAT", "--to-destination", fmt.Sprintf("%s:%d", containerIP, containerPort))
	if err := dnatCmd.Run(); err != nil {
		return fmt.Errorf("failed to set up DNAT rule: %v", err)
	}

	// Set up the FORWARD rule to allow traffic from the bridge to the container
	forwardCmd := exec.Command("iptables", "-A", "FORWARD", "-p", "tcp", "-d", containerIP, "--dport", fmt.Sprintf("%d", containerPort), "-j", "ACCEPT")
	if err := forwardCmd.Run(); err != nil {
		return fmt.Errorf("failed to set up FORWARD rule: %v", err)
	}

	// Set up the MASQUERADE rule to ensure correct return traffic routing
	masqueradeCmd := exec.Command("iptables", "-t", "nat", "-A", "POSTROUTING", "-s", containerIP, "-j", "MASQUERADE")
	if err := masqueradeCmd.Run(); err != nil {
		return fmt.Errorf("failed to set up MASQUERADE rule: %v", err)
	}

	fmt.Printf("Exposing container port %d on host port %d\n", containerPort, hostPort)
	return nil
}

func (m *ContainerNetworkManager) teardownIptables() error {
	for hostPort, containerPort := range m.exposedPorts {
		containerIP := m.ipContainer.IP.String()

		// Remove the DNAT rule
		dnatCmd := exec.Command("iptables", "-t", "nat", "-D", "PREROUTING", "-p", "tcp", "--dport", fmt.Sprintf("%d", hostPort), "-j", "DNAT", "--to-destination", fmt.Sprintf("%s:%d", containerIP, containerPort))
		if err := dnatCmd.Run(); err != nil {
			fmt.Printf("failed to remove DNAT rule for port %d: %v\n", hostPort, err)
		}

		// Remove the FORWARD rule
		forwardCmd := exec.Command("iptables", "-D", "FORWARD", "-p", "tcp", "-d", containerIP, "--dport", fmt.Sprintf("%d", containerPort), "-j", "ACCEPT")
		if err := forwardCmd.Run(); err != nil {
			fmt.Printf("failed to remove FORWARD rule for port %d: %v\n", hostPort, err)
		}

		// Remove the MASQUERADE rule
		masqueradeCmd := exec.Command("iptables", "-t", "nat", "-D", "POSTROUTING", "-s", containerIP, "-j", "MASQUERADE")
		if err := masqueradeCmd.Run(); err != nil {
			fmt.Printf("failed to remove MASQUERADE rule for IP %s: %v\n", containerIP, err)
		}

		fmt.Printf("Removed iptables rule for container port %d on host port %d\n", containerPort, hostPort)
	}
	return nil
}

// generateUniqueIPs generates a pair of unique IP addresses for the veth interfaces
func generateUniqueIPForContainer(subnet string) (*net.IPNet, error) {
	_, ipNet, err := net.ParseCIDR(subnet)
	if err != nil {
		return nil, err
	}

	baseIP := ipNet.IP
	for i := 2; i < 255; i++ { // Start at .2 to avoid .1, which is often the gateway
		ip := net.IPv4(baseIP[0], baseIP[1], baseIP[2], byte(i))
		if !isIPInUse(ip.String()) {
			return &net.IPNet{IP: ip, Mask: ipNet.Mask}, nil
		}
	}

	return nil, fmt.Errorf("no available IPs in subnet %s", subnet)
}

func isIPInUse(ip string) bool {
	interfaces, err := net.Interfaces()
	if err != nil {
		return false // Default to assuming the IP is not in use if we can't get interfaces
	}

	for _, iface := range interfaces {
		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}

		for _, addr := range addrs {
			if ip == addr.(*net.IPNet).IP.String() {
				return true
			}
		}
	}

	return false
}

func (m *ContainerNetworkManager) GetRandomPort() int {
	return m.randomPort
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

// getIPFromInterface gets the IP address from a given interface.
// Returns an error if the interface doesn't exist or no IP is found.
func getIPFromInterface(ifname string) (string, error) {
	iface, err := net.InterfaceByName(ifname)
	if err != nil {
		return "", err
	}

	addrs, err := iface.Addrs()
	if err != nil {
		return "", err
	}

	if len(addrs) < 1 {
		return "", fmt.Errorf("no ip addresses found on <%s> interface", ifname)
	}

	ip, _, err := net.ParseCIDR(addrs[0].String())
	if err != nil {
		return "", err
	}

	return ip.String(), nil
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
