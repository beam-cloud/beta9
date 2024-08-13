package worker

import (
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"

	"github.com/opencontainers/runtime-spec/specs-go"
	"github.com/vishvananda/netlink"
	"github.com/vishvananda/netns"
	"golang.org/x/sys/unix"
)

type ContainerNetworkManager struct {
	containerId   string
	namespace     string
	vethHost      string
	vethContainer string
	containerPort int
	exposedPorts  map[int]int // Map of hostPort -> containerPort
}

func NewContainerNetworkManager(containerId string, containerPort int) *ContainerNetworkManager {
	namespace := "testns" // fmt.Sprintf("ns_%s", containerId)
	vethHost := "veth_host"
	vethContainer := "veth_container"

	return &ContainerNetworkManager{
		containerId:   containerId,
		namespace:     namespace,
		vethHost:      vethHost,
		vethContainer: vethContainer,
		containerPort: containerPort,
		exposedPorts:  make(map[int]int),
	}
}

func (m *ContainerNetworkManager) Setup(spec *specs.Spec) error {
	// Create a new network namespace for the container
	nsPath := filepath.Join("/var/run/netns", m.namespace)

	// Save the current (host) namespace
	hostNs, err := netns.Get()
	if err != nil {
		return fmt.Errorf("failed to get host namespace: %v", err)
	}
	defer hostNs.Close()

	// Create a veth pair in the host namespace
	hostVeth, containerVeth, err := createVethPair(m.vethHost, m.vethContainer)
	if err != nil {
		return fmt.Errorf("failed to create veth pair: %v", err)
	}

	// Set up the bridge in the host namespace and add the host side of the veth pair to it
	bridgeName := "br0" // or any name you prefer
	if err := setupBridge(bridgeName, hostVeth); err != nil {
		return fmt.Errorf("failed to setup bridge: %v", err)
	}

	newNs, err := netns.NewNamed(m.namespace)
	if err != nil {
		return fmt.Errorf("failed to create network namespace: %v", err)
	}
	defer newNs.Close()

	// Move the container side of the veth pair into the new namespace
	log.Printf("Moving interface %s to namespace %s", containerVeth.Attrs().Name, m.namespace)
	if err := setLinkNamespace(containerVeth, newNs); err != nil {
		return fmt.Errorf("failed to assign veth to namespace: %v", err)
	}

	// Update the spec to use the new network namespace
	spec.Linux.Namespaces = append(spec.Linux.Namespaces, specs.LinuxNamespace{
		Type: specs.NetworkNamespace,
		Path: nsPath,
	})

	// Configure the network inside the container's namespace
	err = netns.Set(newNs)
	if err != nil {
		return fmt.Errorf("failed to set namespace: %v", err)
	}
	defer netns.Set(netns.None()) // Reset to the original namespace after configuration

	// if err := configureContainerNetwork(containerVeth); err != nil {
	// 	return fmt.Errorf("failed to configure container network: %v", err)
	// }

	return nil
}

func createVethPair(hostVethName, containerVethName string) (*netlink.Veth, netlink.Link, error) {
	hostVeth := &netlink.Veth{
		LinkAttrs: netlink.LinkAttrs{Name: hostVethName},
		PeerName:  containerVethName,
	}

	if err := netlink.LinkAdd(hostVeth); err != nil {
		return nil, nil, err
	}

	containerVeth, err := netlink.LinkByName(containerVethName)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get container veth: %v", err)
	}

	log.Println(containerVeth)

	return hostVeth, containerVeth, nil
}

func setupBridge(bridgeName string, veth *netlink.Veth) error {
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

	return netlink.LinkSetUp(veth) // Bring up the host side of the veth pair
}

func configureContainerNetwork(containerVeth *netlink.Veth) error {
	// Set up the container's side of the veth pair
	if err := netlink.LinkSetUp(containerVeth); err != nil {
		return err
	}

	// Assign an IP address (example 192.168.1.2/24)
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
		Gw:        net.ParseIP("192.168.1.1"), // Example gateway IP
	}

	return netlink.RouteAdd(defaultRoute)
}

func setLinkNamespace(link netlink.Link, netns netns.NsHandle) error {
	return netlink.LinkSetNsFd(link, int(netns))
}

func (m *ContainerNetworkManager) TearDown() error {
	// Implement the tear down logic if needed
	return nil
}

func (m *ContainerNetworkManager) ExposePort(hostPort int) error {
	m.exposedPorts[hostPort] = m.containerPort
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
