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

const (
	containerBridge string = "br0"
)

type ContainerNetworkManager struct {
	containerId   string
	namespace     string
	bridge        string
	vethHost      string
	vethContainer string
	containerPort int
	exposedPorts  map[int]int // Map of hostPort -> containerPort
}

func NewContainerNetworkManager(containerId string, containerPort int) *ContainerNetworkManager {
	truncatedId := containerId[len(containerId)-8:]

	namespace := containerId
	vethHost := fmt.Sprintf("veth_h_%s", truncatedId)
	vethContainer := fmt.Sprintf("veth_c_%s", truncatedId)
	bridge := containerBridge

	return &ContainerNetworkManager{
		containerId:   containerId,
		namespace:     namespace,
		bridge:        bridge,
		vethHost:      vethHost,
		vethContainer: vethContainer,
		containerPort: containerPort,
		exposedPorts:  make(map[int]int),
	}
}

func (m *ContainerNetworkManager) Setup(spec *specs.Spec) error {
	hostNS, err := netns.Get()
	if err != nil {
		log.Fatalf("Failed to get host namespace: %v", err)
	}
	defer hostNS.Close()

	nsPath := filepath.Join("/var/run/netns", m.namespace)

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

	if err := setupBridge(m.bridge, hostVeth); err != nil {
		return err
	}

	newNs, err := netns.NewNamed(m.namespace)
	if err != nil {
		return err
	}
	defer newNs.Close()

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

	// Update the spec to use the new network namespace
	spec.Linux.Namespaces = append(spec.Linux.Namespaces, specs.LinuxNamespace{
		Type: specs.NetworkNamespace,
		Path: nsPath,
	})

	// Configure the network inside the container's namespace
	err = netns.Set(newNs)
	if err != nil {
		return err
	}
	defer netns.Set(hostNS) // Reset to the original namespace after setting up the container network

	if err := configureContainerNetwork(containerVeth); err != nil {
		return err
	}

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
