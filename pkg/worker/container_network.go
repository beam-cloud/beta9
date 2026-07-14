package worker

import (
	"fmt"
	"net"
	"strconv"
	"strings"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/opencontainers/runtime-spec/specs-go"
)

type ContainerNetwork interface {
	Setup(containerId string, spec *specs.Spec, request *types.ContainerRequest) error
	TearDown(containerId string) error
	ExposePort(containerId string, hostPort, containerPort int) error
	ExposePorts(containerId string, bindings []PortBinding) error
	ReservePorts(containerId string, count int) ([]int, error)
	ReleasePortReservations(containerId string)
	UpdateNetworkPermissions(containerId string, request *types.ContainerRequest) error
	ContainerPortAddress(containerId string, binding PortBinding) (string, error)
	ContainerPortAddressMap(containerId string, bindings []PortBinding) (map[int32]string, error)
	Close() error
}

type localContainerNetwork struct {
	*ContainerNetworkManager
	podAddr string
}

var _ ContainerNetwork = (*localContainerNetwork)(nil)
var _ ContainerNetwork = (*agentContainerNetwork)(nil)

func (m *localContainerNetwork) ContainerPortAddress(_ string, binding PortBinding) (string, error) {
	if m.podAddr == "" {
		return "", fmt.Errorf("pod address is empty")
	}
	return joinHostPort(m.podAddr, binding.HostPort), nil
}

func (m *localContainerNetwork) ContainerPortAddressMap(containerId string, bindings []PortBinding) (map[int32]string, error) {
	addressMap := make(map[int32]string, len(bindings))
	for _, binding := range bindings {
		address, err := m.ContainerPortAddress(containerId, binding)
		if err != nil {
			return nil, err
		}
		addressMap[int32(binding.ContainerPort)] = address
	}
	return addressMap, nil
}

type agentContainerNetwork struct {
	*localContainerNetwork
}

func joinHostPort(host string, port int) string {
	host = strings.TrimSpace(host)
	if strings.HasPrefix(host, "[") && strings.HasSuffix(host, "]") {
		unwrapped := strings.TrimSuffix(strings.TrimPrefix(host, "["), "]")
		if net.ParseIP(unwrapped) != nil {
			host = unwrapped
		}
	}
	return net.JoinHostPort(host, strconv.Itoa(port))
}

func newContainerNetwork(base *ContainerNetworkManager, podAddr string, persistent bool, machineID, transport, routeLocalTargetHost string) ContainerNetwork {
	base.podAddr = podAddr
	base.forcePortProxy = hostRequiresPortProxy(podAddr) || hostRequiresPortProxy(routeLocalTargetHost)
	local := &localContainerNetwork{
		ContainerNetworkManager: base,
		podAddr:                 podAddr,
	}
	if persistent && machineID != "" && transport != "" {
		return &agentContainerNetwork{localContainerNetwork: local}
	}
	return local
}

func hostRequiresPortProxy(host string) bool {
	host = strings.TrimSpace(host)
	if host == "" {
		return false
	}
	if strings.EqualFold(host, "localhost") {
		return true
	}
	if strings.HasPrefix(host, "[") && strings.HasSuffix(host, "]") {
		host = strings.TrimSuffix(strings.TrimPrefix(host, "["), "]")
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}
