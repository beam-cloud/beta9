package worker

import (
	"fmt"
	"net"
	"strconv"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/opencontainers/runtime-spec/specs-go"
)

type ContainerNetwork interface {
	Setup(containerId string, spec *specs.Spec, request *types.ContainerRequest) error
	TearDown(containerId string) error
	ExposePort(containerId string, hostPort, containerPort int) error
	ExposePorts(containerId string, bindings []PortBinding) error
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
	return net.JoinHostPort(m.podAddr, strconv.Itoa(binding.HostPort)), nil
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

func (m *agentContainerNetwork) ContainerPortAddress(containerId string, binding PortBinding) (string, error) {
	info, err := m.getContainerNetworkInfo(containerId)
	if err != nil {
		return "", err
	}
	if info.ContainerIp == "" {
		return "", fmt.Errorf("container %s has no bridge IP", containerId)
	}
	return net.JoinHostPort(info.ContainerIp, strconv.Itoa(binding.ContainerPort)), nil
}

func (m *agentContainerNetwork) ContainerPortAddressMap(containerId string, bindings []PortBinding) (map[int32]string, error) {
	info, err := m.getContainerNetworkInfo(containerId)
	if err != nil {
		return nil, err
	}
	if info.ContainerIp == "" {
		return nil, fmt.Errorf("container %s has no bridge IP", containerId)
	}

	addressMap := make(map[int32]string, len(bindings))
	for _, binding := range bindings {
		addressMap[int32(binding.ContainerPort)] = net.JoinHostPort(info.ContainerIp, strconv.Itoa(binding.ContainerPort))
	}
	return addressMap, nil
}

func newContainerNetwork(base *ContainerNetworkManager, podAddr string, persistent bool, machineID, transport string) ContainerNetwork {
	local := &localContainerNetwork{
		ContainerNetworkManager: base,
		podAddr:                 podAddr,
	}
	if persistent && machineID != "" && transport != "" {
		return &agentContainerNetwork{localContainerNetwork: local}
	}
	return local
}
