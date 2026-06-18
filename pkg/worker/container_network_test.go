package worker

import (
	"testing"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/stretchr/testify/require"
)

func TestNewContainerNetworkUsesLocalImplementationByDefault(t *testing.T) {
	network := newContainerNetwork(&ContainerNetworkManager{}, "10.0.0.2", false, "", "")

	_, ok := network.(*localContainerNetwork)
	require.True(t, ok)

	address, err := network.ContainerPortAddress("container-one", PortBinding{HostPort: 32000, ContainerPort: 8001})
	require.NoError(t, err)
	require.Equal(t, "10.0.0.2:32000", address)

	addressMap, err := network.ContainerPortAddressMap("container-one", []PortBinding{
		{HostPort: 32000, ContainerPort: 8001},
		{HostPort: 32001, ContainerPort: 2222},
	})
	require.NoError(t, err)
	require.Equal(t, map[int32]string{
		8001: "10.0.0.2:32000",
		2222: "10.0.0.2:32001",
	}, addressMap)
}

func TestNewContainerNetworkFormatsBracketedIPv6PodAddress(t *testing.T) {
	network := newContainerNetwork(&ContainerNetworkManager{}, "[2600:1f18:37a4:c02::7286]", false, "", "")

	address, err := network.ContainerPortAddress("container-one", PortBinding{HostPort: 32000, ContainerPort: 8001})
	require.NoError(t, err)
	require.Equal(t, "[2600:1f18:37a4:c02::7286]:32000", address)

	addressMap, err := network.ContainerPortAddressMap("container-one", []PortBinding{
		{HostPort: 32000, ContainerPort: 8001},
		{HostPort: 32001, ContainerPort: 2222},
	})
	require.NoError(t, err)
	require.Equal(t, map[int32]string{
		8001: "[2600:1f18:37a4:c02::7286]:32000",
		2222: "[2600:1f18:37a4:c02::7286]:32001",
	}, addressMap)
}

func TestNewContainerNetworkUsesAgentImplementationForPersistentMachine(t *testing.T) {
	containerID := "container-one"
	containerInstances := common.NewSafeMap[*ContainerInstance]()
	containerInstances.Set(containerID, &ContainerInstance{
		Id:          containerID,
		ContainerIp: "192.168.0.44",
	})

	network := newContainerNetwork(&ContainerNetworkManager{containerInstances: containerInstances}, "127.0.0.1", true, "machine-one", "tsnet_restricted")

	_, ok := network.(*agentContainerNetwork)
	require.True(t, ok)

	address, err := network.ContainerPortAddress(containerID, PortBinding{HostPort: 32000, ContainerPort: 8001})
	require.NoError(t, err)
	require.Equal(t, "192.168.0.44:8001", address)

	addressMap, err := network.ContainerPortAddressMap(containerID, []PortBinding{
		{HostPort: 32000, ContainerPort: 8001},
		{HostPort: 32001, ContainerPort: 2222},
	})
	require.NoError(t, err)
	require.Equal(t, map[int32]string{
		8001: "192.168.0.44:8001",
		2222: "192.168.0.44:2222",
	}, addressMap)
}

func TestNewContainerNetworkFormatsAgentIPv6ContainerAddress(t *testing.T) {
	containerID := "container-one"
	containerInstances := common.NewSafeMap[*ContainerInstance]()
	containerInstances.Set(containerID, &ContainerInstance{
		Id:          containerID,
		ContainerIp: "fd00:abcd::3f",
	})

	network := newContainerNetwork(&ContainerNetworkManager{containerInstances: containerInstances}, "127.0.0.1", true, "machine-one", "tsnet_restricted")

	address, err := network.ContainerPortAddress(containerID, PortBinding{HostPort: 32000, ContainerPort: 8001})
	require.NoError(t, err)
	require.Equal(t, "[fd00:abcd::3f]:8001", address)

	addressMap, err := network.ContainerPortAddressMap(containerID, []PortBinding{
		{HostPort: 32000, ContainerPort: 8001},
		{HostPort: 32001, ContainerPort: 2222},
	})
	require.NoError(t, err)
	require.Equal(t, map[int32]string{
		8001: "[fd00:abcd::3f]:8001",
		2222: "[fd00:abcd::3f]:2222",
	}, addressMap)
}
