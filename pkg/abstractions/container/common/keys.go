package container_common

import "fmt"

var Keys = &keys{}

type keys struct{}

var (
	containerTunnelAddress string = "container:%s:tunnel:%s"
)

func (k *keys) ContainerTunnelAddress(workspaceName, containerId string) string {
	return fmt.Sprintf(containerTunnelAddress, workspaceName, containerId)
}
