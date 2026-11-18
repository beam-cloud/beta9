package types

import (
	"fmt"
	"strings"
)

const (
	BackendRouteScheme               = "route"
	BackendRouteTransportDirect      = "direct"
	BackendRouteTransportTSNet       = "tsnet_restricted"
	BackendRouteTransportLocalDirect = "local_direct"
	BackendRouteStateOpening         = "opening"
	BackendRouteStateReady           = "ready"
	BackendRouteStateDegraded        = "degraded"
	BackendRouteStateClosing         = "closing"
	BackendRouteProtocolTCP          = "tcp"
	BackendRouteKindWorker           = "worker"
	BackendRouteKindContainer        = "container"
	DefaultAgentTSNetRouteProxyPort  = 29443
	DefaultAgentWorkerContainerMode  = "worker-container"
	DefaultAgentLocalDevMode         = "local-dev"
)

type BackendRoute struct {
	RouteID     string `json:"route_id"`
	WorkspaceID string `json:"workspace_id"`
	PoolName    string `json:"pool_name"`
	MachineID   string `json:"machine_id"`
	WorkerID    string `json:"worker_id"`
	ContainerID string `json:"container_id"`
	Kind        string `json:"kind"`
	Port        int32  `json:"port"`
	Protocol    string `json:"protocol"`
	Transport   string `json:"transport"`
	LocalTarget string `json:"local_target"`
	ProxyTarget string `json:"proxy_target"`
	State       string `json:"state"`
	Error       string `json:"error"`
	UpdatedAt   int64  `json:"updated_at"`
}

func BackendRouteAddress(routeID string) string {
	return fmt.Sprintf("%s://%s", BackendRouteScheme, routeID)
}

// NormalizeBackendRouteTransport canonicalizes transport names (dashes to
// underscores) and applies the default.
func NormalizeBackendRouteTransport(transport string) string {
	transport = strings.TrimSpace(strings.ReplaceAll(transport, "-", "_"))
	if transport == "" {
		return BackendRouteTransportTSNet
	}
	return transport
}

func BackendRouteID(machineID, workerID, containerID, kind string, port int32) string {
	return strings.Join([]string{machineID, workerID, containerID, kind, fmt.Sprintf("%d", port)}, ":")
}

func ParseBackendRouteAddress(address string) (string, bool) {
	prefix := BackendRouteScheme + "://"
	if !strings.HasPrefix(address, prefix) {
		return "", false
	}
	routeID := strings.TrimPrefix(address, prefix)
	if routeID == "" {
		return "", false
	}
	return routeID, true
}
