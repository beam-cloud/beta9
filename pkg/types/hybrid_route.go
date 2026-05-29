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
	DefaultHybridTSNetRouteProxyPort = DefaultAgentTSNetRouteProxyPort
	DefaultAgentWorkerContainerMode  = "worker-container"
	DefaultHybridWorkerContainerMode = DefaultAgentWorkerContainerMode
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
