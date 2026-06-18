package worker

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/beam-cloud/beta9/pkg/metrics"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/rs/zerolog/log"
)

func (s *Worker) backendRouteFor(request *types.ContainerRequest, kind string, port int32, localTarget string) *pb.BackendRoute {
	if !s.agentWorker() {
		return nil
	}
	localTarget = s.agentRouteLocalTarget(localTarget)
	return &pb.BackendRoute{
		RouteId:     types.BackendRouteID(s.machineID, s.workerId, request.ContainerId, kind, port),
		WorkspaceId: request.WorkspaceId,
		PoolName:    s.poolName,
		MachineId:   s.machineID,
		WorkerId:    s.workerId,
		ContainerId: request.ContainerId,
		Kind:        kind,
		Port:        port,
		Protocol:    types.BackendRouteProtocolTCP,
		Transport:   s.routeTransport,
		LocalTarget: localTarget,
		State:       types.BackendRouteStateOpening,
	}
}

func (s *Worker) registerContainerPorts(ctx context.Context, request *types.ContainerRequest, bindings []PortBinding) error {
	if len(bindings) == 0 {
		return nil
	}

	phaseStart := time.Now()
	addressMap, err := s.containerPortAddressMap(request.ContainerId, bindings)
	if err != nil {
		metrics.RecordWorkerStartupPhase("set_container_address_map", time.Since(phaseStart), request, map[string]string{"success": "false"})
		s.recordStartupLifecycle(ctx, request, types.ContainerLifecycleSetAddressMap, phaseStart, false, map[string]string{"port_count": fmt.Sprintf("%d", len(bindings))})
		return err
	}
	s.cacheContainerAddressMap(request.ContainerId, addressMap)

	primaryPort := int32(bindings[0].ContainerPort)
	primaryTarget := addressMap[primaryPort]
	if primaryTarget == "" {
		return fmt.Errorf("container %s has no address for port %d", request.ContainerId, primaryPort)
	}

	addressPhaseStart := time.Now()
	if _, err := handleGRPCResponse(s.containerRepoClient.SetContainerAddress(context.Background(), &pb.SetContainerAddressRequest{
		ContainerId: request.ContainerId,
		Address:     primaryTarget,
		Route:       s.backendRouteFor(request, types.BackendRouteKindContainer, primaryPort, primaryTarget),
	})); err != nil {
		metrics.RecordWorkerStartupPhase("set_container_address", time.Since(addressPhaseStart), request, map[string]string{"success": "false"})
		s.recordStartupLifecycle(ctx, request, types.ContainerLifecycleSetContainerAddr, addressPhaseStart, false, nil)
		return err
	}
	metrics.RecordWorkerStartupPhase("set_container_address", time.Since(addressPhaseStart), request, map[string]string{"success": "true"})
	s.recordStartupLifecycle(ctx, request, types.ContainerLifecycleSetContainerAddr, addressPhaseStart, true, nil)

	routes := make([]*pb.BackendRoute, 0, len(bindings))
	for _, binding := range bindings {
		containerPort := int32(binding.ContainerPort)
		localTarget := addressMap[containerPort]
		if route := s.backendRouteFor(request, types.BackendRouteKindContainer, containerPort, localTarget); route != nil {
			routes = append(routes, route)
		}
	}

	mapPhaseStart := time.Now()
	if _, err := handleGRPCResponse(s.containerRepoClient.SetContainerAddressMap(context.Background(), &pb.SetContainerAddressMapRequest{
		ContainerId: request.ContainerId,
		AddressMap:  addressMap,
		Routes:      routes,
	})); err != nil {
		metrics.RecordWorkerStartupPhase("set_container_address_map", time.Since(mapPhaseStart), request, map[string]string{"success": "false"})
		s.recordStartupLifecycle(ctx, request, types.ContainerLifecycleSetAddressMap, mapPhaseStart, false, map[string]string{"port_count": fmt.Sprintf("%d", len(addressMap))})
		return err
	}
	metrics.RecordWorkerStartupPhase("set_container_address_map", time.Since(mapPhaseStart), request, map[string]string{
		"port_count": fmt.Sprintf("%d", len(addressMap)),
		"success":    "true",
	})
	s.recordStartupLifecycle(ctx, request, types.ContainerLifecycleSetAddressMap, mapPhaseStart, true, map[string]string{"port_count": fmt.Sprintf("%d", len(addressMap))})

	log.Info().
		Str("container_id", request.ContainerId).
		Str("stub_type", request.Stub.Type.Kind()).
		Int("port_count", len(addressMap)).
		Int("route_count", len(routes)).
		Interface("address_map", addressMap).
		Msg("registered container network addresses")

	return nil
}

func (s *Worker) cacheContainerAddressMap(containerId string, addressMap map[int32]string) {
	if len(addressMap) == 0 || s.containerInstances == nil {
		return
	}

	instance, exists := s.containerInstances.Get(containerId)
	if !exists || instance == nil {
		return
	}

	instance.ContainerAddressMap = cloneContainerAddressMap(addressMap)
	s.containerInstances.Set(containerId, instance)
}

func cloneContainerAddressMap(addressMap map[int32]string) map[int32]string {
	if len(addressMap) == 0 {
		return nil
	}

	cloned := make(map[int32]string, len(addressMap))
	for port, address := range addressMap {
		cloned[port] = address
	}
	return cloned
}

func (s *Worker) containerPortAddressMap(containerId string, bindings []PortBinding) (map[int32]string, error) {
	if s.containerNetworkManager == nil {
		return nil, fmt.Errorf("container network manager unavailable")
	}
	return s.containerNetworkManager.ContainerPortAddressMap(containerId, bindings)
}

func (s *Worker) agentRouteLocalTarget(localTarget string) string {
	if s.routeLocalTargetHost == "" {
		return localTarget
	}

	host, port, err := net.SplitHostPort(localTarget)
	if err != nil {
		return localTarget
	}
	if ip := net.ParseIP(host); ip != nil && !ip.IsLoopback() {
		return localTarget
	}
	return net.JoinHostPort(s.routeLocalTargetHost, port)
}
