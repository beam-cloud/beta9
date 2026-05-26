package worker

import (
	"context"
	"fmt"
	"net"

	"github.com/beam-cloud/beta9/pkg/common"
	pb "github.com/beam-cloud/beta9/proto"
)

func validateCIDR(entry string) (string, bool, error) {
	return common.ValidateCIDR(entry)
}

type containerNetworkInfo struct {
	ContainerIp   string
	ContainerIpv6 string // empty if IPv6 is not enabled
	Namespace     string
	VethHost      string
	Comment       string
}

// getContainerNetworkInfo retrieves the container's network information including
// IPv4 address, derived IPv6 address, veth host name, and iptables comment.
func getContainerNetworkInfo(ctx context.Context, workerRepoClient pb.WorkerRepositoryServiceClient, networkPrefix, containerId string, ipv6Enabled bool) (*containerNetworkInfo, error) {
	containerIpResponse, err := handleGRPCResponse(workerRepoClient.GetContainerIp(ctx, &pb.GetContainerIpRequest{
		NetworkPrefix: networkPrefix,
		ContainerId:   containerId,
	}))
	if err != nil {
		return nil, err
	}

	return containerNetworkInfoFromIP(containerId, containerIpResponse.IpAddress, ipv6Enabled)
}

func containerNetworkInfoFromIP(containerId string, containerIp string, ipv6Enabled bool) (*containerNetworkInfo, error) {
	vethHost, _ := containerVethNames(containerId)

	info := &containerNetworkInfo{
		ContainerIp: containerIp,
		Namespace:   containerId,
		VethHost:    vethHost,
		Comment:     containerNetworkComment(vethHost, containerId, containerId),
	}

	if ipv6Enabled {
		ip := net.ParseIP(containerIp)
		if ip == nil || ip.To4() == nil {
			return nil, fmt.Errorf("invalid IPv4 address: %s", containerIp)
		}
		_, ipv6Net, err := net.ParseCIDR(containerSubnetIPv6)
		if err != nil {
			return nil, fmt.Errorf("failed to parse IPv6 subnet: %w", err)
		}
		ipv6Address, err := containerIPv6Address(ip, ipv6Net)
		if err != nil {
			return nil, err
		}
		info.ContainerIpv6 = ipv6Address.String()
	}

	return info, nil
}

func containerNetworkInfoFromSlot(containerId string, slot *containerNetworkSlot, ipv6Enabled bool) (*containerNetworkInfo, error) {
	if slot == nil {
		return nil, fmt.Errorf("missing network slot for container %s", containerId)
	}

	namespace := slot.namespace
	if namespace == "" {
		namespace = slot.id
	}
	vethHost := slot.vethHost
	if vethHost == "" {
		vethHost, _ = containerVethNames(namespace)
	}

	info := &containerNetworkInfo{
		ContainerIp: slot.ip,
		Namespace:   namespace,
		VethHost:    vethHost,
		Comment:     containerNetworkComment(vethHost, containerId, namespace),
	}

	if ipv6Enabled {
		if slot.ipv6 != "" {
			info.ContainerIpv6 = slot.ipv6
		} else {
			ip := net.ParseIP(slot.ip)
			if ip == nil || ip.To4() == nil {
				return nil, fmt.Errorf("invalid IPv4 address: %s", slot.ip)
			}
			_, ipv6Net, err := net.ParseCIDR(containerSubnetIPv6)
			if err != nil {
				return nil, fmt.Errorf("failed to parse IPv6 subnet: %w", err)
			}
			ipv6Address, err := containerIPv6Address(ip, ipv6Net)
			if err != nil {
				return nil, err
			}
			info.ContainerIpv6 = ipv6Address.String()
		}
	}

	return info, nil
}
