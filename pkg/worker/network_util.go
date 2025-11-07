package worker

import (
	"context"
	"fmt"
	"net"

	pb "github.com/beam-cloud/beta9/proto"
)

// validateCIDR validates CIDR notation and returns:
// - normalized CIDR string
// - boolean indicating if it's IPv6
// - error if invalid
func validateCIDR(entry string) (string, bool, error) {
	_, ipNet, err := net.ParseCIDR(entry)
	if err != nil {
		return "", false, fmt.Errorf("not a valid CIDR notation (e.g., 8.8.8.8/32 or 10.0.0.0/8)")
	}

	isIPv6 := ipNet.IP.To4() == nil
	return ipNet.String(), isIPv6, nil
}

type containerNetworkInfo struct {
	ContainerIp   string
	ContainerIpv6 string // empty if IPv6 is not enabled
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

	truncatedContainerId := containerId[len(containerId)-5:]
	vethHost := fmt.Sprintf("%s%s", containerVethHostPrefix, truncatedContainerId)
	comment := fmt.Sprintf("%s:%s", vethHost, containerId)
	containerIp := containerIpResponse.IpAddress

	info := &containerNetworkInfo{
		ContainerIp: containerIp,
		VethHost:    vethHost,
		Comment:     comment,
	}

	if ipv6Enabled {
		ip := net.ParseIP(containerIp)
		if ip == nil {
			return nil, fmt.Errorf("invalid IPv4 address: %s", containerIp)
		}
		ipv4LastOctet := int(ip.To4()[3])
		_, ipv6Net, err := net.ParseCIDR(containerSubnetIPv6)
		if err != nil {
			return nil, fmt.Errorf("failed to parse IPv6 subnet: %w", err)
		}
		ipv6Prefix := ipv6Net.IP.String()
		info.ContainerIpv6 = fmt.Sprintf("%s%x", ipv6Prefix, ipv4LastOctet)
	}

	return info, nil
}
