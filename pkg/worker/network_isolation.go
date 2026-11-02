package worker

import (
	"fmt"
	"net"
	"net/url"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/coreos/go-iptables/iptables"
	"github.com/rs/zerolog/log"
)

// containerIsolationChain filters what containers and image builds send
// through the worker's bridges. It runs in mangle PREROUTING, ahead of the
// filter rules that apply allow lists, so no allow list can lift it, and it
// sees each destination before the egress firewall redirects it.
const containerIsolationChain = "B9-ISOLATION"

const containerIsolationRefreshInterval = time.Minute

// buildahBridgeLinkName is the bridge buildah attaches RUN steps to.
const buildahBridgeLinkName = "cni-podman0"

// Containers never reach link-local addresses (which include the instance
// metadata service) or other containers. Off private pools they never reach
// private ranges either; on a private pool those belong to the machine's
// owner, and allow lists may open them.
var (
	isolatedNetworks        = []string{"169.254.0.0/16", containerSubnet, "fe80::/10", containerSubnetIPv6, "fd00:ec2::254/128"}
	isolatedPrivateNetworks = []string{"10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16", "100.64.0.0/10", "fc00::/7", "64:ff9b::/96"}
)

type isolationEndpoint struct {
	address  string
	protocol string
	port     int
}

// hostNetworks returns the /56 around each of the host's global IPv6
// addresses. Cloud VPCs number nodes, pods and managed databases from one
// globally routable block, a /56 on AWS, that the private ranges miss.
func hostNetworks(addresses []net.Addr) []string {
	mask := net.CIDRMask(56, 128)
	var networks []string
	for _, address := range addresses {
		ipNet, ok := address.(*net.IPNet)
		if !ok || ipNet.IP.To4() != nil || !ipNet.IP.IsGlobalUnicast() || ipNet.IP.IsPrivate() {
			continue
		}
		network := (&net.IPNet{IP: ipNet.IP.Mask(mask), Mask: mask}).String()
		if !slices.Contains(networks, network) {
			networks = append(networks, network)
		}
	}
	return networks
}

func containerIsolationRules(ipv6, privateNetworks bool, hostNetworks []string, endpoints []isolationEndpoint) (allow, deny [][]string) {
	allow = append(allow, []string{"-m", "conntrack", "--ctstate", "ESTABLISHED,RELATED", "-j", "RETURN"})
	if ipv6 {
		// Router and neighbor discovery.
		for _, icmpType := range []string{"133", "135", "136"} {
			allow = append(allow, []string{"-p", "ipv6-icmp", "--icmpv6-type", icmpType, "-j", "RETURN"})
		}
	}
	for _, endpoint := range endpoints {
		if strings.Contains(endpoint.address, ":") == ipv6 {
			allow = append(allow, []string{"-d", endpoint.address, "-p", endpoint.protocol, "--dport", strconv.Itoa(endpoint.port), "-j", "RETURN"})
		}
	}

	deny = append(deny, []string{"-m", "addrtype", "--dst-type", "LOCAL", "-j", "DROP"})
	networks := append([]string(nil), isolatedNetworks...)
	if !privateNetworks {
		networks = append(networks, isolatedPrivateNetworks...)
	}
	for _, network := range append(networks, hostNetworks...) {
		if strings.Contains(network, ":") == ipv6 {
			deny = append(deny, []string{"-d", network, "-j", "DROP"})
		}
	}
	return allow, deny
}

// isolateContainers installs containerIsolationChain and lets containers reach
// endpoints the chain does not let through yet. Workers sharing a host network
// share the chain, so each adds its endpoints ahead of the drops. An address an
// endpoint stops resolving to stays open, since containers may still hold DNS
// answers that name it.
func (m *ContainerNetworkManager) isolateContainers() error {
	endpoints, err := m.isolationEndpoints()
	if err != nil {
		return err
	}
	endpoints = slices.DeleteFunc(endpoints, func(endpoint isolationEndpoint) bool {
		_, isolated := m.isolatedEndpoints[endpoint]
		return isolated
	})
	if len(endpoints) == 0 && m.isolatedEndpoints != nil {
		return nil
	}

	m.iptablesMu.Lock()
	defer m.iptablesMu.Unlock()
	for _, family := range []struct {
		ipt  *iptables.IPTables
		ipv6 bool
	}{{m.ipt, false}, {m.ipt6, true}} {
		if family.ipt == nil {
			continue
		}
		exists, err := family.ipt.ChainExists("mangle", containerIsolationChain)
		if err != nil {
			return err
		}
		if !exists {
			if err := family.ipt.NewChain("mangle", containerIsolationChain); err != nil {
				return err
			}
		}
		allow, deny := containerIsolationRules(family.ipv6, m.privateNetworks, m.hostNetworks, endpoints)
		for _, rule := range allow {
			if err := family.ipt.InsertUnique("mangle", containerIsolationChain, 1, rule...); err != nil {
				return err
			}
		}
		for _, rule := range deny {
			if err := family.ipt.AppendUnique("mangle", containerIsolationChain, rule...); err != nil {
				return err
			}
		}
		for _, bridge := range []string{containerBridgeLinkName, buildahBridgeLinkName} {
			if err := family.ipt.InsertUnique("mangle", "PREROUTING", 1, "-i", bridge, "-j", containerIsolationChain); err != nil {
				return err
			}
		}
	}
	if m.isolatedEndpoints == nil {
		m.isolatedEndpoints = map[isolationEndpoint]struct{}{}
	}
	for _, endpoint := range endpoints {
		m.isolatedEndpoints[endpoint] = struct{}{}
	}
	return nil
}

// refreshContainerIsolation follows endpoints to new addresses, such as a load
// balancer's, and retries any that did not resolve before.
func (m *ContainerNetworkManager) refreshContainerIsolation() {
	ticker := time.NewTicker(containerIsolationRefreshInterval)
	defer ticker.Stop()

	for {
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C:
			if err := m.isolateContainers(); err != nil {
				log.Warn().Err(err).Msg("failed to refresh container network isolation")
			}
		}
	}
}

// isolationEndpoints are the destinations containers and builds keep reaching
// inside isolated networks: DNS resolvers, the service proxy, the gateway, the
// registries builds pull from and push to, and the apt mirror and proxy.
func (m *ContainerNetworkManager) isolationEndpoints() ([]isolationEndpoint, error) {
	var endpoints []isolationEndpoint
	for _, path := range []string{hostResolvConfPath, containerResolvConfSource(m.config.Worker.UseHostResolvConf, hostResolvConfPath)} {
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		for _, line := range strings.Split(string(data), "\n") {
			if fields := strings.Fields(line); len(fields) >= 2 && fields[0] == "nameserver" {
				endpoints = append(endpoints, isolationEndpoint{fields[1], "udp", 53}, isolationEndpoint{fields[1], "tcp", 53})
			}
		}
	}

	for _, port := range []int{m.config.Abstractions.Pod.TCP.ExternalPort, m.config.GatewayService.HTTP.ExternalPort} {
		if port > 0 {
			for _, address := range []string{containerBridgeAddress, serviceAddressRange, containerBridgeAddressIPv6} {
				endpoints = append(endpoints, isolationEndpoint{address, "tcp", port})
			}
		}
	}

	gateway := newGatewayContainerEnvironment(m.config)
	image := m.config.ImageService
	for _, target := range []string{
		net.JoinHostPort(gateway.grpcHost, gateway.grpcPort),
		net.JoinHostPort(gateway.httpHost, gateway.httpPort),
		image.BuildRegistry,
		image.Runner.BaseImageRegistry,
		image.BuildApt.Mirror,
		image.BuildApt.Proxy,
	} {
		host, port := endpointHostPort(target)
		if host == "" {
			continue
		}
		addresses, err := net.LookupIP(host)
		if err != nil {
			log.Warn().Str("host", host).Err(err).Msg("unable to resolve a destination containers may need")
			continue
		}
		for _, ip := range addresses {
			endpoints = append(endpoints, isolationEndpoint{ip.String(), "tcp", port})
		}
	}
	return endpoints, nil
}

// endpointHostPort parses a host[:port][/path] or URL, defaulting to HTTPS.
func endpointHostPort(target string) (string, int) {
	if target == "" {
		return "", 0
	}
	if !strings.Contains(target, "://") {
		target = "//" + target
	}
	u, err := url.Parse(target)
	if err != nil || u.Hostname() == "" {
		return "", 0
	}
	if port, err := strconv.Atoi(u.Port()); err == nil {
		return u.Hostname(), port
	}
	if u.Scheme == "http" {
		return u.Hostname(), 80
	}
	return u.Hostname(), 443
}

// buildNetwork gives an image build a namespace behind the container bridge,
// so buildah's own fetches, such as FROM and ADD URLs, are isolated like its
// RUN steps.
func (m *ContainerNetworkManager) buildNetwork() (string, func(), error) {
	slot, err := m.createNetworkSlot()
	if err != nil {
		return "", nil, fmt.Errorf("create build network: %w", err)
	}
	return slot.netnsPath, func() {
		if err := m.releaseUnusedNetworkSlot(slot); err != nil {
			log.Warn().Err(err).Str("network_slot", slot.id).Msg("failed to release build network")
		}
	}, nil
}
