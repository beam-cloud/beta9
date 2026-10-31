package worker

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/coreos/go-iptables/iptables"
	"net"
	"os"
	"strconv"
	"strings"
)

const isolationChainPrefix = "B9-I-"
const isolationAllowChain = "B9-LOCAL"

func isolationRules(ipv6 bool, denied, resolvers []string, proxyPorts []int) [][]string {
	var rules [][]string
	if ipv6 {
		for _, kind := range []string{"133", "135", "136"} {
			rules = append(rules, []string{"-p", "ipv6-icmp", "--icmpv6-type", kind, "-j", "RETURN"})
		}
	}
	// Only the configured resolver may receive private DNS traffic.
	for _, ip := range resolvers {
		if strings.Contains(ip, ":") == ipv6 {
			for _, protocol := range []string{"udp", "tcp"} {
				rules = append(rules, []string{"-d", ip, "-p", protocol, "--dport", "53", "-j", "RETURN"})
			}
		}
	}
	addresses := []string{containerBridgeAddress, serviceAddressRange}
	if ipv6 {
		addresses = []string{containerBridgeAddressIPv6}
	}
	for _, address := range addresses {
		for _, port := range proxyPorts {
			if port > 0 {
				rules = append(rules, []string{"-d", address, "-p", "tcp", "--dport", strconv.Itoa(port), "-j", "RETURN"})
			}
		}
	}
	for _, cidr := range append(common.RestrictedEgressNetworks(), denied...) {
		ip, _, err := net.ParseCIDR(cidr)
		if err != nil {
			continue
		}
		if (ip.To4() == nil) == ipv6 {
			rules = append(rules, []string{"-d", cidr, "-j", "DROP"})
		}
	}
	return rules
}

func (m *ContainerNetworkManager) ensureNetworkIsolation() error {
	for _, cidr := range m.config.Worker.Egress.DeniedCIDRs {
		if _, _, err := net.ParseCIDR(cidr); err != nil {
			return fmt.Errorf("invalid restricted network: %w", err)
		}
	}
	var resolvers []string
	data, err := os.ReadFile(containerResolvConfSource(m.config.Worker.UseHostResolvConf, hostResolvConfPath))
	if err != nil {
		return err
	}
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) > 1 && fields[0] == "nameserver" {
			resolvers = append(resolvers, fields[1])
		}
	}
	ports := []int{m.config.Worker.Egress.HTTPPort, m.config.Worker.Egress.TLSPort, m.config.Worker.Egress.OtherPort, m.config.Abstractions.Pod.TCP.ExternalPort, m.config.GatewayService.HTTP.ExternalPort}
	policy, _ := json.Marshal(m.config.Worker.Egress.DeniedCIDRs)
	digest := sha256.Sum256(policy)
	chainName := isolationChainPrefix + hex.EncodeToString(digest[:6])
	for _, family := range []struct {
		table *iptables.IPTables
		ipv6  bool
	}{{m.ipt, false}, {m.ipt6, true}} {
		if family.table == nil {
			continue
		}
		if err := family.table.NewChain("filter", chainName); err != nil {
			exists, lookupErr := family.table.ChainExists("filter", chainName)
			if lookupErr != nil || !exists {
				return err
			}
		}
		if err := family.table.NewChain("filter", isolationAllowChain); err != nil {
			exists, e := family.table.ChainExists("filter", isolationAllowChain)
			if e != nil || !exists {
				return err
			}
		}
		// Listeners are shared by workers on this host. Add each worker's exact
		// resolver/proxy endpoints before the common destination restrictions.
		for _, rule := range isolationRules(family.ipv6, nil, resolvers, ports) {
			if rule[len(rule)-1] != "RETURN" {
				continue
			}
			local := append([]string(nil), rule...)
			local[len(local)-1] = "ACCEPT"
			if err := family.table.AppendUnique("filter", isolationAllowChain, local...); err != nil {
				return err
			}
		}
		if err := family.table.InsertUnique("filter", chainName, 1, "-j", isolationAllowChain); err != nil {
			return err
		}
		for _, endpoint := range []struct {
			host string
			port int
		}{{m.config.GatewayService.GRPC.ExternalHost, m.config.GatewayService.GRPC.ExternalPort}, {m.config.GatewayService.HTTP.ExternalHost, m.config.GatewayService.HTTP.ExternalPort}} {
			if endpoint.host == "" || endpoint.port <= 0 {
				continue
			}
			addresses, err := net.LookupIP(endpoint.host)
			if err != nil {
				return fmt.Errorf("resolve gateway endpoint: %w", err)
			}
			for _, ip := range addresses {
				if (ip.To4() == nil) == family.ipv6 {
					if err := family.table.AppendUnique("filter", isolationAllowChain, "-d", ip.String(), "-p", "tcp", "--dport", strconv.Itoa(endpoint.port), "-j", "ACCEPT"); err != nil {
						return err
					}
				}
			}
		}
		for _, rule := range isolationRules(family.ipv6, m.config.Worker.Egress.DeniedCIDRs, nil, nil) {
			if err := family.table.AppendUnique("filter", chainName, rule...); err != nil {
				return err
			}
		}
		for _, bridge := range []string{containerBridgeLinkName, "cni-podman0"} {
			for _, chain := range []string{"FORWARD", "INPUT"} {
				if err := family.table.InsertUnique("filter", chain, 1, "-i", bridge, "-j", chainName); err != nil {
					return err
				}
			}
		}
		// CNI's firewall plugin consults this chain before adding container accepts.
		if err := family.table.NewChain("filter", "CNI-ADMIN"); err != nil {
			exists, e := family.table.ChainExists("filter", "CNI-ADMIN")
			if e != nil || !exists {
				return err
			}
		}
		for _, bridge := range []string{containerBridgeLinkName, "cni-podman0"} {
			if err := family.table.InsertUnique("filter", "CNI-ADMIN", 1, "-i", bridge, "-j", chainName); err != nil {
				return err
			}
		}
		for _, hook := range []string{"INPUT", "FORWARD", "CNI-ADMIN"} {
			old, err := family.table.List("filter", hook)
			if err != nil {
				return err
			}
			for _, line := range old {
				fields := strings.Fields(line)
				if len(fields) == 6 && fields[0] == "-A" && fields[2] == "-i" && (fields[3] == containerBridgeLinkName || fields[3] == "cni-podman0") && fields[4] == "-j" && strings.HasPrefix(fields[5], isolationChainPrefix) && fields[5] != chainName {
					if err := family.table.Delete("filter", hook, fields[2:]...); err != nil {
						return err
					}
				}
			}
		}
	}
	for _, path := range []string{"/proc/sys/net/bridge/bridge-nf-call-iptables", "/proc/sys/net/bridge/bridge-nf-call-ip6tables"} {
		if err := os.WriteFile(path, []byte("1\n"), 0644); err != nil {
			return fmt.Errorf("enable bridge filtering: %w", err)
		}
	}
	return nil
}
