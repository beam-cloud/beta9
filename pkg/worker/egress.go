package worker

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/beam-cloud/beta9/pkg/worker/egress"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/opencontainers/runtime-spec/specs-go"
)

func (m *ContainerNetworkManager) registerEgress(id string, request *types.ContainerRequest) error {
	info, err := m.getContainerNetworkInfo(id)
	if err != nil {
		return err
	}
	return m.egress.Register(id, []string{info.ContainerIp, info.ContainerIpv6}, egress.Policy{BindingsOnly: request.BlockNetwork && len(request.AllowList) == 0, Open: request.NetworkPolicy() == types.ContainerNetworkPolicyOpen, AllowList: request.EgressAllowList(), Bindings: request.SecretBindings})
}

func (m *ContainerNetworkManager) refreshSecretBindings(ctx context.Context, id string, bindings []types.SecretBinding) ([]types.SecretBinding, error) {
	instance, ok := m.containerInstances.Get(id)
	if !ok || instance.Request == nil {
		return nil, fmt.Errorf("container is unavailable")
	}
	request := instance.Request
	names := make([]string, 0, len(bindings))
	for _, binding := range bindings {
		names = append(names, binding.SecretName)
	}
	response, err := m.workerRepoClient.GetContainerRuntimeCredentials(ctx, &pb.GetContainerRuntimeCredentialsRequest{WorkspaceId: request.WorkspaceId, StubId: request.StubId, ContainerId: id, BindingNames: names})
	if err != nil {
		return nil, err
	}
	if !response.Ok {
		return nil, fmt.Errorf("secret binding refresh denied")
	}
	latest := types.SecretBindingsFromProto(response.SecretBindings)
	for i := range latest {
		for _, binding := range bindings {
			if latest[i].SecretName == binding.SecretName {
				latest[i].EnvName = binding.EnvName
			}
		}
	}
	return latest, nil
}

type egressRule struct {
	table, chain string
	args         []string
}

func egressRules(ip, comment string, config types.EgressConfig, restricted bool, resolvers []string) []egressRule {
	var rules []egressRule
	for _, route := range []struct{ port, listener int }{{443, config.TLSPort}, {80, config.HTTPPort}, {0, config.OtherPort}} {
		args := []string{"-i", containerBridgeLinkName, "-s", ip, "-p", "tcp"}
		if route.port != 0 {
			args = append(args, "--dport", strconv.Itoa(route.port))
		}
		args = append(args, "-j", "REDIRECT", "--to-ports", strconv.Itoa(route.listener))
		rules = append(rules, egressRule{"nat", "PREROUTING", args})
	}
	if restricted {
		for _, chain := range []string{"FORWARD", "INPUT"} {
			rules = append(rules, egressRule{"filter", chain, []string{"-i", containerBridgeLinkName, "-s", ip, "-m", "conntrack", "--ctdir", "ORIGINAL", "-j", "DROP"}})
			for _, resolver := range resolvers {
				if strings.Contains(ip, ":") != strings.Contains(resolver, ":") {
					continue
				}
				rules = append(rules, egressRule{"filter", chain, []string{"-i", containerBridgeLinkName, "-s", ip, "-p", "udp", "--dport", "53", "-d", resolver, "-j", "ACCEPT"}})
			}
		}
		for _, port := range []int{config.TLSPort, config.HTTPPort, config.OtherPort} {
			rules = append(rules, egressRule{"filter", "INPUT", []string{"-i", containerBridgeLinkName, "-s", ip, "-p", "tcp", "--dport", strconv.Itoa(port), "-j", "ACCEPT"}})
		}
	}
	for i := range rules {
		rules[i].args = append(rules[i].args, "-m", "comment", "--comment", comment)
	}
	return rules
}

func (m *ContainerNetworkManager) setupEgressRules(id string, request *types.ContainerRequest) error {
	info, err := m.getContainerNetworkInfo(id)
	if err != nil {
		return err
	}
	data, err := os.ReadFile(containerResolvConfSource(m.config.Worker.UseHostResolvConf, hostResolvConfPath))
	if err != nil {
		return err
	}
	var resolvers []string
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) >= 2 && fields[0] == "nameserver" {
			resolvers = append(resolvers, fields[1])
		}
	}
	m.iptablesMu.Lock()
	defer m.iptablesMu.Unlock()
	for _, family := range []struct {
		ip   string
		ipv6 bool
	}{{info.ContainerIp, false}, {info.ContainerIpv6, true}} {
		ipt := m.ipt
		if family.ipv6 {
			ipt = m.ipt6
		}
		if ipt == nil || family.ip == "" {
			continue
		}
		rules := egressRules(family.ip, info.Comment, m.config.Worker.Egress, request.NetworkRestricted(), resolvers)
		for _, rule := range rules {
			if rule.table == "nat" {
				err = ipt.AppendUnique(rule.table, rule.chain, rule.args...)
			} else {
				err = ipt.InsertUnique(rule.table, rule.chain, 1, rule.args...)
			}
			if err != nil {
				return fmt.Errorf("install egress rule: %w", err)
			}
		}
	}
	return nil
}

func (s *Worker) attachEgressTrust(request *types.ContainerRequest, spec *specs.Spec) error {
	if s.egress == nil {
		if len(request.SecretBindings) > 0 {
			return fmt.Errorf("brokered secrets require the worker egress firewall")
		}
		return nil
	}
	if !request.Stub.Type.IsSandbox() {
		return nil
	}
	path, err := s.egress.TrustBundle(request.ContainerId, filepath.Join(baseConfigPath, request.ContainerId))
	if err != nil {
		return err
	}
	spec.Mounts = slices.DeleteFunc(spec.Mounts, func(m specs.Mount) bool { return m.Destination == egress.TrustBundlePath })
	spec.Mounts = append(spec.Mounts, specs.Mount{Type: "none", Source: path, Destination: egress.TrustBundlePath, Options: []string{"ro", "rbind", "rprivate", "nosuid", "noexec", "nodev"}})
	if len(request.SecretBindings) > 0 {
		spec.Process.Env = upsertEnvVars(spec.Process.Env, egress.TrustEnvironment())
	}
	return nil
}

func (m *ContainerNetworkManager) networkUpdateGuard(info *containerNetworkInfo, install bool) error {
	m.iptablesMu.Lock()
	defer m.iptablesMu.Unlock()
	for _, family := range []struct {
		ip   string
		ipv6 bool
	}{{info.ContainerIp, false}, {info.ContainerIpv6, true}} {
		ipt := m.ipt
		if family.ipv6 {
			ipt = m.ipt6
		}
		if ipt == nil || family.ip == "" {
			continue
		}
		for _, chain := range []string{"INPUT", "FORWARD"} {
			rule := []string{"-s", family.ip, "-j", "DROP", "-m", "comment", "--comment", info.Comment + "-beam-egress-update"}
			var err error
			if install {
				err = ipt.InsertUnique("filter", chain, 1, rule...)
			} else {
				err = ipt.DeleteIfExists("filter", chain, rule...)
			}
			if err != nil {
				return err
			}
		}
	}
	return nil
}
