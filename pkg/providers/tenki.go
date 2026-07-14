package providers

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"text/template"
	"time"

	tenki "github.com/TenkiCloud/tenki-sdk-go/sandbox"
	"github.com/rs/zerolog/log"

	"github.com/beam-cloud/beta9/pkg/network"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
)

const (
	tenkiProvisionTimeout  = 3 * time.Minute
	defaultTenkiDiskSizeGB = 20

	// Metadata keys stamped on every Tenki sandbox we create so that
	// listMachines can rediscover the machines belonging to this
	// cluster/pool after a controller restart.
	tenkiMetaClusterName = "beta9-cluster-name"
	tenkiMetaPoolName    = "beta9-pool-name"
	tenkiMetaMachineID   = "beta9-machine-id"
)

type TenkiProvider struct {
	*ExternalProvider
	client         *tenki.Client
	providerConfig types.TenkiProviderConfig
}

func NewTenkiProvider(ctx context.Context, appConfig types.AppConfig, providerRepo repository.ProviderRepository, workerRepo repository.WorkerRepository, tailscale *network.Tailscale) (*TenkiProvider, error) {
	providerConfig := appConfig.Providers.Tenki

	clientOpts := []tenki.Option{tenki.WithAuthToken(providerConfig.AuthToken)}
	if providerConfig.BaseURL != "" {
		clientOpts = append(clientOpts, tenki.WithBaseURL(providerConfig.BaseURL))
	}

	client, err := tenki.New(clientOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create tenki client: %w", err)
	}

	tenkiProvider := &TenkiProvider{
		client:         client,
		providerConfig: providerConfig,
	}

	baseProvider := NewExternalProvider(ctx, &ExternalProviderConfig{
		Name:                 string(types.ProviderTenki),
		ClusterName:          appConfig.ClusterName,
		AppConfig:            appConfig,
		TailScale:            tailscale,
		ProviderRepo:         providerRepo,
		WorkerRepo:           workerRepo,
		ListMachinesFunc:     tenkiProvider.listMachines,
		TerminateMachineFunc: tenkiProvider.TerminateMachine,
	})
	tenkiProvider.ExternalProvider = baseProvider

	return tenkiProvider, nil
}

// ProvisionMachine boots a Tenki microVM sandbox sized to the compute request
// and starts the beta9 agent inside it. Tenki is a CPU-only sandbox platform,
// so GPU requests are rejected up front.
func (p *TenkiProvider) ProvisionMachine(ctx context.Context, poolName, token string, compute types.ProviderComputeRequest) (string, error) {
	if compute.Gpu != "" {
		return "", fmt.Errorf("tenki provider does not support GPU workloads (requested %q)", compute.Gpu)
	}

	// compute.Cpu is expressed in millicores; Tenki sizes sandboxes in whole
	// cores, so round up to the next core.
	cpuCores := compute.Cpu / 1000
	if compute.Cpu%1000 != 0 {
		cpuCores++
	}
	if cpuCores < 1 {
		cpuCores = 1
	}

	diskSizeGB := p.providerConfig.DiskSizeGB
	if diskSizeGB <= 0 {
		diskSizeGB = defaultTenkiDiskSizeGB
	}

	machineId := MachineId()

	bootstrap, err := renderTenkiBootstrap(userDataConfig{
		TailscaleAuth:     p.AppConfig.Tailscale.AuthKey,
		TailscaleUrl:      p.AppConfig.Tailscale.ControlURL,
		RegistrationToken: token,
		MachineId:         machineId,
		PoolName:          poolName,
		ProviderName:      p.Name,
	})
	if err != nil {
		return "", err
	}

	sandboxName := fmt.Sprintf("%s-%s-%s", p.ClusterName, poolName, machineId)

	session, err := p.client.CreateAndWait(ctx, tenkiProvisionTimeout,
		tenki.WithName(sandboxName),
		tenki.WithCPUCores(int32(cpuCores)),
		tenki.WithMemoryMB(int32(compute.Memory)),
		tenki.WithDiskSizeGB(diskSizeGB),
		tenki.WithSticky(),
		tenki.WithAllowInbound(true),
		tenki.WithAllowOutbound(true),
		tenki.WithMetadata(map[string]string{
			tenkiMetaClusterName: p.ClusterName,
			tenkiMetaPoolName:    poolName,
			tenkiMetaMachineID:   machineId,
		}),
		tenki.WithTags("beta9", p.ClusterName, poolName),
	)
	if err != nil {
		return "", fmt.Errorf("failed to create tenki sandbox: %w", err)
	}

	// The beta9 agent is long-lived, so the bootstrap script starts it detached
	// and Exec returns once the agent has been spawned. On any failure we close
	// the sandbox so a broken bootstrap does not leak a running machine.
	if _, err := session.Exec(ctx, "sudo", tenki.WithArgs("bash", "-c", bootstrap)); err != nil {
		_ = session.Close(ctx)
		return "", fmt.Errorf("failed to bootstrap beta9 agent on tenki sandbox %s: %w", session.ID, err)
	}

	log.Info().
		Str("provider", p.Name).
		Str("machine_id", machineId).
		Str("session_id", session.ID).
		Int64("cpu_cores", cpuCores).
		Int64("memory_mb", compute.Memory).
		Msg("provisioned tenki sandbox")

	err = p.ProviderRepo.AddMachine(string(types.ProviderTenki), poolName, machineId, &types.ProviderMachineState{
		Cpu:               cpuCores * 1000,
		Memory:            compute.Memory,
		RegistrationToken: token,
		AutoConsolidate:   true,
	})
	if err != nil {
		_ = session.Close(ctx)
		return "", err
	}

	return machineId, nil
}

// listMachines returns the beta9 machineId -> Tenki sessionId map for every
// sandbox this cluster/pool owns, discovered via the metadata stamped at
// create time.
func (p *TenkiProvider) listMachines(ctx context.Context, poolName string) (map[string]string, error) {
	sessions, err := p.client.List(ctx)
	if err != nil {
		return nil, err
	}

	machines := make(map[string]string)
	for _, session := range sessions {
		if session.Metadata[tenkiMetaClusterName] != p.ClusterName {
			continue
		}
		if session.Metadata[tenkiMetaPoolName] != poolName {
			continue
		}

		machineId := session.Metadata[tenkiMetaMachineID]
		if machineId == "" {
			continue
		}

		machines[machineId] = session.ID
	}

	return machines, nil
}

func (p *TenkiProvider) TerminateMachine(ctx context.Context, poolName, instanceId, machineId string) error {
	if instanceId == "" {
		return errors.New("invalid instance ID")
	}

	session, err := p.client.Get(ctx, instanceId)
	if err != nil {
		return fmt.Errorf("failed to resolve tenki sandbox %s: %w", instanceId, err)
	}

	if err := session.Close(ctx); err != nil {
		return fmt.Errorf("failed to terminate tenki sandbox %s: %w", instanceId, err)
	}

	if err := p.ProviderRepo.RemoveMachine(p.Name, poolName, machineId); err != nil {
		log.Error().Str("provider", p.Name).Str("machine_id", machineId).Err(err).Msg("unable to remove machine state")
		return err
	}

	log.Info().Str("provider", p.Name).Str("machine_id", machineId).Msg("terminated machine")
	return nil
}

func renderTenkiBootstrap(cfg userDataConfig) (string, error) {
	tmpl, err := template.New("tenki-bootstrap").Parse(tenkiBootstrapTemplate)
	if err != nil {
		return "", fmt.Errorf("error parsing tenki bootstrap template: %w", err)
	}

	var out bytes.Buffer
	if err := tmpl.Execute(&out, cfg); err != nil {
		return "", fmt.Errorf("error executing tenki bootstrap template: %w", err)
	}

	return out.String(), nil
}

// tenkiBootstrapTemplate downloads the beta9 agent and starts it detached. The
// Tenki guest is a full systemd Ubuntu VM (overlayfs, cgroup v2, user
// namespaces, /dev/net/tun all present), so the agent can install its own
// container runtime and join the Tailscale mesh exactly as it does on any
// other provider.
const tenkiBootstrapTemplate string = `set -euo pipefail
curl -L -o /usr/local/bin/beta9-agent https://release.beam.cloud/agent/agent
chmod +x /usr/local/bin/beta9-agent
setsid /usr/local/bin/beta9-agent \
  --token "{{.RegistrationToken}}" \
  --machine-id "{{.MachineId}}" \
  --tailscale-url "{{.TailscaleUrl}}" \
  --tailscale-auth "{{.TailscaleAuth}}" \
  --pool-name "{{.PoolName}}" \
  --provider-name "{{.ProviderName}}" </dev/null >/var/log/beta9-agent.log 2>&1 &
`
