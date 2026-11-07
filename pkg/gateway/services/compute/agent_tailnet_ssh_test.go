package compute

import (
	"context"
	"testing"
	"time"

	model "github.com/beam-cloud/beta9/pkg/compute"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/stretchr/testify/require"
)

func TestAgentTailnetSSHOwnershipBoundary(t *testing.T) {
	tests := []struct {
		name   string
		change func(*Service, *model.PoolState, *model.AgentTokenState)
		allow  bool
		key    string
	}{
		{name: "managed serverless", allow: true, key: "serverless-agent-key"},
		{name: "API-managed serverless", change: func(_ *Service, pool *model.PoolState, _ *model.AgentTokenState) {
			pool.ManagementSource = types.WorkerPoolManagementSourceAPI
		}, allow: true, key: "serverless-agent-key"},
		{name: "private", change: func(_ *Service, _ *model.PoolState, agent *model.AgentTokenState) {
			agent.ManagedPoolInstanceID = ""
			agent.Mode = string(types.PoolModePrivate)
		}},
		{name: "provider", change: func(_ *Service, _ *model.PoolState, agent *model.AgentTokenState) {
			agent.ManagedPoolInstanceID = ""
			agent.Mode = string(types.PoolModeProvider)
		}},
		{name: "external without managed identity", change: func(_ *Service, _ *model.PoolState, agent *model.AgentTokenState) {
			agent.ManagedPoolInstanceID = ""
		}},
		{name: "stale managed identity", change: func(_ *Service, _ *model.PoolState, agent *model.AgentTokenState) {
			agent.ManagedPoolInstanceID = "old-instance"
		}},
		{name: "wrong workspace", change: func(_ *Service, pool *model.PoolState, _ *model.AgentTokenState) {
			pool.WorkspaceID = "tenant-workspace"
		}},
		{name: "wrong pool", change: func(_ *Service, pool *model.PoolState, _ *model.AgentTokenState) {
			pool.Name = "another-pool"
		}},
		{name: "untrusted pool source", change: func(_ *Service, pool *model.PoolState, _ *model.AgentTokenState) {
			pool.ManagementSource = ""
		}},
		{name: "config selector-bound", change: func(s *Service, pool *model.PoolState, _ *model.AgentTokenState) {
			config := s.appConfig.Worker.Pools[pool.Name]
			config.RequiresPoolSelector = true
			s.appConfig.Worker.Pools[pool.Name] = config
		}},
		{name: "API selector-bound", change: func(_ *Service, pool *model.PoolState, _ *model.AgentTokenState) {
			pool.ManagementSource = types.WorkerPoolManagementSourceAPI
			pool.WorkerConfig.RequiresPoolSelector = true
		}},
		{name: "non-serverless definition", change: func(s *Service, pool *model.PoolState, _ *model.AgentTokenState) {
			config := s.appConfig.Worker.Pools[pool.Name]
			config.Mode = types.PoolModeProvider
			s.appConfig.Worker.Pools[pool.Name] = config
		}},
		{name: "failover reservation", change: func(_ *Service, pool *model.PoolState, _ *model.AgentTokenState) {
			pool.CreatedByTokenID = types.FailoverOnDemandPoolCreator
		}},
		{name: "reserved nodes", change: func(_ *Service, pool *model.PoolState, _ *model.AgentTokenState) {
			pool.ReservedNodes = 1
		}},
		{name: "reservation inventory", change: func(_ *Service, pool *model.PoolState, _ *model.AgentTokenState) {
			pool.Reservations = []model.Reservation{{ID: "reserved-machine"}}
		}},
		{name: "BYOC", change: func(_ *Service, pool *model.PoolState, _ *model.AgentTokenState) {
			pool.BYOC = &model.BYOCProviderState{Provider: "aws"}
		}},
		{name: "inactive pool", change: func(_ *Service, pool *model.PoolState, _ *model.AgentTokenState) {
			pool.Status = "deleting"
		}},
		{name: "missing active definition", change: func(s *Service, _ *model.PoolState, _ *model.AgentTokenState) {
			s.appConfig.Worker.Pools = nil
		}},
		{name: "repository error", change: func(s *Service, _ *model.PoolState, _ *model.AgentTokenState) {
			s.managedPoolRepo.(*fakeManagedPoolRepo).getErr = context.DeadlineExceeded
		}},
		{name: "feature disabled", change: func(s *Service, _ *model.PoolState, _ *model.AgentTokenState) {
			s.appConfig.Tailscale.AgentSSH.Enabled = false
		}, key: "serverless-agent-key"},
		{name: "no serverless credential preserves transport", change: func(s *Service, _ *model.PoolState, _ *model.AgentTokenState) {
			s.appConfig.Tailscale.ServerlessAgentAuthKey = ""
		}},
		{name: "shared credential cannot enable SSH", change: func(s *Service, _ *model.PoolState, _ *model.AgentTokenState) {
			s.appConfig.Tailscale.ServerlessAgentAuthKey = "existing-agent-key"
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := types.WorkerPoolConfig{Mode: types.PoolModeExternal}
			pool := newManagedPoolState("admin-workspace", "serverless", types.WorkerPoolManagementSourceConfig, "admin", config, time.Now())
			agent := &model.AgentTokenState{
				TokenHash: hashComputeToken("agent-token"), WorkspaceID: pool.WorkspaceID,
				PoolName: pool.Name, MachineID: "machine", Mode: string(types.PoolModeExternal),
				ManagedPoolInstanceID: pool.ManagedInstanceID,
			}
			service := managedPoolTestService(types.AppConfig{
				Worker:    types.WorkerConfig{Pools: map[string]types.WorkerPoolConfig{pool.Name: config}},
				Tailscale: types.TailscaleConfig{Enabled: true, AuthKey: "gateway-key", AgentAuthKey: "existing-agent-key", ServerlessAgentAuthKey: "serverless-agent-key", AgentSSH: types.TailscaleAgentSSHConfig{Enabled: true}},
			}, &fakeComputeRepo{
				pools: map[string][]*model.PoolState{pool.WorkspaceID: {pool}},
				machines: map[string][]*model.AgentTokenState{
					fakeComputeKey(agent.WorkspaceID, agent.PoolName): {agent},
				},
			})
			if tt.change != nil {
				tt.change(service, pool, agent)
			}
			if pool.ManagementSource == types.WorkerPoolManagementSourceAPI {
				service.appConfig.Worker.Pools = nil
			}
			wantKey := tt.key
			if wantKey == "" {
				wantKey = "existing-agent-key"
			}
			key, ssh := service.agentTransportIdentity(context.Background(), agent)
			require.Equal(t, wantKey, key)
			require.Equal(t, tt.allow, ssh)
			response, err := service.RequestAgentTransportCredential(context.Background(), &pb.RequestAgentTransportCredentialRequest{AgentToken: "agent-token", Transport: types.BackendRouteTransportTSNet})
			require.NoError(t, err)
			require.Equal(t, tt.allow, response.TailnetSsh)
			if response.Ok {
				require.Equal(t, wantKey, response.AuthKey)
				require.Equal(t, "beam-agent-machine", response.Hostname)
				require.False(t, response.Ephemeral, "existing agents must retain their persisted node")
			}
		})
	}
}
