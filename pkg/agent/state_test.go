package agent

import (
	"testing"

	"github.com/beam-cloud/beta9/pkg/types"
)

func TestRuntimeStateRoundTrip(t *testing.T) {
	t.Setenv(types.AgentStateDirEnv, t.TempDir())

	res := &joinResponse{
		Ok:          true,
		WorkspaceID: "workspace-1",
		PoolName:    "private-dev",
		MachineID:   "machine-1",
		AgentToken:  "agent-token",
		Bootstrap: bootstrapConfig{
			GatewayHTTPURL:  "https://gateway.beam.cloud",
			GatewayGRPCHost: "gateway.beam.cloud",
			GatewayGRPCPort: 443,
			GatewayGRPCTLS:  true,
			Transport:       "tsnet_restricted",
			Executor:        types.DefaultAgentWorkerContainerMode,
			Fallback:        types.PrivatePoolFallbackInternal,
		},
	}

	if err := saveRuntimeState("https://gateway.beam.cloud/", res); err != nil {
		t.Fatal(err)
	}

	loaded, err := loadRuntimeState("https://gateway.beam.cloud")
	if err != nil {
		t.Fatal(err)
	}
	if loaded == nil {
		t.Fatal("expected saved state")
	}
	if loaded.AgentToken != res.AgentToken || loaded.PoolName != res.PoolName || loaded.Bootstrap.Transport != res.Bootstrap.Transport {
		t.Fatalf("unexpected state: %#v", loaded)
	}
}
