package gatewayservices

import (
	"context"
	"testing"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
)

func TestDeleteMachineWithoutAuthIsRejected(t *testing.T) {
	response, err := (&GatewayService{}).DeleteMachine(context.Background(), &pb.DeleteMachineRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if response.Ok || response.ErrMsg != "Unauthorized Access" {
		t.Fatalf("DeleteMachine() response = %+v", response)
	}
}

func TestDeleteMachineDoesNotFallBackWithoutComputeService(t *testing.T) {
	ctx := auth.ContextWithAuthInfo(context.Background(), &auth.AuthInfo{
		Workspace: &types.Workspace{ExternalId: "workspace-1"},
		Token:     &types.Token{ExternalId: "workspace-token", TokenType: types.TokenTypeWorkspacePrimary},
	})
	response, err := (&GatewayService{}).DeleteMachine(ctx, &pb.DeleteMachineRequest{
		PoolName:  "pool-1",
		MachineId: "machine-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if response.Ok || response.ErrMsg != "compute service is unavailable" {
		t.Fatalf("DeleteMachine() response = %+v", response)
	}
}

func TestClassifyMachinePool(t *testing.T) {
	gateway := &GatewayService{appConfig: types.AppConfig{Worker: types.WorkerConfig{Pools: map[string]types.WorkerPoolConfig{
		"local":          {Mode: types.PoolModeLocal},
		"agent-external": {Mode: types.PoolModeExternal},
		"private":        {Mode: types.PoolModePrivate},
		"provider-pool":  {Mode: types.PoolModeProvider},
	}}}}

	tests := map[string]machinePoolBackend{
		"":               machinePoolManagedAgent,
		"unknown":        machinePoolManagedAgent,
		"local":          machinePoolLocal,
		"agent-external": machinePoolManagedAgent,
		"private":        machinePoolManagedAgent,
		"provider-pool":  machinePoolManagedAgent,
	}
	for poolName, want := range tests {
		t.Run(poolName, func(t *testing.T) {
			got, _ := gateway.classifyMachinePool(poolName)
			if got != want {
				t.Fatalf("classifyMachinePool(%q) = %d, want %d", poolName, got, want)
			}
		})
	}
}

func TestMachineListResponseReportsConfiguredStubLimits(t *testing.T) {
	response := machineListResponse(nil, nil, nil, 0, types.StubLimits{Cpu: 96_000, Memory: 48 * 1024})

	if response.MaxCpuMillicores != 96_000 || response.MaxMemoryMb != 48*1024 {
		t.Fatalf("machineListResponse() limits = %d/%d, want %d/%d", response.MaxCpuMillicores, response.MaxMemoryMb, 96_000, 48*1024)
	}
}
