package gatewayservices

import (
	"context"
	"testing"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/repository"
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
	agentProvider := types.ProviderAgent
	externalProvider := types.ProviderEC2
	gateway := &GatewayService{appConfig: types.AppConfig{Worker: types.WorkerConfig{Pools: map[string]types.WorkerPoolConfig{
		"local":             {Mode: types.PoolModeLocal},
		"agent-external":    {Mode: types.PoolModeExternal},
		"agent-provider":    {Mode: types.PoolModeExternal, Provider: &agentProvider},
		"provider-external": {Mode: types.PoolModeExternal, Provider: &externalProvider},
		"private":           {Mode: types.PoolModePrivate},
		"marketplace":       {Mode: types.PoolModeMarketplace},
	}}}}

	tests := map[string]machinePoolBackend{
		"":                  machinePoolManagedAgent,
		"unknown":           machinePoolManagedAgent,
		"local":             machinePoolLocal,
		"agent-external":    machinePoolManagedAgent,
		"agent-provider":    machinePoolManagedAgent,
		"provider-external": machinePoolProvider,
		"private":           machinePoolManagedAgent,
		"marketplace":       machinePoolManagedAgent,
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

func TestDeleteMachineRoutesConfiguredExternalPoolToProviderRepository(t *testing.T) {
	rdb, err := repository.NewRedisClientForTest()
	if err != nil {
		t.Fatal(err)
	}
	repo := repository.NewProviderRedisRepositoryForTest(rdb)
	provider := types.ProviderEC2
	if err := repo.AddMachine(string(provider), "external", "machine-1", &types.ProviderMachineState{PoolName: "external"}); err != nil {
		t.Fatal(err)
	}
	gateway := &GatewayService{
		appConfig: types.AppConfig{Worker: types.WorkerConfig{Pools: map[string]types.WorkerPoolConfig{
			"external": {Mode: types.PoolModeExternal, Provider: &provider},
		}}},
		providerRepo: repo,
	}
	ctx := auth.ContextWithAuthInfo(context.Background(), &auth.AuthInfo{
		Workspace: &types.Workspace{ExternalId: "admin-workspace"},
		Token:     &types.Token{ExternalId: "admin-token", TokenType: types.TokenTypeClusterAdmin},
	})

	response, err := gateway.DeleteMachine(ctx, &pb.DeleteMachineRequest{PoolName: "external", MachineId: "machine-1"})
	if err != nil {
		t.Fatal(err)
	}
	if !response.Ok {
		t.Fatalf("DeleteMachine() response = %+v", response)
	}
	machines, err := repo.ListAllMachines(string(provider), "external", false)
	if err != nil {
		t.Fatal(err)
	}
	if len(machines) != 0 {
		t.Fatalf("provider machines = %+v", machines)
	}
}
