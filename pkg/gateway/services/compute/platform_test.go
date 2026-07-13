package compute

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/auth"
	model "github.com/beam-cloud/beta9/pkg/compute"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
)

type fakePlatformBackendRepo struct {
	repository.BackendRepository
	workspace *types.Workspace
}

type fakePlatformWorkerPoolRepo struct {
	repository.WorkerPoolRepository
	deleted []string
}

func (r *fakePlatformWorkerPoolRepo) DeleteWorkerPoolState(_ context.Context, poolName string) error {
	r.deleted = append(r.deleted, poolName)
	return nil
}

func (r *fakePlatformBackendRepo) GetAdminWorkspace(context.Context) (*types.Workspace, error) {
	return r.workspace, nil
}

func platformOperatorAuth() *auth.AuthInfo {
	return &auth.AuthInfo{
		Workspace: &types.Workspace{ExternalId: "platform-workspace"},
		Token: &types.Token{
			ExternalId: "operator-token",
			TokenType:  types.TokenTypeClusterAdmin,
			Active:     true,
		},
	}
}

func platformTestService(config types.AppConfig, repo *fakeComputeRepo) *Service {
	return &Service{
		appConfig:   config,
		backendRepo: &fakePlatformBackendRepo{workspace: &types.Workspace{ExternalId: "platform-workspace"}},
		computeRepo: repo,
		workerRepo:  &fakeWorkerRepo{},
	}
}

func TestPlatformPoolServiceRejectsForgedClientCapability(t *testing.T) {
	service := &Service{}
	nonOperator := &auth.AuthInfo{
		Workspace: &types.Workspace{ExternalId: "workspace-1"},
		Token:     &types.Token{ExternalId: "user-token", TokenType: types.TokenTypeWorkspacePrimary},
	}

	if _, err := service.ListPlatformPools(context.Background(), nonOperator); !errors.Is(err, model.ErrPlatformPermissionDenied) {
		t.Fatalf("ListPlatformPools() error = %v, want permission denied", err)
	}
	if _, err := service.CreatePlatformPool(context.Background(), nonOperator, "forged", types.WorkerPoolConfig{}); !errors.Is(err, model.ErrPlatformPermissionDenied) {
		t.Fatalf("CreatePlatformPool() error = %v, want permission denied", err)
	}
	if _, err := service.UpdatePlatformPool(context.Background(), nonOperator, "forged", types.WorkerPoolConfig{}); !errors.Is(err, model.ErrPlatformPermissionDenied) {
		t.Fatalf("UpdatePlatformPool() error = %v, want permission denied", err)
	}
	if err := service.DeletePlatformPool(context.Background(), nonOperator, "forged"); !errors.Is(err, model.ErrPlatformPermissionDenied) {
		t.Fatalf("DeletePlatformPool() error = %v, want permission denied", err)
	}
	if _, _, err := service.CreatePlatformMachine(context.Background(), nonOperator, "forged"); !errors.Is(err, model.ErrPlatformPermissionDenied) {
		t.Fatalf("CreatePlatformMachine() error = %v, want permission denied", err)
	}
	if _, _, err := service.ListPlatformMachines(context.Background(), nonOperator, "forged"); !errors.Is(err, model.ErrPlatformPermissionDenied) {
		t.Fatalf("ListPlatformMachines() error = %v, want permission denied", err)
	}
	if _, err := service.DeletePlatformMachine(context.Background(), nonOperator, "forged", "machine-forged"); !errors.Is(err, model.ErrPlatformPermissionDenied) {
		t.Fatalf("DeletePlatformMachine() error = %v, want permission denied", err)
	}
}

func TestPlatformPoolLifecyclePreservesWorkerConfiguration(t *testing.T) {
	repo := &fakeComputeRepo{}
	service := platformTestService(types.AppConfig{}, repo)
	enabled := true
	config := types.WorkerPoolConfig{
		Mode:                      types.PoolModeExternal,
		GPUType:                   "H100",
		ContainerRuntime:          types.ContainerRuntimeRunc.String(),
		ContainerStartConcurrency: 7,
		NetworkSlotPoolSize:       48,
		Priority:                  120,
		Preemptable:               true,
		ImagesPath:                "/mnt/images",
		StoragePath:               "/mnt/storage",
		DurableDisksPath:          "/mnt/disks",
		Cache: types.WorkerPoolCacheConfig{
			Enabled: &enabled,
			Disk: types.WorkerPoolCacheDiskConfig{
				Enabled:      &enabled,
				HostPath:     "/mnt/cache",
				MountPath:    "/cache",
				MaxUsagePct:  80,
				MinFreeBytes: 1024,
			},
		},
	}

	created, err := service.CreatePlatformPool(context.Background(), platformOperatorAuth(), "public-h100", config)
	if err != nil {
		t.Fatal(err)
	}
	if !created.Editable || created.Controller != model.PlatformPoolControllerAgent {
		t.Fatalf("created pool = %+v", created)
	}
	if created.Config.Cache.Disk.HostPath != config.Cache.Disk.HostPath || created.Config.ContainerStartConcurrency != 7 {
		t.Fatalf("worker config was not preserved: %+v", created.Config)
	}
	state, err := repo.GetPoolState(context.Background(), "platform-workspace", "public-h100")
	if err != nil || state == nil {
		t.Fatalf("saved state = %+v, err = %v", state, err)
	}
	if !state.PlatformManaged || state.PlatformSource != model.PlatformPoolSourceDashboard || state.Mode != string(types.PoolModeExternal) {
		t.Fatalf("saved platform identity = %+v", state)
	}
	if state.WorkerConfig == nil || state.WorkerConfig.DurableDisksPath != "/mnt/disks" {
		t.Fatalf("saved worker config = %+v", state.WorkerConfig)
	}

	config.Priority = 250
	updated, err := service.UpdatePlatformPool(context.Background(), platformOperatorAuth(), "public-h100", config)
	if err != nil {
		t.Fatal(err)
	}
	if updated.Config.Priority != 250 {
		t.Fatalf("priority = %d, want 250", updated.Config.Priority)
	}

	repo.machines = map[string][]*model.AgentTokenState{
		fakeComputeKey("platform-workspace", "public-h100"): {
			{WorkspaceID: "platform-workspace", PoolName: "public-h100", MachineID: "machine-1"},
		},
	}
	config.Priority = 300
	if _, err := service.UpdatePlatformPool(context.Background(), platformOperatorAuth(), "public-h100", config); !errors.Is(err, model.ErrPlatformPoolInUse) {
		t.Fatalf("update with inventory error = %v, want in-use", err)
	}
	if err := service.DeletePlatformPool(context.Background(), platformOperatorAuth(), "public-h100"); !errors.Is(err, model.ErrPlatformPoolInUse) {
		t.Fatalf("delete with inventory error = %v, want in-use", err)
	}

	repo.machines = nil
	workerPoolRepo := &fakePlatformWorkerPoolRepo{}
	service.workerPoolRepo = workerPoolRepo
	if err := service.DeletePlatformPool(context.Background(), platformOperatorAuth(), "public-h100"); err != nil {
		t.Fatal(err)
	}
	if len(workerPoolRepo.deleted) != 1 || workerPoolRepo.deleted[0] != "public-h100" {
		t.Fatalf("deleted worker pool states = %v", workerPoolRepo.deleted)
	}
	state, err = repo.GetPoolState(context.Background(), "platform-workspace", "public-h100")
	if err != nil || state != nil {
		t.Fatalf("state after delete = %+v, err = %v", state, err)
	}
}

func TestReconcilePlatformPoolsOnlyMaterializesProviderlessExternalPools(t *testing.T) {
	legacyProvider := types.ProviderEC2
	service := platformTestService(types.AppConfig{Worker: types.WorkerConfig{Pools: map[string]types.WorkerPoolConfig{
		"local":        {Mode: types.PoolModeLocal},
		"legacy":       {Mode: types.PoolModeExternal, Provider: &legacyProvider},
		"public-agent": {Mode: types.PoolModeExternal, Priority: 10},
	}}}, &fakeComputeRepo{})

	if err := service.ReconcilePlatformPools(context.Background()); err != nil {
		t.Fatal(err)
	}
	states, err := service.computeRepo.ListPoolStates(context.Background(), "platform-workspace", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(states) != 1 || states[0].Name != "public-agent" || states[0].PlatformSource != model.PlatformPoolSourceConfig {
		t.Fatalf("reconciled states = %+v", states)
	}
}

func TestReconcilePlatformPoolsBackfillsDashboardPoolInstanceIdentity(t *testing.T) {
	state := newPlatformPoolState("platform-workspace", "dashboard-pool", model.PlatformPoolSourceDashboard, "operator-token", types.WorkerPoolConfig{Mode: types.PoolModeExternal}, time.Now())
	state.PlatformInstanceID = ""
	repo := &fakeComputeRepo{pools: map[string][]*model.PoolState{"platform-workspace": {state}}}
	service := platformTestService(types.AppConfig{}, repo)

	if err := service.ReconcilePlatformPools(context.Background()); err != nil {
		t.Fatal(err)
	}
	updated, err := repo.GetPoolState(context.Background(), "platform-workspace", "dashboard-pool")
	if err != nil || updated == nil || updated.PlatformInstanceID == "" {
		t.Fatalf("reconciled dashboard state = %+v, err = %v", updated, err)
	}
}

func TestReconcilePlatformPoolsPrunesStaleConfigStateWithoutInventory(t *testing.T) {
	repo := &fakeComputeRepo{pools: map[string][]*model.PoolState{
		"platform-workspace": {
			newPlatformPoolState("platform-workspace", "removed-agent-pool", model.PlatformPoolSourceConfig, "platform-config", types.WorkerPoolConfig{Mode: types.PoolModeExternal}, time.Now()),
		},
	}}
	service := platformTestService(types.AppConfig{}, repo)

	if err := service.ReconcilePlatformPools(context.Background()); err != nil {
		t.Fatal(err)
	}
	state, err := repo.GetPoolState(context.Background(), "platform-workspace", "removed-agent-pool")
	if err != nil || state != nil {
		t.Fatalf("stale state after reconcile = %+v, err = %v", state, err)
	}
}

func TestReconcilePlatformPoolsDoesNotActivateStaleConfigStateWithInventory(t *testing.T) {
	stale := newPlatformPoolState("platform-workspace", "changed-to-local", model.PlatformPoolSourceConfig, "platform-config", types.WorkerPoolConfig{Mode: types.PoolModeExternal}, time.Now())
	repo := &fakeComputeRepo{
		pools: map[string][]*model.PoolState{"platform-workspace": {stale}},
		machines: map[string][]*model.AgentTokenState{
			fakeComputeKey("platform-workspace", "changed-to-local"): {
				{WorkspaceID: "platform-workspace", PoolName: "changed-to-local", MachineID: "machine-1"},
			},
		},
	}
	service := platformTestService(types.AppConfig{Worker: types.WorkerConfig{Pools: map[string]types.WorkerPoolConfig{
		"changed-to-local": {Mode: types.PoolModeLocal},
	}}}, repo)

	err := service.ReconcilePlatformPools(context.Background())
	if err == nil || !strings.Contains(err.Error(), "still has inventory") {
		t.Fatalf("ReconcilePlatformPools() error = %v, want stale inventory error", err)
	}
	state, getErr := repo.GetPoolState(context.Background(), "platform-workspace", "changed-to-local")
	if getErr != nil || state == nil {
		t.Fatalf("stale state should remain available for draining: state=%+v err=%v", state, getErr)
	}
	if _, handled, createErr := service.CreatePlatformMachine(context.Background(), platformOperatorAuth(), "changed-to-local"); createErr != nil || handled {
		t.Fatalf("CreatePlatformMachine() handled = %v, err = %v; stale pool must not accept inventory", handled, createErr)
	}
}

func TestCreatePlatformMachineUsesMachineBoundPlatformToken(t *testing.T) {
	repo := &fakeComputeRepo{}
	service := platformTestService(types.AppConfig{GatewayService: types.GatewayServiceConfig{
		HTTP: types.HTTPConfig{ExternalHost: "gateway.example.com", ExternalPort: 443, TLS: true},
	}}, repo)
	if _, err := service.CreatePlatformPool(context.Background(), platformOperatorAuth(), "public-cpu", types.WorkerPoolConfig{Mode: types.PoolModeExternal}); err != nil {
		t.Fatal(err)
	}

	bootstrap, handled, err := service.CreatePlatformMachine(context.Background(), platformOperatorAuth(), "public-cpu")
	if err != nil || !handled {
		t.Fatalf("CreatePlatformMachine() handled = %v, err = %v", handled, err)
	}
	if bootstrap.MachineID == "" || !strings.Contains(bootstrap.InstallCommand, "/install/agent") {
		t.Fatalf("bootstrap = %+v", bootstrap)
	}
	tokenState := repo.joinTokens[hashComputeToken(bootstrap.Token)]
	if tokenState == nil || !tokenState.PlatformManaged || tokenState.Mode != string(types.PoolModeExternal) || tokenState.MachineID != bootstrap.MachineID || tokenState.PlatformPoolInstanceID == "" {
		t.Fatalf("join token state = %+v", tokenState)
	}
	joinResponse, err := service.JoinAgent(context.Background(), &pb.JoinAgentRequest{
		JoinToken:          bootstrap.Token,
		MachineFingerprint: "fingerprint-public-cpu",
		Hostname:           "public-cpu-1",
		CpuCount:           8,
		MemoryMb:           16384,
		Schedulable:        true,
	})
	if err != nil || !joinResponse.Ok {
		t.Fatalf("JoinAgent() response = %+v, err = %v", joinResponse, err)
	}
	joined, err := repo.GetAgentMachineState(context.Background(), "platform-workspace", "public-cpu", bootstrap.MachineID)
	if err != nil || joined == nil || joined.Mode != string(types.PoolModeExternal) {
		t.Fatalf("joined platform machine = %+v, err = %v", joined, err)
	}
}

func TestDeletedPlatformPoolTokenCannotJoinRecreatedPool(t *testing.T) {
	repo := &fakeComputeRepo{}
	service := platformTestService(types.AppConfig{}, repo)
	if _, err := service.CreatePlatformPool(context.Background(), platformOperatorAuth(), "public-cpu", types.WorkerPoolConfig{Mode: types.PoolModeExternal}); err != nil {
		t.Fatal(err)
	}
	bootstrap, handled, err := service.CreatePlatformMachine(context.Background(), platformOperatorAuth(), "public-cpu")
	if err != nil || !handled {
		t.Fatalf("CreatePlatformMachine() handled = %v, err = %v", handled, err)
	}
	oldTokenState := repo.joinTokens[hashComputeToken(bootstrap.Token)]
	if oldTokenState == nil || oldTokenState.PlatformPoolInstanceID == "" {
		t.Fatalf("old join token state = %+v", oldTokenState)
	}

	if err := service.DeletePlatformPool(context.Background(), platformOperatorAuth(), "public-cpu"); err != nil {
		t.Fatal(err)
	}
	if _, err := service.CreatePlatformPool(context.Background(), platformOperatorAuth(), "public-cpu", types.WorkerPoolConfig{Mode: types.PoolModeExternal}); err != nil {
		t.Fatal(err)
	}
	newState, err := repo.GetPoolState(context.Background(), "platform-workspace", "public-cpu")
	if err != nil || newState == nil {
		t.Fatalf("recreated pool state = %+v, err = %v", newState, err)
	}
	if newState.PlatformInstanceID == oldTokenState.PlatformPoolInstanceID {
		t.Fatal("recreated pool reused its deleted instance identity")
	}

	response, err := service.JoinAgent(context.Background(), &pb.JoinAgentRequest{
		JoinToken:          bootstrap.Token,
		MachineFingerprint: "stale-installer",
		Hostname:           "stale-host",
		CpuCount:           4,
		MemoryMb:           8192,
		Schedulable:        true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if response.Ok || response.ErrMsg != "join token is invalid or expired" {
		t.Fatalf("JoinAgent() response = %+v", response)
	}
}

func TestPlatformJoinTokenCannotJoinNonPlatformPool(t *testing.T) {
	repo := &fakeComputeRepo{
		pools: map[string][]*model.PoolState{
			"platform-workspace": {
				{
					WorkspaceID:      "platform-workspace",
					Name:             "tenant-private",
					Mode:             string(types.PoolModePrivate),
					Config:           &pb.PoolConfig{Name: "tenant-private", Mode: string(types.PoolModePrivate)},
					CreatedByTokenID: "private-owner",
				},
			},
		},
		joinTokens: map[string]*model.JoinTokenState{},
	}
	service := platformTestService(types.AppConfig{}, repo)
	rawToken := "platform-join-token"
	repo.joinTokens[hashComputeToken(rawToken)] = &model.JoinTokenState{
		TokenHash:              hashComputeToken(rawToken),
		WorkspaceID:            "platform-workspace",
		PoolName:               "tenant-private",
		CreatedByTokenID:       "operator-token",
		PlatformManaged:        true,
		PlatformPoolInstanceID: "wrong-platform-pool-instance",
		Mode:                   string(types.PoolModeExternal),
	}

	response, err := service.JoinAgent(context.Background(), &pb.JoinAgentRequest{
		JoinToken:          rawToken,
		MachineFingerprint: "fingerprint-1",
		Hostname:           "host-1",
		CpuCount:           4,
		MemoryMb:           8192,
		Schedulable:        true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if response.Ok || response.ErrMsg != "join token is invalid or expired" {
		t.Fatalf("JoinAgent() response = %+v", response)
	}
}
