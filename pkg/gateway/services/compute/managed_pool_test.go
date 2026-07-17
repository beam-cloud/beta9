package compute

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/auth"
	model "github.com/beam-cloud/beta9/pkg/compute"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/scheduler"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
)

type fakeManagedPoolBackendRepo struct {
	repository.BackendRepository
	workspace         *types.Workspace
	workspaceFailures int
	workspaceCalls    int
	started           chan struct{}
}

func (r *fakeManagedPoolBackendRepo) GetAdminWorkspace(ctx context.Context) (*types.Workspace, error) {
	r.workspaceCalls++
	if r.started != nil {
		select {
		case r.started <- struct{}{}:
		default:
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if r.workspaceFailures > 0 {
		r.workspaceFailures--
		return nil, errors.New("workspace temporarily unavailable")
	}
	if r.workspace != nil {
		return r.workspace, nil
	}
	return &types.Workspace{ExternalId: "admin-workspace"}, nil
}

type managedPoolWorkerTokenBackend struct {
	repository.BackendRepository
	workspaceID string
}

func (r *managedPoolWorkerTokenBackend) GetWorkspaceByExternalId(_ context.Context, workspaceID string) (types.Workspace, error) {
	r.workspaceID = workspaceID
	return types.Workspace{Id: 7, ExternalId: workspaceID}, nil
}

func (r *managedPoolWorkerTokenBackend) GetAdminWorkspace(context.Context) (*types.Workspace, error) {
	return nil, errors.New("unexpected admin workspace scan")
}

type fakeManagedPoolWorkerPoolRepo struct {
	repository.WorkerPoolRepository
	deleted []string
}

type fakeManagedPoolRepo struct {
	repo   *fakeComputeRepo
	saved  chan struct{}
	getErr error
}

func (*fakeManagedPoolRepo) WithManagedPoolStateLock(ctx context.Context, _, _ string, fn func(context.Context) error) error {
	return fn(ctx)
}
func (r *fakeManagedPoolRepo) SaveManagedPoolState(ctx context.Context, workspaceID string, state *model.PoolState) error {
	if err := r.repo.SavePoolState(ctx, workspaceID, state); err != nil {
		return err
	}
	if r.saved != nil {
		r.saved <- struct{}{}
	}
	return nil
}
func (r *fakeManagedPoolRepo) GetManagedPoolState(ctx context.Context, workspaceID, name string) (*model.PoolState, error) {
	if r.getErr != nil {
		return nil, r.getErr
	}
	return r.repo.GetPoolState(ctx, workspaceID, name)
}
func (r *fakeManagedPoolRepo) ListManagedPoolStates(ctx context.Context, workspaceID string, limit int) ([]*model.PoolState, error) {
	return r.repo.ListPoolStates(ctx, workspaceID, limit)
}
func (r *fakeManagedPoolRepo) DeleteManagedPoolState(ctx context.Context, workspaceID, name string) error {
	return r.repo.DeletePoolState(ctx, workspaceID, name)
}

func (r *fakeManagedPoolWorkerPoolRepo) DeleteWorkerPoolState(_ context.Context, poolName string) error {
	r.deleted = append(r.deleted, poolName)
	return nil
}

func clusterAdminAuth() *auth.AuthInfo {
	return &auth.AuthInfo{
		Workspace: &types.Workspace{ExternalId: "admin-workspace"},
		Token: &types.Token{
			ExternalId: "operator-token",
			TokenType:  types.TokenTypeClusterAdmin,
			Active:     true,
		},
	}
}

func managedPoolTestService(config types.AppConfig, repo *fakeComputeRepo) *Service {
	managedStates := &fakeComputeRepo{pools: repo.pools}
	repo.pools = nil
	workers := &fakeWorkerRepo{}
	return &Service{
		appConfig:            config,
		backendRepo:          &fakeManagedPoolBackendRepo{},
		scheduler:            scheduler.NewSchedulerForCapacityChecks(workers, repo, scheduler.NewWorkerPoolManager(false)),
		computeRepo:          repo,
		managedPoolRepo:      &fakeManagedPoolRepo{repo: managedStates},
		workerRepo:           workers,
		managedPoolInstances: map[string]string{},
	}
}

func TestServiceStartDoesNotWaitForManagedPoolReconciliation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	backend := &fakeManagedPoolBackendRepo{started: make(chan struct{}, 1)}
	service := &Service{
		appConfig: types.AppConfig{
			Worker: types.WorkerConfig{Pools: map[string]types.WorkerPoolConfig{
				"public-agent": {Mode: types.PoolModeExternal},
			}},
		},
		backendRepo:          backend,
		scheduler:            scheduler.NewSchedulerForCapacityChecks(&fakeWorkerRepo{}, &fakeComputeRepo{}, scheduler.NewWorkerPoolManager(false)),
		computeRepo:          &fakeComputeRepo{},
		managedPoolRepo:      &fakeManagedPoolRepo{repo: &fakeComputeRepo{}},
		managedPoolInstances: map[string]string{},
	}

	returned := make(chan struct{})
	go func() {
		service.Start(ctx)
		close(returned)
	}()

	select {
	case <-returned:
	case <-time.After(time.Second):
		cancel()
		t.Fatal("service startup blocked on managed pool reconciliation")
	}
	select {
	case <-backend.started:
	case <-time.After(time.Second):
		cancel()
		t.Fatal("managed pool reconciliation did not start")
	}
	cancel()
}

func TestManagedPoolServiceRejectsForgedClientCapability(t *testing.T) {
	service := managedPoolTestService(types.AppConfig{}, &fakeComputeRepo{})
	nonAdmin := &auth.AuthInfo{
		Workspace: &types.Workspace{ExternalId: "workspace-1"},
		Token:     &types.Token{ExternalId: "user-token", TokenType: types.TokenTypeWorkspacePrimary, Active: true},
	}

	ctx := context.Background()
	if _, err := service.ListManagedPools(ctx, nonAdmin); !errors.Is(err, model.ErrManagedPermissionDenied) {
		t.Fatal(err)
	}
	if _, err := service.CreateManagedPool(ctx, nonAdmin, "forged", types.WorkerPoolConfig{}); !errors.Is(err, model.ErrManagedPermissionDenied) {
		t.Fatal(err)
	}
	if _, err := service.UpdateManagedPool(ctx, nonAdmin, "forged", types.WorkerPoolConfig{}); !errors.Is(err, model.ErrManagedPermissionDenied) {
		t.Fatal(err)
	}
	if err := service.DeleteManagedPool(ctx, nonAdmin, "forged"); !errors.Is(err, model.ErrManagedPermissionDenied) {
		t.Fatal(err)
	}
	if _, _, err := service.CreateManagedMachine(ctx, nonAdmin, "forged"); !errors.Is(err, model.ErrManagedPermissionDenied) {
		t.Fatal(err)
	}
	if _, _, err := service.ListManagedMachines(ctx, nonAdmin, "forged"); !errors.Is(err, model.ErrManagedPermissionDenied) {
		t.Fatal(err)
	}
	if _, err := service.DeleteManagedMachine(ctx, nonAdmin, "forged", "machine-forged"); !errors.Is(err, model.ErrManagedPermissionDenied) {
		t.Fatal(err)
	}
}

func TestManagedPoolCPUAffinityDefaultsDisabled(t *testing.T) {
	config, err := normalizeManagedPoolConfig(types.WorkerPoolConfig{Mode: types.PoolModeExternal})
	if err != nil {
		t.Fatal(err)
	}
	if config.CPUAffinityEnforced {
		t.Fatal("managed pool CPU affinity must default to disabled")
	}
}

func TestManagedPoolLifecyclePreservesWorkerConfiguration(t *testing.T) {
	repo := &fakeComputeRepo{}
	service := managedPoolTestService(types.AppConfig{}, repo)
	enabled := true
	config := types.WorkerPoolConfig{
		Mode:                      types.PoolModeExternal,
		GPUType:                   "H100",
		RequiresPoolSelector:      true,
		ContainerRuntime:          types.ContainerRuntimeRunc.String(),
		CPUAffinityEnforced:       true,
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

	created, err := service.CreateManagedPool(context.Background(), clusterAdminAuth(), "public-h100", config)
	if err != nil {
		t.Fatal(err)
	}
	if created.Source != types.WorkerPoolManagementSourceAPI || created.Controller != types.WorkerPoolControllerAgent {
		t.Fatalf("created pool = %+v", created)
	}
	if created.Config.Cache.Disk.HostPath != config.Cache.Disk.HostPath || created.Config.ContainerStartConcurrency != 7 || !created.Config.CPUAffinityEnforced {
		t.Fatalf("worker config was not preserved: %+v", created.Config)
	}
	if created.Config.RequiresPoolSelector {
		t.Fatal("managed pool must be available to selector-less serverless requests")
	}
	state, err := service.managedPoolRepo.GetManagedPoolState(context.Background(), "admin-workspace", "public-h100")
	if err != nil || state == nil {
		t.Fatalf("saved state = %+v, err = %v", state, err)
	}
	if state.ManagementSource != types.WorkerPoolManagementSourceAPI || state.Mode != string(types.PoolModeExternal) {
		t.Fatalf("saved managed identity = %+v", state)
	}
	if state.WorkerConfig == nil || state.WorkerConfig.DurableDisksPath != "/mnt/disks" {
		t.Fatalf("saved worker config = %+v", state.WorkerConfig)
	}
	if !state.WorkerConfig.CPUAffinityEnforced {
		t.Fatal("persisted managed pool must preserve CPU affinity configuration")
	}
	if state.WorkerConfig.RequiresPoolSelector {
		t.Fatal("persisted managed pool must not require a pool selector")
	}
	if err := service.ReconcileManagedPools(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !service.scheduler.HasManagedPoolForGPU("H100", false) {
		t.Fatal("managed pool was not available to serverless scheduling")
	}
	createdInstanceID := state.ManagedInstanceID

	config.Priority = 250
	updated, err := service.UpdateManagedPool(context.Background(), clusterAdminAuth(), "public-h100", config)
	if err != nil {
		t.Fatal(err)
	}
	if updated.Config.Priority != 250 {
		t.Fatalf("priority = %d, want 250", updated.Config.Priority)
	}
	state, err = service.managedPoolRepo.GetManagedPoolState(context.Background(), "admin-workspace", "public-h100")
	if err != nil || state == nil || state.ManagedInstanceID != createdInstanceID {
		t.Fatalf("updated pool identity = %+v, err = %v", state, err)
	}

	repo.machines = map[string][]*model.AgentTokenState{
		fakeComputeKey("admin-workspace", "public-h100"): {
			{WorkspaceID: "admin-workspace", PoolName: "public-h100", MachineID: "machine-1"},
		},
	}
	config.Priority = 300
	updated, err = service.UpdateManagedPool(context.Background(), clusterAdminAuth(), "public-h100", config)
	if err != nil || updated.Config.Priority != 300 {
		t.Fatalf("live update = %+v, err = %v", updated, err)
	}
	if err := service.DeleteManagedPool(context.Background(), clusterAdminAuth(), "public-h100"); !errors.Is(err, model.ErrManagedPoolInUse) {
		t.Fatalf("delete with inventory error = %v, want in-use", err)
	}

	repo.machines = nil
	workerPoolRepo := &fakeManagedPoolWorkerPoolRepo{}
	service.workerPoolRepo = workerPoolRepo
	if err := service.DeleteManagedPool(context.Background(), clusterAdminAuth(), "public-h100"); err != nil {
		t.Fatal(err)
	}
	if len(workerPoolRepo.deleted) != 1 || workerPoolRepo.deleted[0] != "public-h100" {
		t.Fatalf("deleted worker pool states = %v", workerPoolRepo.deleted)
	}
	state, err = service.managedPoolRepo.GetManagedPoolState(context.Background(), "admin-workspace", "public-h100")
	if err != nil || state != nil {
		t.Fatalf("state after delete = %+v, err = %v", state, err)
	}
}

func TestManagedPoolsConvergeAcrossGatewayReplicas(t *testing.T) {
	rdb, err := repository.NewRedisClientForTest()
	if err != nil {
		t.Fatal(err)
	}
	computeRepo := repository.NewComputeRedisRepository(rdb)
	newReplica := func() (*Service, *scheduler.WorkerPoolManager) {
		manager := scheduler.NewWorkerPoolManager(false)
		workers := &fakeWorkerRepo{}
		return &Service{
			appConfig: types.AppConfig{
				Worker: types.WorkerConfig{Pools: map[string]types.WorkerPoolConfig{
					"config-pool": {Mode: types.PoolModeExternal, Priority: 5},
				}},
			},
			backendRepo: &fakeManagedPoolBackendRepo{},
			scheduler: scheduler.NewSchedulerForCapacityChecks(
				workers,
				computeRepo,
				manager,
			),
			computeRepo:          computeRepo,
			managedPoolRepo:      repository.NewManagedPoolRedisRepository(rdb),
			workerRepo:           workers,
			redisClient:          rdb,
			managedPoolInstances: map[string]string{},
		}, manager
	}

	replicaA, managerA := newReplica()
	replicaB, managerB := newReplica()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go replicaA.runManagedPoolReconciler(ctx)
	go replicaB.runManagedPoolReconciler(ctx)
	waitForManagedPoolReplicas(t, []*scheduler.WorkerPoolManager{managerA, managerB}, "config-pool", true, 5)
	stateA, err := replicaA.managedPoolRepo.GetManagedPoolState(context.Background(), "admin-workspace", "config-pool")
	if err != nil {
		t.Fatal(err)
	}
	stateB, err := replicaB.managedPoolRepo.GetManagedPoolState(context.Background(), "admin-workspace", "config-pool")
	if err != nil {
		t.Fatal(err)
	}
	if stateA == nil || stateB == nil || stateA.ManagedInstanceID == "" || stateA.ManagedInstanceID != stateB.ManagedInstanceID {
		t.Fatalf("config pool identity diverged: replica A=%+v replica B=%+v", stateA, stateB)
	}

	config := types.WorkerPoolConfig{Mode: types.PoolModeExternal, Priority: 10}
	if _, err := replicaA.CreateManagedPool(context.Background(), clusterAdminAuth(), "shared-pool", config); err != nil {
		t.Fatal(err)
	}
	waitForManagedPoolReplicas(t, []*scheduler.WorkerPoolManager{managerA, managerB}, "shared-pool", true, 10)

	config.Priority = 25
	if _, err := replicaA.UpdateManagedPool(context.Background(), clusterAdminAuth(), "shared-pool", config); err != nil {
		t.Fatal(err)
	}
	waitForManagedPoolReplicas(t, []*scheduler.WorkerPoolManager{managerA, managerB}, "shared-pool", true, 25)

	if err := replicaA.DeleteManagedPool(context.Background(), clusterAdminAuth(), "shared-pool"); err != nil {
		t.Fatal(err)
	}
	waitForManagedPoolReplicas(t, []*scheduler.WorkerPoolManager{managerA, managerB}, "shared-pool", false, 0)
}

func waitForManagedPoolReplicas(t *testing.T, managers []*scheduler.WorkerPoolManager, name string, present bool, priority int32) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		matched := true
		for _, manager := range managers {
			pool, ok := manager.GetPool(name)
			if ok != present || ok && pool.Config.Priority != priority {
				matched = false
				break
			}
		}
		if matched {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("pool %q did not converge across gateway replicas", name)
}

func TestManagedExternalWorkerUsesItsPersistedWorkspace(t *testing.T) {
	backend := &managedPoolWorkerTokenBackend{}
	service := &Service{backendRepo: backend}
	workspace, tokenType, err := service.workerTokenWorkspaceAndType(context.Background(), &model.AgentTokenState{
		WorkspaceID: "admin-workspace",
		Mode:        string(types.PoolModeExternal),
	})
	if err != nil {
		t.Fatal(err)
	}
	if workspace.ExternalId != "admin-workspace" || backend.workspaceID != "admin-workspace" || tokenType != types.TokenTypeWorker {
		t.Fatalf("workspace = %+v, lookup = %q, token type = %q", workspace, backend.workspaceID, tokenType)
	}
}

func TestReconcileManagedPoolsOnlyMaterializesAgentExternalPools(t *testing.T) {
	legacyProvider := types.ProviderEC2
	agentProvider := types.ProviderAgent
	service := managedPoolTestService(types.AppConfig{Worker: types.WorkerConfig{Pools: map[string]types.WorkerPoolConfig{
		"local":                {Mode: types.PoolModeLocal},
		"legacy":               {Mode: types.PoolModeExternal, Provider: &legacyProvider},
		"providerless-agent":   {Mode: types.PoolModeExternal, Priority: 10},
		"named-agent-provider": {Mode: types.PoolModeExternal, Provider: &agentProvider, Priority: 20},
	}}}, &fakeComputeRepo{})

	if err := service.ReconcileManagedPools(context.Background()); err != nil {
		t.Fatal(err)
	}
	states, err := service.managedPoolRepo.ListManagedPoolStates(context.Background(), "admin-workspace", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(states) != 2 {
		t.Fatalf("reconciled states = %+v", states)
	}
	names := map[string]struct{}{}
	for _, state := range states {
		if state.ManagementSource != types.WorkerPoolManagementSourceConfig || state.WorkerConfig == nil || state.WorkerConfig.Provider != nil {
			t.Fatalf("reconciled state = %+v", state)
		}
		names[state.Name] = struct{}{}
	}
	for _, name := range []string{"providerless-agent", "named-agent-provider"} {
		if _, ok := names[name]; !ok {
			t.Fatalf("missing reconciled pool %q in %+v", name, states)
		}
	}
}

func TestConfigManagedPoolsUseReplicaLocalConfigWithoutSharedRewrite(t *testing.T) {
	computeRepo := &fakeComputeRepo{}
	managedRepo := &fakeManagedPoolRepo{repo: &fakeComputeRepo{}}
	makeReplica := func(priority int32) (*Service, *scheduler.WorkerPoolManager) {
		manager := scheduler.NewWorkerPoolManager(false)
		return &Service{
			appConfig: types.AppConfig{
				Worker: types.WorkerConfig{Pools: map[string]types.WorkerPoolConfig{
					"config-pool": {Mode: types.PoolModeExternal, Priority: priority},
				}},
			},
			backendRepo: &fakeManagedPoolBackendRepo{},
			scheduler: scheduler.NewSchedulerForCapacityChecks(
				&fakeWorkerRepo{},
				computeRepo,
				manager,
			),
			computeRepo:          computeRepo,
			managedPoolRepo:      managedRepo,
			managedPoolInstances: map[string]string{},
		}, manager
	}

	oldReplica, oldManager := makeReplica(10)
	newReplica, newManager := makeReplica(20)
	for _, service := range []*Service{oldReplica, newReplica, oldReplica} {
		if err := service.ReconcileManagedPools(context.Background()); err != nil {
			t.Fatal(err)
		}
	}

	persisted, err := managedRepo.GetManagedPoolState(context.Background(), "admin-workspace", "config-pool")
	if err != nil {
		t.Fatal(err)
	}
	if persisted == nil || persisted.WorkerConfig == nil || persisted.WorkerConfig.Priority != 10 {
		t.Fatalf("shared identity was rewritten by mixed config: %+v", persisted)
	}
	if pool, ok := oldManager.GetPool("config-pool"); !ok || pool.Config.Priority != 10 {
		t.Fatalf("old replica controller = %+v, present = %v", pool, ok)
	}
	if pool, ok := newManager.GetPool("config-pool"); !ok || pool.Config.Priority != 20 {
		t.Fatalf("new replica controller = %+v, present = %v", pool, ok)
	}
}

func TestManagedPoolReconciliationRetriesStartupFailure(t *testing.T) {
	repo := &fakeComputeRepo{}
	service := managedPoolTestService(types.AppConfig{Worker: types.WorkerConfig{Pools: map[string]types.WorkerPoolConfig{
		"public-agent": {Mode: types.PoolModeExternal},
	}}}, repo)
	backend := &fakeManagedPoolBackendRepo{
		workspace:         &types.Workspace{ExternalId: "admin-workspace"},
		workspaceFailures: 1,
	}
	service.backendRepo = backend

	if err := service.ReconcileManagedPools(context.Background()); err == nil {
		t.Fatal("initial reconciliation unexpectedly succeeded")
	}
	ctx, cancel := context.WithCancel(context.Background())
	saved := make(chan struct{}, 1)
	service.managedPoolRepo.(*fakeManagedPoolRepo).saved = saved
	go service.runManagedPoolReconciler(ctx)
	select {
	case <-saved:
	case <-time.After(time.Second):
		cancel()
		t.Fatal("managed pool reconciliation did not recover")
	}
	cancel()
	state, err := service.managedPoolRepo.GetManagedPoolState(context.Background(), "admin-workspace", "public-agent")
	if err != nil || state == nil {
		t.Fatalf("pool was not restored after retry: state=%+v err=%v", state, err)
	}
	if backend.workspaceCalls != 2 {
		t.Fatalf("workspace calls = %d, want 2", backend.workspaceCalls)
	}
}

func TestReconcileManagedPoolsRetainsRemovedConfigIdentity(t *testing.T) {
	staleAt := time.Now().Add(-time.Hour)
	repo := &fakeComputeRepo{pools: map[string][]*model.PoolState{
		"admin-workspace": {
			newManagedPoolState("admin-workspace", "removed-agent-pool", types.WorkerPoolManagementSourceConfig, string(types.WorkerPoolManagementSourceConfig), types.WorkerPoolConfig{Mode: types.PoolModeExternal}, staleAt),
		},
	}}
	service := managedPoolTestService(types.AppConfig{}, repo)

	if err := service.ReconcileManagedPools(context.Background()); err != nil {
		t.Fatal(err)
	}
	state, err := service.managedPoolRepo.GetManagedPoolState(context.Background(), "admin-workspace", "removed-agent-pool")
	if err != nil || state == nil {
		t.Fatalf("stable config identity after reconcile = %+v, err = %v", state, err)
	}
}

func TestManagedPoolMachineMaintenance(t *testing.T) {
	now := time.Date(2026, time.July, 13, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name         string
		lastSeen     time.Time
		wantMachine  bool
		wantWorker   bool
		workerStatus types.WorkerStatus
	}{
		{
			name:         "disable disconnected machine",
			lastSeen:     now.Add(-model.AgentHeartbeatTimeout - time.Second),
			wantMachine:  true,
			wantWorker:   true,
			workerStatus: types.WorkerStatusDisabled,
		},
		{
			name:        "prune expired machine",
			lastSeen:    now.Add(-staleMachineRetention - time.Second),
			wantMachine: false,
			wantWorker:  false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			const (
				workspaceID = "admin-workspace"
				poolName    = "removed-config-pool"
				machineID   = "machine-1"
			)
			pool := newManagedPoolState(
				workspaceID,
				poolName,
				types.WorkerPoolManagementSourceConfig,
				string(types.WorkerPoolManagementSourceConfig),
				types.WorkerPoolConfig{Mode: types.PoolModeExternal},
				now.Add(-time.Hour),
			)
			machine := &model.AgentTokenState{
				TokenHash:             "agent-token",
				WorkspaceID:           workspaceID,
				PoolName:              poolName,
				MachineID:             machineID,
				ManagedPoolInstanceID: pool.ManagedInstanceID,
				Schedulable:           true,
				LastJoinAt:            test.lastSeen,
				LastHeartbeatAt:       test.lastSeen,
			}
			repo := &fakeComputeRepo{
				pools: map[string][]*model.PoolState{workspaceID: {pool}},
				machines: map[string][]*model.AgentTokenState{
					fakeComputeKey(workspaceID, poolName): {machine},
				},
			}
			service := managedPoolTestService(types.AppConfig{}, repo)
			workers := &fakeWorkerRepo{worker: &types.Worker{
				Id:        model.AgentMachineWorkerID(machineID),
				MachineId: machineID,
				PoolName:  poolName,
				Status:    types.WorkerStatusAvailable,
			}}
			service.workerRepo = workers

			if err := service.reconcileManagedComputeAt(context.Background(), now); err != nil {
				t.Fatal(err)
			}
			gotMachine, err := repo.GetAgentMachineState(context.Background(), workspaceID, poolName, machineID)
			if err != nil {
				t.Fatal(err)
			}
			if (gotMachine != nil) != test.wantMachine {
				t.Fatalf("machine after maintenance = %+v, want present = %v", gotMachine, test.wantMachine)
			}
			if (workers.worker != nil) != test.wantWorker {
				t.Fatalf("worker after maintenance = %+v, want present = %v", workers.worker, test.wantWorker)
			}
			if workers.worker != nil && workers.worker.Status != test.workerStatus {
				t.Fatalf("worker status = %q, want %q", workers.worker.Status, test.workerStatus)
			}

			definition, err := service.managedPoolRepo.GetManagedPoolState(context.Background(), workspaceID, poolName)
			if err != nil || definition == nil {
				t.Fatalf("managed definition was changed: state=%+v err=%v", definition, err)
			}
			legacy, err := repo.ListAllPoolStates(context.Background(), 0)
			if err != nil || len(legacy) != 0 {
				t.Fatalf("managed definition leaked into tenant state: states=%+v err=%v", legacy, err)
			}
		})
	}
}

func TestReconcileManagedPoolsDoesNotActivateStaleConfigStateWithInventory(t *testing.T) {
	staleAt := time.Now().Add(-time.Hour)
	stale := newManagedPoolState("admin-workspace", "changed-to-local", types.WorkerPoolManagementSourceConfig, string(types.WorkerPoolManagementSourceConfig), types.WorkerPoolConfig{Mode: types.PoolModeExternal}, staleAt)
	repo := &fakeComputeRepo{
		pools: map[string][]*model.PoolState{"admin-workspace": {stale}},
		machines: map[string][]*model.AgentTokenState{
			fakeComputeKey("admin-workspace", "changed-to-local"): {
				{WorkspaceID: "admin-workspace", PoolName: "changed-to-local", MachineID: "machine-1"},
			},
		},
	}
	service := managedPoolTestService(types.AppConfig{Worker: types.WorkerConfig{Pools: map[string]types.WorkerPoolConfig{
		"changed-to-local": {Mode: types.PoolModeLocal},
	}}}, repo)

	if err := service.ReconcileManagedPools(context.Background()); err != nil {
		t.Fatal(err)
	}
	state, getErr := service.managedPoolRepo.GetManagedPoolState(context.Background(), "admin-workspace", "changed-to-local")
	if getErr != nil || state == nil {
		t.Fatalf("stale state should remain available for draining: state=%+v err=%v", state, getErr)
	}
	if _, handled, createErr := service.CreateManagedMachine(context.Background(), clusterAdminAuth(), "changed-to-local"); createErr != nil || handled {
		t.Fatalf("CreateManagedMachine() handled = %v, err = %v; stale pool must not accept inventory", handled, createErr)
	}
	if handled, deleteErr := service.DeleteManagedMachine(context.Background(), clusterAdminAuth(), "changed-to-local", "machine-1"); deleteErr != nil || !handled {
		t.Fatalf("DeleteManagedMachine() handled = %v, err = %v; stale inventory must remain drainable", handled, deleteErr)
	}
}

func TestCreateManagedMachineUsesMachineBoundToken(t *testing.T) {
	repo := &fakeComputeRepo{}
	service := managedPoolTestService(types.AppConfig{GatewayService: types.GatewayServiceConfig{
		HTTP: types.HTTPConfig{ExternalHost: "gateway.example.com", ExternalPort: 443, TLS: true},
	}}, repo)
	if _, err := service.CreateManagedPool(context.Background(), clusterAdminAuth(), "public-cpu", types.WorkerPoolConfig{Mode: types.PoolModeExternal}); err != nil {
		t.Fatal(err)
	}
	bootstrap, handled, err := service.CreateManagedMachine(context.Background(), clusterAdminAuth(), "public-cpu")
	if err != nil || !handled {
		t.Fatalf("CreateManagedMachine() handled = %v, err = %v", handled, err)
	}
	if bootstrap.MachineID == "" || !strings.Contains(bootstrap.InstallCommand, "/install/agent") {
		t.Fatalf("bootstrap = %+v", bootstrap)
	}
	tokenState := repo.joinTokens[hashComputeToken(bootstrap.Token)]
	if tokenState == nil || tokenState.Mode != string(types.PoolModeExternal) || tokenState.MachineID != bootstrap.MachineID || tokenState.ManagedPoolInstanceID == "" {
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
	joined, err := repo.GetAgentMachineState(context.Background(), "admin-workspace", "public-cpu", bootstrap.MachineID)
	if err != nil || joined == nil || joined.Mode != string(types.PoolModeExternal) {
		t.Fatalf("joined managed machine = %+v, err = %v", joined, err)
	}
}

func TestDeletedManagedPoolTokenCannotJoinRecreatedPool(t *testing.T) {
	repo := &fakeComputeRepo{}
	service := managedPoolTestService(types.AppConfig{}, repo)
	if _, err := service.CreateManagedPool(context.Background(), clusterAdminAuth(), "public-cpu", types.WorkerPoolConfig{Mode: types.PoolModeExternal}); err != nil {
		t.Fatal(err)
	}
	bootstrap, handled, err := service.CreateManagedMachine(context.Background(), clusterAdminAuth(), "public-cpu")
	if err != nil || !handled {
		t.Fatalf("CreateManagedMachine() handled = %v, err = %v", handled, err)
	}
	oldTokenState := repo.joinTokens[hashComputeToken(bootstrap.Token)]
	if oldTokenState == nil || oldTokenState.ManagedPoolInstanceID == "" {
		t.Fatalf("old join token state = %+v", oldTokenState)
	}

	if err := service.DeleteManagedPool(context.Background(), clusterAdminAuth(), "public-cpu"); err != nil {
		t.Fatal(err)
	}
	if _, err := service.CreateManagedPool(context.Background(), clusterAdminAuth(), "public-cpu", types.WorkerPoolConfig{Mode: types.PoolModeExternal}); err != nil {
		t.Fatal(err)
	}
	newState, err := service.managedPoolRepo.GetManagedPoolState(context.Background(), "admin-workspace", "public-cpu")
	if err != nil || newState == nil {
		t.Fatalf("recreated pool state = %+v, err = %v", newState, err)
	}
	if newState.ManagedInstanceID == oldTokenState.ManagedPoolInstanceID {
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

func TestManagedPoolDeleteAndJoinCannotCreateOrphanMachine(t *testing.T) {
	rdb, err := repository.NewRedisClientForTest()
	if err != nil {
		t.Fatal(err)
	}
	computeRepo := repository.NewComputeRedisRepository(rdb)
	newReplica := func() *Service {
		return &Service{
			backendRepo:          &fakeManagedPoolBackendRepo{},
			computeRepo:          computeRepo,
			managedPoolRepo:      repository.NewManagedPoolRedisRepository(rdb),
			managedPoolInstances: map[string]string{},
		}
	}
	creator := newReplica()
	deleter := newReplica()
	managedRepo := creator.managedPoolRepo

	for i := 0; i < 25; i++ {
		name := fmt.Sprintf("race-pool-%d", i)
		if _, err := creator.CreateManagedPool(context.Background(), clusterAdminAuth(), name, types.WorkerPoolConfig{Mode: types.PoolModeExternal}); err != nil {
			t.Fatal(err)
		}
		bootstrap, handled, err := creator.CreateManagedMachine(context.Background(), clusterAdminAuth(), name)
		if err != nil || !handled {
			t.Fatalf("CreateManagedMachine() handled=%v err=%v", handled, err)
		}

		start := make(chan struct{})
		joinResult := make(chan *pb.JoinAgentResponse, 1)
		deleteResult := make(chan error, 1)
		go func() {
			<-start
			response, _ := creator.JoinAgent(context.Background(), &pb.JoinAgentRequest{
				JoinToken:          bootstrap.Token,
				MachineFingerprint: "fingerprint-" + name,
				Hostname:           name,
				CpuCount:           4,
				MemoryMb:           8192,
				Schedulable:        true,
			})
			joinResult <- response
		}()
		go func() {
			<-start
			deleteResult <- deleter.DeleteManagedPool(context.Background(), clusterAdminAuth(), name)
		}()
		close(start)

		joined := <-joinResult
		deleteErr := <-deleteResult
		state, stateErr := managedRepo.GetManagedPoolState(context.Background(), "admin-workspace", name)
		if stateErr != nil {
			t.Fatal(stateErr)
		}
		machine, machineErr := computeRepo.GetAgentMachineState(context.Background(), "admin-workspace", name, bootstrap.MachineID)
		if machineErr != nil {
			t.Fatal(machineErr)
		}
		if joined != nil && joined.Ok {
			if !errors.Is(deleteErr, model.ErrManagedPoolInUse) || state == nil || machine == nil {
				t.Fatalf("join won but delete was not rejected: response=%+v delete=%v state=%+v machine=%+v", joined, deleteErr, state, machine)
			}
			continue
		}
		if deleteErr != nil || state != nil || machine != nil {
			t.Fatalf("delete won but join left state: response=%+v delete=%v state=%+v machine=%+v", joined, deleteErr, state, machine)
		}
	}
}

func TestManagedJoinTokenCannotJoinUnmanagedPool(t *testing.T) {
	repo := &fakeComputeRepo{
		pools: map[string][]*model.PoolState{
			"admin-workspace": {
				{
					WorkspaceID:      "admin-workspace",
					Name:             "tenant-private",
					Mode:             string(types.PoolModePrivate),
					Config:           &pb.PoolConfig{Name: "tenant-private", Mode: string(types.PoolModePrivate)},
					CreatedByTokenID: "private-owner",
				},
			},
		},
		joinTokens: map[string]*model.JoinTokenState{},
	}
	service := managedPoolTestService(types.AppConfig{}, repo)
	rawToken := "managed-pool-join-token"
	repo.joinTokens[hashComputeToken(rawToken)] = &model.JoinTokenState{
		TokenHash:             hashComputeToken(rawToken),
		WorkspaceID:           "admin-workspace",
		PoolName:              "tenant-private",
		CreatedByTokenID:      "operator-token",
		ManagedPoolInstanceID: "wrong-managed-pool-instance",
		Mode:                  string(types.PoolModeExternal),
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
