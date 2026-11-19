package compute

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/auth"
	model "github.com/beam-cloud/beta9/pkg/compute"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
)

type ManagedMachineBootstrap struct {
	MachineID      string
	PoolName       string
	Token          string
	InstallCommand string
}

func (s *Service) requireManagedPoolRepository() error {
	if s == nil || s.managedPoolRepo == nil {
		return errors.New("managed pool repository is unavailable")
	}
	return nil
}

func (s *Service) withManagedPoolStateLock(ctx context.Context, workspaceID, name string, fn func(context.Context) error) error {
	if err := s.requireManagedPoolRepository(); err != nil {
		return err
	}
	return s.managedPoolRepo.WithManagedPoolStateLock(ctx, workspaceID, name, fn)
}

func (s *Service) requireManagedPoolReconciler() error {
	if err := s.requireManagedPoolRepository(); err != nil {
		return err
	}
	if s.scheduler == nil {
		return errors.New("scheduler is unavailable")
	}
	return nil
}

func requireClusterAdmin(authInfo *auth.AuthInfo) error {
	if authInfo == nil || authInfo.Token == nil ||
		!authInfo.Token.Active || authInfo.Token.DisabledByClusterAdmin ||
		authInfo.Token.TokenType != types.TokenTypeClusterAdmin {
		return model.ErrManagedPermissionDenied
	}
	return nil
}

func (s *Service) adminWorkspaceID(ctx context.Context) (string, error) {
	if s == nil || s.backendRepo == nil {
		return "", errors.New("admin workspace is unavailable")
	}
	workspace, err := s.backendRepo.GetAdminWorkspace(ctx)
	if err != nil {
		return "", err
	}
	if workspace == nil || strings.TrimSpace(workspace.ExternalId) == "" {
		return "", errors.New("admin workspace is unavailable")
	}
	return workspace.ExternalId, nil
}

func (s *Service) managedPoolWorkspace(ctx context.Context, authInfo *auth.AuthInfo) (string, error) {
	if err := requireClusterAdmin(authInfo); err != nil {
		return "", err
	}
	return s.adminWorkspaceID(ctx)
}

func normalizeManagedPoolConfig(config types.WorkerPoolConfig) (types.WorkerPoolConfig, error) {
	if config.Mode == "" {
		config.Mode = types.PoolModeExternal
	}
	if config.Mode != types.PoolModeExternal {
		return types.WorkerPoolConfig{}, fmt.Errorf("managed pools must use mode %q", types.PoolModeExternal)
	}
	config.ApplyProvisioning()
	// Selector-bound agent capacity uses private pool state, not managed pools.
	config.RequiresPoolSelector = false
	// Runtime is the Kubernetes RuntimeClass used by local workers.
	// Agent-backed workers use Docker GPU injection, but keeping the canonical
	// NVIDIA default in the stored config makes GPU pool behavior consistent
	// across the API, config files, and the dashboard editor.
	if strings.TrimSpace(config.GPUType) != "" && strings.TrimSpace(config.Runtime) == "" {
		config.Runtime = "nvidia"
	}
	if config.ContainerRuntime == "" {
		config.ContainerRuntime = types.ContainerRuntimeRunc.String()
	}
	switch config.ContainerRuntime {
	case types.ContainerRuntimeRunc.String(), types.ContainerRuntimeGvisor.String(), types.ContainerRuntimeMicroVM.String():
	default:
		return types.WorkerPoolConfig{}, fmt.Errorf("unsupported container runtime %q", config.ContainerRuntime)
	}
	if err := config.Validate(); err != nil {
		return types.WorkerPoolConfig{}, err
	}
	for field, path := range map[string]string{
		"storage_path":          config.StoragePath,
		"images_path":           config.ImagesPath,
		"durable_disks_path":    config.DurableDisksPath,
		"cache.disk.host_path":  config.Cache.Disk.HostPath,
		"cache.disk.mount_path": config.Cache.Disk.MountPath,
	} {
		if path = strings.TrimSpace(path); path != "" && !filepath.IsAbs(path) {
			return types.WorkerPoolConfig{}, fmt.Errorf("%s must be an absolute path", field)
		}
	}
	return config, nil
}

func managedPoolConfig(name string, config types.WorkerPoolConfig) *pb.PoolConfig {
	pool := &pb.PoolConfig{
		Name:      name,
		Selector:  name,
		Mode:      string(types.PoolModeExternal),
		Transport: defaultPrivateTransport,
		Fallback:  defaultPrivateFallback,
		Priority:  config.Priority,
	}
	if config.GPUType != "" {
		pool.Gpu = []string{config.GPUType}
	}
	return pool
}

func newManagedPoolState(workspaceID, name string, source types.WorkerPoolManagementSource, createdBy string, config types.WorkerPoolConfig, now time.Time) *model.PoolState {
	configCopy := config
	return &model.PoolState{
		WorkspaceID:       workspaceID,
		Name:              name,
		Selector:          name,
		Config:            managedPoolConfig(name, config),
		Status:            types.ComputePoolStatusActive,
		Source:            model.SourceAttached,
		Mode:              string(types.PoolModeExternal),
		Transport:         defaultPrivateTransport,
		Fallback:          defaultPrivateFallback,
		Priority:          config.Priority,
		CreatedByTokenID:  createdBy,
		CreatedAt:         now,
		UpdatedAt:         now,
		ManagementSource:  source,
		ManagedInstanceID: uuid.NewString(),
		WorkerConfig:      &configCopy,
	}
}

func managedPoolStateWithConfig(state *model.PoolState, config types.WorkerPoolConfig) *model.PoolState {
	if state == nil {
		return nil
	}
	next := *state
	configCopy := config
	next.Selector = state.Name
	next.Config = managedPoolConfig(state.Name, config)
	next.Mode = string(types.PoolModeExternal)
	next.Transport = defaultPrivateTransport
	next.Fallback = defaultPrivateFallback
	next.Priority = config.Priority
	next.WorkerConfig = &configCopy
	return &next
}

// activeManagedPoolState overlays config-owned definitions with this
// replica's process config. Redis stores only their stable identity; mixed
// versions must never rewrite shared state back and forth during a rollout.
func (s *Service) activeManagedPoolState(state *model.PoolState) (*model.PoolState, error) {
	if state == nil || state.Name == "" || state.WorkspaceID == "" || state.WorkerConfig == nil {
		return nil, nil
	}
	if state.Name != state.Selector || state.Mode != string(types.PoolModeExternal) {
		return nil, errors.New("managed pool identity is invalid")
	}

	config := *state.WorkerConfig
	switch state.ManagementSource {
	case types.WorkerPoolManagementSourceConfig:
		var exists bool
		config, exists = s.appConfig.Worker.Pools[state.Name]
		if !exists || !config.AgentHosted() {
			return nil, nil
		}
	case types.WorkerPoolManagementSourceAPI:
		if _, exists := s.appConfig.Worker.Pools[state.Name]; exists {
			return nil, fmt.Errorf("API-managed pool %q conflicts with config", state.Name)
		}
	default:
		return nil, errors.New("managed pool source is invalid")
	}

	normalized, err := normalizeManagedPoolConfig(config)
	if err != nil {
		return nil, err
	}
	return managedPoolStateWithConfig(state, normalized), nil
}

// ReconcileManagedPools converges this replica's in-memory scheduler with the
// isolated managed-pool definitions in Redis. The distributed repository lock
// protects definitions; every replica still performs its own local hydration.
func (s *Service) ReconcileManagedPools(ctx context.Context) error {
	if err := s.requireManagedPoolReconciler(); err != nil {
		return err
	}

	s.managedPoolSyncMu.Lock()
	defer s.managedPoolSyncMu.Unlock()

	pools, catalogErr := s.managedPoolCatalog(ctx)
	if pools == nil {
		return catalogErr
	}
	return errors.Join(catalogErr, s.syncManagedPoolControllers(ctx, pools))
}

func (s *Service) managedPoolCatalog(ctx context.Context) (map[string]*model.PoolState, error) {
	workspaceID, err := s.adminWorkspaceID(ctx)
	if err != nil {
		return nil, err
	}
	states, err := s.managedPoolRepo.ListManagedPoolStates(ctx, workspaceID, 0)
	if err != nil {
		return nil, err
	}
	pools := make(map[string]*model.PoolState, len(states))
	for _, state := range states {
		if state == nil || state.Name == "" || state.WorkspaceID != workspaceID || state.WorkerConfig == nil {
			return nil, errors.New("invalid managed pool state")
		}
		if pools[state.Name] != nil {
			return nil, fmt.Errorf("duplicate managed pool %q", state.Name)
		}
		pools[state.Name] = state
	}

	names := make([]string, 0, len(s.appConfig.Worker.Pools))
	for name, config := range s.appConfig.Worker.Pools {
		if config.AgentHosted() {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	var catalogErrors []error
	for _, name := range names {
		config, err := normalizeManagedPoolConfig(s.appConfig.Worker.Pools[name])
		if err != nil {
			catalogErrors = append(catalogErrors, fmt.Errorf("pool %q: %w", name, err))
			continue
		}
		if state := pools[name]; state != nil {
			if state.ManagementSource != types.WorkerPoolManagementSourceConfig {
				catalogErrors = append(catalogErrors, fmt.Errorf("config-managed pool %q conflicts with API-managed state", name))
			}
			continue
		}

		var created *model.PoolState
		err = s.withManagedPoolStateLock(ctx, workspaceID, name, func(lockCtx context.Context) error {
			current, err := s.managedPoolRepo.GetManagedPoolState(lockCtx, workspaceID, name)
			if err != nil {
				return err
			}
			if current != nil {
				if current.ManagementSource != types.WorkerPoolManagementSourceConfig {
					return fmt.Errorf("config-managed pool %q conflicts with API-managed state", name)
				}
				created = current
				return nil
			}
			created = newManagedPoolState(workspaceID, name, types.WorkerPoolManagementSourceConfig, string(types.WorkerPoolManagementSourceConfig), config, time.Now().UTC())
			return s.managedPoolRepo.SaveManagedPoolState(lockCtx, workspaceID, created)
		})
		if err != nil {
			catalogErrors = append(catalogErrors, fmt.Errorf("reconcile config pool %q: %w", name, err))
			continue
		}
		pools[name] = created
	}

	for name, state := range pools {
		if state.ManagedInstanceID == "" {
			delete(pools, name)
			catalogErrors = append(catalogErrors, fmt.Errorf("managed pool %q has no instance identity", name))
		}
	}
	return pools, errors.Join(catalogErrors...)
}

func (s *Service) syncManagedPoolControllers(ctx context.Context, pools map[string]*model.PoolState) error {
	names := make([]string, 0, len(pools))
	for name := range pools {
		names = append(names, name)
	}
	sort.Strings(names)
	next := make(map[string]string, len(names))
	var syncErrors []error

	for _, name := range names {
		state, err := s.activeManagedPoolState(pools[name])
		if err != nil {
			syncErrors = append(syncErrors, fmt.Errorf("validate managed pool %q: %w", name, err))
			continue
		}
		if state == nil {
			// Config removal is local during a rolling deployment. Stop using the
			// controller on this replica, but leave the stable shared identity
			// untouched so mixed configs cannot create/delete it in a loop.
			continue
		}
		instanceID := state.ManagedInstanceID
		if instanceID == "" {
			syncErrors = append(syncErrors, fmt.Errorf("managed pool %q has no instance identity", name))
			continue
		}
		installed := s.managedPoolInstances[name] == instanceID
		if err := s.scheduler.EnsureAgentPool(state.WorkspaceID, state); err != nil {
			syncErrors = append(syncErrors, fmt.Errorf("hydrate managed pool %q: %w", name, err))
			continue
		}

		if !installed {
			if err := s.confirmManagedPool(ctx, state); err != nil {
				s.scheduler.DeleteManagedAgentPool(name, instanceID)
				syncErrors = append(syncErrors, fmt.Errorf("confirm managed pool %q: %w", name, err))
				continue
			}
		}
		next[name] = instanceID
	}

	for name, instanceID := range s.managedPoolInstances {
		if _, ok := next[name]; !ok {
			s.scheduler.DeleteManagedAgentPool(name, instanceID)
		}
	}
	s.managedPoolInstances = next
	return errors.Join(syncErrors...)
}

func (s *Service) confirmManagedPool(ctx context.Context, expected *model.PoolState) error {
	current, err := s.managedPoolRepo.GetManagedPoolState(ctx, expected.WorkspaceID, expected.Name)
	if err != nil {
		return err
	}
	current, err = s.activeManagedPoolState(current)
	if err != nil {
		return err
	}
	if current == nil || current.ManagementSource != expected.ManagementSource || current.ManagedInstanceID != expected.ManagedInstanceID {
		return errors.New("managed pool changed during reconciliation")
	}
	return nil
}

func (s *Service) ensureManagedAgentMachine(ctx context.Context, state *model.PoolState, machineID string) error {
	s.managedPoolSyncMu.Lock()
	defer s.managedPoolSyncMu.Unlock()

	if err := s.scheduler.EnsureAgentMachine(state.WorkspaceID, state, machineID); err != nil {
		return err
	}
	if err := s.confirmManagedPool(ctx, state); err != nil {
		s.scheduler.DeleteManagedAgentPool(state.Name, state.ManagedInstanceID)
		if s.managedPoolInstances[state.Name] == state.ManagedInstanceID {
			delete(s.managedPoolInstances, state.Name)
		}
		return err
	}
	if s.managedPoolInstances == nil {
		s.managedPoolInstances = map[string]string{}
	}
	s.managedPoolInstances[state.Name] = state.ManagedInstanceID
	return nil
}

func (s *Service) managedPoolState(ctx context.Context, authInfo *auth.AuthInfo, name string) (*model.PoolState, error) {
	state, err := s.persistedManagedPoolState(ctx, authInfo, name)
	if err != nil {
		return nil, err
	}
	return s.activeManagedPoolState(state)
}

func (s *Service) persistedManagedPoolState(ctx context.Context, authInfo *auth.AuthInfo, name string) (*model.PoolState, error) {
	workspaceID, err := s.managedPoolWorkspace(ctx, authInfo)
	if err != nil {
		return nil, err
	}
	if err := s.requireManagedPoolRepository(); err != nil {
		return nil, err
	}
	state, err := s.managedPoolRepo.GetManagedPoolState(ctx, workspaceID, name)
	if err != nil {
		return nil, err
	}
	if state == nil || state.ManagementSource == "" || state.Mode != string(types.PoolModeExternal) || state.WorkerConfig == nil {
		return nil, nil
	}
	return state, nil
}

func (s *Service) managedPoolView(ctx context.Context, state *model.PoolState) (*model.ManagedPool, error) {
	if state == nil || state.WorkerConfig == nil {
		return nil, model.ErrManagedPoolNotFound
	}
	view := &model.ManagedPool{
		Name:       state.Name,
		Config:     *state.WorkerConfig,
		Source:     state.ManagementSource,
		Controller: types.WorkerPoolControllerAgent,
	}
	if s.workerPoolRepo != nil {
		poolState, err := s.workerPoolRepo.GetWorkerPoolState(ctx, state.Name)
		if err == nil {
			view.State = poolState
		}
	}
	if s.computeRepo == nil {
		return view, nil
	}
	machines, err := s.computeRepo.ListAgentTokenStates(ctx, state.WorkspaceID, state.Name)
	if err != nil {
		return nil, err
	}
	view.MachineCount = len(machines)
	now := time.Now().UTC()
	for _, machine := range machines {
		if machine != nil && machine.Schedulable && model.AgentMachineConnected(machine, now) {
			view.ReadyMachineCount++
		}
	}
	return view, nil
}

func configuredPoolController(config types.WorkerPoolConfig) types.WorkerPoolController {
	switch {
	case config.Mode == types.PoolModeLocal:
		return types.WorkerPoolControllerLocal
	case config.AgentHosted():
		return types.WorkerPoolControllerAgent
	default:
		return ""
	}
}

func (s *Service) ListManagedPools(ctx context.Context, authInfo *auth.AuthInfo) ([]*model.ManagedPool, error) {
	workspaceID, err := s.managedPoolWorkspace(ctx, authInfo)
	if err != nil {
		return nil, err
	}

	persisted := map[string]*model.PoolState{}
	if err := s.requireManagedPoolRepository(); err != nil {
		return nil, err
	}
	states, err := s.managedPoolRepo.ListManagedPoolStates(ctx, workspaceID, 0)
	if err != nil {
		return nil, err
	}
	for _, state := range states {
		if state != nil {
			persisted[state.Name] = state
		}
	}

	views := make(map[string]*model.ManagedPool, len(s.appConfig.Worker.Pools)+len(persisted))
	for name, config := range s.appConfig.Worker.Pools {
		view := &model.ManagedPool{
			Name:       name,
			Config:     config,
			Source:     types.WorkerPoolManagementSourceConfig,
			Controller: configuredPoolController(config),
		}
		if s.workerPoolRepo != nil {
			if poolState, stateErr := s.workerPoolRepo.GetWorkerPoolState(ctx, name); stateErr == nil {
				view.State = poolState
				view.MachineCount = int(poolState.RegisteredMachines)
				view.ReadyMachineCount = int(poolState.ReadyMachines)
			}
		}
		if state := persisted[name]; state != nil && view.Controller == types.WorkerPoolControllerAgent {
			state, stateErr := s.activeManagedPoolState(state)
			if stateErr != nil {
				return nil, stateErr
			}
			if state != nil {
				view, err = s.managedPoolView(ctx, state)
				if err != nil {
					return nil, err
				}
			}
		}
		views[name] = view
	}

	for _, state := range persisted {
		if state.ManagementSource != types.WorkerPoolManagementSourceAPI {
			continue
		}
		state, err = s.activeManagedPoolState(state)
		if err != nil {
			return nil, err
		}
		if state == nil {
			continue
		}
		if _, collision := views[state.Name]; collision {
			return nil, fmt.Errorf("pool %q exists in config and persisted state", state.Name)
		}
		view, err := s.managedPoolView(ctx, state)
		if err != nil {
			return nil, err
		}
		views[state.Name] = view
	}

	names := make([]string, 0, len(views))
	for name := range views {
		names = append(names, name)
	}
	sort.Strings(names)
	out := make([]*model.ManagedPool, 0, len(names))
	for _, name := range names {
		out = append(out, views[name])
	}
	return out, nil
}

func (s *Service) CreateManagedPool(ctx context.Context, authInfo *auth.AuthInfo, name string, config types.WorkerPoolConfig) (*model.ManagedPool, error) {
	workspaceID, err := s.managedPoolWorkspace(ctx, authInfo)
	if err != nil {
		return nil, err
	}
	if err := s.requireManagedPoolRepository(); err != nil {
		return nil, err
	}
	name = strings.TrimSpace(name)
	if name == "" {
		return nil, fmt.Errorf("%w: pool name is required", model.ErrInvalidManagedPoolConfig)
	}
	if err := model.ValidatePoolName(name); err != nil {
		return nil, fmt.Errorf("%w: %v", model.ErrInvalidManagedPoolConfig, err)
	}
	if _, exists := s.appConfig.Worker.Pools[name]; exists {
		return nil, model.ErrManagedPoolConflict
	}
	config, err = normalizeManagedPoolConfig(config)
	if err == nil {
		err = s.validateProvider(config)
	}
	if err != nil {
		return nil, fmt.Errorf("%w: %v", model.ErrInvalidManagedPoolConfig, err)
	}
	state := newManagedPoolState(workspaceID, name, types.WorkerPoolManagementSourceAPI, computeActorTokenID(authInfo), config, time.Now().UTC())

	err = s.withManagedPoolStateLock(ctx, workspaceID, name, func(lockCtx context.Context) error {
		existing, err := s.managedPoolRepo.GetManagedPoolState(lockCtx, workspaceID, name)
		if err != nil {
			return err
		}
		if existing != nil {
			return model.ErrManagedPoolConflict
		}
		if err := s.managedPoolRepo.SaveManagedPoolState(lockCtx, workspaceID, state); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return s.managedPoolView(ctx, state)
}

func (s *Service) managedPoolHasInventory(ctx context.Context, state *model.PoolState) (bool, error) {
	for _, reservation := range state.Reservations {
		if reservation.Managed() && !reservation.Closed() {
			return true, nil
		}
	}
	if s.computeRepo == nil {
		return false, errors.New("compute repository is unavailable")
	}
	machines, err := s.computeRepo.ListAgentTokenStates(ctx, state.WorkspaceID, state.Name)
	if err != nil {
		return false, err
	}
	if len(machines) > 0 {
		return true, nil
	}
	if s.workerRepo == nil {
		return false, nil
	}
	workers, err := s.workerRepo.GetAllWorkersInPool(state.Name)
	if err != nil {
		return false, err
	}
	// A workspace can give a pool of its own this pool's name. The workers of
	// that pool are not this one's.
	return slices.ContainsFunc(workers, func(worker *types.Worker) bool { return worker.ControlPlaneManaged }), nil
}

func (s *Service) UpdateManagedPool(ctx context.Context, authInfo *auth.AuthInfo, name string, config types.WorkerPoolConfig) (*model.ManagedPool, error) {
	workspaceID, err := s.managedPoolWorkspace(ctx, authInfo)
	if err != nil {
		return nil, err
	}
	if err := s.requireManagedPoolRepository(); err != nil {
		return nil, err
	}
	config, err = normalizeManagedPoolConfig(config)
	if err == nil {
		err = s.validateProvider(config)
	}
	if err != nil {
		return nil, fmt.Errorf("%w: %v", model.ErrInvalidManagedPoolConfig, err)
	}
	var updated *model.PoolState
	err = s.withManagedPoolStateLock(ctx, workspaceID, name, func(lockCtx context.Context) error {
		existing, err := s.managedPoolRepo.GetManagedPoolState(lockCtx, workspaceID, name)
		if err != nil {
			return err
		}
		if existing == nil || existing.ManagementSource == "" {
			return model.ErrManagedPoolNotFound
		}
		if existing.ManagementSource != types.WorkerPoolManagementSourceAPI {
			return model.ErrManagedPoolImmutable
		}
		if existing.WorkerConfig == nil {
			return model.ErrManagedPoolNotFound
		}
		if reflect.DeepEqual(*existing.WorkerConfig, config) {
			updated = existing
			return nil
		}
		if config.Buys() && !existing.WorkerConfig.Buys() {
			inUse, err := s.managedPoolHasInventory(lockCtx, existing)
			if err != nil {
				return err
			}
			if inUse {
				return fmt.Errorf("%w: %q has machines that were joined by hand; remove them first, or create a new pool",
					model.ErrManagedPoolBuysMachines, name)
			}
		}
		updated = managedPoolStateWithConfig(existing, config)
		updated.UpdatedAt = time.Now().UTC()
		if err := s.managedPoolRepo.SaveManagedPoolState(lockCtx, workspaceID, updated); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if err := s.notifyAgentPool(ctx, updated.WorkspaceID, updated.Name); err != nil {
		log.Warn().Err(err).Str("pool_name", updated.Name).Msg("failed to notify pool agents; periodic refresh remains active")
	}
	return s.managedPoolView(ctx, updated)
}

func (s *Service) DeleteManagedPool(ctx context.Context, authInfo *auth.AuthInfo, name string) error {
	workspaceID, err := s.managedPoolWorkspace(ctx, authInfo)
	if err != nil {
		return err
	}
	if err := s.requireManagedPoolRepository(); err != nil {
		return err
	}
	err = s.withManagedPoolStateLock(ctx, workspaceID, name, func(lockCtx context.Context) error {
		state, err := s.managedPoolRepo.GetManagedPoolState(lockCtx, workspaceID, name)
		if err != nil {
			return err
		}
		if state == nil || state.ManagementSource == "" {
			return model.ErrManagedPoolNotFound
		}
		if state.ManagementSource != types.WorkerPoolManagementSourceAPI {
			return model.ErrManagedPoolImmutable
		}
		// A pool that buys its machines can be deleted while it has some:
		// the provisioner drains them and gives them back. Machines that
		// were joined by hand have nobody to take them away, and such a pool
		// has none.
		if state.WorkerConfig == nil || !state.WorkerConfig.Buys() {
			inUse, err := s.managedPoolHasInventory(lockCtx, state)
			if err != nil {
				return err
			}
			if inUse {
				return model.ErrManagedPoolInUse
			}
		}
		if s.workerPoolRepo != nil {
			if err := s.workerPoolRepo.DeleteWorkerPoolState(lockCtx, name); err != nil {
				return err
			}
		}
		if err := s.managedPoolRepo.DeleteManagedPoolState(lockCtx, workspaceID, name); err != nil {
			return err
		}
		return nil
	})
	return err
}

func (s *Service) CreateManagedMachine(ctx context.Context, authInfo *auth.AuthInfo, poolName string) (*ManagedMachineBootstrap, error) {
	state, err := s.managedPoolState(ctx, authInfo, poolName)
	if err != nil {
		return nil, err
	}
	if state == nil {
		return nil, model.ErrManagedPoolNotFound
	}
	if state.ManagedInstanceID == "" {
		return nil, fmt.Errorf("pool %q has not completed secure reconciliation", state.Name)
	}
	if state.ManagementSource == types.WorkerPoolManagementSourceConfig {
		configured, ok := s.appConfig.Worker.Pools[state.Name]
		if !ok || !configured.AgentHosted() {
			return nil, model.ErrManagedPoolNotFound
		}
	}
	if state.WorkerConfig.Buys() {
		return nil, fmt.Errorf("%w: %q gets its machines from %s; raise provisioning.maxNodes for more",
			model.ErrManagedPoolBuysMachines, state.Name, strings.Join(state.WorkerConfig.Provisioning.Providers, ", "))
	}
	machineID := "machine-" + uuid.NewString()
	token, _, err := s.createManagedMachineJoinToken(ctx, state, machineID, defaultPrivateJoinTTL)
	if err != nil {
		return nil, err
	}
	return &ManagedMachineBootstrap{
		MachineID:      machineID,
		PoolName:       state.Name,
		Token:          token,
		InstallCommand: s.joinCommandForToken(token),
	}, nil
}

func (s *Service) ListManagedMachines(ctx context.Context, authInfo *auth.AuthInfo, poolName string) ([]*pb.Machine, error) {
	workspaceID, err := s.managedPoolWorkspace(ctx, authInfo)
	if err != nil {
		return nil, err
	}
	if err := s.requireManagedPoolRepository(); err != nil {
		return nil, err
	}
	states, err := s.managedPoolRepo.ListManagedPoolStates(ctx, workspaceID, 0)
	if err != nil {
		return nil, err
	}
	found := poolName == ""
	out := []*pb.Machine{}
	for _, state := range states {
		if state == nil || state.ManagementSource == "" || state.WorkerConfig == nil {
			continue
		}
		if poolName != "" && state.Name != poolName {
			continue
		}
		found = true
		machines, err := s.computeRepo.ListAgentTokenStates(ctx, workspaceID, state.Name)
		if err != nil {
			return nil, err
		}
		for _, machine := range machines {
			out = append(out, s.agentMachineToProto(machine))
		}
	}
	if !found {
		return nil, model.ErrManagedPoolNotFound
	}
	s.fillMissingMachineNetwork(ctx, out)
	sort.Slice(out, func(i, j int) bool { return out[i].Id < out[j].Id })
	return out, nil
}

func (s *Service) DeleteManagedMachine(ctx context.Context, authInfo *auth.AuthInfo, poolName, machineID string) error {
	state, err := s.persistedManagedPoolState(ctx, authInfo, poolName)
	if err != nil || state == nil {
		if err != nil {
			return err
		}
		return model.ErrManagedPoolNotFound
	}
	machine, err := s.computeRepo.GetAgentMachineState(ctx, state.WorkspaceID, state.Name, machineID)
	if err != nil {
		return err
	}
	if machine == nil {
		return model.ErrManagedPoolNotFound
	}
	// A machine the pool bought is the provisioner's to give back. Removed
	// here it would be paid for until the provisioner missed its worker, and
	// could join again in the meantime.
	if s.buysMachines(state) {
		return fmt.Errorf("%w: %q gives its machines back itself, once they are idle", model.ErrManagedPoolBuysMachines, state.Name)
	}
	if s.containerRepo != nil {
		containers, err := s.containerRepo.GetActiveContainersByWorkerId(model.AgentMachineWorkerID(machine.MachineID))
		if err != nil {
			return err
		}
		if len(containers) > 0 {
			return fmt.Errorf("machine must be drained before deletion: %w", model.ErrManagedPoolInUse)
		}
	}
	return s.removePrivateMachine(ctx, machine)
}

// buysMachines reports whether the pool of a stored record buys its machines.
// A pool of the config file is what this replica's config says it is: its
// record keeps the config the pool was first stored with.
func (s *Service) buysMachines(stored *model.PoolState) bool {
	if stored.ManagementSource == types.WorkerPoolManagementSourceConfig {
		return s.appConfig.Worker.Pools[stored.Name].Buys()
	}
	return stored.WorkerConfig.Buys()
}

// SetAgentWorkerCordon persists operator intent on every agent-managed machine
// so a worker TTL expiry or agent reconnect cannot silently undo a cordon.
func (s *Service) SetAgentWorkerCordon(ctx context.Context, worker *types.Worker, cordoned bool) (bool, error) {
	if s == nil || s.computeRepo == nil || worker == nil || worker.WorkspaceId == "" || worker.PoolName == "" || worker.MachineId == "" {
		return false, nil
	}
	machine, err := s.computeRepo.GetAgentMachineState(ctx, worker.WorkspaceId, worker.PoolName, worker.MachineId)
	if err != nil || machine == nil {
		return false, err
	}
	if worker.Id != model.AgentMachineWorkerID(machine.MachineID) {
		return false, nil
	}
	machine.Cordoned = cordoned
	if err := s.saveComputeAgentTokenState(ctx, machine); err != nil {
		return true, err
	}
	if err = s.workerRepo.SetWorkerCordon(worker.Id, cordoned); err != nil {
		return true, err
	}
	if err = s.notifyAgentPool(ctx, machine.WorkspaceID, machine.PoolName); err != nil {
		log.Warn().Err(err).Str("worker_id", worker.Id).Msg("failed to notify cordoned agent; periodic refresh remains active")
	}
	return true, nil
}

// The gateway's side of machines that are bought for a pool. The provisioner
// (pkg/provisioner) decides what to buy and when to give it back; this is
// how such a machine joins its pool, and what is cleaned up when it leaves.

// BuyingPools returns the config of every pool that buys its machines, by
// name, and why each pool that could not be loaded could not. Such a pool
// may buy machines too. It fails when the pools could not be read at all.
func (s *Service) BuyingPools(ctx context.Context) (map[string]types.WorkerPoolConfig, map[string]error, error) {
	if err := s.requireManagedPoolRepository(); err != nil {
		return nil, nil, err
	}
	// What the catalog found wrong with single pools is reported by the
	// reconciler of managed pools. Here it only says which pools are in
	// doubt.
	catalog, catalogErr := s.managedPoolCatalog(ctx)
	if catalog == nil && catalogErr != nil {
		return nil, nil, catalogErr
	}

	pools := map[string]types.WorkerPoolConfig{}
	unloaded := map[string]error{}
	for name, persisted := range catalog {
		state, err := s.activeManagedPoolState(persisted)
		switch {
		case err != nil:
			unloaded[name] = err
		case state != nil && state.WorkerConfig.Buys():
			// Nil is a pool this replica's config no longer has.
			pools[name] = *state.WorkerConfig
		}
	}
	// A pool of this replica's config that the catalog left out is one it
	// found something wrong with.
	if catalogErr != nil {
		for name, config := range s.appConfig.Worker.Pools {
			if _, listed := catalog[name]; !listed && config.Buys() {
				unloaded[name] = catalogErr
			}
		}
	}
	return pools, unloaded, nil
}

// JoinScript returns the shell script that makes a new machine join the pool
// as machineID, and the hash of the token in it.
func (s *Service) JoinScript(ctx context.Context, pool, machineID string) (string, string, error) {
	state, err := s.buyingPoolState(ctx, pool)
	if err != nil {
		return "", "", err
	}
	// How long a machine takes to boot is the provider's business, so the
	// token does not expire. It admits one machine ID, and it is revoked
	// when the machine is given back.
	token, tokenState, err := s.createManagedMachineJoinToken(ctx, state, machineID, 0)
	if err != nil {
		return "", "", err
	}
	return s.joinCommandForToken(token), tokenState.TokenHash, nil
}

// ForgetMachine removes what the gateway holds about a machine that was
// bought for a pool: the token it joins with, its worker, whose requests go
// back to the backlog, and its agent. A machine may be forgotten twice, or
// before it ever joined.
func (s *Service) ForgetMachine(ctx context.Context, pool, machineID, tokenHash string) error {
	workspaceID, err := s.adminWorkspaceID(ctx)
	if err != nil {
		return err
	}
	if err := s.forgetJoinToken(ctx, tokenHash); err != nil {
		return err
	}
	machine, err := s.computeRepo.GetAgentMachineState(ctx, workspaceID, pool, machineID)
	if err != nil {
		return err
	}
	if machine == nil {
		// It never joined. All it has is the worker that stood for it while
		// it booted.
		return s.removePrivateMachineWorker(&model.AgentTokenState{WorkspaceID: workspaceID, PoolName: pool, MachineID: machineID})
	}
	return s.removePrivateMachine(ctx, machine)
}

// revokedJoinTokenKept is how long the join token of a machine that was
// bought stays in Redis once it is revoked: the shortest life the repository
// gives a token.
const revokedJoinTokenKept = time.Second

// forgetJoinToken ends the join token of a machine that was bought: it
// admits nothing from now on, and a moment later it is gone from Redis. The
// token does not expire, so one that was only marked as revoked would stay
// for good. A token that is gone already is not an error.
//
// The repository cannot delete a token, so it is saved once more: marked as
// revoked, which every release honours, and to lapse at once.
func (s *Service) forgetJoinToken(ctx context.Context, tokenHash string) error {
	if tokenHash == "" {
		return nil
	}
	state, err := s.computeRepo.GetJoinTokenState(ctx, tokenHash)
	if err != nil || state == nil {
		return err
	}
	state.Revoked = true
	return s.saveComputeJoinTokenState(ctx, state, revokedJoinTokenKept)
}

// buyingPoolState returns the state of a pool that buys its machines, as
// this replica runs it.
func (s *Service) buyingPoolState(ctx context.Context, pool string) (*model.PoolState, error) {
	if err := s.requireManagedPoolRepository(); err != nil {
		return nil, err
	}
	workspaceID, err := s.adminWorkspaceID(ctx)
	if err != nil {
		return nil, err
	}
	persisted, err := s.managedPoolRepo.GetManagedPoolState(ctx, workspaceID, pool)
	if err != nil {
		return nil, err
	}
	state, err := s.activeManagedPoolState(persisted)
	if err != nil {
		return nil, err
	}
	if state == nil || !state.WorkerConfig.Buys() {
		return nil, fmt.Errorf("pool %q does not buy machines", pool)
	}
	if state.ManagedInstanceID == "" {
		return nil, fmt.Errorf("pool %q has not completed secure reconciliation", pool)
	}
	return state, nil
}

// createManagedMachineJoinToken stores a token that lets one machine join a
// managed pool as machineID. A ttl of zero means it does not expire.
func (s *Service) createManagedMachineJoinToken(ctx context.Context, state *model.PoolState, machineID string, ttl time.Duration) (string, *model.JoinTokenState, error) {
	token, tokenState, err := newPoolJoinToken(state.WorkspaceID, state.Name, time.Time{}, ttl, machineID)
	if err != nil {
		return "", nil, err
	}
	tokenState.Mode = string(types.PoolModeExternal)
	tokenState.ManagedPoolInstanceID = state.ManagedInstanceID
	if err := s.savePoolJoinToken(ctx, tokenState, ttl); err != nil {
		return "", nil, err
	}
	return token, tokenState, nil
}

// validateProvider reports a pool that is to buy from a provider this
// gateway cannot buy from: one it has no credentials for, or one that is
// disabled.
func (s *Service) validateProvider(config types.WorkerPoolConfig) error {
	names := config.Provisioning.Providers
	i := slices.IndexFunc(names, func(name string) bool { return !s.providers.IsEnabled(name) })
	if i < 0 {
		return nil
	}
	name := names[i]
	// others names the providers a pool can buy from, after the word for
	// what this one is not.
	others := func(word string) string {
		if names := s.providers.Names(); len(names) > 0 {
			return word + ": " + strings.Join(names, ", ")
		}
		return "none are " + word
	}
	if _, configured := s.providers.Get(name); configured {
		return fmt.Errorf("provisioning.providers: %q is disabled under providers.disabled (%s)", name, others("enabled"))
	}
	return fmt.Errorf("provisioning.providers: %q is not configured under providers (%s)", name, others("configured"))
}
