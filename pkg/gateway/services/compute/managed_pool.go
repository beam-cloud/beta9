package compute

import (
	"context"
	"errors"
	"fmt"
	"reflect"
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
	if !config.AgentHosted() {
		return types.WorkerPoolConfig{}, errors.New("managed pools are agent-backed and cannot set provider")
	}
	config.Provider = nil
	// Selector-bound agent capacity uses private pool state, not managed pools.
	config.RequiresPoolSelector = false
	if config.ContainerRuntime == "" {
		config.ContainerRuntime = types.ContainerRuntimeRunc.String()
	}
	switch config.ContainerRuntime {
	case types.ContainerRuntimeRunc.String(), types.ContainerRuntimeGvisor.String():
	default:
		return types.WorkerPoolConfig{}, fmt.Errorf("unsupported container runtime %q", config.ContainerRuntime)
	}
	if config.ContainerRuntime == types.ContainerRuntimeGvisor.String() {
		config.CPUAffinityEnforced = true
	}
	if len(config.UserData) > 256*1024 {
		return types.WorkerPoolConfig{}, fmt.Errorf("user data exceeds 256 KiB")
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
		Preemptible:       config.Preemptable,
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
	next.Preemptible = config.Preemptable
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
	case config.Mode == types.PoolModeExternal && config.Provider != nil:
		return types.WorkerPoolControllerProvider
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
	return len(workers) > 0, nil
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
		inUse, err := s.managedPoolHasInventory(lockCtx, state)
		if err != nil {
			return err
		}
		if inUse {
			return model.ErrManagedPoolInUse
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
	machineID := "machine-" + uuid.NewString()
	token, tokenState, err := newPoolJoinToken(state.WorkspaceID, state.Name, time.Time{}, defaultPrivateJoinTTL, machineID)
	if err != nil {
		return nil, err
	}
	tokenState.Mode = string(types.PoolModeExternal)
	tokenState.ManagedPoolInstanceID = state.ManagedInstanceID
	if err := s.savePoolJoinToken(ctx, tokenState, defaultPrivateJoinTTL); err != nil {
		return nil, err
	}
	gatewayURL := strings.TrimRight(s.appConfig.GatewayService.HTTP.GetExternalURL(), "/")
	command := agentInstallCommand(gatewayURL, token, isLocalGatewayURL(gatewayURL), agentWorkerImage(s.appConfig))
	return &ManagedMachineBootstrap{
		MachineID:      machineID,
		PoolName:       state.Name,
		Token:          token,
		InstallCommand: command,
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
