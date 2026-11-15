package compute

import (
	"context"
	"fmt"
	"strings"
	"time"

	model "github.com/beam-cloud/beta9/pkg/compute"
	"github.com/beam-cloud/beta9/pkg/types"
)

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
	if err := s.revokeComputeJoinTokenHash(ctx, tokenHash); err != nil {
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

// validateProvisioning reports a pool that is to buy from a provider this
// gateway has no credentials for.
func (s *Service) validateProvisioning(config types.WorkerPoolConfig) error {
	if !config.Buys() {
		return nil
	}
	if _, ok := s.providers.Get(config.Provisioning.Provider); ok {
		return nil
	}
	configured := "none are"
	if names := s.providers.Names(); len(names) > 0 {
		configured = "configured: " + strings.Join(names, ", ")
	}
	return fmt.Errorf("provisioning.provider %q is not configured under providers (%s)", config.Provisioning.Provider, configured)
}
