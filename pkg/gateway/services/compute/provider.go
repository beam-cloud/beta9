package compute

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/auth"
	model "github.com/beam-cloud/beta9/pkg/compute"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
)

// Provider pools: a workspace contributes its own machines to the managed
// endpoint fleet. Machines join through the same agent install as private
// pools, but the pool is scheduled only by the platform's managed endpoint
// controller, and the workspace earns a share of the tokens sold on it.

const providerPoolPriority = int32(100)

// GetProviderJoinCommand ensures the workspace's provider pool for the GPU
// type exists and mints a join token for a new machine.
func (s *Service) GetProviderJoinCommand(ctx context.Context, in *pb.GetProviderJoinCommandRequest) (*pb.GetProviderJoinCommandResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)
	workspaceID, actorTokenID := computeWorkspaceID(authInfo), computeActorTokenID(authInfo)
	if workspaceID == "" || actorTokenID == "" {
		return &pb.GetProviderJoinCommandResponse{Ok: false, ErrMsg: "missing workspace auth"}, nil
	}
	gpu := string(types.NormalizeGPUType(strings.TrimSpace(in.GetGpu())))
	if gpu == "" || gpu == string(types.NO_GPU) || gpu == string(types.GPU_ANY) {
		return &pb.GetProviderJoinCommandResponse{Ok: false, ErrMsg: "gpu type is required"}, nil
	}
	poolName := model.ProviderPoolName(workspaceID, gpu)
	var state *model.PoolState
	err := s.withPoolStateLock(ctx, workspaceID, poolName, func(lockCtx context.Context) error {
		var err error
		state, err = s.ensureProviderPoolLocked(lockCtx, workspaceID, poolName, gpu, actorTokenID)
		return err
	})
	if err != nil {
		return &pb.GetProviderJoinCommandResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	command, token, expiresAt, err := s.createPrivatePoolJoinCommandForWorkspace(ctx, workspaceID, poolName, state.CreatedAt, in.GetTtl(), "")
	if err != nil {
		return &pb.GetProviderJoinCommandResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	return &pb.GetProviderJoinCommandResponse{Ok: true, Command: command, Token: token, ExpiresAt: timestampOrNil(expiresAt), PoolName: poolName}, nil
}

func (s *Service) ensureProviderPoolLocked(ctx context.Context, workspaceID, poolName, gpu, actorTokenID string) (*model.PoolState, error) {
	existing, err := s.computeRepo.GetPoolState(ctx, workspaceID, poolName)
	if err != nil {
		return nil, err
	}
	if existing != nil {
		if existing.Mode != string(types.PoolModeProvider) {
			return nil, fmt.Errorf("pool %q is not a provider pool", poolName)
		}
		return existing, nil
	}
	now := time.Now().UTC()
	config := &pb.PoolConfig{
		Name: poolName, Selector: poolName, Gpu: []string{gpu},
		Mode: string(types.PoolModeProvider), Transport: defaultPrivateTransport,
		Fallback: types.PrivatePoolFallbackFail, Priority: providerPoolPriority,
	}
	state := &model.PoolState{
		Name: poolName, Selector: poolName, Config: config, Status: types.ComputePoolStatusActive,
		Source: model.SourceAttached, Mode: config.Mode, Transport: config.Transport, Fallback: config.Fallback,
		Priority: config.Priority, CreatedByTokenID: actorTokenID, CreatedAt: now, UpdatedAt: now,
	}
	if err := s.computeRepo.SavePoolState(ctx, workspaceID, state); err != nil {
		return nil, err
	}
	if s.scheduler != nil {
		if err := s.scheduler.EnsureAgentPool(workspaceID, state); err != nil {
			return nil, s.rollbackPoolState(ctx, workspaceID, poolName, nil, err)
		}
	}
	return state, nil
}

// ListProviderMachines lists every machine the workspace has contributed,
// across its provider pools.
func (s *Service) ListProviderMachines(ctx context.Context, in *pb.ListProviderMachinesRequest) (*pb.ListProviderMachinesResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)
	workspaceID := computeWorkspaceID(authInfo)
	if workspaceID == "" {
		return &pb.ListProviderMachinesResponse{Ok: false, ErrMsg: "missing workspace auth"}, nil
	}
	states, err := s.listPrivatePoolStates(ctx, workspaceID, 0)
	if err != nil {
		return &pb.ListProviderMachinesResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	out := []*pb.Machine{}
	for _, state := range states {
		if state == nil || state.Mode != string(types.PoolModeProvider) {
			continue
		}
		machines, err := s.computeRepo.ListAgentTokenStates(ctx, workspaceID, state.Name)
		if err != nil {
			return &pb.ListProviderMachinesResponse{Ok: false, ErrMsg: err.Error()}, nil
		}
		for _, machine := range machines {
			out = append(out, s.agentMachineToProto(machine))
		}
	}
	return &pb.ListProviderMachinesResponse{Ok: true, Machines: out}, nil
}
