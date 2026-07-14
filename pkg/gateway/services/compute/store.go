package compute

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/beam-cloud/beta9/pkg/auth"
	model "github.com/beam-cloud/beta9/pkg/compute"
)

func (s *Service) savePrivatePoolState(ctx context.Context, workspaceID string, state *model.PoolState) error {
	return s.computeRepo.SavePoolState(ctx, workspaceID, state)
}

func (s *Service) withPoolStateLock(ctx context.Context, workspaceID, poolName string, fn func(context.Context) error) error {
	if s == nil || s.computeRepo == nil {
		return errors.New("compute repository is unavailable")
	}
	return s.computeRepo.WithPoolStateLock(ctx, workspaceID, poolName, fn)
}

func (s *Service) getPrivatePoolState(ctx context.Context, workspaceID, name string) (*model.PoolState, error) {
	return s.computeRepo.GetPoolState(ctx, workspaceID, name)
}

func (s *Service) listPrivatePoolStates(ctx context.Context, workspaceID string, limit int) ([]*model.PoolState, error) {
	return s.computeRepo.ListPoolStates(ctx, workspaceID, limit)
}

func (s *Service) deletePrivatePoolState(ctx context.Context, workspaceID, name string) error {
	return s.computeRepo.DeletePoolState(ctx, workspaceID, name)
}

func (s *Service) rollbackPoolState(ctx context.Context, workspaceID, poolName string, previous *model.PoolState, cause error) error {
	var rollbackErr error
	if previous == nil {
		rollbackErr = s.deletePrivatePoolState(ctx, workspaceID, poolName)
	} else {
		rollbackErr = s.savePrivatePoolState(ctx, workspaceID, previous)
	}
	return errors.Join(cause, rollbackErr)
}

func (s *Service) saveComputeJoinTokenState(ctx context.Context, state *model.JoinTokenState, ttl time.Duration) error {
	return s.computeRepo.SaveJoinTokenState(ctx, state, ttl)
}

func (s *Service) getComputeJoinTokenState(ctx context.Context, token string) (*model.JoinTokenState, error) {
	if token == "" {
		return nil, nil
	}
	return s.computeRepo.GetJoinTokenState(ctx, hashComputeToken(token))
}

func (s *Service) saveComputeAgentTokenState(ctx context.Context, state *model.AgentTokenState) error {
	return s.computeRepo.SaveAgentTokenState(ctx, state, 24*time.Hour)
}

func (s *Service) getComputeAgentTokenState(ctx context.Context, token string) (*model.AgentTokenState, error) {
	if token == "" {
		return nil, nil
	}
	return s.computeRepo.GetAgentTokenState(ctx, hashComputeToken(token))
}

func (s *Service) getCurrentComputeAgentTokenState(ctx context.Context, token string) (*model.AgentTokenState, error) {
	state, err := s.getComputeAgentTokenState(ctx, token)
	if err != nil || state == nil {
		return state, err
	}
	return s.currentComputeAgentState(ctx, state)
}

func (s *Service) currentComputeAgentState(ctx context.Context, state *model.AgentTokenState) (*model.AgentTokenState, error) {
	if state == nil {
		return nil, nil
	}
	current, err := s.computeRepo.GetAgentMachineState(ctx, state.WorkspaceID, state.PoolName, state.MachineID)
	if err != nil || current == nil {
		return current, err
	}
	if current.TokenHash != state.TokenHash {
		return nil, nil
	}
	return current, nil
}

func (s *Service) getOwnedPrivatePoolState(ctx context.Context, authInfo *auth.AuthInfo, poolName string) (*model.PoolState, error) {
	workspaceID := computeWorkspaceID(authInfo)
	if workspaceID == "" {
		return nil, fmt.Errorf("missing workspace auth")
	}

	state, err := s.getPrivatePoolState(ctx, workspaceID, poolName)
	if err != nil {
		return nil, err
	}
	if state == nil || !poolStateIsPrivate(state) || !computePoolCreatedByAuth(state, authInfo) {
		return nil, nil
	}
	return state, nil
}

func computePoolCreatedByAuth(state *model.PoolState, authInfo *auth.AuthInfo) bool {
	if state == nil || state.CreatedByTokenID == "" {
		return false
	}
	return state.CreatedByTokenID == computeOwnerTokenID(authInfo)
}

func computeOwnerTokenID(authInfo *auth.AuthInfo) string {
	if authInfo == nil || authInfo.Token == nil {
		return ""
	}
	return authInfo.Token.ExternalId
}

func computeWorkspaceID(authInfo *auth.AuthInfo) string {
	if authInfo == nil || authInfo.Workspace == nil {
		return ""
	}
	return authInfo.Workspace.ExternalId
}
