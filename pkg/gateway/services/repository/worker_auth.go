package repository_services

import (
	"context"
	"errors"
	"strings"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
)

var errWorkerIdentityUnauthorized = errors.New("worker token is not bound to the claimed worker process")

// authorizeRegisteredWorkerToken binds a worker mutation to the exact token
// minted for that worker. Managed workers persist the token id on their Redis
// worker record; agent workers are checked against their authoritative slot.
// Merely holding another worker token (even in the same workspace) is never
// sufficient to act as a sibling worker.
func authorizeRegisteredWorkerToken(
	ctx context.Context,
	authInfo *auth.AuthInfo,
	worker *types.Worker,
	computeRepo repository.ComputeRepository,
) error {
	if authInfo == nil || authInfo.Token == nil || worker == nil ||
		!types.IsWorkerTokenType(authInfo.Token.TokenType) ||
		strings.TrimSpace(authInfo.Token.ExternalId) == "" {
		return errWorkerIdentityUnauthorized
	}
	if tokenID := strings.TrimSpace(worker.WorkerTokenId); tokenID != "" {
		if tokenID == authInfo.Token.ExternalId {
			return nil
		}
		return errWorkerIdentityUnauthorized
	}
	if computeRepo == nil {
		return errWorkerIdentityUnauthorized
	}

	workspaceID := strings.TrimSpace(worker.WorkspaceId)
	if workspaceID == "" && authInfo.Workspace != nil {
		workspaceID = strings.TrimSpace(authInfo.Workspace.ExternalId)
	}
	if workspaceID == "" || strings.TrimSpace(worker.PoolName) == "" || strings.TrimSpace(worker.MachineId) == "" {
		return errWorkerIdentityUnauthorized
	}
	if authInfo.Token.TokenType == types.TokenTypeWorkerPrivate &&
		(authInfo.Workspace == nil || authInfo.Workspace.ExternalId != workspaceID) {
		return errWorkerIdentityUnauthorized
	}

	slots, err := computeRepo.ListAgentWorkerSlotStates(ctx, workspaceID, worker.PoolName, worker.MachineId)
	if err != nil {
		return err
	}
	for _, slot := range slots {
		if slot != nil && slot.WorkerID == worker.Id && slot.WorkerTokenID == authInfo.Token.ExternalId {
			return nil
		}
	}
	return errWorkerIdentityUnauthorized
}
