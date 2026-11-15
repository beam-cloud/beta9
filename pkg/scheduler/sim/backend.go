package schedsim

import (
	"context"
	"database/sql"
	"fmt"
	"sync"

	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
)

// backend answers the few Postgres queries the scheduler makes. Any other
// query panics on the nil embedded interface, which is how a scenario finds
// out that the scheduler started to depend on one.
type backend struct {
	repository.BackendRepository

	mu sync.Mutex
	// tokens and revoked count the worker tokens made and revoked.
	tokens  int
	revoked int
}

func (b *backend) GetAdminWorkspace(ctx context.Context) (*types.Workspace, error) {
	return &types.Workspace{Id: 1, ExternalId: "admin", Name: "admin"}, nil
}

func (b *backend) CreateWorkerToken(ctx context.Context, workspaceId uint, tokenType string, workerId string) (types.Token, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.tokens++
	return types.Token{Key: fmt.Sprintf("worker-token-%d", b.tokens)}, nil
}

func (b *backend) BindWorkerToken(ctx context.Context, tokenKey, workerId string) (bool, error) {
	return true, nil
}

// RevokeTokenByExternalId is what a pool asks for a worker it could not add.
func (b *backend) RevokeTokenByExternalId(ctx context.Context, externalId string) (string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.revoked++
	return "", nil
}

// workerTokens returns how many worker tokens were made, and how many of
// them were revoked.
func (b *backend) workerTokens() (made, revoked int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.tokens, b.revoked
}

// GetConcurrencyLimitByWorkspaceId reports no limit, so the platform default
// applies.
func (b *backend) GetConcurrencyLimitByWorkspaceId(ctx context.Context, workspaceId string) (*types.ConcurrencyLimit, error) {
	return nil, sql.ErrNoRows
}

func (b *backend) GetImageCredentials(ctx context.Context, workspace *types.Workspace, imageId string) (string, error) {
	return "", nil
}

func (b *backend) GetLatestCheckpointByStubId(ctx context.Context, stubExternalId string, compatibilityKey ...string) (*types.Checkpoint, error) {
	return nil, nil
}
