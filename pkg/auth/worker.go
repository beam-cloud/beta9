package auth

import (
	"context"

	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	ErrWorkerTokenRequired  = status.Error(codes.PermissionDenied, "worker token is required")
	ErrContainerNotAssigned = status.Error(codes.PermissionDenied, "container is not assigned to this worker")
)

// Worker is a caller holding a worker token. Cluster workers are trusted
// infrastructure that serves every workspace. Private and provider workers run
// on hardware the platform doesn't control, so they act only in the workspace
// their token belongs to.
type Worker struct {
	ID        string
	Trusted   bool
	Token     *types.Token
	Workspace *types.Workspace
}

// WorkerFromContext returns the caller when it holds a worker token bound to a
// worker.
func WorkerFromContext(ctx context.Context) (*Worker, error) {
	authInfo, _ := AuthInfoFromContext(ctx)
	return WorkerFromAuthInfo(authInfo)
}

func WorkerFromAuthInfo(authInfo *AuthInfo) (*Worker, error) {
	if authInfo == nil || authInfo.Token == nil || !types.IsWorkerTokenType(authInfo.Token.TokenType) || authInfo.Token.WorkerID == "" {
		return nil, ErrWorkerTokenRequired
	}
	worker := &Worker{
		ID:        authInfo.Token.WorkerID,
		Trusted:   authInfo.Token.TokenType == types.TokenTypeWorker,
		Token:     authInfo.Token,
		Workspace: authInfo.Workspace,
	}
	if !worker.Trusted && (worker.Workspace == nil || worker.Workspace.ExternalId == "") {
		return nil, status.Error(codes.PermissionDenied, "worker workspace is required")
	}
	return worker, nil
}

// InWorkspace reports whether the worker may act in the workspace with this
// external ID.
func (w *Worker) InWorkspace(externalID string) bool {
	return externalID != "" && (w.Trusted || externalID == w.Workspace.ExternalId)
}

// InWorkspaceID is InWorkspace for an internal workspace ID.
func (w *Worker) InWorkspaceID(id uint) bool {
	return w.Trusted || (id != 0 && id == w.Workspace.Id)
}

// Owns reports whether the scheduler assigned the container to this worker. An
// untrusted worker's containers must also be in its workspace.
func (w *Worker) Owns(state *types.ContainerState) bool {
	return state != nil && state.WorkerId != "" && state.WorkerId == w.ID && (w.Trusted || state.WorkspaceId == w.Workspace.ExternalId)
}

// Container returns the state of a container assigned to this worker. The
// error for a missing state is returned as is.
func (w *Worker) Container(repo repository.ContainerRepository, containerID string) (*types.ContainerState, error) {
	if containerID == "" {
		return nil, ErrContainerNotAssigned
	}
	state, err := repo.GetContainerState(containerID)
	if err != nil {
		return nil, err
	}
	if !w.Owns(state) {
		return nil, ErrContainerNotAssigned
	}
	return state, nil
}

// RunsStub reports whether the worker has an active container of the stub.
func (w *Worker) RunsStub(repo repository.ContainerRepository, stubID string) bool {
	return stubID != "" && len(w.Containers(repo, stubID)) > 0
}

// Containers returns the worker's active containers, only those of stubID
// when it is set.
func (w *Worker) Containers(repo repository.ContainerRepository, stubID string) []types.ContainerState {
	if repo == nil {
		return nil
	}
	containers, err := repo.GetActiveContainersByWorkerId(w.ID)
	if err != nil {
		return nil
	}
	var owned []types.ContainerState
	for _, container := range containers {
		if w.Owns(&container) && (stubID == "" || container.StubId == stubID) {
			owned = append(owned, container)
		}
	}
	return owned
}
