package gatewayservices

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func TestContainerTimestamp(t *testing.T) {
	if timestamp := containerTimestamp(0); timestamp != nil {
		t.Fatalf("expected zero start time to be omitted, got %s", timestamp)
	}

	const unixSeconds = int64(1_700_000_000)
	timestamp := containerTimestamp(unixSeconds)
	if timestamp == nil || !timestamp.AsTime().Equal(time.Unix(unixSeconds, 0)) {
		t.Fatalf("unexpected timestamp: %v", timestamp)
	}
}

type attachContainerRepository struct {
	repository.ContainerRepository
	state              *types.ContainerState
	err                error
	requestedContainer string
}

type snapshotReplayBackendRepository struct {
	repository.BackendRepository
	snapshot    *types.StateSnapshot
	lookupCount int
}

type stateSnapshotReferenceBackendRepository struct {
	repository.BackendRepository
	workspaceID uint
	snapshotID  string
	kind        string
	referenceID string
	released    bool
	err         error
}

func (r *stateSnapshotReferenceBackendRepository) RetainStateSnapshotReference(_ context.Context, workspaceID uint,
	snapshotID, kind, referenceID string,
) (*types.StateSnapshotReference, error) {
	r.workspaceID, r.snapshotID, r.kind, r.referenceID = workspaceID, snapshotID, kind, referenceID
	if r.err != nil {
		return nil, r.err
	}
	return &types.StateSnapshotReference{SnapshotExternalId: snapshotID, Kind: kind, ReferenceId: referenceID}, nil
}

func (r *stateSnapshotReferenceBackendRepository) ReleaseStateSnapshotReference(_ context.Context, workspaceID uint,
	snapshotID, kind, referenceID string,
) (*types.StateSnapshotReference, error) {
	r.workspaceID, r.snapshotID, r.kind, r.referenceID, r.released = workspaceID, snapshotID, kind, referenceID, true
	if r.err != nil {
		return nil, r.err
	}
	return &types.StateSnapshotReference{SnapshotExternalId: snapshotID, Kind: kind, ReferenceId: referenceID,
		Released: true}, nil
}

func (r *snapshotReplayBackendRepository) GetStateSnapshotByOperationForWorkspace(context.Context, uint, string, string) (*types.StateSnapshot, error) {
	r.lookupCount++
	return r.snapshot, nil
}

func TestSnapshotContainerStateRejectsPublicPublishBeforeCapture(t *testing.T) {
	backend := &snapshotReplayBackendRepository{}
	gws := &GatewayService{backendRepo: backend}
	ctx := auth.ContextWithAuthInfo(context.Background(), &auth.AuthInfo{
		Workspace: &types.Workspace{Id: 7, ExternalId: "workspace"},
		Token:     &types.Token{TokenType: types.TokenTypeWorkspace},
	})

	response, err := gws.SnapshotContainerState(ctx, &pb.GatewaySnapshotContainerStateRequest{
		ContainerId: "container", OperationId: "operation", Mode: "live", Publish: true,
	})
	require.NoError(t, err)
	require.False(t, response.Ok)
	require.Equal(t, "public whole-root state publishing is disabled", response.ErrorMsg)
	require.Zero(t, backend.lookupCount, "publish must be rejected before repository lookup or worker capture")
}

func TestStateSnapshotReferenceAPIIsWorkspaceScopedAndAdditive(t *testing.T) {
	const (
		snapshotID  = "ae64dbd5-f687-4da9-b766-7b49d79b4db1"
		referenceID = "machine:machine-17:ae64dbd5-f687-4da9-b766-7b49d79b4db1"
	)
	backend := &stateSnapshotReferenceBackendRepository{}
	gws := &GatewayService{backendRepo: backend}
	ctx := auth.ContextWithAuthInfo(context.Background(), &auth.AuthInfo{
		Workspace: &types.Workspace{Id: 17, ExternalId: "workspace"},
		Token:     &types.Token{TokenType: types.TokenTypeWorkspace},
	})

	retained, err := gws.RetainStateSnapshotReference(ctx, &pb.StateSnapshotReferenceRequest{
		StateSnapshotId: snapshotID, Kind: "machine", ReferenceId: referenceID,
	})
	require.NoError(t, err)
	require.True(t, retained.Ok)
	require.Equal(t, "active", retained.Status)
	require.EqualValues(t, 17, backend.workspaceID)
	require.Equal(t, snapshotID, backend.snapshotID)
	require.Equal(t, referenceID, backend.referenceID)

	released, err := gws.ReleaseStateSnapshotReference(ctx, &pb.StateSnapshotReferenceRequest{
		StateSnapshotId: snapshotID, Kind: "machine", ReferenceId: referenceID,
	})
	require.NoError(t, err)
	require.True(t, released.Ok)
	require.Equal(t, "released", released.Status)
	require.True(t, backend.released)
}

func TestStateSnapshotReferenceAPIRejectsMissingWorkspaceAuthorization(t *testing.T) {
	backend := &stateSnapshotReferenceBackendRepository{}
	gws := &GatewayService{backendRepo: backend}
	response, err := gws.RetainStateSnapshotReference(context.Background(), &pb.StateSnapshotReferenceRequest{
		StateSnapshotId: "ae64dbd5-f687-4da9-b766-7b49d79b4db1", Kind: "machine", ReferenceId: "machine:m:s",
	})
	require.NoError(t, err)
	require.False(t, response.Ok)
	require.Equal(t, "Unauthorized Access", response.ErrorMsg)
	require.Zero(t, backend.workspaceID)
}

func TestSnapshotContainerStateReplaysTerminalRepositoryResultBeforeWorkerRouting(t *testing.T) {
	generation := types.StateGeneration{
		VolumeId: "21d4182a-4930-47b4-a987-e50c4a80156f", GenerationId: "7aee3365-2963-4a6d-b9fb-2c934924880d",
		CloneParentGenerationId: "acee3e88-20d7-4bbc-92cc-4b839ad6bc55",
		Name:                    "root", MountPath: "/", Root: true, Generation: 1,
	}
	gws := &GatewayService{backendRepo: &snapshotReplayBackendRepository{snapshot: &types.StateSnapshot{
		ExternalId: "e4f41f9a-524c-4906-8ea3-b36b32f45c27", Mode: "terminal", IncludeMemory: true,
		Visible: true, Status: types.StateSnapshotStatusAvailable, RestoreMode: "memory",
		CheckpointId: "checkpoint", Generations: []types.StateGeneration{generation},
	}}}
	ctx := auth.ContextWithAuthInfo(context.Background(), &auth.AuthInfo{
		Workspace: &types.Workspace{Id: 7, ExternalId: "workspace"},
		Token:     &types.Token{TokenType: types.TokenTypeWorkspace},
	})
	response, err := gws.SnapshotContainerState(ctx, &pb.GatewaySnapshotContainerStateRequest{
		ContainerId: "gone-container", OperationId: "terminal-op", Mode: "terminal",
		IncludeMemory: true, Visible: true,
	})
	require.NoError(t, err)
	require.True(t, response.Ok)
	require.Equal(t, "e4f41f9a-524c-4906-8ea3-b36b32f45c27", response.StateSnapshotId)
	require.True(t, response.HasMemory)
	require.Equal(t, generation.CloneParentGenerationId, response.Generations[0].CloneParentGenerationId)
}

func (r *attachContainerRepository) GetContainerState(containerId string) (*types.ContainerState, error) {
	r.requestedContainer = containerId
	return r.state, r.err
}

type attachContainerStream struct {
	grpc.ServerStream
	ctx      context.Context
	messages []*pb.ContainerStreamMessage
	sent     []*pb.AttachToContainerResponse
}

func (s *attachContainerStream) Context() context.Context {
	return s.ctx
}

func (s *attachContainerStream) Recv() (*pb.ContainerStreamMessage, error) {
	if len(s.messages) == 0 {
		return nil, io.EOF
	}

	message := s.messages[0]
	s.messages = s.messages[1:]
	return message, nil
}

func (s *attachContainerStream) Send(response *pb.AttachToContainerResponse) error {
	s.sent = append(s.sent, response)
	return nil
}

func TestAttachToContainerRejectsUnauthorizedContainer(t *testing.T) {
	tests := []struct {
		name      string
		tokenType string
		state     *types.ContainerState
		repoErr   error
	}{
		{
			name:      "foreign workspace",
			tokenType: types.TokenTypeWorkspace,
			state: &types.ContainerState{
				ContainerId: "victim-container",
				WorkspaceId: "victim-workspace",
			},
		},
		{
			name:      "restricted token",
			tokenType: types.TokenTypeWorkspaceRestricted,
			state: &types.ContainerState{
				ContainerId: "attacker-container",
				WorkspaceId: "attacker-workspace",
			},
		},
		{
			name:      "missing container state",
			tokenType: types.TokenTypeWorkspace,
		},
		{
			name:      "container lookup error",
			tokenType: types.TokenTypeWorkspace,
			repoErr:   errors.New("lookup failed"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			containerRepo := &attachContainerRepository{
				state: tt.state,
				err:   tt.repoErr,
			}
			ctx := auth.ContextWithAuthInfo(context.Background(), &auth.AuthInfo{
				Workspace: &types.Workspace{ExternalId: "attacker-workspace"},
				Token:     &types.Token{TokenType: tt.tokenType},
			})
			stream := &attachContainerStream{
				ctx: ctx,
				messages: []*pb.ContainerStreamMessage{{
					Payload: &pb.ContainerStreamMessage_AttachRequest{
						AttachRequest: &pb.AttachToContainerRequest{ContainerId: "victim-container"},
					},
				}},
			}
			gws := &GatewayService{containerRepo: containerRepo}

			err := gws.AttachToContainer(stream)

			require.NoError(t, err)
			require.Len(t, stream.sent, 1)
			require.Equal(t, &pb.AttachToContainerResponse{
				Done:     true,
				ExitCode: 1,
				Output:   "Container not found",
			}, stream.sent[0])
			if tt.tokenType == types.TokenTypeWorkspaceRestricted {
				require.Empty(t, containerRepo.requestedContainer)
			} else {
				require.Equal(t, "victim-container", containerRepo.requestedContainer)
			}
		})
	}
}

func TestBindSyncRequestToContainer(t *testing.T) {
	request := &pb.SyncContainerWorkspaceRequest{
		ContainerId: "client-controlled-container",
		Path:        "file.txt",
		Data:        []byte("contents"),
	}

	bound := bindSyncRequestToContainer(request, "authorized-container")

	require.NotSame(t, request, bound)
	require.Equal(t, "authorized-container", bound.ContainerId)
	require.Equal(t, "client-controlled-container", request.ContainerId)
	require.Equal(t, request.Path, bound.Path)
	require.Equal(t, request.Data, bound.Data)
	require.Nil(t, bindSyncRequestToContainer(nil, "authorized-container"))
}
