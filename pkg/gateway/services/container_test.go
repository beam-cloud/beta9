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
