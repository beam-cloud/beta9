package worker

import (
	"context"
	"strings"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func (s *ContainerRuntimeServer) replaySandboxRequest(ctx context.Context, request interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	ids := md.Get(common.RequestIDHeader)
	method := strings.TrimPrefix(info.FullMethod, "/container.ContainerService/")
	switch method {
	case "ContainerSandboxExec", "ContainerSandboxStdout", "ContainerSandboxStderr", "ContainerSandboxKill",
		"ContainerSandboxUploadFile", "ContainerSandboxDeleteFile", "ContainerSandboxCreateDirectory", "ContainerSandboxDeleteDirectory",
		"ContainerSandboxExposePort", "ContainerSandboxUpdateNetworkPermissions", "ContainerSandboxReplaceInFiles":
	default:
		return handler(ctx, request)
	}
	if len(ids) == 0 {
		return handler(ctx, request)
	}
	if _, err := uuid.Parse(ids[0]); err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid request ID")
	}
	containerRequest, ok := request.(interface{ GetContainerId() string })
	if !ok {
		return handler(ctx, request)
	}
	instance, exists := s.containerInstances.Get(containerRequest.GetContainerId())
	if !exists {
		return handler(ctx, request)
	}
	return instance.requests.Do(ctx, ids[0], info.FullMethod, request.(proto.Message), md.Get(common.RequestAckHeader), func(operationCtx context.Context) (interface{}, error) {
		return handler(operationCtx, request)
	})
}
