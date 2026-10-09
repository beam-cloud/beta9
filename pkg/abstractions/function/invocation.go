package function

import (
	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Reattach to an accepted invocation without scheduling another container.
func (fs *ContainerFunctionService) resumeInvocation(in *pb.FunctionInvokeRequest, stream pb.FunctionService_FunctionInvokeServer) error {
	authInfo, _ := auth.AuthInfoFromContext(stream.Context())
	current, err := fs.backendRepo.GetTaskWithRelated(stream.Context(), in.TaskId)
	if err != nil {
		return err
	}
	if current == nil {
		return status.Error(codes.NotFound, "invocation not found")
	}
	if current.Workspace.ExternalId != authInfo.Workspace.ExternalId || current.Stub.ExternalId != in.StubId {
		return status.Error(codes.PermissionDenied, "invocation does not belong to this function")
	}
	if current.Status.IsCompleted() {
		result, err := fs.rdb.Get(stream.Context(), Keys.FunctionResult(authInfo.Workspace.Name, in.TaskId)).Bytes()
		if err != nil && current.Status == types.TaskStatusComplete {
			return status.Error(codes.Unavailable, "function result is not yet available")
		}
		exitCode, err := fs.containerRepo.GetContainerExitCode(current.ContainerId)
		if err != nil {
			exitCode = 1
			if current.Status == types.TaskStatusComplete {
				exitCode = 0
			}
		}
		return stream.Send(&pb.FunctionInvokeResponse{TaskId: in.TaskId, Done: true, ExitCode: int32(exitCode), Result: result})
	}
	task := &FunctionTask{fs: fs, containerId: current.ContainerId, msg: &types.TaskMessage{
		TaskId: in.TaskId, StubId: in.StubId, WorkspaceName: authInfo.Workspace.Name,
	}}
	return fs.streamAt(stream.Context(), stream, authInfo, task, &in.OutputOffset)
}
