package output

import (
	"context"
	"errors"
	"io"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/clients"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type OutputService interface {
	pb.OutputServiceServer
	OutputSaveStream(stream pb.OutputService_OutputSaveStreamServer) error
	OutputStat(ctx context.Context, in *pb.OutputStatRequest) (*pb.OutputStatResponse, error)
	OutputPublicURL(ctx context.Context, in *pb.OutputPublicURLRequest) (*pb.OutputPublicURLResponse, error)
}

type OutputRedisService struct {
	pb.UnimplementedOutputServiceServer

	config      types.AppConfig
	backendRepo repository.BackendRepository
}

func NewOutputRedisService(config types.AppConfig, backendRepo repository.BackendRepository) (OutputService, error) {
	return &OutputRedisService{config: config, backendRepo: backendRepo}, nil
}

func (o *OutputRedisService) OutputSaveStream(stream pb.OutputService_OutputSaveStreamServer) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	authInfo, _ := auth.AuthInfoFromContext(stream.Context())
	reader := common.NewChunkReader(first.Content, func() ([]byte, error) {
		chunk, err := stream.Recv()
		if err != nil {
			return nil, err
		}
		if chunk.TaskId != first.TaskId || chunk.Filename != first.Filename {
			return nil, errors.New("output changed during upload")
		}
		return chunk.Content, nil
	})
	id, err := o.saveOutput(stream.Context(), authInfo.Workspace, first.TaskId, first.Filename, reader)
	if err != nil {
		return stream.SendAndClose(&pb.OutputSaveResponse{ErrMsg: err.Error()})
	}
	return stream.SendAndClose(&pb.OutputSaveResponse{Ok: true, Id: id})
}

func (o *OutputRedisService) saveOutput(ctx context.Context, workspace *types.Workspace, taskID, filename string, reader io.Reader) (string, error) {
	if name := filepath.Base(filename); name == "." || name == ".." || name == "/" {
		return "", errors.New("output filename is required")
	}
	task, err := getOutputTask(ctx, o.backendRepo, taskID)
	if err != nil {
		return "", err
	}
	client, err := clients.NewWorkspaceStorageClient(ctx, workspace.Name, workspace.Storage)
	if err != nil {
		return "", err
	}
	id := uuid.NewString()
	return id, client.UploadWithReader(ctx, taskOutputKey(task, id, filename), reader)
}

func (o *OutputRedisService) OutputStat(ctx context.Context, in *pb.OutputStatRequest) (*pb.OutputStatResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	stat, err := o.statOutput(ctx, authInfo, in.TaskId, in.Id, in.Filename)
	if err != nil {
		return &pb.OutputStatResponse{
			Ok:     false,
			ErrMsg: "Unable stat output",
		}, nil
	}

	return &pb.OutputStatResponse{
		Ok: true,
		Stat: &pb.OutputStat{
			Mode:  stat.Mode,
			Size:  stat.Size,
			Atime: timestamppb.New(stat.Atime),
			Mtime: timestamppb.New(stat.Mtime),
		},
	}, nil
}

func (o *OutputRedisService) OutputPublicURL(ctx context.Context, in *pb.OutputPublicURLRequest) (*pb.OutputPublicURLResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	url, err := o.setPublicURL(ctx, authInfo, in.TaskId, in.Id, in.Filename, in.Expires)
	if err != nil {
		return &pb.OutputPublicURLResponse{
			Ok:     false,
			ErrMsg: "Unable to get public URL",
		}, nil
	}

	return &pb.OutputPublicURLResponse{
		Ok:        true,
		PublicUrl: url,
	}, nil
}

// Output business logic

type Stat struct {
	Mode  string
	Size  int64
	Atime time.Time
	Mtime time.Time
}

func getOutputTask(ctx context.Context, backendRepo repository.BackendRepository, taskId string) (*types.TaskWithRelated, error) {
	task, err := backendRepo.GetTaskWithRelated(ctx, taskId)
	if err != nil {
		return nil, err
	}
	if task == nil {
		return nil, errors.New("task not found")
	}
	return task, nil
}

func (o *OutputRedisService) statOutput(ctx context.Context, authInfo *auth.AuthInfo, taskId, outputId, filename string) (*Stat, error) {
	task, err := getOutputTask(ctx, o.backendRepo, taskId)
	if err != nil {
		return nil, err
	}
	client, err := clients.NewWorkspaceStorageClient(ctx, authInfo.Workspace.Name, authInfo.Workspace.Storage)
	if err != nil {
		return nil, err
	}
	exists, object, err := client.Head(ctx, taskOutputKey(task, outputId, filename))
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, errors.New("output does not exist")
	}
	return &Stat{Mode: "0644", Size: aws.ToInt64(object.ContentLength), Atime: aws.ToTime(object.LastModified), Mtime: aws.ToTime(object.LastModified)}, nil
}

func (o *OutputRedisService) setPublicURL(ctx context.Context, authInfo *auth.AuthInfo, taskId, outputId, filename string, expires uint32) (string, error) {
	return SetPublicURL(ctx, o.config, o.backendRepo, authInfo, taskId, outputId, filename, expires)
}

func SetPublicURL(ctx context.Context, config types.AppConfig, backendRepo repository.BackendRepository, authInfo *auth.AuthInfo, taskId, outputId, filename string, expires uint32) (string, error) {
	task, err := getOutputTask(ctx, backendRepo, taskId)
	if err != nil {
		return "", err
	}
	client, err := clients.NewWorkspaceStorageClientWithDefaultPresignEndpoint(ctx, authInfo.Workspace.Name, authInfo.Workspace.Storage, config.Storage.WorkspaceStorage)
	if err != nil {
		return "", err
	}
	return client.GeneratePresignedGetURL(ctx, taskOutputKey(task, outputId, filename), int64(expires))
}

func taskOutputKey(task *types.TaskWithRelated, outputID, filename string) string {
	return path.Join(types.DefaultOutputsPrefix, task.Stub.ExternalId, task.ExternalId, outputID, filepath.Base(filename))
}

func GetTaskOutputFiles(ctx context.Context, authInfo *auth.AuthInfo, task *types.TaskWithRelated) (map[string]string, error) {
	client, err := clients.NewWorkspaceStorageClient(ctx, authInfo.Workspace.Name, authInfo.Workspace.Storage)
	if err != nil {
		return nil, err
	}
	prefix := path.Join(types.DefaultOutputsPrefix, task.Stub.ExternalId, task.ExternalId) + "/"
	objects, discoveryErr := client.ListWithPrefixAllRegions(ctx, prefix)
	files := map[string]string{}
	for _, object := range objects {
		if object.Key == nil || !strings.HasPrefix(*object.Key, prefix) || strings.HasSuffix(*object.Key, "/") {
			continue
		}
		parts := strings.Split(strings.TrimPrefix(*object.Key, prefix), "/")
		if len(parts) == 2 && parts[0] != "" && parts[1] != "" {
			files[parts[0]] = parts[1]
		}
	}
	return files, discoveryErr
}
