package output

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"time"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/clients"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
	"golang.org/x/sys/unix"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	outputRoutePrefix string = "/output"
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
	rdb         *common.RedisClient
	backendRepo repository.BackendRepository
}

func NewOutputRedisService(config types.AppConfig, redisClient *common.RedisClient, backendRepo repository.BackendRepository, routeGroup *echo.Group) (OutputService, error) {
	outputService := &OutputRedisService{
		config:      config,
		rdb:         redisClient,
		backendRepo: backendRepo,
	}

	registerOutputRoutes(routeGroup.Group(outputRoutePrefix), outputService)

	return outputService, nil
}

type OutputSaveContent struct {
	Filename string
	TaskID   string
	Content  []byte
}

func (o *OutputRedisService) OutputSaveStream(stream pb.OutputService_OutputSaveStreamServer) error {
	ctx := stream.Context()
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	ch := make(chan OutputSaveContent)

	go func() {
		defer close(ch)

		for {
			in, err := stream.Recv()
			if err != nil {
				return
			}

			ch <- OutputSaveContent{
				Filename: in.Filename,
				TaskID:   in.TaskId,
				Content:  in.Content,
			}
		}
	}()

	id, err := o.writeToFile(ctx, ch, authInfo.Workspace.Name)
	if err != nil {
		return stream.SendAndClose(&pb.OutputSaveResponse{})
	}

	return stream.SendAndClose(&pb.OutputSaveResponse{
		Ok: true,
		Id: id,
	})
}

func (o *OutputRedisService) writeToFile(ctx context.Context, contentCh <-chan OutputSaveContent, workspaceName string) (string, error) {
	var file *os.File
	var filePath string
	outputId := uuid.New().String()

	for content := range contentCh {
		if file == nil {
			task, err := o.backendRepo.GetTaskWithRelated(ctx, content.TaskID)
			if err != nil {
				return "", err
			}

			dirPath := path.Join(types.DefaultOutputsPath, workspaceName, task.Stub.ExternalId, task.ExternalId, outputId)
			filePath = path.Join(dirPath, filepath.Base(content.Filename))

			if err = os.MkdirAll(dirPath, 0755); err != nil {
				return "", err
			}

			file, err = os.Create(filePath)
			if err != nil {
				os.RemoveAll(dirPath)
				return "", err
			}
			defer file.Close()
		}

		if _, err := file.Write(content.Content); err != nil {
			os.RemoveAll(filePath)
			return "", err
		}
	}

	return outputId, file.Sync()
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

func (o *OutputRedisService) statOutput(ctx context.Context, authInfo *auth.AuthInfo, taskId, outputId, filename string) (*Stat, error) {
	workspaceName := authInfo.Workspace.Name

	task, err := o.backendRepo.GetTaskWithRelated(ctx, taskId)
	if err != nil {
		return nil, err
	}

	fullPath := path.Join(types.DefaultOutputsPath, fmt.Sprint(workspaceName), task.Stub.ExternalId, task.ExternalId, outputId, filepath.Base(filename))

	if authInfo.Workspace.StorageAvailable() {
		fullPath = path.Join(types.DefaultOutputsPrefix, task.Stub.ExternalId, task.ExternalId, outputId, filepath.Base(filename))

		storageClient, err := clients.NewWorkspaceStorageClient(ctx, authInfo.Workspace.Name, authInfo.Workspace.Storage)
		if err != nil {
			return nil, err
		}

		exists, object, err := storageClient.Head(ctx, fullPath)
		if err != nil {
			return nil, err
		}

		if exists {
			size := int64(0)
			if object.ContentLength != nil {
				size = *object.ContentLength
			}

			return &Stat{
				Mode:  "0644",
				Size:  size,
				Atime: *object.LastModified,
				Mtime: *object.LastModified,
			}, nil
		} else {
			return nil, errors.New("output does not exist")
		}
	}

	var stat unix.Stat_t
	err = unix.Stat(fullPath, &stat)
	if err != nil {
		return nil, err
	}

	return &Stat{
		Mode:  os.FileMode(stat.Mode).String(),
		Size:  stat.Size,
		Atime: time.Unix(stat.Atim.Sec, stat.Atim.Nsec),
		Mtime: time.Unix(stat.Mtim.Sec, stat.Mtim.Nsec),
	}, nil
}

func (o *OutputRedisService) setPublicURL(ctx context.Context, authInfo *auth.AuthInfo, taskId, outputId, filename string, expires uint32) (string, error) {
	return SetPublicURL(ctx, o.config, o.backendRepo, o.rdb, authInfo, taskId, outputId, filename, expires)
}

func (o *OutputRedisService) getPublicURL(id string) (string, error) {
	return o.rdb.Get(context.TODO(), Keys.outputPublicURL(id)).Result()
}

func SetPublicURL(ctx context.Context, config types.AppConfig, backendRepo repository.BackendRepository, redisClient *common.RedisClient, authInfo *auth.AuthInfo, taskId, outputId, filename string, expires uint32) (string, error) {
	task, err := backendRepo.GetTaskWithRelated(ctx, taskId)
	if err != nil {
		return "", err
	}

	fullPath := GetTaskOutputPath(authInfo.Workspace.Name, task, outputId, filename)

	if authInfo.Workspace.StorageAvailable() {
		storageClient, err := clients.NewWorkspaceStorageClient(ctx, authInfo.Workspace.Name, authInfo.Workspace.Storage)
		if err != nil {
			return "", err
		}

		fullPath = path.Join(types.DefaultOutputsPrefix, task.Stub.ExternalId, task.ExternalId, outputId, filepath.Base(filename))
		presignedURL, err := storageClient.GeneratePresignedGetURL(ctx, fullPath, int64(expires))
		if err != nil {
			return "", err
		}

		return presignedURL, nil
	}

	if err = redisClient.Set(ctx, Keys.outputPublicURL(outputId), fullPath, time.Duration(expires)*time.Second).Err(); err != nil {
		return "", err
	}

	return fmt.Sprintf("%v/output/id/%v", config.GatewayService.HTTP.GetExternalURL(), outputId), nil
}

func GetTaskOutputRootPath(workspaceName string, task *types.TaskWithRelated) string {
	return filepath.Join(types.DefaultOutputsPath, workspaceName, task.Stub.ExternalId, task.ExternalId)
}

func GetTaskOutputPath(workspaceName string, task *types.TaskWithRelated, outputId string, filename string) string {
	return filepath.Join(types.DefaultOutputsPath, workspaceName, task.Stub.ExternalId, task.ExternalId, outputId, filepath.Base(filename))
}

func GetTaskOutputFiles(workspaceName string, task *types.TaskWithRelated) map[string]string {
	outputPath := GetTaskOutputRootPath(workspaceName, task)

	filePaths := map[string]string{}
	filepath.WalkDir(outputPath, func(path string, d os.DirEntry, err error) error {
		if err != nil && os.IsNotExist(err) {
			return nil
		}
		if !d.IsDir() {
			outputId := filepath.Base(filepath.Dir(path))
			fileName := filepath.Base(path)
			filePaths[outputId] = fileName
		}
		return nil
	})

	return filePaths
}

// Redis keys
var (
	Keys                   = &keys{}
	outputPublicURL string = "output:%s"
)

type keys struct{}

func (k *keys) outputPublicURL(outputId string) string {
	return fmt.Sprintf(outputPublicURL, outputId)
}
