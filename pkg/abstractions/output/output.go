package output

import (
	"context"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"time"

	"github.com/beam-cloud/beta9/pkg/auth"
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
	OutputSave(ctx context.Context, in *pb.OutputSaveRequest) (*pb.OutputSaveResponse, error)
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

func (o *OutputRedisService) OutputSave(ctx context.Context, in *pb.OutputSaveRequest) (*pb.OutputSaveResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	tasks, err := o.backendRepo.ListTasksWithRelated(ctx, types.TaskFilter{TaskId: in.TaskId, WorkspaceID: authInfo.Workspace.Id})
	if err != nil || len(tasks) != 1 {
		return &pb.OutputSaveResponse{
			Ok:     false,
			ErrMsg: "Unable to find task",
		}, nil
	}

	task := tasks[0]
	outputId := uuid.New().String()
	filename := filepath.Base(in.Filename)
	fullPath := path.Join(types.DefaultOutputsPath, fmt.Sprint(authInfo.Workspace.Name), task.Stub.ExternalId, task.ExternalId, outputId, filename)

	if err = os.MkdirAll(path.Dir(fullPath), 0755); err != nil {
		return &pb.OutputSaveResponse{
			Ok:     false,
			ErrMsg: "Unable to create file",
		}, nil
	}

	file, err := os.Create(fullPath)
	if err != nil {
		return &pb.OutputSaveResponse{
			Ok:     false,
			ErrMsg: "Unable to create file",
		}, nil
	}
	defer file.Close()

	if _, err := file.Write(in.Content); err != nil {
		return &pb.OutputSaveResponse{
			Ok:     false,
			ErrMsg: "Unable to write file",
		}, nil
	}

	return &pb.OutputSaveResponse{
		Ok: true,
		Id: outputId,
	}, nil
}

func (o *OutputRedisService) OutputStat(ctx context.Context, in *pb.OutputStatRequest) (*pb.OutputStatResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	tasks, err := o.backendRepo.ListTasksWithRelated(ctx, types.TaskFilter{TaskId: in.TaskId, WorkspaceID: authInfo.Workspace.Id})
	if err != nil || len(tasks) != 1 {
		return &pb.OutputStatResponse{
			Ok:     false,
			ErrMsg: "Unable to find task",
		}, nil
	}

	task := tasks[0]
	fullPath := path.Join(types.DefaultOutputsPath, fmt.Sprint(authInfo.Workspace.Name), task.Stub.ExternalId, task.ExternalId, in.Id, filepath.Base(in.Filename))

	var stat unix.Stat_t
	err = unix.Stat(fullPath, &stat)
	if err != nil {
		return &pb.OutputStatResponse{
			Ok:     false,
			ErrMsg: "Unable to stat output",
		}, nil
	}

	return &pb.OutputStatResponse{
		Ok: true,
		Stat: &pb.OutputStat{
			Mode:  os.FileMode(stat.Mode).String(),
			Size:  stat.Size,
			Atime: timestamppb.New(time.Unix(stat.Atim.Sec, stat.Atim.Nsec)),
			Mtime: timestamppb.New(time.Unix(stat.Mtim.Sec, stat.Mtim.Nsec)),
		},
	}, nil
}

func (o *OutputRedisService) OutputPublicURL(ctx context.Context, in *pb.OutputPublicURLRequest) (*pb.OutputPublicURLResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	tasks, err := o.backendRepo.ListTasksWithRelated(ctx, types.TaskFilter{TaskId: in.TaskId, WorkspaceID: authInfo.Workspace.Id})
	if err != nil || len(tasks) != 1 {
		return &pb.OutputPublicURLResponse{
			Ok:     false,
			ErrMsg: "Unable to find task",
		}, nil
	}

	task := tasks[0]
	filename := filepath.Base(in.Filename)
	fullPath := path.Join(types.DefaultOutputsPath, fmt.Sprint(authInfo.Workspace.Name), task.Stub.ExternalId, task.ExternalId, in.Id, filename)

	err = o.rdb.Set(ctx, Keys.PublicURL(in.Id), fullPath, time.Duration(in.Expires)*time.Second).Err()
	if err != nil {
		return &pb.OutputPublicURLResponse{
			Ok:     false,
			ErrMsg: "Unable to generate URL",
		}, nil
	}

	return &pb.OutputPublicURLResponse{
		Ok:        true,
		PublicUrl: fmt.Sprintf("%v/output/id/%v", o.config.GatewayService.ExternalURL, in.Id),
	}, nil
}

func (o *OutputRedisService) getURL(id string) (string, error) {
	return o.rdb.Get(context.TODO(), Keys.PublicURL(id)).Result()
}

// Redis keys
var (
	Keys             = &keys{}
	publicURL string = "output:%s"
)

type keys struct{}

func (k *keys) PublicURL(outputId string) string {
	return fmt.Sprintf(publicURL, outputId)
}
