package scheduler

import (
	"context"

	"github.com/beam-cloud/beta9/internal/common"
	"github.com/beam-cloud/beta9/internal/types"
	pb "github.com/beam-cloud/beta9/proto"
)

type SchedulerService struct {
	pb.UnimplementedSchedulerServer
	Scheduler *Scheduler
}

func NewSchedulerService(config types.AppConfig, redisClient *common.RedisClient) (*SchedulerService, error) {
	scheduler, err := NewScheduler(config, redisClient)
	if err != nil {
		return nil, err
	}

	go scheduler.processRequests() // Start processing ContainerRequests

	return &SchedulerService{
		Scheduler: scheduler,
	}, nil
}

// Get Scheduler version
func (wbs *SchedulerService) GetVersion(ctx context.Context, in *pb.VersionRequest) (*pb.VersionResponse, error) {
	return &pb.VersionResponse{Version: SchedulerConfig.Version}, nil
}

// Run a container
func (wbs *SchedulerService) RunContainer(ctx context.Context, in *pb.RunContainerRequest) (*pb.RunContainerResponse, error) {
	cpuRequest, err := ParseCPU(in.Cpu)
	if err != nil {
		return &pb.RunContainerResponse{
			Success: false,
			Error:   err.Error(),
		}, nil
	}

	memoryRequest, err := ParseMemory(in.Memory)
	if err != nil {
		return &pb.RunContainerResponse{
			Success: false,
			Error:   err.Error(),
		}, nil
	}

	err = wbs.Scheduler.Run(&types.ContainerRequest{
		ContainerId:     in.ContainerId,
		EntryPoint:      in.EntryPoint,
		Env:             in.Env,
		Cpu:             cpuRequest,
		Memory:          memoryRequest,
		Gpu:             in.Gpu,
		ImageId:         in.ImageId,
		ScheduleTimeout: float64(in.ScheduleTimeout),
	})

	if err != nil {
		return &pb.RunContainerResponse{
			Success: false,
			Error:   err.Error(),
		}, nil
	}

	return &pb.RunContainerResponse{
		Success: true,
		Error:   "",
	}, nil
}

// Stop a container
func (wbs *SchedulerService) StopContainer(ctx context.Context, in *pb.StopContainerRequest) (*pb.StopContainerResponse, error) {
	err := wbs.Scheduler.Stop(in.ContainerId)

	if err != nil {
		return &pb.StopContainerResponse{
			Success: false,
			Error:   err.Error(),
		}, nil
	}

	return &pb.StopContainerResponse{
		Success: true,
		Error:   "",
	}, nil
}
