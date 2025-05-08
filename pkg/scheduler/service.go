package scheduler

import (
	"context"

	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
)

type SchedulerService struct {
	pb.UnimplementedSchedulerServer
	Scheduler *Scheduler
}

func NewSchedulerService(scheduler *Scheduler) (*SchedulerService, error) {
	go scheduler.StartProcessingRequests() // Start processing ContainerRequests

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
		ContainerId: in.ContainerId,
		EntryPoint:  in.EntryPoint,
		Env:         in.Env,
		Cpu:         cpuRequest,
		Memory:      memoryRequest,
		Gpu:         in.Gpu,
		ImageId:     in.ImageId,
		RetryCount:  0,
		ClipVersion: in.ClipVersion,
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
