package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"log"

	"github.com/beam-cloud/beam/internal/common"
	"github.com/beam-cloud/beam/internal/types"
	pb "github.com/beam-cloud/beam/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type SchedulerService struct {
	pb.UnimplementedSchedulerServer
	Scheduler *Scheduler
}

func NewSchedulerService() (*SchedulerService, error) {
	Scheduler, err := NewScheduler()
	if err != nil {
		return nil, err
	}

	go Scheduler.processRequests() // Start processing ContainerRequests

	return &SchedulerService{
		Scheduler: Scheduler,
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

// Sets up worker pools reported by the agent/client.
func (wbs *SchedulerService) SubscribeWorkerEvents(
	req *pb.SubscribeWorkerEventRequest,
	stream pb.Scheduler_SubscribeWorkerEventsServer,
) error {
	if req.AgentInfo == nil {
		return status.Error(codes.InvalidArgument, "invalid agent token")
	}

	// Get and update agent
	agent, err := wbs.Scheduler.beamRepo.GetAgentByToken(req.AgentInfo.Token)
	if err != nil {
		return status.Error(codes.Internal, "invalid agent token")
	}

	agent.IsOnline = true
	agent.CloudProvider = req.AgentInfo.CloudProvider
	agent.Version = req.AgentInfo.Version
	workerPools := make(map[string]string, len(req.WorkerPools))
	for _, pool := range req.WorkerPools {
		workerPools[pool.Name] = ""
	}
	agent.SetPools(workerPools)

	agent, err = wbs.Scheduler.beamRepo.UpdateAgent(agent)
	if err != nil {
		log.Printf("Unable to update agent in database: %v\n", err)
	}

	// Register worker pools
	for _, pool := range req.WorkerPools {
		// Find an existing pool. If found, don't register/overwrite it and prevent it from being removed
		// when the client disconects later on.
		if _, ok := wbs.Scheduler.workerPoolManager.GetPool(pool.Name); ok {
			delete(workerPools, pool.Name)
			continue
		}

		// No pool registered, lets add one
		controller := NewRemoteWorkerPoolController(pool.Name, &RemoteWorkerPoolControllerConfig{
			agent:             agent,
			workerEventStream: stream,
			workerRepo:        wbs.Scheduler.workerRepo,
		})

		// Create a WorkerPool and register it with the WorkerPoolManager.
		// WorkerPoolManager only needs the pool name to find a controller. So we'll create a
		// WorkerPoolResource with just the name and register it with the WorkerPoolManager.
		resource := types.NewWorkerPoolResource(pool.Name)
		wbs.Scheduler.workerPoolManager.SetPool(resource, controller)
	}

	log.Printf("Agent <%v> has connected with pools %+v\n", agent.ExternalID, workerPools)
	<-stream.Context().Done()
	log.Printf("Agent <%v> has disconnected\n", agent.ExternalID)

	// Clean up
	for poolName := range workerPools {
		wbs.Scheduler.workerPoolManager.RemovePool(poolName)
	}

	agent.IsOnline = false
	if _, err = wbs.Scheduler.beamRepo.UpdateAgent(agent); err != nil {
		log.Printf("Unable to update agent info on client disconnect: %v\n", err)
	}

	return status.Error(codes.Canceled, "client disconnected")
}

func (wbs *SchedulerService) RegisterWorker(ctx context.Context, in *pb.RegisterWorkerRequest) (*pb.RegisterWorkerResponse, error) {
	if in.AgentInfo == nil {
		return nil, errors.New("invalid agent token")
	}

	_, err := wbs.Scheduler.beamRepo.GetAgentByToken(in.AgentInfo.Token)
	if err != nil {
		return nil, errors.New("invalid agent token")
	}

	return &pb.RegisterWorkerResponse{}, wbs.Scheduler.workerRepo.AddWorker(&types.Worker{
		Id:     in.Worker.Id,
		Cpu:    in.Worker.Cpu,
		Memory: in.Worker.Memory,
		Gpu:    in.Worker.GpuType,
		PoolId: in.Worker.PoolId,
		Status: types.WorkerStatus(in.Worker.Status),
	})
}

func (wbs *SchedulerService) GetNextTask(ctx context.Context, in *pb.GetNextTaskRequest) (*pb.GetNextTaskResponse, error) {
	taskAvailable := false

	identity, authorized, err := wbs.Scheduler.beamRepo.AuthorizeServiceToServiceToken(in.S2SToken)
	if err != nil || !authorized {
		return &pb.GetNextTaskResponse{
			Task:          nil,
			TaskAvailable: false,
		}, nil
	}

	task, err := wbs.Scheduler.taskRepo.GetNextTask(in.QueueName, in.ContainerId, identity.ExternalID)
	if task != nil && err == nil {
		taskAvailable = true
	}

	return &pb.GetNextTaskResponse{
		Task:          task,
		TaskAvailable: taskAvailable,
	}, nil
}

func (wbs *SchedulerService) GetTaskStream(req *pb.GetTaskStreamRequest, stream pb.Scheduler_GetTaskStreamServer) error {
	identity, authorized, err := wbs.Scheduler.beamRepo.AuthorizeServiceToServiceToken(req.S2SToken)
	if err != nil || !authorized {
		return err
	}

	return wbs.Scheduler.taskRepo.GetTaskStream(req.QueueName, req.ContainerId, identity.ExternalID, stream)
}

func (wbs *SchedulerService) StartTask(ctx context.Context, in *pb.StartTaskRequest) (*pb.StartTaskResponse, error) {
	identity, authorized, err := wbs.Scheduler.beamRepo.AuthorizeServiceToServiceToken(in.S2SToken)
	if err != nil || !authorized {
		return nil, errors.New("invalid s2s token")
	}

	_, err = wbs.Scheduler.beamRepo.UpdateActiveTask(in.TaskId, types.BeamAppTaskStatusRunning, identity.ExternalID)
	if err != nil {
		return &pb.StartTaskResponse{
			Ok: false,
		}, nil
	}

	err = wbs.Scheduler.taskRepo.StartTask(in.TaskId, in.QueueName, in.ContainerId, identity.ExternalID)
	return &pb.StartTaskResponse{
		Ok: err == nil,
	}, nil
}

func (wbs *SchedulerService) EndTask(ctx context.Context, in *pb.EndTaskRequest) (*pb.EndTaskResponse, error) {
	identity, authorized, err := wbs.Scheduler.beamRepo.AuthorizeServiceToServiceToken(in.S2SToken)
	if err != nil || !authorized {
		return nil, errors.New("invalid s2s token")
	}

	err = wbs.Scheduler.taskRepo.EndTask(in.TaskId, in.QueueName, in.ContainerId, in.ContainerHostname, identity.ExternalID, float64(in.TaskDuration), float64(in.ScaleDownDelay))
	if err != nil {
		return &pb.EndTaskResponse{
			Ok: false,
		}, nil
	}

	_, err = wbs.Scheduler.beamRepo.UpdateActiveTask(in.TaskId, in.TaskStatus, identity.ExternalID)
	if err != nil {
		return &pb.EndTaskResponse{
			Ok: false,
		}, nil
	}

	return &pb.EndTaskResponse{
		Ok: true,
	}, nil
}

func (wbs *SchedulerService) MonitorTask(req *pb.MonitorTaskRequest, stream pb.Scheduler_MonitorTaskServer) error {
	identity, authorized, err := wbs.Scheduler.beamRepo.AuthorizeServiceToServiceToken(req.S2SToken)
	if err != nil || !authorized {
		return errors.New("invalid s2s token")
	}

	task, err := wbs.Scheduler.beamRepo.GetAppTask(req.TaskId)
	if err != nil {
		return err
	}

	taskPolicy := types.TaskPolicy{}
	err = json.Unmarshal([]byte(task.TaskPolicy), &taskPolicy)
	if err != nil {
		taskPolicy = common.DefaultTaskPolicy
	}

	timeoutCallback := func() error {
		_, err = wbs.Scheduler.beamRepo.UpdateActiveTask(
			task.TaskId,
			types.BeamAppTaskStatusTimeout,
			identity.ExternalID,
		)
		if err != nil {
			return err
		}

		return nil
	}

	return wbs.Scheduler.taskRepo.MonitorTask(
		task,
		req.QueueName,
		req.ContainerId,
		identity.ExternalID,
		int64(taskPolicy.Timeout),
		stream,
		timeoutCallback,
	)
}
