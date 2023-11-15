package scheduler

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/beam-cloud/beam/pkg/repository"
	"github.com/beam-cloud/beam/pkg/types"
	pb "github.com/beam-cloud/beam/proto"
	"github.com/google/uuid"
)

// Determines how long to wait for a new Worker to start up.
// This only affects remote workers.
const AddWorkerTimeout = 10 * time.Minute

type RemoteWorkerPoolControllerConfig struct {
	agent             *types.Agent
	workerEventStream pb.Scheduler_SubscribeWorkerEventsServer
	workerRepo        repository.WorkerRepository
}

type RemoteWorkerPoolController struct {
	name   string
	config *RemoteWorkerPoolControllerConfig
}

func NewRemoteWorkerPoolController(workerPoolName string, config *RemoteWorkerPoolControllerConfig) WorkerPoolController {
	return &RemoteWorkerPoolController{
		name:   workerPoolName,
		config: config,
	}
}

func (c *RemoteWorkerPoolController) Name() string {
	return c.name
}

func (c *RemoteWorkerPoolController) AddWorker(cpu int64, memory int64, gpuType string) (*types.Worker, error) {
	workerId := c.generateWorkerId()
	return c.addWorkerWithId(workerId, cpu, memory, gpuType)
}

func (c *RemoteWorkerPoolController) AddWorkerWithId(workerId string, cpu int64, memory int64, gpuType string) (*types.Worker, error) {
	return c.addWorkerWithId(workerId, cpu, memory, gpuType)
}

func (c *RemoteWorkerPoolController) addWorkerWithId(workerId string, cpu int64, memory int64, gpuType string) (*types.Worker, error) {
	worker := &types.Worker{
		Id:     workerId,
		Status: types.WorkerStatusPending,
		Cpu:    cpu,
		Memory: memory,
		Gpu:    gpuType,
	}

	if err := c.config.workerRepo.AddWorker(worker); err != nil {
		return nil, fmt.Errorf("failed to add worker to repo: %v", err)
	}

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), AddWorkerTimeout)
		defer cancel()

		workerEvent := &pb.SubscribeWorkerEventResponse{
			Worker: &pb.Worker{
				Id:      worker.Id,
				Cpu:     worker.Cpu,
				Memory:  worker.Memory,
				GpuType: worker.Gpu,
			},
			WorkerPool: &pb.WorkerPool{Name: c.name},
		}

		if err := c.config.workerEventStream.Send(workerEvent); err != nil {
			log.Printf("Failed sending worker event to agent: agent <%s>, worker <%s>, worker pool <%s>: %v\n", c.config.agent.ExternalID, worker.Id, c.name, err)
		}

		ticker := time.NewTicker(3 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				log.Printf("Timed out adding worker, removing worker state: agent <%s>, worker <%s>, worker pool <%s>\n", c.config.agent.ExternalID, worker.Id, c.name)
				c.config.workerRepo.RemoveWorker(worker)
				return
			case <-ticker.C:
				w, err := c.config.workerRepo.GetWorkerById(worker.Id)
				if err == nil && w.Status == types.WorkerStatusAvailable {
					log.Printf("Worker added by agent: agent <%s>, worker <%s>, worker pool <%s>\n", c.config.agent.ExternalID, worker.Id, c.name)
					return
				}
			}
		}
	}()

	return worker, nil
}

func (c *RemoteWorkerPoolController) FreeCapacity() (*WorkerPoolCapacity, error) {
	return &WorkerPoolCapacity{}, nil
}

func (c *RemoteWorkerPoolController) generateWorkerId() string {
	return uuid.New().String()[:8]
}
