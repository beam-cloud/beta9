package gatewayservices

import (
	"context"
	"errors"
	"time"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"golang.org/x/exp/slices"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (gws *GatewayService) ListWorkers(ctx context.Context, in *pb.ListWorkersRequest) (*pb.ListWorkersResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	if !auth.HasPermission(authInfo) {
		return &pb.ListWorkersResponse{
			Ok:     false,
			ErrMsg: "Unauthorized Access",
		}, nil
	}

	if _, err := isClusterAdmin(ctx); err != nil {
		return &pb.ListWorkersResponse{
			Ok:     false,
			ErrMsg: err.Error(),
		}, nil
	}

	workers, err := gws.workerRepo.GetAllWorkers()
	if err != nil {
		return &pb.ListWorkersResponse{
			Ok:     false,
			ErrMsg: err.Error(),
		}, nil
	}

	sortWorkers(workers)

	pbWorkers := make([]*pb.Worker, len(workers))
	for i, w := range workers {
		pbWorkers[i] = &pb.Worker{
			Id:            w.Id,
			Status:        string(w.Status),
			Gpu:           w.Gpu,
			PoolName:      w.PoolName,
			MachineId:     w.MachineId,
			Priority:      w.Priority,
			TotalCpu:      w.TotalCpu,
			TotalMemory:   w.TotalMemory,
			TotalGpuCount: w.TotalGpuCount,
			FreeCpu:       w.FreeCpu,
			FreeMemory:    w.FreeMemory,
			FreeGpuCount:  w.FreeGpuCount,
			BuildVersion:  w.BuildVersion,
		}

		containers, err := gws.containerRepo.GetActiveContainersByWorkerId(w.Id)
		if err != nil {
			continue
		}

		pbWorkers[i].ActiveContainers = make([]*pb.Container, len(containers))
		for j, c := range containers {
			pbWorkers[i].ActiveContainers[j] = &pb.Container{
				ContainerId: c.ContainerId,
				WorkspaceId: string(c.WorkspaceId),
				Status:      string(c.Status),
				ScheduledAt: timestamppb.New(time.Unix(c.ScheduledAt, 0)),
			}
		}
	}

	return &pb.ListWorkersResponse{
		Ok:      true,
		Workers: pbWorkers,
	}, nil
}

func sortWorkers(w []*types.Worker) {
	slices.SortFunc(w, func(i, j *types.Worker) int {
		if i.PoolName < j.PoolName {
			return -1
		}
		if i.PoolName > j.PoolName {
			return 1
		}
		if i.Status < j.Status {
			return -1
		}
		if i.Status > j.Status {
			return 1
		}
		if i.MachineId < j.MachineId {
			return -1
		}
		if i.MachineId > j.MachineId {
			return 1
		}
		if i.Id < j.Id {
			return -1
		}
		if i.Id > j.Id {
			return 1
		}
		return 0
	})
}

func (gws *GatewayService) CordonWorker(ctx context.Context, in *pb.CordonWorkerRequest) (*pb.CordonWorkerResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	if !auth.HasPermission(authInfo) {
		return &pb.CordonWorkerResponse{
			Ok:     false,
			ErrMsg: "Unauthorized Access",
		}, nil
	}

	if _, err := isClusterAdmin(ctx); err != nil {
		return &pb.CordonWorkerResponse{
			Ok:     false,
			ErrMsg: err.Error(),
		}, nil
	}

	worker, err := gws.workerRepo.GetWorkerById(in.WorkerId)
	if err != nil {
		return &pb.CordonWorkerResponse{
			Ok:     false,
			ErrMsg: err.Error(),
		}, nil
	}

	if err := gws.workerRepo.UpdateWorkerStatus(worker.Id, types.WorkerStatusDisabled); err != nil {
		return &pb.CordonWorkerResponse{
			Ok:     false,
			ErrMsg: err.Error(),
		}, nil
	}

	return &pb.CordonWorkerResponse{
		Ok: true,
	}, nil
}

func (gws *GatewayService) UncordonWorker(ctx context.Context, in *pb.UncordonWorkerRequest) (*pb.UncordonWorkerResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	if !auth.HasPermission(authInfo) {
		return &pb.UncordonWorkerResponse{
			Ok:     false,
			ErrMsg: "Unauthorized Access",
		}, nil
	}
	if _, err := isClusterAdmin(ctx); err != nil {
		return &pb.UncordonWorkerResponse{
			Ok:     false,
			ErrMsg: err.Error(),
		}, nil
	}

	worker, err := gws.workerRepo.GetWorkerById(in.WorkerId)
	if err != nil {
		return &pb.UncordonWorkerResponse{
			Ok:     false,
			ErrMsg: err.Error(),
		}, nil
	}

	err = gws.workerRepo.UpdateWorkerStatus(worker.Id, types.WorkerStatusAvailable)
	if err != nil {
		return &pb.UncordonWorkerResponse{
			Ok:     false,
			ErrMsg: err.Error(),
		}, nil
	}

	return &pb.UncordonWorkerResponse{
		Ok: true,
	}, nil
}

func (gws *GatewayService) DrainWorker(ctx context.Context, in *pb.DrainWorkerRequest) (*pb.DrainWorkerResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	if !auth.HasPermission(authInfo) {
		return &pb.DrainWorkerResponse{
			Ok:     false,
			ErrMsg: "Unauthorized Access",
		}, nil
	}

	if _, err := isClusterAdmin(ctx); err != nil {
		return &pb.DrainWorkerResponse{
			Ok:     false,
			ErrMsg: err.Error(),
		}, nil
	}

	worker, err := gws.workerRepo.GetWorkerById(in.WorkerId)
	if err != nil {
		return &pb.DrainWorkerResponse{
			Ok:     false,
			ErrMsg: err.Error(),
		}, nil
	}

	containers, err := gws.containerRepo.GetActiveContainersByWorkerId(worker.Id)
	if err != nil {
		return &pb.DrainWorkerResponse{
			Ok:     false,
			ErrMsg: err.Error(),
		}, err
	}

	var group errgroup.Group
	for _, container := range containers {
		group.Go(func() error {
			return gws.scheduler.Stop(&types.StopContainerArgs{ContainerId: container.ContainerId, Reason: types.StopContainerReasonAdmin})
		})
	}
	if err := group.Wait(); err != nil {
		return &pb.DrainWorkerResponse{
			Ok:     false,
			ErrMsg: err.Error(),
		}, nil
	}

	return &pb.DrainWorkerResponse{
		Ok: true,
	}, nil
}

func isClusterAdmin(ctx context.Context) (bool, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)
	if authInfo.Token.TokenType == types.TokenTypeClusterAdmin {
		return true, nil
	}
	return false, errors.New("This action is not permitted")
}
