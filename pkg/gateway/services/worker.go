package gatewayservices

import (
	"cmp"
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

	workers, err := gws.workerRepo.GetAllWorkers()
	if err != nil {
		return &pb.ListWorkersResponse{
			Ok:     false,
			ErrMsg: err.Error(),
		}, nil
	}

	workers = gws.filterVisibleWorkers(authInfo, workers)
	sortWorkers(workers)

	pbWorkers := make([]*pb.Worker, len(workers))
	for i, w := range workers {
		rolloutVersion := ""
		if w.RolloutGeneration != "" {
			rolloutVersion = types.WorkerImageVersion(w.WorkerImageOverride, gws.appConfig.Worker.ImageTag)
		}
		pbWorkers[i] = &pb.Worker{
			Id:                  w.Id,
			Status:              string(w.Status),
			Gpu:                 w.Gpu,
			PoolName:            w.PoolName,
			MachineId:           w.MachineId,
			Priority:            w.Priority,
			TotalCpu:            w.TotalCpu,
			TotalMemory:         w.TotalMemory,
			TotalGpuCount:       w.TotalGpuCount,
			FreeCpu:             w.FreeCpu,
			FreeMemory:          w.FreeMemory,
			FreeGpuCount:        w.FreeGpuCount,
			BuildVersion:        w.BuildVersion,
			CordonRequested:     w.CordonRequested,
			RolloutGeneration:   w.RolloutGeneration,
			RolloutBuildVersion: rolloutVersion,
			WorkerImageOverride: w.WorkerImageOverride,
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

// Scope worker management by authenticated identity, never by pool name alone.
func (gws *GatewayService) filterVisibleWorkers(authInfo *auth.AuthInfo, workers []*types.Worker) []*types.Worker {
	filtered := make([]*types.Worker, 0, len(workers))
	for _, worker := range workers {
		if gws.workerVisibleTo(authInfo, worker) {
			filtered = append(filtered, worker)
		}
	}
	return filtered
}

func (gws *GatewayService) workerVisibleTo(authInfo *auth.AuthInfo, worker *types.Worker) bool {
	if worker == nil || authInfo == nil || authInfo.Token == nil {
		return false
	}
	if authInfo.Token.TokenType != types.TokenTypeClusterAdmin {
		return authInfo.Workspace != nil && !worker.ControlPlaneManaged &&
			worker.WorkspaceId == authInfo.Workspace.ExternalId
	}
	if worker.ControlPlaneManaged {
		return true
	}
	if worker.WorkspaceId != "" {
		return false
	}
	pool, ok := gws.appConfig.Worker.Pools[worker.PoolName]
	if !ok || pool.Mode == types.PoolModePrivate {
		return false
	}
	// Distinguish a private agent worker from a same-named configured pool.
	return !worker.RequiresPoolSelector || pool.RequiresPoolSelector
}

func (gws *GatewayService) visibleWorker(authInfo *auth.AuthInfo, workerID string) (*types.Worker, error) {
	worker, err := gws.workerRepo.GetWorkerById(workerID)
	if err != nil {
		return nil, err
	}
	if !gws.workerVisibleTo(authInfo, worker) {
		return nil, errors.New("worker not found")
	}
	return worker, nil
}

func sortWorkers(w []*types.Worker) {
	slices.SortFunc(w, func(i, j *types.Worker) int {
		return cmp.Or(
			cmp.Compare(i.PoolName, j.PoolName),
			cmp.Compare(i.Status, j.Status),
			cmp.Compare(i.MachineId, j.MachineId),
			cmp.Compare(i.Id, j.Id),
		)
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

	worker, err := gws.visibleWorker(authInfo, in.WorkerId)
	if err != nil {
		return &pb.CordonWorkerResponse{
			Ok:     false,
			ErrMsg: err.Error(),
		}, nil
	}
	if err := gws.setWorkerCordon(ctx, worker, true); err != nil {
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
	worker, err := gws.visibleWorker(authInfo, in.WorkerId)
	if err != nil {
		return &pb.UncordonWorkerResponse{
			Ok:     false,
			ErrMsg: err.Error(),
		}, nil
	}
	err = gws.setWorkerCordon(ctx, worker, false)
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

	worker, err := gws.visibleWorker(authInfo, in.WorkerId)
	if err != nil {
		return &pb.DrainWorkerResponse{
			Ok:     false,
			ErrMsg: err.Error(),
		}, nil
	}
	// Drain is a safe, one-step operation: stop new scheduling before asking
	// existing containers to terminate. The cordon remains until an explicit
	// uncordon, which makes the command predictable for maintenance scripts.
	if err := gws.setWorkerCordon(ctx, worker, true); err != nil {
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
		if container.Status == types.ContainerStatusStopping {
			continue
		}
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

func (gws *GatewayService) setWorkerCordon(ctx context.Context, worker *types.Worker, cordoned bool) error {
	if gws.computeService != nil {
		handled, err := gws.computeService.SetAgentWorkerCordon(ctx, worker, cordoned)
		if err != nil || handled {
			return err
		}
	}
	return gws.workerRepo.SetWorkerCordon(worker.Id, cordoned)
}

func isClusterAdmin(ctx context.Context) (bool, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)
	if authInfo != nil && authInfo.Token != nil &&
		authInfo.Token.Active && !authInfo.Token.DisabledByClusterAdmin &&
		authInfo.Token.TokenType == types.TokenTypeClusterAdmin {
		return true, nil
	}
	return false, errors.New("This action is not permitted")
}
