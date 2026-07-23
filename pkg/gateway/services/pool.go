package gatewayservices

import (
	"context"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
)

func (gws *GatewayService) ListPools(ctx context.Context, in *pb.ListPoolsRequest) (*pb.ListPoolsResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	if !auth.HasPermission(authInfo) {
		return &pb.ListPoolsResponse{
			Ok:     false,
			ErrMsg: "Unauthorized Access",
		}, nil
	}

	if authInfo.Token.TokenType != types.TokenTypeClusterAdmin {
		return &pb.ListPoolsResponse{
			Ok:     false,
			ErrMsg: "This action is not permitted",
		}, nil
	}

	if gws.computeService == nil {
		return &pb.ListPoolsResponse{Ok: false, ErrMsg: "compute service is unavailable"}, nil
	}
	pools, err := gws.computeService.ListManagedPools(ctx, authInfo)
	if err != nil {
		return nil, err
	}
	formattedPools := make([]*pb.Pool, 0, len(pools))
	for _, pool := range pools {
		if pool != nil {
			formattedPools = append(formattedPools, poolToProto(pool.Name, pool.Config, pool.State))
		}
	}
	return &pb.ListPoolsResponse{Ok: true, Pools: formattedPools}, nil
}

func poolToProto(name string, config types.WorkerPoolConfig, state *types.WorkerPoolState) *pb.Pool {
	pool := &pb.Pool{
		Name:                  name,
		Gpu:                   config.GPUType,
		MinFreeGpu:            config.PoolSizing.MinFreeGPU,
		MinFreeCpu:            config.PoolSizing.MinFreeCPU,
		MinFreeMemory:         config.PoolSizing.MinFreeMemory,
		DefaultWorkerCpu:      config.PoolSizing.DefaultWorkerCPU,
		DefaultWorkerMemory:   config.PoolSizing.DefaultWorkerMemory,
		DefaultWorkerGpuCount: config.PoolSizing.DefaultWorkerGpuCount,
	}
	if state != nil {
		pool.State = state.ToProto()
	}
	return pool
}
