package gatewayservices

import (
	"context"
	"sort"

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

	pools := gws.appConfig.Worker.Pools

	var keys []string
	for key := range pools {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	formattedPools := []*pb.Pool{}
	for _, poolName := range keys {
		poolConfig := pools[poolName]
		poolState, err := gws.workerPoolRepo.GetWorkerPoolState(ctx, poolName)
		if err != nil {
			return nil, err
		}

		formattedPools = append(formattedPools, &pb.Pool{
			Name:                  poolName,
			Gpu:                   poolConfig.GPUType,
			MinFreeGpu:            poolConfig.PoolSizing.MinFreeGPU,
			MinFreeCpu:            poolConfig.PoolSizing.MinFreeCPU,
			MinFreeMemory:         poolConfig.PoolSizing.MinFreeMemory,
			DefaultWorkerCpu:      poolConfig.PoolSizing.DefaultWorkerCPU,
			DefaultWorkerMemory:   poolConfig.PoolSizing.DefaultWorkerMemory,
			DefaultWorkerGpuCount: poolConfig.PoolSizing.DefaultWorkerGpuCount,
			State:                 poolState.ToProto(),
		})
	}

	return &pb.ListPoolsResponse{
		Ok:    true,
		Pools: formattedPools,
	}, nil
}
