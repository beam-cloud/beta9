package gatewayservices

import (
	"cmp"
	"context"
	"fmt"
	"math"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/types"

	pb "github.com/beam-cloud/beta9/proto"
	"golang.org/x/exp/slices"
)

func (gws *GatewayService) ListMachines(ctx context.Context, in *pb.ListMachinesRequest) (*pb.ListMachinesResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	if !auth.HasPermission(authInfo) {
		return &pb.ListMachinesResponse{
			Ok:       false,
			ErrMsg:   "Unauthorized Access",
			Machines: []*pb.Machine{},
		}, nil
	}

	gpus, err := gws.scheduler.GetServerlessGPUAvailability()
	if err != nil {
		return nil, err
	}
	supportedGpus := gws.supportedServerlessGpus()

	if authInfo.Token.TokenType != types.TokenTypeClusterAdmin {
		if gws.computeService == nil {
			return &pb.ListMachinesResponse{Ok: false, ErrMsg: "compute service is unavailable"}, nil
		}
		machines, err := gws.computeService.ListPrivateMachines(ctx, authInfo, in.PoolName, int(in.Limit))
		if err != nil {
			return &pb.ListMachinesResponse{Ok: false, ErrMsg: err.Error()}, nil
		}
		return machineListResponse(gpus, supportedGpus, machines, in.Limit, gws.appConfig.GatewayService.StubLimits), nil
	}

	if gws.localMachinePool(in.PoolName) {
		return &pb.ListMachinesResponse{Ok: false, ErrMsg: fmt.Sprintf("pool %q is managed by its local controller", in.PoolName)}, nil
	}
	if gws.computeService == nil {
		return &pb.ListMachinesResponse{Ok: false, ErrMsg: "compute service is unavailable"}, nil
	}

	machines, err := gws.computeService.ListManagedMachines(ctx, authInfo, in.PoolName)
	if err != nil {
		return &pb.ListMachinesResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	return machineListResponse(gpus, supportedGpus, machines, in.Limit, gws.appConfig.GatewayService.StubLimits), nil
}

// supportedServerlessGpus reports pool-config-based serverless support per
// GPU type, so scale-to-zero pools still show as available.
func (gws *GatewayService) supportedServerlessGpus() map[string]bool {
	supported := map[string]bool{}
	if gws.scheduler == nil {
		return supported
	}
	for _, gpu := range types.AllGPUTypes() {
		if gpu == types.GPU_ANY {
			continue
		}
		supported[gpu.String()] = gws.scheduler.HasManagedPoolForGPU(gpu.String())
	}
	return supported
}

func machineListResponse(gpus map[string]bool, supportedGpus map[string]bool, machines []*pb.Machine, limit uint32, stubLimits types.StubLimits) *pb.ListMachinesResponse {
	slices.SortFunc(machines, func(i, j *pb.Machine) int {
		return cmp.Or(cmp.Compare(i.PoolName, j.PoolName), cmp.Compare(i.Id, j.Id))
	})
	if limit > 0 && len(machines) > int(limit) {
		machines = machines[:limit]
	}
	return &pb.ListMachinesResponse{
		Ok:               true,
		Gpus:             gpus,
		SupportedGpus:    supportedGpus,
		Machines:         machines,
		MaxCpuMillicores: uint32(min(stubLimits.Cpu, math.MaxUint32)),
		MaxMemoryMb:      uint32(min(stubLimits.Memory, math.MaxUint32)),
	}
}

func (gws *GatewayService) localMachinePool(name string) bool {
	pool, configured := gws.appConfig.Worker.Pools[name]
	return configured && !pool.AgentHosted()
}

func (gws *GatewayService) CreateMachine(ctx context.Context, in *pb.CreateMachineRequest) (*pb.CreateMachineResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	if !auth.HasPermission(authInfo) {
		return &pb.CreateMachineResponse{
			Ok:     false,
			ErrMsg: "Unauthorized Access",
		}, nil
	}

	if authInfo.Token.TokenType != types.TokenTypeClusterAdmin {
		return &pb.CreateMachineResponse{Ok: false, ErrMsg: "machine creation is managed through private pool join or BYOC scaling"}, nil
	}

	if gws.localMachinePool(in.PoolName) {
		return &pb.CreateMachineResponse{Ok: false, ErrMsg: fmt.Sprintf("pool %q is managed by its local controller", in.PoolName)}, nil
	}
	if gws.computeService == nil {
		return &pb.CreateMachineResponse{Ok: false, ErrMsg: "compute service is unavailable"}, nil
	}
	bootstrap, err := gws.computeService.CreateManagedMachine(ctx, authInfo, in.PoolName)
	if err != nil {
		return &pb.CreateMachineResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	return &pb.CreateMachineResponse{
		Ok: true,
		Machine: &pb.Machine{
			Id:       bootstrap.MachineID,
			PoolName: bootstrap.PoolName,
		},
		InstallCommand: bootstrap.InstallCommand,
	}, nil
}

func (gws *GatewayService) DeleteMachine(ctx context.Context, in *pb.DeleteMachineRequest) (*pb.DeleteMachineResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)
	if !auth.HasPermission(authInfo) {
		return &pb.DeleteMachineResponse{Ok: false, ErrMsg: "Unauthorized Access"}, nil
	}
	if authInfo.Token.TokenType == types.TokenTypeClusterAdmin {
		if gws.localMachinePool(in.PoolName) {
			return &pb.DeleteMachineResponse{Ok: false, ErrMsg: fmt.Sprintf("pool %q is managed by its local controller", in.PoolName)}, nil
		}
		if gws.computeService == nil {
			return &pb.DeleteMachineResponse{Ok: false, ErrMsg: "compute service is unavailable"}, nil
		}
		if err := gws.computeService.DeleteManagedMachine(ctx, authInfo, in.PoolName, in.MachineId); err != nil {
			return &pb.DeleteMachineResponse{Ok: false, ErrMsg: err.Error()}, nil
		}
		return &pb.DeleteMachineResponse{Ok: true}, nil
	}
	if gws.computeService == nil {
		return &pb.DeleteMachineResponse{Ok: false, ErrMsg: "compute service is unavailable"}, nil
	}
	return gws.computeService.DeletePrivateMachine(ctx, in)
}
