package gatewayservices

import (
	"cmp"
	"context"
	"fmt"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/providers"
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
		return machineListResponse(gpus, supportedGpus, machines, in.Limit), nil
	}

	backend, pool := gws.classifyMachinePool(in.PoolName)
	if backend == machinePoolProvider {
		machines, err := gws.listProviderMachines(in.PoolName, pool)
		if err != nil {
			return &pb.ListMachinesResponse{Ok: false, ErrMsg: err.Error()}, nil
		}
		return machineListResponse(gpus, supportedGpus, machines, in.Limit), nil
	}
	if backend == machinePoolLocal {
		return &pb.ListMachinesResponse{Ok: false, ErrMsg: fmt.Sprintf("pool %q is managed by its local controller", in.PoolName)}, nil
	}
	if gws.computeService == nil {
		return &pb.ListMachinesResponse{Ok: false, ErrMsg: "compute service is unavailable"}, nil
	}

	machines, err := gws.computeService.ListManagedMachines(ctx, authInfo, in.PoolName)
	if err != nil {
		return &pb.ListMachinesResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	if in.PoolName == "" {
		providerMachines, err := gws.listAllProviderMachines()
		if err != nil {
			return &pb.ListMachinesResponse{Ok: false, ErrMsg: err.Error()}, nil
		}
		machines = append(machines, providerMachines...)
	}
	return machineListResponse(gpus, supportedGpus, machines, in.Limit), nil
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
		supported[gpu.String()] = gws.scheduler.HasManagedPoolForGPU(gpu.String(), false)
	}
	return supported
}

func machineListResponse(gpus map[string]bool, supportedGpus map[string]bool, machines []*pb.Machine, limit uint32) *pb.ListMachinesResponse {
	slices.SortFunc(machines, func(i, j *pb.Machine) int {
		return cmp.Or(cmp.Compare(i.PoolName, j.PoolName), cmp.Compare(i.Id, j.Id))
	})
	if limit > 0 && len(machines) > int(limit) {
		machines = machines[:limit]
	}
	return &pb.ListMachinesResponse{Ok: true, Gpus: gpus, SupportedGpus: supportedGpus, Machines: machines}
}

type machinePoolBackend uint8

const (
	machinePoolManagedAgent machinePoolBackend = iota
	machinePoolProvider
	machinePoolLocal
)

// classifyMachinePool routes cluster-admin machine operations before any
// repository call, so one pool can never fall through into another backend.
func (gws *GatewayService) classifyMachinePool(name string) (machinePoolBackend, types.WorkerPoolConfig) {
	pool, configured := gws.appConfig.Worker.Pools[name]
	if !configured || pool.AgentHosted() {
		return machinePoolManagedAgent, pool
	}
	if pool.Mode == types.PoolModeExternal && pool.Provider != nil {
		return machinePoolProvider, pool
	}
	return machinePoolLocal, pool
}

func (gws *GatewayService) listAllProviderMachines() ([]*pb.Machine, error) {
	machines := []*pb.Machine{}
	for name, pool := range gws.appConfig.Worker.Pools {
		backend, _ := gws.classifyMachinePool(name)
		if backend != machinePoolProvider {
			continue
		}
		poolMachines, err := gws.listProviderMachines(name, pool)
		if err != nil {
			return nil, err
		}
		machines = append(machines, poolMachines...)
	}
	return machines, nil
}

func (gws *GatewayService) listProviderMachines(poolName string, pool types.WorkerPoolConfig) ([]*pb.Machine, error) {
	if gws.providerRepo == nil || pool.Provider == nil {
		return nil, fmt.Errorf("provider machine repository is unavailable")
	}
	machines, err := gws.providerRepo.ListAllMachines(string(*pool.Provider), poolName, false)
	if err != nil {
		return nil, fmt.Errorf("unable to list machines: %w", err)
	}
	out := make([]*pb.Machine, 0, len(machines))
	for _, machine := range machines {
		out = append(out, providerMachineToProto(machine))
	}
	return out, nil
}

func providerMachineToProto(machine *types.ProviderMachine) *pb.Machine {
	if machine == nil || machine.State == nil {
		return &pb.Machine{}
	}
	metrics := machine.Metrics
	if metrics == nil {
		metrics = &types.ProviderMachineMetrics{}
	}
	status := machine.State.Status
	if status == types.MachineStatusReady {
		status = types.MachineStatusAvailable
	}
	return &pb.Machine{
		Id:            machine.State.MachineId,
		Cpu:           machine.State.Cpu,
		Memory:        machine.State.Memory,
		Gpu:           machine.State.Gpu,
		GpuCount:      machine.State.GpuCount,
		Status:        string(status),
		PoolName:      machine.State.PoolName,
		LastKeepalive: machine.State.LastKeepalive,
		Created:       machine.State.Created,
		AgentVersion:  machine.State.AgentVersion,
		MachineMetrics: &pb.MachineMetrics{
			TotalCpuAvailable:    int32(metrics.TotalCpuAvailable),
			TotalMemoryAvailable: int32(metrics.TotalMemoryAvailable),
			CpuUtilizationPct:    float32(metrics.CpuUtilizationPct),
			MemoryUtilizationPct: float32(metrics.MemoryUtilizationPct),
			WorkerCount:          int32(metrics.WorkerCount),
			ContainerCount:       int32(metrics.ContainerCount),
			FreeGpuCount:         int32(metrics.FreeGpuCount),
			CacheUsagePct:        float32(metrics.CacheUsagePct),
			CacheCapacity:        int32(metrics.CacheCapacity),
			CacheMemoryUsage:     int32(metrics.CacheMemoryUsage),
			CacheCpuUsage:        float32(metrics.CacheCpuUsage),
		},
	}
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

	backend, pool := gws.classifyMachinePool(in.PoolName)
	if backend == machinePoolProvider {
		return gws.createProviderMachine(ctx, authInfo, in.PoolName, pool)
	}
	if backend == machinePoolLocal {
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
			Id:                bootstrap.MachineID,
			PoolName:          bootstrap.PoolName,
			ProviderName:      types.DefaultAgentName,
			RegistrationToken: bootstrap.Token,
		},
		InstallCommand: bootstrap.InstallCommand,
	}, nil
}

func (gws *GatewayService) createProviderMachine(ctx context.Context, authInfo *auth.AuthInfo, poolName string, pool types.WorkerPoolConfig) (*pb.CreateMachineResponse, error) {
	if gws.providerRepo == nil || pool.Provider == nil {
		return &pb.CreateMachineResponse{Ok: false, ErrMsg: "provider machine repository is unavailable"}, nil
	}
	if authInfo.Token.WorkspaceId == nil {
		return &pb.CreateMachineResponse{Ok: false, ErrMsg: "missing admin workspace"}, nil
	}
	token, err := gws.backendRepo.CreateToken(ctx, *authInfo.Token.WorkspaceId, types.TokenTypeMachine, true)
	if err != nil {
		return &pb.CreateMachineResponse{Ok: false, ErrMsg: fmt.Sprintf("unable to create token: %v", err)}, nil
	}

	machineID := providers.MachineId()
	if err := gws.providerRepo.AddMachine(string(*pool.Provider), poolName, machineID, &types.ProviderMachineState{
		PoolName:          poolName,
		RegistrationToken: token.Key,
		Gpu:               pool.GPUType,
		AutoConsolidate:   false,
	}); err != nil {
		return &pb.CreateMachineResponse{Ok: false, ErrMsg: fmt.Sprintf("unable to create machine: %v", err)}, nil
	}

	return &pb.CreateMachineResponse{
		Ok: true,
		Machine: &pb.Machine{
			Id:                machineID,
			RegistrationToken: token.Key,
			PoolName:          poolName,
			Gpu:               pool.GPUType,
			ProviderName:      string(*pool.Provider),
			TailscaleUrl:      gws.appConfig.Tailscale.ControlURL,
			TailscaleAuth:     gws.appConfig.Tailscale.AuthKey,
			UserData:          pool.UserData,
		},
		AgentUpstreamUrl:    gws.appConfig.Agent.UpstreamURL,
		AgentUpstreamBranch: gws.appConfig.Agent.UpstreamBranch,
		AgentUpstreamToken:  gws.appConfig.Agent.UpstreamToken,
	}, nil
}

func (gws *GatewayService) DeleteMachine(ctx context.Context, in *pb.DeleteMachineRequest) (*pb.DeleteMachineResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)
	if !auth.HasPermission(authInfo) {
		return &pb.DeleteMachineResponse{Ok: false, ErrMsg: "Unauthorized Access"}, nil
	}
	if authInfo.Token.TokenType == types.TokenTypeClusterAdmin {
		backend, pool := gws.classifyMachinePool(in.PoolName)
		if backend == machinePoolProvider {
			if gws.providerRepo == nil || pool.Provider == nil {
				return &pb.DeleteMachineResponse{Ok: false, ErrMsg: "provider machine repository is unavailable"}, nil
			}
			if err := gws.providerRepo.RemoveMachine(string(*pool.Provider), in.PoolName, in.MachineId); err != nil {
				return &pb.DeleteMachineResponse{Ok: false, ErrMsg: fmt.Sprintf("unable to delete machine: %v", err)}, nil
			}
			return &pb.DeleteMachineResponse{Ok: true}, nil
		}
		if backend == machinePoolLocal {
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
