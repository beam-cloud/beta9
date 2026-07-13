package gatewayservices

import (
	"context"
	"fmt"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/providers"
	"github.com/beam-cloud/beta9/pkg/types"

	pb "github.com/beam-cloud/beta9/proto"
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

	gpus, err := gws.workerRepo.GetGpuAvailability()
	if err != nil {
		return nil, err
	}

	// Non-cluster admins only see GPU availability
	if authInfo.Token.TokenType != types.TokenTypeClusterAdmin {
		return &pb.ListMachinesResponse{
			Ok:   true,
			Gpus: gpus,
		}, nil
	}

	// Cluster admins see all machines associated with a cluster
	formattedMachines := []*pb.Machine{}
	if gws.computeService != nil {
		managedMachines, handled, err := gws.computeService.ListManagedMachines(ctx, authInfo, in.PoolName)
		if err != nil {
			return &pb.ListMachinesResponse{Ok: false, ErrMsg: err.Error()}, nil
		}
		if handled && in.PoolName != "" {
			return &pb.ListMachinesResponse{Ok: true, Gpus: gpus, Machines: managedMachines}, nil
		}
		formattedMachines = append(formattedMachines, managedMachines...)
	}
	if in.PoolName != "" {
		pool, ok := gws.appConfig.Worker.Pools[in.PoolName]
		if !ok {
			return &pb.ListMachinesResponse{
				Ok:     false,
				ErrMsg: "Invalid pool",
			}, nil
		}

		if pool.Provider == nil {
			return &pb.ListMachinesResponse{
				Ok:     false,
				ErrMsg: "Local pools don't currently track machines",
			}, nil
		}

		machines, err := gws.providerRepo.ListAllMachines(string(*pool.Provider), in.PoolName, false)
		if err != nil {
			return &pb.ListMachinesResponse{
				Ok:     false,
				ErrMsg: fmt.Sprintf("Unable to list machines: %s", err.Error()),
			}, nil
		}

		for _, machine := range machines {
			if machine.Metrics == nil {
				machine.Metrics = &types.ProviderMachineMetrics{}
			}

			formattedMachines = append(formattedMachines, providerMachineToProto(machine))
		}

	} else {
		for poolName, pool := range gws.appConfig.Worker.Pools {
			if pool.Provider == nil {
				continue
			}

			machines, err := gws.providerRepo.ListAllMachines(string(*pool.Provider), poolName, false)
			if err != nil {
				return &pb.ListMachinesResponse{
					Ok:     false,
					ErrMsg: fmt.Sprintf("Unable to list machines: %s", err.Error()),
				}, nil
			}

			for _, machine := range machines {
				if machine.Metrics == nil {
					machine.Metrics = &types.ProviderMachineMetrics{}
				}

				formattedMachines = append(formattedMachines, providerMachineToProto(machine))
			}
		}
	}

	return &pb.ListMachinesResponse{
		Ok:       true,
		Gpus:     gpus,
		Machines: formattedMachines,
	}, nil
}

func providerMachineToProto(machine *types.ProviderMachine) *pb.Machine {
	if machine == nil || machine.State == nil {
		return &pb.Machine{}
	}
	if machine.Metrics == nil {
		machine.Metrics = &types.ProviderMachineMetrics{}
	}
	return &pb.Machine{
		Id:            machine.State.MachineId,
		Cpu:           machine.State.Cpu,
		Memory:        machine.State.Memory,
		Gpu:           machine.State.Gpu,
		GpuCount:      machine.State.GpuCount,
		Status:        string(providerMachineStatus(machine.State.Status)),
		PoolName:      machine.State.PoolName,
		LastKeepalive: machine.State.LastKeepalive,
		Created:       machine.State.Created,
		AgentVersion:  machine.State.AgentVersion,
		MachineMetrics: &pb.MachineMetrics{
			TotalCpuAvailable:    int32(machine.Metrics.TotalCpuAvailable),
			TotalMemoryAvailable: int32(machine.Metrics.TotalMemoryAvailable),
			CpuUtilizationPct:    float32(machine.Metrics.CpuUtilizationPct),
			MemoryUtilizationPct: float32(machine.Metrics.MemoryUtilizationPct),
			WorkerCount:          int32(machine.Metrics.WorkerCount),
			ContainerCount:       int32(machine.Metrics.ContainerCount),
			FreeGpuCount:         int32(machine.Metrics.FreeGpuCount),
			CacheUsagePct:        float32(machine.Metrics.CacheUsagePct),
			CacheCapacity:        int32(machine.Metrics.CacheCapacity),
			CacheMemoryUsage:     int32(machine.Metrics.CacheMemoryUsage),
			CacheCpuUsage:        float32(machine.Metrics.CacheCpuUsage),
		},
	}
}

func providerMachineStatus(status types.MachineStatus) types.MachineStatus {
	if status == types.MachineStatusReady {
		return types.MachineStatusAvailable
	}
	return status
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
		return &pb.CreateMachineResponse{
			Ok:     false,
			ErrMsg: "This action is not permitted",
		}, nil
	}

	if gws.computeService != nil {
		bootstrap, handled, err := gws.computeService.CreateManagedMachine(ctx, authInfo, in.PoolName)
		if err != nil {
			return &pb.CreateMachineResponse{Ok: false, ErrMsg: err.Error()}, nil
		}
		if handled {
			return &pb.CreateMachineResponse{
				Ok: true,
				Machine: &pb.Machine{
					Id:                bootstrap.MachineID,
					PoolName:          bootstrap.PoolName,
					ProviderName:      "agent",
					RegistrationToken: bootstrap.Token,
				},
				InstallCommand: bootstrap.InstallCommand,
			}, nil
		}
	}

	pool, ok := gws.appConfig.Worker.Pools[in.PoolName]
	if !ok {
		return &pb.CreateMachineResponse{
			Ok:     false,
			ErrMsg: "Invalid pool name",
		}, nil
	}

	// Create a one-time auth token
	token, err := gws.backendRepo.CreateToken(ctx, *authInfo.Token.WorkspaceId, types.TokenTypeMachine, true)
	if err != nil {
		return &pb.CreateMachineResponse{
			Ok:     false,
			ErrMsg: fmt.Sprintf("Unable to create token: %s", err.Error()),
		}, nil
	}

	if pool.Provider == nil {
		return &pb.CreateMachineResponse{
			Ok:     false,
			ErrMsg: "This pool does not currently support machine creation",
		}, nil
	}

	machineId := providers.MachineId()
	err = gws.providerRepo.AddMachine(string(*pool.Provider), in.PoolName, machineId, &types.ProviderMachineState{
		PoolName:          in.PoolName,
		RegistrationToken: token.Key,
		Gpu:               pool.GPUType,
		AutoConsolidate:   false,
	})
	if err != nil {
		return &pb.CreateMachineResponse{
			Ok:     false,
			ErrMsg: fmt.Sprintf("Unable to create machine: %s", err.Error()),
		}, nil
	}

	return &pb.CreateMachineResponse{
		Ok: true,
		Machine: &pb.Machine{
			Id:                machineId,
			RegistrationToken: token.Key,
			PoolName:          in.PoolName,
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
	clusterAdmin, _ := isClusterAdmin(ctx)
	if gws.computeService != nil && clusterAdmin {
		handled, err := gws.computeService.DeleteManagedMachine(ctx, authInfo, in.PoolName, in.MachineId)
		if err != nil {
			return &pb.DeleteMachineResponse{Ok: false, ErrMsg: err.Error()}, nil
		}
		if handled {
			return &pb.DeleteMachineResponse{Ok: true}, nil
		}
	}

	if gws.computeService != nil {
		handled, res, err := gws.computeService.DeletePoolMachine(ctx, in)
		if handled || err != nil {
			return res, err
		}
	}

	if !auth.HasPermission(authInfo) {
		return &pb.DeleteMachineResponse{
			Ok:     false,
			ErrMsg: "Unauthorized Access",
		}, nil
	}

	if authInfo.Token.TokenType != types.TokenTypeClusterAdmin {
		return &pb.DeleteMachineResponse{
			Ok:     false,
			ErrMsg: "This action is not permitted",
		}, nil
	}

	pool, ok := gws.appConfig.Worker.Pools[in.PoolName]
	if !ok {
		return &pb.DeleteMachineResponse{
			Ok:     false,
			ErrMsg: "Invalid pool name",
		}, nil
	}

	err := gws.providerRepo.RemoveMachine(string(*pool.Provider), in.PoolName, in.MachineId)
	if err != nil {
		return &pb.DeleteMachineResponse{
			Ok:     false,
			ErrMsg: fmt.Sprintf("Unable to delete machine: %s", err.Error()),
		}, nil
	}

	return &pb.DeleteMachineResponse{
		Ok: true,
	}, nil
}
