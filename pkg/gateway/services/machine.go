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

			formattedMachines = append(formattedMachines, &pb.Machine{
				Id:            machine.State.MachineId,
				Cpu:           machine.State.Cpu,
				Memory:        machine.State.Memory,
				Gpu:           machine.State.Gpu,
				GpuCount:      machine.State.GpuCount,
				Status:        string(machine.State.Status),
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
			})
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

				formattedMachines = append(formattedMachines, &pb.Machine{
					Id:            machine.State.MachineId,
					Cpu:           machine.State.Cpu,
					Memory:        machine.State.Memory,
					Gpu:           machine.State.Gpu,
					GpuCount:      machine.State.GpuCount,
					Status:        string(machine.State.Status),
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
				})
			}
		}
	}

	return &pb.ListMachinesResponse{
		Ok:       true,
		Gpus:     gpus,
		Machines: formattedMachines,
	}, nil
}

func (gws *GatewayService) CreateMachine(ctx context.Context, in *pb.CreateMachineRequest) (*pb.CreateMachineResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)
	if authInfo.Token.TokenType != types.TokenTypeClusterAdmin {
		return &pb.CreateMachineResponse{
			Ok:     false,
			ErrMsg: "This action is not permitted",
		}, nil
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
	}, nil
}

func (gws *GatewayService) DeleteMachine(ctx context.Context, in *pb.DeleteMachineRequest) (*pb.DeleteMachineResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)
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
