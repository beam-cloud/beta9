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
	if authInfo.Token.TokenType != types.TokenTypeClusterAdmin {
		return &pb.ListMachinesResponse{
			Ok:     false,
			ErrMsg: "This action is not permitted",
		}, nil
	}

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

		machines, err := gws.providerRepo.ListAllMachines(string(*pool.Provider), in.PoolName)
		if err != nil {
			return &pb.ListMachinesResponse{
				Ok:     false,
				ErrMsg: fmt.Sprintf("Unable to list machines: %s", err.Error()),
			}, nil
		}

		for _, machine := range machines {
			formattedMachines = append(formattedMachines, &pb.Machine{
				Id:       machine.State.MachineId,
				Cpu:      machine.State.Cpu,
				Memory:   machine.State.Memory,
				Gpu:      machine.State.Gpu,
				GpuCount: machine.State.GpuCount,
				Status:   string(machine.State.Status),
				PoolName: machine.State.PoolName,
			})
		}

	} else {
		for poolName, pool := range gws.appConfig.Worker.Pools {
			if pool.Provider == nil {
				continue
			}

			machines, err := gws.providerRepo.ListAllMachines(string(*pool.Provider), poolName)
			if err != nil {
				return &pb.ListMachinesResponse{
					Ok:     false,
					ErrMsg: fmt.Sprintf("Unable to list machines: %s", err.Error()),
				}, nil
			}

			for _, machine := range machines {
				formattedMachines = append(formattedMachines, &pb.Machine{
					Id:       machine.State.MachineId,
					Cpu:      machine.State.Cpu,
					Memory:   machine.State.Memory,
					Gpu:      machine.State.Gpu,
					GpuCount: machine.State.GpuCount,
					Status:   string(machine.State.Status),
					PoolName: machine.State.PoolName,
				})
			}
		}
	}

	return &pb.ListMachinesResponse{
		Ok:       true,
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
			ErrMsg: "Invalid pool",
		}, nil
	}

	// Create a one-time auth token
	token, err := gws.backendRepo.CreateToken(ctx, *authInfo.Token.WorkspaceId, types.TokenTypeMachine, false)
	if err != nil {
		return &pb.CreateMachineResponse{
			Ok:     false,
			ErrMsg: fmt.Sprintf("Unable to create token: %s", err.Error()),
		}, nil
	}

	machineId := providers.MachineId()
	err = gws.providerRepo.AddMachine(string(*pool.Provider), in.PoolName, machineId, &types.ProviderMachineState{
		PoolName:          in.PoolName,
		RegistrationToken: token.Key,
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
		},
	}, nil
}
