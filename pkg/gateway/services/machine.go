package gatewayservices

import (
	"context"
	"log"

	"github.com/beam-cloud/beta9/pkg/auth"
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

	machines := []*pb.Machine{}
	return &pb.ListMachinesResponse{
		Ok:       true,
		Machines: machines,
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

	log.Println(gws.providerRepo.AddMachine("ec2", "mypool", "randomnewid", &types.ProviderMachineState{}))

	return &pb.CreateMachineResponse{
		Ok: true,
	}, nil
}
