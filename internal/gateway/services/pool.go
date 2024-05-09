package gatewayservices

import (
	"context"
	"log"

	"github.com/beam-cloud/beta9/internal/auth"
	pb "github.com/beam-cloud/beta9/proto"
)

func (gws *GatewayService) ListPools(ctx context.Context, in *pb.ListPoolsRequest) (*pb.ListPoolsResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	log.Println("authInfo:", authInfo)

	// gws.appConfig.Worker.Pools

	return &pb.ListPoolsResponse{
		Ok: true,
		Pools: []*pb.Pool{
			&pb.Pool{Name: "lambdalabs-a100"},
		},
	}, nil
}
