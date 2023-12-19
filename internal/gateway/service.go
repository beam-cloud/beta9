package gateway

import (
	"context"

	pb "github.com/beam-cloud/beam/proto"
)

type GatewayService struct {
	gw *Gateway
	pb.UnimplementedGatewayServiceServer
}

func NewGatewayService(gw *Gateway) (*GatewayService, error) {
	return &GatewayService{
		gw: gw,
	}, nil
}

func (wbs *GatewayService) HeadObject(ctx context.Context, in *pb.HeadObjectRequest) (*pb.HeadObjectResponse, error) {
	return &pb.HeadObjectResponse{
		Ok: true,
	}, nil
}

func (wbs *GatewayService) PutObject(ctx context.Context, in *pb.PutObjectRequest) (*pb.PutObjectResponse, error) {
	return &pb.PutObjectResponse{
		Ok: true,
	}, nil
}

func (wbs *GatewayService) PutAndExtractObject(ctx context.Context, in *pb.PutAndExtractObjectRequest) (*pb.PutAndExtractObjectResponse, error) {
	return &pb.PutAndExtractObjectResponse{
		Ok: true,
	}, nil
}
