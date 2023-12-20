package gateway

import (
	"context"
	"log"

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

func (gws *GatewayService) HeadObject(ctx context.Context, in *pb.HeadObjectRequest) (*pb.HeadObjectResponse, error) {
	// TODO: implement HeadObject
	return &pb.HeadObjectResponse{
		Ok: true,
	}, nil
}

func (gws *GatewayService) PutObject(ctx context.Context, in *pb.PutObjectRequest) (*pb.PutObjectResponse, error) {
	log.Println("metadata:", in.ObjectMetadata)

	// TODO: implement PutObject
	return &pb.PutObjectResponse{
		Ok: true,
	}, nil
}
