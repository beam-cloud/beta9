package endpoint

import (
	"context"

	pb "github.com/beam-cloud/beam/proto"
)

type WebEndpointService interface {
	pb.WebEndpointServiceServer
	WebEndpointServe(ctx context.Context, in *pb.WebEndpointServeRequest) (*pb.WebEndpointServeResponse, error)
}
