package signal

import (
	"context"

	pb "github.com/beam-cloud/beta9/proto"
)

type SignalService interface {
	pb.SignalServiceServer
	SignalSet(ctx context.Context, in *pb.SignalSetRequest) (*pb.SignalSetResponse, error)
	SignalClear(ctx context.Context, in *pb.SignalClearRequest) (*pb.SignalClearResponse, error)
	SignalMonitor(req *pb.SignalMonitorRequest, stream pb.SignalService_SignalMonitorServer) error
}
