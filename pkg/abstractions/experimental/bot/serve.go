package bot

import (
	"context"
	"time"

	"github.com/beam-cloud/beta9/pkg/auth"

	pb "github.com/beam-cloud/beta9/proto"
)

func (pbs *PetriBotService) StartBotServe(in *pb.StartBotServeRequest, stream pb.BotService_StartBotServeServer) error {
	ctx := stream.Context()
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	instance, err := pbs.getOrCreateBotInstance(in.StubId)
	if err != nil {
		return err
	}

	if authInfo.Workspace.ExternalId != instance.stub.Workspace.ExternalId {
		stream.Send(&pb.StartBotServeResponse{Done: true, Output: "Invalid stub", ExitCode: 1})
		instance.cancelFunc()
		return nil
	}

	for {
		select {
		case <-stream.Context().Done():
			instance.cancelFunc()
			return nil
		default:
			resp, err := instance.botInterface.outputBuffer.Pop()
			if err == nil {
				stream.Send(&pb.StartBotServeResponse{Done: false, Output: resp})
			} else {
				time.Sleep(time.Millisecond * 100)
			}
		}
	}
}

func (s *PetriBotService) BotServeKeepAlive(ctx context.Context, in *pb.BotServeKeepAliveRequest) (*pb.BotServeKeepAliveResponse, error) {
	return &pb.BotServeKeepAliveResponse{Ok: true}, nil
}
