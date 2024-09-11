package bot

import (
	"context"
	"time"

	"github.com/beam-cloud/beta9/pkg/auth"

	pb "github.com/beam-cloud/beta9/proto"
)

func (pbs *PetriBotService) StartBotServe(in *pb.StartBotServeRequest, stream pb.BotService_StartBotServeServer) error {
	ctx := stream.Context()
	_, _ = auth.AuthInfoFromContext(ctx)

	instance, err := pbs.getOrCreateBotInstance(in.StubId)
	if err != nil {
		return err
	}

	stream.Send(&pb.StartBotServeResponse{Done: false, Output: instance.token.Key + "\n"})

	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				// outputChan <- common.OutputMsg{Msg: ""}
			case <-ctx.Done():
				return
			}
		}
	}()

	for {
		select {
		case <-stream.Context().Done():
			instance.cancelFunc()
			return nil
		default:
			stream.Send(&pb.StartBotServeResponse{Done: false, Output: "Bot running\n"})
			time.Sleep(time.Second * 1)
		}
	}
}

func (s *PetriBotService) StopBotServe(ctx context.Context, in *pb.StopBotServeRequest) (*pb.StopBotServeResponse, error) {
	return &pb.StopBotServeResponse{Ok: true}, nil
}

func (s *PetriBotService) BotServeKeepAlive(ctx context.Context, in *pb.BotServeKeepAliveRequest) (*pb.BotServeKeepAliveResponse, error) {
	return &pb.BotServeKeepAliveResponse{Ok: true}, nil
}
