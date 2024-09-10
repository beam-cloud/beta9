package bot

import (
	"context"
	"log"
	"time"

	"github.com/beam-cloud/beta9/pkg/auth"

	pb "github.com/beam-cloud/beta9/proto"
)

func (s *PetriBotService) StartBotServe(in *pb.StartBotServeRequest, stream pb.BotService_StartBotServeServer) error {
	ctx := stream.Context()
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	log.Printf("authInfo: %+v\n", authInfo)

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
