package bot

import (
	"context"
	"log"

	"github.com/beam-cloud/beta9/pkg/auth"

	pb "github.com/beam-cloud/beta9/proto"
)

func (s *PetriBotService) StartBotServe(in *pb.StartBotServeRequest, stream pb.BotService_StartBotServeServer) error {
	ctx := stream.Context()
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	log.Println("authInfo: ", authInfo)

	return nil
}

func (s *PetriBotService) StopBotServe(ctx context.Context, in *pb.StopBotServeRequest) (*pb.StopBotServeResponse, error) {
	return &pb.StopBotServeResponse{Ok: true}, nil
}

func (s *PetriBotService) BotServeKeepAlive(ctx context.Context, in *pb.BotServeKeepAliveRequest) (*pb.BotServeKeepAliveResponse, error) {
	return &pb.BotServeKeepAliveResponse{Ok: true}, nil
}
