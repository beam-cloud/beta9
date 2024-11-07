package bot

import (
	"context"

	"github.com/beam-cloud/beta9/pkg/auth"

	pb "github.com/beam-cloud/beta9/proto"
)

func (pbs *PetriBotService) StartBotServe(ctx context.Context, in *pb.StartBotServeRequest) (*pb.StartBotServeResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	instance, err := pbs.getOrCreateBotInstance(in.StubId)
	if err != nil {
		return &pb.StartBotServeResponse{Ok: false}, nil
	}

	if authInfo.Workspace.ExternalId != instance.stub.Workspace.ExternalId {
		instance.cancelFunc()
		return &pb.StartBotServeResponse{Ok: false}, nil
	}

	return &pb.StartBotServeResponse{Ok: true}, nil
}

func (s *PetriBotService) BotServeKeepAlive(ctx context.Context, in *pb.BotServeKeepAliveRequest) (*pb.BotServeKeepAliveResponse, error) {
	return &pb.BotServeKeepAliveResponse{Ok: true}, nil
}
