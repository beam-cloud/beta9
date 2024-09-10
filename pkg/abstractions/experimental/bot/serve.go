package bot

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"time"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/types"

	pb "github.com/beam-cloud/beta9/proto"
)

func (s *PetriBotService) StartBotServe(in *pb.StartBotServeRequest, stream pb.BotService_StartBotServeServer) error {
	ctx := stream.Context()
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	log.Printf("authInfo: %+v\n", authInfo)

	stub, err := s.backendRepo.GetStubByExternalId(ctx, in.StubId)
	if err != nil {
		return errors.New("invalid stub id")
	}

	var stubConfig *types.StubConfigV1 = &types.StubConfigV1{}
	err = json.Unmarshal([]byte(stub.Config), stubConfig)
	if err != nil {
		return err
	}

	var botConfig BotConfig
	err = json.Unmarshal(stubConfig.Extra, &botConfig)
	if err != nil {
		return err
	}

	log.Printf("botConfig: %+v\n", botConfig)

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
	log.Println("keepalive")
	return &pb.BotServeKeepAliveResponse{Ok: true}, nil
}
