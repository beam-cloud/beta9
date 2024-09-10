package bot

import (
	"context"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
)

type BotConfig struct {
	Places      []string
	Transitions []BotTransitionConfig
}

type BotTransitionConfig struct {
	Cpu     int64         `json:"cpu"`
	Gpu     types.GpuType `json:"gpu"`
	Memory  int64         `json:"memory"`
	ImageId string        `json:"image_id"`
}

type BotService interface {
	pb.BotServiceServer
	StartBotServe(in *pb.StartBotServeRequest, stream pb.BotService_StartBotServeServer) error
	StopBotServe(ctx context.Context, in *pb.StopBotServeRequest) (*pb.StopBotServeResponse, error)
}

type PetriBotService struct {
	pb.UnimplementedBotServiceServer
	rdb *common.RedisClient
}

func NewPetriBotService(rdb *common.RedisClient) (BotService, error) {
	return &PetriBotService{
		rdb: rdb,
	}, nil
}
