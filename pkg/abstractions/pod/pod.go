package pod

import (
	"context"
	"log"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/network"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/scheduler"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
)

type PodServiceOpts struct {
	Config        types.AppConfig
	BackendRepo   repository.BackendRepository
	ContainerRepo repository.ContainerRepository
	Tailscale     *network.Tailscale
	Scheduler     *scheduler.Scheduler
	RedisClient   *common.RedisClient
	EventRepo     repository.EventRepository
}

type PodService interface {
	pb.PodServiceServer
	CreatePod(ctx context.Context, in *pb.CreatePodRequest) (*pb.CreatePodResponse, error)
}

type CmdPodService struct {
	pb.PodServiceServer
	backendRepo     repository.BackendRepository
	containerRepo   repository.ContainerRepository
	scheduler       *scheduler.Scheduler
	keyEventManager *common.KeyEventManager
	rdb             *common.RedisClient
	tailscale       *network.Tailscale
	config          types.AppConfig
	eventRepo       repository.EventRepository
}

func NewPodService(
	ctx context.Context,
	opts PodServiceOpts,
) (PodService, error) {
	keyEventManager, err := common.NewKeyEventManager(opts.RedisClient)
	if err != nil {
		return nil, err
	}

	ps := &CmdPodService{
		backendRepo:     opts.BackendRepo,
		containerRepo:   opts.ContainerRepo,
		scheduler:       opts.Scheduler,
		rdb:             opts.RedisClient,
		keyEventManager: keyEventManager,
		tailscale:       opts.Tailscale,
		config:          opts.Config,
		eventRepo:       opts.EventRepo,
	}

	return ps, nil
}

// CreatePod creates a new container that will run to completion, with an associated task
func (s *CmdPodService) CreatePod(ctx context.Context, in *pb.CreatePodRequest) (*pb.CreatePodResponse, error) {
	log.Println("CreatePod", in)
	return &pb.CreatePodResponse{
		PodId: "123",
	}, nil
}
