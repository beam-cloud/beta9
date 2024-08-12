package container_tunnel

import (
	"context"

	pb "github.com/beam-cloud/beta9/proto"

	container_common "github.com/beam-cloud/beta9/pkg/abstractions/container/common"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/network"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/scheduler"
	"github.com/beam-cloud/beta9/pkg/types"
)

type ContainerTunnel struct {
}

type ContainerTunnelService struct {
	pb.ContainerServiceServer
	backendRepo     repository.BackendRepository
	containerRepo   repository.ContainerRepository
	scheduler       *scheduler.Scheduler
	keyEventManager *common.KeyEventManager
	rdb             *common.RedisClient
	tailscale       *network.Tailscale
	config          types.AppConfig
	eventRepo       repository.EventRepository
}

func NewContainerTunnelService(
	ctx context.Context,
	opts container_common.ContainerServiceOpts,
) (*ContainerTunnelService, error) {
	keyEventManager, err := common.NewKeyEventManager(opts.RedisClient)
	if err != nil {
		return nil, err
	}

	svc := &ContainerTunnelService{
		backendRepo:     opts.BackendRepo,
		containerRepo:   opts.ContainerRepo,
		scheduler:       opts.Scheduler,
		rdb:             opts.RedisClient,
		keyEventManager: keyEventManager,
		tailscale:       opts.Tailscale,
		config:          opts.Config,
		eventRepo:       opts.EventRepo,
	}

	return svc, nil
}
