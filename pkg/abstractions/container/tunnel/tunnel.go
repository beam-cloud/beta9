package container_tunnel

import (
	"context"
	"time"

	pb "github.com/beam-cloud/beta9/proto"

	container_common "github.com/beam-cloud/beta9/pkg/abstractions/container/common"
	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/network"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/scheduler"
	"github.com/beam-cloud/beta9/pkg/types"
)

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

func (ts *ContainerTunnelService) CreateTunnel(ctx context.Context, in *pb.CreateTunnelRequest) (*pb.CreateTunnelResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	hostname, err := ts.containerRepo.GetWorkerAddress(ctx, in.ContainerId)
	if err != nil {
		return &pb.CreateTunnelResponse{
			Ok: false,
		}, nil
	}

	conn, err := network.ConnectToHost(ctx, hostname, time.Second*30, ts.tailscale, ts.config.Tailscale)
	if err != nil {
		return &pb.CreateTunnelResponse{
			Ok: false,
		}, nil
	}
	defer conn.Close()

	client, err := common.NewRunCClient(hostname, authInfo.Token.Key, conn)
	if err != nil {
		return &pb.CreateTunnelResponse{
			Ok: false,
		}, nil
	}
	defer client.Close()

	r, err := client.ExposePort(in.ContainerId, in.Port)
	if err != nil {
		return &pb.CreateTunnelResponse{
			Ok: false,
		}, nil
	}

	if r.Ok {
		err := ts.rdb.Set(ctx, container_common.Keys.ContainerTunnelAddress(authInfo.Workspace.Name, in.ContainerId), "itismy", 0).Err()
		if err != nil {
			return &pb.CreateTunnelResponse{Ok: false}, nil
		}
	}

	return &pb.CreateTunnelResponse{
		Ok: r.Ok,
	}, nil
}
