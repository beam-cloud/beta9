// Package vm owns persistent CPU VM identities and reuses sandbox scheduling
// and durable disks for each cold boot.
package vm

import (
	"context"
	"fmt"
	"strings"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/labstack/echo/v4"
)

type Runtime interface {
	pb.PodServiceServer
	RunVM(context.Context, *auth.AuthInfo, string, string, types.VMSpec, string, map[string]string) error
	VMPortReady(context.Context, string, string, uint32) (bool, error)
	ForwardVM(echo.Context, string, string) error
	TunnelVM(echo.Context, string, uint32) error
}

type Gateway interface {
	GetOrCreateStub(context.Context, *pb.GetOrCreateStubRequest) (*pb.GetOrCreateStubResponse, error)
	StopContainer(context.Context, *pb.StopContainerRequest) (*pb.StopContainerResponse, error)
}

type Service struct {
	ctx         context.Context
	rdb         *common.RedisClient
	repo        repository.VMRepository
	backend     repository.BackendRepository
	containers  repository.ContainerRepository
	runtime     Runtime
	gateway     Gateway
	domain      string
	baseURL     string
	defaultPool string
}

type ServiceOpts struct {
	RedisClient   *common.RedisClient
	Config        types.VMConfig
	BackendRepo   repository.BackendRepository
	ContainerRepo repository.ContainerRepository
	Runtime       Runtime
	Gateway       Gateway
	RouteGroup    *echo.Group
	Server        *echo.Echo
}

func New(ctx context.Context, opts ServiceOpts) error {
	repo, ok := opts.BackendRepo.(repository.VMRepository)
	if !ok {
		return fmt.Errorf("backend does not support persistent VMs")
	}

	s := &Service{
		ctx:         ctx,
		rdb:         opts.RedisClient,
		repo:        repo,
		backend:     opts.BackendRepo,
		containers:  opts.ContainerRepo,
		runtime:     opts.Runtime,
		gateway:     opts.Gateway,
		domain:      opts.Config.Domain,
		baseURL:     strings.TrimSuffix(opts.Config.BaseURL, "/"),
		defaultPool: opts.Config.DefaultPool,
	}

	registerVMRoutes(opts.RouteGroup, s)
	opts.Server.Any("/vm/:handle/:port", s.proxy)
	opts.Server.Any("/vm/:handle/:port/*", s.proxy)
	if s.domain != "" {
		opts.Server.Pre(s.hostRoute)
	}

	go s.reconcile(ctx)
	return nil
}

func registerVMRoutes(api *echo.Group, s *Service) {
	api.GET("/:workspaceId", auth.WithStrictWorkspaceAuth(s.list))
	api.POST("/:workspaceId", auth.WithStrictWorkspaceAuth(s.create))
	api.GET("/:workspaceId/artifacts/:kind", auth.WithStrictWorkspaceAuth(s.artifacts))
	api.DELETE("/:workspaceId/artifacts/:kind/:artifact", auth.WithStrictWorkspaceAuth(s.removeArtifact))
	api.GET("/:workspaceId/:name", auth.WithStrictWorkspaceAuth(s.get))
	api.PATCH("/:workspaceId/:name", auth.WithStrictWorkspaceAuth(s.update))
	api.GET("/:workspaceId/:name/tunnel/:port", auth.WithStrictWorkspaceAuth(s.tunnel))
	api.POST("/:workspaceId/:name/:action", auth.WithStrictWorkspaceAuth(s.action))
	api.DELETE("/:workspaceId/:name", auth.WithStrictWorkspaceAuth(s.remove))
}
