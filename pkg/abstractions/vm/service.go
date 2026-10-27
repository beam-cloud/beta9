// Package vm owns persistent CPU VM identities and reuses sandbox scheduling
// and durable disks for each cold boot.
package vm

import (
	"context"
	"fmt"
	"strings"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/network"
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

type ImageVerifier interface {
	VerifyImageBuild(context.Context, *pb.VerifyImageBuildRequest) (*pb.VerifyImageBuildResponse, error)
}

type Service struct {
	ctx           context.Context
	rdb           *common.RedisClient
	repo          repository.VMRepository
	backend       repository.BackendRepository
	containers    repository.ContainerRepository
	runtime       Runtime
	gateway       Gateway
	images        ImageVerifier
	config        types.VMConfig
	publicNetwork *publicVMNetwork
	webrtc        *vmWebRTC
}

type ServiceOpts struct {
	RedisClient   *common.RedisClient
	Config        types.VMConfig
	BackendRepo   repository.BackendRepository
	ContainerRepo repository.ContainerRepository
	Runtime       Runtime
	Gateway       Gateway
	Images        ImageVerifier
	RouteGroup    *echo.Group
	Server        *echo.Echo
	BackendDialer *network.BackendDialer
	TCPConfig     types.PodTCPConfig // Shared raw TCP listener used by ICE-TCP media.
}

func New(ctx context.Context, opts ServiceOpts) error {
	repo, ok := opts.BackendRepo.(repository.VMRepository)
	if !ok {
		return fmt.Errorf("backend does not support persistent VMs")
	}

	opts.Config.BaseURL = strings.TrimSuffix(opts.Config.BaseURL, "/")
	s := &Service{
		ctx:        ctx,
		rdb:        opts.RedisClient,
		repo:       repo,
		backend:    opts.BackendRepo,
		containers: opts.ContainerRepo,
		runtime:    opts.Runtime,
		gateway:    opts.Gateway,
		images:     opts.Images,
		config:     opts.Config,
	}

	if err := s.startPublicNetwork(opts.Config.Network, opts.BackendDialer); err != nil {
		return err
	}
	if err := s.startWebRTC(opts.TCPConfig, opts.Config.WebRTC, opts.BackendDialer); err != nil {
		return err
	}

	registerVMRoutes(opts.RouteGroup, s)
	opts.Server.Any("/vm/:handle/:port", s.proxy)
	opts.Server.Any("/vm/:handle/:port/*", s.proxy)
	if s.config.Domain != "" {
		opts.Server.Pre(s.hostRoute)
	}

	go s.reconcile(ctx)
	return nil
}

func registerVMRoutes(api *echo.Group, s *Service) {
	// Resolve the authenticated workspace without a separate discovery RPC.
	// This alias has exactly the same restricted-token checks as other VM routes.
	api.Use(func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			if c.Param("workspaceId") == "self" {
				cc, ok := c.(*auth.HttpAuthContext)
				if !ok || cc.AuthInfo == nil || cc.AuthInfo.Workspace == nil {
					return echo.NewHTTPError(401)
				}
				values := append([]string(nil), c.ParamValues()...)
				for i, name := range c.ParamNames() {
					if name == "workspaceId" {
						values[i] = cc.AuthInfo.Workspace.ExternalId
					}
				}
				c.SetParamValues(values...)
			}
			return next(c)
		}
	})
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
