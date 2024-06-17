package gateway

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/beam-cloud/beta9/pkg/abstractions/endpoint"
	"github.com/beam-cloud/beta9/pkg/abstractions/secret"
	"github.com/beam-cloud/beta9/pkg/task"
	"github.com/labstack/echo-contrib/pprof"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"google.golang.org/grpc"

	"github.com/beam-cloud/beta9/pkg/abstractions/container"
	"github.com/beam-cloud/beta9/pkg/abstractions/function"
	"github.com/beam-cloud/beta9/pkg/abstractions/image"
	dmap "github.com/beam-cloud/beta9/pkg/abstractions/map"
	simplequeue "github.com/beam-cloud/beta9/pkg/abstractions/queue"
	"github.com/beam-cloud/beta9/pkg/abstractions/taskqueue"
	apiv1 "github.com/beam-cloud/beta9/pkg/api/v1"
	"github.com/beam-cloud/beta9/pkg/network"
	metrics "github.com/beam-cloud/beta9/pkg/repository/metrics"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"

	output "github.com/beam-cloud/beta9/pkg/abstractions/output"
	volume "github.com/beam-cloud/beta9/pkg/abstractions/volume"
	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/common"
	gatewayservices "github.com/beam-cloud/beta9/pkg/gateway/services"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/scheduler"
	"github.com/beam-cloud/beta9/pkg/storage"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
)

type Gateway struct {
	pb.UnimplementedSchedulerServer
	Config         types.AppConfig
	httpServer     *http.Server
	grpcServer     *grpc.Server
	RedisClient    *common.RedisClient
	TaskDispatcher *task.Dispatcher
	TaskRepo       repository.TaskRepository
	WorkspaceRepo  repository.WorkspaceRepository
	ContainerRepo  repository.ContainerRepository
	BackendRepo    repository.BackendRepository
	ProviderRepo   repository.ProviderRepository
	Tailscale      *network.Tailscale
	metricsRepo    repository.MetricsRepository
	Storage        storage.Storage
	Scheduler      *scheduler.Scheduler
	ctx            context.Context
	cancelFunc     context.CancelFunc
	baseRouteGroup *echo.Group
	rootRouteGroup *echo.Group
}

func NewGateway() (*Gateway, error) {
	configManager, err := common.NewConfigManager[types.AppConfig]()
	if err != nil {
		return nil, err
	}
	config := configManager.GetConfig()

	redisClient, err := common.NewRedisClient(config.Database.Redis, common.WithClientName("Beta9Gateway"))
	if err != nil {
		return nil, err
	}

	metricsRepo, err := metrics.NewMetrics(config.Monitoring, string(metrics.MetricsSourceGateway))
	if err != nil {
		return nil, err
	}

	backendRepo, err := repository.NewBackendPostgresRepository(config.Database.Postgres)
	if err != nil {
		return nil, err
	}

	storage, err := storage.NewStorage(config.Storage)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	gateway := &Gateway{
		RedisClient: redisClient,
		ctx:         ctx,
		cancelFunc:  cancel,
		Storage:     storage,
	}

	tailscaleRepo := repository.NewTailscaleRedisRepository(redisClient, config)
	tailscale := network.GetOrCreateTailscale(network.TailscaleConfig{
		ControlURL: config.Tailscale.ControlURL,
		AuthKey:    config.Tailscale.AuthKey,
		Debug:      config.Tailscale.Debug,
		Ephemeral:  true,
	}, tailscaleRepo)

	workspaceRepo := repository.NewWorkspaceRedisRepository(redisClient)

	scheduler, err := scheduler.NewScheduler(ctx, config, redisClient, metricsRepo, backendRepo, workspaceRepo, tailscale)
	if err != nil {
		return nil, err
	}

	containerRepo := repository.NewContainerRedisRepository(redisClient)
	providerRepo := repository.NewProviderRedisRepository(redisClient)
	taskRepo := repository.NewTaskRedisRepository(redisClient)
	taskDispatcher, err := task.NewDispatcher(ctx, taskRepo)
	if err != nil {
		return nil, err
	}

	gateway.Config = config
	gateway.Scheduler = scheduler
	gateway.TaskRepo = taskRepo
	gateway.WorkspaceRepo = workspaceRepo
	gateway.ContainerRepo = containerRepo
	gateway.ProviderRepo = providerRepo
	gateway.BackendRepo = backendRepo
	gateway.Tailscale = tailscale
	gateway.TaskDispatcher = taskDispatcher
	gateway.metricsRepo = metricsRepo

	return gateway, nil
}

func (g *Gateway) initHttp() error {
	e := echo.New()
	e.HideBanner = true
	e.HidePort = true

	if g.Config.DebugMode {
		pprof.Register(e)
	}

	e.Pre(middleware.RemoveTrailingSlash())
	configureEchoLogger(e, g.Config.GatewayService.HTTP.EnablePrettyLogs)
	e.Use(middleware.CORSWithConfig(middleware.CORSConfig{
		AllowOrigins: g.Config.GatewayService.HTTP.CORS.AllowedOrigins,
		AllowHeaders: g.Config.GatewayService.HTTP.CORS.AllowedHeaders,
		AllowMethods: g.Config.GatewayService.HTTP.CORS.AllowedMethods,
	}))
	e.Use(middleware.Recover())

	// Accept both HTTP/2 and HTTP/1
	g.httpServer = &http.Server{
		Addr:    fmt.Sprintf(":%v", g.Config.GatewayService.HTTP.Port),
		Handler: h2c.NewHandler(e, &http2.Server{}),
	}

	authMiddleware := auth.AuthMiddleware(g.BackendRepo)
	g.baseRouteGroup = e.Group(apiv1.HttpServerBaseRoute)
	g.rootRouteGroup = e.Group(apiv1.HttpServerRootRoute)

	apiv1.NewHealthGroup(g.baseRouteGroup.Group("/health"), g.RedisClient)
	apiv1.NewMachineGroup(g.baseRouteGroup.Group("/machine", authMiddleware), g.ProviderRepo, g.Tailscale, g.Config)
	apiv1.NewWorkspaceGroup(g.baseRouteGroup.Group("/workspace", authMiddleware), g.BackendRepo, g.Config)
	apiv1.NewTokenGroup(g.baseRouteGroup.Group("/token", authMiddleware), g.BackendRepo, g.Config)
	apiv1.NewTaskGroup(g.baseRouteGroup.Group("/task", authMiddleware), g.RedisClient, g.BackendRepo, g.TaskDispatcher, g.Config)
	apiv1.NewContainerGroup(g.baseRouteGroup.Group("/container", authMiddleware), g.BackendRepo, g.ContainerRepo, *g.Scheduler, g.Config)
	apiv1.NewStubGroup(g.baseRouteGroup.Group("/stub", authMiddleware), g.BackendRepo, g.Config)
	apiv1.NewConcurrencyLimitGroup(g.baseRouteGroup.Group("/concurrency-limit", authMiddleware), g.BackendRepo, g.WorkspaceRepo)
	apiv1.NewDeploymentGroup(g.baseRouteGroup.Group("/deployment", authMiddleware), g.BackendRepo, g.ContainerRepo, *g.Scheduler, g.RedisClient, g.Config)

	return nil
}

func (g *Gateway) initGrpc() error {
	authInterceptor := auth.NewAuthInterceptor(g.BackendRepo)

	serverOptions := []grpc.ServerOption{
		grpc.UnaryInterceptor(authInterceptor.Unary()),
		grpc.StreamInterceptor(authInterceptor.Stream()),
		grpc.MaxRecvMsgSize(g.Config.GatewayService.GRPC.MaxRecvMsgSize * 1024 * 1024),
		grpc.MaxSendMsgSize(g.Config.GatewayService.GRPC.MaxSendMsgSize * 1024 * 1024),
	}

	g.grpcServer = grpc.NewServer(
		serverOptions...,
	)

	return nil
}

func (g *Gateway) registerServices() error {
	// Register map service
	rm, err := dmap.NewRedisMapService(g.RedisClient)
	if err != nil {
		return err
	}
	pb.RegisterMapServiceServer(g.grpcServer, rm)

	// Register simple queue service
	rq, err := simplequeue.NewRedisSimpleQueueService(g.RedisClient)
	if err != nil {
		return err
	}
	pb.RegisterSimpleQueueServiceServer(g.grpcServer, rq)

	// Register image service
	is, err := image.NewRuncImageService(g.ctx, image.ImageServiceOpts{
		Config:        g.Config,
		ContainerRepo: g.ContainerRepo,
		Scheduler:     g.Scheduler,
		Tailscale:     g.Tailscale,
	})
	if err != nil {
		return err
	}
	pb.RegisterImageServiceServer(g.grpcServer, is)

	// Register function service
	fs, err := function.NewRuncFunctionService(g.ctx, function.FunctionServiceOpts{
		Config:         g.Config,
		RedisClient:    g.RedisClient,
		BackendRepo:    g.BackendRepo,
		TaskRepo:       g.TaskRepo,
		ContainerRepo:  g.ContainerRepo,
		Scheduler:      g.Scheduler,
		Tailscale:      g.Tailscale,
		RouteGroup:     g.rootRouteGroup,
		TaskDispatcher: g.TaskDispatcher,
	})
	if err != nil {
		return err
	}
	pb.RegisterFunctionServiceServer(g.grpcServer, fs)

	// Register task queue service
	tq, err := taskqueue.NewRedisTaskQueueService(g.ctx, taskqueue.TaskQueueServiceOpts{
		Config:         g.Config,
		RedisClient:    g.RedisClient,
		BackendRepo:    g.BackendRepo,
		TaskRepo:       g.TaskRepo,
		ContainerRepo:  g.ContainerRepo,
		Scheduler:      g.Scheduler,
		Tailscale:      g.Tailscale,
		RouteGroup:     g.rootRouteGroup,
		TaskDispatcher: g.TaskDispatcher,
	})
	if err != nil {
		return err
	}
	pb.RegisterTaskQueueServiceServer(g.grpcServer, tq)

	// Register endpoint service
	ws, err := endpoint.NewHTTPEndpointService(g.ctx, endpoint.EndpointServiceOpts{
		ContainerRepo:  g.ContainerRepo,
		BackendRepo:    g.BackendRepo,
		TaskRepo:       g.TaskRepo,
		RedisClient:    g.RedisClient,
		Scheduler:      g.Scheduler,
		RouteGroup:     g.rootRouteGroup,
		Tailscale:      g.Tailscale,
		TaskDispatcher: g.TaskDispatcher,
	})
	if err != nil {
		return err
	}
	pb.RegisterEndpointServiceServer(g.grpcServer, ws)

	// Register volume service
	vs, err := volume.NewGlobalVolumeService(g.BackendRepo, g.RedisClient, g.rootRouteGroup)
	if err != nil {
		return err
	}
	pb.RegisterVolumeServiceServer(g.grpcServer, vs)

	// Register container service
	cs, err := container.NewContainerService(
		g.ctx,
		container.ContainerServiceOpts{
			Config:        g.Config,
			BackendRepo:   g.BackendRepo,
			ContainerRepo: g.ContainerRepo,
			Tailscale:     g.Tailscale,
			Scheduler:     g.Scheduler,
			RedisClient:   g.RedisClient,
		},
	)
	if err != nil {
		return err
	}
	pb.RegisterContainerServiceServer(g.grpcServer, cs)

	// Register output service
	o, err := output.NewOutputRedisService(g.Config, g.RedisClient, g.BackendRepo, g.rootRouteGroup)
	if err != nil {
		return err
	}
	pb.RegisterOutputServiceServer(g.grpcServer, o)

	// Register Secret service
	ss := secret.NewSecretService(g.BackendRepo, g.rootRouteGroup)
	pb.RegisterSecretServiceServer(g.grpcServer, ss)

	// Register scheduler
	s, err := scheduler.NewSchedulerService(g.Scheduler)
	if err != nil {
		return err
	}
	pb.RegisterSchedulerServer(g.grpcServer, s)

	// Register gateway services
	// (catch-all for external gateway grpc endpoints that don't fit into an abstraction)
	gws, err := gatewayservices.NewGatewayService(&gatewayservices.GatewayServiceOpts{
		Config:         g.Config,
		BackendRepo:    g.BackendRepo,
		ContainerRepo:  g.ContainerRepo,
		ProviderRepo:   g.ProviderRepo,
		Scheduler:      g.Scheduler,
		TaskDispatcher: g.TaskDispatcher,
		RedisClient:    g.RedisClient,
	})
	if err != nil {
		return err
	}
	pb.RegisterGatewayServiceServer(g.grpcServer, gws)

	return nil
}

// Gateway entry point
func (g *Gateway) Start() error {
	var err error

	err = g.initGrpc()
	if err != nil {
		log.Fatalf("Failed to initialize grpc server: %v", err)
	}

	err = g.initHttp()
	if err != nil {
		log.Fatalf("Failed to initialize http server: %v", err)
	}

	err = g.registerServices()
	if err != nil {
		log.Fatalf("Failed to register services: %v", err)
	}

	go func() {
		lis, err := net.Listen("tcp", fmt.Sprintf(":%d", g.Config.GatewayService.GRPC.Port))
		if err != nil {
			log.Fatalf("Failed to listen: %v", err)
		}

		if g.grpcServer.Serve(lis); err != nil {
			log.Fatalf("Failed to start grpc server: %v\n", err)
		}
	}()

	go func() {
		lis, err := net.Listen("tcp", fmt.Sprintf(":%d", g.Config.GatewayService.HTTP.Port))
		if err != nil {
			log.Fatalf("Failed to listen: %v", err)
		}

		if err := g.httpServer.Serve(lis); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Failed to start http server: %v", err)
		}
	}()

	log.Println("Gateway http server running @", g.Config.GatewayService.HTTP.Port)
	log.Println("Gateway grpc server running @", g.Config.GatewayService.GRPC.Port)

	terminationSignal := make(chan os.Signal, 1)
	defer close(terminationSignal)

	signal.Notify(terminationSignal, os.Interrupt, syscall.SIGTERM)

	<-terminationSignal
	log.Println("Termination signal received. Shutting down...")

	ctx, cancel := context.WithTimeout(g.ctx, g.Config.GatewayService.ShutdownTimeout)
	defer cancel()
	g.httpServer.Shutdown(ctx)
	g.grpcServer.GracefulStop()
	g.cancelFunc()

	return nil
}
